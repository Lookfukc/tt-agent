package memory

import (
	"context"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// Default eviction parameters: evict after half an hour of idle, sweep
// every five minutes.
const (
	defaultIdle  = 30 * time.Minute
	defaultSweep = 5 * time.Minute
)

// TTL is an idle-eviction decorator.
//
// It wraps any core.Memory: Add/Recent touch the session's activity
// time, and sessions idle for longer than idle are cleared
// periodically by a single background janitor. For Persistent, Clear
// also deletes from disk — this is the unified exit for unbounded
// growth of both memory and disk.
//
// Two pitfalls of the old implementation are avoided here:
//   - the janitor starts once per construction (not one goroutine per session)
//   - expiry uses last-activity time, not creation time (long-lived
//     sessions are not killed mid-run)
type TTL struct {
	inner core.Memory
	idle  time.Duration
	sweep time.Duration

	mu     sync.Mutex
	last   map[string]time.Time
	cancel context.CancelFunc
	done   chan struct{}
}

// NewTTL constructs an idle-evicting memory.
//
// The janitor starts with construction and stops when ctx is
// cancelled; the actual eviction moment falls within [idle,
// idle+sweep) — decrease sweep for tighter control.
// ctx: janitor lifetime; after cancel no more eviction happens
// (already-stored data is left untouched).
// inner: the actual storage; Persistent/Summary both work.
// idle: how long idle before eviction; <= 0 uses the default of 30 minutes.
// sweep: check interval; <= 0 uses the default of 5 minutes, should be smaller than idle.
// returns: a ready instance.
func NewTTL(ctx context.Context, inner core.Memory, idle, sweep time.Duration) *TTL {
	if idle <= 0 {
		idle = defaultIdle
	}
	if sweep <= 0 {
		sweep = defaultSweep
	}
	ctx, cancel := context.WithCancel(ctx)
	t := &TTL{
		inner:  inner,
		idle:   idle,
		sweep:  sweep,
		last:   make(map[string]time.Time),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go t.janitor(ctx)
	return t
}

// Add appends messages and touches the activity time.
func (t *TTL) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	t.touch(sessionID)
	return t.inner.Add(ctx, sessionID, msgs...)
}

// Recent returns messages and touches the activity time.
func (t *TTL) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	t.touch(sessionID)
	return t.inner.Recent(ctx, sessionID, budget)
}

// Clear empties the session and removes it from the tracking table.
func (t *TTL) Clear(ctx context.Context, sessionID string) error {
	t.mu.Lock()
	delete(t.last, sessionID)
	t.mu.Unlock()
	return t.inner.Clear(ctx, sessionID)
}

// Unwrap exposes the inner storage.
//
// Equal-wrapper decoration layers like Summary use this to reach the
// Split/Trim implementation through TTL; paths that bypass touch must
// touch on their own (see Summary.splitInner).
func (t *TTL) Unwrap() core.Memory { return t.inner }

// touch records the session's activity time, executed before the
// inner operation: the activity time the janitor sees while holding
// the lock is guaranteed to include this access.
func (t *TTL) touch(sessionID string) {
	t.mu.Lock()
	t.last[sessionID] = time.Now()
	t.mu.Unlock()
}

// janitor periodically clears idle sessions and exits when ctx is
// cancelled.
func (t *TTL) janitor(ctx context.Context) {
	defer close(t.done)
	tk := time.NewTicker(t.sweep)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.sweepExpired()
		}
	}
}

// sweepExpired clears sessions past the idle threshold.
//
// inner.Clear completes while holding the lock: if Clear removal and
// the sweep were not atomic, the window "scan judges expired →
// release lock → Add touches and recreates → Clear deletes the new
// data" genuinely exists. Clear is a map delete plus a file delete;
// the cost of holding the lock is negligible.
func (t *TTL) sweepExpired() {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, at := range t.last {
		if now.Sub(at) <= t.idle {
			continue
		}
		// The Clear used for eviction does not use the janitor's ctx:
		// that is a lifetime signal, not a cancellation signal for
		// this sweep.
		_ = t.inner.Clear(context.Background(), id)
		delete(t.last, id)
	}
}
