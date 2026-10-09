// Memory events: an observability surface for the memory subsystem.
//
// Session stores, snapshots and long-term memory emit lifecycle events
// so operators can audit, mirror or react to memory changes without
// wrapping every implementation. Delivery is asynchronous by contract:
// a slow observer must never slow a memory write.
package observer

import (
	"sync"
	"sync/atomic"
	"time"
)

// MemoryEventKind enumerates memory lifecycle events.
type MemoryEventKind string

const (
	// EventMessagesAppended fires when messages join a session.
	EventMessagesAppended MemoryEventKind = "messages_appended"
	// EventSessionTrimmed fires when the oldest non-system messages
	// are physically discarded.
	EventSessionTrimmed MemoryEventKind = "session_trimmed"
	// EventSessionCleared fires when a session is deleted entirely.
	EventSessionCleared MemoryEventKind = "session_cleared"
	// EventSummarySaved fires when a compaction summary is persisted.
	EventSummarySaved MemoryEventKind = "summary_saved"
	// EventSnapshotCaptured fires when a session snapshot is taken.
	EventSnapshotCaptured MemoryEventKind = "snapshot_captured"
	// EventSessionRolledBack fires when a session is restored from a
	// snapshot.
	EventSessionRolledBack MemoryEventKind = "session_rolled_back"
	// EventFactRemembered fires when a long-term fact is stored.
	EventFactRemembered MemoryEventKind = "fact_remembered"
	// EventFactForgotten fires when a long-term fact is deleted.
	EventFactForgotten MemoryEventKind = "fact_forgotten"
)

// MemoryEvent describes one memory lifecycle occurrence.
//
// Events deliberately carry no message content — only counts and IDs.
// Observers (notably HTTP webhooks) may leave the process, and leaks
// must not be one misconfigured URL away.
type MemoryEvent struct {
	Kind    MemoryEventKind `json:"kind"`
	Backend string          `json:"backend,omitempty"` // redis / sqlite / postgres / file / ltm
	Session string          `json:"session,omitempty"` // session ID or namespace
	At      time.Time       `json:"at"`
	Detail  map[string]any  `json:"detail,omitempty"`
}

// MemoryObserver receives memory events.
//
// Implementations are invoked asynchronously through a bus (see
// NewMemoryEventBus); with a direct hand-off they would sit on the
// memory write path, which the contract forbids.
type MemoryObserver interface {
	OnMemoryEvent(e MemoryEvent)
}

// MemoryObserverFunc adapts a function to MemoryObserver.
type MemoryObserverFunc func(e MemoryEvent)

// OnMemoryEvent implements MemoryObserver.
func (f MemoryObserverFunc) OnMemoryEvent(e MemoryEvent) { f(e) }

// MemoryEventBus dispatches events to registered observers
// asynchronously.
//
// Emit never blocks: events land in a buffered queue and a single
// dispatcher goroutine fans them out. When the queue is full the
// newest event is dropped and counted — memory writes must proceed
// even if observability is backed up.
type MemoryEventBus struct {
	observers []MemoryObserver
	queue     chan MemoryEvent
	done      chan struct{}
	wg        sync.WaitGroup

	closed atomic.Bool
	drops  atomic.Uint64
	mu     sync.Mutex
}

// defaultEventQueue bounds the dispatch backlog.
const defaultEventQueue = 1024

// NewMemoryEventBus builds a bus fan-out to the given observers.
//
// buffer <= 0 uses the default; a larger buffer absorbs observer
// bursts at the cost of memory. Start is implicit: the dispatcher
// goroutine begins with construction and stops on Close.
func NewMemoryEventBus(buffer int, observers ...MemoryObserver) *MemoryEventBus {
	if buffer <= 0 {
		buffer = defaultEventQueue
	}
	b := &MemoryEventBus{
		observers: observers,
		queue:     make(chan MemoryEvent, buffer),
		done:      make(chan struct{}),
	}
	b.wg.Add(1)
	go b.dispatch()
	return b
}

// Emit queues an event for asynchronous delivery.
//
// It is the method memory stores call; it never blocks and never
// fails. Dropping under backpressure is the intended behavior.
func (b *MemoryEventBus) Emit(e MemoryEvent) {
	if b.closed.Load() {
		return
	}
	if e.At.IsZero() {
		e.At = time.Now()
	}
	select {
	case b.queue <- e:
	default:
		b.drops.Add(1) // observability must not stall the write path
	}
}

// OnMemoryEvent implements MemoryObserver, so one bus can forward to
// another (chaining aggregation trees).
func (b *MemoryEventBus) OnMemoryEvent(e MemoryEvent) { b.Emit(e) }

// Drops reports how many events were discarded under backpressure.
func (b *MemoryEventBus) Drops() uint64 { return b.drops.Load() }

// Close stops dispatching and waits for in-flight fan-out to finish.
//
// Events still queued are delivered before Close returns, so a test or
// shutdown can assert on everything emitted so far.
func (b *MemoryEventBus) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	close(b.done)
	b.wg.Wait()
}

// dispatch fans queued events out to observers until done.
func (b *MemoryEventBus) dispatch() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			b.drain()
			return
		case e := <-b.queue:
			b.fanout(e)
		}
	}
}

// drain delivers whatever is still queued at shutdown.
func (b *MemoryEventBus) drain() {
	for {
		select {
		case e := <-b.queue:
			b.fanout(e)
		default:
			return
		}
	}
}

// fanout hands one event to every observer.
//
// A panicking observer is recovered and skipped: one bad sink must not
// take down the memory path's dispatcher.
func (b *MemoryEventBus) fanout(e MemoryEvent) {
	b.mu.Lock()
	observers := b.observers
	b.mu.Unlock()
	for _, o := range observers {
		func() {
			defer func() { _ = recover() }()
			o.OnMemoryEvent(e)
		}()
	}
}
