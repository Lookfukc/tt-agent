package observer_test

import (
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// collector records events for assertions.
type collector struct {
	mu     sync.Mutex
	events []observer.MemoryEvent
	// block, when closed, makes OnMemoryEvent hang: for the
	// non-blocking assertion.
	block chan struct{}
}

func (c *collector) OnMemoryEvent(e observer.MemoryEvent) {
	if c.block != nil {
		<-c.block
	}
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

func (c *collector) snapshot() []observer.MemoryEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]observer.MemoryEvent(nil), c.events...)
}

// waitFor polls until cond passes.
func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

// TestBusDeliversAsynchronously proves emission returns before
// delivery: a slow observer must not slow the emitter.
func TestBusDeliversAsynchronously(t *testing.T) {
	c := &collector{block: make(chan struct{})}
	bus := observer.NewMemoryEventBus(8, c)
	defer bus.Close()

	start := time.Now()
	for i := 0; i < 4; i++ {
		bus.Emit(observer.MemoryEvent{Kind: observer.EventMessagesAppended})
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Emit blocked %s on a slow observer", elapsed)
	}
	close(c.block) // release the collector
	waitFor(t, "4 events", func() bool { return len(c.snapshot()) == 4 })
}

// TestBusCloseFlushesQueued proves Close delivers everything emitted
// so far — tests can assert without sleeps.
func TestBusCloseFlushesQueued(t *testing.T) {
	c := &collector{block: make(chan struct{})}
	// 队列大于默认派发节奏：先灌满再放行
	bus := observer.NewMemoryEventBus(64, c)
	for i := 0; i < 10; i++ {
		bus.Emit(observer.MemoryEvent{Kind: observer.EventSessionCleared})
	}
	close(c.block)
	bus.Close()
	if got := len(c.snapshot()); got != 10 {
		t.Fatalf("delivered %d events after Close, want 10", got)
	}
}

// TestBusDropsUnderBackpressure proves saturation drops and counts
// instead of blocking or panicking.
func TestBusDropsUnderBackpressure(t *testing.T) {
	c := &collector{block: make(chan struct{})}
	bus := observer.NewMemoryEventBus(4, c) // tiny queue
	defer func() {
		close(c.block)
		bus.Close()
	}()
	for i := 0; i < 100; i++ {
		bus.Emit(observer.MemoryEvent{Kind: observer.EventMessagesAppended})
	}
	if bus.Drops() == 0 {
		t.Fatal("expected drops with a tiny queue and a blocked observer")
	}
}

// TestBusRecoversPanickingObserver proves one bad sink cannot kill the
// dispatcher: later events still reach healthy observers.
func TestBusRecoversPanickingObserver(t *testing.T) {
	good := &collector{}
	bad := &collector{block: make(chan struct{})}
	close(bad.block) // unused; panics below instead

	panicSink := observer.MemoryObserverFunc(func(observer.MemoryEvent) {
		panic("boom")
	})
	bus := observer.NewMemoryEventBus(8, panicSink, good)
	defer bus.Close()

	bus.Emit(observer.MemoryEvent{Kind: observer.EventFactRemembered})
	waitFor(t, "healthy observer still served", func() bool { return len(good.snapshot()) == 1 })
}
