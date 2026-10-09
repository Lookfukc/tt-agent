package ltm_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/ltm"
	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// TestMemoryEmitsFactEvents proves Remember and Forget fire events
// carrying the namespace and fact ID — and nothing else.
func TestMemoryEmitsFactEvents(t *testing.T) {
	var mu sync.Mutex
	var events []observer.MemoryEvent
	sink := observer.MemoryObserverFunc(func(e observer.MemoryEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	bus := observer.NewMemoryEventBus(16, sink)
	defer bus.Close()

	mem := ltm.New(ltm.NewMemoryStore(ltm.MemoryOptions{}), ltm.Options{
		Observer: bus,
		Backend:  "memory",
	})
	ctx := context.Background()

	fact, err := mem.Remember(ctx, "u1", "The user loves pizza", nil)
	if err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if err := mem.Forget(ctx, "u1", fact.ID); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	bus.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2: %+v", len(events), events)
	}
	if events[0].Kind != observer.EventFactRemembered || events[1].Kind != observer.EventFactForgotten {
		t.Fatalf("kinds = %s, %s", events[0].Kind, events[1].Kind)
	}
	for _, e := range events {
		if e.Backend != "memory" || e.Session != "u1" {
			t.Fatalf("event mislabeled: %+v", e)
		}
		if e.Detail["fact_id"] != fact.ID {
			t.Fatalf("fact id missing: %+v", e.Detail)
		}
	}
}
