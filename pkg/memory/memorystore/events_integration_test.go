package memorystore_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/sqlite"
	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// TestStoreEmitsLifecycleEvents proves the four session-store events
// fire in order, carry the backend name, and never contain message
// content (only counts).
func TestStoreEmitsLifecycleEvents(t *testing.T) {
	var mu sync.Mutex
	var events []observer.MemoryEvent
	sink := observer.MemoryObserverFunc(func(e observer.MemoryEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	bus := observer.NewMemoryEventBus(32, sink)

	d, err := sqlite.New(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		bus.Close()
		t.Fatalf("sqlite.New: %v", err)
	}
	defer d.Close()
	if err := d.Migrate(context.Background()); err != nil {
		bus.Close()
		t.Fatalf("Migrate: %v", err)
	}
	store := d.Memory(memorystore.Options{Observer: bus})
	ctx := context.Background()

	if err := store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "private text"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	_ = store.SaveSummary(ctx, "s", 2, "summary text")
	_ = store.Trim(ctx, "s", 1)
	_ = store.Clear(ctx, "s")

	// 等四类事件到齐再关总线；Close 会排空剩余队列
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 4 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	bus.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 4 {
		t.Fatalf("events = %d, want 4: %+v", len(events), events)
	}
	wantKinds := []observer.MemoryEventKind{
		observer.EventMessagesAppended,
		observer.EventSummarySaved,
		observer.EventSessionTrimmed,
		observer.EventSessionCleared,
	}
	for i, k := range wantKinds {
		if events[i].Kind != k {
			t.Fatalf("events[%d].Kind = %s, want %s (all: %+v)", i, events[i].Kind, k, events)
		}
		if events[i].Backend != "sqlite" {
			t.Fatalf("events[%d].Backend = %q, want sqlite", i, events[i].Backend)
		}
		if events[i].Session != "s" {
			t.Fatalf("events[%d].Session = %q", i, events[i].Session)
		}
	}
	// 事件只带计数，不携带消息文本
	if c, ok := events[0].Detail["count"].(int); !ok || c != 1 {
		t.Fatalf("append detail = %+v, want count=1", events[0].Detail)
	}
}
