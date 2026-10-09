package memorystore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/sqlite"
)

// TestCappedReadsCounter proves reads that hit the maxScan window are
// counted, and normal reads are not: the silent-loss condition the
// counter exists to expose becomes observable.
func TestCappedReadsCounter(t *testing.T) {
	d, err := sqlite.New(filepath.Join(t.TempDir(), "cap.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	defer d.Close()
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// 小窗口：5 条
	store := d.Memory(memorystore.Options{MaxScan: 5})
	ctx := context.Background()

	var msgs []core.Message
	for i := 0; i < 12; i++ {
		msgs = append(msgs, core.Message{Role: core.RoleUser, Content: "m"})
	}
	if err := store.Add(ctx, "s", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if got := store.CappedReads(); got != 0 {
		t.Fatalf("counter = %d before any read", got)
	}
	if _, err := store.Recent(ctx, "s", 1<<30); err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if got := store.CappedReads(); got != 1 {
		t.Fatalf("capped read not counted: %d", got)
	}

	// 短会话读不计数
	if err := store.Add(ctx, "short", core.Message{Role: core.RoleUser, Content: "x"}); err != nil {
		t.Fatalf("Add short: %v", err)
	}
	if _, err := store.Recent(ctx, "short", 1<<30); err != nil {
		t.Fatalf("Recent short: %v", err)
	}
	if got := store.CappedReads(); got != 1 {
		t.Fatalf("short read wrongly counted: %d", got)
	}

	// 再读一次长会话，计数继续增长
	_, _ = store.Recent(ctx, "s", 1<<30)
	if got := store.CappedReads(); got != 2 {
		t.Fatalf("counter = %d after second capped read", got)
	}
}
