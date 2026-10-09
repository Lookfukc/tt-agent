package snapshot_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/snapshot"
	"github.com/Lookfukc/tt-agent/pkg/memory/sqlite"
)

// fixture bundles a driver, its memory view and the snapshot store, all
// sharing one database so snapshots ride the same storage as the log.
type fixture struct {
	driver *sqlite.Driver
	mem    *memorystore.Store
	snaps  *snapshot.Store
}

// newFixture builds a migrated SQLite-backed fixture.
//
// It takes testing.TB so benchmarks share the same setup.
func newFixture(t testing.TB) fixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snap.db")
	d, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return fixture{
		driver: d,
		mem:    d.Memory(memorystore.Options{}),
		snaps:  snapshot.New(d, snapshot.Options{}),
	}
}

// TestCaptureAndRollback is the core round trip: take a snapshot, add
// more messages, roll back, and confirm the session looks like it did.
func TestCaptureAndRollback(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleSystem, Content: "sys"})
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "first"})

	entry, err := f.snaps.Capture(ctx, "s", "before-second")
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if entry.MessageCount != 2 {
		t.Fatalf("captured count = %d, want 2", entry.MessageCount)
	}
	if entry.Label != "before-second" || entry.ID == "" {
		t.Fatalf("entry = %+v", entry)
	}

	// 快照之后再写入若干消息
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "second"})
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleAssistant, Content: "third"})
	if got, _ := f.mem.Recent(ctx, "s", 1<<20); len(got) != 4 {
		t.Fatalf("before rollback = %d msgs, want 4", len(got))
	}

	if _, err := f.snaps.Rollback(ctx, "s", entry.ID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	got, err := f.mem.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent after rollback: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("after rollback = %d msgs, want 2: %+v", len(got), got)
	}
	if got[0].Role != core.RoleSystem || got[0].Content != "sys" || got[1].Content != "first" {
		t.Fatalf("rollback restored wrong state: %+v", got)
	}
}

// TestRollbackPreservesMessageShape proves a restored tool-call exchange
// keeps the pairing that providers demand.
func TestRollbackPreservesMessageShape(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	_ = f.mem.Add(ctx, "s",
		core.Message{Role: core.RoleUser, Content: "ask"},
		core.Message{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
			{ID: "c1", Name: "calc", Arguments: `{"x":1}`},
		}},
		core.Message{Role: core.RoleTool, ToolCallID: "c1", Content: "42"},
	)
	entry, err := f.snaps.Capture(ctx, "s", "with-tool-call")
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleAssistant, Content: "after"})

	if _, err := f.snaps.Rollback(ctx, "s", entry.ID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	got, _ := f.mem.Recent(ctx, "s", 1<<20)
	if len(got) != 3 {
		t.Fatalf("restored %d msgs, want 3", len(got))
	}
	if len(got[1].ToolCalls) != 1 || got[1].ToolCalls[0].ID != "c1" {
		t.Fatalf("tool call lost in restore: %+v", got[1])
	}
	if got[2].Role != core.RoleTool || got[2].ToolCallID != "c1" {
		t.Fatalf("tool result lost in restore: %+v", got[2])
	}
}

// TestListReturnsNewestFirst covers enumeration.
func TestListReturnsNewestFirst(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "x"})

	first, err := f.snaps.Capture(ctx, "s", "one")
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	// 保证时间戳不同（纳秒级时钟在同一循环里可能相同）
	time.Sleep(2 * time.Millisecond)
	second, err := f.snaps.Capture(ctx, "s", "two")
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}

	list, err := f.snaps.List(ctx, "s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("snapshots = %d, want 2: %+v", len(list), list)
	}
	if list[0].ID != second.ID || list[1].ID != first.ID {
		t.Fatalf("order wrong: %+v", list)
	}
}

// TestDeleteRemovesOnlyTarget proves deletion is precise.
func TestDeleteRemovesOnlyTarget(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "x"})

	keep, _ := f.snaps.Capture(ctx, "s", "keep")
	time.Sleep(2 * time.Millisecond)
	drop, _ := f.snaps.Capture(ctx, "s", "drop")

	if err := f.snaps.Delete(ctx, "s", drop.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	list, err := f.snaps.List(ctx, "s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].ID != keep.ID {
		t.Fatalf("wrong snapshot survived: %+v", list)
	}
}

// TestRollbackUnknownSnapshotIsReported covers the error path.
func TestRollbackUnknownSnapshotIsReported(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "x"})

	if _, err := f.snaps.Rollback(ctx, "s", "no-such-id"); !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Fatalf("err = %v, want ErrNoSnapshot", err)
	}
}

// TestRollbackSafeKeepsPreRollbackState is the whole point of the safe
// variant: after rolling back, the discarded state is still one
// Rollback away, so a bad decision is reversible.
func TestRollbackSafeKeepsPreRollbackState(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "good turn"})
	early, err := f.snaps.Capture(ctx, "s", "early")
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "bad turn"})

	restored, safety, err := f.snaps.RollbackSafe(ctx, "s", early.ID, "before-undo")
	if err != nil {
		t.Fatalf("RollbackSafe: %v", err)
	}
	if restored.ID != early.ID {
		t.Fatalf("restored = %s, want %s", restored.ID, early.ID)
	}
	if safety.Label != "before-undo" || safety.MessageCount != 2 {
		t.Fatalf("safety entry = %+v", safety)
	}

	// 回滚后只剩 good turn
	got, _ := f.mem.Recent(ctx, "s", 1<<20)
	if len(got) != 1 || got[0].Content != "good turn" {
		t.Fatalf("after RollbackSafe = %+v", got)
	}

	// 后悔了：滚回安全快照，bad turn 回来了
	if _, err := f.snaps.Rollback(ctx, "s", safety.ID); err != nil {
		t.Fatalf("undo via safety: %v", err)
	}
	got, _ = f.mem.Recent(ctx, "s", 1<<20)
	if len(got) != 2 || got[1].Content != "bad turn" {
		t.Fatalf("safety rollback did not restore the discarded state: %+v", got)
	}
}

// TestRollbackSafeBadTargetLeavesNoSafetySnapshot proves a mistyped
// target ID fails before capturing, so it cannot litter the snapshot
// log with pointless entries.
func TestRollbackSafeBadTargetLeavesNoSafetySnapshot(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "x"})

	if _, _, err := f.snaps.RollbackSafe(ctx, "s", "typo-id", "safety"); !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Fatalf("err = %v, want ErrNoSnapshot", err)
	}
	list, err := f.snaps.List(ctx, "s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("failed RollbackSafe left snapshots behind: %+v", list)
	}
}

// TestMaxPerSessionEvictsOldest proves snapshot history is bounded.
func TestMaxPerSessionEvictsOldest(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.snaps = snapshot.New(f.driver, snapshot.Options{MaxPerSession: 3})
	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "x"})

	var lastID string
	for i := 0; i < 6; i++ {
		e, err := f.snaps.Capture(ctx, "s", "snap")
		if err != nil {
			t.Fatalf("Capture #%d: %v", i, err)
		}
		lastID = e.ID
		time.Sleep(2 * time.Millisecond) // 保证 ID（纳秒时间戳）互不相同
	}

	list, err := f.snaps.List(ctx, "s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("snapshots = %d, want 3 (cap not enforced): %+v", len(list), list)
	}
	if list[0].ID != lastID {
		t.Fatalf("newest snapshot was evicted: %+v", list)
	}
}

// TestSnapshotsDoNotLeakIntoSession is the isolation invariant: snapshot
// bookkeeping must never appear as conversation messages.
func TestSnapshotsDoNotLeakIntoSession(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	_ = f.mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "real message"})
	if _, err := f.snaps.Capture(ctx, "s", "snap"); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	got, err := f.mem.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 1 || got[0].Content != "real message" {
		t.Fatalf("snapshot data leaked into the session: %+v", got)
	}
}

// TestCaptureEmptySession covers snapshotting a session with no messages.
func TestCaptureEmptySession(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	entry, err := f.snaps.Capture(ctx, "empty", "initial")
	if err != nil {
		t.Fatalf("Capture empty: %v", err)
	}
	if entry.MessageCount != 0 {
		t.Fatalf("count = %d, want 0", entry.MessageCount)
	}
	// 回滚到空状态应当清空会话
	_ = f.mem.Add(ctx, "empty", core.Message{Role: core.RoleUser, Content: "later"})
	if _, err := f.snaps.Rollback(ctx, "empty", entry.ID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	got, _ := f.mem.Recent(ctx, "empty", 1<<20)
	if len(got) != 0 {
		t.Fatalf("rollback to empty did not clear session: %+v", got)
	}
}

// TestEncryptedSnapshots proves snapshots honor the codec.
func TestEncryptedSnapshots(t *testing.T) {
	codec, err := memorystore.NewEncryptedCodec([]byte("secret-key"), nil)
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	path := filepath.Join(t.TempDir(), "enc.db")
	d, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	defer d.Close()
	ctx := context.Background()
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	mem := d.Memory(memorystore.Options{Codec: codec})
	snaps := snapshot.New(d, snapshot.Options{Codec: codec})

	_ = mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "TOPSECRET"})
	if _, err := snaps.Capture(ctx, "s", "snap"); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	// 快照行本身必须是密文
	rows, err := d.DB().QueryContext(ctx,
		`SELECT data FROM agent_messages WHERE session_id LIKE 'snapshot:%'`)
	if err != nil {
		t.Fatalf("query snapshots: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if strings.Contains(string(raw), "TOPSECRET") {
			t.Fatal("snapshot stored plaintext")
		}
	}
}
