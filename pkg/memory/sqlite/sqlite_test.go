package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/sqlite"
)

// newDriver opens a migrated database in a temp dir.
func newDriver(t *testing.T) *sqlite.Driver {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.db")
	d, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return d
}

// TestMigrateIsIdempotent proves startup can call Migrate repeatedly.
func TestMigrateIsIdempotent(t *testing.T) {
	d := newDriver(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := d.Migrate(ctx); err != nil {
			t.Fatalf("Migrate #%d: %v", i, err)
		}
	}
}

// TestBasicRoundTrip covers append and read.
func TestBasicRoundTrip(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	msgs := []core.Message{
		{Role: core.RoleSystem, Content: "sys"},
		{Role: core.RoleUser, Content: "hello"},
		{Role: core.RoleAssistant, Content: "hi there"},
	}
	if err := store.Add(ctx, "s", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 3 || got[0].Content != "sys" || got[2].Content != "hi there" {
		t.Fatalf("Recent = %+v", got)
	}
}

// TestToolCallPairingSurvivesTruncation is the invariant that keeps
// providers from returning 400.
func TestToolCallPairingSurvivesTruncation(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	big := strings.Repeat("z", 300)
	msgs := []core.Message{
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "c1", Name: "t", Arguments: `{"a":"` + big + `"}`}}},
		{Role: core.RoleTool, ToolCallID: "c1", Content: big},
		{Role: core.RoleUser, Content: "next"},
	}
	if err := store.Add(ctx, "s", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, _ := store.Recent(ctx, "s", 30)
	for _, m := range got {
		if m.Role != core.RoleTool {
			continue
		}
		found := false
		for _, o := range got {
			if o.Role == core.RoleAssistant {
				for _, tc := range o.ToolCalls {
					if tc.ID == m.ToolCallID {
						found = true
					}
				}
			}
		}
		if !found {
			t.Fatalf("orphan tool message: %+v", got)
		}
	}
}

// TestTrimUsesSQLPath exercises the SQL-level delete.
func TestTrimUsesSQLPath(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
	for i := 0; i < 10; i++ {
		msgs = append(msgs, core.Message{Role: core.RoleUser, Content: string(rune('a' + i))})
	}
	_ = store.Add(ctx, "s", msgs...)

	if err := store.Trim(ctx, "s", 4); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	got, _ := store.Recent(ctx, "s", 1<<20)
	if len(got) != 7 { // 1 system + 6 survivors
		t.Fatalf("after trim = %d: %+v", len(got), got)
	}
	if got[0].Role != core.RoleSystem || got[1].Content != "e" {
		t.Fatalf("wrong survivors: %+v", got)
	}
}

// TestSummaryUpsertOverwrites proves repeated compaction updates one row.
func TestSummaryUpsertOverwrites(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	_ = store.SaveSummary(ctx, "s", 1, "first")
	_ = store.SaveSummary(ctx, "s", 2, "second")
	text, covered, err := store.LoadSummary(ctx, "s")
	if err != nil || text != "second" || covered != 2 {
		t.Fatalf("summary = (%q, %d, %v)", text, covered, err)
	}
	// 只有一行
	var rows int
	if err := d.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM agent_summaries WHERE session_id = ?`, "s").Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 1 {
		t.Fatalf("summary rows = %d, want 1", rows)
	}
}

// TestPersistenceAcrossReopen is the whole point of a disk backend.
func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")
	ctx := context.Background()

	first, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := first.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	store := first.Memory(memorystore.Options{})
	if err := store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "durable"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := store.SaveSummary(ctx, "s", 1, "kept summary"); err != nil {
		t.Fatalf("SaveSummary: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	reopened := second.Memory(memorystore.Options{})
	got, err := reopened.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent after reopen: %v", err)
	}
	if len(got) != 1 || got[0].Content != "durable" {
		t.Fatalf("messages lost across reopen: %+v", got)
	}
	text, covered, _ := reopened.LoadSummary(ctx, "s")
	if text != "kept summary" || covered != 1 {
		t.Fatalf("summary lost across reopen: (%q, %d)", text, covered)
	}
}

// TestPruneIdleSessions covers the retention sweep.
func TestPruneIdleSessions(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	_ = store.Add(ctx, "old", core.Message{Role: core.RoleUser, Content: "x"})
	_ = store.SaveSummary(ctx, "old", 1, "s")

	// 非正阈值表示「不清理」，而不是「全部过期」：这是更安全的默认，
	// 避免配置写错成 0 时清空整个库。
	if n, err := d.PruneIdleSessions(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("prune with 1h idle removed %d (%v), want 0", n, err)
	}
	if n, err := d.PruneIdleSessions(ctx, 0); err != nil || n != 0 {
		t.Fatalf("prune with zero idle removed %d (%v), want 0", n, err)
	}
	if n, err := d.PruneIdleSessions(ctx, -time.Second); err != nil || n != 0 {
		t.Fatalf("prune with negative idle removed %d (%v), want 0", n, err)
	}

	// 极小阈值 + 已过期的数据才会被清：这里把摘要时间改成很久以前
	if _, err := d.DB().ExecContext(ctx,
		`UPDATE agent_summaries SET updated_at = ? WHERE session_id = ?`,
		time.Now().Add(-48*time.Hour).UTC().Format(time.RFC3339Nano), "old"); err != nil {
		t.Fatalf("backdate summary: %v", err)
	}
	if _, err := d.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE session_id = ?`,
		time.Now().Add(-48*time.Hour).UTC().Format(time.RFC3339Nano), "old"); err != nil {
		t.Fatalf("backdate messages: %v", err)
	}
	if _, err := d.PruneIdleSessions(ctx, time.Hour); err != nil {
		t.Fatalf("prune: %v", err)
	}
	got, _ := store.Recent(ctx, "old", 1<<20)
	if len(got) != 0 {
		t.Fatalf("pruned session still has messages: %+v", got)
	}
}

// TestPruneKeepsSessionsWithoutSummary is the regression guard for the
// leak this sweep originally had: sessions that were never summarized
// must be judged by their messages, not skipped entirely.
func TestPruneKeepsSessionsWithoutSummary(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	// 只有消息、没有任何摘要的会话
	_ = store.Add(ctx, "nosummary", core.Message{Role: core.RoleUser, Content: "x"})

	// 阈值宽松时不能被删
	if _, err := d.PruneIdleSessions(ctx, time.Hour); err != nil {
		t.Fatalf("prune: %v", err)
	}
	got, _ := store.Recent(ctx, "nosummary", 1<<20)
	if len(got) != 1 {
		t.Fatalf("active session without summary was pruned: %+v", got)
	}

	// 回填时间后应当被删
	if _, err := d.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE session_id = ?`,
		time.Now().Add(-48*time.Hour).UTC().Format(time.RFC3339Nano), "nosummary"); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	if _, err := d.PruneIdleSessions(ctx, time.Hour); err != nil {
		t.Fatalf("prune: %v", err)
	}
	got, _ = store.Recent(ctx, "nosummary", 1<<20)
	if len(got) != 0 {
		t.Fatalf("idle session without summary leaked: %+v", got)
	}
}

// TestEncryptedAtRest proves ciphertext in the SQLite file.
func TestEncryptedAtRest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "enc.db")
	codec, err := memorystore.NewEncryptedCodec([]byte("k"), nil)
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	d, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.Close()
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	store := d.Memory(memorystore.Options{Codec: codec})
	ctx := context.Background()

	if err := store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "SECRETVALUE"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// 直接读原始 BLOB，不得包含明文
	var raw []byte
	if err := d.DB().QueryRowContext(ctx,
		`SELECT data FROM agent_messages WHERE session_id = ?`, "s").Scan(&raw); err != nil {
		t.Fatalf("raw read: %v", err)
	}
	if strings.Contains(string(raw), "SECRETVALUE") {
		t.Fatal("plaintext stored in database")
	}

	got, _ := store.Recent(ctx, "s", 1<<20)
	if len(got) != 1 || got[0].Content != "SECRETVALUE" {
		t.Fatalf("decrypt failed: %+v", got)
	}
}

// TestConcurrentWritersSharedFile proves two handles on one file can
// both write — the capability JSONL lacks.
func TestConcurrentWritersSharedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	ctx := context.Background()

	a, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open a: %v", err)
	}
	defer a.Close()
	if err := a.Migrate(ctx); err != nil {
		t.Fatalf("migrate a: %v", err)
	}
	b, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open b: %v", err)
	}
	defer b.Close()

	storeA := a.Memory(memorystore.Options{})
	storeB := b.Memory(memorystore.Options{})
	for i := 0; i < 5; i++ {
		if err := storeA.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "from-a"}); err != nil {
			t.Fatalf("A add: %v", err)
		}
		if err := storeB.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "from-b"}); err != nil {
			t.Fatalf("B add: %v", err)
		}
	}
	got, err := storeA.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("shared file has %d messages, want 10", len(got))
	}
}

// TestEmptyPathRejected covers constructor validation.
func TestEmptyPathRejected(t *testing.T) {
	if _, err := sqlite.New("", sqlite.Options{}); err == nil {
		t.Fatal("expected error for empty path")
	}
}
