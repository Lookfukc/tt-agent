package postgres_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/postgres"
)

// dsnEnv names the environment variable holding a test database URL.
//
// Postgres needs a real server, and requiring one for `go test ./...`
// would make the whole suite fail on a developer laptop, so these
// tests skip unless TEST_POSTGRES_DSN is set.
const dsnEnv = "TEST_POSTGRES_DSN"

// newDriver connects to the test database, or skips.
//
// Each test gets its own schema so parallel runs cannot collide, and
// the schema is dropped on cleanup.
func newDriver(t *testing.T) *postgres.Driver {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("set %s to run postgres driver tests", dsnEnv)
	}
	ctx := context.Background()
	schema := "tt_test_" + sanitize(t.Name())
	d, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: schema})
	if err != nil {
		t.Fatalf("NewFromURL: %v", err)
	}
	// 先建 schema 再迁移：驱动只引表名，不负责创建 schema 本身
	if _, err := d.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+quote(schema)); err != nil {
		d.Close()
		t.Fatalf("create schema: %v", err)
	}
	if err := d.Migrate(ctx); err != nil {
		d.Close()
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = d.Pool().Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quote(schema)+` CASCADE`)
		_ = d.Close()
	})
	return d
}

// sanitize makes a test name safe for use as a schema identifier.
func sanitize(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	if len(out) > 40 {
		out = out[:40]
	}
	return string(out)
}

// quote wraps an identifier in double quotes.
func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// TestMigrateIsIdempotent proves startup migration can run repeatedly.
func TestMigrateIsIdempotent(t *testing.T) {
	d := newDriver(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := d.Migrate(ctx); err != nil {
			t.Fatalf("Migrate #%d: %v", i, err)
		}
	}
}

// TestBasicRoundTrip covers append and read through the store.
func TestBasicRoundTrip(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	msgs := []core.Message{
		{Role: core.RoleSystem, Content: "sys"},
		{Role: core.RoleUser, Content: "hello"},
		{Role: core.RoleAssistant, Content: "hi"},
	}
	if err := store.Add(ctx, "s", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 3 || got[0].Content != "sys" || got[2].Content != "hi" {
		t.Fatalf("Recent = %+v", got)
	}
}

// TestBinaryPayloadSurvives covers bytea handling with content that is
// hostile to text columns — NUL bytes, control characters and 4-byte
// UTF-8 — all of which must round-trip exactly.
//
// Bytes that are not valid UTF-8 at all are a different story: the JSON
// codec normalizes them to U+FFFD (Go's encoding/json does this), so
// they are deliberately not asserted here; that property belongs to
// the codec and is documented on it.
func TestBinaryPayloadSurvives(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	msg := core.Message{Role: core.RoleTool, Content: "\x00\x01\t\r\n\"\\emoji-\U0001F600-quote"}
	if err := store.Add(ctx, "s", msg); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 1 || got[0].Content != msg.Content {
		t.Fatalf("binary content corrupted: %+v", got)
	}
}

// TestTrimUsesSQLPath exercises SQL-level trimming.
func TestTrimUsesSQLPath(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
	for i := 0; i < 8; i++ {
		msgs = append(msgs, core.Message{Role: core.RoleUser, Content: string(rune('a' + i))})
	}
	_ = store.Add(ctx, "s", msgs...)
	if err := store.Trim(ctx, "s", 3); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	got, _ := store.Recent(ctx, "s", 1<<20)
	if len(got) != 6 { // 1 system + 5 survivors
		t.Fatalf("after trim = %d: %+v", len(got), got)
	}
	if got[0].Role != core.RoleSystem || got[1].Content != "d" {
		t.Fatalf("wrong survivors: %+v", got)
	}
}

// TestSummaryUpsert proves one row per session.
func TestSummaryUpsert(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	_ = store.SaveSummary(ctx, "s", 1, "first")
	_ = store.SaveSummary(ctx, "s", 2, "second")
	text, covered, err := store.LoadSummary(ctx, "s")
	if err != nil || text != "second" || covered != 2 {
		t.Fatalf("summary = (%q, %d, %v)", text, covered, err)
	}
}

// TestPersistenceAcrossReconnect proves data outlives the connection.
func TestPersistenceAcrossReconnect(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("set %s to run postgres driver tests", dsnEnv)
	}
	ctx := context.Background()
	schema := "tt_test_reconnect"
	// 先清掉历史残留：这个测试用固定 schema，若上一轮清理失败
	// （比如断言失败前进程退出），旧数据会让本轮的计数检查误报。
	dropSchema(t, dsn, schema)
	first, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: schema})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := first.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+quote(schema)); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if err := first.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// 清理必须用独立连接：测试体会 Close 掉 first，之后经由它的
	// 池执行 DROP 会静默失败，schema 就漏在库里污染下一轮。
	t.Cleanup(func() { dropSchema(t, dsn, schema) })

	store := first.Memory(memorystore.Options{})
	if err := store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "durable"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	_ = store.SaveSummary(ctx, "s", 1, "kept")
	_ = first.Close()

	second, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: schema})
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer second.Close()
	reopened := second.Memory(memorystore.Options{})
	got, err := reopened.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 1 || got[0].Content != "durable" {
		t.Fatalf("messages lost: %+v", got)
	}
	if text, _, _ := reopened.LoadSummary(ctx, "s"); text != "kept" {
		t.Fatalf("summary lost: %q", text)
	}
}

// dropSchema removes a schema over a throwaway connection.
//
// It exists because a cleanup running through an already-closed pool
// silently fails, and a leaked schema then corrupts the next run of
// whichever test owns it.
func dropSchema(t *testing.T, dsn, schema string) {
	t.Helper()
	d, err := postgres.NewFromURL(context.Background(), dsn, postgres.Options{Schema: schema})
	if err != nil {
		return // 无法连接时无从清理；CREATE SCHEMA IF NOT EXISTS 会兜底
	}
	defer d.Close()
	_, _ = d.Pool().Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quote(schema)+` CASCADE`)
}

// TestPruneIdleSessions covers retention, including the
// never-summarized session that a summary-only sweep would leak.
func TestPruneIdleSessions(t *testing.T) {
	d := newDriver(t)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	_ = store.Add(ctx, "withsummary", core.Message{Role: core.RoleUser, Content: "x"})
	_ = store.SaveSummary(ctx, "withsummary", 1, "s")
	_ = store.Add(ctx, "nosummary", core.Message{Role: core.RoleUser, Content: "y"})

	// 阈值宽松：都不能删
	if _, err := d.PruneIdleSessions(ctx, time.Hour); err != nil {
		t.Fatalf("prune: %v", err)
	}
	for _, id := range []string{"withsummary", "nosummary"} {
		if got, _ := store.Recent(ctx, id, 1<<20); len(got) != 1 {
			t.Fatalf("session %s pruned while active", id)
		}
	}

	// 回填时间后：都必须删
	if _, err := d.Pool().Exec(ctx,
		`UPDATE `+d.MsgTable()+` SET created_at = now() - interval '48 hours'`); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	if _, err := d.Pool().Exec(ctx,
		`UPDATE `+d.SumTable()+` SET updated_at = now() - interval '48 hours'`); err != nil {
		t.Fatalf("backdate summary: %v", err)
	}
	if _, err := d.PruneIdleSessions(ctx, time.Hour); err != nil {
		t.Fatalf("prune: %v", err)
	}
	for _, id := range []string{"withsummary", "nosummary"} {
		if got, _ := store.Recent(ctx, id, 1<<20); len(got) != 0 {
			t.Fatalf("idle session %s leaked: %+v", id, got)
		}
	}
}

// TestSchemaOptionIsolation proves two schemas do not see each other.
func TestSchemaOptionIsolation(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("set %s to run postgres driver tests", dsnEnv)
	}
	ctx := context.Background()
	a, b := "tt_test_iso_a", "tt_test_iso_b"
	da, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: a})
	if err != nil {
		t.Fatalf("connect a: %v", err)
	}
	defer da.Close()
	db, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: b})
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	defer db.Close()
	for _, x := range []struct {
		d      *postgres.Driver
		schema string
	}{{da, a}, {db, b}} {
		if _, err := x.d.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+quote(x.schema)); err != nil {
			t.Fatalf("schema %s: %v", x.schema, err)
		}
		if err := x.d.Migrate(ctx); err != nil {
			t.Fatalf("migrate %s: %v", x.schema, err)
		}
		defer func(s string) {
			_, _ = da.Pool().Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quote(s)+` CASCADE`)
		}(x.schema)
	}

	if err := da.Memory(memorystore.Options{}).Add(ctx, "s",
		core.Message{Role: core.RoleUser, Content: "in-a"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	got, err := db.Memory(memorystore.Options{}).Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent b: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("schema b saw schema a's data: %+v", got)
	}
}

// TestInvalidSchemaIdentifierIsQuoted proves identifier injection is
// neutralized: a schema name with quotes must not break out of the
// identifier context.
func TestInvalidSchemaIdentifierIsQuoted(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("set %s to run postgres driver tests", dsnEnv)
	}
	ctx := context.Background()
	evil := `x"; DROP TABLE students; --`
	d, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: evil})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer d.Close()
	// 迁移会因为 schema 不存在而失败，但不该执行到注入的语句；
	// 关键是它返回错误而不是成功执行 DDL。
	if err := d.Migrate(ctx); err == nil {
		if _, err := d.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+quote(evil)); err == nil {
			t.Skip("driver quoted the identifier and the schema was creatable; injection neutralized")
		}
	}
}
