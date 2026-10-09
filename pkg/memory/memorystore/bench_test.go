// Cross-backend benchmarks.
//
// Every benchmark is hermetic (miniredis, temp files) except the
// Postgres variants, which skip without TEST_POSTGRES_DSN. Numbers are
// meant for relative comparison between backends and shapes, not as
// absolute truths; see README for the measured table.
package memorystore_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/postgres"
	ttredis "github.com/Lookfukc/tt-agent/pkg/memory/redis"
	"github.com/Lookfukc/tt-agent/pkg/memory/sqlite"
)

// benchMsg builds a message of realistic chat size (~60 chars).
func benchMsg(i int) core.Message {
	return core.Message{Role: core.RoleUser, Content: fmt.Sprintf("bench message %06d aaaaaaaaaa bbbbbbbbbb cccccccccc", i)}
}

// newFileStore builds the JSONL-backed implementation.
func newFileStore(b *testing.B) core.Memory {
	b.Helper()
	mem, err := memory.NewPersistentWithLRU(b.TempDir(), nil, 1024)
	if err != nil {
		b.Fatalf("NewPersistent: %v", err)
	}
	return mem
}

// newSQLiteStore builds a migrated SQLite store.
func newSQLiteStore(b *testing.B) *memorystore.Store {
	b.Helper()
	d, err := sqlite.New(filepath.Join(b.TempDir(), "bench.db"), sqlite.Options{})
	if err != nil {
		b.Fatalf("sqlite.New: %v", err)
	}
	b.Cleanup(func() { _ = d.Close() })
	if err := d.Migrate(context.Background()); err != nil {
		b.Fatalf("Migrate: %v", err)
	}
	return d.Memory(memorystore.Options{})
}

// newRedisStore builds a store over an in-process Redis.
func newRedisStore(b *testing.B) *memorystore.Store {
	b.Helper()
	srv := miniredis.RunT(b)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	d := ttredis.New(client, ttredis.Options{})
	b.Cleanup(func() { _ = d.Close() })
	return d.Memory(memorystore.Options{})
}

// fillSession appends n messages once, outside the timed loop.
func fillSession(b *testing.B, mem core.Memory, sessionID string, n int) {
	b.Helper()
	const batch = 500
	for i := 0; i < n; i += batch {
		end := min(i+batch, n)
		msgs := make([]core.Message, 0, end-i)
		for j := i; j < end; j++ {
			msgs = append(msgs, benchMsg(j))
		}
		if err := mem.Add(context.Background(), sessionID, msgs...); err != nil {
			b.Fatalf("fill Add: %v", err)
		}
	}
}

// ---------- Add throughput ----------

func BenchmarkFileAdd(b *testing.B)   { benchAdd(b, newFileStore(b)) }
func BenchmarkSQLiteAdd(b *testing.B) { benchAdd(b, newSQLiteStore(b)) }
func BenchmarkRedisAdd(b *testing.B)  { benchAdd(b, newRedisStore(b)) }

// benchAdd measures one-message appends: the agent loop's real write
// pattern (user turn, assistant turn, tool results arrive separately).
func benchAdd(b *testing.B, mem core.Memory) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mem.Add(ctx, "s", benchMsg(i)); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}
}

// ---------- Recent at various session lengths ----------

func BenchmarkFileRecent1000(b *testing.B)   { benchRecent(b, newFileStore(b), 1000) }
func BenchmarkSQLiteRecent100(b *testing.B)  { benchRecent(b, newSQLiteStore(b), 100) }
func BenchmarkSQLiteRecent1000(b *testing.B) { benchRecent(b, newSQLiteStore(b), 1000) }
func BenchmarkSQLiteRecent5000(b *testing.B) { benchRecent(b, newSQLiteStore(b), 5000) }
func BenchmarkRedisRecent100(b *testing.B)   { benchRecent(b, newRedisStore(b), 100) }
func BenchmarkRedisRecent1000(b *testing.B)  { benchRecent(b, newRedisStore(b), 1000) }
func BenchmarkRedisRecent5000(b *testing.B)  { benchRecent(b, newRedisStore(b), 5000) }

// benchRecent measures a full-window read: every request assembly
// before truncation looks like this, so it bounds per-turn overhead.
//
// The 5000 shape deliberately exceeds the default maxScan of 2000:
// the difference between 1000 and 5000 shows exactly what the cap
// costs (and saves).
func benchRecent(b *testing.B, mem core.Memory, size int) {
	b.Helper()
	ctx := context.Background()
	fillSession(b, mem, "s", size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mem.Recent(ctx, "s", 1<<30); err != nil {
			b.Fatalf("Recent: %v", err)
		}
	}
}

// ---------- Trim ----------

func BenchmarkSQLiteTrim(b *testing.B) { benchTrim(b, newSQLiteStore(b)) }
func BenchmarkRedisTrim(b *testing.B)  { benchTrim(b, newRedisStore(b)) }

// benchTrim measures removing 10 messages from a 1000-message session,
// re-trimming the same window each time so the measurement is stable.
func benchTrim(b *testing.B, mem core.Memory) {
	b.Helper()
	ctx := context.Background()
	fillSession(b, mem, "s", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%50 == 0 { // 每轮补回 500 条，维持可修剪的存量
			b.StopTimer()
			fillSession(b, mem, "s", 500)
			b.StartTimer()
		}
		if err := mem.(interface {
			Trim(ctx context.Context, sessionID string, n int) error
		}).Trim(ctx, "s", 10); err != nil {
			b.Fatalf("Trim: %v", err)
		}
	}
}

// ---------- Codec overhead ----------

// BenchmarkPlainCodecEncode is the encryption baseline.
func BenchmarkPlainCodecEncode(b *testing.B) { benchEncode(b, memorystore.JSONCodec{}) }

// BenchmarkEncryptedCodecEncode measures what AES-256-GCM adds on the
// write path. If this dominates Add, encryption is mispriced; if it is
// noise next to storage I/O (expected), it is free safety.
func BenchmarkEncryptedCodecEncode(b *testing.B) {
	b.Helper()
	codec, err := memorystore.NewEncryptedCodec([]byte("bench-key"), nil)
	if err != nil {
		b.Fatalf("codec: %v", err)
	}
	benchEncode(b, codec)
}

func benchEncode(b *testing.B, codec memorystore.Codec) {
	b.Helper()
	rec := memorystore.MessageRecord{
		TS:  time.Now(),
		Msg: benchMsg(0),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.EncodeMessage(rec); err != nil {
			b.Fatalf("EncodeMessage: %v", err)
		}
	}
}

// BenchmarkPlainCodecDecode is the decryption baseline.
func BenchmarkPlainCodecDecode(b *testing.B) {
	rec := memorystore.MessageRecord{TS: time.Now(), Msg: benchMsg(0)}
	data, err := memorystore.JSONCodec{}.EncodeMessage(rec)
	if err != nil {
		b.Fatalf("encode: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := (memorystore.JSONCodec{}).DecodeMessage(data); err != nil {
			b.Fatalf("DecodeMessage: %v", err)
		}
	}
}

// BenchmarkEncryptedCodecDecode measures the read path with encryption.
func BenchmarkEncryptedCodecDecode(b *testing.B) {
	codec, err := memorystore.NewEncryptedCodec([]byte("bench-key"), nil)
	if err != nil {
		b.Fatalf("codec: %v", err)
	}
	data, err := codec.EncodeMessage(memorystore.MessageRecord{TS: time.Now(), Msg: benchMsg(0)})
	if err != nil {
		b.Fatalf("encode: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.DecodeMessage(data); err != nil {
			b.Fatalf("DecodeMessage: %v", err)
		}
	}
}

// ---------- Postgres (needs a server) ----------

// newPostgresStore connects to TEST_POSTGRES_DSN or skips the
// benchmark. Each run gets a throwaway schema so reruns are clean.
func newPostgresStore(b *testing.B) *memorystore.Store {
	b.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		b.Skip("set TEST_POSTGRES_DSN to run postgres benchmarks")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("tt_bench_%d", time.Now().UnixNano()%1e9)
	d, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: schema})
	if err != nil {
		b.Fatalf("NewFromURL: %v", err)
	}
	b.Cleanup(func() {
		_, _ = d.Pool().Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
		_ = d.Close()
	})
	if _, err := d.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`); err != nil {
		b.Fatalf("schema: %v", err)
	}
	if err := d.Migrate(ctx); err != nil {
		b.Fatalf("Migrate: %v", err)
	}
	return d.Memory(memorystore.Options{})
}

func BenchmarkPostgresAdd(b *testing.B)        { benchAdd(b, newPostgresStore(b)) }
func BenchmarkPostgresRecent1000(b *testing.B) { benchRecent(b, newPostgresStore(b), 1000) }
func BenchmarkPostgresTrim(b *testing.B)       { benchTrim(b, newPostgresStore(b)) }
