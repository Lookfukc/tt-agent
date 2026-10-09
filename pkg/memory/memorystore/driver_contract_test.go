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

	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/postgres"
	"github.com/Lookfukc/tt-agent/pkg/memory/redis"
	"github.com/Lookfukc/tt-agent/pkg/memory/sqlite"
)

// TestRedisDriverContract runs the shared scenario suite against Redis.
func TestRedisDriverContract(t *testing.T) {
	runContract(t, memoryHarness{
		name: "redis",
		newStore: func(t *testing.T) (*memorystore.Store, func()) {
			srv := miniredis.RunT(t)
			client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
			d := redis.New(client, redis.Options{})
			return d.Memory(memorystore.Options{}), func() { _ = d.Close() }
		},
	})
}

// TestSQLiteDriverContract runs the shared scenario suite against SQLite.
func TestSQLiteDriverContract(t *testing.T) {
	runContract(t, memoryHarness{
		name: "sqlite",
		newStore: func(t *testing.T) (*memorystore.Store, func()) {
			d, err := sqlite.New(filepath.Join(t.TempDir(), "contract.db"), sqlite.Options{})
			if err != nil {
				t.Fatalf("sqlite.New: %v", err)
			}
			if err := d.Migrate(t.Context()); err != nil {
				t.Fatalf("Migrate: %v", err)
			}
			return d.Memory(memorystore.Options{}), func() { _ = d.Close() }
		},
	})
}

// TestPostgresDriverContract runs the shared scenario suite against a
// real PostgreSQL. It needs a server, so it skips unless
// TEST_POSTGRES_DSN points at one; CI should always set it so the
// parity guarantee covers all four backends.
func TestPostgresDriverContract(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run the postgres contract suite")
	}
	runContract(t, memoryHarness{
		name: "postgres",
		newStore: func(t *testing.T) (*memorystore.Store, func()) {
			ctx := context.Background()
			schema := fmt.Sprintf("tt_contract_%d", time.Now().UnixNano()%1e9)
			d, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: schema})
			if err != nil {
				t.Fatalf("NewFromURL: %v", err)
			}
			if _, err := d.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`); err != nil {
				t.Fatalf("create schema: %v", err)
			}
			if err := d.Migrate(ctx); err != nil {
				t.Fatalf("Migrate: %v", err)
			}
			cleanup := func() {
				_, _ = d.Pool().Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
				_ = d.Close()
			}
			return d.Memory(memorystore.Options{}), cleanup
		},
	})
}

// TestEncryptedCodecContract runs the suite with encryption enabled, so
// every driver is proven to work through a codec that rewrites bytes.
func TestEncryptedCodecContract(t *testing.T) {
	runContract(t, memoryHarness{
		name: "sqlite-encrypted",
		newStore: func(t *testing.T) (*memorystore.Store, func()) {
			codec, err := memorystore.NewEncryptedCodec([]byte("contract-key"), nil)
			if err != nil {
				t.Fatalf("codec: %v", err)
			}
			d, err := sqlite.New(filepath.Join(t.TempDir(), "enc.db"), sqlite.Options{})
			if err != nil {
				t.Fatalf("sqlite.New: %v", err)
			}
			if err := d.Migrate(t.Context()); err != nil {
				t.Fatalf("Migrate: %v", err)
			}
			return d.Memory(memorystore.Options{Codec: codec}), func() { _ = d.Close() }
		},
	})
}

// TestCodecRoundTripIsStable proves encode/decode is lossless for every
// field a message can carry, since drivers store only bytes.
func TestCodecRoundTripIsStable(t *testing.T) {
	codec := memorystore.JSONCodec{}
	original := memorystore.MessageRecord{
		TS:  time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Msg: coreMessageWithEverything(),
	}
	data, err := codec.EncodeMessage(original)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	decoded, err := codec.DecodeMessage(data)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if decoded.Msg.Role != original.Msg.Role ||
		decoded.Msg.Content != original.Msg.Content ||
		decoded.Msg.ToolCallID != original.Msg.ToolCallID ||
		decoded.Msg.Reasoning != original.Msg.Reasoning ||
		len(decoded.Msg.ToolCalls) != len(original.Msg.ToolCalls) ||
		len(decoded.Msg.ContentParts) != len(original.Msg.ContentParts) {
		t.Fatalf("round trip lost data:\n got %+v\nwant %+v", decoded.Msg, original.Msg)
	}
	if !decoded.TS.Equal(original.TS) {
		t.Fatalf("timestamp lost: got %v want %v", decoded.TS, original.TS)
	}
}

// TestLegacyBareMessageDecodes proves old JSONL-format payloads still
// load through the shared codec, so a dataset can move from the file
// backend to a database without rewriting.
func TestLegacyBareMessageDecodes(t *testing.T) {
	codec := memorystore.JSONCodec{}
	legacy := []byte(`{"role":"user","content":"legacy line"}`)
	rec, err := codec.DecodeMessage(legacy)
	if err != nil {
		t.Fatalf("DecodeMessage legacy: %v", err)
	}
	if rec.Msg.Role != "user" || rec.Msg.Content != "legacy line" {
		t.Fatalf("legacy decode = %+v", rec.Msg)
	}
	if !rec.TS.IsZero() {
		t.Fatalf("legacy record should have zero timestamp, got %v", rec.TS)
	}
}

// TestCorruptRecordRejected proves garbage is reported, not accepted.
func TestCorruptRecordRejected(t *testing.T) {
	codec := memorystore.JSONCodec{}
	if _, err := codec.DecodeMessage([]byte(`{"nonsense":true}`)); err == nil {
		t.Fatal("expected error for a record with no role")
	}
}

// TestValidSessionID covers the shared validation rule.
func TestValidSessionID(t *testing.T) {
	valid := []string{"s1", "user-123", "a.b", "session_42", "UPPER"}
	for _, id := range valid {
		if !memorystore.ValidSessionID(id) {
			t.Errorf("ValidSessionID(%q) = false, want true", id)
		}
	}
	invalid := []string{"", "../x", "a/b", `a\b`, "..", "x..y", string(make([]byte, 200))}
	for _, id := range invalid {
		if memorystore.ValidSessionID(id) {
			t.Errorf("ValidSessionID(%q) = true, want false", id)
		}
	}
}
