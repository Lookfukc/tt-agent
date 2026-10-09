package pgvector_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/ltm"
	"github.com/Lookfukc/tt-agent/pkg/ltm/pgvector"
)

const dsnEnv = "TEST_POSTGRES_DSN"

// newStore connects to the test database (skipping when absent),
// creates a throwaway schema, and migrates it.
func newStore(t *testing.T) *pgvector.Store {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("set %s to run pgvector store tests", dsnEnv)
	}
	ctx := context.Background()
	schema := fmt.Sprintf("tt_ltm_%d", time.Now().UnixNano()%1e9)
	s, err := pgvector.NewFromURL(ctx, dsn, pgvector.Options{Dim: 8, Schema: schema})
	if err != nil {
		t.Fatalf("NewFromURL: %v", err)
	}
	if _, err := s.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
		_ = s.Close()
	})
	return s
}

// TestVectorSearchRanksRelevantFirst proves the headline capability:
// cosine similarity over pgvector returns the semantically closest
// fact first.
func TestVectorSearchRanksRelevantFirst(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	// 两个方向不同的向量：pizza 在 0 维激活，berlin 在 1 维
	pizzaVec := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	berlinVec := []float32{0, 1, 0, 0, 0, 0, 0, 0}
	if _, err := s.Upsert(ctx, ltm.Fact{ID: "food", Namespace: "u1", Text: "The user loves pizza"}, pizzaVec); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if _, err := s.Upsert(ctx, ltm.Fact{ID: "home", Namespace: "u1", Text: "The user lives in Berlin"}, berlinVec); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// 用 pizza 方向的查询向量做余弦检索
	facts, err := s.Search(ctx, "u1", "food pizza", pizzaVec, 2)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(facts) == 0 {
		t.Fatal("vector search returned nothing")
	}
	if !strings.Contains(facts[0].Text, "pizza") {
		t.Fatalf("top result = %q, want the pizza fact", facts[0].Text)
	}
	if facts[0].Score <= 0 {
		t.Fatalf("score missing: %+v", facts[0])
	}
	// 余弦 = 1 的自匹配必须排在余弦 = 0 的前面
	if len(facts) > 1 && facts[1].Score >= facts[0].Score {
		t.Fatalf("ranking not descending: %+v", facts)
	}
}

// TestUpsertIsIdempotent proves re-learning updates one row.
func TestUpsertIsIdempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := s.Upsert(ctx, ltm.Fact{ID: "f1", Namespace: "u", Text: "same fact"}, []float32{1, 0, 0, 0, 0, 0, 0, 0}); err != nil {
			t.Fatalf("Upsert #%d: %v", i, err)
		}
	}
	facts, err := s.List(ctx, "u", 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want 1 (dedup failed): %+v", len(facts), facts)
	}
}

// TestDimensionMismatchDegradesToKeyword proves a provider change
// stores facts without vectors instead of failing the write.
func TestDimensionMismatchDegradesToKeyword(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	wrong := make([]float32, 4) // 表列是 vector(8)
	if _, err := s.Upsert(ctx, ltm.Fact{ID: "f", Namespace: "u", Text: "The user has a dog"}, wrong); err != nil {
		t.Fatalf("Upsert with wrong-dim vector: %v", err)
	}

	facts, err := s.Search(ctx, "u", "dog", nil, 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(facts) == 0 || !strings.Contains(facts[0].Text, "dog") {
		t.Fatalf("keyword fallback failed: %+v", facts)
	}
}

// TestNamespacesAreIsolated is the privacy invariant.
func TestNamespacesAreIsolated(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	_, _ = s.Upsert(ctx, ltm.Fact{ID: "a", Namespace: "alice", Text: "Alice is vegetarian"}, nil)
	_, _ = s.Upsert(ctx, ltm.Fact{ID: "b", Namespace: "bob", Text: "Bob loves pizza"}, nil)

	facts, err := s.Search(ctx, "alice", "pizza", nil, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	for _, f := range facts {
		if strings.Contains(f.Text, "Bob") {
			t.Fatalf("cross-namespace leak: %+v", facts)
		}
	}
}

// TestListNewestFirst covers ordering.
func TestListNewestFirst(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	_, _ = s.Upsert(ctx, ltm.Fact{ID: "old", Namespace: "u", Text: "first"}, nil)
	_, _ = s.Upsert(ctx, ltm.Fact{ID: "new", Namespace: "u", Text: "second"}, nil)

	facts, err := s.List(ctx, "u", 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(facts) != 2 || facts[0].ID != "new" {
		t.Fatalf("order wrong: %+v", facts)
	}
}

// TestMetadataRoundTrip proves jsonb metadata survives.
func TestMetadataRoundTrip(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	if _, err := s.Upsert(ctx, ltm.Fact{
		ID: "m", Namespace: "u", Text: "meta fact",
		Metadata: map[string]string{"source": "session-7", "confidence": "high"},
	}, nil); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	facts, err := s.List(ctx, "u", 1)
	if err != nil || len(facts) != 1 {
		t.Fatalf("List: %v %+v", err, facts)
	}
	if facts[0].Metadata["source"] != "session-7" || facts[0].Metadata["confidence"] != "high" {
		t.Fatalf("metadata lost: %+v", facts[0].Metadata)
	}
}

// TestDeleteAndClear covers removal.
func TestDeleteAndClear(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	_, _ = s.Upsert(ctx, ltm.Fact{ID: "keep", Namespace: "u", Text: "a"}, nil)
	_, _ = s.Upsert(ctx, ltm.Fact{ID: "drop", Namespace: "u", Text: "b"}, nil)

	if err := s.Delete(ctx, "u", "drop"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	facts, _ := s.List(ctx, "u", 10)
	if len(facts) != 1 || facts[0].ID != "keep" {
		t.Fatalf("wrong survivor: %+v", facts)
	}

	if err := s.Clear(ctx, "u"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	facts, _ = s.List(ctx, "u", 10)
	if len(facts) != 0 {
		t.Fatalf("Clear left facts: %+v", facts)
	}
}

// TestEmptyNamespaceRejected covers validation.
func TestEmptyNamespaceRejected(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Upsert(ctx, ltm.Fact{ID: "x", Text: "t"}, nil); err == nil {
		t.Fatal("expected error for empty namespace")
	}
	if err := s.Clear(ctx, ""); err == nil {
		t.Fatal("expected error for empty namespace on Clear")
	}
}

// TestNaNVectorDegrades proves non-finite floats do not fail the write.
func TestNaNVectorDegrades(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	nan := []float32{1, 0, 0, 0, 0, 0, 0, float32(math.Inf(1))} // +Inf：pgvector 拒绝非有限值
	if _, err := s.Upsert(ctx, ltm.Fact{ID: "n", Namespace: "u", Text: "nan fact"}, nan); err != nil {
		t.Fatalf("Upsert with non-finite vector: %v", err)
	}
	facts, err := s.List(ctx, "u", 5)
	if err != nil || len(facts) != 1 {
		t.Fatalf("fact with non-finite vector was not stored: %v %+v", err, facts)
	}
}
