package ltm_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/ltm"
)

// benchVec builds a deterministic pseudo-embedding: keyword-ish
// hashing into a fixed-width vector, good enough to exercise the
// cosine path realistically.
func benchVec(text string, dim int) []float32 {
	v := make([]float32, dim)
	h := 2166136261
	for _, r := range text {
		h = (h ^ int(r)) * 16777619
		// 哈希乘法会溢出成负数：先归一到 [0, dim) 再做下标
		v[((h%dim)+dim)%dim] += 1
	}
	return v
}

// newFilledStore builds an in-process store holding n facts.
func newFilledStore(b *testing.B, n int) (*ltm.MemoryStore, *ltm.Memory) {
	b.Helper()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{MaxFactsPerNamespace: n + 1})
	mem := ltm.New(store, ltm.Options{
		Embedder: ltm.EmbedderFunc(func(_ context.Context, text string) ([]float32, error) {
			return benchVec(text, 64), nil
		}),
	})
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if _, err := mem.Remember(ctx, "u", fmt.Sprintf("fact %05d: the user prefers item %d over item %d", i, i%17, (i+3)%17), nil); err != nil {
			b.Fatalf("Remember: %v", err)
		}
	}
	return store, mem
}

// BenchmarkRemember measures the write path: hash-id dedup lookup,
// embedding and upsert.
func BenchmarkRemember(b *testing.B) {
	_, mem := newFilledStore(b, 100)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mem.Remember(ctx, "u", fmt.Sprintf("bench fact %d", i), nil); err != nil {
			b.Fatalf("Remember: %v", err)
		}
	}
}

// Keyword recall: embedding disabled, ranking by token overlap.
func BenchmarkRecallKeyword100(b *testing.B)  { benchRecallKeyword(b, 100) }
func BenchmarkRecallKeyword1000(b *testing.B) { benchRecallKeyword(b, 1000) }
func BenchmarkRecallKeyword5000(b *testing.B) { benchRecallKeyword(b, 5000) }

func benchRecallKeyword(b *testing.B, n int) {
	store, _ := newFilledStore(b, n)
	// 无 embedder 的门面：Recall 走关键词路径
	mem := ltm.New(store, ltm.Options{})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mem.Recall(ctx, "u", "user prefers item", 5); err != nil {
			b.Fatalf("Recall: %v", err)
		}
	}
}

// Vector recall: cosine similarity over every stored vector.
func BenchmarkRecallVector1000(b *testing.B) { benchRecallVector(b, 1000) }
func BenchmarkRecallVector5000(b *testing.B) { benchRecallVector(b, 5000) }

func benchRecallVector(b *testing.B, n int) {
	store, mem := newFilledStore(b, n)
	_ = store
	ctx := context.Background()
	query := benchVec("user prefers item", 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 直接走 Store.Search 带 query 向量，绕过门面的 embed
		if _, err := mem.Store().Search(ctx, "u", "user prefers item", query, 5); err != nil {
			b.Fatalf("Search: %v", err)
		}
	}
}
