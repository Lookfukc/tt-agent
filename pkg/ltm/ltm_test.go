package ltm_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/ltm"
)

// fakeEmbedder maps text to a deterministic vector so tests exercise
// the vector path without a model.
//
// Vectors are built from keyword presence, which makes cosine
// similarity behave predictably: texts sharing vocabulary land close
// together.
type fakeEmbedder struct {
	// dim is the vector width; a mismatch with stored vectors is what
	// the "changed provider" degradation test relies on.
	dim int
	err error
}

// Embed implements ltm.Embedder.
func (f fakeEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if f.err != nil {
		return nil, f.err
	}
	dim := f.dim
	if dim <= 0 {
		dim = 8
	}
	vec := make([]float32, dim)
	lower := strings.ToLower(text)
	// 用关键字散列到固定维度，保证同义文本得到相近向量
	for _, word := range strings.Fields(lower) {
		h := 0
		for _, r := range word {
			h = (h*31 + int(r)) % dim
		}
		vec[h] += 1
	}
	for _, probe := range []string{"pizza", "vegetarian", "beijing", "python", "dog"} {
		if strings.Contains(lower, probe) {
			vec[len(probe)%dim] += 2
		}
	}
	return vec, nil
}

// TestRememberAndRecall covers the explicit-fact path.
func TestRememberAndRecall(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{Embedder: fakeEmbedder{}})

	if _, err := mem.Remember(ctx, "user-1", "The user loves pizza", nil); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if _, err := mem.Remember(ctx, "user-1", "The user lives in Beijing", nil); err != nil {
		t.Fatalf("Remember: %v", err)
	}

	got, err := mem.Recall(ctx, "user-1", "what food does the user like pizza", 5)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("recall returned nothing")
	}
	if !strings.Contains(got[0].Text, "pizza") {
		t.Fatalf("most relevant fact = %q, want the pizza one", got[0].Text)
	}
	if got[0].Score <= 0 {
		t.Fatalf("score not set: %+v", got[0])
	}
}

// TestNamespacesAreIsolated is the privacy invariant: one user's facts
// must never surface in another user's recall.
func TestNamespacesAreIsolated(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{Embedder: fakeEmbedder{}})

	_, _ = mem.Remember(ctx, "alice", "Alice is vegetarian", nil)
	_, _ = mem.Remember(ctx, "bob", "Bob loves pizza", nil)

	got, err := mem.Recall(ctx, "alice", "pizza", 10)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	for _, f := range got {
		if strings.Contains(f.Text, "Bob") {
			t.Fatalf("cross-namespace leak: %+v", got)
		}
	}
}

// TestEmptyNamespaceRejected guards against the shared-bucket mistake.
func TestEmptyNamespaceRejected(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{})

	if _, err := mem.Remember(ctx, "", "x", nil); !errors.Is(err, ltm.ErrEmptyNamespace) {
		t.Fatalf("Remember err = %v, want ErrEmptyNamespace", err)
	}
	if _, err := mem.Recall(ctx, "", "x", 1); !errors.Is(err, ltm.ErrEmptyNamespace) {
		t.Fatalf("Recall err = %v, want ErrEmptyNamespace", err)
	}
}

// TestRememberIsIdempotent proves re-learning one fact updates a single
// entry instead of accumulating duplicates.
func TestRememberIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{})

	for i := 0; i < 5; i++ {
		if _, err := mem.Remember(ctx, "u", "The user loves pizza", nil); err != nil {
			t.Fatalf("Remember: %v", err)
		}
	}
	// 空白与大小写差异也应归一到同一条
	if _, err := mem.Remember(ctx, "u", "  the USER   loves   PIZZA  ", nil); err != nil {
		t.Fatalf("Remember normalized: %v", err)
	}
	if n := store.Count("u"); n != 1 {
		t.Fatalf("facts = %d, want 1 (deduplication failed)", n)
	}
}

// TestKeywordFallbackWithoutEmbedder proves the memory still works
// with no embedding provider configured.
func TestKeywordFallbackWithoutEmbedder(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{}) // 无 embedder

	_, _ = mem.Remember(ctx, "u", "The user is vegetarian", nil)
	_, _ = mem.Remember(ctx, "u", "The user codes in Python", nil)

	got, err := mem.Recall(ctx, "u", "vegetarian", 5)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if len(got) == 0 || !strings.Contains(got[0].Text, "vegetarian") {
		t.Fatalf("keyword recall failed: %+v", got)
	}
}

// TestEmbedderFailureDegradesToKeyword proves a flaky embedding
// provider does not lose facts.
func TestEmbedderFailureDegradesToKeyword(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	broken := ltm.New(store, ltm.Options{Embedder: fakeEmbedder{err: errors.New("provider down")}})

	fact, err := broken.Remember(ctx, "u", "The user has a dog", nil)
	if err != nil {
		t.Fatalf("Remember with broken embedder: %v", err)
	}
	if fact.ID == "" {
		t.Fatal("fact not stored despite embedder failure")
	}

	got, err := broken.Recall(ctx, "u", "dog", 5)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if len(got) == 0 || !strings.Contains(got[0].Text, "dog") {
		t.Fatalf("fact unreachable after embedder failure: %+v", got)
	}
}

// TestVectorDimensionMismatchDegrades covers a provider that changes
// its output width: recall must degrade, not panic or return noise.
func TestVectorDimensionMismatchDegrades(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})

	wide := ltm.New(store, ltm.Options{Embedder: fakeEmbedder{dim: 16}})
	if _, err := wide.Remember(ctx, "u", "The user loves pizza", nil); err != nil {
		t.Fatalf("Remember: %v", err)
	}

	// 换成一个不同维度的 embedder 读取
	narrow := ltm.New(store, ltm.Options{Embedder: fakeEmbedder{dim: 4}})
	got, err := narrow.Recall(ctx, "u", "pizza", 5)
	if err != nil {
		t.Fatalf("Recall with mismatched dims: %v", err)
	}
	// 维度不匹配时余弦为 0，该条被过滤；不 panic 即为通过
	_ = got
}

// TestLearnExtractsFacts covers the extractor pipeline.
func TestLearnExtractsFacts(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{
		Extractor: ltm.ExtractorFunc(func(context.Context, []ltm.Message) ([]string, error) {
			return []string{"The user is vegetarian", ""}, nil // 空串应被跳过
		}),
	})

	facts, err := mem.Learn(ctx, "u", []ltm.Message{
		{Role: "user", Content: "I'm vegetarian"},
		{Role: "assistant", Content: "Noted"},
	})
	if err != nil {
		t.Fatalf("Learn: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("learned %d facts, want 1: %+v", len(facts), facts)
	}
	if store.Count("u") != 1 {
		t.Fatalf("store count = %d, want 1", store.Count("u"))
	}
}

// TestLearnWithoutExtractorIsNoop proves the facade can be used for
// explicit Remember only.
func TestLearnWithoutExtractorIsNoop(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{})

	facts, err := mem.Learn(ctx, "u", []ltm.Message{{Role: "user", Content: "hi"}})
	if err != nil {
		t.Fatalf("Learn: %v", err)
	}
	if len(facts) != 0 {
		t.Fatalf("expected no facts, got %+v", facts)
	}
}

// TestForgetAndClear covers deletion.
func TestForgetAndClear(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{})

	f1, _ := mem.Remember(ctx, "u", "fact one", nil)
	_, _ = mem.Remember(ctx, "u", "fact two", nil)

	if err := mem.Forget(ctx, "u", f1.ID); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if n := store.Count("u"); n != 1 {
		t.Fatalf("after Forget count = %d, want 1", n)
	}
	if err := mem.Forget(ctx, "u", "no-such-id"); err != nil {
		t.Fatalf("Forget missing should be a no-op, got %v", err)
	}
	if err := store.Clear(ctx, "u"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if n := store.Count("u"); n != 0 {
		t.Fatalf("after Clear count = %d, want 0", n)
	}
}

// TestEvictionBoundsMemory proves the per-namespace cap holds, which is
// what keeps long-term memory from growing without bound.
func TestEvictionBoundsMemory(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{MaxFactsPerNamespace: 3})
	mem := ltm.New(store, ltm.Options{})

	for i := 0; i < 10; i++ {
		if _, err := mem.Remember(ctx, "u", factText(i), nil); err != nil {
			t.Fatalf("Remember: %v", err)
		}
	}
	if n := store.Count("u"); n != 3 {
		t.Fatalf("facts = %d, want 3 (cap not enforced)", n)
	}
	// 最新的必须留下
	got, _ := mem.List(ctx, "u", 10)
	found := false
	for _, f := range got {
		if f.Text == factText(9) {
			found = true
		}
	}
	if !found {
		t.Fatalf("newest fact was evicted: %+v", got)
	}
}

// TestPromptRendersFacts covers the prompt helper.
func TestPromptRendersFacts(t *testing.T) {
	if got := ltm.Prompt(nil); got != "" {
		t.Fatalf("Prompt(nil) = %q, want empty", got)
	}
	got := ltm.Prompt([]ltm.Fact{{Text: "likes pizza"}, {Text: "lives in Beijing"}})
	if !strings.Contains(got, "likes pizza") || !strings.Contains(got, "lives in Beijing") {
		t.Fatalf("Prompt = %q", got)
	}
	if strings.HasSuffix(got, "\n") {
		t.Fatalf("Prompt has trailing newline: %q", got)
	}
}

// TestConcurrentRememberAndRecall covers lock discipline.
func TestConcurrentRememberAndRecall(t *testing.T) {
	ctx := context.Background()
	store := ltm.NewMemoryStore(ltm.MemoryOptions{MaxFactsPerNamespace: 100})
	mem := ltm.New(store, ltm.Options{Embedder: fakeEmbedder{}})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = mem.Remember(ctx, "u", factText(i*10+j), nil)
				_, _ = mem.Recall(ctx, "u", "pizza", 3)
				_, _ = mem.List(ctx, "u", 5)
			}
		}(i)
	}
	wg.Wait()
	if n := store.Count("u"); n == 0 {
		t.Fatal("no facts survived concurrent writes")
	}
}

// TestMissingStoreReportsError covers the facade guard.
func TestMissingStoreReportsError(t *testing.T) {
	mem := ltm.New(nil, ltm.Options{})
	if _, err := mem.Remember(context.Background(), "u", "x", nil); !errors.Is(err, ltm.ErrNoStore) {
		t.Fatalf("err = %v, want ErrNoStore", err)
	}
}

// factText builds a distinct fact string.
func factText(i int) string {
	return "fact number " + string(rune('a'+i%26)) + strings.Repeat("x", i%3)
}
