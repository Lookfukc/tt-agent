package ltm

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore is an in-process Store with optional vector search.
//
// Facts live in a map keyed by namespace, so recall is O(facts in the
// namespace) — plenty for the tens-to-thousands of facts a single
// user or tenant accumulates. Swap in an external Store for larger
// namespaces or multi-process sharing.
//
// A MemoryStore is safe for concurrent use.
type MemoryStore struct {
	mu       sync.RWMutex
	ns       map[string]map[string]*entry
	maxFacts int
	now      func() time.Time
	seq      uint64 // 单调递增的写入序号
}

// entry is a stored fact plus its vector and insertion order.
type entry struct {
	fact Fact
	// seq is a monotonic insertion counter, used as the tie-breaker
	// for eviction and ordering.
	//
	// Timestamps alone are not enough: facts written in the same
	// nanosecond (a tight loop, or a coarse clock) compare equal, and
	// the resulting eviction order would be arbitrary — which is how
	// the newest fact can get dropped while older ones survive.
	seq    uint64
	vector []float32
}

// MemoryOptions configures the in-process store.
type MemoryOptions struct {
	// MaxFactsPerNamespace caps retained facts, evicting the least
	// recently updated when exceeded. 0 means the default.
	//
	// A cap exists because long-term memory is the one store that
	// grows forever by design: without a bound, a chatty user turns
	// into unbounded heap.
	MaxFactsPerNamespace int
}

// defaultMaxFacts bounds one namespace.
const defaultMaxFacts = 1000

// NewMemoryStore builds an in-process store.
func NewMemoryStore(opts MemoryOptions) *MemoryStore {
	limit := opts.MaxFactsPerNamespace
	if limit <= 0 {
		limit = defaultMaxFacts
	}
	return &MemoryStore{
		ns:       make(map[string]map[string]*entry),
		maxFacts: limit,
		now:      time.Now,
	}
}

// Upsert implements Store.
func (s *MemoryStore) Upsert(_ context.Context, fact Fact, vector []float32) (Fact, error) {
	if fact.Namespace == "" {
		return Fact{}, ErrEmptyNamespace
	}
	if fact.ID == "" {
		fact.ID = hashID(fact.Namespace + "\x00" + normalize(fact.Text))
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	bucket, ok := s.ns[fact.Namespace]
	if !ok {
		bucket = make(map[string]*entry)
		s.ns[fact.Namespace] = bucket
	}
	if prev, exists := bucket[fact.ID]; exists {
		fact.CreatedAt = prev.fact.CreatedAt
	} else {
		fact.CreatedAt = now
	}
	fact.UpdatedAt = now
	s.seq++
	bucket[fact.ID] = &entry{fact: fact, vector: cloneVector(vector), seq: s.seq}
	s.evictLocked(bucket)
	return fact, nil
}

// Search implements Store.
//
// Ranking prefers vector similarity when both the query vector and a
// stored vector exist; otherwise it falls back to keyword overlap.
// Mixing vector and keyword scores for different facts within one
// result set is avoided by picking a single strategy per call, so
// scores stay comparable.
func (s *MemoryStore) Search(_ context.Context, namespace, query string, vector []float32, limit int) ([]Fact, error) {
	if namespace == "" {
		return nil, ErrEmptyNamespace
	}
	if limit <= 0 {
		limit = defaultRecallLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucket := s.ns[namespace]
	if len(bucket) == 0 {
		return nil, nil
	}

	useVector := len(vector) > 0 && s.hasVectorsLocked(bucket)
	items := make([]ranked, 0, len(bucket))
	for _, e := range bucket {
		var score float64
		if useVector {
			score = scoreCosine(vector, e.vector)
			if score <= 0 {
				// A zero similarity means the fact is unrelated;
				// keeping it would dilute the prompt with noise.
				continue
			}
		} else {
			score = scoreKeyword(query, e.fact.Text)
			if score <= 0 {
				continue
			}
		}
		fact := e.fact
		fact.Score = score
		items = append(items, ranked{fact: fact, score: score, seq: e.seq})
	}
	sortByScore(items)
	if len(items) > limit {
		items = items[:limit]
	}
	return toFacts(items), nil
}

// List implements Store, newest first.
func (s *MemoryStore) List(_ context.Context, namespace string, limit int) ([]Fact, error) {
	if namespace == "" {
		return nil, ErrEmptyNamespace
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucket := s.ns[namespace]
	entries := make([]*entry, 0, len(bucket))
	for _, e := range bucket {
		entries = append(entries, e)
	}
	// 按写入序号倒序，避免同纳秒写入时排序不稳定
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].seq > entries[j].seq
	})
	out := make([]Fact, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.fact)
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Delete implements Store.
func (s *MemoryStore) Delete(_ context.Context, namespace, id string) error {
	if namespace == "" {
		return ErrEmptyNamespace
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if bucket, ok := s.ns[namespace]; ok {
		delete(bucket, id)
		if len(bucket) == 0 {
			delete(s.ns, namespace)
		}
	}
	return nil
}

// Clear implements Store.
func (s *MemoryStore) Clear(_ context.Context, namespace string) error {
	if namespace == "" {
		return ErrEmptyNamespace
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ns, namespace)
	return nil
}

// Count reports how many facts a namespace holds.
func (s *MemoryStore) Count(namespace string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ns[namespace])
}

// hasVectorsLocked reports whether any fact in the bucket has a vector.
//
// Called with the read lock held.
func (s *MemoryStore) hasVectorsLocked(bucket map[string]*entry) bool {
	for _, e := range bucket {
		if len(e.vector) > 0 {
			return true
		}
	}
	return false
}

// evictLocked drops the oldest facts beyond the cap.
//
// Ordering uses the insertion sequence, not the timestamp: two facts
// written in the same nanosecond would otherwise compare equal and the
// eviction choice would be arbitrary.
//
// Called with the write lock held.
func (s *MemoryStore) evictLocked(bucket map[string]*entry) {
	limit := s.maxFacts
	if limit <= 0 || len(bucket) <= limit {
		return
	}
	victims := make([]*entry, 0, len(bucket))
	for _, e := range bucket {
		victims = append(victims, e)
	}
	sort.Slice(victims, func(i, j int) bool {
		return victims[i].seq < victims[j].seq
	})
	for i := 0; i < len(victims)-limit; i++ {
		delete(bucket, victims[i].fact.ID)
	}
}

// toFacts unwraps ranked items into facts.
func toFacts(items []ranked) []Fact {
	out := make([]Fact, 0, len(items))
	for _, it := range items {
		out = append(out, it.fact)
	}
	return out
}

// cloneVector copies a vector so callers cannot mutate stored state.
func cloneVector(v []float32) []float32 {
	if len(v) == 0 {
		return nil
	}
	out := make([]float32, len(v))
	copy(out, v)
	return out
}

// compile-time proof that the in-process store satisfies Store.
var _ Store = (*MemoryStore)(nil)
