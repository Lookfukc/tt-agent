package observer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// TraceSpan is a snapshot of a finished or in-flight span.
type TraceSpan struct {
	TraceID    string         `json:"trace_id"`
	SpanID     string         `json:"span_id"`
	ParentID   string         `json:"parent_span_id,omitempty"`
	Name       string         `json:"name"`
	StartedAt  time.Time      `json:"started_at"`
	EndedAt    time.Time      `json:"ended_at"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// Duration returns the elapsed time.
// returns: end time minus start time; time until now if not yet ended
func (s *TraceSpan) Duration() time.Duration {
	end := s.EndedAt
	if end.IsZero() {
		end = time.Now()
	}
	return end.Sub(s.StartedAt)
}

// memorySpan is the tracer-side span implementation.
type memorySpan struct {
	tracer *MemoryTracer
	data   TraceSpan
	mu     sync.Mutex
	ended  bool
}

// SetAttr attaches an attribute.
//
// Calls after End are no-ops: the span has already been handed to readers
// via Spans(), and further writes would be a data race against iteration.
func (s *memorySpan) SetAttr(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	if s.data.Attributes == nil {
		s.data.Attributes = make(map[string]any)
	}
	s.data.Attributes[key] = value
}

// RecordError records the error text.
func (s *memorySpan) RecordError(err error) {
	if err == nil {
		return
	}
	s.SetAttr("error", err.Error())
}

// End finalizes and commits the span.
func (s *memorySpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	s.data.EndedAt = time.Now()
	s.tracer.append(&s.data)
}

// defaultMaxSpans is the default span retention limit.
const defaultMaxSpans = 10000

// MemoryTracer is an in-memory tracer that exports the span tree for
// testing and debugging.
//
// Spans are retained with a cap, in end order: a long-lived process
// appending indefinitely would eat through memory, so once the cap is
// reached the oldest span (the earliest ended) is evicted; when a full
// trace is needed, switch to a disk-backed tracer implementation.
type MemoryTracer struct {
	mu    sync.Mutex
	spans []*TraceSpan
	limit int
}

// NewMemoryTracer constructs a tracer retaining the most recent 10000
// spans by default.
// returns: a usable tracer instance
func NewMemoryTracer() *MemoryTracer {
	return &MemoryTracer{limit: defaultMaxSpans}
}

// NewMemoryTracerWithLimit constructs a tracer with a specified span
// retention limit.
// n: the span limit; <=0 degrades to the default limit
// returns: a usable tracer instance
func NewMemoryTracerWithLimit(n int) *MemoryTracer {
	if n <= 0 {
		n = defaultMaxSpans
	}
	return &MemoryTracer{limit: n}
}

// StartSpan implements core.Tracer.
func (t *MemoryTracer) StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, core.Span) {
	parent := core.SpanFromContext(ctx)
	data := TraceSpan{
		TraceID:   newID(16),
		SpanID:    newID(8),
		Name:      name,
		StartedAt: time.Now(),
	}
	// When the parent span was produced by this tracer, inherit the
	// TraceID and establish the parent-child link
	if p, ok := parent.(*memorySpan); ok {
		data.TraceID = p.data.TraceID
		data.ParentID = p.data.SpanID
	}
	for i := 0; i+1 < len(attrs); i += 2 {
		if data.Attributes == nil {
			data.Attributes = make(map[string]any)
		}
		if k, ok := attrs[i].(string); ok {
			data.Attributes[k] = attrs[i+1]
		}
	}
	span := &memorySpan{tracer: t, data: data}
	return core.WithSpan(ctx, span), span
}

// Spans exports a copy of all finished spans, in end order.
//
// Attributes are deep-copied as well: a shallow copy would share the map,
// and any concurrent write during a reader's iteration would be a data
// race.
// returns: the list of spans
func (t *MemoryTracer) Spans() []TraceSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]TraceSpan, 0, len(t.spans))
	for _, s := range t.spans {
		copied := *s
		if s.Attributes != nil {
			copied.Attributes = make(map[string]any, len(s.Attributes))
			for k, v := range s.Attributes {
				copied.Attributes[k] = v
			}
		}
		out = append(out, copied)
	}
	return out
}

// TraceCount counts by TraceID.
// returns: the number of distinct traces
func (t *MemoryTracer) TraceCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	seen := make(map[string]bool)
	for _, s := range t.spans {
		seen[s.TraceID] = true
	}
	return len(seen)
}

// append appends a finished span, evicting the oldest when at capacity.
func (t *MemoryTracer) append(s *TraceSpan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.spans) >= t.limit {
		// Evict the earliest-ended span: after shifting the head off,
		// append in place; the underlying array is reallocated only when
		// its capacity is exhausted, amortized O(1)
		t.spans = append(t.spans[1:], s)
		return
	}
	t.spans = append(t.spans, s)
}

// newID generates a random hex identifier.
// bytes: the number of bytes
// returns: the hex string
func newID(bytes int) string {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		// If the random source fails, degrade to a timestamp so that
		// uniqueness remains usable
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
