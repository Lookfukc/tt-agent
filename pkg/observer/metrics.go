// Package observer provides aggregation and querying of framework
// runtime metrics.
package observer

import (
	"sync"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// Stats holds aggregated metrics per provider.
type Stats struct {
	Calls           int64 `json:"calls"`
	Errors          int64 `json:"errors"`
	InputTokens     int64 `json:"input_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

// Metrics is an in-process metrics aggregator, safe for concurrent use.
type Metrics struct {
	mu    sync.Mutex
	stats map[string]*Stats
}

// NewMetrics constructs the aggregator.
// returns: a usable aggregator instance
func NewMetrics() *Metrics {
	return &Metrics{stats: make(map[string]*Stats)}
}

// Record records one call result.
// providerID: the provider identifier
// usage: the cumulative usage for this call; may be zero-valued
// err: non-nil counts as a failure
func (m *Metrics) Record(providerID string, usage core.Usage, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.stats[providerID]
	if !ok {
		s = &Stats{}
		m.stats[providerID] = s
	}
	s.Calls++
	if err != nil {
		s.Errors++
	}
	s.InputTokens += usage.InputTokens
	s.OutputTokens += usage.OutputTokens
	s.ReasoningTokens += usage.ReasoningTokens
}

// Snapshot exports a copy of the current metrics.
// returns: the mapping from provider to metrics
func (m *Metrics) Snapshot() map[string]Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Stats, len(m.stats))
	for k, v := range m.stats {
		out[k] = *v
	}
	return out
}
