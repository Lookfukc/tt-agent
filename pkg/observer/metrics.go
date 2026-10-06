// Package observer 提供框架运行指标的聚合与查询能力
package observer

import (
	"sync"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// Stats 单提供商聚合指标
type Stats struct {
	Calls           int64 `json:"calls"`
	Errors          int64 `json:"errors"`
	InputTokens     int64 `json:"input_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

// Metrics 进程内指标聚合，并发安全
type Metrics struct {
	mu    sync.Mutex
	stats map[string]*Stats
}

// NewMetrics 构造聚合器
// returns: 可用的聚合器实例
func NewMetrics() *Metrics {
	return &Metrics{stats: make(map[string]*Stats)}
}

// Record 记录一次调用结果
// providerID: 提供商标识
// usage: 本次累计用量，可为零值
// err: 非 nil 记为失败
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

// Snapshot 导出当前指标副本
// returns: 提供商到指标的映射
func (m *Metrics) Snapshot() map[string]Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Stats, len(m.stats))
	for k, v := range m.stats {
		out[k] = *v
	}
	return out
}
