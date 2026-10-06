package observer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// TraceSpan 已结束或进行中的 span 快照
type TraceSpan struct {
	TraceID    string         `json:"trace_id"`
	SpanID     string         `json:"span_id"`
	ParentID   string         `json:"parent_span_id,omitempty"`
	Name       string         `json:"name"`
	StartedAt  time.Time      `json:"started_at"`
	EndedAt    time.Time      `json:"ended_at"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// Duration 耗时
// returns: 结束时间减开始时间，未结束时为至今
func (s *TraceSpan) Duration() time.Duration {
	end := s.EndedAt
	if end.IsZero() {
		end = time.Now()
	}
	return end.Sub(s.StartedAt)
}

// memorySpan 追踪器侧的 span 实现
type memorySpan struct {
	tracer *MemoryTracer
	data   TraceSpan
	mu     sync.Mutex
	ended  bool
}

// SetAttr 附加属性
//
// End 之后的调用为空操作：span 已交给 Spans() 的读者，
// 再写入会与遍历构成数据竞态
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

// RecordError 记录错误文本
func (s *memorySpan) RecordError(err error) {
	if err == nil {
		return
	}
	s.SetAttr("error", err.Error())
}

// End 落账并结束
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

// defaultMaxSpans 默认 span 保留上限
const defaultMaxSpans = 10000

// MemoryTracer 进程内内存追踪器，导出 span 树用于测试与调试
//
// span 按结束顺序有上限地保留：长生命周期进程持续追加会吃穿内存，
// 达到上限后淘汰最旧 span（即最早结束的），需要完整轨迹时改用
// 落盘的追踪器实现
type MemoryTracer struct {
	mu    sync.Mutex
	spans []*TraceSpan
	limit int
}

// NewMemoryTracer 构造追踪器，默认保留最近 10000 个 span
// returns: 可用的追踪器实例
func NewMemoryTracer() *MemoryTracer {
	return &MemoryTracer{limit: defaultMaxSpans}
}

// NewMemoryTracerWithLimit 构造指定 span 保留上限的追踪器
// n: span 上限，<=0 时退化为默认上限
// returns: 可用的追踪器实例
func NewMemoryTracerWithLimit(n int) *MemoryTracer {
	if n <= 0 {
		n = defaultMaxSpans
	}
	return &MemoryTracer{limit: n}
}

// StartSpan 实现 core.Tracer
func (t *MemoryTracer) StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, core.Span) {
	parent := core.SpanFromContext(ctx)
	data := TraceSpan{
		TraceID:   newID(16),
		SpanID:    newID(8),
		Name:      name,
		StartedAt: time.Now(),
	}
	// 父 span 是本追踪器产物时继承 TraceID 并建立父子
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

// Spans 导出全部已结束 span 的副本，按结束顺序
//
// Attributes 一并深拷贝：浅拷贝共享 map，读者遍历期间
// 任何并发写入都是数据竞态
// returns: span 列表
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

// TraceCount 按 TraceID 计数
// returns: 独立链路数
func (t *MemoryTracer) TraceCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	seen := make(map[string]bool)
	for _, s := range t.spans {
		seen[s.TraceID] = true
	}
	return len(seen)
}

// append 追加已结束 span，满容量时淘汰最旧
func (t *MemoryTracer) append(s *TraceSpan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.spans) >= t.limit {
		// 淘汰最早结束的 span：头部前移后原地追加，
		// 底层数组容量耗尽时才整体搬迁，摊还 O(1)
		t.spans = append(t.spans[1:], s)
		return
	}
	t.spans = append(t.spans, s)
}

// newID 生成随机 hex 标识
// bytes: 字节数
// returns: hex 字符串
func newID(bytes int) string {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		// 随机源失败退化为时间戳，保证唯一性可用
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
