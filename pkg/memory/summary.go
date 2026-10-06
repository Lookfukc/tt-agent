package memory

import (
	"context"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// summaryCacheEntry 会话级摘要缓存
type summaryCacheEntry struct {
	// dropped 已压缩的旧消息条数，数量变化才重新压缩
	dropped int
	// text 压缩产物
	text string
}

// Summary 摘要压缩记忆
//
// 包装内层 Memory：预算截断会丢弃的旧消息不再直接扔掉，
// 而是用 LLM 压缩成一段摘要作为上下文前缀。摘要不落盘，
// 由丢弃数量驱动缓存，同一截断点重复询问不重复压缩
type Summary struct {
	inner   core.Memory
	llm     core.LLM
	counter core.TokenCounter
	mu      sync.Mutex
	cache   map[string]*summaryCacheEntry
}

// NewSummary 构造摘要记忆
//
// 预算估算默认沿用内置粗估而非内层 Memory 的精确计数器：
// core.Memory 接口不暴露计数器，摘要层拿不到内层实例的口径；
// 粗估偏保守（宁少勿超），截断点只会更早不会超窗
// inner: 实际存储，Buffer 或 Persistent
// llm: 用于压缩的模型，建议用廉价小模型
// returns: 可用的记忆实例
func NewSummary(inner core.Memory, llm core.LLM) *Summary {
	return NewSummaryWithCounter(inner, llm, nil)
}

// NewSummaryWithCounter 构造注入 token 估算器的摘要记忆
//
// 内外层估算口径必须一致时使用（如内层 Persistent 配了精确
// 分词计数器），预算装填才会与内层截断对齐
// inner: 实际存储，Buffer 或 Persistent
// llm: 用于压缩的模型，建议用廉价小模型
// c: token 估算器，nil 退化为内置粗估
// returns: 可用的记忆实例
func NewSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary {
	if c == nil {
		c = roughCounter{}
	}
	return &Summary{
		inner:   inner,
		llm:     llm,
		counter: c,
		cache:   make(map[string]*summaryCacheEntry),
	}
}

// Add 透传内层存储
func (s *Summary) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	return s.inner.Add(ctx, sessionID, msgs...)
}

// Clear 清空会话并作废摘要缓存
func (s *Summary) Clear(ctx context.Context, sessionID string) error {
	s.mu.Lock()
	delete(s.cache, sessionID)
	s.mu.Unlock()
	return s.inner.Clear(ctx, sessionID)
}

// Recent 取回预算内消息，超出部分以摘要替代
func (s *Summary) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	// 取全量才能知道截断丢弃了什么
	all, err := s.inner.Recent(ctx, sessionID, 1<<62)
	if err != nil {
		return nil, err
	}

	kept, dropped := pickWithinBudget(all, budget, s.counter)
	if len(dropped) == 0 {
		return kept, nil
	}

	text := s.summarize(ctx, sessionID, len(dropped), dropped)
	if text == "" {
		return kept, nil
	}
	// 摘要插在系统消息之后、保留历史之前
	out := make([]core.Message, 0, len(kept)+1)
	inserted := false
	for _, m := range kept {
		if !inserted && m.Role != core.RoleSystem {
			out = append(out, core.Message{
				Role:    core.RoleSystem,
				Content: "此前对话摘要：\n" + text,
			})
			inserted = true
		}
		out = append(out, m)
	}
	if !inserted {
		out = append(out, core.Message{Role: core.RoleSystem, Content: "此前对话摘要：\n" + text})
	}
	return out, nil
}

// summarize 压缩被丢弃的消息，命中缓存直接返回
//
// LLM 失败时返回空串退化为纯截断，压缩是优化不是正确性依赖；
// 空结果不写缓存，否则 dropped 数不变期间永远命中失败结果
func (s *Summary) summarize(ctx context.Context, sessionID string, dropped int, msgs []core.Message) string {
	s.mu.Lock()
	if entry, ok := s.cache[sessionID]; ok && entry.dropped == dropped {
		s.mu.Unlock()
		return entry.text
	}
	s.mu.Unlock()

	text := s.compress(ctx, msgs)
	if text == "" {
		return ""
	}

	s.mu.Lock()
	s.cache[sessionID] = &summaryCacheEntry{dropped: dropped, text: text}
	s.mu.Unlock()
	return text
}

// compress 调用 LLM 生成摘要
// returns: 摘要文本，失败时空串
func (s *Summary) compress(ctx context.Context, msgs []core.Message) string {
	// 单条消息截到 2KB，控制压缩请求本身的成本
	var body string
	for i, m := range msgs {
		content := m.Content
		if len(content) > 2048 {
			// 字节截断可能切碎 UTF-8 尾字符，回退到 rune 边界
			cut := 2048
			for cut > 0 && !utf8.RuneStart(content[cut]) {
				cut--
			}
			content = content[:cut] + "..."
		}
		body += fmt.Sprintf("[%s] %s\n", m.Role, content)
		if i >= 200 {
			body += "（更早消息省略）\n"
			break
		}
	}

	resp, err := s.llm.Chat(ctx, core.ChatRequest{
		Model: "summarizer",
		Messages: []core.Message{
			{Role: core.RoleSystem, Content: "把对话压缩成不超过 200 字的要点摘要，保留结论、决定与未完成事项"},
			{Role: core.RoleUser, Content: body},
		},
	})
	if err != nil || resp == nil || resp.Content == "" {
		return ""
	}
	return resp.Content
}
