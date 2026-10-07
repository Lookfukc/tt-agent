package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// captureLLM 记录收到的请求并按脚本回复，可附带用量事件
type captureLLM struct {
	mu    sync.Mutex
	reqs  []core.ChatRequest
	turns []core.Message
	usage core.Usage
}

// Chat 未使用，循环统一走流式
func (c *captureLLM) Chat(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("not implemented")
}

// ChatStream 记录请求后以事件流返回本轮预设消息
func (c *captureLLM) ChatStream(_ context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	c.mu.Lock()
	c.reqs = append(c.reqs, req)
	idx := len(c.reqs) - 1
	usage := c.usage
	c.mu.Unlock()

	msg := c.turns[min(idx, len(c.turns)-1)]
	events := make(chan core.StreamEvent, 6)
	go func() {
		defer close(events)
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: msg.Content}
		for i, tc := range msg.ToolCalls {
			events <- core.StreamEvent{Type: core.StreamDeltaToolCall, ToolCallDelta: core.ToolCallDelta{
				Index: i, ID: tc.ID, Name: tc.Name, ArgsPart: tc.Arguments,
			}}
		}
		if usage.InputTokens != 0 || usage.OutputTokens != 0 || usage.ReasoningTokens != 0 {
			events <- core.StreamEvent{Type: core.StreamUsage, Usage: usage}
		}
		events <- core.StreamEvent{Type: core.StreamDone, FinishReason: core.FinishStop}
	}()
	return events, nil
}

// firstReq 首个记录到的请求
// returns: 请求副本
func (c *captureLLM) firstReq() core.ChatRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reqs[0]
}

// scriptedMemory 可编程失败的记忆：Add 命中 failRole 时失败，
// Recent 在 recentErr 非空时失败，其余行为对齐 Buffer
type scriptedMemory struct {
	inner     *memory.Buffer
	failRole  core.Role
	recentErr error
}

// Add 命中失败角色即报错，否则透传内层
func (s *scriptedMemory) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	for _, m := range msgs {
		if m.Role == s.failRole {
			return errors.New("scripted memory failure")
		}
	}
	return s.inner.Add(ctx, sessionID, msgs...)
}

// Recent 按脚本报错或透传内层
func (s *scriptedMemory) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	if s.recentErr != nil {
		return nil, s.recentErr
	}
	return s.inner.Recent(ctx, sessionID, budget)
}

// Clear 透传内层
func (s *scriptedMemory) Clear(ctx context.Context, sessionID string) error {
	return s.inner.Clear(ctx, sessionID)
}

// TestR2H5OutgoingRequestSanitized 悬空 tool_calls 与孤儿 tool 消息只在请求侧修补
//
// 记忆预置 [assistant(1 个调用无结果), 孤儿 tool, user]，
// 模型收到的请求必须含该调用的合成结果且不含孤儿；
// 记忆本身保持真相，不落任何修补产物
func TestR2H5OutgoingRequestSanitized(t *testing.T) {
	ctx := context.Background()
	buf := memory.NewBuffer(nil)
	preload := []core.Message{
		{Role: core.RoleUser, Content: "旧问题"},
		{Role: core.RoleAssistant, Content: "", ToolCalls: []core.ToolCall{
			{ID: "dangling", Name: "echo", Arguments: `{}`},
		}},
		{Role: core.RoleTool, ToolCallID: "orphan", Content: "无主结果"},
		{Role: core.RoleUser, Content: "新问题"},
	}
	if err := buf.Add(ctx, "s1", preload...); err != nil {
		t.Fatalf("Add: %v", err)
	}

	llm := &captureLLM{turns: []core.Message{{Role: core.RoleAssistant, Content: "done"}}}
	loop := agent.NewLoop(llm, nil, buf, agent.Config{Model: "m", MaxIterations: 2, TokenBudget: 100_000})
	if _, _, err := loop.Run(ctx, "s1", "继续"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	req := llm.firstReq()
	synthAt, orphanSeen := -1, false
	callerAt := -1
	for i, m := range req.Messages {
		switch {
		case m.Role == core.RoleAssistant && len(m.ToolCalls) > 0 && m.ToolCalls[0].ID == "dangling":
			callerAt = i
		case m.Role == core.RoleTool && m.ToolCallID == "dangling":
			synthAt = i
			if m.Content != "tool error: interrupted before execution" {
				t.Errorf("H5: synthesized content = %q", m.Content)
			}
		case m.Role == core.RoleTool && m.ToolCallID == "orphan":
			orphanSeen = true
		}
	}
	if synthAt == -1 {
		t.Fatal("H5: dangling tool call sent to provider without result — API would 400")
	}
	if callerAt == -1 || synthAt != callerAt+1 {
		t.Fatalf("H5: synthesized result not right after caller group, callerAt=%d synthAt=%d", callerAt, synthAt)
	}
	if orphanSeen {
		t.Error("H5: orphan tool result leaked into provider request — API would 400")
	}

	// 记忆未被污染：预置消息原样保留，无合成结果混入
	hist, _ := buf.Recent(ctx, "s1", 100_000)
	if len(hist) != len(preload)+2 { // +user("继续") +assistant("done")
		t.Fatalf("H5: memory mutated by sanitize, len = %d, want %d", len(hist), len(preload)+2)
	}
	for _, m := range hist {
		if m.Content == "tool error: interrupted before execution" {
			t.Error("H5: synthesized result persisted into memory")
		}
	}
}

// TestN9UsageKeptOnToolResultAddFailure 工具结果落盘失败时本轮用量不得丢弃
func TestN9UsageKeptOnToolResultAddFailure(t *testing.T) {
	ctx := context.Background()
	mem := &scriptedMemory{inner: memory.NewBuffer(nil), failRole: core.RoleTool}
	llm := &captureLLM{
		turns: []core.Message{
			{Role: core.RoleAssistant, FinishReason: core.FinishToolCalls, ToolCalls: []core.ToolCall{
				{ID: "t1", Name: "echo", Arguments: `{}`},
			}},
		},
		usage: core.Usage{InputTokens: 42, OutputTokens: 7},
	}

	loop := agent.NewLoop(llm, nil, mem, agent.Config{Model: "m", MaxIterations: 2})
	_, total, err := loop.Run(ctx, "s1", "go")
	if err == nil {
		t.Fatal("N9: want error when tool-result Add fails")
	}
	if total.InputTokens != 42 || total.OutputTokens != 7 {
		t.Fatalf("N9: round usage lost on tool-result Add failure, got %+v", total)
	}
}

// TestN13EventErrorOnMemoryFailures 记忆层三条失败路径都必须发 EventError
func TestN13EventErrorOnMemoryFailures(t *testing.T) {
	ctx := context.Background()
	plainTurn := []core.Message{{Role: core.RoleAssistant, Content: "ok"}}
	toolTurn := []core.Message{
		{Role: core.RoleAssistant, FinishReason: core.FinishToolCalls, ToolCalls: []core.ToolCall{
			{ID: "t1", Name: "echo", Arguments: `{}`},
		}},
	}

	// runAndCollect 跑一轮并收集 EventError 事件；OnEvent 契约允许
	// 多 goroutine 回调，收集端按契约加锁
	runAndCollect := func(mem core.Memory, llm core.LLM) ([]agent.LoopEvent, error) {
		var mu sync.Mutex
		var errs []agent.LoopEvent
		cfg := agent.Config{
			Model: "m", MaxIterations: 2,
			OnEvent: func(e agent.LoopEvent) {
				if e.Type == agent.EventError {
					mu.Lock()
					errs = append(errs, e)
					mu.Unlock()
				}
			},
		}
		_, _, runErr := agent.NewLoop(llm, nil, mem, cfg).Run(ctx, "s1", "go")
		mu.Lock()
		defer mu.Unlock()
		return errs, runErr
	}

	t.Run("RecentFail", func(t *testing.T) {
		mem := &scriptedMemory{inner: memory.NewBuffer(nil), recentErr: errors.New("recent boom")}
		errs, runErr := runAndCollect(mem, &captureLLM{turns: plainTurn})
		if runErr == nil {
			t.Fatal("N13: want Run error when Recent fails")
		}
		if len(errs) == 0 {
			t.Fatal("N13: Recent failure emitted no EventError")
		}
		if errs[0].Err == nil || errs[0].Err.Error() != "recent boom" || errs[0].SessionID != "s1" {
			t.Errorf("N13: bad error event %+v", errs[0])
		}
	})
	t.Run("AddAssistantFail", func(t *testing.T) {
		mem := &scriptedMemory{inner: memory.NewBuffer(nil), failRole: core.RoleAssistant}
		errs, runErr := runAndCollect(mem, &captureLLM{turns: plainTurn})
		if runErr == nil {
			t.Fatal("N13: want Run error when Add(assistant) fails")
		}
		if len(errs) == 0 {
			t.Fatal("N13: Add(assistant) failure emitted no EventError")
		}
	})
	t.Run("AddToolFail", func(t *testing.T) {
		mem := &scriptedMemory{inner: memory.NewBuffer(nil), failRole: core.RoleTool}
		errs, runErr := runAndCollect(mem, &captureLLM{turns: toolTurn})
		if runErr == nil {
			t.Fatal("N13: want Run error when Add(tool) fails")
		}
		if len(errs) == 0 {
			t.Fatal("N13: Add(tool) failure emitted no EventError")
		}
	})
}

// TestL_E7MemoryTracerSpanCap 超过上限后 Spans() 只保留最近有限个 span
func TestL_E7MemoryTracerSpanCap(t *testing.T) {
	tr := observer.NewMemoryTracerWithLimit(3)
	for i := 0; i < 10; i++ {
		_, span := tr.StartSpan(context.Background(), fmt.Sprintf("span-%d", i))
		span.End()
	}
	spans := tr.Spans()
	if len(spans) != 3 {
		t.Fatalf("L-E7: spans len = %d, want 3 (capped)", len(spans))
	}
	// 淘汰从最旧（最早结束）开始，保留最后 3 个
	for i, want := range []string{"span-7", "span-8", "span-9"} {
		if spans[i].Name != want {
			t.Errorf("L-E7: spans[%d] = %s, want %s (oldest must be evicted)", i, spans[i].Name, want)
		}
	}

	// 默认构造器同样有界：超过默认上限后长度封顶
	def := observer.NewMemoryTracer()
	for i := 0; i < 10005; i++ {
		_, span := def.StartSpan(context.Background(), "s")
		span.End()
	}
	if got := len(def.Spans()); got != 10000 {
		t.Fatalf("L-E7: default tracer spans len = %d, want 10000", got)
	}
}
