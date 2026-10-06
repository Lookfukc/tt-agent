package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/core"
	"github.com/Lookfukc/send-agent/pkg/memory"
	"github.com/Lookfukc/send-agent/pkg/tools"
)

// scriptedLLM 按脚本逐轮返回预设消息
type scriptedLLM struct {
	calls int
	turns []core.Message
}

// Chat 未使用，循环统一走流式
func (s *scriptedLLM) Chat(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("not implemented")
}

// ChatStream 以异步事件流返回本轮预设消息，对齐真实适配器的生产者契约
func (s *scriptedLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	msg := s.turns[min(s.calls, len(s.turns)-1)]
	s.calls++
	events := make(chan core.StreamEvent, 4)
	go func() {
		defer close(events)
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: msg.Content}
		for i, tc := range msg.ToolCalls {
			events <- core.StreamEvent{Type: core.StreamDeltaToolCall, ToolCallDelta: core.ToolCallDelta{
				Index: i, ID: tc.ID, Name: tc.Name, ArgsPart: tc.Arguments,
			}}
		}
		events <- core.StreamEvent{Type: core.StreamDone, FinishReason: core.FinishStop}
	}()
	return events, nil
}

// stubTool 记录调用的测试工具
type stubTool struct{ executed []string }

// loopConfig 测试用循环配置，熔断上限 3 轮
func loopConfig() agent.Config {
	return agent.Config{Model: "m", MaxIterations: 3}
}

// Name 工具名
func (s *stubTool) Name() string { return "echo" }

// Description 工具描述
func (s *stubTool) Description() string { return "echo" }

// Parameters 参数 schema
func (s *stubTool) Parameters() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }

// Execute 记录参数并回显
func (s *stubTool) Execute(_ context.Context, args json.RawMessage) (core.ToolResult, error) {
	s.executed = append(s.executed, string(args))
	return core.ToolResult{Text: "echoed"}, nil
}

func TestLoopToolCallRoundTrip(t *testing.T) {
	llm := &scriptedLLM{turns: []core.Message{
		{
			Role: core.RoleAssistant, Content: "", FinishReason: core.FinishToolCalls,
			ToolCalls: []core.ToolCall{{ID: "t1", Name: "echo", Arguments: `{"text":"hi"}`}},
		},
		{Role: core.RoleAssistant, Content: "完成"},
	}}
	tool := &stubTool{}
	reg := tools.NewRegistry()
	reg.Register(tool)
	mem := memory.NewBuffer(nil)

	loop := agent.NewLoop(llm, reg, mem, agent.Config{Model: "m", MaxIterations: 4})
	msg, _, err := loop.Run(context.Background(), "s1", "调用工具")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if msg.Content != "完成" {
		t.Errorf("final = %q", msg.Content)
	}
	if len(tool.executed) != 1 || tool.executed[0] != `{"text":"hi"}` {
		t.Errorf("tool executed = %v", tool.executed)
	}
	if llm.calls != 2 {
		t.Errorf("llm calls = %d, want 2", llm.calls)
	}

	history, _ := mem.Recent(context.Background(), "s1", 100_000)
	// user → assistant(tool) → tool → assistant
	if len(history) != 4 {
		t.Fatalf("history len = %d, want 4", len(history))
	}
	if history[1].ToolCalls[0].ID != "t1" || history[2].Role != core.RoleTool || history[2].ToolCallID != "t1" {
		t.Errorf("history = %+v", history)
	}
}

func TestLoopMaxIterations(t *testing.T) {
	// 每轮都要求工具调用，验证熔断
	always := core.Message{
		Role: core.RoleAssistant, FinishReason: core.FinishToolCalls,
		ToolCalls: []core.ToolCall{{ID: "t1", Name: "echo", Arguments: `{}`}},
	}
	llm := &scriptedLLM{turns: []core.Message{always}}
	reg := tools.NewRegistry()
	reg.Register(&stubTool{})

	loop := agent.NewLoop(llm, reg, memory.NewBuffer(nil), loopConfig())
	_, _, err := loop.Run(context.Background(), "s1", "go")
	if !errors.Is(err, agent.ErrMaxIterations) {
		t.Fatalf("err = %v, want ErrMaxIterations", err)
	}
	if llm.calls != 3 {
		t.Errorf("calls = %d, want 3", llm.calls)
	}
}

func TestLoopToolFailureFeedsModel(t *testing.T) {
	llm := &scriptedLLM{turns: []core.Message{
		{
			Role: core.RoleAssistant, FinishReason: core.FinishToolCalls,
			ToolCalls: []core.ToolCall{{ID: "t1", Name: "missing_tool", Arguments: `{}`}},
		},
		{Role: core.RoleAssistant, Content: "工具不存在，改用直接回答"},
	}}
	loop := agent.NewLoop(llm, tools.NewRegistry(), memory.NewBuffer(nil), loopConfig())

	msg, _, err := loop.Run(context.Background(), "s1", "go")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if msg.Content != "工具不存在，改用直接回答" {
		t.Errorf("final = %q", msg.Content)
	}
}
