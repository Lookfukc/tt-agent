package test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/tools"
	"github.com/Lookfukc/tt-agent/pkg/tools/builtin"
)

// usageScriptedLLM 按轮返回预设消息并附带用量
type usageScriptedLLM struct {
	calls int
	turns []core.Message
	usage []core.Usage
}

// Chat 未使用
func (u *usageScriptedLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream 异步返回本轮消息与用量
func (u *usageScriptedLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	msg := u.turns[min(u.calls, len(u.turns)-1)]
	usage := u.usage[min(u.calls, len(u.usage)-1)]
	u.calls++
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
		events <- core.StreamEvent{Type: core.StreamUsage, Usage: usage}
		events <- core.StreamEvent{Type: core.StreamDone}
	}()
	return events, nil
}

// TestH1UsageAccumulated Run 返回的累计用量必须非零（原 bug：恒为零）
func TestH1UsageAccumulated(t *testing.T) {
	// 两轮：第一轮工具调用，第二轮收敛；每轮各带一份用量
	llm := &usageScriptedLLM{
		turns: []core.Message{
			{
				Role: core.RoleAssistant, FinishReason: core.FinishToolCalls,
				ToolCalls: []core.ToolCall{{ID: "t1", Name: "echo", Arguments: `{}`}},
			},
			{Role: core.RoleAssistant, Content: "完成"},
		},
		usage: []core.Usage{
			{InputTokens: 10, OutputTokens: 5},
			{InputTokens: 20, OutputTokens: 8},
		},
	}
	var doneUsage *core.Usage
	reg := tools.NewRegistry()
	reg.Register(&stubTool{})
	loop := agent.NewLoop(llm, reg, memory.NewBuffer(nil), agent.Config{
		Model: "m", MaxIterations: 4,
		OnEvent: func(e agent.LoopEvent) {
			if e.Type == agent.EventDone && e.Usage != nil {
				doneUsage = e.Usage
			}
		},
	})
	_, usage, err := loop.Run(context.Background(), "h1", "x")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if usage.InputTokens != 30 || usage.OutputTokens != 13 {
		t.Fatalf("H1: accumulated usage = %+v, want {30 13}", usage)
	}
	if doneUsage == nil || doneUsage.InputTokens != 30 {
		t.Fatal("H1: EventDone carries wrong usage")
	}
}

// TestH2CalculatorModuloFloatPanic 原 bug：5 % 0.5 触发整型除零 panic
func TestH2CalculatorModuloFloatPanic(t *testing.T) {
	calc := builtin.NewCalculator()
	// 修复前：int64(0.5)==0 → panic: integer divide by zero
	res, err := calc.Execute(context.Background(), json.RawMessage(`{"expression":"5 % 0.5"}`))
	if err != nil {
		t.Fatalf("5 %% 0.5: %v", err)
	}
	var out struct {
		Value float64 `json:"value"`
	}
	_ = json.Unmarshal([]byte(res.Render()), &out)
	if out.Value != 0 {
		t.Errorf("5 %% 0.5 = %v, want 0", out.Value)
	}
	// 非整数取模不再静默截断
	res, err = calc.Execute(context.Background(), json.RawMessage(`{"expression":"7.9 % 2.5"}`))
	if err != nil {
		t.Fatalf("7.9 %% 2.5: %v", err)
	}
	_ = json.Unmarshal([]byte(res.Render()), &out)
	if out.Value < 0.39 || out.Value > 0.41 { // 7.9-2*2.5=0.9
		t.Errorf("7.9 %% 2.5 = %v, want ~0.9", out.Value)
	}
	if _, err := calc.Execute(context.Background(), json.RawMessage(`{"expression":"5 % 0"}`)); err == nil {
		t.Error("modulo by zero should error")
	}
}

// panicTool 执行即 panic 的工具
type panicTool struct{}

// Name 工具名
func (panicTool) Name() string { return "bomb" }

// Description 工具描述
func (panicTool) Description() string { return "panic" }

// Parameters 参数 schema
func (panicTool) Parameters() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }

// Execute 直接 panic
func (panicTool) Execute(_ context.Context, _ json.RawMessage) (core.ToolResult, error) {
	panic("boom")
}

// TestH2ToolPanicRecovered 工具 panic 不得打崩进程，应转为工具错误回传模型
func TestH2ToolPanicRecovered(t *testing.T) {
	llm := &scriptedLLM{turns: []core.Message{
		{
			Role: core.RoleAssistant, FinishReason: core.FinishToolCalls,
			ToolCalls: []core.ToolCall{{ID: "t1", Name: "bomb", Arguments: `{}`}},
		},
		{Role: core.RoleAssistant, Content: "工具炸了但我还活着"},
	}}
	reg := tools.NewRegistry()
	reg.Register(panicTool{})
	mem := memory.NewBuffer(nil)
	loop := agent.NewLoop(llm, reg, mem, agent.Config{Model: "m", MaxIterations: 3})

	msg, _, err := loop.Run(context.Background(), "h2", "x")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(msg.Content, "活着") {
		t.Errorf("final = %q", msg.Content)
	}
	// panic 应作为 tool error 落历史
	history, _ := mem.Recent(context.Background(), "h2", 1_000_000)
	found := false
	for _, m := range history {
		if m.Role == core.RoleTool && strings.Contains(m.Content, "panicked") {
			found = true
		}
	}
	if !found {
		t.Error("panic not recorded as tool error in history")
	}
}
