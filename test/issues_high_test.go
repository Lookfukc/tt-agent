package test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
	"github.com/Lookfukc/tt-agent/pkg/tools"
	"github.com/Lookfukc/tt-agent/pkg/tools/builtin"
)

// usageScriptedLLM returns a preset message per turn, with attached usage
type usageScriptedLLM struct {
	calls int
	turns []core.Message
	usage []core.Usage
}

// Chat is unused
func (u *usageScriptedLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream asynchronously returns the current turn's message and usage
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

// TestH1UsageAccumulated verifies accumulated usage returned by Run must be non-zero (original bug: always zero)
func TestH1UsageAccumulated(t *testing.T) {
	// Two rounds: round 1 makes a tool call, round 2 converges; each round carries its own usage
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
	loop := agent.NewLoop(llm, reg, memorytest.NewBuffer(nil), agent.Config{
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

// TestH2CalculatorModuloFloatPanic: original bug: 5 % 0.5 triggered an integer divide-by-zero panic
func TestH2CalculatorModuloFloatPanic(t *testing.T) {
	calc := builtin.NewCalculator()
	// Before the fix: int64(0.5)==0 → panic: integer divide by zero
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
	// Non-integer modulo is no longer silently truncated
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

// panicTool is a tool that panics on execution
type panicTool struct{}

// Name returns the tool name
func (panicTool) Name() string { return "bomb" }

// Description returns the tool description
func (panicTool) Description() string { return "panic" }

// Parameters returns the parameter schema
func (panicTool) Parameters() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }

// Execute panics immediately
func (panicTool) Execute(_ context.Context, _ json.RawMessage) (core.ToolResult, error) {
	panic("boom")
}

// TestH2ToolPanicRecovered verifies a tool panic must not crash the process; it should be converted to a tool error fed back to the model
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
	mem := memorytest.NewBuffer(nil)
	loop := agent.NewLoop(llm, reg, mem, agent.Config{Model: "m", MaxIterations: 3})

	msg, _, err := loop.Run(context.Background(), "h2", "x")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(msg.Content, "活着") {
		t.Errorf("final = %q", msg.Content)
	}
	// The panic should land in history as a tool error
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
