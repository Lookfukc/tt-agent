package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
	"github.com/Lookfukc/tt-agent/pkg/tools"
)

// scriptedLLM returns scripted messages turn by turn.
type scriptedLLM struct {
	calls int
	turns []core.Message
}

// Chat is unused; the loop always goes through streaming.
func (s *scriptedLLM) Chat(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("not implemented")
}

// ChatStream returns this turn's scripted message as an async event stream, matching the producer contract of real adapters.
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

// stubTool is a test tool that records its invocations.
type stubTool struct{ executed []string }

// loopConfig is the loop config used in tests, with a circuit-breaker cap of 3 iterations.
func loopConfig() agent.Config {
	return agent.Config{Model: "m", MaxIterations: 3}
}

// Name returns the tool name.
func (s *stubTool) Name() string { return "echo" }

// Description returns the tool description.
func (s *stubTool) Description() string { return "echo" }

// Parameters returns the parameter schema.
func (s *stubTool) Parameters() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }

// Execute records the arguments and echoes them back.
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
	mem := memorytest.NewBuffer(nil)

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
	// Every turn requests a tool call, verifying the circuit breaker.
	always := core.Message{
		Role: core.RoleAssistant, FinishReason: core.FinishToolCalls,
		ToolCalls: []core.ToolCall{{ID: "t1", Name: "echo", Arguments: `{}`}},
	}
	llm := &scriptedLLM{turns: []core.Message{always}}
	reg := tools.NewRegistry()
	reg.Register(&stubTool{})

	loop := agent.NewLoop(llm, reg, memorytest.NewBuffer(nil), loopConfig())
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
	loop := agent.NewLoop(llm, tools.NewRegistry(), memorytest.NewBuffer(nil), loopConfig())

	msg, _, err := loop.Run(context.Background(), "s1", "go")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if msg.Content != "工具不存在，改用直接回答" {
		t.Errorf("final = %q", msg.Content)
	}
}
