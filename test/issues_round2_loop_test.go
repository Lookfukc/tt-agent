package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// captureLLM records received requests, replies per script, and can attach usage events
type captureLLM struct {
	mu    sync.Mutex
	reqs  []core.ChatRequest
	turns []core.Message
	usage core.Usage
}

// Chat is unused; the loop always uses streaming
func (c *captureLLM) Chat(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("not implemented")
}

// ChatStream records the request, then returns the current turn's preset message as an event stream
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

// firstReq returns the first recorded request
// returns: a copy of the request
func (c *captureLLM) firstReq() core.ChatRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reqs[0]
}

// scriptedMemory is a programmable failing memory: Add fails when it hits failRole,
// Recent fails when recentErr is non-nil, and other behavior matches Buffer
type scriptedMemory struct {
	inner     *memorytest.Buffer
	failRole  core.Role
	recentErr error
}

// Add errors when it hits the failing role, otherwise passes through to the inner store
func (s *scriptedMemory) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	for _, m := range msgs {
		if m.Role == s.failRole {
			return errors.New("scripted memory failure")
		}
	}
	return s.inner.Add(ctx, sessionID, msgs...)
}

// Recent errors per script or passes through to the inner store
func (s *scriptedMemory) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	if s.recentErr != nil {
		return nil, s.recentErr
	}
	return s.inner.Recent(ctx, sessionID, budget)
}

// Clear passes through to the inner store
func (s *scriptedMemory) Clear(ctx context.Context, sessionID string) error {
	return s.inner.Clear(ctx, sessionID)
}

// TestR2H5OutgoingRequestSanitized: dangling tool_calls and orphan tool messages are repaired only on the request side
//
// Memory is preloaded with [assistant(1 call without result), orphan tool, user];
// the request the model receives must contain a synthesized result for that call
// and no orphan; the memory itself keeps the truth, persisting no repair artifacts
func TestR2H5OutgoingRequestSanitized(t *testing.T) {
	ctx := context.Background()
	buf := memorytest.NewBuffer(nil)
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

	// Memory is not polluted: the preloaded messages remain verbatim, no synthesized results mixed in
	hist, _ := buf.Recent(ctx, "s1", 100_000)
	if len(hist) != len(preload)+2 { // +user +assistant("done")
		t.Fatalf("H5: memory mutated by sanitize, len = %d, want %d", len(hist), len(preload)+2)
	}
	for _, m := range hist {
		if m.Content == "tool error: interrupted before execution" {
			t.Error("H5: synthesized result persisted into memory")
		}
	}
}

// TestN9UsageKeptOnToolResultAddFailure verifies this round's usage must not be dropped when persisting tool results fails
func TestN9UsageKeptOnToolResultAddFailure(t *testing.T) {
	ctx := context.Background()
	mem := &scriptedMemory{inner: memorytest.NewBuffer(nil), failRole: core.RoleTool}
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

// TestN13EventErrorOnMemoryFailures verifies all three memory-layer failure paths must emit EventError
func TestN13EventErrorOnMemoryFailures(t *testing.T) {
	ctx := context.Background()
	plainTurn := []core.Message{{Role: core.RoleAssistant, Content: "ok"}}
	toolTurn := []core.Message{
		{Role: core.RoleAssistant, FinishReason: core.FinishToolCalls, ToolCalls: []core.ToolCall{
			{ID: "t1", Name: "echo", Arguments: `{}`},
		}},
	}

	// runAndCollect runs one round and collects EventError events; the OnEvent
	// contract allows multi-goroutine callbacks, so the collector locks per contract
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
		mem := &scriptedMemory{inner: memorytest.NewBuffer(nil), recentErr: errors.New("recent boom")}
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
		mem := &scriptedMemory{inner: memorytest.NewBuffer(nil), failRole: core.RoleAssistant}
		errs, runErr := runAndCollect(mem, &captureLLM{turns: plainTurn})
		if runErr == nil {
			t.Fatal("N13: want Run error when Add(assistant) fails")
		}
		if len(errs) == 0 {
			t.Fatal("N13: Add(assistant) failure emitted no EventError")
		}
	})
	t.Run("AddToolFail", func(t *testing.T) {
		mem := &scriptedMemory{inner: memorytest.NewBuffer(nil), failRole: core.RoleTool}
		errs, runErr := runAndCollect(mem, &captureLLM{turns: toolTurn})
		if runErr == nil {
			t.Fatal("N13: want Run error when Add(tool) fails")
		}
		if len(errs) == 0 {
			t.Fatal("N13: Add(tool) failure emitted no EventError")
		}
	})
}

// TestL_E7MemoryTracerSpanCap verifies that past the cap, Spans() keeps only a limited number of recent spans
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
	// Eviction starts from the oldest (earliest ended); the last 3 are kept
	for i, want := range []string{"span-7", "span-8", "span-9"} {
		if spans[i].Name != want {
			t.Errorf("L-E7: spans[%d] = %s, want %s (oldest must be evicted)", i, spans[i].Name, want)
		}
	}

	// The default constructor is likewise bounded: length caps out past the default limit
	def := observer.NewMemoryTracer()
	for i := 0; i < 10005; i++ {
		_, span := def.StartSpan(context.Background(), "s")
		span.End()
	}
	if got := len(def.Spans()); got != 10000 {
		t.Fatalf("L-E7: default tracer spans len = %d, want 10000", got)
	}
}
