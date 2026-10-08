package test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
	"github.com/Lookfukc/tt-agent/pkg/tools"
)

// slowTool takes a fixed duration and records start/end times, used to verify parallelism
type slowTool struct {
	mu     sync.Mutex
	starts []time.Time
	ends   []time.Time
}

// Name returns the tool name.
func (s *slowTool) Name() string { return "slow" }

// Description returns the tool description.
func (s *slowTool) Description() string { return "slow" }

// Parameters returns the parameter schema.
func (s *slowTool) Parameters() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }

// Execute sleeps 100ms and records start/end times.
func (s *slowTool) Execute(_ context.Context, _ json.RawMessage) (core.ToolResult, error) {
	s.mu.Lock()
	s.starts = append(s.starts, time.Now())
	s.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	s.mu.Lock()
	s.ends = append(s.ends, time.Now())
	s.mu.Unlock()
	return core.ToolResult{Text: "done"}, nil
}

// TestParallelToolExecution verifies that two tools running in parallel take roughly a single run's time, not double.
func TestParallelToolExecution(t *testing.T) {
	llm := &scriptedLLM{turns: []core.Message{
		{
			Role: core.RoleAssistant, FinishReason: core.FinishToolCalls,
			ToolCalls: []core.ToolCall{
				{ID: "t1", Name: "slow", Arguments: `{}`},
				{ID: "t2", Name: "slow", Arguments: `{}`},
			},
		},
		{Role: core.RoleAssistant, Content: "完成"},
	}}
	tool := &slowTool{}
	reg := tools.NewRegistry()
	reg.Register(tool)

	loop := agent.NewLoop(llm, reg, memorytest.NewBuffer(nil), loopConfig())
	start := time.Now()
	if _, _, err := loop.Run(context.Background(), "s1", "go"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 180*time.Millisecond {
		t.Errorf("elapsed = %v, tools did not run in parallel", elapsed)
	}

	// Each tool started before the other finished, proving the executions overlapped.
	if len(tool.starts) != 2 {
		t.Fatalf("executions = %d", len(tool.starts))
	}
	if !(tool.starts[1].Before(tool.ends[0]) || tool.starts[0].Before(tool.ends[1])) {
		t.Error("tool executions did not overlap")
	}
}

func TestParallelToolResultOrder(t *testing.T) {
	llm := &scriptedLLM{turns: []core.Message{
		{
			Role: core.RoleAssistant, FinishReason: core.FinishToolCalls,
			ToolCalls: []core.ToolCall{
				{ID: "t1", Name: "slow", Arguments: `{"i":1}`},
				{ID: "t2", Name: "slow", Arguments: `{"i":2}`},
			},
		},
		{Role: core.RoleAssistant, Content: "完成"},
	}}
	reg := tools.NewRegistry()
	reg.Register(&slowTool{})
	mem := memorytest.NewBuffer(nil)
	loop := agent.NewLoop(llm, reg, mem, loopConfig())
	if _, _, err := loop.Run(context.Background(), "s2", "go"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	history, _ := mem.Recent(context.Background(), "s2", 1_000_000)
	var toolIDs []string
	for _, m := range history {
		if m.Role == core.RoleTool {
			toolIDs = append(toolIDs, m.ToolCallID)
		}
	}
	if len(toolIDs) != 2 || toolIDs[0] != "t1" || toolIDs[1] != "t2" {
		t.Errorf("tool result order = %v, want [t1 t2]", toolIDs)
	}
}
