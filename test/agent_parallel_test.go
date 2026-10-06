package test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/core"
	"github.com/Lookfukc/send-agent/pkg/memory"
	"github.com/Lookfukc/send-agent/pkg/tools"
)

// slowTool 固定耗时并记录起止时间的工具，用于验证并行
type slowTool struct {
	mu     sync.Mutex
	starts []time.Time
	ends   []time.Time
}

// Name 工具名
func (s *slowTool) Name() string { return "slow" }

// Description 工具描述
func (s *slowTool) Description() string { return "slow" }

// Parameters 参数 schema
func (s *slowTool) Parameters() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }

// Execute 睡 100ms 记录起止
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

// TestParallelToolExecution 两个工具并行时总耗时应接近单次而非两倍
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

	loop := agent.NewLoop(llm, reg, memory.NewBuffer(nil), loopConfig())
	start := time.Now()
	if _, _, err := loop.Run(context.Background(), "s1", "go"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 180*time.Millisecond {
		t.Errorf("elapsed = %v, tools did not run in parallel", elapsed)
	}

	// 两个工具的启动时间都在另一个结束之前，说明存在重叠
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
	mem := memory.NewBuffer(nil)
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
