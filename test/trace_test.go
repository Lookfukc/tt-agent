package test

import (
	"context"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/core"
	"github.com/Lookfukc/send-agent/pkg/memory"
	"github.com/Lookfukc/send-agent/pkg/observer"
	"github.com/Lookfukc/send-agent/pkg/orchestrator"
	"github.com/Lookfukc/send-agent/pkg/tools"
)

func TestLoopSpans(t *testing.T) {
	tracer := observer.NewMemoryTracer()
	llm := &scriptedLLM{turns: []core.Message{
		{
			Role: core.RoleAssistant, FinishReason: core.FinishToolCalls,
			ToolCalls: []core.ToolCall{{ID: "t1", Name: "echo", Arguments: `{}`}},
		},
		{Role: core.RoleAssistant, Content: "完成"},
	}}
	reg := tools.NewRegistry()
	reg.Register(&stubTool{})

	loop := agent.NewLoop(llm, reg, memory.NewBuffer(nil), agent.Config{
		Model: "m", MaxIterations: 4, Tracer: tracer,
	})
	if _, _, err := loop.Run(context.Background(), "s1", "go"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	spans := tracer.Spans()
	names := make(map[string]int)
	for _, s := range spans {
		names[s.Name]++
	}
	// run(1) + iter(2) + llm.stream(2) + tool.exec(1)
	for _, want := range []struct {
		name  string
		count int
	}{
		{"agent.run", 1}, {"agent.iter", 2}, {"llm.stream", 2}, {"tool.exec", 1},
	} {
		if names[want.name] != want.count {
			t.Errorf("%s = %d, want %d", want.name, names[want.name], want.count)
		}
	}
	if tracer.TraceCount() != 1 {
		t.Errorf("traces = %d, want 1", tracer.TraceCount())
	}

	// 树结构：除根外每个 span 的 ParentID 都能在集合里找到
	ids := make(map[string]bool)
	for _, s := range spans {
		ids[s.SpanID] = true
	}
	roots := 0
	for _, s := range spans {
		if s.ParentID == "" {
			roots++
		} else if !ids[s.ParentID] {
			t.Errorf("span %s has dangling parent %s", s.Name, s.ParentID)
		}
	}
	if roots != 1 {
		t.Errorf("roots = %d, want 1 (agent.run)", roots)
	}

	// tool span 携带工具名属性
	for _, s := range spans {
		if s.Name == "tool.exec" && s.Attributes["tool"] != "echo" {
			t.Errorf("tool span attrs = %v", s.Attributes)
		}
	}
}

func TestWorkflowSpans(t *testing.T) {
	tracer := observer.NewMemoryTracer()
	o := orchestrator.New(nil)
	o.Tracer = tracer
	mk := func() *agent.Loop {
		return agent.NewLoop(echoLLM{}, nil, memory.NewBuffer(nil), agent.Config{Model: "m"})
	}
	o.RegisterAgent("writer", mk())
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name:  "w",
		Steps: []orchestrator.Step{orchestrator.AgentStep{Agent: "writer", Input: "$input"}},
	})
	if _, err := o.Run(context.Background(), "w", "x"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	spans := tracer.Spans()
	found := map[string]bool{}
	for _, s := range spans {
		found[s.Name] = true
	}
	if !found["workflow.run"] || !found["workflow.step"] {
		t.Errorf("missing spans, got %v", found)
	}
	if tracer.TraceCount() != 1 {
		t.Errorf("traces = %d, want 1", tracer.TraceCount())
	}
}
