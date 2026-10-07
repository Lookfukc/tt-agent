package test

import (
	"context"
	"errors"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/orchestrator"
)

// echoLLM 把收到的用户输入原样返回，便于断言模板解析
type echoLLM struct{}

// Chat 未使用
func (echoLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("not implemented")
}

// ChatStream 把首条 user 消息内容作为输出返回
func (echoLLM) ChatStream(_ context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	var input string
	for _, m := range req.Messages {
		if m.Role == core.RoleUser {
			input = m.Content
		}
	}
	events := make(chan core.StreamEvent, 3)
	go func() {
		defer close(events)
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "echo:" + input}
		events <- core.StreamEvent{Type: core.StreamDone}
	}()
	return events, nil
}

// newOrchestrator 装配双 Agent 编排器：writer 与 reviewer
func newOrchestrator(t *testing.T, store orchestrator.RunStore) *orchestrator.Orchestrator {
	t.Helper()
	o := orchestrator.New(store)
	mk := func() *agent.Loop {
		return agent.NewLoop(echoLLM{}, nil, memory.NewBuffer(nil), agent.Config{Model: "m"})
	}
	o.RegisterAgent("writer", mk())
	o.RegisterAgent("reviewer", mk())
	return o
}

func TestWorkflowSequentialChain(t *testing.T) {
	o := newOrchestrator(t, nil)
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name: "review",
		Steps: []orchestrator.Step{
			orchestrator.AgentStep{Agent: "writer", Input: "$input"},
		},
	})
	ctx := context.Background()
	run, err := o.Run(ctx, "review", "写首诗")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Status != orchestrator.StatusDone {
		t.Errorf("status = %s, err=%s", run.Status, run.Err)
	}
	if run.Outputs["writer"] != "echo:写首诗" {
		t.Errorf("output = %q", run.Outputs["writer"])
	}
}

func TestWorkflowTemplates(t *testing.T) {
	o := newOrchestrator(t, nil)
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name: "chain",
		Steps: []orchestrator.Step{
			orchestrator.AgentStep{Agent: "writer", Input: "$input"},
			orchestrator.AgentStep{Agent: "reviewer", Input: "$prev"},
			orchestrator.AgentStep{Agent: "writer", Name: "polish", Input: "$step.reviewer.output"},
		},
	})
	run, err := o.Run(context.Background(), "chain", "初始")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Outputs["reviewer"] != "echo:echo:初始" {
		t.Errorf("$prev chain = %q", run.Outputs["reviewer"])
	}
	if run.Outputs["polish"] != "echo:echo:echo:初始" {
		t.Errorf("$step ref = %q", run.Outputs["polish"])
	}
}

func TestCheckpointPauseAndResume(t *testing.T) {
	store, err := orchestrator.NewFileRunStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileRunStore: %v", err)
	}
	o := newOrchestrator(t, store)
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name: "review",
		Steps: []orchestrator.Step{
			orchestrator.AgentStep{Agent: "writer", Input: "$input"},
			orchestrator.CheckpointStep{Name: "approve", Prompt: "审核草稿"},
			orchestrator.AgentStep{Agent: "reviewer", Input: "$prev"},
		},
	})

	ctx := context.Background()
	run, err := o.Run(ctx, "review", "草稿")
	if !errors.Is(err, orchestrator.ErrCheckpoint) {
		t.Fatalf("err = %v, want ErrCheckpoint", err)
	}
	if run.Status != orchestrator.StatusWaiting || run.StepIdx != 1 {
		t.Errorf("run = %+v", run)
	}

	// 模拟重启：工作流定义随代码重新注册，运行状态来自落盘
	o2 := newOrchestrator(t, store)
	_ = o2.RegisterWorkflow(&orchestrator.Workflow{
		Name: "review",
		Steps: []orchestrator.Step{
			orchestrator.AgentStep{Agent: "writer", Input: "$input"},
			orchestrator.CheckpointStep{Name: "approve", Prompt: "审核草稿"},
			orchestrator.AgentStep{Agent: "reviewer", Input: "$prev"},
		},
	})
	resumed, err := o2.Resume(ctx, run.ID, "人工批复：通过")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Status != orchestrator.StatusDone {
		t.Errorf("status = %s, err=%s", resumed.Status, resumed.Err)
	}
	if resumed.Outputs["reviewer"] != "echo:人工批复：通过" {
		t.Errorf("human input not fed: %q", resumed.Outputs["reviewer"])
	}
}

func TestWorkflowUnknownAgent(t *testing.T) {
	o := orchestrator.New(nil)
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name:  "bad",
		Steps: []orchestrator.Step{orchestrator.AgentStep{Agent: "ghost", Input: "$input"}},
	})
	run, err := o.Run(context.Background(), "bad", "x")
	if err == nil {
		t.Fatal("want error")
	}
	if run.Status != orchestrator.StatusFailed || run.Err == "" {
		t.Errorf("run = %+v", run)
	}
}
