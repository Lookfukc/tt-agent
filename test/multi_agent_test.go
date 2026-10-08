package test

import (
	"context"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
	"github.com/Lookfukc/tt-agent/pkg/orchestrator"
)

// scriptedTurnsLLM returns scripted content based on the call count.
type scriptedTurnsLLM struct {
	turns []string
	calls int
}

// Chat is unused.
func (s *scriptedTurnsLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream returns this turn's scripted text.
func (s *scriptedTurnsLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	text := s.turns[min(s.calls, len(s.turns)-1)]
	s.calls++
	events := make(chan core.StreamEvent, 3)
	go func() {
		defer close(events)
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: text}
		events <- core.StreamEvent{Type: core.StreamDone}
	}()
	return events, nil
}

func TestRouterStep(t *testing.T) {
	o := orchestrator.New(nil)
	mk := func(turns ...string) *agent.Loop {
		return agent.NewLoop(&scriptedTurnsLLM{turns: turns}, nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"})
	}
	o.RegisterAgent("classifier", mk("reviewer"))
	o.RegisterAgent("writer", mk("写作结果"))
	o.RegisterAgent("reviewer", mk("审核结果"))
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name: "routed",
		Steps: []orchestrator.Step{
			orchestrator.RouterStep{
				Name: "route", Router: "classifier",
				Candidates: []string{"writer", "reviewer"},
				Input:      "$input",
			},
		},
	})

	run, err := o.Run(context.Background(), "routed", "请审核这段代码")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Status != orchestrator.StatusDone {
		t.Fatalf("status = %s, err = %s", run.Status, run.Err)
	}
	if run.Outputs["route"] != "审核结果" {
		t.Errorf("output = %q", run.Outputs["route"])
	}
	if run.Outputs["route:route"] != "reviewer" {
		t.Errorf("route decision = %q", run.Outputs["route:route"])
	}
}

func TestRouterStepInvalidChoice(t *testing.T) {
	o := orchestrator.New(nil)
	mk := func(turns ...string) *agent.Loop {
		return agent.NewLoop(&scriptedTurnsLLM{turns: turns}, nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"})
	}
	o.RegisterAgent("classifier", mk("不存在的agent"))
	o.RegisterAgent("writer", mk("x"))
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name: "bad",
		Steps: []orchestrator.Step{
			orchestrator.RouterStep{
				Router: "classifier", Candidates: []string{"writer"}, Input: "$input",
			},
		},
	})
	run, err := o.Run(context.Background(), "bad", "x")
	if err == nil {
		t.Fatal("want failure on invalid choice")
	}
	if run.Status != orchestrator.StatusFailed {
		t.Errorf("status = %s", run.Status)
	}
}

func TestSupervisorStep(t *testing.T) {
	o := orchestrator.New(nil)
	mk := func(turns ...string) *agent.Loop {
		return agent.NewLoop(&scriptedTurnsLLM{turns: turns}, nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"})
	}
	// Supervisor: first delegate to writer, then reviewer, finally converge.
	o.RegisterAgent("boss", mk(
		"WORKER writer\n写初稿",
		"WORKER reviewer\n审核初稿",
		"DONE\n定稿完成",
	))
	o.RegisterAgent("writer", mk("初稿内容"))
	o.RegisterAgent("reviewer", mk("审核通过"))
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name: "supervised",
		Steps: []orchestrator.Step{
			orchestrator.SupervisorStep{
				Name: "final", Supervisor: "boss",
				Workers: []string{"writer", "reviewer"},
				Input:   "$input",
			},
		},
	})

	run, err := o.Run(context.Background(), "supervised", "做个报告")
	if err != nil {
		t.Fatalf("Run: %v, runErr=%s", err, run.Err)
	}
	if run.Status != orchestrator.StatusDone {
		t.Fatalf("status = %s, err = %s", run.Status, run.Err)
	}
	if run.Outputs["final"] != "定稿完成" {
		t.Errorf("final = %q", run.Outputs["final"])
	}
}

func TestSupervisorRoundsExhausted(t *testing.T) {
	o := orchestrator.New(nil)
	mk := func(turns ...string) *agent.Loop {
		return agent.NewLoop(&scriptedTurnsLLM{turns: turns}, nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"})
	}
	// Delegates forever, never converges.
	o.RegisterAgent("boss", mk("WORKER writer\n继续"))
	o.RegisterAgent("writer", mk("干"))
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name: "loop",
		Steps: []orchestrator.Step{
			orchestrator.SupervisorStep{
				Supervisor: "boss", Workers: []string{"writer"},
				Input: "$input", MaxRounds: 3,
			},
		},
	})
	run, err := o.Run(context.Background(), "loop", "x")
	if err == nil {
		t.Fatal("want rounds exhausted error")
	}
	if run.Status != orchestrator.StatusFailed {
		t.Errorf("status = %s", run.Status)
	}
}
