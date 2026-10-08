package test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
	"github.com/Lookfukc/tt-agent/pkg/orchestrator"
)

// TestM_M1SessionIDTraversalBlocked verifies session-ID path traversal must be rejected
func TestM_M1SessionIDTraversalBlocked(t *testing.T) {
	p, err := memory.NewPersistent(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	ctx := context.Background()
	for _, evil := range []string{"../evil", "a/b", `a\b`, "..", ""} {
		if err := p.Add(ctx, evil, core.Message{Role: core.RoleUser, Content: "x"}); err == nil {
			t.Errorf("M-M1: Add(%q) must be rejected", evil)
		}
		if err := p.Clear(ctx, evil); err == nil {
			t.Errorf("M-M1: Clear(%q) must be rejected", evil)
		}
	}
}

// TestM_M2PoisonLineSkipped verifies an oversized poison line must not block recovery of subsequent messages
func TestM_M2PoisonLineSkipped(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/s1.jsonl"
	good1 := `{"role":"user","content":"first"}`
	good2 := `{"role":"assistant","content":"last"}`
	poison := `{"role":"user","content":"` + strings.Repeat("x", 5<<20) + `"}`
	content := good1 + "\n" + poison + "\n" + good2 + "\n"
	if err := writeFile(path, content); err != nil {
		t.Fatalf("write: %v", err)
	}

	p, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	got, err := p.Recent(context.Background(), "s1", 1_000_000)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 2 || got[0].Content != "first" || got[1].Content != "last" {
		t.Fatalf("M-M2: recovery stopped at poison line, got %d msgs", len(got))
	}
}

// TestM_O2ResumeInterruptedRun verifies a run left in running state by cancellation can be resumed
func TestM_O2ResumeInterruptedRun(t *testing.T) {
	store, err := orchestrator.NewFileRunStore(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	o := orchestrator.New(store)
	// Slow agent: ensures the cancellation lands mid-step rather than between steps
	dummy := 0
	slow := agent.NewLoop(&countingEchoLLM{delay: 300 * time.Millisecond, calls: &dummy},
		nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"})
	o.RegisterAgent("writer", slow)
	o.RegisterAgent("reviewer", agent.NewLoop(echoLLM{}, nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"}))
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name: "review",
		Steps: []orchestrator.Step{
			orchestrator.AgentStep{Agent: "writer", Input: "$input"},
			orchestrator.AgentStep{Agent: "reviewer", Input: "$prev"},
		},
	})

	cctx, ccancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		ccancel()
	}()
	run, runErr := o.Run(cctx, "review", "草稿")
	if runErr == nil {
		t.Fatal("want cancellation error")
	}
	if run.Status != orchestrator.StatusRunning {
		t.Fatalf("M-O2: interrupted run status = %s, want running (failed would lock out Resume)", run.Status)
	}

	// The old implementation only accepted waiting, so interrupted runs were stuck forever; now they can rerun from the current step
	resumed, err := o.Resume(context.Background(), run.ID, "")
	if err != nil {
		t.Fatalf("M-O2: Resume interrupted run: %v", err)
	}
	if resumed.Status != orchestrator.StatusDone {
		t.Fatalf("resumed status = %s, err=%s", resumed.Status, resumed.Err)
	}
}

// TestM_A1LoopEventCarriesSession verifies events must carry the session identifier
func TestM_A1LoopEventCarriesSession(t *testing.T) {
	var sessions []string
	loop := agent.NewLoop(echoLLM{}, nil, memorytest.NewBuffer(nil), agent.Config{
		Model: "m",
		OnEvent: func(e agent.LoopEvent) {
			if e.SessionID != "" && e.Type == agent.EventDone {
				sessions = append(sessions, e.SessionID)
			}
		},
	})
	if _, _, err := loop.Run(context.Background(), "sess-A", "x"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, _, err := loop.Run(context.Background(), "sess-B", "y"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(sessions) != 2 || sessions[0] != "sess-A" || sessions[1] != "sess-B" {
		t.Fatalf("M-A1: session routing lost, got %v", sessions)
	}
}

// TestM_O1ConcurrentResumeSingleExecution verifies concurrent Resume executes only one copy
func TestM_O1ConcurrentResumeSingleExecution(t *testing.T) {
	store, _ := orchestrator.NewFileRunStore(t.TempDir())
	o := orchestrator.New(store)
	calls := 0
	slow := agent.NewLoop(&countingEchoLLM{delay: 150 * time.Millisecond, calls: &calls},
		nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"})
	o.RegisterAgent("worker", slow)
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name:  "w",
		Steps: []orchestrator.Step{orchestrator.AgentStep{Agent: "worker", Input: "$input"}},
	})

	// Cancel to create an interrupted running state (cancelled mid-step)
	cctx, ccancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		ccancel()
	}()
	run, _ := o.Run(cctx, "w", "x")
	if run == nil || run.Status != orchestrator.StatusRunning {
		t.Fatalf("setup failed: %+v", run)
	}

	// Two concurrent Resumes: only one should acquire execution; the other, after queuing, sees a terminal state
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := o.Resume(context.Background(), run.ID, "")
			done <- err
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("M-O1: concurrent Resume hung")
		}
	}
	if calls > 1 {
		t.Fatalf("M-O1: step executed %d times under concurrent Resume, want 1", calls)
	}
}

// countingEchoLLM is an echo model that counts and delays
type countingEchoLLM struct {
	delay time.Duration
	calls *int
}

// Chat is unused
func (c *countingEchoLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream echoes after a delay
func (c *countingEchoLLM) ChatStream(ctx context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	var input string
	for _, m := range req.Messages {
		if m.Role == core.RoleUser {
			input = m.Content
		}
	}
	events := make(chan core.StreamEvent, 3)
	go func() {
		defer close(events)
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			events <- core.StreamEvent{Type: core.StreamError, Err: ctx.Err()}
			return
		}
		// Only count responses actually produced; cancelled attempts don't count
		*c.calls++
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "echo:" + input}
		events <- core.StreamEvent{Type: core.StreamDone}
	}()
	return events, nil
}

// writeFile is a test helper that writes a file
// returns: error
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
