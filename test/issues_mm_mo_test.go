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
	"github.com/Lookfukc/tt-agent/pkg/orchestrator"
)

// TestM_M1SessionIDTraversalBlocked 会话 ID 路径穿越必须被拒
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

// TestM_M2PoisonLineSkipped 超长毒行不得阻断后续消息恢复
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

// TestM_O2ResumeInterruptedRun 取消遗留的 running 运行可续跑
func TestM_O2ResumeInterruptedRun(t *testing.T) {
	store, err := orchestrator.NewFileRunStore(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	o := orchestrator.New(store)
	// 慢 agent：确保取消落在步骤执行中段而非两步之间
	dummy := 0
	slow := agent.NewLoop(&countingEchoLLM{delay: 300 * time.Millisecond, calls: &dummy},
		nil, memory.NewBuffer(nil), agent.Config{Model: "m"})
	o.RegisterAgent("writer", slow)
	o.RegisterAgent("reviewer", agent.NewLoop(echoLLM{}, nil, memory.NewBuffer(nil), agent.Config{Model: "m"}))
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

	// 旧实现只接受 waiting，中断的 run 永久卡死；现在可从当前步重跑
	resumed, err := o.Resume(context.Background(), run.ID, "")
	if err != nil {
		t.Fatalf("M-O2: Resume interrupted run: %v", err)
	}
	if resumed.Status != orchestrator.StatusDone {
		t.Fatalf("resumed status = %s, err=%s", resumed.Status, resumed.Err)
	}
}

// TestM_A1LoopEventCarriesSession 事件必须携带会话标识
func TestM_A1LoopEventCarriesSession(t *testing.T) {
	var sessions []string
	loop := agent.NewLoop(echoLLM{}, nil, memory.NewBuffer(nil), agent.Config{
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

// TestM_O1ConcurrentResumeSingleExecution 并发 Resume 只执行一份
func TestM_O1ConcurrentResumeSingleExecution(t *testing.T) {
	store, _ := orchestrator.NewFileRunStore(t.TempDir())
	o := orchestrator.New(store)
	calls := 0
	slow := agent.NewLoop(&countingEchoLLM{delay: 150 * time.Millisecond, calls: &calls},
		nil, memory.NewBuffer(nil), agent.Config{Model: "m"})
	o.RegisterAgent("worker", slow)
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name:  "w",
		Steps: []orchestrator.Step{orchestrator.AgentStep{Agent: "worker", Input: "$input"}},
	})

	// 取消制造 running 中断态（步骤执行中取消）
	cctx, ccancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		ccancel()
	}()
	run, _ := o.Run(cctx, "w", "x")
	if run == nil || run.Status != orchestrator.StatusRunning {
		t.Fatalf("setup failed: %+v", run)
	}

	// 两个并发 Resume：只有一个应拿到执行权，另一个排队后看到终态
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

// countingEchoLLM 计数并延时的 echo 模型
type countingEchoLLM struct {
	delay time.Duration
	calls *int
}

// Chat 未使用
func (c *countingEchoLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream 延时后回显
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
		// 只计真实产出的响应，被取消的尝试不计
		*c.calls++
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "echo:" + input}
		events <- core.StreamEvent{Type: core.StreamDone}
	}()
	return events, nil
}

// writeFile 测试辅助写文件
// returns: 错误
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
