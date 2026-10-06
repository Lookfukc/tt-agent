package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/memory"
	"github.com/Lookfukc/send-agent/pkg/orchestrator"
)

// captureRunStore 包装存储并记录 Save 出现过的 runID，
// 用于在 Run 落盘后立刻拿到 ID 发起并发 Resume
type captureRunStore struct {
	mu    sync.Mutex
	seen  []string
	inner orchestrator.RunStore
}

// Save 记录新 runID 后透传内层
func (c *captureRunStore) Save(run *orchestrator.RunState) error {
	c.mu.Lock()
	if len(c.seen) == 0 || c.seen[len(c.seen)-1] != run.ID {
		c.seen = append(c.seen, run.ID)
	}
	c.mu.Unlock()
	return c.inner.Save(run)
}

// Get 透传内层
func (c *captureRunStore) Get(id string) (*orchestrator.RunState, bool) {
	return c.inner.Get(id)
}

// firstID 等待首个 runID 出现
// returns: runID；超时未出现返回 false
func (c *captureRunStore) firstID(timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := len(c.seen)
		c.mu.Unlock()
		if n > 0 {
			return c.seen[0], true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return "", false
}

// TestM_O1RunVsResumeSingleExecution 对在途 Run 的 runID 并发 Resume 只执行一份
//
// Run 先落盘（running）再执行慢步骤，窗口期内客户端已能从存储看到
// runID 并发起 Resume；不取锁的 Run 会与 Resume 并行推进状态机，
// 双份执行双倍 LLM 花费
func TestM_O1RunVsResumeSingleExecution(t *testing.T) {
	store, err := orchestrator.NewFileRunStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileRunStore: %v", err)
	}
	capStore := &captureRunStore{inner: store}
	o := orchestrator.New(capStore)
	calls := 0
	slow := agent.NewLoop(&countingEchoLLM{delay: 200 * time.Millisecond, calls: &calls},
		nil, memory.NewBuffer(nil), agent.Config{Model: "m"})
	o.RegisterAgent("worker", slow)
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name:  "w",
		Steps: []orchestrator.Step{orchestrator.AgentStep{Agent: "worker", Input: "$input"}},
	})

	runDone := make(chan error, 1)
	go func() {
		_, err := o.Run(context.Background(), "w", "x")
		runDone <- err
	}()

	runID, ok := capStore.firstID(2 * time.Second)
	if !ok {
		t.Fatal("M-O1: run state never persisted, cannot stage Resume")
	}

	resumeDone := make(chan error, 1)
	go func() {
		_, err := o.Resume(context.Background(), runID, "")
		resumeDone <- err
	}()

	// Resume 在 Run 持锁期间必须排队而非并行执行，两侧都不能卡死
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("M-O1: Run hung under concurrent Resume")
	}
	select {
	case <-resumeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("M-O1: Resume hung waiting for Run's lock")
	}
	if calls > 1 {
		t.Fatalf("M-O1: step executed %d times under concurrent Run+Resume, want 1", calls)
	}
}

// TestN8RunLocksRefCountCleaned runLocks 表是私有字段无法直接观测容量，
// 间接验证：锁条目引用计数若只增不减或提前删除，表现为表无限膨胀
// 或后续加锁错位卡死——大量顺序与并发 Run 必须全部正常完成
func TestN8RunLocksRefCountCleaned(t *testing.T) {
	o := orchestrator.New(nil)
	o.RegisterAgent("fast", agent.NewLoop(echoLLM{}, nil, memory.NewBuffer(nil), agent.Config{Model: "m"}))
	_ = o.RegisterWorkflow(&orchestrator.Workflow{
		Name:  "w",
		Steps: []orchestrator.Step{orchestrator.AgentStep{Agent: "fast", Input: "$input"}},
	})

	errCh := make(chan error, 1)
	go func() {
		for i := 0; i < 200; i++ {
			run, err := o.Run(context.Background(), "w", "x")
			if err != nil || run.Status != orchestrator.StatusDone {
				errCh <- fmt.Errorf("sequential Run %d: err=%v status=%s", i, err, run.Status)
				return
			}
		}
		errCh <- nil
	}()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("N8: sequential Runs hung — runLocks release path broken")
	}

	// 并发不同 runID 的 Run 摇表锁路径：get-or-create 与删除都在表锁内，
	// 不允许出现丢条目或死锁
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := o.Run(context.Background(), "w", "x"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
