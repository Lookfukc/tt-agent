package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
	"github.com/Lookfukc/tt-agent/pkg/orchestrator"
)

// captureRunStore wraps a store and records runIDs seen in Save,
// used to grab the ID right after Run persists so a concurrent Resume can be issued
type captureRunStore struct {
	mu    sync.Mutex
	seen  []string
	inner orchestrator.RunStore
}

// Save records the new runID, then passes through to the inner store
func (c *captureRunStore) Save(run *orchestrator.RunState) error {
	c.mu.Lock()
	if len(c.seen) == 0 || c.seen[len(c.seen)-1] != run.ID {
		c.seen = append(c.seen, run.ID)
	}
	c.mu.Unlock()
	return c.inner.Save(run)
}

// Get passes through to the inner store
func (c *captureRunStore) Get(id string) (*orchestrator.RunState, bool) {
	return c.inner.Get(id)
}

// firstID waits for the first runID to appear
// returns: the runID; false if it does not appear before the timeout
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

// TestM_O1RunVsResumeSingleExecution: a concurrent Resume on an in-flight Run's runID executes only one copy
//
// Run persists first (running) then executes the slow step; during that window
// a client can already see the runID in the store and issue a Resume; a Run
// that does not take the lock advances the state machine in parallel with
// Resume, producing double execution and double LLM cost
func TestM_O1RunVsResumeSingleExecution(t *testing.T) {
	store, err := orchestrator.NewFileRunStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileRunStore: %v", err)
	}
	capStore := &captureRunStore{inner: store}
	o := orchestrator.New(capStore)
	calls := 0
	slow := agent.NewLoop(&countingEchoLLM{delay: 200 * time.Millisecond, calls: &calls},
		nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"})
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

	// Resume must queue rather than run in parallel while Run holds the lock; neither side may deadlock
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

// TestN8RunLocksRefCountCleaned: the runLocks table is a private field so its size
// cannot be observed directly; verified indirectly: if a lock entry's refcount
// only ever grows or is deleted too early, the symptom is unbounded table growth
// or misaligned locking and hangs later — a large number of sequential and
// concurrent Runs must all complete normally
func TestN8RunLocksRefCountCleaned(t *testing.T) {
	o := orchestrator.New(nil)
	o.RegisterAgent("fast", agent.NewLoop(echoLLM{}, nil, memorytest.NewBuffer(nil), agent.Config{Model: "m"}))
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

	// Concurrent Runs with different runIDs shake the table-lock path: both
	// get-or-create and deletion happen under the table lock; lost entries or
	// deadlocks are not allowed
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
