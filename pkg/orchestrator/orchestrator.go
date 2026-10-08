// Package orchestrator provides multi-agent workflow orchestration with
// resume-from-checkpoint capability.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// ErrCheckpoint indicates the workflow paused at a manual checkpoint,
// awaiting Resume.
var ErrCheckpoint = errors.New("workflow paused at checkpoint")

// Status is the run status.
type Status string

const (
	StatusRunning Status = "running"
	StatusWaiting Status = "waiting" // paused at a checkpoint
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

// Step is a workflow step, a closed sum type allowing only agents and
// checkpoints.
type Step interface{ stepKind() }

// AgentStep executes one registered agent.
type AgentStep struct {
	// Name is the step name, referenced by later steps as
	// $step.<name>.output; falls back to the agent name when empty.
	Name string
	// Agent is the registered agent name.
	Agent string
	// Input is the input template: $input is the workflow's initial
	// input, $prev is the previous step's output, and
	// $step.<name>.output references a given step's output;
	// non-template strings are passed through verbatim.
	Input string
}

// stepKind implements the Step interface.
func (AgentStep) stepKind() {}

// CheckpointStep is a manual checkpoint: execution pauses and persists
// here, awaiting a human Resume.
type CheckpointStep struct {
	// Name is the checkpoint name.
	Name string
	// Prompt is the explanatory note shown to the reviewer.
	Prompt string
}

// stepKind implements the Step interface.
func (CheckpointStep) stepKind() {}

// Workflow is a sequential workflow.
//
// V1 does linear execution only; dispatch modes like Router/Supervisor
// will be added once the state model stabilizes.
type Workflow struct {
	Name  string
	Steps []Step
}

// RunState is the persisted state of one workflow run.
type RunState struct {
	ID        string
	Workflow  string
	StepIdx   int
	Input     string
	Prev      string
	Outputs   map[string]string
	Status    Status
	Err       string
	Usage     core.Usage
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Orchestrator is the registration center for agents and workflows;
// Run/Resume drive the state machine.
type Orchestrator struct {
	mu        sync.RWMutex
	agents    map[string]*agent.Loop
	workflows map[string]*Workflow
	store     RunStore
	seq       atomic.Int64

	// Tracer is the trace collector; nil uses the no-op implementation.
	// It emits workflow.run and workflow.step spans.
	Tracer core.Tracer

	runMu    sync.Mutex
	runLocks map[string]*runLock
}

// New constructs an orchestrator.
// store: the run state store; nil disables persistence (state is lost on
// restart — only suitable for single-process, single-use runs)
// returns: a usable orchestrator
func New(store RunStore) *Orchestrator {
	return &Orchestrator{
		agents:    make(map[string]*agent.Loop),
		workflows: make(map[string]*Workflow),
		store:     store,
		runLocks:  make(map[string]*runLock),
	}
}

// runLock is a per-run mutex entry.
//
// refs is a reference count governing the lifecycle: both acquisition and
// waiting count as references, and the entry is removed from the table
// only at zero; otherwise lock entries accumulate without bound as runs
// pile up, leaking indefinitely in long-lived processes.
type runLock struct {
	mu   sync.Mutex
	refs int
}

// lockRun acquires the per-run mutex.
//
// It serializes Run/Resume for the same run within a process; across
// processes, the storage layer must support conditional writes. Waiters
// increment refs while holding the table lock before blocking on
// entry.mu, guaranteeing that when the holder releases there is still an
// entry in the table to lock — a waiter can never block on a deleted
// entry.
// returns: the unlock function
func (o *Orchestrator) lockRun(runID string) func() {
	o.runMu.Lock()
	entry, ok := o.runLocks[runID]
	if !ok {
		entry = &runLock{}
		o.runLocks[runID] = entry
	}
	entry.refs++
	o.runMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		o.runMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(o.runLocks, runID)
		}
		o.runMu.Unlock()
	}
}

// RegisterAgent registers an agent loop.
// name: the unique agent name; AgentStep.Agent references it
func (o *Orchestrator) RegisterAgent(name string, loop *agent.Loop) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.agents[name] = loop
}

// RegisterWorkflow registers a workflow; duplicate names overwrite.
func (o *Orchestrator) RegisterWorkflow(wf *Workflow) error {
	if wf.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.workflows[wf.Name] = wf
	return nil
}

// Run starts one workflow execution.
// ctx: on cancellation the current step aborts; the state stays in the
// store and can be Resumed
// wfName: the workflow name
// input: the initial input, referenced by $input in step templates
// returns: the run state at completion or pause; ErrCheckpoint when
// paused at a checkpoint
func (o *Orchestrator) Run(ctx context.Context, wfName, input string) (*RunState, error) {
	ctx, span := o.tracer().StartSpan(ctx, "workflow.run", "workflow", wfName)
	defer span.End()

	wf, ok := o.workflow(wfName)
	if !ok {
		return nil, fmt.Errorf("workflow not registered: %s", wfName)
	}
	run := &RunState{
		ID:        fmt.Sprintf("run-%d-%d", time.Now().UnixMilli(), o.seq.Add(1)),
		Workflow:  wf.Name,
		Input:     input,
		Outputs:   make(map[string]string),
		Status:    StatusRunning,
		CreatedAt: time.Now(),
	}
	o.save(run)
	// Share the run lock with Resume: state hits disk before execution,
	// and within that window the client may already have the runID and
	// issue a Resume; without the lock, double execution means double LLM
	// cost
	unlock := o.lockRun(run.ID)
	defer unlock()
	return o.execute(ctx, run)
}

// Resume continues execution from a checkpoint or interruption.
//
// waiting: a manual checkpoint, humanInput becomes the next step's input;
// running: a run left behind by a crash/cancellation, re-run from the
// current step (at-least-once semantics). A fully completed execution
// only ever leaves done/failed, so running necessarily means interrupted.
// runID: the paused or interrupted run ID
// humanInput: the human input, serving as the checkpoint output for the
// next step's $prev
// returns: the run state after continuing execution
func (o *Orchestrator) Resume(ctx context.Context, runID, humanInput string) (*RunState, error) {
	// Serialize concurrent Run/Resume on the same run: check-mutate-execute
	// must be atomic, otherwise double execution means double LLM cost
	unlock := o.lockRun(runID)
	defer unlock()

	run, ok := o.store.Get(runID)
	if !ok {
		return nil, fmt.Errorf("run not found: %s", runID)
	}
	if run.Status != StatusWaiting && run.Status != StatusRunning {
		return nil, fmt.Errorf("run %s is %s, nothing to resume", runID, run.Status)
	}
	if _, ok := o.workflow(run.Workflow); !ok {
		return nil, fmt.Errorf("workflow no longer registered: %s", run.Workflow)
	}
	if run.Status == StatusWaiting {
		run.Prev = humanInput
		run.StepIdx++
	}
	run.Status = StatusRunning
	run.Err = ""
	o.save(run)
	return o.execute(ctx, run)
}

// Get queries the run state.
// returns: the run state; ok is false if it does not exist
func (o *Orchestrator) Get(runID string) (*RunState, bool) {
	return o.store.Get(runID)
}

// execute advances the state machine from the current StepIdx until
// completion, failure, or a checkpoint.
func (o *Orchestrator) execute(ctx context.Context, run *RunState) (*RunState, error) {
	wf, ok := o.workflow(run.Workflow)
	if !ok {
		return nil, fmt.Errorf("workflow not registered: %s", run.Workflow)
	}
	for run.StepIdx < len(wf.Steps) {
		if err := ctx.Err(); err != nil {
			run.Err = err.Error()
			o.save(run)
			return run, err
		}
		stepCtx, stepSpan := o.tracer().StartSpan(ctx, "workflow.step", "index", run.StepIdx)
		var stepErr error
		var paused bool
		switch s := wf.Steps[run.StepIdx].(type) {
		case AgentStep:
			stepErr = o.runAgentStep(stepCtx, run, s)
		case RouterStep:
			stepErr = o.runRouterStep(stepCtx, run, s)
		case SupervisorStep:
			stepErr = o.runSupervisorStep(stepCtx, run, s)
		case CheckpointStep:
			paused = true
		default:
			stepErr = fmt.Errorf("unknown step type at %d", run.StepIdx)
		}
		if stepErr != nil {
			stepSpan.RecordError(stepErr)
			stepSpan.End()
			if ctx.Err() != nil {
				// Caller cancellation is not a step failure: keep running
				// (interrupted state); marking it failed would seal off
				// Resume's continuation path
				run.Err = stepErr.Error()
				o.save(run)
				return run, stepErr
			}
			run.Status = StatusFailed
			run.Err = stepErr.Error()
			o.save(run)
			return run, stepErr
		}
		stepSpan.End()
		if paused {
			// Persist before returning, so the run can still be Resumed
			// after a process crash
			run.Status = StatusWaiting
			run.Err = ""
			o.save(run)
			return run, ErrCheckpoint
		}
	}
	run.Status = StatusDone
	run.Err = ""
	o.save(run)
	return run, nil
}

// tracer returns the configured tracer, degrading to the no-op
// implementation when nil.
// returns: the tracer
func (o *Orchestrator) tracer() core.Tracer {
	if o.Tracer != nil {
		return o.Tracer
	}
	return core.NoopTracer()
}

// runAgentStep executes a single agent step and records its output.
func (o *Orchestrator) runAgentStep(ctx context.Context, run *RunState, s AgentStep) error {
	o.mu.RLock()
	loop, ok := o.agents[s.Agent]
	o.mu.RUnlock()
	if !ok {
		return fmt.Errorf("agent not registered: %s", s.Agent)
	}

	input := resolveInput(s.Input, run)
	msg, usage, err := loop.Run(ctx, sessionID(run.ID, s), input)
	run.Usage.Add(usage)
	if err != nil {
		return fmt.Errorf("step %q: %w", stepName(s), err)
	}

	name := stepName(s)
	run.Outputs[name] = msg.Content
	run.Prev = msg.Content
	run.StepIdx++
	o.save(run)
	return nil
}

// stepName returns the step's reference name.
// returns: falls back to the agent name when Name is empty
func stepName(s AgentStep) string {
	if s.Name != "" {
		return s.Name
	}
	return s.Agent
}

// sessionID provides per-step session isolation; re-running a same-named
// step appends to rather than overwrites history.
// returns: the session ID formed by joining the runID and step name
func sessionID(runID string, s AgentStep) string {
	return runID + ":" + stepName(s)
}

// resolveInput resolves the input template.
//
// Only whole-string template matching is done: $input / $prev /
// $step.<name>.output — no substring interpolation, to avoid introducing
// escaping rules.
func resolveInput(tpl string, run *RunState) string {
	switch {
	case tpl == "$input":
		return run.Input
	case tpl == "$prev":
		return run.Prev
	case strings.HasPrefix(tpl, "$step.") && strings.HasSuffix(tpl, ".output"):
		name := strings.TrimSuffix(strings.TrimPrefix(tpl, "$step."), ".output")
		return run.Outputs[name]
	default:
		return tpl
	}
}

// save persists the run state.
//
// A persistence failure does not interrupt execution, but must raise an
// alarm: losing checkpoint state means no continuation after a crash, and
// silently swallowing it would delay discovery of the problem by hours.
func (o *Orchestrator) save(run *RunState) {
	run.UpdatedAt = time.Now()
	if o.store != nil {
		if err := o.store.Save(run); err != nil {
			slog.Warn("persist run state failed", "run", run.ID, "err", err)
		}
	}
}

// workflow looks up a workflow.
// returns: the workflow; ok is false if not registered
func (o *Orchestrator) workflow(name string) (*Workflow, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	wf, ok := o.workflows[name]
	return wf, ok
}
