// Package orchestrator 提供多 Agent 工作流编排与断点续跑能力
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

// ErrCheckpoint 工作流停在人工检查点，等待 Resume
var ErrCheckpoint = errors.New("workflow paused at checkpoint")

// Status 运行状态
type Status string

const (
	StatusRunning Status = "running"
	StatusWaiting Status = "waiting" // 停在检查点
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

// Step 工作流步骤，封闭的 sum type，只允许 Agent 与检查点两种
type Step interface{ stepKind() }

// AgentStep 执行一个已注册 Agent
type AgentStep struct {
	// Name 步骤名，供后续步骤以 $step.<name>.output 引用输出，空则取 Agent 名
	Name string
	// Agent 已注册的 Agent 名
	Agent string
	// Input 输入模板：$input 为工作流初始输入，$prev 为上一步输出，
	// $step.<name>.output 引用指定步骤输出；非模板字符串原样传入
	Input string
}

// stepKind 实现 Step 接口
func (AgentStep) stepKind() {}

// CheckpointStep 人工检查点，执行到此暂停并持久化，等待人工 Resume
type CheckpointStep struct {
	// Name 检查点名
	Name string
	// Prompt 给审核人的提示说明
	Prompt string
}

// stepKind 实现 Step 接口
func (CheckpointStep) stepKind() {}

// Workflow 顺序工作流
//
// V1 只做线性执行，Router/Supervisor 等分发模式待状态模型稳定后再加
type Workflow struct {
	Name  string
	Steps []Step
}

// RunState 一次工作流运行的持久化状态
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

// Orchestrator Agent 与工作流注册中心，Run/Resume 驱动状态机
type Orchestrator struct {
	mu        sync.RWMutex
	agents    map[string]*agent.Loop
	workflows map[string]*Workflow
	store     RunStore
	seq       atomic.Int64

	// Tracer 链路追踪，nil 用空实现；产生 workflow.run 与 workflow.step span
	Tracer core.Tracer

	runMu    sync.Mutex
	runLocks map[string]*runLock
}

// New 构造编排器
// store: 运行状态存储，nil 则不持久化（重启丢状态，仅单次进程内使用）
// returns: 可用的编排器
func New(store RunStore) *Orchestrator {
	return &Orchestrator{
		agents:    make(map[string]*agent.Loop),
		workflows: make(map[string]*Workflow),
		store:     store,
		runLocks:  make(map[string]*runLock),
	}
}

// runLock 运行级互斥条目
//
// refs 引用计数管生命周期：获取与等待都算引用，归零才从表里删除，
// 否则锁条目随运行数无限累积，长生命周期进程持续泄漏
type runLock struct {
	mu   sync.Mutex
	refs int
}

// lockRun 取运行级互斥锁
//
// 进程内串行化同 run 的 Run/Resume；跨进程需存储层支持条件写。
// 等待者在表锁内先 refs++ 再阻塞于 entry.mu，保证持有者释放时
// 表里仍有条目可锁，不会出现等待者对着已删除条目空等
// returns: 解锁函数
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

// RegisterAgent 注册 Agent 循环
// name: Agent 唯一名，AgentStep.Agent 引用此名
func (o *Orchestrator) RegisterAgent(name string, loop *agent.Loop) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.agents[name] = loop
}

// RegisterWorkflow 注册工作流，重名覆盖
func (o *Orchestrator) RegisterWorkflow(wf *Workflow) error {
	if wf.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.workflows[wf.Name] = wf
	return nil
}

// Run 启动一次工作流执行
// ctx: 取消时当前步骤中断，状态留在存储中可 Resume
// wfName: 工作流名
// input: 初始输入，步骤模板中的 $input 引用此值
// returns: 最终或暂停时的运行状态；停在检查点时返回 ErrCheckpoint
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
	// 与 Resume 共用运行锁：状态先落盘再执行，窗口期内客户端可能
	// 已拿到 runID 发起 Resume；不锁会双份执行双倍 LLM 花费
	unlock := o.lockRun(run.ID)
	defer unlock()
	return o.execute(ctx, run)
}

// Resume 从检查点或中断处继续执行
//
// waiting：人工检查点，humanInput 作为下一步输入；
// running：进程崩溃/取消遗留的运行，从当前步骤重跑（至少一次语义）。
// 完整执行结束只会留下 done/failed，running 必属中断
// runID: 暂停或中断的运行 ID
// humanInput: 人工输入，作为检查点输出供下一步 $prev 引用
// returns: 继续执行后的运行状态
func (o *Orchestrator) Resume(ctx context.Context, runID, humanInput string) (*RunState, error) {
	// 同 run 并发 Run/Resume 串行化：检查-变更-执行必须原子，
	// 否则双份执行双倍 LLM 花费
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

// Get 查询运行状态
// returns: 运行状态；ok 为 false 表示不存在
func (o *Orchestrator) Get(runID string) (*RunState, bool) {
	return o.store.Get(runID)
}

// execute 从当前 StepIdx 推进状态机直到完成、失败或检查点
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
				// 调用方取消不是步骤失败：保持 running（中断态），
				// 标成 failed 会关死 Resume 的续跑路径
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
			// 先落盘再返回，进程崩溃后仍可 Resume
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

// tracer 取配置的追踪器，nil 退化为空实现
// returns: 追踪器
func (o *Orchestrator) tracer() core.Tracer {
	if o.Tracer != nil {
		return o.Tracer
	}
	return core.NoopTracer()
}

// runAgentStep 执行单个 Agent 步骤并记录输出
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

// stepName 取步骤引用名
// returns: Name 为空时退回 Agent 名
func stepName(s AgentStep) string {
	if s.Name != "" {
		return s.Name
	}
	return s.Agent
}

// sessionID 步骤级会话隔离，同名步骤重跑时追加而非覆盖历史
// returns: runID 与步骤名拼接的会话 ID
func sessionID(runID string, s AgentStep) string {
	return runID + ":" + stepName(s)
}

// resolveInput 解析输入模板
//
// 只做整串模板匹配：$input / $prev / $step.<name>.output，
// 不做子串插值，避免引入转义规则
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

// save 持久化运行状态
//
// 落盘失败不中断执行，但必须告警：checkpoint 状态丢失意味着
// 崩溃后无法续跑，静默吞掉会让问题迟到数小时才暴露
func (o *Orchestrator) save(run *RunState) {
	run.UpdatedAt = time.Now()
	if o.store != nil {
		if err := o.store.Save(run); err != nil {
			slog.Warn("persist run state failed", "run", run.ID, "err", err)
		}
	}
}

// workflow 查找工作流
// returns: 工作流；ok 为 false 表示未注册
func (o *Orchestrator) workflow(name string) (*Workflow, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	wf, ok := o.workflows[name]
	return wf, ok
}
