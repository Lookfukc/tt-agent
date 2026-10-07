// Package agent 提供 ReAct 模式的 Agent 执行循环
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/tools"
)

// ErrMaxIterations 迭代熔断错误
var ErrMaxIterations = errors.New("agent loop: max iterations exceeded")

// LoopEvent 循环过程事件，供外部观测与流式转发
type LoopEvent struct {
	// SessionID 事件归属会话；同一 Loop 并发服务多会话时
	// 回调来自多个 goroutine，靠它区分路由
	SessionID string
	Iter      int
	Type      LoopEventType
	Text      string // 文本增量或工具名
	Reasoning string
	Call      *core.ToolCall
	Usage     *core.Usage // 仅 EventDone 携带累计用量
	Err       error
}

// LoopEventType 事件类别
type LoopEventType int

const (
	EventIterStart LoopEventType = iota
	EventDeltaText
	EventDeltaReasoning
	EventToolCall
	EventToolResult
	EventDone
	EventError
)

// Config 循环配置
type Config struct {
	SystemPrompt  string
	Model         string
	Temperature   *float64
	Thinking      *core.ThinkingConfig
	MaxIterations int
	// TokenBudget 单次 LLM 调用的输入 token 预算，超限触发记忆截断
	TokenBudget int64
	// OnEvent 过程回调，nil 表示不观测；回调阻塞会拖慢整个循环。
	// 同一 Loop 并发服务多个会话时回调会来自多个 goroutine，
	// 回调方必须并发安全，并按 LoopEvent.SessionID 路由
	OnEvent func(LoopEvent)

	// Tracer 链路追踪，nil 用空实现；产生 run/iter/llm/tool 四级 span
	Tracer core.Tracer
}

// Loop ReAct 循环：推理 → 工具调用 → 观察 → 再推理，直到产出最终回答
type Loop struct {
	llm    core.LLM
	tools  *tools.Registry
	memory core.Memory
	cfg    Config
}

// NewLoop 构造循环实例，无状态可复用
// llm: 对话能力实现
// toolReg: 工具注册表，可为 nil 表示纯对话
// mem: 会话记忆，按 sessionID 隔离
// returns: 可用的循环实例
func NewLoop(llm core.LLM, toolReg *tools.Registry, mem core.Memory, cfg Config) *Loop {
	if cfg.MaxIterations <= 0 {
		// 没有熔断上限的循环一次失控就能烧穿预算
		cfg.MaxIterations = 16
	}
	if cfg.TokenBudget <= 0 {
		cfg.TokenBudget = 32_000
	}
	return &Loop{llm: llm, tools: toolReg, memory: mem, cfg: cfg}
}

// Run 执行一轮完整对话
// ctx: 取消时中断当前 LLM 调用或工具执行
// sessionID: 会话标识，历史与新消息都落在此会话
// input: 用户输入
// returns: 最终 assistant 消息、全轮累计用量、终止性错误
func (l *Loop) Run(ctx context.Context, sessionID, input string) (core.Message, core.Usage, error) {
	ctx, span := l.tracer().StartSpan(ctx, "agent.run", "model", l.cfg.Model, "session", sessionID)
	defer span.End()

	if err := l.memory.Add(ctx, sessionID, core.Message{
		Role: core.RoleUser, Content: input,
	}); err != nil {
		span.RecordError(err)
		return core.Message{}, core.Usage{}, err
	}

	var total core.Usage
	for iter := 1; iter <= l.cfg.MaxIterations; iter++ {
		if err := ctx.Err(); err != nil {
			return core.Message{}, total, err
		}
		iterCtx, iterSpan := l.tracer().StartSpan(ctx, "agent.iter", "iter", iter)
		result, err := l.iterate(iterCtx, sessionID, iter)
		iterSpan.End()
		// 先累加再判错：失败轮的已产生用量也要计入
		total.Add(result.Usage)
		if err != nil {
			span.RecordError(err)
			return core.Message{}, total, err
		}
		if result.Final {
			l.emit(LoopEvent{SessionID: sessionID, Type: EventDone, Iter: iter, Text: result.Message.Content, Usage: &total})
			return result.Message, total, nil
		}
	}
	err := fmt.Errorf("%w after %d iterations", ErrMaxIterations, l.cfg.MaxIterations)
	l.emit(LoopEvent{SessionID: sessionID, Type: EventError, Err: err})
	return core.Message{}, total, err
}

// iterResult 单轮迭代产物
type iterResult struct {
	Message core.Message
	Final   bool
	// Usage 本轮 LLM 用量，失败轮也携带已产生部分
	Usage core.Usage
}

// iterate 执行一轮推理与工具调用
// returns: 本轮消息与是否收敛
func (l *Loop) iterate(ctx context.Context, sessionID string, iter int) (iterResult, error) {
	l.emit(LoopEvent{SessionID: sessionID, Type: EventIterStart, Iter: iter})

	history, err := l.memory.Recent(ctx, sessionID, l.cfg.TokenBudget)
	if err != nil {
		l.emit(LoopEvent{SessionID: sessionID, Type: EventError, Iter: iter, Err: err})
		return iterResult{}, err
	}

	msg, usage, err := l.callLLM(ctx, history, sessionID, iter)
	if err != nil {
		l.emit(LoopEvent{SessionID: sessionID, Type: EventError, Iter: iter, Err: err})
		return iterResult{Usage: usage}, err
	}

	if err := l.memory.Add(ctx, sessionID, msg); err != nil {
		// 记忆层失败也走事件通道，调用方不能只靠返回值观测
		l.emit(LoopEvent{SessionID: sessionID, Type: EventError, Iter: iter, Err: err})
		return iterResult{Usage: usage}, err
	}

	if len(msg.ToolCalls) == 0 {
		return iterResult{Message: msg, Final: true, Usage: usage}, nil
	}

	results := l.execTools(ctx, sessionID, iter, msg.ToolCalls)
	for _, r := range results {
		content := r.result.Render()
		if r.err != nil {
			// 工具失败不中断循环，把错误回传给模型让其自行调整
			content = fmt.Sprintf("tool error: %v", r.err)
		}
		if err := l.memory.Add(ctx, sessionID, core.Message{
			Role: core.RoleTool, Content: content, ToolCallID: r.call.ID,
		}); err != nil {
			// 本轮用量已产生，失败路径也要带回给 Run 累计
			l.emit(LoopEvent{SessionID: sessionID, Type: EventError, Iter: iter, Err: err})
			return iterResult{Usage: usage}, err
		}
	}
	return iterResult{Message: msg, Final: false, Usage: usage}, nil
}

// tracer 取配置的追踪器，nil 退化为空实现
// returns: 追踪器
func (l *Loop) tracer() core.Tracer {
	if l.cfg.Tracer != nil {
		return l.cfg.Tracer
	}
	return core.NoopTracer()
}

// sanitizeHistory 规范化发往提供商的历史，修复悬空工具调用
//
// 记忆保存事实，仅在组装请求时修补：崩溃或落盘失败会让
// assistant(tool_calls) 的结果永久缺失，之后每轮请求都带着
// 悬空调用，Anthropic/OpenAI 一律 400，会话就此卡死。规则：
//   - assistant(tool_calls) 后紧跟的连续 tool 消息中，无对应
//     调用 ID 的（孤儿）与重复 ID 的直接丢弃；
//   - 没等到结果的调用 ID，在组尾合成错误结果补齐；
//   - 不紧跟 assistant(tool_calls) 的 tool 消息即孤儿，丢弃；
//   - 其余消息原样透传
//
// 修补结果不落记忆，内层存储保持真相
// returns: 可安全发给提供商的消息切片，不修改入参
func sanitizeHistory(history []core.Message) []core.Message {
	out := make([]core.Message, 0, len(history))
	for i := 0; i < len(history); i++ {
		m := history[i]
		if m.Role != core.RoleAssistant || len(m.ToolCalls) == 0 {
			if m.Role == core.RoleTool {
				// 前面没有携带调用的 assistant：孤儿结果，丢弃
				continue
			}
			out = append(out, m)
			continue
		}
		out = append(out, m)
		answered := make(map[string]bool, len(m.ToolCalls))
		for i+1 < len(history) && history[i+1].Role == core.RoleTool {
			r := history[i+1]
			if !answered[r.ToolCallID] && containsCallID(m.ToolCalls, r.ToolCallID) {
				answered[r.ToolCallID] = true
				out = append(out, r)
			}
			i++
		}
		for _, tc := range m.ToolCalls {
			if !answered[tc.ID] {
				out = append(out, core.Message{
					Role: core.RoleTool, ToolCallID: tc.ID,
					Content: "tool error: interrupted before execution",
				})
			}
		}
	}
	return out
}

// containsCallID 判断调用列表是否含指定 ID
// returns: true 表示存在
func containsCallID(calls []core.ToolCall, id string) bool {
	for _, c := range calls {
		if c.ID == id {
			return true
		}
	}
	return false
}

// callLLM 发起一次流式调用并聚合结果
//
// 统一走流式路径，非流式模型由适配器内部降级，循环只维护一条通路
func (l *Loop) callLLM(ctx context.Context, history []core.Message, sessionID string, iter int) (core.Message, core.Usage, error) {
	ctx, span := l.tracer().StartSpan(ctx, "llm.stream", "model", l.cfg.Model, "iter", iter)
	defer span.End()

	req := core.ChatRequest{
		Model: l.cfg.Model,
		// 请求侧规范化：悬空的 assistant(tool_calls) 补合成结果，
		// 孤儿 tool 消息剔除；记忆本身不动
		Messages: sanitizeHistory(history),
		Thinking: l.cfg.Thinking,
	}
	if l.cfg.Temperature != nil {
		req.Temperature = l.cfg.Temperature
	}
	if l.tools != nil {
		if specs := l.tools.Specs(); len(specs) > 0 {
			req.Tools = specs
		}
	}

	events, err := l.llm.ChatStream(ctx, req)
	if err != nil {
		span.RecordError(err)
		return core.Message{}, core.Usage{}, err
	}
	acc := core.NewStreamAccumulator()
	for e := range events {
		if e.Type == core.StreamError {
			span.RecordError(e.Err)
			return acc.Message(), acc.Usage(), e.Err
		}
		acc.Feed(e)
		switch e.Type {
		case core.StreamDeltaText:
			l.emit(LoopEvent{SessionID: sessionID, Type: EventDeltaText, Text: e.Text, Iter: iter})
		case core.StreamDeltaReasoning:
			l.emit(LoopEvent{SessionID: sessionID, Type: EventDeltaReasoning, Reasoning: e.Reasoning, Iter: iter})
		}
	}
	usage := acc.Usage()
	span.SetAttr("input_tokens", usage.InputTokens)
	span.SetAttr("output_tokens", usage.OutputTokens)
	return acc.Message(), usage, nil
}

// toolOutcome 单个工具调用的执行结果
type toolOutcome struct {
	call   core.ToolCall
	result core.ToolResult
	err    error
}

// execTools 并行执行一轮的全部工具调用
//
// 事件发射集中在发起 Run 的 goroutine 内；同一 Loop 并发跑
// 多个会话时回调仍会来自多个 goroutine，回调方按 SessionID 路由；
// 结果按调用顺序返回，保证落记忆的顺序与模型请求一致
func (l *Loop) execTools(ctx context.Context, sessionID string, iter int, calls []core.ToolCall) []toolOutcome {
	outcomes := make([]toolOutcome, len(calls))
	var wg sync.WaitGroup
	for i, call := range calls {
		l.emit(LoopEvent{SessionID: sessionID, Type: EventToolCall, Iter: iter, Call: &call})
		wg.Add(1)
		go func(i int, call core.ToolCall) {
			defer wg.Done()
			// 工具实现在第三方代码里，panic 必须兜住转为错误，
			// 否则模型构造的输入可以直接打崩整个进程
			defer func() {
				if r := recover(); r != nil {
					outcomes[i] = toolOutcome{
						call: call,
						err:  fmt.Errorf("tool %s panicked: %v", call.Name, r),
					}
				}
			}()
			toolCtx, span := l.tracer().StartSpan(ctx, "tool.exec", "tool", call.Name)
			result, err := l.execTool(toolCtx, call)
			if err != nil {
				span.RecordError(err)
			}
			span.End()
			outcomes[i] = toolOutcome{call: call, result: result, err: err}
		}(i, call)
	}
	wg.Wait()
	for _, o := range outcomes {
		l.emit(LoopEvent{SessionID: sessionID, Type: EventToolResult, Iter: iter, Call: &o.call, Err: o.err})
	}
	return outcomes
}

// execTool 执行单个工具调用，不发射事件
// returns: 工具结果；工具不存在或执行失败时返回错误
func (l *Loop) execTool(ctx context.Context, call core.ToolCall) (core.ToolResult, error) {
	if l.tools == nil {
		return core.ToolResult{}, fmt.Errorf("no tool registry attached")
	}
	tool, err := l.tools.MustGet(call.Name)
	if err != nil {
		return core.ToolResult{}, err
	}
	return tool.Execute(ctx, json.RawMessage(call.Arguments))
}

// emit 派发过程事件
func (l *Loop) emit(e LoopEvent) {
	if l.cfg.OnEvent != nil {
		l.cfg.OnEvent(e)
	}
}
