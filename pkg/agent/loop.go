// Package agent provides a ReAct-style agent execution loop.
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

// ErrMaxIterations is the iteration circuit-breaker error.
var ErrMaxIterations = errors.New("agent loop: max iterations exceeded")

// LoopEvent is a loop-progress event for external observation and
// streaming forwarding.
type LoopEvent struct {
	// SessionID is the session the event belongs to; when one Loop
	// concurrently serves multiple sessions the callback comes from
	// multiple goroutines — this field distinguishes routing.
	SessionID string
	Iter      int
	Type      LoopEventType
	Text      string // text delta or tool name
	Reasoning string
	Call      *core.ToolCall
	Usage     *core.Usage // cumulative usage, carried only by EventDone
	Err       error
}

// LoopEventType is the event category.
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

// Config is the loop configuration.
type Config struct {
	SystemPrompt  string
	Model         string
	Temperature   *float64
	Thinking      *core.ThinkingConfig
	MaxIterations int
	// TokenBudget is the input token budget per LLM call; exceeding it
	// triggers memory truncation.
	TokenBudget int64
	// OnEvent is the progress callback; nil means no observation. A
	// blocking callback slows down the whole loop. When one Loop
	// concurrently serves multiple sessions the callback comes from
	// multiple goroutines: the callback owner must be concurrency-safe
	// and route by LoopEvent.SessionID.
	OnEvent func(LoopEvent)

	// Tracer is the trace sink; nil uses the no-op implementation.
	// Produces run/iter/llm/tool four-level spans.
	Tracer core.Tracer
}

// Loop is the ReAct loop: reason → tool call → observe → reason
// again, until a final answer is produced.
type Loop struct {
	llm    core.LLM
	tools  *tools.Registry
	memory core.Memory
	cfg    Config
}

// NewLoop constructs a loop instance; stateless and reusable.
// llm: the chat capability implementation.
// toolReg: the tool registry; may be nil for pure conversation.
// mem: session memory, isolated by sessionID; only stateless mode
// (RunWithHistory) may pass nil, in which case Run returns an error.
// returns: a usable loop instance.
func NewLoop(llm core.LLM, toolReg *tools.Registry, mem core.Memory, cfg Config) *Loop {
	if cfg.MaxIterations <= 0 {
		// A loop without a circuit-breaker cap can burn through the
		// budget with a single runaway.
		cfg.MaxIterations = 16
	}
	if cfg.TokenBudget <= 0 {
		cfg.TokenBudget = 32_000
	}
	return &Loop{llm: llm, tools: toolReg, memory: mem, cfg: cfg}
}

// Run executes one full conversation turn (stateful mode).
// ctx: cancellation interrupts the current LLM call or tool execution.
// sessionID: session identifier; history and new messages both land
// in this session.
// input: the user input.
// returns: the final assistant message, cumulative usage for the
// whole run, and a terminal error.
func (l *Loop) Run(ctx context.Context, sessionID, input string) (core.Message, core.Usage, error) {
	ctx, span := l.tracer().StartSpan(ctx, "agent.run", "model", l.cfg.Model, "session", sessionID)
	defer span.End()
	return l.run(ctx, sessionID, input)
}

// run is the loop body, shared by Run and RunWithHistory.
//
// The memory source differs (session storage vs one-shot carrier),
// but the loop logic is identical.
func (l *Loop) run(ctx context.Context, sessionID, input string) (core.Message, core.Usage, error) {
	if l.memory == nil {
		return core.Message{}, core.Usage{}, fmt.Errorf("agent loop: no memory attached, use RunWithHistory for stateless mode")
	}
	if err := l.memory.Add(ctx, sessionID, core.Message{
		Role: core.RoleUser, Content: input,
	}); err != nil {
		return core.Message{}, core.Usage{}, err
	}

	var total core.Usage
	for iter := 1; iter <= l.cfg.MaxIterations; iter++ {
		if err := ctx.Err(); err != nil {
			return core.Message{}, total, err
		}
		iterCtx, iterSpan := l.tracer().StartSpan(ctx, "agent.iter", "iter", iter)
		result, err := l.iterate(iterCtx, sessionID, iter)
		if err != nil {
			iterSpan.RecordError(err)
		}
		iterSpan.End()
		// Accumulate before checking the error: a failed iteration's
		// already-incurred usage must still be counted.
		total.Add(result.Usage)
		if err != nil {
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

// iterResult is the product of a single iteration.
type iterResult struct {
	Message core.Message
	Final   bool
	// Usage is this iteration's LLM usage; failed iterations still
	// carry the already-incurred part.
	Usage core.Usage
}

// iterate performs one round of reasoning and tool calls.
// returns: this iteration's message and whether it converged.
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
		// Memory-layer failures also go through the event channel;
		// the caller must not rely on the return value alone to
		// observe them.
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
			// A tool failure does not break the loop; the error is fed
			// back to the model so it can adjust on its own.
			content = fmt.Sprintf("tool error: %v", r.err)
		}
		if err := l.memory.Add(ctx, sessionID, core.Message{
			Role: core.RoleTool, Content: content, ToolCallID: r.call.ID,
		}); err != nil {
			// This iteration's usage was already incurred; the failure
			// path must still carry it back for Run to accumulate.
			l.emit(LoopEvent{SessionID: sessionID, Type: EventError, Iter: iter, Err: err})
			return iterResult{Usage: usage}, err
		}
	}
	return iterResult{Message: msg, Final: false, Usage: usage}, nil
}

// tracer returns the configured tracer; nil degrades to the no-op
// implementation.
// returns: the tracer.
func (l *Loop) tracer() core.Tracer {
	if l.cfg.Tracer != nil {
		return l.cfg.Tracer
	}
	return core.NoopTracer()
}

// sanitizeHistory normalizes the history sent to the provider,
// repairing dangling tool calls.
//
// Memory stores facts; repairs happen only when assembling the
// request: a crash or failed disk write can leave an
// assistant(tool_calls) permanently missing its results, after which
// every request carries the dangling call and Anthropic/OpenAI return
// 400, wedging the session. Rules:
//   - among the consecutive tool messages immediately following an
//     assistant(tool_calls), those without a matching call ID
//     (orphans) and those with duplicate IDs are dropped;
//   - call IDs that never received a result get a synthesized error
//     result appended at the end of the group;
//   - tool messages not immediately following an
//     assistant(tool_calls) are orphans and dropped;
//   - all other messages pass through unchanged
//
// The repaired result is not written back to memory; the inner
// storage keeps the truth.
// returns: a message slice safe to send to the provider; the input is
// not modified.
func sanitizeHistory(history []core.Message) []core.Message {
	out := make([]core.Message, 0, len(history))
	for i := 0; i < len(history); i++ {
		m := history[i]
		if m.Role != core.RoleAssistant || len(m.ToolCalls) == 0 {
			if m.Role == core.RoleTool {
				// No preceding assistant carrying calls: orphan
				// result, drop it.
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

// containsCallID reports whether the call list contains the given ID.
// returns: true if present.
func containsCallID(calls []core.ToolCall, id string) bool {
	for _, c := range calls {
		if c.ID == id {
			return true
		}
	}
	return false
}

// callLLM issues one streaming call and aggregates the result.
//
// Everything goes through the streaming path; non-streaming models
// degrade inside the adapter, so the loop maintains a single path.
func (l *Loop) callLLM(ctx context.Context, history []core.Message, sessionID string, iter int) (core.Message, core.Usage, error) {
	ctx, span := l.tracer().StartSpan(ctx, "llm.stream", "model", l.cfg.Model, "iter", iter)
	defer span.End()

	messages := sanitizeHistory(history)
	// The system prompt is configuration, not history: it is injected
	// at request-assembly time on every iteration rather than written
	// into memory. Idempotent across iterations, identical for the
	// stateful and stateless paths, and never persisted beside user
	// data. (This was a real bug found by the demo consumer: the field
	// was plumbed through every layer but never reached the request.)
	if l.cfg.SystemPrompt != "" {
		messages = append([]core.Message{{
			Role: core.RoleSystem, Content: l.cfg.SystemPrompt,
		}}, messages...)
	}
	req := core.ChatRequest{
		Model: l.cfg.Model,
		// Request-side normalization: dangling assistant(tool_calls)
		// get synthesized results, orphan tool messages are removed;
		// memory itself is untouched.
		Messages: messages,
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

// toolOutcome is the execution result of a single tool call.
type toolOutcome struct {
	call   core.ToolCall
	result core.ToolResult
	err    error
}

// execTools executes all tool calls of one iteration in parallel.
//
// Event emission is concentrated in the goroutine that started Run;
// when one Loop concurrently runs multiple sessions the callback still
// comes from multiple goroutines — callback owners route by SessionID.
// Results are returned in call order, keeping the order written to
// memory consistent with the model's request.
func (l *Loop) execTools(ctx context.Context, sessionID string, iter int, calls []core.ToolCall) []toolOutcome {
	outcomes := make([]toolOutcome, len(calls))
	var wg sync.WaitGroup
	for i, call := range calls {
		l.emit(LoopEvent{SessionID: sessionID, Type: EventToolCall, Iter: iter, Call: &call})
		wg.Add(1)
		go func(i int, call core.ToolCall) {
			defer wg.Done()
			// Tool implementations live in third-party code; panics
			// must be caught and converted to errors, otherwise input
			// constructed by the model could crash the whole process
			// directly.
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

// execTool executes a single tool call without emitting events.
// returns: the tool result; an error if the tool is missing or its
// execution failed.
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

// emit dispatches a progress event.
func (l *Loop) emit(e LoopEvent) {
	if l.cfg.OnEvent != nil {
		l.cfg.OnEvent(e)
	}
}
