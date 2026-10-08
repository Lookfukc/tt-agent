// Stateless mode: the caller supplies its own history; the loop
// carries it only transiently in-process and, when finished, hands
// back all messages added during this run for the caller to persist
// as it sees fit.
package agent

import (
	"context"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/internal/sessionlog"
)

// statelessSession is the internal session identifier for stateless
// mode.
//
// The SessionID in event callbacks and trace spans uses this
// placeholder; callers can distinguish by it.
const statelessSession = "stateless"

// ephemeralMemory is a single-session in-process memory, exclusive to
// stateless mode.
//
// It is initialized with the caller-provided history and lives only
// for the duration of this run; Recent reuses sessionlog.Split for
// budget truncation and tool_calls atomic-group pairing, so this
// assembly logic applies equally in stateless mode (one of the
// framework's core values).
type ephemeralMemory struct {
	mu sync.Mutex
	// seedN is the initial history length: Msgs[:seedN] is the
	// caller's history, Msgs[seedN:] is what this run added and will
	// hand back.
	seedN int
	log   sessionlog.Log
}

// newEphemeralMemory constructs from the caller's history.
//
// The input slice is read-only and not retained: the history is
// copied in, so later caller modifications do not affect the run.
func newEphemeralMemory(history []core.Message) *ephemeralMemory {
	e := &ephemeralMemory{seedN: len(history)}
	if len(history) > 0 {
		e.log.Add(sessionlog.Rough{}, time.Now(), history...)
	}
	return e
}

// Add appends messages.
func (e *ephemeralMemory) Add(_ context.Context, _ string, msgs ...core.Message) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.log.Add(sessionlog.Rough{}, time.Now(), msgs...)
	return nil
}

// Recent returns the most recent messages within budget.
//
// The budget comes from Config.TokenBudget; system messages are
// always kept, and over-window history is truncated oldest-to-newest
// by atomic group — the caller need not control history length
// itself.
func (e *ephemeralMemory) Recent(_ context.Context, _ string, budget int64) ([]core.Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	kept, _ := e.log.Split(budget)
	return kept, nil
}

// Clear has no cleanup semantics in stateless mode; the contents are
// simply discarded.
func (e *ephemeralMemory) Clear(_ context.Context, _ string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.log = sessionlog.Log{}
	e.seedN = 0
	return nil
}

// newMessages hands back all messages added during this run
// (including this turn's user input).
//
// The order is production order: user → assistant(tool_calls) →
// tool... → final assistant. The caller persists them as-is; passing
// them as history next turn completes the round. Returns a copy —
// internal state is not retained by outside holders.
func (e *ephemeralMemory) newMessages() []core.Message {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]core.Message, len(e.log.Msgs)-e.seedN)
	copy(out, e.log.Msgs[e.seedN:])
	return out
}

// RunResult is the result of a stateless run.
type RunResult struct {
	// Message is the final assistant answer.
	Message core.Message
	// NewMessages is every message added during this run (including
	// the input); after the caller persists them, passing them back
	// as history next turn continues the conversation.
	NewMessages []core.Message
	// Usage is the cumulative usage for the whole run.
	Usage core.Usage
}

// RunWithHistory executes one full conversation turn statelessly.
//
// The caller owns and stores the history (its own database/files/
// anywhere) and passes the full history each turn; the framework
// internally handles budget truncation, tool_calls atomic-group
// pairing, and dangling-call repair, and hands the added messages
// back to the caller for persistence when the run finishes.
// It shares the same loop body as Run: tool calling, iteration
// circuit-breaking, event streaming, and tracing behave identically;
// the SessionID in event callbacks is always "stateless".
// ctx: cancellation interrupts the current LLM call or tool execution.
// history: the caller's full stored history, in original order; may
// be empty to start a brand-new conversation.
// input: the user input.
// returns: the run result; NewMessages always carries the messages
// already produced — on error the caller may choose to persist the
// partial result or abandon it entirely.
func (l *Loop) RunWithHistory(ctx context.Context, history []core.Message, input string) (RunResult, error) {
	em := newEphemeralMemory(history)
	stateless := *l // shallow copy: llm/tools/cfg shared, only the memory swapped
	stateless.memory = em

	ctx, span := l.tracer().StartSpan(ctx, "agent.run", "mode", "stateless", "model", l.cfg.Model)
	defer span.End()

	msg, usage, err := stateless.run(ctx, statelessSession, input)
	return RunResult{Message: msg, NewMessages: em.newMessages(), Usage: usage}, err
}
