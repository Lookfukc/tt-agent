// Package sessionlog provides the shared implementation of a
// single-session message log.
//
// Internal package: for reuse only inside this module's pkg/ (memory
// and memorytest); it is not part of the public API and its
// signatures may change as internal needs require.
package sessionlog

import (
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// Log is a single-session message log: the messages themselves plus
// aligned token estimate caches plus push times.
//
// Tokens are computed and cached once at append time; packing a
// budget only does table lookups and sums, never recounting the full
// history message by message.
type Log struct {
	Msgs []core.Message
	Toks []int64     // aligned with Msgs: per-message token estimate
	Tss  []time.Time // aligned with Msgs: push time, carried on disk writes and rewrites
}

// Add appends messages and fills in the caches.
//
// Each message goes through the counter individually then sums: the
// per-message floor (the estimate is never below the message count)
// applies message by message, so the total only errs high, never low —
// budget truncation prefers under over over.
// c: token estimator.
// now: push time for this batch of messages.
// msgs: messages to append.
func (l *Log) Add(c core.TokenCounter, now time.Time, msgs ...core.Message) {
	for _, m := range msgs {
		l.Msgs = append(l.Msgs, m)
		l.Toks = append(l.Toks, c.Count([]core.Message{m}))
		l.Tss = append(l.Tss, now)
	}
}

// Split returns the most recent history within budget.
//
// System messages are always kept in front and never dropped; the
// rest are packed from newest to oldest by atomic group. An
// assistant(tool_calls) and its subsequent tool messages enter and
// leave together — packing message by message could, when "the tool
// result fits but the parent message does not", produce an orphaned
// tool message, and Anthropic/OpenAI both return 400 for ownerless
// tool messages, permanently breaking the session.
// returns: kept messages in chronological order, and dropped
// (truncated) messages in chronological order.
func (l *Log) Split(budget int64) (kept, dropped []core.Message) {
	var system []core.Message
	var sysToks int64
	var restIdx []int
	for i, m := range l.Msgs {
		if m.Role == core.RoleSystem {
			system = append(system, m)
			sysToks += l.Toks[i]
		} else {
			restIdx = append(restIdx, i)
		}
	}

	// Slice into atomic groups (at index granularity): tool messages
	// are attached to their nearest preceding assistant(tool_calls).
	type msgGroup struct{ idx []int }
	var groups []msgGroup
	for i := 0; i < len(restIdx); {
		g := msgGroup{idx: []int{restIdx[i]}}
		i++
		if len(l.Msgs[g.idx[0]].ToolCalls) > 0 {
			for i < len(restIdx) && l.Msgs[restIdx[i]].Role == core.RoleTool {
				g.idx = append(g.idx, restIdx[i])
				i++
			}
		}
		groups = append(groups, g)
	}

	used := sysToks
	keepFrom := len(groups)
	for i := len(groups) - 1; i >= 0; i-- {
		var cost int64
		for _, idx := range groups[i].idx {
			cost += l.Toks[idx]
		}
		if used+cost > budget {
			break
		}
		used += cost
		keepFrom = i
	}

	for _, g := range groups[keepFrom:] {
		for _, idx := range g.idx {
			kept = append(kept, l.Msgs[idx])
		}
	}
	for _, g := range groups[:keepFrom] {
		for _, idx := range g.idx {
			dropped = append(dropped, l.Msgs[idx])
		}
	}
	if len(kept) == 0 && len(groups) > 0 {
		// Extreme budget where not even one group fits: keep the
		// newest group even over budget — an empty history is less
		// recoverable than an oversized window.
		for _, idx := range groups[len(groups)-1].idx {
			kept = append(kept, l.Msgs[idx])
		}
		dropped = dropped[:len(dropped)-len(kept)]
	}
	return append(system, kept...), dropped
}

// Trim physically discards the oldest n non-system messages.
//
// System messages do not count and are never deleted, strictly
// aligned with Split's drop-side semantics: what Split drops is
// exactly a prefix of the oldest non-system messages (dropped as
// whole atomic groups, never breaking an assistant(tool_calls)+tool
// pairing), so a Split followed by Trim is safe.
func (l *Log) Trim(n int) {
	if n <= 0 {
		return
	}
	msgs := make([]core.Message, 0, len(l.Msgs))
	toks := make([]int64, 0, len(l.Toks))
	tss := make([]time.Time, 0, len(l.Tss))
	for i, m := range l.Msgs {
		if n > 0 && m.Role != core.RoleSystem {
			n--
			continue
		}
		msgs = append(msgs, m)
		toks = append(toks, l.Toks[i])
		tss = append(tss, l.Tss[i])
	}
	l.Msgs, l.Toks, l.Tss = msgs, toks, tss
}

// Count returns the number of non-system messages, i.e. the range
// Trim can act on.
// returns: the non-system message count.
func (l *Log) Count() int {
	n := 0
	for _, m := range l.Msgs {
		if m.Role != core.RoleSystem {
			n++
		}
	}
	return n
}

// roughDivisor is the conservative estimation divisor.
//
// Chinese is roughly 1.5 characters/token, English roughly 4; the
// smaller divisor keeps the estimate on the high side, so budget
// truncation prefers under over over.
const roughDivisor = 2

// Rough is the built-in character-level rough token counter.
//
// Exact counting depends on provider tokenizers; within the framework
// only a conservative estimate is made.
type Rough struct{}

// Count roughly estimates tokens from character count.
// returns: the estimate, never less than the message count.
func (Rough) Count(msgs []core.Message) int64 {
	var chars int64
	for _, m := range msgs {
		chars += int64(len(m.Content) + len(m.Reasoning))
		for _, tc := range m.ToolCalls {
			chars += int64(len(tc.Name) + len(tc.Arguments))
		}
	}
	est := chars / roughDivisor
	if est < int64(len(msgs)) {
		est = int64(len(msgs))
	}
	return est
}
