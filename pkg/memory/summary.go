package memory

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/internal/sessionlog"
)

// Splitter provides both in-budget and out-of-budget messages in one
// call.
//
// Summary needs the content of truncated messages to compress them;
// the old path had to fetch everything with a huge budget and split
// locally. Implementations supporting this interface avoid that full
// materialization.
type Splitter interface {
	// Split returns the most recent messages within budget and the
	// older messages dropped by truncation.
	Split(ctx context.Context, sessionID string, budget int64) (kept, dropped []core.Message, err error)
}

// Trimmer physically discards the oldest n non-system messages.
//
// n counts non-system messages and never deletes system messages,
// strictly aligned with the drop-side semantics of Splitter: what
// Split drops is exactly a prefix of the oldest non-system messages
// (as whole atomic groups), so a Split followed by Trim can never
// break an assistant(tool_calls)+tool pairing.
type Trimmer interface {
	// Trim discards the oldest n non-system messages.
	Trim(ctx context.Context, sessionID string, n int) error
}

// SummaryStore is the summary persistence capability.
//
// The Summary decorator hands the summary text and the folded-in
// message count to the inner persistence layer, and restores the
// cache from it after a process restart. For compact mode
// (NewCompactingSummary) this is a correctness requirement: the old
// messages have been physically deleted, so the summary text is the
// only copy of that stretch of context. Inner layers that do not
// implement this interface degrade to a pure in-memory cache (lost on
// restart).
type SummaryStore interface {
	// SaveSummary persists the session summary.
	// covered: number of messages folded into the summary.
	// text: the summary text.
	SaveSummary(ctx context.Context, sessionID string, covered int, text string) error
	// LoadSummary reads back the session summary.
	// returns: the summary text and folded-in count; ("", 0, nil) when absent.
	LoadSummary(ctx context.Context, sessionID string) (text string, covered int, err error)
}

// summaryCacheEntry is the per-session summary cache.
type summaryCacheEntry struct {
	// dropped is the number of messages folded into the summary; in
	// compact mode (already physically Trimmed) it is always 0, in
	// non-compact mode it equals the current truncation point. Hits,
	// fallbacks, and advances are all driven by comparing it against
	// the actual dropped message count.
	dropped int
	// text is the compression output.
	text string
}

// Summary is a summarizing memory.
//
// It wraps an inner Memory: old messages that budget truncation would
// discard are no longer thrown away directly; instead they are
// compressed by an LLM into a summary serving as the context prefix.
// The summary cache is driven by the "folded-in count": repeated
// queries at the same truncation point do not re-compress, and when
// the truncation point advances only the increment is compressed and
// rollingly merged into the old summary.
type Summary struct {
	inner   core.Memory
	llm     core.LLM
	counter core.TokenCounter
	// compact is the compaction mode: after a summary succeeds, the
	// folded-in messages are physically deleted from inner layers that
	// support Trimmer, keeping memory and disk usage of long sessions
	// bounded.
	compact bool
	// injected reports whether a custom estimator was injected: when
	// injected, budget packing must be executed by this layer (the
	// Split fast path uses the inner layer's measure and would bypass
	// the injection).
	injected bool
	mu       sync.Mutex
	cache    map[string]*summaryCacheEntry
	locks    map[string]*sync.Mutex
}

// NewSummary constructs a summarizing memory.
//
// Budget estimation defaults to the built-in rough estimate rather
// than the inner Memory's precise counter: the core.Memory interface
// does not expose the counter, so the summary layer cannot access the
// inner instance's measure; the rough estimate errs conservative
// (better under than over), so the truncation point only arrives
// earlier, never exceeds the window. Inner messages are not deleted
// and are kept in full (audit-friendly, unbounded usage — use
// NewCompactingSummary when bounded).
// inner: the actual storage, e.g. Persistent.
// llm: the model used for compression; a cheap small model is recommended.
// returns: a usable memory instance.
func NewSummary(inner core.Memory, llm core.LLM) *Summary {
	return newSummary(inner, llm, nil, false)
}

// NewSummaryWithCounter constructs a summarizing memory with an
// injected token estimator.
//
// Use this when the inner and outer estimation measures must agree
// (e.g. the inner Persistent is configured with an exact tokenizer
// counter), so that budget packing aligns with the inner truncation.
// inner: the actual storage, e.g. Persistent.
// llm: the model used for compression; a cheap small model is recommended.
// c: token estimator; nil degrades to the built-in rough estimate.
// returns: a usable memory instance.
func NewSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary {
	return newSummary(inner, llm, c, false)
}

// NewCompactingSummary constructs a summarizing memory in physically
// compacting mode.
//
// Differs from NewSummary: old messages swallowed by the summary are
// subsequently physically deleted from the inner layer (the inner
// layer must implement Trimmer — Persistent does; TTL decoration does
// not affect addressing), so long-session storage usage converges as
// the summary rolls forward. The summary is persisted per session via
// the inner layer's SummaryStore capability (Persistent implements
// it) and automatically restored after restart without re-compressing;
// if the inner layer does not support SummaryStore the summary lives
// only in memory, and that stretch of context is lost on restart.
// inner: the actual storage.
// llm: the model used for compression; a cheap small model is recommended.
// returns: a usable memory instance.
func NewCompactingSummary(inner core.Memory, llm core.LLM) *Summary {
	return newSummary(inner, llm, nil, true)
}

// NewCompactingSummaryWithCounter constructs a physically compacting
// summarizing memory with an injected estimator.
// inner: the actual storage.
// llm: the model used for compression.
// c: token estimator; nil degrades to the built-in rough estimate.
// returns: a usable memory instance.
func NewCompactingSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary {
	return newSummary(inner, llm, c, true)
}

// newSummary is the shared constructor.
// c: token estimator; nil degrades to the built-in rough estimate.
// compact: whether to physically compact.
// returns: a ready instance.
func newSummary(inner core.Memory, llm core.LLM, c core.TokenCounter, compact bool) *Summary {
	injected := c != nil
	if c == nil {
		c = sessionlog.Rough{}
	}
	return &Summary{
		inner:    inner,
		llm:      llm,
		counter:  c,
		compact:  compact,
		injected: injected,
		cache:    make(map[string]*summaryCacheEntry),
		locks:    make(map[string]*sync.Mutex),
	}
}

// Add passes through to the inner storage.
func (s *Summary) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	return s.inner.Add(ctx, sessionID, msgs...)
}

// Clear empties the session and invalidates the summary cache.
func (s *Summary) Clear(ctx context.Context, sessionID string) error {
	unlock := s.lockSession(sessionID)
	defer unlock()
	s.mu.Lock()
	delete(s.cache, sessionID)
	delete(s.locks, sessionID)
	s.mu.Unlock()
	return s.inner.Clear(ctx, sessionID)
}

// Recent returns in-budget messages, replacing the excess with a
// summary.
func (s *Summary) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	// The whole "split → compress → Trim" is serialized per session:
	// concurrent Recents would each Split to the same dropped count
	// then each Trim, and the second Trim would delete messages not
	// yet summarized — that is data loss, not mere waste.
	unlock := s.lockSession(sessionID)
	defer unlock()

	kept, dropped, err := s.splitInner(ctx, sessionID, budget)
	if err != nil {
		return nil, err
	}

	text := s.resolveSummary(ctx, sessionID, dropped)
	if text == "" {
		return kept, nil
	}
	// The summary is inserted after system messages and before the
	// retained history.
	out := make([]core.Message, 0, len(kept)+1)
	inserted := false
	for _, m := range kept {
		if !inserted && m.Role != core.RoleSystem {
			out = append(out, core.Message{
				Role:    core.RoleSystem,
				Content: "此前对话摘要：\n" + text,
			})
			inserted = true
		}
		out = append(out, m)
	}
	if !inserted {
		out = append(out, core.Message{Role: core.RoleSystem, Content: "此前对话摘要：\n" + text})
	}
	return out, nil
}

// splitInner fetches in-budget and out-of-budget messages, preferring
// the inner layer's Split fast path.
//
// When a custom estimator has been injected the fast path is not used
// — Split packs the budget by the inner layer's measure, bypassing
// the injected one (L-M1 semantics). When unwrapping the decoration
// chain to address a Split implementation, passing through TTL must
// touch it: Split bypasses TTL.Recent, and without the touch the
// janitor would misjudge an active session as idle and evict it.
// returns: kept messages, dropped messages.
func (s *Summary) splitInner(ctx context.Context, sessionID string, budget int64) ([]core.Message, []core.Message, error) {
	if !s.injected {
		m := s.unwrapTouched(sessionID)
		if sp, ok := m.(Splitter); ok {
			return sp.Split(ctx, sessionID, budget)
		}
	}
	// Inner layer does not support Split: fetch everything and split
	// locally. Goes through s.inner rather than the unwrapped result
	// to preserve decoration semantics such as the TTL touch.
	all, err := s.inner.Recent(ctx, sessionID, 1<<62)
	if err != nil {
		return nil, nil, err
	}
	var log sessionlog.Log
	log.Add(s.counter, time.Now(), all...)
	kept, dropped := log.Split(budget)
	return kept, dropped, nil
}

// resolveSummary computes the summary text for the current truncation
// point.
//
// On a hit or budget fallback (cached folded-in count >= current
// dropped count) the old summary is reused directly — in the fallback
// case the old summary's coverage is a superset, so no information is
// lost; on an advance only the increment is compressed and rollingly
// merged into the old summary. In compact mode Trim happens
// immediately after a successful compression; on failure it degrades
// to pure truncation without writing the cache, retrying next round.
// On an in-memory cache miss it first tries restoring from the inner
// SummaryStore (after a process restart the summary is not lost and
// is not re-compressed); after a successful compression it is written
// through to the inner layer.
// returns: the summary text; empty string when no summary is needed
// or compression failed.
func (s *Summary) resolveSummary(ctx context.Context, sessionID string, dropped []core.Message) string {
	s.mu.Lock()
	entry, ok := s.cache[sessionID]
	s.mu.Unlock()
	if !ok {
		if st, supports := unwrapMemory(s.inner).(SummaryStore); supports {
			if text, covered, err := st.LoadSummary(ctx, sessionID); err == nil && text != "" {
				entry = &summaryCacheEntry{dropped: covered, text: text}
				s.mu.Lock()
				s.cache[sessionID] = entry
				s.mu.Unlock()
			}
		}
	}

	var prior string
	covered := 0
	if ok || entry != nil {
		prior, covered = entry.text, entry.dropped
	}
	if covered >= len(dropped) {
		return prior
	}

	text := s.compress(ctx, prior, dropped[covered:])
	if text == "" {
		return ""
	}

	cacheDropped := len(dropped)
	if s.compact {
		if tr, ok := unwrapMemory(s.inner).(Trimmer); ok {
			if err := tr.Trim(ctx, sessionID, len(dropped)); err == nil {
				// Physically deleted; next round dropped restarts from 0.
				cacheDropped = 0
			}
			// Trim failed: messages are still in the inner layer; the
			// cache records the current truncation point so the next
			// round hits without re-compressing.
		}
	}
	s.mu.Lock()
	s.cache[sessionID] = &summaryCacheEntry{dropped: cacheDropped, text: text}
	s.mu.Unlock()
	// Write through to inner persistence; failure does not affect
	// correctness in this process (the in-memory cache is in place) —
	// after restart it degrades to re-compression, losing no messages
	// (in non-compact mode messages are still in the inner layer).
	if st, supports := unwrapMemory(s.inner).(SummaryStore); supports {
		_ = st.SaveSummary(ctx, sessionID, cacheDropped, text)
	}
	return text
}

// unwrapMemory unwraps the decoration chain to the innermost storage.
func unwrapMemory(m core.Memory) core.Memory {
	for {
		u, ok := m.(interface{ Unwrap() core.Memory })
		if !ok {
			return m
		}
		m = u.Unwrap()
	}
}

// unwrapTouched unwraps the decoration chain to the innermost storage,
// first touching the activity time of each decoration layer it passes
// through (e.g. TTL).
// returns: the innermost storage.
func (s *Summary) unwrapTouched(sessionID string) core.Memory {
	m := s.inner
	for {
		if t, ok := m.(interface{ touch(string) }); ok {
			t.touch(sessionID)
		}
		u, ok := m.(interface{ Unwrap() core.Memory })
		if !ok {
			return m
		}
		m = u.Unwrap()
	}
}

// lockSession takes a per-session mutex.
//
// A global lock would spread the LLM compression's seconds of latency
// across all sessions; per-session serialization keeps Trim safe
// without cross-session interference. The lock object is removed with
// Clear; at the instant of removal an in-flight holder and a newly
// created lock may briefly run in parallel — Clear semantically
// terminates the session anyway, which is acceptable.
// returns: the unlock function.
func (s *Summary) lockSession(sessionID string) func() {
	s.mu.Lock()
	l, ok := s.locks[sessionID]
	if !ok {
		l = &sync.Mutex{}
		s.locks[sessionID] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// compress calls the LLM to produce a summary, rollingly merging when
// the prior summary is non-empty.
// returns: the summary text; empty string on failure.
func (s *Summary) compress(ctx context.Context, prior string, msgs []core.Message) string {
	// Each message is truncated to 2KB, controlling the cost of the
	// compression request itself.
	var body string
	for i, m := range msgs {
		content := m.Content
		if len(content) > 2048 {
			// Byte truncation may split a trailing UTF-8 character;
			// fall back to a rune boundary.
			cut := 2048
			for cut > 0 && !utf8.RuneStart(content[cut]) {
				cut--
			}
			content = content[:cut] + "..."
		}
		body += fmt.Sprintf("[%s] %s\n", m.Role, content)
		if i >= 200 {
			body += "（更早消息省略）\n"
			break
		}
	}

	sysPrompt := "把对话压缩成不超过 200 字的要点摘要，保留结论、决定与未完成事项"
	user := body
	if prior != "" {
		sysPrompt = "把此前摘要与新增对话合并为不超过 200 字的要点摘要，保留结论、决定与未完成事项，输出合并后的完整摘要"
		user = "此前摘要：\n" + prior + "\n\n新增对话：\n" + body
	}

	resp, err := s.llm.Chat(ctx, core.ChatRequest{
		Model: "summarizer",
		Messages: []core.Message{
			{Role: core.RoleSystem, Content: sysPrompt},
			{Role: core.RoleUser, Content: user},
		},
	})
	if err != nil || resp == nil || resp.Content == "" {
		return ""
	}
	return resp.Content
}
