// Package extractor provides a ready-to-use ltm.Extractor backed by
// any core.LLM.
//
// The extraction prompt asks the model for durable user facts as a
// JSON array; the parser tolerates code fences and surrounding prose,
// because models wrap JSON in markdown regardless of instructions.
// Using core.LLM (rather than a raw HTTP client) means the extractor
// inherits the framework's protocol adapters, middleware and retry —
// point it at the same LLM the agent already uses, or at a cheaper
// one.
package extractor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/ltm"
)

// defaultPrompt is the extraction instruction.
//
// Third-person short statements keep facts readable out of context
// (they are injected as system-prompt fragments later), and the
// explicit "empty array" clause keeps models from inventing facts to
// fill the shape.
const defaultPrompt = `Extract durable facts worth remembering about the user from the conversation below.

Rules:
- Only stable, reusable facts: preferences, constraints, background, goals. Not small talk, not transient context.
- Each fact is one short third-person statement, e.g. "The user is vegetarian".
- Return ONLY a JSON array of strings. No markdown, no commentary.
- If nothing is worth remembering, return [].`

// maxContentPerMessage caps one message's contribution to the prompt,
// bounding extraction cost on huge tool outputs.
const maxContentPerMessage = 2048

// LLMExtractor distills conversations through an LLM.
type LLMExtractor struct {
	llm     core.LLM
	prompt  string
	maxMsgs int
	lenient bool
}

// Options configures the extractor.
type Options struct {
	// Prompt overrides the extraction instruction.
	Prompt string

	// MaxMessages caps how many recent messages are considered.
	// Default 100; older messages are dropped from the prompt.
	MaxMessages int

	// Lenient makes LLM and parse failures return no facts instead of
	// an error. Learning is a background nicety: when true, a flaky
	// model never breaks the conversation flow that triggered it.
	Lenient bool
}

// defaultMaxMessages bounds the conversation window handed to the LLM.
const defaultMaxMessages = 100

// New builds an extractor over an LLM.
//
// A cheap model is the right choice: extraction is classification-adjacent,
// not generation.
func New(llm core.LLM, opts Options) *LLMExtractor {
	if opts.MaxMessages <= 0 {
		opts.MaxMessages = defaultMaxMessages
	}
	if opts.Prompt == "" {
		opts.Prompt = defaultPrompt
	}
	return &LLMExtractor{llm: llm, prompt: opts.Prompt, maxMsgs: opts.MaxMessages, lenient: opts.Lenient}
}

// Extract implements ltm.Extractor.
func (e *LLMExtractor) Extract(ctx context.Context, msgs []ltm.Message) ([]string, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	facts, err := e.extract(ctx, msgs)
	if err != nil && e.lenient {
		return nil, nil
	}
	return facts, err
}

// extract does the real work; Extract adds the lenient wrapper.
func (e *LLMExtractor) extract(ctx context.Context, msgs []ltm.Message) ([]string, error) {
	body := renderConversation(msgs, e.maxMsgs)
	if body == "" {
		return nil, nil
	}
	resp, err := e.llm.Chat(ctx, core.ChatRequest{
		Model: "extractor",
		Messages: []core.Message{
			{Role: core.RoleSystem, Content: e.prompt},
			{Role: core.RoleUser, Content: body},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("extractor: llm: %w", err)
	}
	if resp == nil || resp.Content == "" {
		return nil, nil
	}
	facts, err := parseFacts(resp.Content)
	if err != nil {
		return nil, fmt.Errorf("extractor: parse: %w", err)
	}
	return facts, nil
}

// renderConversation formats messages for the prompt, newest-bounded.
func renderConversation(msgs []ltm.Message, max int) string {
	if len(msgs) > max {
		msgs = msgs[len(msgs)-max:]
	}
	var b strings.Builder
	for _, m := range msgs {
		content := m.Content
		if len(content) > maxContentPerMessage {
			// Byte truncation can split a UTF-8 tail; back up to a rune
			// start so the prompt never carries mojibake.
			cut := maxContentPerMessage
			for cut > 0 && !utf8.RuneStart(content[cut]) {
				cut--
			}
			content = content[:cut] + "…"
		}
		b.WriteString("[" + m.Role + "] " + content + "\n")
	}
	return strings.TrimSpace(b.String())
}

// parseFacts pulls a JSON string array out of a model reply.
//
// Models wrap JSON in ``` fences and prose despite instructions, so
// the parser scans for the first '[' to the last ']' and parses that
// slice; strict JSON decoding of the whole reply would fail on the
// very models people actually use.
func parseFacts(reply string) ([]string, error) {
	trimmed := strings.TrimSpace(reply)
	if trimmed == "" {
		return nil, nil
	}
	start := strings.Index(trimmed, "[")
	end := strings.LastIndex(trimmed, "]")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("no JSON array in reply: %q", snippet(trimmed))
	}
	var facts []string
	if err := json.Unmarshal([]byte(trimmed[start:end+1]), &facts); err != nil {
		return nil, fmt.Errorf("bad JSON array (%v): %q", err, snippet(trimmed))
	}
	out := make([]string, 0, len(facts))
	for _, f := range facts {
		f = strings.TrimSpace(f)
		if f != "" {
			out = append(out, f)
		}
	}
	return out, nil
}

// snippet trims a reply for error messages.
func snippet(s string) string {
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

// compile-time proof the extractor plugs into ltm.
var _ ltm.Extractor = (*LLMExtractor)(nil)
