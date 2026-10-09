package extractor_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/ltm"
	"github.com/Lookfukc/tt-agent/pkg/ltm/extractor"
)

// scriptedLLM replies with a fixed content (or error) and records the
// request it saw.
type scriptedLLM struct {
	reply string
	err   error
	seen  core.ChatRequest
}

func (s *scriptedLLM) Chat(_ context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	s.seen = req
	if s.err != nil {
		return nil, s.err
	}
	return &core.ChatResponse{Content: s.reply}, nil
}

func (s *scriptedLLM) ChatStream(context.Context, core.ChatRequest) (<-chan core.StreamEvent, error) {
	return nil, errors.New("not implemented")
}

var conv = []ltm.Message{
	{Role: "user", Content: "I'm vegetarian, and I mainly write Go."},
	{Role: "assistant", Content: "Noted!"},
}

// TestCleanJSON covers the ideal reply shape.
func TestCleanJSON(t *testing.T) {
	llm := &scriptedLLM{reply: `["The user is vegetarian","The user writes Go"]`}
	got, err := extractor.New(llm, extractor.Options{}).Extract(context.Background(), conv)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(got) != 2 || got[0] != "The user is vegetarian" {
		t.Fatalf("facts = %v", got)
	}
}

// TestFencedJSON covers markdown-wrapped replies — what models
// actually emit regardless of instructions.
func TestFencedJSON(t *testing.T) {
	llm := &scriptedLLM{reply: "```json\n[\"The user is vegetarian\"]\n```"}
	got, err := extractor.New(llm, extractor.Options{}).Extract(context.Background(), conv)
	if err != nil || len(got) != 1 || got[0] != "The user is vegetarian" {
		t.Fatalf("facts = %v, err = %v", got, err)
	}
}

// TestProseAroundJSON covers conversational padding.
func TestProseAroundJSON(t *testing.T) {
	llm := &scriptedLLM{reply: "Here are the facts I found:\n[\"The user writes Go\"]\nHope that helps!"}
	got, err := extractor.New(llm, extractor.Options{}).Extract(context.Background(), conv)
	if err != nil || len(got) != 1 {
		t.Fatalf("facts = %v, err = %v", got, err)
	}
}

// TestEmptyArrayIsNoFacts covers the nothing-memorable outcome.
func TestEmptyArrayIsNoFacts(t *testing.T) {
	llm := &scriptedLLM{reply: `[]`}
	got, err := extractor.New(llm, extractor.Options{}).Extract(context.Background(), conv)
	if err != nil || len(got) != 0 {
		t.Fatalf("facts = %v, err = %v", got, err)
	}
}

// TestEmptyStringsDropped keeps blank facts out of the store.
func TestEmptyStringsDropped(t *testing.T) {
	llm := &scriptedLLM{reply: `["The user writes Go", "", "   "]`}
	got, _ := extractor.New(llm, extractor.Options{}).Extract(context.Background(), conv)
	if len(got) != 1 {
		t.Fatalf("facts = %v", got)
	}
}

// TestStrictModeFailsOnGarbage proves a nonsense reply is an error by
// default — silent data loss (zero facts from a broken model) should
// be visible.
func TestStrictModeFailsOnGarbage(t *testing.T) {
	llm := &scriptedLLM{reply: "I could not parse that conversation, sorry."}
	_, err := extractor.New(llm, extractor.Options{}).Extract(context.Background(), conv)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "no JSON array") {
		t.Fatalf("err = %v", err)
	}
}

// TestLenientModeDegrades proves the lenient option turns model
// failures into "no facts" — learning must not break the flow that
// triggered it.
func TestLenientModeDegrades(t *testing.T) {
	failing := &scriptedLLM{err: errors.New("provider down")}
	got, err := extractor.New(failing, extractor.Options{Lenient: true}).Extract(context.Background(), conv)
	if err != nil || got != nil {
		t.Fatalf("lenient on LLM error: %v %v", got, err)
	}

	garbage := &scriptedLLM{reply: "no json here"}
	got, err = extractor.New(garbage, extractor.Options{Lenient: true}).Extract(context.Background(), conv)
	if err != nil || got != nil {
		t.Fatalf("lenient on parse error: %v %v", got, err)
	}
}

// TestLLMFailureIsErrorByDefault keeps the strict contract honest.
func TestLLMFailureIsErrorByDefault(t *testing.T) {
	failing := &scriptedLLM{err: errors.New("boom")}
	if _, err := extractor.New(failing, extractor.Options{}).Extract(context.Background(), conv); err == nil {
		t.Fatal("expected error in strict mode")
	}
}

// TestPromptShape asserts the conversation is rendered with roles and
// sent as one user message under the extraction system prompt.
func TestPromptShape(t *testing.T) {
	llm := &scriptedLLM{reply: `[]`}
	if _, err := extractor.New(llm, extractor.Options{}).Extract(context.Background(), conv); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(llm.seen.Messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(llm.seen.Messages))
	}
	if llm.seen.Messages[0].Role != core.RoleSystem || len(llm.seen.Messages[0].Content) < 50 {
		t.Fatalf("system prompt malformed: %+v", llm.seen.Messages[0])
	}
	user := llm.seen.Messages[1].Content
	if !strings.Contains(user, "[user] I'm vegetarian") || !strings.Contains(user, "[assistant] Noted!") {
		t.Fatalf("conversation not rendered with roles: %q", user)
	}
}

// TestMaxMessagesBoundsPrompt proves the window cap keeps old turns
// out of the LLM call.
func TestMaxMessagesBoundsPrompt(t *testing.T) {
	llm := &scriptedLLM{reply: `[]`}
	var long []ltm.Message
	for i := 0; i < 30; i++ {
		long = append(long, ltm.Message{Role: "user", Content: strings.Repeat("x", 10)})
	}
	if _, err := extractor.New(llm, extractor.Options{MaxMessages: 5}).Extract(context.Background(), long); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	// 30 条各 10 字符 + 角色标记，只保留最近 5 条
	user := llm.seen.Messages[1].Content
	if got := strings.Count(user, "[user]"); got != 5 {
		t.Fatalf("prompt carried %d messages, want 5", got)
	}
}

// TestEmptyConversationShortCircuits proves no LLM call for nothing.
func TestEmptyConversationShortCircuits(t *testing.T) {
	llm := &scriptedLLM{reply: `["should not be asked"]`}
	got, err := extractor.New(llm, extractor.Options{}).Extract(context.Background(), nil)
	if err != nil || got != nil {
		t.Fatalf("facts = %v, err = %v", got, err)
	}
	if llm.seen.Model != "" {
		t.Fatal("LLM was called for an empty conversation")
	}
}

// TestLearnIntegration wires the extractor into the facade end to end.
func TestLearnIntegration(t *testing.T) {
	llm := &scriptedLLM{reply: `["The user is vegetarian"]`}
	store := ltm.NewMemoryStore(ltm.MemoryOptions{})
	mem := ltm.New(store, ltm.Options{Extractor: extractor.New(llm, extractor.Options{})})

	learned, err := mem.Learn(context.Background(), "u1", conv)
	if err != nil {
		t.Fatalf("Learn: %v", err)
	}
	if len(learned) != 1 || learned[0].Text != "The user is vegetarian" {
		t.Fatalf("learned = %+v", learned)
	}
	if store.Count("u1") != 1 {
		t.Fatalf("store count = %d", store.Count("u1"))
	}
}
