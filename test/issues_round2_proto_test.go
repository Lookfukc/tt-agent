package test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// TestRound2N5CancelDuringStalledScan: cancellation during a blocked read must be classified as non-retryable cancellation
//
// Original bug: cancellation surfaced inside Scan as a body-read error, got
// wrapped as retryable ErrNetwork, and the retry logic kept replaying the same
// request with an already-cancelled ctx
func TestRound2N5CancelDuringStalledScan(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"前半\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-release // Server blocks on the second half, creating a blocked read
	}))
	defer srv.Close()
	defer close(release)

	p := protocol.NewOpenAI("t", srv.URL, "k", protocol.Quirks{})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := p.ChatStream(ctx, core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	for e := range events {
		if e.Type == core.StreamDeltaText {
			break
		}
	}
	cancel()

	var gotErr error
	deadline := time.After(3 * time.Second)
collect:
	for {
		select {
		case e, ok := <-events:
			if !ok {
				break collect
			}
			if e.Type == core.StreamError {
				gotErr = e.Err
			}
		case <-deadline:
			t.Fatal("N5: stream did not close within deadline")
		}
	}
	if gotErr == nil {
		t.Fatal("N5: stream closed without StreamError")
	}
	if kind := core.ErrorKindOf(gotErr); kind != core.ErrCanceled {
		t.Errorf("N5: kind = %v, want ErrCanceled", kind)
	}
	if core.Retryable(gotErr) {
		t.Errorf("N5: cancellation must not be retryable, err = %v", gotErr)
	}
}

// TestRound2LP2ErrTooLongIdentifiable: the overlong-line error must be programmatically identifiable via errors.Is
//
// Original bug: the error message was only joined with %v, so callers could not
// distinguish bufio.ErrTooLong from a generic network error
func TestRound2LP2ErrTooLongIdentifiable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", strings.Repeat("a", 2*1024*1024))
	}))
	defer srv.Close()

	p := protocol.NewOpenAI("t", srv.URL, "k", protocol.Quirks{})
	events, err := p.ChatStream(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	_, _, serr := core.CollectStream(events)
	if serr == nil {
		t.Fatal("L-P2: oversized line must fail the stream")
	}
	if !errors.Is(serr, bufio.ErrTooLong) {
		t.Errorf("L-P2: err = %v, want errors.Is(err, bufio.ErrTooLong)", serr)
	}
}

// TestRound2N6AnthropicResponseFormatNonRetryable: a ResponseFormat rejection must be non-retryable ErrUnsupported
//
// Original bug: a bare fmt.Errorf fell through error classification into a retryable network error
func TestRound2N6AnthropicResponseFormatNonRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{}`)
	}))
	defer srv.Close()

	p := protocol.NewAnthropic("a", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:          "m",
		ResponseFormat: &core.ResponseFormat{Name: "out", Schema: json.RawMessage(`{"type":"object"}`)},
	})
	if err == nil {
		t.Fatal("N6: ResponseFormat must be rejected")
	}
	if kind := core.ErrorKindOf(err); kind != core.ErrUnsupported {
		t.Errorf("N6: kind = %v, want ErrUnsupported", kind)
	}
	if core.Retryable(err) {
		t.Errorf("N6: unsupported capability must not be retryable, err = %v", err)
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Errorf("N6: err = %v, want message mentioning unsupported", err)
	}
}

// TestRound2N10GeminiStreamSameNameParallelCalls verifies synthesized IDs of same-name parallel calls in streaming must be distinct
func TestRound2N10GeminiStreamSameNameParallelCalls(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n",
			`{"candidates":[{"content":{"parts":[`+
				`{"functionCall":{"name":"echo","args":{"a":1}}},`+
				`{"functionCall":{"name":"echo","args":{"b":2}}}`+
				`]}}]}`)
	}))
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "k", protocol.Quirks{})
	events, err := p.ChatStream(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	msg, _, serr := core.CollectStream(events)
	if serr != nil {
		t.Fatalf("stream error: %v", serr)
	}
	if len(msg.ToolCalls) != 2 {
		t.Fatalf("N10: toolcalls = %d, want 2", len(msg.ToolCalls))
	}
	if msg.ToolCalls[0].ID == msg.ToolCalls[1].ID {
		t.Fatalf("N10: duplicate tool call IDs %q for same-name parallel calls", msg.ToolCalls[0].ID)
	}
	for _, tc := range msg.ToolCalls {
		if tc.Name != "echo" {
			t.Errorf("N10: name = %q, want echo", tc.Name)
		}
	}
}

// TestRound2N10GeminiChatSameNameParallelCalls verifies non-streaming same-name parallel calls are likewise disambiguated
func TestRound2N10GeminiChatSameNameParallelCalls(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"candidates":[{"content":{"parts":[`+
			`{"functionCall":{"name":"echo","args":{"a":1}}},`+
			`{"functionCall":{"name":"echo","args":{"b":2}}}`+
			`]},"finishReason":"STOP"}]}`)
	}))
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "k", protocol.Quirks{})
	resp, err := p.Chat(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("N10: toolcalls = %d, want 2", len(resp.ToolCalls))
	}
	if resp.ToolCalls[0].ID == resp.ToolCalls[1].ID {
		t.Fatalf("N10: duplicate tool call IDs %q", resp.ToolCalls[0].ID)
	}
	if resp.ToolCalls[0].Arguments != `{"a":1}` || resp.ToolCalls[1].Arguments != `{"b":2}` {
		t.Errorf("N10: arguments = %q / %q", resp.ToolCalls[0].Arguments, resp.ToolCalls[1].Arguments)
	}
}

// TestRound2N10GeminiSynthesizedIDRoundTrip: both old and new synthesized ID forms must resolve back to the tool name
//
// When the new format gemini:<name>:<index> and the old format gemini:<name>
// are both sent back, functionResponse must map back to the correct tool name
func TestRound2N10GeminiSynthesizedIDRoundTrip(t *testing.T) {
	got, srv := probeBody(t, `{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}`)
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model: "m",
		Messages: []core.Message{
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
				{ID: "gemini:echo", Name: "echo", Arguments: `{}`},
				{ID: "gemini:echo:1", Name: "echo", Arguments: `{}`},
				{ID: "gemini:f", Name: "f", Arguments: `{}`},
			}},
			{Role: core.RoleTool, ToolCallID: "gemini:echo", Content: "一"},
			{Role: core.RoleTool, ToolCallID: "gemini:echo:1", Content: "二"},
			{Role: core.RoleTool, ToolCallID: "gemini:f", Content: "三"},
		},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	contents := got["contents"].([]any)
	user := contents[len(contents)-1].(map[string]any)
	parts := user["parts"].([]any)
	if len(parts) != 3 {
		t.Fatalf("N10: functionResponse parts = %d, want 3", len(parts))
	}
	for i, want := range []string{"echo", "echo", "f"} {
		fr := parts[i].(map[string]any)["functionResponse"].(map[string]any)
		if fr["name"] != want {
			t.Errorf("N10: part[%d] name = %v, want %v", i, fr["name"], want)
		}
	}
}

// TestRound2N11AnthropicThinkingDropsExtraTemperature: sampling parameters injected via Extra must also be cleared
//
// Original bug: temperature cleanup happened before the Extra merge, so a
// same-named field in Extra leaked into the request body with thinking enabled,
// and Anthropic returned 400 outright
func TestRound2N11AnthropicThinkingDropsExtraTemperature(t *testing.T) {
	got, srv := probeBody(t, `{"content":[{"type":"text","text":"ok"}]}`)
	defer srv.Close()

	p := protocol.NewAnthropic("a", srv.URL, "k", protocol.Quirks{})
	temp := 0.7
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:       "m",
		Temperature: &temp,
		Thinking:    &core.ThinkingConfig{Enabled: true},
		Extra:       map[string]any{"temperature": 0.7, "top_p": 0.9},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if _, exists := got["temperature"]; exists {
		t.Fatal("N11: temperature injected via Extra must be dropped when thinking enabled")
	}
	if _, exists := got["top_p"]; exists {
		t.Fatal("N11: top_p must be dropped when thinking enabled")
	}
}

// TestRound2N11AnthropicThinkingMaxTokensGuard: max_tokens must exceed the thinking budget
//
// Anthropic hard-requires max_tokens > thinking.budget_tokens; violating it gets an immediate 400
func TestRound2N11AnthropicThinkingMaxTokensGuard(t *testing.T) {
	got, srv := probeBody(t, `{"content":[{"type":"text","text":"ok"}]}`)
	defer srv.Close()

	p := protocol.NewAnthropic("a", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:     "m",
		MaxTokens: 100,
		Thinking:  &core.ThinkingConfig{Enabled: true, BudgetTokens: 4096},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	budget, ok := got["thinking"].(map[string]any)["budget_tokens"].(float64)
	if !ok {
		t.Fatalf("N11: thinking budget missing from body: %v", got["thinking"])
	}
	maxTokens, ok := got["max_tokens"].(float64)
	if !ok {
		t.Fatalf("N11: max_tokens missing from body")
	}
	if maxTokens <= budget {
		t.Fatalf("N11: max_tokens = %v must exceed thinking budget %v", maxTokens, budget)
	}
}
