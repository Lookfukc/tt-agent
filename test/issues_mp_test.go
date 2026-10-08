package test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// TestM_P1AnthropicThinkingDropsTemperature verifies sampling parameters must be dropped when thinking is enabled
func TestM_P1AnthropicThinkingDropsTemperature(t *testing.T) {
	got, srv := probeBody(t, `{"content":[{"type":"text","text":"ok"}]}`)
	defer srv.Close()

	p := protocol.NewAnthropic("a", srv.URL, "k", protocol.Quirks{})
	temp := 0.7
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:       "m",
		Temperature: &temp,
		Thinking:    &core.ThinkingConfig{Enabled: true},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if _, exists := got["temperature"]; exists {
		t.Fatal("M-P1: temperature must be dropped when thinking enabled, API returns 400")
	}
	if _, exists := got["top_p"]; exists {
		t.Fatal("M-P1: top_p must be dropped when thinking enabled")
	}
}

// TestM_P2AnthropicMergesParallelToolResults verifies parallel tool results are merged into a single user message
func TestM_P2AnthropicMergesParallelToolResults(t *testing.T) {
	got, srv := probeBody(t, `{"content":[{"type":"text","text":"ok"}]}`)
	defer srv.Close()

	p := protocol.NewAnthropic("a", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model: "m",
		Messages: []core.Message{
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
				{ID: "t1", Name: "f", Arguments: `{}`},
				{ID: "t2", Name: "g", Arguments: `{}`},
			}},
			{Role: core.RoleTool, ToolCallID: "t1", Content: "一"},
			{Role: core.RoleTool, ToolCallID: "t2", Content: "二"},
		},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	msgs := got["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("M-P2: messages = %d, want 2 (assistant + single merged user)", len(msgs))
	}
	user := msgs[1].(map[string]any)
	if user["role"] != "user" {
		t.Fatalf("role = %v", user["role"])
	}
	blocks := user["content"].([]any)
	if len(blocks) != 2 {
		t.Fatalf("merged blocks = %d, want 2", len(blocks))
	}
}

// TestM_P2GeminiMergesParallelToolResults verifies Gemini likewise merges into a single user content
func TestM_P2GeminiMergesParallelToolResults(t *testing.T) {
	got, srv := probeBody(t, `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`)
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model: "m",
		Messages: []core.Message{
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
				{ID: "gemini:f", Name: "f", Arguments: `{}`},
				{ID: "gemini:g", Name: "g", Arguments: `{}`},
			}},
			{Role: core.RoleTool, ToolCallID: "gemini:f", Content: "一"},
			{Role: core.RoleTool, ToolCallID: "gemini:g", Content: "二"},
		},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	contents := got["contents"].([]any)
	if len(contents) != 2 {
		t.Fatalf("M-P2: contents = %d, want 2", len(contents))
	}
	user := contents[1].(map[string]any)
	parts := user["parts"].([]any)
	if len(parts) != 2 {
		t.Fatalf("merged functionResponse parts = %d, want 2", len(parts))
	}
}

// TestM_P3CancelEmitsStreamError verifies cancellation must produce StreamError rather than a silent close
func TestM_P3CancelEmitsStreamError(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"前半\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-release // Block the second half, waiting for the client to cancel
	}))
	defer srv.Close()
	defer close(release)

	p := protocol.NewOpenAI("t", srv.URL, "k", protocol.Quirks{})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := p.ChatStream(ctx, core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	// Cancel after the first content event arrives
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
			t.Fatal("M-P3: stream did not close within deadline")
		}
	}
	if gotErr == nil {
		t.Fatal("M-P3: channel closed without StreamError, truncated content looks complete to caller")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", gotErr)
	}
}

// TestM_P4SingleDoneEvent verifies an OpenAI stream allows exactly one StreamDone, as the terminal event
func TestM_P4SingleDoneEvent(t *testing.T) {
	srv := serveSSE(t,
		`data: {"choices":[{"index":0,"delta":{"content":"好"}}]}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		`data: {"usage":{"prompt_tokens":5,"completion_tokens":3}}`,
		`data: [DONE]`,
	)
	defer srv.Close()

	p := protocol.NewOpenAI("t", srv.URL, "k", protocol.Quirks{})
	events, err := p.ChatStream(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	doneCount, usageSeen, doneBeforeUsage := 0, false, false
	for e := range events {
		switch e.Type {
		case core.StreamDone:
			doneCount++
			if !usageSeen {
				doneBeforeUsage = true
			}
		case core.StreamUsage:
			usageSeen = true
		}
	}
	if doneCount != 1 {
		t.Fatalf("M-P4: StreamDone emitted %d times, want 1", doneCount)
	}
	if doneBeforeUsage {
		t.Fatal("M-P4: Done before usage — contract-following consumers lose usage")
	}
}

// TestM_P6OpenAIReasoningNotDoubleCounted verifies reasoning tokens are a subset of completion tokens and must not be double-counted
func TestM_P6OpenAIReasoningNotDoubleCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"choices":[{"message":{"content":"答"}}],`+
			`"usage":{"prompt_tokens":10,"completion_tokens":9,"completion_tokens_details":{"reasoning_tokens":6}}}`)
	}))
	defer srv.Close()

	p := protocol.NewOpenAI("t", srv.URL, "k", protocol.Quirks{})
	resp, err := p.Chat(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if resp.Usage.OutputTokens != 3 || resp.Usage.ReasoningTokens != 6 {
		t.Fatalf("M-P6: usage = %+v, want Output=3 Reasoning=6 (9 completion includes 6 reasoning)",
			resp.Usage)
	}
	if resp.Usage.Total() != 9 {
		t.Fatalf("M-P6: Total = %d, want 9", resp.Usage.Total())
	}
}

// TestM_P7GeminiRejectsExternalImageURL verifies external image URLs produce an explicit error instead of being mapped to fileData
func TestM_P7GeminiRejectsExternalImageURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		fmt.Fprint(w, `{"candidates":[{"content":{"parts":[{"text":"x"}]}}]}`)
	}))
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:    "m",
		Messages: []core.Message{core.UserImage("看图", "https://example.com/a.png")},
	})
	if err == nil {
		t.Fatal("M-P7: external image url must be rejected, fileData only accepts Files API URIs")
	}
}
