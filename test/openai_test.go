// Package test verifies framework behavior across layers as a black box, depending only on exported APIs.
package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// serveSSE starts a test server that returns SSE line by line.
func serveSSE(t *testing.T, lines ...string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, l := range lines {
			fmt.Fprintln(w, l)
		}
	}))
}

func TestChatStreamAccumulates(t *testing.T) {
	srv := serveSSE(t,
		`data: {"choices":[{"index":0,"delta":{"reasoning_content":"思考"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":"你"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":"好"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"echo","arguments":"{\"te"}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"xt\":\"a\"}"}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		`data: {"usage":{"prompt_tokens":10,"completion_tokens":5,"completion_tokens_details":{"reasoning_tokens":3}}}`,
		`data: [DONE]`,
	)
	defer srv.Close()

	p := protocol.NewOpenAI("test", srv.URL, "key", protocol.Quirks{})
	events, err := p.ChatStream(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	msg, usage, serr := core.CollectStream(events)
	if serr != nil {
		t.Fatalf("stream error: %v", serr)
	}
	if msg.Content != "你好" {
		t.Errorf("content = %q, want 你好", msg.Content)
	}
	if msg.Reasoning != "思考" {
		t.Errorf("reasoning = %q, want 思考", msg.Reasoning)
	}
	if msg.FinishReason != core.FinishToolCalls {
		t.Errorf("finish = %q, want tool_calls", msg.FinishReason)
	}
	if len(msg.ToolCalls) != 1 || msg.ToolCalls[0].Arguments != `{"text":"a"}` {
		t.Errorf("toolcalls = %+v, want merged args", msg.ToolCalls)
	}
	if usage.InputTokens != 10 || usage.ReasoningTokens != 3 {
		t.Errorf("usage = %+v", usage)
	}
}

func TestChatParsesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"id":"1","model":"m","choices":[{"finish_reason":"stop","message":{"content":"hi","reasoning_content":"r"}}],"usage":{"prompt_tokens":1,"completion_tokens":2}}`)
	}))
	defer srv.Close()

	p := protocol.NewOpenAI("test", srv.URL, "key", protocol.Quirks{})
	resp, err := p.Chat(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if resp.Content != "hi" || resp.Reasoning != "r" || resp.Usage.OutputTokens != 2 {
		t.Errorf("resp = %+v", resp)
	}
}

func TestHTTPErrorMapping(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(429)
		fmt.Fprint(w, `{"error":{"message":"rate limited"}}`)
	}))
	defer srv.Close()

	p := protocol.NewOpenAI("test", srv.URL, "key", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{Model: "m"})
	ce, ok := err.(*core.Error)
	if !ok {
		t.Fatalf("err type = %T", err)
	}
	if ce.Kind != core.ErrRateLimited || ce.RetryAfter == nil || *ce.RetryAfter != 2*time.Second {
		t.Errorf("err = %+v", ce)
	}
	if !core.Retryable(err) {
		t.Error("429 should be retryable")
	}
}

func TestQuirksPatchRequest(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		fmt.Fprint(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer srv.Close()

	p := protocol.NewOpenAI("test", srv.URL, "key", protocol.Quirks{
		PatchRequest: func(body map[string]any, _ core.ChatRequest) {
			delete(body, "temperature")
			body["enable_thinking"] = true
		},
	})
	temp := 0.7
	if _, err := p.Chat(context.Background(), core.ChatRequest{Model: "m", Temperature: &temp}); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if _, exists := got["temperature"]; exists {
		t.Error("temperature should be removed by quirk")
	}
	if got["enable_thinking"] != true {
		t.Error("enable_thinking should be injected by quirk")
	}
}

func TestStreamClosedWithoutDone(t *testing.T) {
	srv := serveSSE(t, `data: {"choices":[{"index":0,"delta":{"content":"x"}}]}`)
	defer srv.Close()

	p := protocol.NewOpenAI("test", srv.URL, "key", protocol.Quirks{})
	events, err := p.ChatStream(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	_, _, serr := core.CollectStream(events)
	if serr == nil || !strings.Contains(serr.Error(), "without termination marker") {
		t.Errorf("want stream closed error, got %v", serr)
	}
}
