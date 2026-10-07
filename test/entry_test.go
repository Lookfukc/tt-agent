package test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/entry"
)

// streamMockLLM 返回固定文本流的 mock
type streamMockLLM struct{ calls int }

// Chat 返回固定响应
func (m *streamMockLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	m.calls++
	return &core.ChatResponse{Content: "hi", Usage: core.Usage{InputTokens: 3, OutputTokens: 2}}, nil
}

// ChatStream 返回两段文本增量
func (m *streamMockLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	m.calls++
	events := make(chan core.StreamEvent, 4)
	go func() {
		defer close(events)
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "你"}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "好"}
		events <- core.StreamEvent{Type: core.StreamDone}
	}()
	return events, nil
}

// errorMockLLM 永远失败
type errorMockLLM struct{}

// Chat 返回不可重试错误
func (errorMockLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, core.NewError(core.ErrAuth, "mock", errors.New("bad key"))
}

// ChatStream 返回不可重试流错误
func (errorMockLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	events := make(chan core.StreamEvent, 1)
	events <- core.StreamEvent{Type: core.StreamError, Err: core.NewError(core.ErrAuth, "mock", errors.New("bad key"))}
	close(events)
	return events, nil
}

// newTestServer 注入 mock LLM 的测试服务
func newTestServer(t *testing.T, llm core.LLM) *entry.Server {
	t.Helper()
	reg := provider.NewRegistry()
	reg.Register(&provider.ProviderConfig{
		ID: "deepseek", Name: "Test", Protocol: "openai",
		BaseURL: "http://127.0.0.1:1/v1", DefaultModel: "deepseek-chat",
		Models: []provider.ModelConfig{{ID: "deepseek-chat"}},
	})
	return entry.NewServer(reg, nil,
		entry.WithConfig(entry.Config{DefaultProvider: "deepseek"}),
		entry.WithLLMFactory(func(string) (core.LLM, error) { return llm, nil }),
	)
}

func TestChatNonStream(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/api/chat", "application/json",
		strings.NewReader(`{"input":"hi","provider_id":"deepseek"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct {
		Content string     `json:"content"`
		Usage   core.Usage `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Content != "你好" {
		t.Errorf("content = %q, want 你好", out.Content)
	}
}

func TestChatStreamSSE(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/api/chat", "application/json",
		strings.NewReader(`{"input":"hi","stream":true}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %s", ct)
	}

	var body strings.Builder
	buf := make([]byte, 512)
	for {
		n, err := resp.Body.Read(buf)
		body.Write(buf[:n])
		if err != nil {
			break
		}
	}
	got := body.String()
	for _, want := range []string{`event: text`, `"delta":"你"`, `"delta":"好"`, `event: done`} {
		if !strings.Contains(got, want) {
			t.Errorf("sse body missing %q:\n%s", want, got)
		}
	}
}

func TestChatStreamError(t *testing.T) {
	srv := newTestServer(t, errorMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/api/chat", "application/json",
		strings.NewReader(`{"input":"hi","stream":true}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	var body strings.Builder
	buf := make([]byte, 512)
	for {
		n, err := resp.Body.Read(buf)
		body.Write(buf[:n])
		if err != nil {
			break
		}
	}
	if !strings.Contains(body.String(), `event: error`) {
		t.Errorf("want error event, got:\n%s", body.String())
	}
}

func TestChatValidation(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/api/chat", "application/json",
		strings.NewReader(`{"provider_id":"nonexistent","input":"hi"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestProvidersEndpoint(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/api/providers")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	var providers []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&providers); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(providers) == 0 {
		t.Error("providers list is empty")
	}
}
