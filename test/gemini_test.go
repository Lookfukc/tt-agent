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

	"github.com/Lookfukc/send-agent/pkg/adapters"
	"github.com/Lookfukc/send-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
	"github.com/Lookfukc/send-agent/pkg/core"
)

func TestGeminiChat(t *testing.T) {
	var gotBody map[string]any
	var gotPath, gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.Header.Get("x-goog-api-key")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		fmt.Fprint(w, `{"candidates":[{"content":{"parts":[{"thought":true,"text":"思考"},{"text":"答案"},{"functionCall":{"name":"echo","args":{"a":1}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":6,"thoughtsTokenCount":3}}`)
	}))
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "gk", protocol.Quirks{})
	resp, err := p.Chat(context.Background(), core.ChatRequest{Model: "gemini-2.5-flash"})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if !strings.HasSuffix(gotPath, "/models/gemini-2.5-flash:generateContent") {
		t.Errorf("path = %s", gotPath)
	}
	if gotKey != "gk" {
		t.Errorf("api key header missing")
	}
	if resp.Content != "答案" || resp.Reasoning != "思考" {
		t.Errorf("resp = %+v", resp)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "echo" || resp.ToolCalls[0].Arguments != `{"a":1}` {
		t.Errorf("toolcalls = %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].ID != "gemini:echo" {
		t.Errorf("synthesized id = %q", resp.ToolCalls[0].ID)
	}
	if resp.Usage.InputTokens != 9 || resp.Usage.ReasoningTokens != 3 {
		t.Errorf("usage = %+v", resp.Usage)
	}
}

func TestGeminiRequestMapping(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		fmt.Fprint(w, `{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}`)
	}))
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "gk", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model: "m",
		Messages: []core.Message{
			{Role: core.RoleSystem, Content: "系统"},
			{Role: core.RoleUser, Content: "你好"},
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
				{ID: "gemini:echo", Name: "echo", Arguments: `{"a":1}`},
			}},
			{Role: core.RoleTool, ToolCallID: "gemini:echo", Content: "结果"},
		},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}

	si := gotBody["systemInstruction"].(map[string]any)
	if si["parts"].([]any)[0].(map[string]any)["text"] != "系统" {
		t.Errorf("systemInstruction = %v", si)
	}
	contents := gotBody["contents"].([]any)
	if len(contents) != 3 {
		t.Fatalf("contents = %v", contents)
	}
	if contents[0].(map[string]any)["role"] != "user" {
		t.Error("user role mapping")
	}
	modelMsg := contents[1].(map[string]any)
	if modelMsg["role"] != "model" {
		t.Errorf("assistant should map to model, got %v", modelMsg["role"])
	}
	fc := modelMsg["parts"].([]any)[0].(map[string]any)["functionCall"].(map[string]any)
	if fc["name"] != "echo" {
		t.Errorf("functionCall = %v", fc)
	}
	fr := contents[2].(map[string]any)["parts"].([]any)[0].(map[string]any)["functionResponse"].(map[string]any)
	if fr["name"] != "gemini:echo" && fr["name"] != "echo" {
		t.Errorf("functionResponse = %v", fr)
	}
	// 合成 ID 内嵌函数名，回传时应还原为裸名
	if fr["name"] != "echo" {
		t.Errorf("name should be extracted from synthesized id, got %q", fr["name"])
	}
}

// TestGeminiChatStream 原生流式：连接关闭即结束，无 [DONE] 标记
func TestGeminiChatStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		write := func(data string) {
			fmt.Fprintf(w, "data: %s\n\n", data)
		}
		write(`{"candidates":[{"content":{"parts":[{"text":"你"}]}}]}`)
		write(`{"candidates":[{"content":{"parts":[{"thought":true,"text":"想一想"}]}}]}`)
		write(`{"candidates":[{"content":{"parts":[{"text":"好"},{"functionCall":{"name":"echo","args":{"a":1}}}]}}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":4}}`)
		write(`{"candidates":[{"content":{"parts":[{}]},"finishReason":"STOP"}]}`)
		// handler 返回连接即关闭，模拟原生 Gemini 终止方式
	}))
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "gk", protocol.Quirks{})
	events, err := p.ChatStream(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	msg, usage, serr := core.CollectStream(events)
	if serr != nil {
		t.Fatalf("stream error: %v", serr)
	}
	if msg.Content != "你好" {
		t.Errorf("content = %q", msg.Content)
	}
	if msg.Reasoning != "想一想" {
		t.Errorf("reasoning = %q", msg.Reasoning)
	}
	if len(msg.ToolCalls) != 1 || msg.ToolCalls[0].Arguments != `{"a":1}` {
		t.Errorf("toolcalls = %+v", msg.ToolCalls)
	}
	if usage.InputTokens != 5 || usage.OutputTokens != 4 {
		t.Errorf("usage = %+v", usage)
	}
}

// TestH6GeminiParallelToolCallsStream 并行 functionCall 必须各自独立
// 原 bug：Index 恒为 0，第二个调用覆盖第一个的名字、参数拼成非法 JSON
func TestH6GeminiParallelToolCallsStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n",
			`{"candidates":[{"content":{"parts":[`+
				`{"functionCall":{"name":"echo","args":{"a":1}}},`+
				`{"functionCall":{"name":"clock","args":{"b":2}}}`+
				`]}}]}`)
	}))
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "gk", protocol.Quirks{})
	events, err := p.ChatStream(context.Background(), core.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	msg, _, serr := core.CollectStream(events)
	if serr != nil {
		t.Fatalf("stream error: %v", serr)
	}
	if len(msg.ToolCalls) != 2 {
		t.Fatalf("H6: toolcalls = %d, want 2 (parallel calls merged)", len(msg.ToolCalls))
	}
	if msg.ToolCalls[0].Name != "echo" || msg.ToolCalls[0].Arguments != `{"a":1}` {
		t.Errorf("call0 = %+v", msg.ToolCalls[0])
	}
	if msg.ToolCalls[1].Name != "clock" || msg.ToolCalls[1].Arguments != `{"b":2}` {
		t.Errorf("call1 = %+v", msg.ToolCalls[1])
	}
}

func TestFactoryProtocols(t *testing.T) {
	// 三协议合成配置，验证 factory 按协议装配；厂商一律来自配置，注册表不内置
	configs := []provider.ProviderConfig{
		{ID: "t-openai", Name: "OpenAI 兼容", Protocol: "openai", BaseURL: "http://127.0.0.1:1/v1"},
		{ID: "t-anthropic", Name: "Anthropic", Protocol: "anthropic", BaseURL: "http://127.0.0.1:1"},
		{ID: "t-gemini", Name: "Gemini", Protocol: "gemini", BaseURL: "http://127.0.0.1:1"},
	}
	for i := range configs {
		configs[i].SetAPIKey("test-key")
		if _, err := adapters.NewLLM(&configs[i]); err != nil {
			t.Errorf("factory %s: %v", configs[i].ID, err)
		}
	}
	// 空注册表不再内置厂商
	if ids := provider.NewRegistry().List(); len(ids) != 0 {
		t.Errorf("empty registry has providers: %v", ids)
	}
}
