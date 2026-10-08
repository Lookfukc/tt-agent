package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

func TestAnthropicChat(t *testing.T) {
	var gotBody map[string]any
	var gotAuth, gotVersion string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		fmt.Fprint(w, `{"id":"msg_1","model":"claude","content":[{"type":"thinking","thinking":"想"},{"type":"text","text":"答案"},{"type":"tool_use","id":"t1","name":"echo","input":{"a":1}}],"stop_reason":"tool_use","usage":{"input_tokens":7,"output_tokens":4}}`)
	}))
	defer srv.Close()

	p := protocol.NewAnthropic("claude", srv.URL, "sk", protocol.Quirks{})
	resp, err := p.Chat(context.Background(), core.ChatRequest{Model: "claude"})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if gotAuth != "sk" || gotVersion == "" {
		t.Errorf("auth headers: %q %q", gotAuth, gotVersion)
	}
	if gotBody["max_tokens"] == nil {
		t.Error("max_tokens must be present, anthropic requires it")
	}
	if resp.Content != "答案" || resp.Reasoning != "想" {
		t.Errorf("resp = %+v", resp)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].ID != "t1" || resp.ToolCalls[0].Arguments != `{"a":1}` {
		t.Errorf("toolcalls = %+v", resp.ToolCalls)
	}
	if resp.FinishReason != core.FinishToolCalls || resp.Usage.InputTokens != 7 {
		t.Errorf("finish=%s usage=%+v", resp.FinishReason, resp.Usage)
	}
}

func TestAnthropicChatStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		write := func(ev string, data string) {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev, data)
		}
		write("message_start", `{"type":"message_start","message":{"usage":{"input_tokens":25,"output_tokens":1}}}`)
		write("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		write("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"你"}}`)
		write("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"好"}}`)
		write("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"t1","name":"echo","input":{}}}`)
		write("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"a\":"}}`)
		write("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"1}"}}`)
		write("content_block_stop", `{"type":"content_block_stop","index":1}`)
		write("message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}`)
		write("message_stop", `{"type":"message_stop"}`)
	}))
	defer srv.Close()

	p := protocol.NewAnthropic("claude", srv.URL, "sk", protocol.Quirks{})
	events, err := p.ChatStream(context.Background(), core.ChatRequest{Model: "claude"})
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
	if len(msg.ToolCalls) != 1 || msg.ToolCalls[0].Name != "echo" || msg.ToolCalls[0].Arguments != `{"a":1}` {
		t.Errorf("toolcalls = %+v", msg.ToolCalls)
	}
	if msg.FinishReason != core.FinishToolCalls {
		t.Errorf("finish = %q", msg.FinishReason)
	}
	// Usage fields are merged across segments: input from message_start, output from message_delta.
	if usage.InputTokens != 25 || usage.OutputTokens != 9 {
		t.Errorf("usage = %+v", usage)
	}
}

func TestAnthropicSystemAndToolResultBlocks(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		fmt.Fprint(w, `{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()

	p := protocol.NewAnthropic("claude", srv.URL, "sk", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model: "claude",
		Messages: []core.Message{
			{Role: core.RoleSystem, Content: "系统指令"},
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "t1", Name: "echo", Arguments: `{}`}}},
			{Role: core.RoleTool, ToolCallID: "t1", Content: "结果"},
		},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if gotBody["system"] != "系统指令" {
		t.Errorf("system = %v", gotBody["system"])
	}
	msgs := gotBody["messages"].([]any)
	// assistant message carries a tool_use block + user message carries a tool_result block
	if len(msgs) != 2 {
		t.Fatalf("messages = %v", msgs)
	}
	assistant := msgs[0].(map[string]any)
	blocks := assistant["content"].([]any)
	if blocks[0].(map[string]any)["type"] != "tool_use" {
		t.Errorf("assistant blocks = %v", blocks)
	}
	user := msgs[1].(map[string]any)
	if user["role"] != "user" {
		t.Errorf("tool_result role = %v", user["role"])
	}
	if user["content"].([]any)[0].(map[string]any)["type"] != "tool_result" {
		t.Errorf("user blocks = %v", user["content"])
	}
}
