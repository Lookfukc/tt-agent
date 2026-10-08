package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
	"github.com/Lookfukc/tt-agent/pkg/tools"
)

// statelessEchoLLM is a single-turn converging echo mock: the answer carries the last user message seen
type statelessEchoLLM struct{}

func (statelessEchoLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Content: "ok"}, nil
}

func (statelessEchoLLM) ChatStream(_ context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	out := make(chan core.StreamEvent, 2)
	go func() {
		defer close(out)
		var lastUser string
		for _, m := range req.Messages {
			if m.Role == core.RoleUser {
				lastUser = m.Content
			}
		}
		out <- core.StreamEvent{Type: core.StreamStart}
		out <- core.StreamEvent{Type: core.StreamDeltaText, Text: "echo:" + lastUser}
		out <- core.StreamEvent{Type: core.StreamDone}
	}()
	return out, nil
}

// TestRunWithHistoryBasic covers basic stateless-mode behavior
//
// History is supplied by the caller, NewMessages returns the messages added this
// round (including the input); concatenating and feeding them back continues the
// conversation across rounds; the loop persists no server-side state
func TestRunWithHistoryBasic(t *testing.T) {
	loop := agent.NewLoop(statelessEchoLLM{}, nil, nil, agent.Config{Model: "m"})
	ctx := context.Background()

	// Round 1: fresh conversation with no history
	res, err := loop.RunWithHistory(ctx, nil, "hello")
	if err != nil {
		t.Fatalf("RunWithHistory: %v", err)
	}
	if res.Message.Content != "echo:hello" {
		t.Fatalf("content = %q", res.Message.Content)
	}
	// New messages = user input + final answer
	if len(res.NewMessages) != 2 ||
		res.NewMessages[0].Role != core.RoleUser || res.NewMessages[0].Content != "hello" ||
		res.NewMessages[1].Role != core.RoleAssistant {
		t.Fatalf("new messages = %+v", res.NewMessages)
	}

	// Round 2: the caller persists the history and passes it back in full
	history := append(res.NewMessages, core.Message{Role: core.RoleUser, Content: "world"})
	res2, err := loop.RunWithHistory(ctx, history, "world")
	if err != nil {
		t.Fatalf("RunWithHistory round2: %v", err)
	}
	// The model should see the round-2 input
	if res2.Message.Content != "echo:world" {
		t.Fatalf("round2 content = %q", res2.Message.Content)
	}
	if len(res2.NewMessages) != 2 || res2.NewMessages[0].Content != "world" {
		t.Fatalf("round2 new messages = %+v", res2.NewMessages)
	}
}

// TestRunWithHistoryBudgetTruncation verifies over-budget history is truncated by atomic groups
//
// Passing dozens of messages still works: when assembling the request the framework
// drops old messages and keeps new ones, preserving the system message
func TestRunWithHistoryBudgetTruncation(t *testing.T) {
	loop := agent.NewLoop(statelessEchoLLM{}, nil, nil, agent.Config{
		Model: "m", TokenBudget: 200,
	})
	ctx := context.Background()

	var history []core.Message
	history = append(history, core.Message{Role: core.RoleSystem, Content: "sys"})
	for i := 0; i < 50; i++ {
		history = append(history, core.Message{
			Role: core.RoleUser, Content: fmt.Sprintf("msg-%02d-%s", i, strings.Repeat("x", 20)),
		})
	}

	res, err := loop.RunWithHistory(ctx, history, "latest")
	if err != nil {
		t.Fatalf("RunWithHistory: %v", err)
	}
	if res.Message.Content != "echo:latest" {
		t.Fatalf("content = %q", res.Message.Content)
	}
	// History is not mutated (the framework does not hold the caller's slice)
	if len(history) != 51 {
		t.Fatalf("caller history mutated: %d", len(history))
	}
}

// TestRunWithHistoryToolCallsRoundtrip covers tool-call messages returned and passed back next round
//
// This round produces assistant(tool_calls) + tool results; the caller persists them
// and passes them back verbatim; the next round's request stays valid (no orphan tool messages)
func TestRunWithHistoryToolCallsRoundtrip(t *testing.T) {
	llm := &scriptedLLM{turns: []core.Message{
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
			{ID: "c1", Name: "echo", Arguments: `{"text":"1+1"}`},
		}},
		{Role: core.RoleAssistant, Content: "the answer is 2"},
	}}
	stub := &stubTool{}
	reg := tools.NewRegistry()
	reg.Register(stub)
	loop := agent.NewLoop(llm, reg, nil, agent.Config{Model: "m"})

	res, err := loop.RunWithHistory(context.Background(), nil, "算一下 1+1")
	if err != nil {
		t.Fatalf("RunWithHistory: %v", err)
	}
	// New messages: user + assistant(tool_calls) + tool + final assistant
	if len(res.NewMessages) != 4 {
		t.Fatalf("new messages = %d msgs, want 4: %+v", len(res.NewMessages), res.NewMessages)
	}
	if len(res.NewMessages[1].ToolCalls) != 1 || res.NewMessages[2].Role != core.RoleTool {
		t.Fatalf("tool pair malformed: %+v", res.NewMessages)
	}
	if len(stub.executed) != 1 {
		t.Fatalf("tool executed = %v", stub.executed)
	}

	// Pass back the full history and run another round (echo finale); the pairing must remain valid
	echo := agent.NewLoop(statelessEchoLLM{}, nil, nil, agent.Config{Model: "m"})
	history := append(append([]core.Message{}, res.NewMessages...), core.Message{Role: core.RoleUser, Content: "thanks"})
	res2, err := echo.RunWithHistory(context.Background(), history, "thanks")
	if err != nil {
		t.Fatalf("RunWithHistory round2: %v", err)
	}
	if res2.Message.Content != "echo:thanks" {
		t.Fatalf("round2 content = %q", res2.Message.Content)
	}
}

// TestRunWithHistoryStatelessNoLeak verifies a stateless run does not touch the loop's attached memory
func TestRunWithHistoryStatelessNoLeak(t *testing.T) {
	buf := memorytest.NewBuffer(nil)
	loop := agent.NewLoop(statelessEchoLLM{}, nil, buf, agent.Config{Model: "m"})
	_, err := loop.RunWithHistory(context.Background(), nil, "hi")
	if err != nil {
		t.Fatalf("RunWithHistory: %v", err)
	}
	msgs, _ := buf.Recent(context.Background(), "stateless", 1<<62)
	if len(msgs) != 0 {
		t.Fatalf("stateless run leaked into memory: %+v", msgs)
	}
}

// TestRunRequiresMemory verifies a loop without memory reports an error on stateful Run
func TestRunRequiresMemory(t *testing.T) {
	loop := agent.NewLoop(statelessEchoLLM{}, nil, nil, agent.Config{Model: "m"})
	_, _, err := loop.Run(context.Background(), "s", "hi")
	if err == nil || !strings.Contains(err.Error(), "no memory") {
		t.Fatalf("Run without memory: err = %v, want no-memory error", err)
	}
}

// TestStatelessHTTP covers the stateless HTTP entry point
//
// messages carries history → the response includes new_messages; session_id and messages are mutually exclusive
func TestStatelessHTTP(t *testing.T) {
	srv := newTestServer(t, statelessEchoLLM{})
	handler := srv.Handler()

	body := `{"messages":[{"role":"user","content":"a"},{"role":"assistant","content":"b"}],"input":"c"}`
	req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Mode        string         `json:"mode"`
		Content     string         `json:"content"`
		NewMessages []core.Message `json:"new_messages"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Mode != "stateless" || resp.Content != "echo:c" {
		t.Fatalf("resp mode=%s content=%q", resp.Mode, resp.Content)
	}
	if len(resp.NewMessages) != 2 || resp.NewMessages[0].Content != "c" {
		t.Fatalf("new_messages = %+v", resp.NewMessages)
	}

	// Mutual-exclusion check
	bad := `{"session_id":"s","messages":[{"role":"user","content":"a"}],"input":"c"}`
	req2 := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(bad))
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("mutually exclusive check: status = %d", rec2.Code)
	}
}

// TestStatelessHTTPStream covers the stateless streaming entry point
//
// SSE progress events are consistent; the done event carries new_messages
func TestStatelessHTTPStream(t *testing.T) {
	srv := newTestServer(t, statelessEchoLLM{})
	handler := srv.Handler()

	body := `{"messages":[{"role":"user","content":"a"}],"input":"b","stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	raw := rec.Body.String()
	if !strings.Contains(raw, "event: text") {
		t.Fatalf("no text event: %s", raw)
	}
	if !strings.Contains(raw, `"new_messages"`) {
		t.Fatalf("done event missing new_messages: %s", raw)
	}
	if strings.Contains(raw, `"session_id"`) {
		t.Fatalf("stateless response should not carry session_id: %s", raw)
	}
}
