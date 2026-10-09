package test

import (
	"context"
	"sync"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
)

// promptCaptureLLM records every request it serves and replies with
// fixed text, so tests can assert exactly what the loop assembled.
type promptCaptureLLM struct {
	mu       sync.Mutex
	requests []core.ChatRequest
}

func (c *promptCaptureLLM) Chat(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Content: "ok"}, nil
}

func (c *promptCaptureLLM) ChatStream(_ context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	c.mu.Unlock()
	out := make(chan core.StreamEvent, 3)
	go func() {
		defer close(out)
		out <- core.StreamEvent{Type: core.StreamStart}
		out <- core.StreamEvent{Type: core.StreamDeltaText, Text: "ok"}
		out <- core.StreamEvent{Type: core.StreamDone}
	}()
	return out, nil
}

func (c *promptCaptureLLM) captured() []core.ChatRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]core.ChatRequest(nil), c.requests...)
}

// TestSystemPromptReachesRequest is the regression guard for a bug the
// demo consumer found: Config.SystemPrompt was plumbed through every
// layer but never injected into the LLM request.
func TestSystemPromptReachesRequest(t *testing.T) {
	t.Run("stateful", func(t *testing.T) {
		llm := &promptCaptureLLM{}
		loop := agent.NewLoop(llm, nil, memorytest.NewBuffer(nil), agent.Config{
			Model:        "m",
			SystemPrompt: "You are terse.",
		})
		if _, _, err := loop.Run(context.Background(), "s", "hi"); err != nil {
			t.Fatalf("Run: %v", err)
		}
		reqs := llm.captured()
		if len(reqs) != 1 {
			t.Fatalf("captured %d requests, want 1", len(reqs))
		}
		first := reqs[0].Messages[0]
		if first.Role != core.RoleSystem || first.Content != "You are terse." {
			t.Fatalf("first message = %+v, want the system prompt", first)
		}
	})

	t.Run("stateless", func(t *testing.T) {
		llm := &promptCaptureLLM{}
		loop := agent.NewLoop(llm, nil, nil, agent.Config{
			Model:        "m",
			SystemPrompt: "You are terse.",
		})
		if _, err := loop.RunWithHistory(context.Background(), nil, "hi"); err != nil {
			t.Fatalf("RunWithHistory: %v", err)
		}
		reqs := llm.captured()
		if len(reqs) != 1 {
			t.Fatalf("captured %d requests, want 1", len(reqs))
		}
		first := reqs[0].Messages[0]
		if first.Role != core.RoleSystem || first.Content != "You are terse." {
			t.Fatalf("first message = %+v, want the system prompt", first)
		}
	})

	t.Run("empty prompt injects nothing", func(t *testing.T) {
		llm := &promptCaptureLLM{}
		loop := agent.NewLoop(llm, nil, nil, agent.Config{Model: "m"})
		if _, err := loop.RunWithHistory(context.Background(), nil, "hi"); err != nil {
			t.Fatalf("RunWithHistory: %v", err)
		}
		reqs := llm.captured()
		if len(reqs[0].Messages) == 0 || reqs[0].Messages[0].Role == core.RoleSystem {
			t.Fatalf("unexpected system message: %+v", reqs[0].Messages)
		}
	})

	t.Run("not persisted to memory", func(t *testing.T) {
		// 系统提示词是配置不是历史：注入不得写进记忆，
		// 否则会随历史增长并干扰预算计算。
		llm := &promptCaptureLLM{}
		mem := memorytest.NewBuffer(nil)
		loop := agent.NewLoop(llm, nil, mem, agent.Config{
			Model:        "m",
			SystemPrompt: "You are terse.",
		})
		if _, _, err := loop.Run(context.Background(), "s", "hi"); err != nil {
			t.Fatalf("Run: %v", err)
		}
		msgs, err := mem.Recent(context.Background(), "s", 1<<30)
		if err != nil {
			t.Fatalf("Recent: %v", err)
		}
		for _, m := range msgs {
			if m.Role == core.RoleSystem {
				t.Fatalf("system prompt leaked into memory: %+v", msgs)
			}
		}
		if len(msgs) != 2 { // user + assistant
			t.Fatalf("memory = %d msgs, want 2: %+v", len(msgs), msgs)
		}
	})
}
