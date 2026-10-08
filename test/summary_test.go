package test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
)

// summaryLLM is a fixed-summary mock that records call counts
type summaryLLM struct{ calls atomic.Int32 }

// Chat returns fixed summary text
func (s *summaryLLM) Chat(_ context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	s.calls.Add(1)
	if len(req.Messages) < 2 || req.Messages[1].Content == "" {
		return nil, errors.New("empty input")
	}
	return &core.ChatResponse{Content: "用户问了数字，助手答了数字"}, nil
}

// ChatStream is unused
func (s *summaryLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	return nil, errors.New("not implemented")
}

func TestSummaryMemory(t *testing.T) {
	ctx := context.Background()
	inner := memorytest.NewBuffer(nil)
	llm := &summaryLLM{}
	mem := memory.NewSummary(inner, llm)

	// System message + 10 alternating conversation messages
	msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
	for i := 0; i < 5; i++ {
		msgs = append(msgs,
			core.Message{Role: core.RoleUser, Content: "很长的用户消息数字" + string(rune('A'+i)) + "内容内容内容内容"},
			core.Message{Role: core.RoleAssistant, Content: "很长的助手回答数字" + string(rune('A'+i)) + "内容内容内容内容"},
		)
	}
	if err := mem.Add(ctx, "s1", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// When the budget only fits 4 messages, old messages should be replaced by a summary
	got, err := mem.Recent(ctx, "s1", 30)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) >= len(msgs) {
		t.Fatalf("len = %d, want compressed below %d", len(got), len(msgs))
	}
	hasSummary := false
	for _, m := range got {
		if m.Role == core.RoleSystem && m.Content != "sys" {
			hasSummary = true
		}
	}
	if !hasSummary {
		t.Errorf("no summary message: %+v", got)
	}
	if llm.calls.Load() != 1 {
		t.Errorf("compress calls = %d, want 1", llm.calls.Load())
	}

	// Fetching again with the same budget hits the cache without recompressing
	_, _ = mem.Recent(ctx, "s1", 30)
	if llm.calls.Load() != 1 {
		t.Errorf("cache miss, calls = %d", llm.calls.Load())
	}

	// The inner store is not polluted and still holds all messages
	all, _ := inner.Recent(ctx, "s1", 1<<62)
	if len(all) != len(msgs) {
		t.Errorf("inner mutated: %d msgs", len(all))
	}
}

func TestSummaryFallsBackOnLLMFailure(t *testing.T) {
	ctx := context.Background()
	inner := memorytest.NewBuffer(nil)
	mem := memory.NewSummary(inner, alwaysFailLLM{})

	msgs := make([]core.Message, 0, 10)
	for i := 0; i < 5; i++ {
		msgs = append(msgs,
			core.Message{Role: core.RoleUser, Content: "长消息长消息长消息"},
			core.Message{Role: core.RoleAssistant, Content: "长回复长回复长回复"},
		)
	}
	_ = mem.Add(ctx, "s1", msgs...)

	got, err := mem.Recent(ctx, "s1", 10)
	if err != nil {
		t.Fatalf("Recent should not fail when compression fails: %v", err)
	}
	if len(got) >= len(msgs) {
		t.Errorf("len = %d, truncation should still apply", len(got))
	}
}

// alwaysFailLLM always fails
type alwaysFailLLM struct{}

// Chat returns an error
func (alwaysFailLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("summarizer down")
}

// ChatStream returns an error
func (alwaysFailLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	return nil, errors.New("summarizer down")
}
