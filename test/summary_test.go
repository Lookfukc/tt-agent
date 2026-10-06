package test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/core"
	"github.com/Lookfukc/send-agent/pkg/memory"
)

// summaryLLM 记录调用次数的固定摘要 mock
type summaryLLM struct{ calls atomic.Int32 }

// Chat 返回固定摘要文本
func (s *summaryLLM) Chat(_ context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	s.calls.Add(1)
	if len(req.Messages) < 2 || req.Messages[1].Content == "" {
		return nil, errors.New("empty input")
	}
	return &core.ChatResponse{Content: "用户问了数字，助手答了数字"}, nil
}

// ChatStream 未使用
func (s *summaryLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	return nil, errors.New("not implemented")
}

func TestSummaryMemory(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewBuffer(nil)
	llm := &summaryLLM{}
	mem := memory.NewSummary(inner, llm)

	// 系统消息 + 10 条交替对话
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

	// 预算只够 4 条时，旧消息应被摘要替代
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

	// 同预算再取，命中缓存不重复压缩
	_, _ = mem.Recent(ctx, "s1", 30)
	if llm.calls.Load() != 1 {
		t.Errorf("cache miss, calls = %d", llm.calls.Load())
	}

	// 内层存储未被污染，仍保留全部消息
	all, _ := inner.Recent(ctx, "s1", 1<<62)
	if len(all) != len(msgs) {
		t.Errorf("inner mutated: %d msgs", len(all))
	}
}

func TestSummaryFallsBackOnLLMFailure(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewBuffer(nil)
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

// alwaysFailLLM 永远失败
type alwaysFailLLM struct{}

// Chat 返回错误
func (alwaysFailLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("summarizer down")
}

// ChatStream 返回错误
func (alwaysFailLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	return nil, errors.New("summarizer down")
}
