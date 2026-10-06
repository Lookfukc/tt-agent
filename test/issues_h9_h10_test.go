package test

import (
	"context"
	"testing"
	"time"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// streamCallLLM 统计 ChatStream 被调次数
type streamCallLLM struct{ calls int }

// Chat 未使用
func (s *streamCallLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream 返回即时结束的空流
func (s *streamCallLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	s.calls++
	events := make(chan core.StreamEvent, 2)
	events <- core.StreamEvent{Type: core.StreamStart}
	events <- core.StreamEvent{Type: core.StreamDone}
	close(events)
	return events, nil
}

// TestH9RateLimitLLMCoversStream RateLimitLLM 必须作用于流式路径
func TestH9RateLimitLLMCoversStream(t *testing.T) {
	inner := &streamCallLLM{}
	limited := core.RateLimitLLM(1, 10*time.Second)(inner)

	if _, err := limited.ChatStream(context.Background(), core.ChatRequest{}); err != nil {
		t.Fatalf("first stream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := limited.ChatStream(ctx, core.ChatRequest{}); err == nil {
		t.Fatal("H9: second stream call should be rate limited")
	}
	if inner.calls != 1 {
		t.Errorf("inner calls = %d, want 1", inner.calls)
	}
}

// usageFirstLLM 先发 Usage 再发文本（Anthropic/Gemini 行为）
type usageFirstLLM struct{ calls int }

// Chat 未使用
func (u *usageFirstLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream 首事件为 Usage，随后失败
func (u *usageFirstLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	u.calls++
	events := make(chan core.StreamEvent, 3)
	events <- core.StreamEvent{Type: core.StreamStart}
	events <- core.StreamEvent{Type: core.StreamUsage, Usage: core.Usage{InputTokens: 5}}
	events <- core.StreamEvent{
		Type: core.StreamError,
		Err:  core.NewError(core.ErrProviderInternal, "x", errFake()),
	}
	close(events)
	return events, nil
}

// TestM_C1UsageNotContentThreshold 用量事件不得视为"已产出内容"，
// 否则 Anthropic/Gemini 的首 token 前重试永远不触发
func TestM_C1UsageNotContentThreshold(t *testing.T) {
	flaky := &usageFirstLLM{}
	wrapped := core.StreamRetry(flaky, 2)

	events, err := wrapped.ChatStream(context.Background(), core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if _, _, serr := core.CollectStream(events); serr == nil {
		t.Fatal("expect stream error surfaced")
	}
	if flaky.calls != 2 {
		t.Fatalf("M-C1: retry not attempted before first content, calls = %d, want 2", flaky.calls)
	}
}

// TestH10CacheKeyDifferentiatesToolArgs 不同的 assistant 工具调用历史不得同键命中
func TestH10CacheKeyDifferentiatesToolArgs(t *testing.T) {
	calls := 0
	inner := core.ChatHandler(func(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
		calls++
		return &core.ChatResponse{Content: "答案"}, nil
	})
	wrapped := core.Cache(time.Minute, 100)(inner)

	base := core.ChatRequest{
		Model: "m",
		Messages: []core.Message{
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
				{ID: "t1", Name: "weather", Arguments: `{"city":"Paris"}`},
			}},
		},
	}
	if _, err := wrapped(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	// 只换工具参数城市
	other := base
	other.Messages = []core.Message{
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
			{ID: "t1", Name: "weather", Arguments: `{"city":"London"}`},
		}},
	}
	if _, err := wrapped(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("H10: tool-arg variant hit wrong cache entry, calls = %d, want 2", calls)
	}
}

// TestH10CacheKeyDifferentiatesImages 不同图片的多模态请求不得同键命中
func TestH10CacheKeyDifferentiatesImages(t *testing.T) {
	calls := 0
	inner := core.ChatHandler(func(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
		calls++
		return &core.ChatResponse{Content: "答案"}, nil
	})
	wrapped := core.Cache(time.Minute, 100)(inner)

	req := func(url string) core.ChatRequest {
		return core.ChatRequest{
			Model:    "m",
			Messages: []core.Message{core.UserImage("这是什么", url)},
		}
	}
	_, _ = wrapped(context.Background(), req("data:image/png;base64,AAA"))
	_, _ = wrapped(context.Background(), req("data:image/png;base64,BBB"))
	if calls != 2 {
		t.Fatalf("H10: image variant hit wrong cache entry, calls = %d, want 2", calls)
	}
}

// TestH10CacheHitReturnsCopy 命中返回深拷贝，写回不得污染缓存
func TestH10CacheHitReturnsCopy(t *testing.T) {
	inner := core.ChatHandler(func(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
		return &core.ChatResponse{
			Content:   "原文",
			ToolCalls: []core.ToolCall{{ID: "t1", Name: "n", Arguments: `{}`}},
		}, nil
	})
	wrapped := core.Cache(time.Minute, 100)(inner)
	req := core.ChatRequest{Model: "m", Messages: []core.Message{core.Text("问")}}

	first, _ := wrapped(context.Background(), req)
	first.Content = "被改了"
	first.ToolCalls[0].Name = "也被改了"

	second, _ := wrapped(context.Background(), req)
	if second.Content != "原文" || second.ToolCalls[0].Name != "n" {
		t.Fatalf("H10: cache polluted by caller mutation: %+v", second)
	}
}

// TestM_C2RetryZeroClamped Retry(0) 不得 panic，至少执行一次
func TestM_C2RetryZeroClamped(t *testing.T) {
	calls := 0
	inner := core.ChatHandler(func(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
		calls++
		return nil, core.NewError(core.ErrInvalidRequest, "x", errFake())
	})
	wrapped := core.Retry(0)(inner)
	if _, err := wrapped(context.Background(), core.ChatRequest{}); err == nil {
		t.Fatal("want error")
	}
	if calls != 1 {
		t.Fatalf("M-C2: calls = %d, want 1 (clamped)", calls)
	}
}

// TestM_C3RateLimitNonPositive RateLimit(0) 不再除零 panic，透传执行
func TestM_C3RateLimitNonPositive(t *testing.T) {
	inner := core.ChatHandler(func(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
		return &core.ChatResponse{Content: "ok"}, nil
	})
	wrapped := core.RateLimit(0, time.Second)(inner)
	resp, err := wrapped(context.Background(), core.ChatRequest{})
	if err != nil || resp.Content != "ok" {
		t.Fatalf("M-C3: RateLimit(0) must pass through, got %v %v", resp, err)
	}
}
