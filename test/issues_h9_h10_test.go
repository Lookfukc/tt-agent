package test

import (
	"context"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// streamCallLLM counts how many times ChatStream was invoked
type streamCallLLM struct{ calls int }

// Chat is unused
func (s *streamCallLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream returns an immediately-ending empty stream
func (s *streamCallLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	s.calls++
	events := make(chan core.StreamEvent, 2)
	events <- core.StreamEvent{Type: core.StreamStart}
	events <- core.StreamEvent{Type: core.StreamDone}
	close(events)
	return events, nil
}

// TestH9RateLimitLLMCoversStream verifies RateLimitLLM must apply to the streaming path
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

// usageFirstLLM emits Usage before text (Anthropic/Gemini behavior)
type usageFirstLLM struct{ calls int }

// Chat is unused
func (u *usageFirstLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream emits Usage as the first event, then fails
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

// TestM_C1UsageNotContentThreshold: usage events must not count as "content produced",
// otherwise the pre-first-token retry for Anthropic/Gemini never triggers
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

// TestH10CacheKeyDifferentiatesToolArgs verifies different assistant tool-call histories must not hit the same cache key
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
	// Vary only the tool argument (the city)
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

// TestH10CacheKeyDifferentiatesImages verifies multimodal requests with different images must not hit the same cache key
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

// TestH10CacheHitReturnsCopy verifies a hit returns a deep copy; writing it back must not pollute the cache
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

// TestM_C2RetryZeroClamped verifies Retry(0) must not panic and executes at least once
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

// TestM_C3RateLimitNonPositive verifies RateLimit(0) no longer panics on divide-by-zero and passes through
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
