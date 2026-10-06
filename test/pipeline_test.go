package test

import (
	"context"
	"errors"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// flakyLLM 前 failN 次返回可重试错误，之后成功
type flakyLLM struct {
	failN int
	calls int
}

// Chat 按失败计数返回错误或成功响应
func (f *flakyLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	f.calls++
	if f.calls <= f.failN {
		return nil, core.NewError(core.ErrProviderInternal, "t", errors.New("boom"))
	}
	return &core.ChatResponse{Content: "ok"}, nil
}

// ChatStream 流式失败：首事件即 StreamError（未产出内容）
func (f *flakyLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	f.calls++
	events := make(chan core.StreamEvent, 2)
	if f.calls <= f.failN {
		events <- core.StreamEvent{Type: core.StreamError, Err: core.NewError(core.ErrProviderInternal, "t", errors.New("boom"))}
		close(events)
		return events, nil
	}
	go func() {
		defer close(events)
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "ok"}
		events <- core.StreamEvent{Type: core.StreamDone}
	}()
	return events, nil
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	flaky := &flakyLLM{failN: 2}
	p := core.NewPipeline(flaky, core.Retry(3))
	resp, err := p.Chat(context.Background(), core.ChatRequest{})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if resp.Content != "ok" || flaky.calls != 3 {
		t.Errorf("resp=%q calls=%d", resp.Content, flaky.calls)
	}
}

func TestRetryExhausts(t *testing.T) {
	flaky := &flakyLLM{failN: 10}
	p := core.NewPipeline(flaky, core.Retry(2))
	_, err := p.Chat(context.Background(), core.ChatRequest{})
	var ce *core.Error
	if !errors.As(err, &ce) || ce.Kind != core.ErrExhausted {
		t.Fatalf("err = %+v, want ErrExhausted", err)
	}
}

func TestNonRetryableFailsFast(t *testing.T) {
	calls := 0
	bad := core.ChatHandler(func(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
		calls++
		return nil, core.NewError(core.ErrInvalidRequest, "t", errors.New("bad"))
	})
	wrapped := core.Retry(5)(bad)
	if _, err := wrapped(context.Background(), core.ChatRequest{}); err == nil {
		t.Fatal("want error")
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestStreamRetryBeforeFirstToken(t *testing.T) {
	flaky := &flakyLLM{failN: 1}
	wrapped := core.StreamRetry(flaky, 3)
	events, err := wrapped.ChatStream(context.Background(), core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	msg, _, serr := core.CollectStream(events)
	if serr != nil {
		t.Fatalf("stream err: %v", serr)
	}
	if msg.Content != "ok" {
		t.Errorf("content = %q", msg.Content)
	}
	if flaky.calls != 2 {
		t.Errorf("calls = %d, want 2", flaky.calls)
	}
}

// fatalStreamLLM 永远返回不可重试的流错误
type fatalStreamLLM struct{ calls int }

// Chat 未使用
func (f *fatalStreamLLM) Chat(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("not implemented")
}

// ChatStream 返回鉴权类流错误
func (f *fatalStreamLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	f.calls++
	events := make(chan core.StreamEvent, 1)
	events <- core.StreamEvent{Type: core.StreamError, Err: core.NewError(core.ErrAuth, "t", errors.New("bad key"))}
	close(events)
	return events, nil
}

func TestStreamRetryNonRetryablePassesThrough(t *testing.T) {
	fatal := &fatalStreamLLM{}
	wrapped := core.StreamRetry(fatal, 3)
	events, err := wrapped.ChatStream(context.Background(), core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if _, _, serr := core.CollectStream(events); serr == nil {
		t.Fatal("want stream error passed through")
	}
	if fatal.calls != 1 {
		t.Errorf("calls = %d, want 1", fatal.calls)
	}
}
