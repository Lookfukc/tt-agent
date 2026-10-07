package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// countingLLM 计数并按脚本返回
type countingLLM struct {
	calls    int
	resp     string
	errValue error
}

// Chat 计数后返回脚本结果
func (c *countingLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	c.calls++
	if c.errValue != nil {
		return nil, c.errValue
	}
	return &core.ChatResponse{Content: c.resp}, nil
}

// ChatStream 返回单文本事件流
func (c *countingLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	c.calls++
	if c.errValue != nil {
		return nil, c.errValue
	}
	events := make(chan core.StreamEvent, 3)
	go func() {
		defer close(events)
		events <- core.StreamEvent{Type: core.StreamStart}
		events <- core.StreamEvent{Type: core.StreamDeltaText, Text: c.resp}
		events <- core.StreamEvent{Type: core.StreamDone}
	}()
	return events, nil
}

func TestCacheHits(t *testing.T) {
	llm := &countingLLM{resp: "答案"}
	p := core.NewPipeline(llm, core.Cache(time.Minute, 100))
	req := core.ChatRequest{Model: "m", Messages: []core.Message{core.Text("问")}}
	for i := 0; i < 3; i++ {
		resp, err := p.Chat(context.Background(), req)
		if err != nil || resp.Content != "答案" {
			t.Fatalf("call %d: resp=%v err=%v", i, resp, err)
		}
	}
	if llm.calls != 1 {
		t.Errorf("calls = %d, want 1 (cache miss only)", llm.calls)
	}

	// 请求变化应失效
	req.Messages[0].Content = "另一个问题"
	_, _ = p.Chat(context.Background(), req)
	if llm.calls != 2 {
		t.Errorf("calls = %d, want 2 after key change", llm.calls)
	}
}

func TestCacheSkipsTools(t *testing.T) {
	llm := &countingLLM{resp: "答案"}
	p := core.NewPipeline(llm, core.Cache(time.Minute, 100))
	req := core.ChatRequest{
		Model: "m",
		Tools: []core.ToolSpec{{Name: "echo"}},
	}
	_, _ = p.Chat(context.Background(), req)
	_, _ = p.Chat(context.Background(), req)
	if llm.calls != 2 {
		t.Errorf("tool-carrying requests must bypass cache, calls = %d", llm.calls)
	}
}

func TestFallbackLLMChat(t *testing.T) {
	primary := &countingLLM{errValue: core.NewError(core.ErrProviderInternal, "p", errors.New("down"))}
	alternate := &countingLLM{resp: "备用"}
	fb := core.FallbackLLM(primary, alternate)

	resp, err := fb.Chat(context.Background(), core.ChatRequest{})
	if err != nil || resp.Content != "备用" {
		t.Fatalf("resp=%v err=%v", resp, err)
	}

	// 不可重试错误不切备
	primary2 := &countingLLM{errValue: core.NewError(core.ErrInvalidRequest, "p", errors.New("bad"))}
	alternate2 := &countingLLM{resp: "不应出现"}
	fb2 := core.FallbackLLM(primary2, alternate2)
	if _, err := fb2.Chat(context.Background(), core.ChatRequest{}); err == nil {
		t.Error("non-retryable should propagate")
	}
	if alternate2.calls != 0 {
		t.Error("alternate must not be called for non-retryable errors")
	}
}

func TestFallbackLLMStream(t *testing.T) {
	// 主 LLM 建连即失败（可重试），切备成功
	primary := &countingLLM{errValue: core.NewError(core.ErrNetwork, "p", errors.New("unreachable"))}
	alternate := &countingLLM{resp: "备用流"}
	fb := core.FallbackLLM(primary, alternate)

	events, err := fb.ChatStream(context.Background(), core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	msg, _, serr := core.CollectStream(events)
	if serr != nil || msg.Content != "备用流" {
		t.Fatalf("msg=%q serr=%v", msg.Content, serr)
	}
}

func TestRateLimitBlocks(t *testing.T) {
	llm := &countingLLM{resp: "ok"}
	p := core.NewPipeline(llm, core.RateLimit(1, 10*time.Second))
	if _, err := p.Chat(context.Background(), core.ChatRequest{}); err != nil {
		t.Fatalf("first call: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := p.Chat(ctx, core.ChatRequest{}); err == nil {
		t.Error("second call should be rate limited")
	}
	if llm.calls != 1 {
		t.Errorf("calls = %d, want 1", llm.calls)
	}
}
