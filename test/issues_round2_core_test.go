package test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// emptyCleanLLM 返回干净但无内容的流（OpenAI 空补全形态：
// Start + usage + Done，无任何 delta 事件）
type emptyCleanLLM struct{ calls int }

// Chat 未使用
func (e *emptyCleanLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream 产出干净空流
func (e *emptyCleanLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	e.calls++
	events := make(chan core.StreamEvent, 3)
	events <- core.StreamEvent{Type: core.StreamStart}
	events <- core.StreamEvent{Type: core.StreamUsage, Usage: core.Usage{InputTokens: 7}}
	events <- core.StreamEvent{Type: core.StreamDone, FinishReason: core.FinishStop}
	close(events)
	return events, nil
}

// altContentLLM 备用 LLM：返回带文本的正常流并计数
type altContentLLM struct{ calls int }

// Chat 未使用
func (a *altContentLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream 返回 Start+文本+Done 流
func (a *altContentLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	a.calls++
	events := make(chan core.StreamEvent, 3)
	events <- core.StreamEvent{Type: core.StreamStart}
	events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "备用"}
	events <- core.StreamEvent{Type: core.StreamDone}
	close(events)
	return events, nil
}

// consumeAllEvents 全量消费流，返回事件类型序列与 usage 事件用量
func consumeAllEvents(events <-chan core.StreamEvent) ([]core.StreamEventType, core.Usage) {
	var types []core.StreamEventType
	var usage core.Usage
	for e := range events {
		types = append(types, e.Type)
		if e.Type == core.StreamUsage {
			usage = e.Usage
		}
	}
	return types, usage
}

// matchEventTypes 逐位比较事件类型序列
func matchEventTypes(got []core.StreamEventType, want ...core.StreamEventType) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestN2StreamRetryCleanEmptyStreamSucceeds 干净空流（无内容也无
// StreamError）是成功：不得进入重试，且缓冲事件（含 usage/Done）
// 必须完整重放给消费者
func TestN2StreamRetryCleanEmptyStreamSucceeds(t *testing.T) {
	empty := &emptyCleanLLM{}
	wrapped := core.StreamRetry(empty, 3)

	events, err := wrapped.ChatStream(context.Background(), core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	types, usage := consumeAllEvents(events)
	if !matchEventTypes(types, core.StreamStart, core.StreamUsage, core.StreamDone) {
		t.Fatalf("N2: event sequence = %v, want [Start Usage Done]", types)
	}
	if usage.InputTokens != 7 {
		t.Fatalf("N2: usage lost in replay, input tokens = %d, want 7", usage.InputTokens)
	}
	if empty.calls != 1 {
		t.Fatalf("N2: clean empty stream must not be retried, calls = %d, want 1", empty.calls)
	}
}

// TestN1FallbackLLMCleanEmptyStreamStaysOnPrimary 主 LLM 的干净空流
// 是成功：不得切备，消费者拿到主 LLM 的完整事件
func TestN1FallbackLLMCleanEmptyStreamStaysOnPrimary(t *testing.T) {
	primary := &emptyCleanLLM{}
	alternate := &altContentLLM{}
	fb := core.FallbackLLM(primary, alternate)

	events, err := fb.ChatStream(context.Background(), core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	types, usage := consumeAllEvents(events)
	if !matchEventTypes(types, core.StreamStart, core.StreamUsage, core.StreamDone) {
		t.Fatalf("N1: event sequence = %v, want primary's [Start Usage Done]", types)
	}
	if usage.InputTokens != 7 {
		t.Fatalf("N1: usage lost in replay, input tokens = %d, want 7", usage.InputTokens)
	}
	if alternate.calls != 0 {
		t.Fatalf("N1: alternate must not be called for clean empty stream, calls = %d", alternate.calls)
	}
	if primary.calls != 1 {
		t.Fatalf("N1: primary calls = %d, want 1", primary.calls)
	}
}

// slowEmitLLM 向无缓冲通道逐个发送事件，全部发出后关闭通道并经 done 通知
type slowEmitLLM struct {
	done  chan struct{}
	total int
}

// Chat 未使用
func (s *slowEmitLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream 慢速生产：事件数远超中间件转发缓冲，生产者收尾以 done 关闭为号
func (s *slowEmitLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	events := make(chan core.StreamEvent)
	go func() {
		defer close(s.done)
		defer close(events)
		events <- core.StreamEvent{Type: core.StreamStart}
		for i := 0; i < s.total; i++ {
			events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "x"}
		}
	}()
	return events, nil
}

// TestN3LoggingLLMAbandonedConsumerTerminates 消费者取消 ctx 并停止
// 读取后，loggingLLM 的转发 goroutine 必须退出并排空源流，上游生产者
// 才能发完全部事件并关闭通道
//
// 局限：goroutine 退出无导出信号，这里通过"生产者能收尾（done 关闭）"
// 间接验证；转发 goroutine 正常退出后 out 会被 close，末尾补一次
// 全量消费确认无死锁
func TestN3LoggingLLMAbandonedConsumerTerminates(t *testing.T) {
	source := &slowEmitLLM{done: make(chan struct{}), total: 50}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	wrapped := core.LoggingLLM(logger)(source)

	ctx, cancel := context.WithCancel(context.Background())
	events, err := wrapped.ChatStream(ctx, core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if e := <-events; e.Type != core.StreamStart {
		t.Fatalf("first event type = %v, want StreamStart", e.Type)
	}
	// 模拟消费者放弃：取消后不再读取
	cancel()

	select {
	case <-source.done:
	case <-time.After(3 * time.Second):
		t.Fatal("N3: copy goroutine blocked forever, upstream producer cannot finish")
	}
	// 转发 goroutine 退出时 close(out)，range 必须能自然终止
	for range events {
	}
}
