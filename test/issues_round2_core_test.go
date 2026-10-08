package test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// emptyCleanLLM returns a clean but contentless stream (the OpenAI empty-completion shape:
// Start + usage + Done, with no delta events at all)
type emptyCleanLLM struct{ calls int }

// Chat is unused
func (e *emptyCleanLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream produces a clean empty stream
func (e *emptyCleanLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	e.calls++
	events := make(chan core.StreamEvent, 3)
	events <- core.StreamEvent{Type: core.StreamStart}
	events <- core.StreamEvent{Type: core.StreamUsage, Usage: core.Usage{InputTokens: 7}}
	events <- core.StreamEvent{Type: core.StreamDone, FinishReason: core.FinishStop}
	close(events)
	return events, nil
}

// altContentLLM is the alternate LLM: returns a normal stream with text and counts calls
type altContentLLM struct{ calls int }

// Chat is unused
func (a *altContentLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream returns a Start+text+Done stream
func (a *altContentLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	a.calls++
	events := make(chan core.StreamEvent, 3)
	events <- core.StreamEvent{Type: core.StreamStart}
	events <- core.StreamEvent{Type: core.StreamDeltaText, Text: "备用"}
	events <- core.StreamEvent{Type: core.StreamDone}
	close(events)
	return events, nil
}

// consumeAllEvents fully consumes the stream, returning the event type sequence and the usage from usage events
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

// matchEventTypes compares event type sequences position by position
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

// TestN2StreamRetryCleanEmptyStreamSucceeds: a clean empty stream (no content and
// no StreamError) is a success: it must not enter retry, and the buffered events
// (including usage/Done) must be replayed to the consumer in full
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

// TestN1FallbackLLMCleanEmptyStreamStaysOnPrimary: a clean empty stream from the
// primary LLM is a success: it must not switch to the alternate; the consumer
// receives the primary LLM's full events
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

// slowEmitLLM sends events one by one into an unbuffered channel, closing it after all are sent and notifying via done
type slowEmitLLM struct {
	done  chan struct{}
	total int
}

// Chat is unused
func (s *slowEmitLLM) Chat(_ context.Context, _ core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}

// ChatStream produces slowly: the event count far exceeds the middleware's forwarding buffer; the producer finishes, signaled by closing done
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

// TestN3LoggingLLMAbandonedConsumerTerminates: after the consumer cancels the ctx
// and stops reading, the loggingLLM's forwarding goroutine must exit and drain
// the source stream, so the upstream producer can finish sending all events
// and close the channel
//
// Limitation: there is no exported signal for the goroutine's exit, so this is
// verified indirectly via "the producer can finish (done closed)"; when the
// forwarding goroutine exits normally, out gets closed, and a final full
// consumption pass at the end confirms there is no deadlock
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
	// Simulate an abandoning consumer: cancel and stop reading
	cancel()

	select {
	case <-source.done:
	case <-time.After(3 * time.Second):
		t.Fatal("N3: copy goroutine blocked forever, upstream producer cannot finish")
	}
	// The forwarding goroutine closes out on exit, so range must terminate naturally
	for range events {
	}
}
