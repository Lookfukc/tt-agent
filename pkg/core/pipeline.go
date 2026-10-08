package core

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"
)

// ChatHandler is the handler in the Chat call chain.
type ChatHandler func(ctx context.Context, req ChatRequest) (*ChatResponse, error)

// ChatMiddleware is middleware for the Chat call chain.
type ChatMiddleware func(next ChatHandler) ChatHandler

// Pipeline is the middleware chain for LLM calls; it itself implements the
// LLM interface.
type Pipeline struct {
	chat   ChatHandler
	stream StreamHandler
}

// StreamHandler is the handler in the ChatStream call chain.
//
// Streaming calls can only be intercepted before the request is sent; once
// the connection is established, middleware can no longer be inserted.
type StreamHandler func(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error)

// NewPipeline constructs the middleware chain.
// llm: the real LLM being wrapped
// middlewares: applied in order; those registered first execute first
// returns: the chain instance implementing the LLM interface
func NewPipeline(llm LLM, middlewares ...ChatMiddleware) *Pipeline {
	p := &Pipeline{
		chat:   llm.Chat,
		stream: llm.ChatStream,
	}
	for i := len(middlewares) - 1; i >= 0; i-- {
		p.chat = middlewares[i](p.chat)
	}
	return p
}

// Chat performs a non-streaming call through the full middleware chain.
func (p *Pipeline) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	return p.chat(ctx, req)
}

// ChatStream performs a streaming call.
//
// It does not go through the ChatMiddleware chain (that middleware only
// covers Chat); when streaming needs Logging/RateLimit coverage, use the
// LLMMiddleware form (LoggingLLM/RateLimitLLM) or the StreamRetry wrapper.
func (p *Pipeline) ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error) {
	return p.stream(ctx, req)
}

// Logging records the elapsed time and outcome of every call.
// logger: the logger; nil selects the default
// returns: the middleware
func Logging(logger *slog.Logger) ChatMiddleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next ChatHandler) ChatHandler {
		return func(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			if err != nil {
				logger.WarnContext(ctx, "llm chat failed",
					"model", req.Model, "elapsed", time.Since(start).String(), "err", err)
				return nil, err
			}
			logger.InfoContext(ctx, "llm chat",
				"model", req.Model,
				"elapsed", time.Since(start).String(),
				"input_tokens", resp.Usage.InputTokens,
				"output_tokens", resp.Usage.OutputTokens,
				"tool_calls", len(resp.ToolCalls))
			return resp, nil
		}
	}
}

// Retry retries non-streaming calls.
// maxAttempts: the cap on total attempts including the first; values below 1
// are treated as 1
// returns: the middleware
func Retry(maxAttempts int) ChatMiddleware {
	if maxAttempts < 1 {
		// Zero or negative would make the loop body never execute, leaving
		// lastErr nil and panicking outright.
		maxAttempts = 1
	}
	return func(next ChatHandler) ChatHandler {
		return func(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
			var lastErr error
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				resp, err := next(ctx, req)
				if err == nil {
					return resp, nil
				}
				lastErr = err
				if !Retryable(err) || ctx.Err() != nil {
					return nil, err
				}
				if attempt == maxAttempts {
					break
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff(attempt, err)):
				}
			}
			exhausted := NewError(ErrExhausted, providerOf(lastErr), lastErr)
			return nil, exhausted
		}
	}
}

// providerOf extracts the provider identifier from the underlying error so
// the exhausted error does not lose context.
//
// It uses errors.As rather than a direct type assertion: *Error values
// wrapped by fmt.Errorf("%w") and the like must also be able to contribute
// their ProviderID.
// returns: the provider ID, or the empty string if none
func providerOf(err error) string {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.ProviderID
	}
	return ""
}

// StreamRetry retries streaming calls before the first token.
//
// A stream that has already emitted content cannot be rolled back and
// replayed, so events are buffered only up to the first meaningful event:
// failures before that point can be safely retried, failures after it are
// passed through as-is.
// maxAttempts: the cap on total attempts including the first
// returns: a new instance wrapping the LLM
func StreamRetry(llm LLM, maxAttempts int) LLM {
	return streamRetryLLM{llm: llm, maxAttempts: maxAttempts}
}

type streamRetryLLM struct {
	llm         LLM
	maxAttempts int
}

// Chat passes through to the underlying LLM.
func (s streamRetryLLM) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	return s.llm.Chat(ctx, req)
}

// ChatStream performs a streaming call that retries only on failure before
// the first content event.
func (s streamRetryLLM) ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error) {
	for attempt := 1; attempt < s.maxAttempts; attempt++ {
		// Each attempt derives its own ctx; when a retry abandons the old
		// stream, canceling it releases the producer.
		attemptCtx, cancel := context.WithCancel(ctx)
		events, err := s.llm.ChatStream(attemptCtx, req)
		if err != nil {
			cancel()
			if !Retryable(err) || ctx.Err() != nil {
				return nil, err
			}
			if !sleep(ctx, backoff(attempt, err)) {
				return nil, ctx.Err()
			}
			continue
		}

		var pending []StreamEvent
		var streamErr error
		for e := range events {
			if e.Type == StreamError {
				cancel()
				streamErr = e.Err
				if Retryable(e.Err) && ctx.Err() == nil {
					break
				}
				// Fatal errors pass through: replay the buffered events first,
				// then append the error event so usage is not lost.
				pending = append(pending, e)
				return replay(pending), nil
			}
			pending = append(pending, e)
			if isContentEvent(e) {
				// After the first content event no replay is possible; take
				// over the stream directly.
				return splice(ctx, pending, events, cancel), nil
			}
		}
		cancel()
		if streamErr == nil {
			// A stream that ends cleanly while empty (e.g. an OpenAI empty
			// completion: Start+Done with no content) is a success; replaying
			// the buffered events (including Done and usage) is enough — it
			// must not be retried.
			return replay(pending), nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Retrying after a mid-stream StreamError: the backoff must carry the
		// original error so Retry-After can be honored.
		if !sleep(ctx, backoff(attempt, streamErr)) {
			return nil, ctx.Err()
		}
	}
	return s.llm.ChatStream(ctx, req)
}

// isContentEvent reports whether an event has already produced substantive
// content.
//
// StreamUsage does not count as content: Anthropic/Gemini emit a usage event
// before any text, and treating it as the threshold would prematurely break
// the "retryable before first token" semantics.
// returns: true if there is already output that cannot be rolled back
func isContentEvent(e StreamEvent) bool {
	switch e.Type {
	case StreamDeltaText, StreamDeltaReasoning, StreamDeltaToolCall:
		return true
	default:
		return false
	}
}

// splice concatenates the buffered events with the remaining events into a
// new stream, releasing the derived ctx after draining.
//
// When the consumer abandons mid-stream (ctx canceled), drain rest until it
// closes before exiting; the producer (adapter) exits via cancellation of
// the same ctx, leaving no blocked senders behind.
func splice(ctx context.Context, pending []StreamEvent, rest <-chan StreamEvent, cancel context.CancelFunc) <-chan StreamEvent {
	out := make(chan StreamEvent, len(pending)+8)
	go func() {
		defer close(out)
		defer cancel()
		for _, e := range pending {
			select {
			case out <- e:
			case <-ctx.Done():
				drain(rest)
				return
			}
		}
		for e := range rest {
			select {
			case out <- e:
			case <-ctx.Done():
				drain(rest)
				return
			}
		}
	}()
	return out
}

// drain discards everything so the producer can finish and exit normally.
func drain(rest <-chan StreamEvent) {
	for range rest {
	}
}

// replay replays fully consumed buffered events as a new stream that ends
// immediately.
//
// The source stream is closed and fully read out, so the buffer is all the
// remaining content: reserving capacity by len means sends never block, and
// no extra goroutine is needed.
// returns: the replayed new event stream
func replay(pending []StreamEvent) <-chan StreamEvent {
	out := make(chan StreamEvent, len(pending))
	for _, e := range pending {
		out <- e
	}
	close(out)
	return out
}

// backoff computes the exponential backoff duration; rate-limit errors
// prefer Retry-After.
// attempt: the current attempt number, starting from 1
// err: the error that triggered the retry
// returns: the wait duration
func backoff(attempt int, err error) time.Duration {
	if ce, ok := err.(*Error); ok && ce.RetryAfter != nil {
		return *ce.RetryAfter
	}
	// Jitter prevents concurrent requests from retrying in lockstep and
	// forming a spike.
	d := time.Duration(1<<uint(min(attempt, 5))) * 200 * time.Millisecond
	return d + time.Duration(rand.Int63n(int64(d/2)))
}

// sleep waits interruptibly.
// returns: false if ctx has already been canceled
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
