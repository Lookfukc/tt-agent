package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// LLMMiddleware is LLM-level middleware.
//
// ChatMiddleware can only cover the non-streaming path, while the server's
// main path (the Agent loop) is always streaming; cross-cutting logic that
// must apply to both paths has to hook in at this layer.
type LLMMiddleware func(LLM) LLM

// LoggingLLM is a dual-path logging middleware.
// logger: the logger; nil selects the default
// returns: the LLM middleware
func LoggingLLM(logger *slog.Logger) LLMMiddleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next LLM) LLM {
		return &loggingLLM{next: next, logger: logger}
	}
}

type loggingLLM struct {
	next   LLM
	logger *slog.Logger
}

// Chat logs non-streaming calls.
func (l *loggingLLM) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	start := time.Now()
	resp, err := l.next.Chat(ctx, req)
	if err != nil {
		l.logger.WarnContext(ctx, "llm chat failed",
			"model", req.Model, "elapsed", time.Since(start).String(), "err", err)
		return nil, err
	}
	l.logger.InfoContext(ctx, "llm chat",
		"model", req.Model,
		"elapsed", time.Since(start).String(),
		"input_tokens", resp.Usage.InputTokens,
		"output_tokens", resp.Usage.OutputTokens,
		"tool_calls", len(resp.ToolCalls))
	return resp, nil
}

// ChatStream logs streaming calls; event counts and the terminal status are
// emitted after the stream closes.
func (l *loggingLLM) ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error) {
	start := time.Now()
	events, err := l.next.ChatStream(ctx, req)
	if err != nil {
		l.logger.WarnContext(ctx, "llm stream failed to start",
			"model", req.Model, "err", err)
		return nil, err
	}
	out := make(chan StreamEvent, 16)
	go func() {
		defer close(out)
		var count int
		var finalErr error
		var usage Usage
		for e := range events {
			if e.Type == StreamError {
				finalErr = e.Err
			}
			if e.Type == StreamUsage {
				usage = e.Usage
			}
			count++
			select {
			case out <- e:
			case <-ctx.Done():
				// The consumer has given up: synchronously drain the source
				// stream so the upstream producer can wrap up and exit.
				drain(events)
				l.logger.WarnContext(ctx, "llm stream interrupted",
					"model", req.Model, "events", count,
					"elapsed", time.Since(start).String(), "err", ctx.Err())
				return
			}
		}
		if finalErr != nil {
			l.logger.WarnContext(ctx, "llm stream ended with error",
				"model", req.Model, "events", count,
				"elapsed", time.Since(start).String(), "err", finalErr)
			return
		}
		l.logger.InfoContext(ctx, "llm stream",
			"model", req.Model, "events", count,
			"elapsed", time.Since(start).String(),
			"input_tokens", usage.InputTokens,
			"output_tokens", usage.OutputTokens)
	}()
	return out, nil
}

// tokenBucket is a token bucket with no background goroutine.
//
// Refill is time-driven and on demand, avoiding Ticker leaks.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	max      float64
	refillPS float64 // refill rate per second
	last     time.Time
}

// newTokenBucket constructs a full bucket.
// n: the window capacity
// window: how long it takes to refill to full
// returns: the token bucket
func newTokenBucket(n float64, window time.Duration) *tokenBucket {
	return &tokenBucket{
		tokens:   n,
		max:      n,
		refillPS: n / window.Seconds(),
		last:     time.Now(),
	}
}

// acquire takes one token, waiting when none is available or returning early
// if ctx is canceled.
// returns: false if ctx was canceled
func (b *tokenBucket) acquire(ctx context.Context) bool {
	for {
		b.mu.Lock()
		now := time.Now()
		b.tokens += now.Sub(b.last).Seconds() * b.refillPS
		if b.tokens > b.max {
			b.tokens = b.max
		}
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return true
		}
		wait := time.Duration((1 - b.tokens) / b.refillPS * float64(time.Second))
		b.mu.Unlock()
		if wait <= 0 {
			wait = time.Millisecond
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
}

// RateLimit is a fixed-rate limiting middleware (non-streaming path).
//
// n<=0 or window<=0 means no limiting; requests pass straight through.
// n: number of calls allowed within the time window
// window: length of the time window
// returns: the middleware
func RateLimit(n int, window time.Duration) ChatMiddleware {
	return func(next ChatHandler) ChatHandler {
		if n <= 0 || window <= 0 {
			return next
		}
		bucket := newTokenBucket(float64(n), window)
		return func(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
			if !bucket.acquire(ctx) {
				return nil, NewError(ErrCanceled, "", ctx.Err())
			}
			return next(ctx, req)
		}
	}
}

// RateLimitLLM is a dual-path rate-limiting middleware.
// n: number of calls allowed within the time window; n<=0 means no limiting
// window: length of the time window
// returns: the LLM middleware
func RateLimitLLM(n int, window time.Duration) LLMMiddleware {
	return func(next LLM) LLM {
		if n <= 0 || window <= 0 {
			return next
		}
		bucket := newTokenBucket(float64(n), window)
		return &rateLimitedLLM{next: next, bucket: bucket}
	}
}

type rateLimitedLLM struct {
	next   LLM
	bucket *tokenBucket
}

// Chat passes through after rate limiting.
func (r *rateLimitedLLM) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	if !r.bucket.acquire(ctx) {
		return nil, NewError(ErrCanceled, "", ctx.Err())
	}
	return r.next.Chat(ctx, req)
}

// ChatStream passes through after rate limiting.
func (r *rateLimitedLLM) ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error) {
	if !r.bucket.acquire(ctx) {
		return nil, NewError(ErrCanceled, "", ctx.Err())
	}
	return r.next.ChatStream(ctx, req)
}

// FallbackLLM provides primary/alternate failover.
//
// Non-streaming: switch to the alternate as soon as the primary fails.
// Streaming: switch only if the failure happens before the first content
// event; switching after content has been produced would duplicate the output.
// primary: the primary LLM
// alternate: the fallback LLM
// returns: the wrapped LLM
func FallbackLLM(primary, alternate LLM) LLM {
	return &fallbackLLM{primary: primary, alternate: alternate}
}

type fallbackLLM struct {
	primary   LLM
	alternate LLM
}

// Chat retries with the alternate LLM when the primary fails.
func (f *fallbackLLM) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	resp, err := f.primary.Chat(ctx, req)
	if err == nil || !Retryable(err) {
		return resp, err
	}
	return f.alternate.Chat(ctx, req)
}

// ChatStream switches to the alternate only if the primary fails before the
// first content event.
func (f *fallbackLLM) ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error) {
	events, err := f.primary.ChatStream(ctx, req)
	if err != nil {
		if !Retryable(err) {
			return nil, err
		}
		return f.alternate.ChatStream(ctx, req)
	}
	var pending []StreamEvent
	var streamErr error
	for e := range events {
		if e.Type == StreamError {
			streamErr = e.Err
			if !Retryable(e.Err) {
				// Fatal errors pass through: replay the buffered events first,
				// then append the error event so usage is not lost.
				pending = append(pending, e)
				return replay(pending), nil
			}
			break
		}
		pending = append(pending, e)
		if isContentEvent(e) {
			return relay(ctx, pending, events), nil
		}
	}
	if streamErr == nil {
		// A stream that ends cleanly while empty (e.g. an OpenAI empty
		// completion: Start+Done with no content) is a success; replaying the
		// primary's buffered events is enough — it must not be misjudged as a
		// failure and switched to the alternate.
		return relay(ctx, pending, events), nil
	}
	// The primary failed without producing content; switch to the alternate
	// and retry. A failure to establish the alternate connection happens
	// before the first event, so per the contract it goes through the error
	// return value rather than an in-stream event.
	altEvents, err := f.alternate.ChatStream(ctx, req)
	if err != nil {
		return nil, err
	}
	return altEvents, nil
}

// relay concatenates the buffered events with the remaining events into a new
// stream; if the consumer abandons, drain and exit.
func relay(ctx context.Context, pending []StreamEvent, rest <-chan StreamEvent) <-chan StreamEvent {
	out := make(chan StreamEvent, len(pending)+8)
	go func() {
		defer close(out)
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

// cacheEntry is a cache entry.
type cacheEntry struct {
	resp      *ChatResponse
	expiresAt time.Time
}

// Cache is a response-cache middleware that applies only to non-streaming calls.
//
// Requests carrying tools may trigger side effects and pass straight through;
// a hit returns a deep copy so that caller mutations of the response cannot
// pollute the cache.
// ttl: how long an entry lives, counted from when the response completes
// maxEntries: the maximum number of entries; exceeding it clears everything,
// to avoid unbounded growth
// returns: the middleware
func Cache(ttl time.Duration, maxEntries int) ChatMiddleware {
	var mu sync.Mutex
	entries := make(map[string]cacheEntry)
	return func(next ChatHandler) ChatHandler {
		return func(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
			if len(req.Tools) > 0 {
				return next(ctx, req)
			}
			key := cacheKey(req)

			mu.Lock()
			if entry, ok := entries[key]; ok && entry.expiresAt.After(time.Now()) {
				resp := cloneResponse(entry.resp)
				mu.Unlock()
				return resp, nil
			}
			mu.Unlock()

			resp, err := next(ctx, req)
			if err != nil {
				return nil, err
			}
			mu.Lock()
			if len(entries) >= maxEntries {
				entries = make(map[string]cacheEntry)
			}
			// Both storing and hitting go through copies: the return value of
			// the first call and the cache entry must not interfere with each
			// other — whatever the caller receives and however it mutates it,
			// the cache is unaffected.
			// TTL is counted from the moment of completion: a slow response
			// should not expire early.
			entries[key] = cacheEntry{resp: cloneResponse(resp), expiresAt: time.Now().Add(ttl)}
			mu.Unlock()
			return resp, nil
		}
	}
}

// cloneResponse deep-copies the response; callers hitting the cache must get
// a private copy.
// returns: the copy
func cloneResponse(resp *ChatResponse) *ChatResponse {
	if resp == nil {
		return nil
	}
	out := *resp
	if resp.ToolCalls != nil {
		out.ToolCalls = make([]ToolCall, len(resp.ToolCalls))
		copy(out.ToolCalls, resp.ToolCalls)
	}
	return &out
}

// cacheKey computes the request fingerprint.
//
// It covers every semantic field of the request: messages including tool
// calls and multimodal parts, and sampling parameters including Thinking and
// ResponseFormat — omitting any one field means a false hit. Message's
// Reasoning/FinishReason do not participate: across retries of the same
// conversation only these two fields can differ, and semantically it is the
// same request.
// returns: a hex hash; on serialization failure it degrades to a hash of the
// fully expanded value — an empty shared key is never used
func cacheKey(req ChatRequest) string {
	type callKey struct {
		ID        string
		Name      string
		Arguments string
	}
	type partKey struct {
		Type     string
		Text     string
		ImageURL string
	}
	type msgKey struct {
		Role         string
		Content      string
		ToolCallID   string
		ToolCalls    []callKey
		ContentParts []partKey
	}
	type formatKey struct {
		Name   string
		Schema string
	}
	type probeKey struct {
		Model          string
		Messages       []msgKey
		Temperature    *float64
		MaxTokens      int64
		Thinking       *ThinkingConfig
		ResponseFormat *formatKey
		Extra          map[string]any
	}
	probe := probeKey{
		Model: req.Model, Temperature: req.Temperature, MaxTokens: req.MaxTokens,
		Extra: req.Extra,
	}
	for _, m := range req.Messages {
		mk := msgKey{
			Role: string(m.Role), Content: m.Content, ToolCallID: m.ToolCallID,
		}
		for _, tc := range m.ToolCalls {
			mk.ToolCalls = append(mk.ToolCalls, callKey{tc.ID, tc.Name, tc.Arguments})
		}
		for _, p := range m.ContentParts {
			mk.ContentParts = append(mk.ContentParts, partKey{p.Type, p.Text, p.ImageURL})
		}
		probe.Messages = append(probe.Messages, mk)
	}
	if req.ResponseFormat != nil {
		probe.ResponseFormat = &formatKey{Name: req.ResponseFormat.Name, Schema: string(req.ResponseFormat.Schema)}
	}
	raw, err := json.Marshal(probe)
	if err != nil {
		// Serialization failure (e.g. Extra containing unencodable values)
		// must not fall back to a shared empty key; degrade to the fully
		// expanded value so different requests still get different keys.
		raw = []byte(fmt.Sprintf("%#v", probe))
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
