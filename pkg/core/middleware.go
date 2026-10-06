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

// LLMMiddleware LLM 级中间件
//
// ChatMiddleware 只能覆盖非流式路径，服务端主路径（Agent 循环）
// 恒为流式；要作用于两条路径的横切逻辑必须挂在这一层
type LLMMiddleware func(LLM) LLM

// LoggingLLM 双路径日志中间件
// logger: 日志器，nil 用默认
// returns: LLM 中间件
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

// Chat 记录非流式调用
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

// ChatStream 记录流式调用，事件计数与终止状态在流关闭后输出
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
				// 消费者已放弃：同步排空源流，让上游生产者能收尾退出
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

// tokenBucket 无后台 goroutine 的令牌桶
//
// 时间驱动按需补充，避免 Ticker 泄漏
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	max      float64
	refillPS float64 // 每秒补充速率
	last     time.Time
}

// newTokenBucket 构造满桶
// n: 窗口容量
// window: 补满耗时
// returns: 令牌桶
func newTokenBucket(n float64, window time.Duration) *tokenBucket {
	return &tokenBucket{
		tokens:   n,
		max:      n,
		refillPS: n / window.Seconds(),
		last:     time.Now(),
	}
}

// acquire 取一个令牌，无令牌时等待或随 ctx 取消
// returns: false 表示 ctx 取消
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

// RateLimit 固定速率限流中间件（非流式路径）
//
// n<=0 或 window<=0 视为不限流，直接透传
// n: 时间窗内允许的调用数
// window: 时间窗长度
// returns: 中间件
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

// RateLimitLLM 双路径限流中间件
// n: 时间窗内允许的调用数；n<=0 不限流
// window: 时间窗长度
// returns: LLM 中间件
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

// Chat 限流后透传
func (r *rateLimitedLLM) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	if !r.bucket.acquire(ctx) {
		return nil, NewError(ErrCanceled, "", ctx.Err())
	}
	return r.next.Chat(ctx, req)
}

// ChatStream 限流后透传
func (r *rateLimitedLLM) ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error) {
	if !r.bucket.acquire(ctx) {
		return nil, NewError(ErrCanceled, "", ctx.Err())
	}
	return r.next.ChatStream(ctx, req)
}

// FallbackLLM 主备切换
//
// 非流式：主 LLM 失败即换备；流式：仅首个内容事件前失败才切换，
// 已产出内容后切换会导致输出重复
// primary: 主 LLM
// alternate: 备 LLM
// returns: 包装后的 LLM
func FallbackLLM(primary, alternate LLM) LLM {
	return &fallbackLLM{primary: primary, alternate: alternate}
}

type fallbackLLM struct {
	primary   LLM
	alternate LLM
}

// Chat 主 LLM 失败时用备 LLM 重试
func (f *fallbackLLM) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	resp, err := f.primary.Chat(ctx, req)
	if err == nil || !Retryable(err) {
		return resp, err
	}
	return f.alternate.Chat(ctx, req)
}

// ChatStream 首个内容事件前主 LLM 失败才切备
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
				// 致命错误透传：先重放缓冲事件再补错误事件，usage 不丢
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
		// 干净收尾的空流（如 OpenAI 空补全：Start+Done 无内容）是
		// 成功，重放主 LLM 的缓冲事件即可，不得误判失败切备
		return relay(ctx, pending, events), nil
	}
	// 主 LLM 未产出内容即失败，切备重试；备用建连失败发生在
	// 首事件前，按契约走 error 返回值而非流内事件
	altEvents, err := f.alternate.ChatStream(ctx, req)
	if err != nil {
		return nil, err
	}
	return altEvents, nil
}

// relay 把已缓冲事件与剩余事件拼接成新流，消费者放弃时排空退出
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

// cacheEntry 缓存条目
type cacheEntry struct {
	resp      *ChatResponse
	expiresAt time.Time
}

// Cache 响应缓存中间件，仅作用于非流式调用
//
// 带工具的请求可能触发副作用，直接穿透；命中返回深拷贝，
// 调用方修改响应不得污染缓存
// ttl: 缓存存活时长，从响应完成时起算
// maxEntries: 最大条目数，超出后整体清空，避免无界增长
// returns: 中间件
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
			// 存入与命中都走拷贝：首次调用的返回值与缓存条目
			// 必须互不干扰，调用方拿到什么改什么都不影响缓存
			// TTL 从完成时刻起算：慢响应不应提前过期
			entries[key] = cacheEntry{resp: cloneResponse(resp), expiresAt: time.Now().Add(ttl)}
			mu.Unlock()
			return resp, nil
		}
	}
}

// cloneResponse 深拷贝响应，命中缓存的调用方拿到的必须是私有副本
// returns: 拷贝
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

// cacheKey 计算请求指纹
//
// 覆盖请求的全部语义字段：消息含工具调用与多模态分片，
// 采样参数含 Thinking 与 ResponseFormat——漏掉任一字段即错误命中
// Message 的 Reasoning/FinishReason 不参与：同一段对话的多次重试
// 只有这两个字段可能不同，语义上是同一请求
// returns: 十六进制哈希；序列化失败时退化为全量展开的哈希，绝不共享空键
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
		// 序列化失败（Extra 含不可编码值等）不能共享空键，
		// 退化为全量展开，保证不同请求不同键
		raw = []byte(fmt.Sprintf("%#v", probe))
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
