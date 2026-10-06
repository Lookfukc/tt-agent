package core

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"
)

// ChatHandler Chat 调用链处理器
type ChatHandler func(ctx context.Context, req ChatRequest) (*ChatResponse, error)

// ChatMiddleware Chat 调用中间件
type ChatMiddleware func(next ChatHandler) ChatHandler

// Pipeline LLM 调用中间件链，本身实现 LLM 接口
type Pipeline struct {
	chat   ChatHandler
	stream StreamHandler
}

// StreamHandler ChatStream 调用链处理器
//
// 流式调用只能拦截请求发出之前，建立连接后中间件无法插入
type StreamHandler func(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error)

// NewPipeline 构造中间件链
// llm: 被包裹的真实 LLM
// middlewares: 依次包裹，先注册者先执行
// returns: 实现 LLM 接口的链实例
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

// Chat 走全中间件链的非流式调用
func (p *Pipeline) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	return p.chat(ctx, req)
}

// ChatStream 流式调用
//
// 不经过 ChatMiddleware 链（中间件只覆盖 Chat）；流式需要
// Logging/RateLimit 覆盖时使用 LLMMiddleware 形态（LoggingLLM/
// RateLimitLLM）或 StreamRetry 包装
func (p *Pipeline) ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error) {
	return p.stream(ctx, req)
}

// Logging 记录每次调用耗时与结果
// logger: 日志器，nil 用默认
// returns: 中间件
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

// Retry 非流式调用重试
// maxAttempts: 总尝试次数上限，含首次；小于 1 按 1 处理
// returns: 中间件
func Retry(maxAttempts int) ChatMiddleware {
	if maxAttempts < 1 {
		// 0 或负数会让循环体一次都不执行，lastErr 为 nil 直接 panic
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

// providerOf 从底层错误提取提供商标识，耗尽错误不丢上下文
//
// 用 errors.As 而非直接断言：被 fmt.Errorf("%w") 等包装过的
// *Error 同样要能贡献 ProviderID
// returns: 提供商 ID，无则空串
func providerOf(err error) string {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.ProviderID
	}
	return ""
}

// StreamRetry 流式调用的首 token 前重试
//
// 流已吐出内容后无法回滚重放，因此只缓冲到首个有效事件：
// 此前失败可安全重试，此后失败原样透传
// maxAttempts: 总尝试次数上限，含首次
// returns: 包裹 LLM 的新实例
func StreamRetry(llm LLM, maxAttempts int) LLM {
	return streamRetryLLM{llm: llm, maxAttempts: maxAttempts}
}

type streamRetryLLM struct {
	llm         LLM
	maxAttempts int
}

// Chat 透传底层 LLM
func (s streamRetryLLM) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	return s.llm.Chat(ctx, req)
}

// ChatStream 首个内容事件前失败才重试的流式调用
func (s streamRetryLLM) ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error) {
	for attempt := 1; attempt < s.maxAttempts; attempt++ {
		// 每次 attempt 独立派生 ctx，重试放弃旧流时取消以释放生产者
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
				// 致命错误透传：先重放缓冲事件再补错误事件，usage 不丢
				pending = append(pending, e)
				return replay(pending), nil
			}
			pending = append(pending, e)
			if isContentEvent(e) {
				// 首个内容事件后不可再重放，直接接管
				return splice(ctx, pending, events, cancel), nil
			}
		}
		cancel()
		if streamErr == nil {
			// 干净收尾的空流（如 OpenAI 空补全：Start+Done 无内容）是
			// 成功，重放缓冲事件（含 Done 与 usage）即可，不得重试
			return replay(pending), nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// 中途 StreamError 重试：退避要带上原始错误以取 Retry-After
		if !sleep(ctx, backoff(attempt, streamErr)) {
			return nil, ctx.Err()
		}
	}
	return s.llm.ChatStream(ctx, req)
}

// isContentEvent 判定事件是否已产出实质内容
//
// StreamUsage 不算内容：Anthropic/Gemini 在文本前先发用量事件，
// 把它当阈值会让"首 token 前可重试"的语义提前失效
// returns: true 表示已有不可回滚的产出
func isContentEvent(e StreamEvent) bool {
	switch e.Type {
	case StreamDeltaText, StreamDeltaReasoning, StreamDeltaToolCall:
		return true
	default:
		return false
	}
}

// splice 把已缓冲事件与剩余事件拼接成新流，排空后释放派生 ctx
//
// 消费者中途放弃（ctx 取消）时排空 rest 至关闭再退出，
// 生产者（适配器）以同一 ctx 取消退出，不留阻塞的发送方
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

// drain 丢弃式排空，让生产者能正常收尾退出
func drain(rest <-chan StreamEvent) {
	for range rest {
	}
}

// replay 把已完整消费的缓冲事件重放为一条立即完结的新流
//
// 源流已关闭且全部读出，缓冲即全部剩余内容：按 len 预留容量，
// 发送不会阻塞，也无需再起 goroutine
// returns: 重放后的新事件流
func replay(pending []StreamEvent) <-chan StreamEvent {
	out := make(chan StreamEvent, len(pending))
	for _, e := range pending {
		out <- e
	}
	close(out)
	return out
}

// backoff 计算指数退避时长，限流错误优先采用 Retry-After
// attempt: 当前尝试序号，从 1 起
// err: 触发重试的错误
// returns: 等待时长
func backoff(attempt int, err error) time.Duration {
	if ce, ok := err.(*Error); ok && ce.RetryAfter != nil {
		return *ce.RetryAfter
	}
	// 抖动避免并发请求同步重试形成尖峰
	d := time.Duration(1<<uint(min(attempt, 5))) * 200 * time.Millisecond
	return d + time.Duration(rand.Int63n(int64(d/2)))
}

// sleep 可中断的等待
// returns: false 表示 ctx 已取消
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
