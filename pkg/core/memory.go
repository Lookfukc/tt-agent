package core

import "context"

// Memory 会话记忆抽象
//
// 按 sessionID 隔离而非挂在 Agent 实例上，Agent 保持无状态，
// 同一 Agent 可并发服务多个会话
type Memory interface {
	// Add 追加消息
	// sessionID: 会话标识，不同会话互不可见
	Add(ctx context.Context, sessionID string, msgs ...Message) error

	// Recent 取回不超预算的最近消息，用于组装 ChatRequest
	// budget: token 预算，实现方负责从新到旧截断并保证系统消息保留
	// returns: 截断后的消息切片，保持原顺序
	Recent(ctx context.Context, sessionID string, budget int64) ([]Message, error)

	// Clear 清空会话
	Clear(ctx context.Context, sessionID string) error
}

// TokenCounter 估算消息的 token 数
//
// 精确计数依赖提供商分词器，框架内只做保守估算，
// 预算检查宁少勿超，避免请求被 400 拒绝
type TokenCounter interface {
	// Count 估算一组消息的 token 总量
	// returns: 估算值
	Count(msgs []Message) int64
}
