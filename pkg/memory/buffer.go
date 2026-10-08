// Package memory 提供会话记忆的进程内实现
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// roughTokens 保守估算系数
//
// 中文约 1.5 字符/token，英文约 4 字符/token，取偏小的除数
// 保证估算偏大，预算截断宁少勿超
const roughTokens = 2

// Buffer 基于滑动窗口的进程内会话记忆
//
// 只适合单实例部署，重启即失；跨实例需换持久化实现，
// 接口不变，调用方无感
type Buffer struct {
	mu       sync.RWMutex
	sessions map[string]*sessionLog
	counter  core.TokenCounter
}

// NewBuffer 构造记忆实例
// counter: token 估算器，nil 时使用内置粗估
// returns: 可用的记忆实例
func NewBuffer(counter core.TokenCounter) *Buffer {
	if counter == nil {
		counter = roughCounter{}
	}
	return &Buffer{sessions: make(map[string]*sessionLog), counter: counter}
}

// Add 追加消息到指定会话
//
// token 估算在此一次算好缓存，Recent 装填不再全量重数
func (b *Buffer) Add(_ context.Context, sessionID string, msgs ...core.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	log := b.logLocked(sessionID)
	log.add(b.counter, time.Now(), msgs...)
	return nil
}

// Recent 取回不超预算的最近消息
//
// 系统消息不占预算且始终保留在前，其余按原子组从新到旧装填
func (b *Buffer) Recent(_ context.Context, sessionID string, budget int64) ([]core.Message, error) {
	kept, _, err := b.Split(context.Background(), sessionID, budget)
	return kept, err
}

// Split 取回预算内外的消息，供 Summary 等装饰层获取被截断内容
func (b *Buffer) Split(_ context.Context, sessionID string, budget int64) ([]core.Message, []core.Message, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	log, ok := b.sessions[sessionID]
	if !ok {
		return nil, nil, nil
	}
	kept, dropped := log.split(budget)
	return kept, dropped, nil
}

// Trim 物理丢弃最旧的 n 条非系统消息，系统消息永不删除
func (b *Buffer) Trim(_ context.Context, sessionID string, n int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if log, ok := b.sessions[sessionID]; ok {
		log.trim(n)
	}
	return nil
}

// Clear 清空会话
func (b *Buffer) Clear(_ context.Context, sessionID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.sessions, sessionID)
	return nil
}

// logLocked 取或建会话日志，调用方必须持有写锁
// returns: 该会话的日志
func (b *Buffer) logLocked(sessionID string) *sessionLog {
	log, ok := b.sessions[sessionID]
	if !ok {
		log = &sessionLog{}
		b.sessions[sessionID] = log
	}
	return log
}

// roughCounter 内置字符级粗估
type roughCounter struct{}

// Count 按字符数粗估 token
// returns: 估算值，恒不小于消息数
func (roughCounter) Count(msgs []core.Message) int64 {
	var chars int64
	for _, m := range msgs {
		chars += int64(len(m.Content) + len(m.Reasoning))
		for _, tc := range m.ToolCalls {
			chars += int64(len(tc.Name) + len(tc.Arguments))
		}
	}
	est := chars / roughTokens
	if est < int64(len(msgs)) {
		est = int64(len(msgs))
	}
	return est
}
