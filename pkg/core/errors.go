package core

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrorKind 错误类别，决定重试策略
type ErrorKind int

const (
	// ErrInvalidRequest 请求参数错误，重试无意义
	ErrInvalidRequest ErrorKind = iota

	// ErrAuth 鉴权失败，需换 key 而非重试
	ErrAuth

	// ErrPermission 配额或权限不足
	ErrPermission

	// ErrRateLimited 限流，可按 RetryAfter 延迟后重试
	ErrRateLimited

	// ErrProviderInternal 提供商服务端错误，可重试
	ErrProviderInternal

	// ErrNetwork 网络层错误，可重试
	ErrNetwork

	// ErrCanceled 调用方主动取消
	ErrCanceled

	// ErrUnsupported 能力不支持，属配置错误
	ErrUnsupported

	// ErrExhausted 重试次数耗尽
	ErrExhausted
)

// Retryable 判断错误类别是否可重试
// returns: true 表示可安全重试同一请求
func (k ErrorKind) Retryable() bool {
	switch k {
	case ErrRateLimited, ErrProviderInternal, ErrNetwork:
		return true
	default:
		return false
	}
}

// Error 统一错误类型，携带重试决策所需全部信息
type Error struct {
	Kind       ErrorKind
	ProviderID string
	StatusCode int
	// RetryAfter 提供商建议的等待时长，nil 表示未知
	RetryAfter *time.Duration
	Err        error
}

// Error 实现 error 接口
func (e *Error) Error() string {
	if e.ProviderID != "" {
		return fmt.Sprintf("%s: %v", e.ProviderID, e.Err)
	}
	return e.Err.Error()
}

// Unwrap 支持 errors.Is / errors.As 透传底层错误
func (e *Error) Unwrap() error {
	return e.Err
}

// NewError 构造统一错误
// providerID: 提供商标识，用于日志与错误信息定位
// returns: 包装后的 *Error
func NewError(kind ErrorKind, providerID string, err error) *Error {
	return &Error{Kind: kind, ProviderID: providerID, Err: err}
}

// ErrorKindOf 提取错误的类别，非 *Error 一律按网络错误处理
// returns: 错误类别
func ErrorKindOf(err error) ErrorKind {
	if err == nil {
		return ErrInvalidRequest
	}
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Kind
	}
	if errors.Is(err, context.Canceled) {
		return ErrCanceled
	}
	// 超时归为取消而非网络错误：预算已耗尽，重试只会再超一次
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrCanceled
	}
	// 未分类错误多半来自 net/http 底层，按可重试处理避免误杀
	return ErrNetwork
}

// Retryable 判断错误是否可重试
// returns: true 表示可安全重试同一请求
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Kind.Retryable()
	}
	return ErrorKindOf(err).Retryable()
}
