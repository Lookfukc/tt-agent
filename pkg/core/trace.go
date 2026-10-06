package core

import "context"

// Span 单次操作的追踪单元
type Span interface {
	// SetAttr 附加属性，End 前有效
	SetAttr(key string, value any)
	// RecordError 记录终止性错误
	RecordError(err error)
	// End 结束 span，之后方法调用为空操作
	End()
}

// Tracer 追踪器抽象，实现方负责导出到观测后端
type Tracer interface {
	// StartSpan 开启子 span 并注入 context
	StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, Span)
}

// spanCtxKey context 注入键
type spanCtxKey struct{}

// SpanFromContext 取当前 span，无则返回 noop
// returns: 当前生效的 span
func SpanFromContext(ctx context.Context) Span {
	if s, ok := ctx.Value(spanCtxKey{}).(Span); ok {
		return s
	}
	return noopSpan{}
}

// noopSpan 空实现
type noopSpan struct{}

// SetAttr 空操作
func (noopSpan) SetAttr(string, any) {}

// RecordError 空操作
func (noopSpan) RecordError(error) {}

// End 空操作
func (noopSpan) End() {}

// noopTracer 空实现
type noopTracer struct{}

// StartSpan 原样返回 context
func (noopTracer) StartSpan(ctx context.Context, _ string, _ ...any) (context.Context, Span) {
	return ctx, noopSpan{}
}

// NoopTracer 默认空追踪器
// returns: 全局可复用的空实现
func NoopTracer() Tracer { return noopTracer{} }

// WithSpan 注入 span 到 context
// returns: 携带 span 的 context
func WithSpan(ctx context.Context, s Span) context.Context {
	return context.WithValue(ctx, spanCtxKey{}, s)
}
