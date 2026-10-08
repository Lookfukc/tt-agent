package core

import "context"

// Span is the tracing unit for a single operation.
type Span interface {
	// SetAttr attaches an attribute; valid until End.
	SetAttr(key string, value any)
	// RecordError records a fatal error.
	RecordError(err error)
	// End ends the span; method calls afterwards are no-ops.
	End()
}

// Tracer is the tracer abstraction; implementations are responsible for
// exporting to an observability backend.
type Tracer interface {
	// StartSpan opens a child span and injects it into the context.
	StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, Span)
}

// spanCtxKey is the context injection key.
type spanCtxKey struct{}

// SpanFromContext returns the current span, or a noop span if there is none.
// returns: the currently active span
func SpanFromContext(ctx context.Context) Span {
	if s, ok := ctx.Value(spanCtxKey{}).(Span); ok {
		return s
	}
	return noopSpan{}
}

// noopSpan is the no-op implementation.
type noopSpan struct{}

// SetAttr is a no-op.
func (noopSpan) SetAttr(string, any) {}

// RecordError is a no-op.
func (noopSpan) RecordError(error) {}

// End is a no-op.
func (noopSpan) End() {}

// noopTracer is the no-op implementation.
type noopTracer struct{}

// StartSpan returns the context unchanged.
func (noopTracer) StartSpan(ctx context.Context, _ string, _ ...any) (context.Context, Span) {
	return ctx, noopSpan{}
}

// NoopTracer is the default no-op tracer.
// returns: a globally reusable no-op implementation
func NoopTracer() Tracer { return noopTracer{} }

// WithSpan injects a span into the context.
// returns: the context carrying the span
func WithSpan(ctx context.Context, s Span) context.Context {
	return context.WithValue(ctx, spanCtxKey{}, s)
}
