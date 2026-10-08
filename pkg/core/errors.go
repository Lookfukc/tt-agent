package core

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrorKind is the error category, which determines the retry policy.
type ErrorKind int

const (
	// ErrInvalidRequest indicates invalid request parameters; retrying is pointless.
	ErrInvalidRequest ErrorKind = iota

	// ErrAuth indicates an authentication failure; the key must be replaced rather than retried.
	ErrAuth

	// ErrPermission indicates insufficient quota or permissions.
	ErrPermission

	// ErrRateLimited indicates rate limiting; retry after the RetryAfter delay.
	ErrRateLimited

	// ErrProviderInternal indicates a provider-side server error; retryable.
	ErrProviderInternal

	// ErrNetwork indicates a network-layer error; retryable.
	ErrNetwork

	// ErrCanceled indicates the caller canceled the request.
	ErrCanceled

	// ErrUnsupported indicates an unsupported capability; a configuration error.
	ErrUnsupported

	// ErrExhausted indicates the retry budget has been exhausted.
	ErrExhausted
)

// Retryable reports whether the error category is retryable.
// returns: true if the same request can be safely retried
func (k ErrorKind) Retryable() bool {
	switch k {
	case ErrRateLimited, ErrProviderInternal, ErrNetwork:
		return true
	default:
		return false
	}
}

// Error is the unified error type carrying all information needed for retry decisions.
type Error struct {
	Kind       ErrorKind
	ProviderID string
	StatusCode int
	// RetryAfter is the provider-suggested wait duration; nil means unknown.
	RetryAfter *time.Duration
	Err        error
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.ProviderID != "" {
		return fmt.Sprintf("%s: %v", e.ProviderID, e.Err)
	}
	return e.Err.Error()
}

// Unwrap supports errors.Is / errors.As propagation of the underlying error.
func (e *Error) Unwrap() error {
	return e.Err
}

// NewError constructs a unified error.
// providerID: provider identifier, used to locate it in logs and error messages
// returns: the wrapped *Error
func NewError(kind ErrorKind, providerID string, err error) *Error {
	return &Error{Kind: kind, ProviderID: providerID, Err: err}
}

// ErrorKindOf extracts the error category; anything that is not a *Error is
// treated as a network error.
// returns: the error category
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
	// Timeouts are classified as cancellation rather than network errors:
	// the budget is already spent, so a retry would just time out again.
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrCanceled
	}
	// Unclassified errors mostly originate from net/http internals; treat
	// them as retryable to avoid rejecting requests prematurely.
	return ErrNetwork
}

// Retryable reports whether the error is retryable.
// returns: true if the same request can be safely retried
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
