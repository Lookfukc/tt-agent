// Package memorytest provides a purely in-process session memory
// implementation, for tests and casual demos only.
//
// This is the migration destination of the former pkg/memory.Buffer:
// its semantics (unbounded residency, lost on restart, nothing on
// disk) are unsuitable for production, so it was removed from the
// public API and consolidated into this test-only subpackage to avoid
// accidental use in production assemblies.
package memorytest

import (
	"context"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/internal/sessionlog"
)

// Buffer is a sliding-window in-process session memory (test-only).
//
// Sessions live in memory without bound and are all lost when the
// process exits. For production use memory.Persistent (on disk) or an
// external storage driver.
type Buffer struct {
	mu       sync.RWMutex
	sessions map[string]*sessionlog.Log
	counter  core.TokenCounter
}

// NewBuffer constructs a test memory instance.
// counter: token estimator; nil uses the built-in rough estimate.
// returns: a usable memory instance.
func NewBuffer(counter core.TokenCounter) *Buffer {
	if counter == nil {
		counter = sessionlog.Rough{}
	}
	return &Buffer{sessions: make(map[string]*sessionlog.Log), counter: counter}
}

// Add appends messages to the given session.
//
// Token estimation is computed once here and cached, so Recent's
// packing no longer recounts everything.
func (b *Buffer) Add(_ context.Context, sessionID string, msgs ...core.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	log := b.logLocked(sessionID)
	log.Add(b.counter, time.Now(), msgs...)
	return nil
}

// Recent returns the most recent messages within budget.
//
// System messages cost no budget and are always kept in front; the
// rest are packed from newest to oldest by atomic group.
func (b *Buffer) Recent(_ context.Context, sessionID string, budget int64) ([]core.Message, error) {
	kept, _, err := b.Split(context.Background(), sessionID, budget)
	return kept, err
}

// Split returns messages inside and outside the budget, for
// decoration layers such as Summary to obtain the truncated content.
func (b *Buffer) Split(_ context.Context, sessionID string, budget int64) ([]core.Message, []core.Message, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	log, ok := b.sessions[sessionID]
	if !ok {
		return nil, nil, nil
	}
	kept, dropped := log.Split(budget)
	return kept, dropped, nil
}

// Trim physically discards the oldest n non-system messages; system
// messages are never deleted.
func (b *Buffer) Trim(_ context.Context, sessionID string, n int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if log, ok := b.sessions[sessionID]; ok {
		log.Trim(n)
	}
	return nil
}

// Clear empties the session.
func (b *Buffer) Clear(_ context.Context, sessionID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.sessions, sessionID)
	return nil
}

// logLocked returns or creates the session log; the caller must hold
// the write lock.
// returns: the session's log.
func (b *Buffer) logLocked(sessionID string) *sessionlog.Log {
	log, ok := b.sessions[sessionID]
	if !ok {
		log = &sessionlog.Log{}
		b.sessions[sessionID] = log
	}
	return log
}
