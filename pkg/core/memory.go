package core

import "context"

// Memory is the session memory abstraction.
//
// State is isolated by sessionID rather than attached to the Agent instance:
// the Agent stays stateless and a single Agent can serve multiple sessions
// concurrently.
type Memory interface {
	// Add appends messages.
	// sessionID: session identifier; sessions are isolated from each other
	Add(ctx context.Context, sessionID string, msgs ...Message) error

	// Recent retrieves the most recent messages that fit within the budget,
	// for assembling a ChatRequest.
	// budget: token budget; implementations are responsible for truncating
	// from newest to oldest while keeping the system message
	// returns: the truncated message slice, in original order
	Recent(ctx context.Context, sessionID string, budget int64) ([]Message, error)

	// Clear clears the session.
	Clear(ctx context.Context, sessionID string) error
}

// TokenCounter estimates the token count of messages.
//
// Exact counting depends on provider tokenizers; the framework only does a
// conservative estimate — better to underuse the budget than exceed it, to
// avoid the request being rejected with a 400.
type TokenCounter interface {
	// Count estimates the total number of tokens in a set of messages.
	// returns: the estimated value
	Count(msgs []Message) int64
}
