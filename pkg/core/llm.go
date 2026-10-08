package core

import "context"

// LLM is the conversation capability abstraction; protocol adapters implement this interface.
//
// Capability differences (vision, structured output, etc.) are Model-level
// information described by the provider registry's ModelConfig, not exposed
// on the interface.
type LLM interface {
	// Chat sends a single conversation request and waits for the complete response.
	Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error)

	// ChatStream sends a streaming conversation request.
	//
	// Channel semantics: the returned channel is closed by the implementation;
	// errors are delivered only via StreamError events; on ctx cancellation
	// the implementation sends StreamError and then closes. Errors occurring
	// before the first event are reported through the error return value, in
	// which case no channel has been created.
	ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error)
}
