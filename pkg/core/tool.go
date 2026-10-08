package core

import (
	"context"
	"encoding/json"
)

// ToolResult is the result of a tool execution, supporting multiple shapes to
// accommodate vision and structured-output scenarios.
type ToolResult struct {
	// Text is the text result, sent back directly as the tool message content.
	Text string

	// Data is the structured result; omitted when empty. The adapter is
	// responsible for serializing it.
	Data any

	// Images holds image results (URLs or base64) for vision models to consume.
	// TODO: wire in once Message supports multimodal content parts.
	Images []string
}

// Render renders the result as the string sent back to the model.
// returns: the JSON serialization of Data when present, otherwise Text
func (r ToolResult) Render() string {
	if r.Data != nil {
		if b, err := json.Marshal(r.Data); err == nil {
			return string(b)
		}
	}
	return r.Text
}

// Tool is the tool abstraction; all tools in the framework implement this interface.
type Tool interface {
	// Name is the unique tool identifier.
	Name() string

	// Description is the natural-language description for the model to
	// understand the tool's purpose.
	Description() string

	// Parameters is the JSON Schema of the parameters.
	Parameters() json.RawMessage

	// Execute runs the tool.
	// ctx: carries timeout and cancellation; long tasks must respond to it
	// args: the parameter JSON given by the model; validating its format is
	// the implementation's responsibility
	// returns: the execution result and a non-nil error; the error is sent
	// back to the model as the tool-failure message
	Execute(ctx context.Context, args json.RawMessage) (ToolResult, error)
}

// ToolFunc is a functional tool, making it easy to register plain functions
// as tools.
type ToolFunc func(ctx context.Context, args json.RawMessage) (ToolResult, error)

// Execute implements the Tool interface.
func (f ToolFunc) Execute(ctx context.Context, args json.RawMessage) (ToolResult, error) {
	return f(ctx, args)
}
