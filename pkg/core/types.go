// Package core provides the unified types and core contracts for a
// multi-platform LLM agent framework.
package core

import (
	"encoding/json"
	"strings"
)

// Role is the message role.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ToolCall is a single tool invocation initiated by the model.
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // raw JSON string, passed through as-is across platforms
}

// UnmarshalArguments parses Arguments into an arbitrary JSON value.
// returns: the parsed value, or nil when Arguments is empty or invalid
func (t ToolCall) UnmarshalArguments() any {
	if t.Arguments == "" {
		return nil
	}
	var v any
	if err := json.Unmarshal([]byte(t.Arguments), &v); err != nil {
		return nil
	}
	return v
}

// ContentPart is a multimodal content part.
type ContentPart struct {
	// Type is the part type: text or image.
	Type string
	// Text is the text content, valid when Type is text.
	Text string
	// ImageURL is the image address: an http(s) URL or a Data URI of the
	// form data:image/png;base64,xxx.
	ImageURL string
}

// ImageData parses an image in Data URI form.
// returns: the MIME type, the base64 data, and whether it is a valid
// base64 Data URI
func (p ContentPart) ImageData() (mimeType, data string, ok bool) {
	const prefix = "data:"
	if !strings.HasPrefix(p.ImageURL, prefix) {
		return "", "", false
	}
	body := p.ImageURL[len(prefix):]
	meta, payload, found := strings.Cut(body, ",")
	if !found {
		return "", "", false
	}
	// Data URIs that are not base64 (e.g. data:text/plain,abc) are not image
	// payloads; falsely reporting ok would stuff plaintext into an image
	// block and get rejected by the API.
	if !strings.HasSuffix(meta, ";base64") {
		return "", "", false
	}
	mimeType = strings.TrimSuffix(meta, ";base64")
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	return mimeType, payload, true
}

// Message is the unified internal message format; all protocol adapters are
// responsible for converting to and from it.
type Message struct {
	Role    Role   `json:"role"`
	Content string `json:"content"`

	// ContentParts holds multimodal parts; when non-empty, adapters use it
	// in place of Content to build the request.
	ContentParts []ContentPart `json:"content_parts,omitempty"`

	// ToolCalls is carried only by assistant messages, denoting the tool
	// calls requested by the model.
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`

	// ToolCallID is carried only by tool-role messages, identifying which
	// call this result responds to.
	ToolCallID string `json:"tool_call_id,omitempty"`

	// Reasoning is chain-of-thought content, used only for display and
	// accounting; it is dropped when sent back to the API.
	Reasoning string `json:"reasoning,omitempty"`

	// FinishReason is carried only by streaming aggregation results; it is
	// ignored when assistant history messages are sent back.
	FinishReason FinishReason `json:"finish_reason,omitempty"`
}

// UserImage conveniently constructs a user message with an image.
// text: the text caption
// imageURL: the image URL or Data URI
// returns: the assembled message
func UserImage(text, imageURL string) Message {
	return Message{
		Role: RoleUser,
		ContentParts: []ContentPart{
			{Type: "text", Text: text},
			{Type: "image", ImageURL: imageURL},
		},
	}
}

// Text conveniently constructs a user message.
func Text(content string) Message {
	return Message{Role: RoleUser, Content: content}
}

// ToolSpec is a tool definition exposed to the model.
type ToolSpec struct {
	Name        string
	Description string
	Parameters  json.RawMessage // JSON Schema
}

// ThinkingConfig is the thinking-mode toggle.
type ThinkingConfig struct {
	Enabled bool
	// BudgetTokens is the token ceiling for thinking; 0 lets the model decide.
	BudgetTokens int64
}

// ResponseFormat is the structured output constraint.
type ResponseFormat struct {
	// Name is the schema name, required by some protocols.
	Name string
	// Schema is the JSON Schema definition.
	Schema json.RawMessage
}

// ChatRequest is the unified representation of a conversation request.
type ChatRequest struct {
	Model       string
	Messages    []Message
	Tools       []ToolSpec
	Temperature *float64
	MaxTokens   int64
	Thinking    *ThinkingConfig

	// ResponseFormat, when non-nil, constrains the model output to JSON
	// conforming to the Schema.
	ResponseFormat *ResponseFormat

	// Extra is the passthrough channel for provider-specific fields, merged
	// into the request body by the protocol adapter.
	Extra map[string]any
}

// FinishReason is the reason generation stopped.
type FinishReason string

const (
	FinishStop          FinishReason = "stop"
	FinishToolCalls     FinishReason = "tool_calls"
	FinishLength        FinishReason = "length"
	FinishContentFilter FinishReason = "content_filter"
)

// Usage is the token usage accounting.
type Usage struct {
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
}

// Total returns the total output tokens, including the thinking portion.
// returns: the sum of OutputTokens and ReasoningTokens
func (u Usage) Total() int64 {
	return u.OutputTokens + u.ReasoningTokens
}

// Add accumulates another usage record into this one.
func (u *Usage) Add(other Usage) {
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.ReasoningTokens += other.ReasoningTokens
}

// ChatResponse is the complete response to a conversation.
type ChatResponse struct {
	ID           string
	Model        string
	Content      string
	Reasoning    string
	ToolCalls    []ToolCall
	FinishReason FinishReason
	Usage        Usage
}

// StreamEventType is the stream event type.
type StreamEventType int

const (
	// StreamStart marks the start of the stream; guaranteed to be the first event.
	StreamStart StreamEventType = iota

	// StreamDeltaText is a text increment.
	StreamDeltaText

	// StreamDeltaReasoning is a chain-of-thought increment.
	StreamDeltaReasoning

	// StreamDeltaToolCall is a tool-call argument increment; fragments of the
	// same call are accumulated by Index.
	StreamDeltaToolCall

	// StreamUsage is usage attached at the end of the stream by some providers.
	StreamUsage

	// StreamDone is the normal termination event; the channel closes after it.
	StreamDone

	// StreamError is the fatal error event; the channel closes after it.
	StreamError
)

// ToolCallDelta is a fragment of a tool-call increment.
type ToolCallDelta struct {
	Index    int
	ID       string // carried only by the first fragment
	Name     string // carried only by the first fragment
	ArgsPart string // appended fragment of Arguments
}

// StreamEvent is a streaming event.
//
// Channel semantics: the producer is responsible for closing; errors are
// delivered only via StreamError events, with no separate error channel; on
// ctx cancellation a StreamError is sent and then the channel closes.
type StreamEvent struct {
	Type          StreamEventType
	Text          string
	Reasoning     string
	ToolCallDelta ToolCallDelta
	Usage         Usage
	FinishReason  FinishReason
	Err           error
}
