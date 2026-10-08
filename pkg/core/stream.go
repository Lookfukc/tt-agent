package core

import (
	"strings"
	"sync"
)

// StreamAccumulator aggregates stream events into a complete message.
//
// Tool-call fragments accumulate arguments by Index, so consumers do not
// need to understand the fragment protocol.
type StreamAccumulator struct {
	mu        sync.Mutex
	text      strings.Builder
	reasoning strings.Builder
	toolOrder []int
	toolCalls map[int]*ToolCall
	usage     Usage
	finish    FinishReason
}

// NewStreamAccumulator constructs an accumulator.
// returns: a ready-to-use accumulator instance
func NewStreamAccumulator() *StreamAccumulator {
	return &StreamAccumulator{toolCalls: make(map[int]*ToolCall)}
}

// Feed ingests one event.
func (a *StreamAccumulator) Feed(e StreamEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch e.Type {
	case StreamDeltaText:
		a.text.WriteString(e.Text)
	case StreamDeltaReasoning:
		a.reasoning.WriteString(e.Reasoning)
	case StreamDeltaToolCall:
		tc, ok := a.toolCalls[e.ToolCallDelta.Index]
		if !ok {
			tc = &ToolCall{}
			a.toolCalls[e.ToolCallDelta.Index] = tc
			a.toolOrder = append(a.toolOrder, e.ToolCallDelta.Index)
		}
		if e.ToolCallDelta.ID != "" {
			tc.ID = e.ToolCallDelta.ID
		}
		if e.ToolCallDelta.Name != "" {
			tc.Name = e.ToolCallDelta.Name
		}
		tc.Arguments += e.ToolCallDelta.ArgsPart
	case StreamUsage:
		// Field-level merge rather than overwrite: Anthropic splits usage
		// across message_start/message_delta, each carrying half; fields
		// missing from the latter keep the earlier values.
		if e.Usage.InputTokens > a.usage.InputTokens {
			a.usage.InputTokens = e.Usage.InputTokens
		}
		if e.Usage.OutputTokens > a.usage.OutputTokens {
			a.usage.OutputTokens = e.Usage.OutputTokens
		}
		if e.Usage.ReasoningTokens > a.usage.ReasoningTokens {
			a.usage.ReasoningTokens = e.Usage.ReasoningTokens
		}
	case StreamDone:
		if e.FinishReason != "" {
			a.finish = e.FinishReason
		}
	}
}

// Message produces the aggregated assistant message.
// returns: the message with Content/Reasoning/ToolCalls filled in
func (a *StreamAccumulator) Message() Message {
	a.mu.Lock()
	defer a.mu.Unlock()
	msg := Message{
		Role:         RoleAssistant,
		Content:      a.text.String(),
		Reasoning:    a.reasoning.String(),
		FinishReason: a.finish,
	}
	for _, idx := range a.toolOrder {
		if tc := a.toolCalls[idx]; tc != nil {
			msg.ToolCalls = append(msg.ToolCalls, *tc)
		}
	}
	return msg
}

// Usage produces the accumulated usage.
// returns: the usage from the last StreamUsage event
func (a *StreamAccumulator) Usage() Usage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.usage
}

// CollectStream is a convenience consumer: aggregate and return the terminal error.
// events: the event stream; the implementation is responsible for closing it
// returns: the aggregated message, the accumulated usage, and the terminal
// error (the Err of the StreamError event)
func CollectStream(events <-chan StreamEvent) (Message, Usage, error) {
	acc := NewStreamAccumulator()
	var lastErr error
	for e := range events {
		if e.Type == StreamError {
			lastErr = e.Err
			continue
		}
		acc.Feed(e)
	}
	return acc.Message(), acc.Usage(), lastErr
}
