package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// anthropicStreamEvent is the stream event payload.
//
// The delta field has different structures under different event types
// (increment vs finish reason), so they are merged into one loose struct
// and read as needed.
type anthropicStreamEvent struct {
	Type string `json:"type"`

	// Message holds the initial usage from message_start.
	Message struct {
		Usage anthropicUsage `json:"usage"`
	} `json:"message"`

	// Index and ContentBlock carry block metadata from content_block_start.
	Index        int            `json:"index"`
	ContentBlock anthropicBlock `json:"content_block"`

	// Delta is the delta payload shared by content_block_delta and message_delta.
	Delta anthropicDelta `json:"delta"`

	// Usage is the cumulative output usage from message_delta.
	Usage anthropicUsage `json:"usage"`

	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// anthropicDelta is the union struct of delta payloads.
type anthropicDelta struct {
	Type string `json:"type"`
	Text string `json:"text"`
	// Thinking is the thinking_delta content.
	Thinking string `json:"thinking"`
	// PartialJSON is the argument fragment from input_json_delta.
	PartialJSON string `json:"partial_json"`
	// StopReason is the finish reason from message_delta.
	StopReason string `json:"stop_reason"`
}

// ChatStream sends a streaming chat request.
//
// Errors before the first event are returned synchronously; after the
// connection is established, errors surface as StreamError events.
func (p *AnthropicProtocol) ChatStream(ctx context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	body, err := p.buildBody(req, true)
	if err != nil {
		return nil, err
	}
	resp, err := p.post(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return nil, p.httpError(resp.StatusCode, resp.Header.Get("Retry-After"), raw)
	}

	events := make(chan core.StreamEvent, 16)
	go func() {
		defer close(events)
		// Leak guard: the response body must be closed once the stream is drained,
		// otherwise every call leaks one connection.
		defer resp.Body.Close()
		d := &anthropicStreamDecoder{reader: newSSEReader(resp, false)}
		emit := func(e core.StreamEvent) bool {
			select {
			case events <- e:
				return true
			case <-ctx.Done():
				// Contract: cancellation must also emit a terminal error event;
				// closing silently would let truncated content be returned as a
				// complete answer.
				select {
				case events <- core.StreamEvent{
					Type: core.StreamError,
					Err:  core.NewError(core.ErrCanceled, p.providerID, ctx.Err()),
				}:
				default:
				}
				return false
			}
		}
		if !emit(core.StreamEvent{Type: core.StreamStart}) {
			return
		}
		for {
			ev, done, err := d.next(ctx)
			if err != nil {
				emit(core.StreamEvent{Type: core.StreamError, Err: err})
				return
			}
			if done {
				emit(core.StreamEvent{Type: core.StreamDone, FinishReason: d.finish})
				return
			}
			if !p.emitEvent(ev, emit) {
				return
			}
		}
	}()
	return events, nil
}

// emitEvent converts a single stream event; returning false means the consumer has cancelled.
func (p *AnthropicProtocol) emitEvent(ev *anthropicStreamEvent, emit func(core.StreamEvent) bool) bool {
	switch ev.Type {
	case "error":
		return emit(core.StreamEvent{
			Type: core.StreamError,
			Err:  core.NewError(core.ErrProviderInternal, p.providerID, fmt.Errorf("stream error: %s", ev.Error.Message)),
		})
	case "message_start":
		return emit(core.StreamEvent{
			Type:  core.StreamUsage,
			Usage: core.Usage{InputTokens: ev.Message.Usage.InputTokens},
		})
	case "message_delta":
		// The finish reason was already recorded by the decoder; only pass through the cumulative output usage here.
		return emit(core.StreamEvent{
			Type:  core.StreamUsage,
			Usage: core.Usage{OutputTokens: ev.Usage.OutputTokens},
		})
	case "content_block_start":
		if ev.ContentBlock.Type == "tool_use" {
			// The block start carries the full id/name; arguments arrive afterwards via input_json_delta.
			return emit(core.StreamEvent{
				Type: core.StreamDeltaToolCall,
				ToolCallDelta: core.ToolCallDelta{
					Index: ev.Index, ID: ev.ContentBlock.ID, Name: ev.ContentBlock.Name,
				},
			})
		}
		return true
	case "content_block_delta":
		switch ev.Delta.Type {
		case "text_delta":
			return emit(core.StreamEvent{Type: core.StreamDeltaText, Text: ev.Delta.Text})
		case "thinking_delta":
			return emit(core.StreamEvent{Type: core.StreamDeltaReasoning, Reasoning: ev.Delta.Thinking})
		case "input_json_delta":
			return emit(core.StreamEvent{
				Type:          core.StreamDeltaToolCall,
				ToolCallDelta: core.ToolCallDelta{Index: ev.Index, ArgsPart: ev.Delta.PartialJSON},
			})
		}
		return true
	}
	return true
}

// anthropicStreamDecoder is the Anthropic stream decoder.
type anthropicStreamDecoder struct {
	reader *sseReader
	// finish is the finish reason recorded from message_delta, emitted with the Done event.
	finish core.FinishReason
}

// next reads the next event, recording the finish reason from message_delta along the way.
// returns: the decoded event; done is true for message_stop.
func (d *anthropicStreamDecoder) next(ctx context.Context) (*anthropicStreamEvent, bool, error) {
	for {
		data, _, err := d.reader.next(ctx)
		if err != nil {
			return nil, false, err
		}
		var ev anthropicStreamEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return nil, false, core.NewError(core.ErrProviderInternal, "",
				fmt.Errorf("decode stream event: %w", err))
		}
		switch ev.Type {
		case "message_stop":
			return nil, true, nil
		case "message_delta":
			if ev.Delta.StopReason != "" {
				d.finish = anthropicFinish(ev.Delta.StopReason)
			}
		}
		return &ev, false, nil
	}
}
