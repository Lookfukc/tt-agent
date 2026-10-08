package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// streamChunk is the JSON increment corresponding to a single SSE data line.
type streamChunk struct {
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Content   string            `json:"content"`
			Reasoning string            `json:"reasoning_content"`
			ToolCalls []openAIToolDelta `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *openAIUsage `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// openAIToolDelta is a tool call fragment.
type openAIToolDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// ChatStream sends a streaming chat request.
//
// Errors before the first event (network/auth/4xx) are returned synchronously;
// all content and errors after the connection is established flow through the
// event channel, with semantics defined by core.StreamEvent.
func (p *OpenAIProtocol) ChatStream(ctx context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
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
		// The decoder holds cross-chunk state and must not be reused.
		d := newStreamDecoder(resp)
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
		finish := core.FinishStop
		for {
			chunk, done, err := d.next(ctx)
			if err != nil {
				emit(core.StreamEvent{Type: core.StreamError, Err: err})
				return
			}
			if done {
				// The terminal event is emitted only here: the finish_reason chunk
				// only records the reason without emitting, and usage arrives after
				// it — a consumer that stops at the first Done must not lose it.
				emit(core.StreamEvent{Type: core.StreamDone, FinishReason: finish})
				return
			}
			if !p.emitChunk(chunk, emit) {
				return
			}
			if len(chunk.Choices) > 0 && chunk.Choices[0].FinishReason != nil {
				finish = normalizeFinish(*chunk.Choices[0].FinishReason)
			}
		}
	}()
	return events, nil
}

// emitChunk decodes a single increment into zero or more events; returning false means the consumer has cancelled.
func (p *OpenAIProtocol) emitChunk(chunk *streamChunk, emit func(core.StreamEvent) bool) bool {
	if chunk.Error != nil {
		return emit(core.StreamEvent{
			Type: core.StreamError,
			Err: core.NewError(core.ErrProviderInternal, p.providerID,
				fmt.Errorf("stream error: %s", chunk.Error.Message)),
		})
	}
	if chunk.Usage != nil {
		if !emit(core.StreamEvent{Type: core.StreamUsage, Usage: chunk.Usage.toCore()}) {
			return false
		}
	}
	for _, choice := range chunk.Choices {
		if choice.Delta.Reasoning != "" {
			if !emit(core.StreamEvent{Type: core.StreamDeltaReasoning, Reasoning: choice.Delta.Reasoning}) {
				return false
			}
		}
		if choice.Delta.Content != "" {
			if !emit(core.StreamEvent{Type: core.StreamDeltaText, Text: choice.Delta.Content}) {
				return false
			}
		}
		for _, tc := range choice.Delta.ToolCalls {
			if !emit(core.StreamEvent{Type: core.StreamDeltaToolCall, ToolCallDelta: core.ToolCallDelta{
				Index: tc.Index, ID: tc.ID, Name: tc.Function.Name, ArgsPart: tc.Function.Arguments,
			}}) {
				return false
			}
		}
	}
	return true
}

// streamDecoder is a stateful SSE decoder.
//
// SSE events may arrive across multiple TCP reads, and tool_calls fragments
// must be accumulated by index, so decoding has to hold per-request state.
type streamDecoder struct {
	reader *sseReader
}

// newStreamDecoder constructs the decoder and enables long-line buffering.
func newStreamDecoder(resp *http.Response) *streamDecoder {
	return &streamDecoder{reader: newSSEReader(resp, false)}
}

// next reads the next increment.
// ctx: used to interrupt a blocking read on cancellation.
// returns: the decoded increment; done is true when the stream ended normally; a non-nil err means the stream is broken.
func (d *streamDecoder) next(ctx context.Context) (*streamChunk, bool, error) {
	for {
		data, done, err := d.reader.next(ctx)
		if err != nil || done {
			return nil, done, err
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return nil, false, core.NewError(core.ErrProviderInternal, "",
				fmt.Errorf("decode stream chunk: %w", err))
		}
		return &chunk, false, nil
	}
}
