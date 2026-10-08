package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// ChatStream sends a streaming chat request.
//
// Gemini's SSE delivers each line as a complete response fragment rather
// than an increment; a functionCall arrives in full within a single fragment.
func (p *GeminiProtocol) ChatStream(ctx context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	body, err := p.buildBody(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.post(ctx, body, ":streamGenerateContent?alt=sse")
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
		// Native Gemini streams end with connection close and carry no [DONE] marker.
		reader := newSSEReader(resp, true)
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
		toolIdx := 0
		// nameSeq counts same-name calls: synthesized IDs disambiguate by occurrence index, accumulated across chunks.
		nameSeq := map[string]int{}
		for {
			data, done, err := reader.next(ctx)
			if err != nil {
				emit(core.StreamEvent{Type: core.StreamError, Err: err})
				return
			}
			if done {
				emit(core.StreamEvent{Type: core.StreamDone, FinishReason: finish})
				return
			}
			if !p.emitChunk(data, &toolIdx, nameSeq, emit) {
				return
			}
			if stopReason := chunkFinishReason(data); stopReason != "" {
				finish = geminiFinish(stopReason)
			}
		}
	}()
	return events, nil
}

// emitChunk converts a single stream fragment.
//
// toolIdx is the tool call index carried across chunks: Gemini's functionCall
// has no index, and the aggregator merges fragments by Index — without an
// independent index, parallel calls would overwrite each other.
// returns: false means the consumer has cancelled.
func (p *GeminiProtocol) emitChunk(data string, toolIdx *int, nameSeq map[string]int, emit func(core.StreamEvent) bool) bool {
	var chunk struct {
		Candidates []struct {
			Content struct {
				Parts []geminiPart `json:"parts"`
			} `json:"content"`
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount     int64 `json:"promptTokenCount"`
			CandidatesTokenCount int64 `json:"candidatesTokenCount"`
			ThoughtsTokenCount   int64 `json:"thoughtsTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		emit(core.StreamEvent{
			Type: core.StreamError,
			Err:  core.NewError(core.ErrProviderInternal, p.providerID, fmt.Errorf("decode stream chunk: %w", err)),
		})
		return false
	}
	if chunk.UsageMetadata.PromptTokenCount > 0 || chunk.UsageMetadata.CandidatesTokenCount > 0 ||
		chunk.UsageMetadata.ThoughtsTokenCount > 0 {
		if !emit(core.StreamEvent{Type: core.StreamUsage, Usage: core.Usage{
			InputTokens:     chunk.UsageMetadata.PromptTokenCount,
			OutputTokens:    chunk.UsageMetadata.CandidatesTokenCount,
			ReasoningTokens: chunk.UsageMetadata.ThoughtsTokenCount,
		}}) {
			return false
		}
	}
	if len(chunk.Candidates) == 0 {
		return true
	}
	for _, part := range chunk.Candidates[0].Content.Parts {
		if part.Thought && part.Text != "" {
			if !emit(core.StreamEvent{Type: core.StreamDeltaReasoning, Reasoning: part.Text}) {
				return false
			}
			continue
		}
		if part.Text != "" {
			if !emit(core.StreamEvent{Type: core.StreamDeltaText, Text: part.Text}) {
				return false
			}
		}
		if part.FunctionCall != nil && part.FunctionCall.Name != "" {
			args, _ := json.Marshal(part.FunctionCall.Args)
			if string(args) == "null" {
				args = []byte("{}")
			}
			idx := *toolIdx
			*toolIdx++
			// Parallel calls with the same name are disambiguated by occurrence
			// index (appending :<n> from the second onward); the first keeps the
			// legacy format gemini:<name>, consistent with non-streaming.
			seq := nameSeq[part.FunctionCall.Name]
			nameSeq[part.FunctionCall.Name] = seq + 1
			if !emit(core.StreamEvent{
				Type: core.StreamDeltaToolCall,
				ToolCallDelta: core.ToolCallDelta{
					Index:    idx,
					ID:       geminiCallID(part.FunctionCall.Name, seq),
					Name:     part.FunctionCall.Name,
					ArgsPart: string(args),
				},
			}) {
				return false
			}
		}
	}
	return true
}

// chunkFinishReason extracts the finish reason from a fragment.
// returns: the finish reason string, or the empty string if absent.
func chunkFinishReason(data string) string {
	var probe struct {
		Candidates []struct {
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
	}
	if json.Unmarshal([]byte(data), &probe) != nil || len(probe.Candidates) == 0 {
		return ""
	}
	return probe.Candidates[0].FinishReason
}
