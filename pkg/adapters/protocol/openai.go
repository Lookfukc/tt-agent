// Package protocol implements request building, response parsing, and stream
// decoding for OpenAI-compatible protocols.
package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// PatchFunc is a request body patch function.
// body: the request body to be sent, modified in place.
// req: the original unified request, carrying information not reflected in body such as Thinking/Extra.
type PatchFunc func(body map[string]any, req core.ChatRequest)

// Quirks collects the correction points for provider deviations under the same OpenAI protocol.
//
// Vendor deviations concentrate in two places: request bodies needing extra or
// removed fields, and proprietary extension fields in responses. They are
// injected as hooks rather than subclasses, so new vendors integrate with zero code.
type Quirks struct {
	// PatchRequest patches the request body before serialization; body is the top-level map whose fields can be added or removed directly.
	PatchRequest PatchFunc

	// DisableStreamUsage set to true for providers that do not recognize the stream_options field.
	DisableStreamUsage bool
}

// OpenAIProtocol is the adapter for OpenAI-compatible protocols.
type OpenAIProtocol struct {
	providerID string
	baseURL    string
	apiKey     string
	client     *http.Client
	quirks     Quirks
}

// NewOpenAI constructs the protocol adapter.
// providerID: the provider identifier, used for error messages and log attribution.
// baseURL: the API root URL, e.g. https://api.deepseek.com/v1.
// apiKey: the authentication key.
// returns: a ready-to-use adapter instance.
func NewOpenAI(providerID, baseURL, apiKey string, quirks Quirks) *OpenAIProtocol {
	return &OpenAIProtocol{
		providerID: providerID,
		baseURL:    baseURL,
		apiKey:     apiKey,
		// Per-request timeouts are controlled by ctx; the client layer only caps to prevent leaks.
		client: &http.Client{}, // the overall timeout is controlled by the caller's ctx; Client.Timeout would cut off long streaming responses
		quirks: quirks,
	}
}

// Chat sends a non-streaming chat request.
func (p *OpenAIProtocol) Chat(ctx context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	// The client layer sets no overall timeout (it would cut off long streams); non-streaming calls apply their own fallback here.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	body, err := p.buildBody(req, false)
	if err != nil {
		return nil, err
	}
	resp, err := p.post(ctx, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, core.NewError(core.ErrNetwork, p.providerID, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, p.httpError(resp.StatusCode, resp.Header.Get("Retry-After"), raw)
	}

	var envelope struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content   string           `json:"content"`
				Reasoning string           `json:"reasoning_content"`
				ToolCalls []openAIToolCall `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
		Usage openAIUsage `json:"usage"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, core.NewError(core.ErrProviderInternal, p.providerID,
			fmt.Errorf("decode response: %w", err))
	}
	if len(envelope.Choices) == 0 {
		return nil, core.NewError(core.ErrProviderInternal, p.providerID,
			fmt.Errorf("empty choices in response"))
	}

	choice := envelope.Choices[0]
	out := &core.ChatResponse{
		ID:           envelope.ID,
		Model:        envelope.Model,
		Content:      choice.Message.Content,
		Reasoning:    choice.Message.Reasoning,
		FinishReason: normalizeFinish(choice.FinishReason),
		Usage:        envelope.Usage.toCore(),
	}
	for _, tc := range choice.Message.ToolCalls {
		out.ToolCalls = append(out.ToolCalls, core.ToolCall{
			ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments,
		})
	}
	return out, nil
}

// buildBody converts the unified request into an OpenAI-format map.
//
// A map is used rather than a struct so that Quirks.PatchRequest can add or remove arbitrary fields.
func (p *OpenAIProtocol) buildBody(req core.ChatRequest, stream bool) (map[string]any, error) {
	msgs := make([]map[string]any, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, buildMessage(m))
	}
	body := map[string]any{
		"model":    req.Model,
		"messages": msgs,
	}
	if len(req.Tools) > 0 {
		tools := make([]map[string]any, 0, len(req.Tools))
		for _, t := range req.Tools {
			var params any
			if len(t.Parameters) > 0 {
				params = json.RawMessage(t.Parameters)
			} else {
				// Some vendors require the schema of a no-argument tool to be an object.
				params = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			tools = append(tools, map[string]any{
				"type": "function",
				"function": map[string]any{
					"name": t.Name, "description": t.Description, "parameters": params,
				},
			})
		}
		body["tools"] = tools
	}
	if req.Temperature != nil {
		body["temperature"] = clampFloat(*req.Temperature)
	}
	if req.MaxTokens > 0 {
		body["max_tokens"] = req.MaxTokens
	}
	if req.ResponseFormat != nil && len(req.ResponseFormat.Schema) > 0 {
		name := req.ResponseFormat.Name
		if name == "" {
			name = "output"
		}
		body["response_format"] = map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   name,
				"schema": json.RawMessage(req.ResponseFormat.Schema),
				// strict mode rejects content that does not match the schema; otherwise the constraint is merely advisory.
				"strict": true,
			},
		}
	}
	for k, v := range req.Extra {
		body[k] = v
	}
	if stream {
		body["stream"] = true
		if !p.quirks.DisableStreamUsage {
			body["stream_options"] = map[string]any{"include_usage": true}
		}
	}
	if p.quirks.PatchRequest != nil {
		p.quirks.PatchRequest(body, req)
	}
	return body, nil
}

// buildMessage converts a single message; Reasoning is not sent back.
func buildMessage(m core.Message) map[string]any {
	msg := map[string]any{"role": string(m.Role)}
	if len(m.ContentParts) > 0 {
		msg["content"] = openAIParts(m.ContentParts)
	} else {
		msg["content"] = m.Content
	}
	if len(m.ToolCalls) > 0 {
		calls := make([]map[string]any, 0, len(m.ToolCalls))
		for _, tc := range m.ToolCalls {
			calls = append(calls, map[string]any{
				"id":   tc.ID,
				"type": "function",
				"function": map[string]any{
					"name":      tc.Name,
					"arguments": tc.Arguments,
				},
			})
		}
		msg["tool_calls"] = calls
	}
	if m.ToolCallID != "" {
		msg["tool_call_id"] = m.ToolCallID
	}
	return msg
}

// post sends the request; network-layer errors are uniformly wrapped.
func (p *OpenAIProtocol) post(ctx context.Context, body map[string]any) (*http.Response, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, core.NewError(core.ErrInvalidRequest, p.providerID, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, core.NewError(core.ErrInvalidRequest, p.providerID, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, core.NewError(core.ErrNetwork, p.providerID, err)
	}
	return resp, nil
}

// httpError converts an HTTP error response into a unified error, attaching retry wait information.
func (p *OpenAIProtocol) httpError(status int, retryAfter string, body []byte) error {
	kind := core.ErrProviderInternal
	switch {
	case status == 400 || status == 404 || status == 422:
		kind = core.ErrInvalidRequest
	case status == 401:
		kind = core.ErrAuth
	case status == 403:
		kind = core.ErrPermission
	case status == 429:
		kind = core.ErrRateLimited
	}
	// Extract the structured error message; fall back to the raw body on failure so it stays diagnosable.
	msg := string(body)
	var errEnvelope struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &errEnvelope) == nil && errEnvelope.Error.Message != "" {
		msg = errEnvelope.Error.Message
	}
	ce := core.NewError(kind, p.providerID, fmt.Errorf("http %d: %s", status, msg))
	ce.StatusCode = status
	if status == 429 && retryAfter != "" {
		if secs, err := strconv.Atoi(retryAfter); err == nil {
			d := time.Duration(secs) * time.Second
			ce.RetryAfter = &d
		}
	}
	return ce
}

// normalizeFinish unifies finish reasons; unknown values are treated as stop.
func normalizeFinish(reason string) core.FinishReason {
	switch reason {
	case "tool_calls", "function_call":
		return core.FinishToolCalls
	case "length":
		return core.FinishLength
	case "content_filter":
		return core.FinishContentFilter
	default:
		return core.FinishStop
	}
}

// openAIParts converts multimodal parts into an OpenAI content array.
// returns: the array of parts.
func openAIParts(parts []core.ContentPart) []map[string]any {
	out := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		switch p.Type {
		case "image":
			out = append(out, map[string]any{
				"type":      "image_url",
				"image_url": map[string]any{"url": p.ImageURL},
			})
		default:
			out = append(out, map[string]any{"type": "text", "text": p.Text})
		}
	}
	return out
}

// clampFloat sanitizes NaN/Inf to avoid JSON serialization failures.
func clampFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

// openAIToolCall is the protocol-level tool call structure.
type openAIToolCall struct {
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// openAIUsage is the protocol-level usage structure.
type openAIUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	// Some providers (DeepSeek/GLM) report thinking tokens in completion_tokens_details.
	CompletionTokensDetails struct {
		ReasoningTokens int64 `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

// toCore converts to the unified usage.
//
// In the OpenAI family (including DeepSeek/GLM), completion_tokens already
// includes thinking tokens and details is only a subset breakdown: the output
// side must be subtracted, otherwise CostOf and Total double-count the billing;
// Gemini's thoughtsTokenCount is an independent figure added directly by its adapter.
func (u openAIUsage) toCore() core.Usage {
	output := u.CompletionTokens - u.CompletionTokensDetails.ReasoningTokens
	if output < 0 {
		output = 0
	}
	return core.Usage{
		InputTokens:     u.PromptTokens,
		OutputTokens:    output,
		ReasoningTokens: u.CompletionTokensDetails.ReasoningTokens,
	}
}
