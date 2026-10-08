package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// anthropicVersion is the API version string; Anthropic requires it on every request.
const anthropicVersion = "2023-06-01"

// defaultAnthropicMaxTokens is the default max_tokens used to satisfy the Anthropic requirement that the field be present.
const defaultAnthropicMaxTokens = 4096

// AnthropicProtocol is the adapter for the Anthropic Messages API.
type AnthropicProtocol struct {
	providerID string
	baseURL    string
	apiKey     string
	client     *http.Client
	quirks     Quirks
}

// NewAnthropic constructs the protocol adapter.
// providerID: the provider identifier.
// baseURL: the API root URL, e.g. https://api.anthropic.com.
// apiKey: the authentication key.
// returns: a ready-to-use adapter instance.
func NewAnthropic(providerID, baseURL, apiKey string, quirks Quirks) *AnthropicProtocol {
	return &AnthropicProtocol{
		providerID: providerID,
		baseURL:    baseURL,
		apiKey:     apiKey,
		client:     &http.Client{}, // the overall timeout is controlled by the caller's ctx; Client.Timeout would cut off long streaming responses
		quirks:     quirks,
	}
}

// Chat sends a non-streaming chat request.
func (p *AnthropicProtocol) Chat(ctx context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
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
		ID         string           `json:"id"`
		Model      string           `json:"model"`
		Content    []anthropicBlock `json:"content"`
		StopReason string           `json:"stop_reason"`
		Usage      anthropicUsage   `json:"usage"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, core.NewError(core.ErrProviderInternal, p.providerID,
			fmt.Errorf("decode response: %w", err))
	}

	out := &core.ChatResponse{
		ID:           envelope.ID,
		Model:        envelope.Model,
		FinishReason: anthropicFinish(envelope.StopReason),
		Usage: core.Usage{
			InputTokens:  envelope.Usage.InputTokens,
			OutputTokens: envelope.Usage.OutputTokens,
		},
	}
	for _, b := range envelope.Content {
		switch b.Type {
		case "text":
			out.Content += b.Text
		case "thinking":
			out.Reasoning += b.Thinking
		case "tool_use":
			out.ToolCalls = append(out.ToolCalls, core.ToolCall{
				ID: b.ID, Name: b.Name, Arguments: string(b.Input),
			})
		}
	}
	return out, nil
}

// buildBody builds the request body.
//
// Key differences from OpenAI: system is a top-level field rather than a message;
// max_tokens is required; message content is an array of blocks, and tool results
// are sent back as tool_result blocks.
func (p *AnthropicProtocol) buildBody(req core.ChatRequest, stream bool) (map[string]any, error) {
	var system string
	msgs := make([]map[string]any, 0, len(req.Messages))
	for i := 0; i < len(req.Messages); i++ {
		m := req.Messages[i]
		switch {
		case m.Role == core.RoleSystem:
			system += m.Content
		case m.Role == core.RoleTool:
			// Roles must strictly alternate: multiple results from parallel tools are
			// merged into a single user message; mapping them one-by-one would produce
			// consecutive user messages and a 400.
			blocks := []map[string]any{{
				"type":        "tool_result",
				"tool_use_id": m.ToolCallID,
				"content":     m.Content,
			}}
			for j := i + 1; j < len(req.Messages) && req.Messages[j].Role == core.RoleTool; j++ {
				nxt := req.Messages[j]
				blocks = append(blocks, map[string]any{
					"type":        "tool_result",
					"tool_use_id": nxt.ToolCallID,
					"content":     nxt.Content,
				})
				i = j
			}
			msgs = append(msgs, map[string]any{"role": "user", "content": blocks})
		case m.Role == core.RoleAssistant && len(m.ToolCalls) > 0:
			blocks := make([]map[string]any, 0, len(m.ToolCalls)+1)
			if m.Content != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": m.Content})
			}
			for _, tc := range m.ToolCalls {
				var input any
				if tc.Arguments != "" {
					input = json.RawMessage(tc.Arguments)
				} else {
					input = map[string]any{}
				}
				blocks = append(blocks, map[string]any{
					"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": input,
				})
			}
			msgs = append(msgs, map[string]any{"role": "assistant", "content": blocks})
		default:
			content := []map[string]any{{"type": "text", "text": m.Content}}
			if len(m.ContentParts) > 0 {
				content = anthropicParts(m.ContentParts)
			}
			msgs = append(msgs, map[string]any{"role": m.Role, "content": content})
		}
	}

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = defaultAnthropicMaxTokens
	}
	body := map[string]any{
		"model":      req.Model,
		"max_tokens": maxTokens,
		"messages":   msgs,
	}
	if system != "" {
		body["system"] = system
	}
	if len(req.Tools) > 0 {
		tools := make([]map[string]any, 0, len(req.Tools))
		for _, t := range req.Tools {
			var schema any
			if len(t.Parameters) > 0 {
				schema = json.RawMessage(t.Parameters)
			} else {
				schema = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			tools = append(tools, map[string]any{
				"name": t.Name, "description": t.Description, "input_schema": schema,
			})
		}
		body["tools"] = tools
	}
	if req.Temperature != nil {
		body["temperature"] = clampFloat(*req.Temperature)
	}
	thinkingBudget := int64(0)
	if req.Thinking != nil && req.Thinking.Enabled {
		// Anthropic's thinking mode requires a budget with a minimum of 1024.
		thinkingBudget = req.Thinking.BudgetTokens
		if thinkingBudget < 1024 {
			thinkingBudget = 1024
		}
		body["thinking"] = map[string]any{"type": "enabled", "budget_tokens": thinkingBudget}
	}
	// Anthropic has no native response_format; silently ignoring it would make
	// callers believe the constraint took effect, so fail explicitly to force
	// them to pick another protocol or approach.
	// Wrap with ErrUnsupported: a bare fmt.Errorf would fall through the unified
	// error classification into a retryable network error, causing pointless replays.
	if req.ResponseFormat != nil {
		return nil, core.NewError(core.ErrUnsupported, p.providerID,
			fmt.Errorf("anthropic: ResponseFormat not supported, use tool-forced structured output instead"))
	}
	for k, v := range req.Extra {
		body[k] = v
	}
	if stream {
		body["stream"] = true
	}
	if p.quirks.PatchRequest != nil {
		p.quirks.PatchRequest(body, req)
	}
	if thinkingBudget > 0 {
		// When thinking is enabled the API only accepts default sampling parameters;
		// sending temperature/top_p yields an immediate 400.
		// The cleanup must happen after the Extra merge and PatchRequest, otherwise
		// sampling parameters injected by the user or a quirk would bypass the guard.
		delete(body, "temperature")
		delete(body, "top_p")
		// The API hard-requires max_tokens > thinking.budget_tokens; violating it is an immediate 400.
		// When insufficient, raise it to budget+1024 to guarantee visible output room beyond the thinking.
		if mt, ok := bodyInt(body["max_tokens"]); !ok || mt <= thinkingBudget {
			body["max_tokens"] = thinkingBudget + 1024
		}
	}
	return body, nil
}

// bodyInt extracts the int64 value of a numeric field in the request body.
//
// Body values may come from this adapter (int) or be passed through from Extra
// (float64 after JSON deserialization); both must be recognized.
// returns: the numeric value and whether parsing succeeded.
func bodyInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}

// anthropicParts converts multimodal parts into Anthropic content blocks.
//
// Data URIs become base64 sources; http URLs stay as url sources.
// returns: the array of content blocks.
func anthropicParts(parts []core.ContentPart) []map[string]any {
	out := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		switch p.Type {
		case "image":
			if mime, data, ok := p.ImageData(); ok {
				out = append(out, map[string]any{
					"type": "image",
					"source": map[string]any{
						"type": "base64", "media_type": mime, "data": data,
					},
				})
			} else {
				out = append(out, map[string]any{
					"type":   "image",
					"source": map[string]any{"type": "url", "url": p.ImageURL},
				})
			}
		default:
			out = append(out, map[string]any{"type": "text", "text": p.Text})
		}
	}
	return out
}

// post sends the request.
func (p *AnthropicProtocol) post(ctx context.Context, body map[string]any) (*http.Response, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, core.NewError(core.ErrInvalidRequest, p.providerID, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/v1/messages", bytes.NewReader(payload))
	if err != nil {
		return nil, core.NewError(core.ErrInvalidRequest, p.providerID, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, core.NewError(core.ErrNetwork, p.providerID, err)
	}
	return resp, nil
}

// httpError converts an HTTP error into a unified error; status code semantics are aligned with OpenAI.
func (p *AnthropicProtocol) httpError(status int, retryAfter string, body []byte) error {
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
	if d, ok := parseRetryAfter(retryAfter); ok {
		ce.RetryAfter = &d
	}
	return ce
}

// parseRetryAfter parses the Retry-After header.
// returns: the wait duration; ok is false when parsing fails.
func parseRetryAfter(v string) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	var secs int
	if _, err := fmt.Sscanf(v, "%d", &secs); err != nil {
		return 0, false
	}
	return time.Duration(secs) * time.Second, true
}

// anthropicFinish maps finish reasons.
// returns: the unified finish reason.
func anthropicFinish(reason string) core.FinishReason {
	switch reason {
	case "tool_use":
		return core.FinishToolCalls
	case "max_tokens":
		return core.FinishLength
	case "refusal":
		return core.FinishContentFilter
	default:
		return core.FinishStop
	}
}

// anthropicBlock is a response content block.
type anthropicBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
	// Thinking is the content of a thinking block.
	Thinking string `json:"thinking"`
	// ID is the identifier of a tool_use block.
	ID string `json:"id"`
	// Name is the tool name of a tool_use block.
	Name string `json:"name"`
	// Input is the arguments object of a tool_use block, passed through as a JSON string.
	Input json.RawMessage `json:"input"`
}

// anthropicUsage is the usage structure.
type anthropicUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}
