package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// GeminiProtocol is the adapter for the Google Gemini API.
//
// Key differences from OpenAI: the role is "model" rather than "assistant";
// system is a separate systemInstruction; functionCall has no ID and one
// must be synthesized locally.
type GeminiProtocol struct {
	providerID string
	baseURL    string
	apiKey     string
	client     *http.Client
	quirks     Quirks
}

// NewGemini constructs the protocol adapter.
// providerID: the provider identifier.
// baseURL: the API root URL, e.g. https://generativelanguage.googleapis.com/v1beta.
// apiKey: the authentication key.
// returns: a ready-to-use adapter instance.
func NewGemini(providerID, baseURL, apiKey string, quirks Quirks) *GeminiProtocol {
	return &GeminiProtocol{
		providerID: providerID,
		baseURL:    baseURL,
		apiKey:     apiKey,
		client:     &http.Client{}, // the overall timeout is controlled by the caller's ctx; Client.Timeout would cut off long streaming responses
		quirks:     quirks,
	}
}

// Chat sends a non-streaming chat request.
func (p *GeminiProtocol) Chat(ctx context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	// The client layer sets no overall timeout (it would cut off long streams); non-streaming calls apply their own fallback here.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	body, err := p.buildBody(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.post(ctx, body, ":generateContent")
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
	return p.parseResponse(raw)
}

// buildBody builds the request body.
func (p *GeminiProtocol) buildBody(req core.ChatRequest) (map[string]any, error) {
	var system string
	contents := make([]map[string]any, 0, len(req.Messages))
	for i := 0; i < len(req.Messages); i++ {
		m := req.Messages[i]
		role := string(m.Role)
		switch m.Role {
		case core.RoleSystem:
			system += m.Content
			continue
		case core.RoleAssistant:
			role = "model"
		case core.RoleTool:
			// Multiple results from parallel tools are merged into a single user
			// content; this is the official pattern — mapping them one-by-one
			// would produce consecutive user roles.
			frParts := []map[string]any{{
				"functionResponse": map[string]any{
					// response must be an object; wrap plain text in a result field.
					"name":     toolNameOf(m.ToolCallID),
					"response": map[string]any{"result": m.Content},
				},
			}}
			for j := i + 1; j < len(req.Messages) && req.Messages[j].Role == core.RoleTool; j++ {
				nxt := req.Messages[j]
				frParts = append(frParts, map[string]any{
					"functionResponse": map[string]any{
						"name":     toolNameOf(nxt.ToolCallID),
						"response": map[string]any{"result": nxt.Content},
					},
				})
				i = j
			}
			contents = append(contents, map[string]any{"role": "user", "parts": frParts})
			continue
		}

		parts := make([]map[string]any, 0, 1+len(m.ToolCalls))
		if len(m.ContentParts) > 0 {
			for _, cp := range m.ContentParts {
				gp, err := geminiPartOf(cp)
				if err != nil {
					return nil, err
				}
				parts = append(parts, gp)
			}
		} else if m.Content != "" {
			parts = append(parts, map[string]any{"text": m.Content})
		}
		for _, tc := range m.ToolCalls {
			var args any
			if tc.Arguments != "" {
				args = json.RawMessage(tc.Arguments)
			} else {
				args = map[string]any{}
			}
			parts = append(parts, map[string]any{
				"functionCall": map[string]any{"name": tc.Name, "args": args},
			})
		}
		if len(parts) == 0 {
			parts = append(parts, map[string]any{"text": ""})
		}
		contents = append(contents, map[string]any{"role": role, "parts": parts})
	}

	body := map[string]any{
		"model":    req.Model,
		"contents": contents,
	}
	if system != "" {
		body["systemInstruction"] = map[string]any{
			"parts": []map[string]any{{"text": system}},
		}
	}
	if len(req.Tools) > 0 {
		decls := make([]map[string]any, 0, len(req.Tools))
		for _, t := range req.Tools {
			var params any
			if len(t.Parameters) > 0 {
				params = json.RawMessage(t.Parameters)
			} else {
				params = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			decls = append(decls, map[string]any{
				"name": t.Name, "description": t.Description, "parameters": params,
			})
		}
		body["tools"] = []map[string]any{{"functionDeclarations": decls}}
	}

	genCfg := map[string]any{}
	if req.Temperature != nil {
		genCfg["temperature"] = clampFloat(*req.Temperature)
	}
	if req.MaxTokens > 0 {
		genCfg["maxOutputTokens"] = req.MaxTokens
	}
	if req.Thinking != nil {
		// thinkingBudget: 0 disables, -1 is dynamic, a positive number is a fixed budget.
		budget := int64(0)
		if req.Thinking.Enabled {
			budget = -1
			if req.Thinking.BudgetTokens > 0 {
				budget = req.Thinking.BudgetTokens
			}
		}
		genCfg["thinkingConfig"] = map[string]any{"thinkingBudget": budget}
	}
	if req.ResponseFormat != nil && len(req.ResponseFormat.Schema) > 0 {
		genCfg["responseMimeType"] = "application/json"
		genCfg["responseSchema"] = json.RawMessage(req.ResponseFormat.Schema)
	}
	if len(genCfg) > 0 {
		body["generationConfig"] = genCfg
	}
	for k, v := range req.Extra {
		body[k] = v
	}
	if p.quirks.PatchRequest != nil {
		p.quirks.PatchRequest(body, req)
	}
	return body, nil
}

// geminiPartOf converts a single multimodal part.
//
// Data URIs become inlineData; external image links fail explicitly:
// fileData.fileUri only accepts URIs returned by the Files API, and
// stuffing an http URL into it yields a 400 — rejecting at assembly
// time beats mapping silently.
// returns: a Gemini part or an error.
func geminiPartOf(p core.ContentPart) (map[string]any, error) {
	if p.Type == "image" {
		if mime, data, ok := p.ImageData(); ok {
			return map[string]any{
				"inlineData": map[string]any{"mimeType": mime, "data": data},
			}, nil
		}
		return nil, fmt.Errorf(
			"gemini: external image url %q not supported, upload via Files API or pass a data URI", p.ImageURL)
	}
	return map[string]any{"text": p.Text}, nil
}

// toolNameOf recovers the function name from a synthesized ID.
//
// Gemini correlates by name rather than ID; adapter-synthesized IDs have the
// form gemini:<name>, disambiguated as gemini:<name>:<occurrence-index> for
// parallel calls with the same name. Tool names are constrained to
// [a-zA-Z0-9_-] and contain no ':', so cutting the index at the last ':' is safe.
// returns: the function name; falls back to the raw ID when recovery fails.
func toolNameOf(callID string) string {
	rest, ok := strings.CutPrefix(callID, "gemini:")
	if !ok {
		return callID
	}
	if i := strings.LastIndex(rest, ":"); i >= 0 {
		return rest[:i]
	}
	return rest
}

// geminiCallID synthesizes a call ID for a functionCall.
//
// Gemini does not natively return call IDs; using the bare name for parallel
// calls with the same name would collide, making them indistinguishable when
// replaying history across protocols. From the second same-name call onward an
// occurrence index (counting from 1) is appended for disambiguation, while the
// first call keeps the legacy format gemini:<name> for compatibility with
// existing session records.
func geminiCallID(name string, seq int) string {
	if seq <= 0 {
		return "gemini:" + name
	}
	return fmt.Sprintf("gemini:%s:%d", name, seq)
}

// post sends the request.
// action: ":generateContent" or ":streamGenerateContent".
// returns: the HTTP response.
func (p *GeminiProtocol) post(ctx context.Context, body map[string]any, action string) (*http.Response, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, core.NewError(core.ErrInvalidRequest, p.providerID, err)
	}
	url := fmt.Sprintf("%s/models/%s%s", p.baseURL, body["model"], action)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, core.NewError(core.ErrInvalidRequest, p.providerID, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-goog-api-key", p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, core.NewError(core.ErrNetwork, p.providerID, err)
	}
	return resp, nil
}

// parseResponse parses a non-streaming response.
// returns: the unified response.
func (p *GeminiProtocol) parseResponse(raw []byte) (*core.ChatResponse, error) {
	var envelope struct {
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
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, core.NewError(core.ErrProviderInternal, p.providerID,
			fmt.Errorf("decode response: %w", err))
	}
	if len(envelope.Candidates) == 0 {
		return nil, core.NewError(core.ErrProviderInternal, p.providerID,
			fmt.Errorf("empty candidates in response"))
	}

	out := &core.ChatResponse{
		FinishReason: geminiFinish(envelope.Candidates[0].FinishReason),
		Usage: core.Usage{
			InputTokens:     envelope.UsageMetadata.PromptTokenCount,
			OutputTokens:    envelope.UsageMetadata.CandidatesTokenCount,
			ReasoningTokens: envelope.UsageMetadata.ThoughtsTokenCount,
		},
	}
	nameSeq := map[string]int{}
	for _, part := range envelope.Candidates[0].Content.Parts {
		if part.Thought {
			out.Reasoning += part.Text
			continue
		}
		if part.Text != "" {
			out.Content += part.Text
		}
		if part.FunctionCall != nil && part.FunctionCall.Name != "" {
			args, _ := json.Marshal(part.FunctionCall.Args)
			if string(args) == "null" {
				args = []byte("{}")
			}
			// Gemini returns no call ID; the synthesized ID embeds the function
			// name, which is recovered when a tool message sends back the
			// functionResponse; parallel calls with the same name are
			// disambiguated by occurrence index to avoid ID collisions.
			seq := nameSeq[part.FunctionCall.Name]
			nameSeq[part.FunctionCall.Name] = seq + 1
			out.ToolCalls = append(out.ToolCalls, core.ToolCall{
				ID:        geminiCallID(part.FunctionCall.Name, seq),
				Name:      part.FunctionCall.Name,
				Arguments: string(args),
			})
		}
	}
	return out, nil
}

// httpError converts an HTTP error into a unified error.
func (p *GeminiProtocol) httpError(status int, retryAfter string, body []byte) error {
	kind := core.ErrProviderInternal
	switch {
	case status == 400 || status == 404:
		kind = core.ErrInvalidRequest
	case status == 401 || status == 403:
		kind = core.ErrAuth
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

// geminiFinish maps finish reasons.
// returns: the unified finish reason.
func geminiFinish(reason string) core.FinishReason {
	switch reason {
	case "MAX_TOKENS":
		return core.FinishLength
	case "SAFETY", "PROHIBITED_CONTENT":
		return core.FinishContentFilter
	default:
		return core.FinishStop
	}
}

// geminiPart is a response content part.
type geminiPart struct {
	Text string `json:"text"`
	// Thought marks a thinking part.
	Thought bool `json:"thought,omitempty"`
	// FunctionCall is a function call part.
	FunctionCall *struct {
		Name string         `json:"name"`
		Args map[string]any `json:"args"`
	} `json:"functionCall,omitempty"`
}
