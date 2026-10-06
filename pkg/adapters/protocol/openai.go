// Package protocol 实现 OpenAI 兼容协议的请求构建、响应解析与流式解码
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

	"github.com/Lookfukc/send-agent/pkg/core"
)

// Quirks 同一 OpenAI 协议下各提供商的偏差修正点
//
// 厂商偏差集中在两处：请求体需要额外字段或删减字段；响应中
// 私有扩展字段。以 hook 形式注入而非派生子类，新厂商零代码接入
type Quirks struct {
	// PatchRequest 请求体序列化前的修补，body 为顶层 map，可直接增删字段
	// body: 待发送的请求体，in place 修改
	// req: 原始统一请求，含 Thinking/Extra 等未落入 body 的信息
	PatchRequest func(body map[string]any, req core.ChatRequest)

	// DisableStreamUsage 部分提供商不认 stream_options 字段时置 true
	DisableStreamUsage bool
}

// OpenAIProtocol OpenAI 兼容协议适配器
type OpenAIProtocol struct {
	providerID string
	baseURL    string
	apiKey     string
	client     *http.Client
	quirks     Quirks
}

// NewOpenAI 构造协议适配器
// providerID: 提供商标识，用于错误信息与日志定位
// baseURL: API 根地址，如 https://api.deepseek.com/v1
// apiKey: 鉴权密钥
// returns: 可用的适配器实例
func NewOpenAI(providerID, baseURL, apiKey string, quirks Quirks) *OpenAIProtocol {
	return &OpenAIProtocol{
		providerID: providerID,
		baseURL:    baseURL,
		apiKey:     apiKey,
		// 每请求级超时由 ctx 控制，client 层只设上限防泄漏
		client: &http.Client{}, // 总超时由调用点 ctx 控制，Client.Timeout 会砍断长流式响应
		quirks: quirks,
	}
}

// Chat 发送非流式对话请求
func (p *OpenAIProtocol) Chat(ctx context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	// client 层不设总超时（会砍断长流式），非流式在此自兜底
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

// buildBody 将统一请求转为 OpenAI 格式 map
//
// 用 map 而非 struct 是为了让 Quirks.PatchRequest 能增删任意字段
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
				// 部分厂商对无参工具要求 schema 必须是 object
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
				// strict 模式拒绝非 schema 内容，否则约束只是建议
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

// buildMessage 转换单条消息，Reasoning 不回传
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

// post 发送请求，网络层错误统一包装
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

// httpError 将 HTTP 错误响应转为统一错误，附重试等待信息
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
	// 提取结构化错误信息，失败时退回原始 body，保底可诊断
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

// normalizeFinish 统一终止原因，未知值按 stop 处理
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

// openAIParts 转换多模态分片为 OpenAI content 数组
// returns: 分片数组
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

// clampFloat 清理 NaN/Inf，避免 JSON 序列化失败
func clampFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

// openAIToolCall 协议层工具调用结构
type openAIToolCall struct {
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// openAIUsage 协议层用量结构
type openAIUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	// 部分提供商（DeepSeek/GLM）在 completion_tokens_details 中给思考 token
	CompletionTokensDetails struct {
		ReasoningTokens int64 `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

// toCore 转为统一用量
//
// OpenAI 系（含 DeepSeek/GLM）的 completion_tokens 已包含思考 token，
// details 只是子集拆分：输出侧必须相减，否则 CostOf 与 Total 双重计费；
// Gemini 的 thoughtsTokenCount 是独立口径，由其适配器直接相加
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
