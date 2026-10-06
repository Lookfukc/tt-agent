package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// anthropicVersion API 版本号，Anthropic 强制要求随请求携带
const anthropicVersion = "2023-06-01"

// defaultAnthropicMaxTokens max_tokens 为 Anthropic 必填项，统一请求缺省值
const defaultAnthropicMaxTokens = 4096

// AnthropicProtocol Anthropic Messages API 适配器
type AnthropicProtocol struct {
	providerID string
	baseURL    string
	apiKey     string
	client     *http.Client
	quirks     Quirks
}

// NewAnthropic 构造协议适配器
// providerID: 提供商标识
// baseURL: API 根地址，如 https://api.anthropic.com
// apiKey: 鉴权密钥
// returns: 可用的适配器实例
func NewAnthropic(providerID, baseURL, apiKey string, quirks Quirks) *AnthropicProtocol {
	return &AnthropicProtocol{
		providerID: providerID,
		baseURL:    baseURL,
		apiKey:     apiKey,
		client:     &http.Client{}, // 总超时由调用点 ctx 控制，Client.Timeout 会砍断长流式响应
		quirks:     quirks,
	}
}

// Chat 发送非流式对话请求
func (p *AnthropicProtocol) Chat(ctx context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
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

// buildBody 构建请求体
//
// 与 OpenAI 的关键差异：system 是顶层字段而非消息；max_tokens 必填；
// 消息 content 是块数组，tool 结果以 tool_result 块回传
func (p *AnthropicProtocol) buildBody(req core.ChatRequest, stream bool) (map[string]any, error) {
	var system string
	msgs := make([]map[string]any, 0, len(req.Messages))
	for i := 0; i < len(req.Messages); i++ {
		m := req.Messages[i]
		switch {
		case m.Role == core.RoleSystem:
			system += m.Content
		case m.Role == core.RoleTool:
			// 角色必须严格交替：并行工具的多条结果合并进
			// 同一条 user 消息，逐条映射会产出连续 user 而 400
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
		// Anthropic 思考模式必须给预算，下限 1024
		thinkingBudget = req.Thinking.BudgetTokens
		if thinkingBudget < 1024 {
			thinkingBudget = 1024
		}
		body["thinking"] = map[string]any{"type": "enabled", "budget_tokens": thinkingBudget}
	}
	// Anthropic 无原生 response_format；静默忽略会让调用方以为
	// 约束生效，显式报错迫使其选别的协议或方案。
	// 用 ErrUnsupported 包装：裸 fmt.Errorf 会被统一错误分类
	// 兜底成可重试的网络错误，导致无意义重放
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
		// 思考开启时 API 只接受默认采样参数，带上 temperature/top_p 直接 400；
		// 清理必须放在 Extra 合并与 PatchRequest 之后，否则用户或
		// quirk 注入的采样参数会绕过拦截
		delete(body, "temperature")
		delete(body, "top_p")
		// API 硬性要求 max_tokens > thinking.budget_tokens，违反直接 400；
		// 不足时抬到 budget+1024，保证思考之外还有可见输出空间
		if mt, ok := bodyInt(body["max_tokens"]); !ok || mt <= thinkingBudget {
			body["max_tokens"] = thinkingBudget + 1024
		}
	}
	return body, nil
}

// bodyInt 提取请求体中数值字段的 int64 值
//
// body 值可能来自本适配器（int），也可能来自 Extra 透传
// （经 JSON 反序列化后是 float64），两种都要认
// returns: 数值与是否解析成功
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

// anthropicParts 转换多模态分片为 Anthropic content 块
//
// Data URI 转 base64 source；http URL 保持 url source
// returns: 内容块数组
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

// post 发送请求
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

// httpError HTTP 错误转统一错误，状态码语义与 OpenAI 对齐
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

// parseRetryAfter 解析 Retry-After 头
// returns: 等待时长；解析失败时 ok 为 false
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

// anthropicFinish 终止原因映射
// returns: 统一终止原因
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

// anthropicBlock 响应内容块
type anthropicBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
	// Thinking thinking 块内容
	Thinking string `json:"thinking"`
	// ID tool_use 块标识
	ID string `json:"id"`
	// Name tool_use 块工具名
	Name string `json:"name"`
	// Input tool_use 块参数对象，透传为 JSON 字符串
	Input json.RawMessage `json:"input"`
}

// anthropicUsage 用量结构
type anthropicUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}
