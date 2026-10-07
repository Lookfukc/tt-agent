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

// GeminiProtocol Google Gemini API 适配器
//
// 与 OpenAI 的关键差异：role 用 model 而非 assistant；system 是
// 独立的 systemInstruction；functionCall 无 ID，需本地合成
type GeminiProtocol struct {
	providerID string
	baseURL    string
	apiKey     string
	client     *http.Client
	quirks     Quirks
}

// NewGemini 构造协议适配器
// providerID: 提供商标识
// baseURL: API 根地址，如 https://generativelanguage.googleapis.com/v1beta
// apiKey: 鉴权密钥
// returns: 可用的适配器实例
func NewGemini(providerID, baseURL, apiKey string, quirks Quirks) *GeminiProtocol {
	return &GeminiProtocol{
		providerID: providerID,
		baseURL:    baseURL,
		apiKey:     apiKey,
		client:     &http.Client{}, // 总超时由调用点 ctx 控制，Client.Timeout 会砍断长流式响应
		quirks:     quirks,
	}
}

// Chat 发送非流式对话请求
func (p *GeminiProtocol) Chat(ctx context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	// client 层不设总超时（会砍断长流式），非流式在此自兜底
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

// buildBody 构建请求体
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
			// 并行工具的多条结果合并进同一条 user content，
			// 官方模式如此，逐条映射会产出连续 user 角色
			frParts := []map[string]any{{
				"functionResponse": map[string]any{
					// response 必须是对象，纯文本包一层 result
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
		// thinkingBudget 0 关、-1 动态，正数为固定预算
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

// geminiPartOf 转换单个多模态分片
//
// Data URI 转 inlineData；外链图片明确报错：
// fileData.fileUri 只接受 Files API 返回的 URI，
// 塞 http URL 会 400，静默映射不如装配期拒绝
// returns: Gemini part 或错误
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

// toolNameOf 从合成 ID 里恢复函数名
//
// Gemini 靠 name 而非 ID 关联，适配器合成的 ID 形如 gemini:<name>，
// 同名并行调用消歧后为 gemini:<name>:<出现序号>。工具名约定为
// [a-zA-Z0-9_-]、不含 ':'，因此按最后一个 ':' 截掉序号是安全的
// returns: 函数名，还原失败时退回 ID 原文
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

// geminiCallID 合成 functionCall 的调用 ID
//
// Gemini 原生不返回调用 ID；同名并行调用直接用名字会撞号，
// 跨协议回放历史时无法区分。第二次同名调用起在名字后追加
// 出现序号（从 1 计）消歧，首次调用保持旧格式 gemini:<name>，
// 兼容既有会话记录
func geminiCallID(name string, seq int) string {
	if seq <= 0 {
		return "gemini:" + name
	}
	return fmt.Sprintf("gemini:%s:%d", name, seq)
}

// post 发送请求
// action: :generateContent 或 :streamGenerateContent
// returns: HTTP 响应
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

// parseResponse 解析非流式响应
// returns: 统一响应
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
			// Gemini 不返回调用 ID，合成 ID 内嵌函数名，tool 消息
			// 回传 functionResponse 时用它还原 name；同名并行调用
			// 按出现序号消歧，避免 ID 撞号
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

// httpError HTTP 错误转统一错误
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

// geminiFinish 终止原因映射
// returns: 统一终止原因
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

// geminiPart 响应内容分片
type geminiPart struct {
	Text string `json:"text"`
	// Thought 思考分片标记
	Thought bool `json:"thought,omitempty"`
	// FunctionCall 函数调用分片
	FunctionCall *struct {
		Name string         `json:"name"`
		Args map[string]any `json:"args"`
	} `json:"functionCall,omitempty"`
}
