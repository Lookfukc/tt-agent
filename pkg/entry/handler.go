package entry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/core"
)

// ChatBody 对话请求体
type ChatBody struct {
	SessionID    string `json:"session_id"`
	ProviderID   string `json:"provider_id"`
	Model        string `json:"model"`
	Input        string `json:"input"`
	Stream       bool   `json:"stream"`
	SystemPrompt string `json:"system_prompt"`
}

// handleHealth 存活探针
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleMetrics 输出运行指标快照
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.metrics.Snapshot())
}

// handleProviders 列出已注册提供商与模型
func (s *Server) handleProviders(w http.ResponseWriter, _ *http.Request) {
	type modelView struct {
		ID            string `json:"id"`
		Thinking      bool   `json:"thinking"`
		ToolCalls     bool   `json:"tool_calls"`
		ContextWindow int64  `json:"context_window"`
	}
	type providerView struct {
		ID           string      `json:"id"`
		Name         string      `json:"name"`
		DefaultModel string      `json:"default_model"`
		Models       []modelView `json:"models"`
	}
	out := make([]providerView, 0)
	for _, id := range s.registry.List() {
		cfg, _ := s.registry.Get(id)
		pv := providerView{ID: cfg.ID, Name: cfg.Name, DefaultModel: cfg.DefaultModel}
		for _, m := range cfg.Models {
			pv.Models = append(pv.Models, modelView{
				ID: m.ID, Thinking: m.Capabilities.Thinking,
				ToolCalls: m.Capabilities.ToolCalls, ContextWindow: m.Capabilities.ContextWindow,
			})
		}
		out = append(out, pv)
	}
	writeJSON(w, http.StatusOK, out)
}

// maxChatBody 请求体上限
//
// 端点无鉴权，不限长即可被灌入任意大 JSON 直到 OOM
const maxChatBody = 4 << 20

// handleChat 对话入口，stream=true 时以 SSE 返回过程事件
func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxChatBody)
	var body ChatBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json: " + err.Error()})
		return
	}
	if body.Input == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "input is required"})
		return
	}
	if body.SessionID == "" {
		body.SessionID = "default"
	}

	cfg, model, err := s.resolveModel(body.ProviderID, body.Model)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	llm, err := s.llmFor(cfg.ID)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}

	if body.Stream {
		s.chatStream(w, r, cfg, llm, model, body)
		return
	}

	loop := s.loop(llm, model, body.SystemPrompt, nil)
	msg, usage, runErr := loop.Run(r.Context(), body.SessionID, body.Input)
	s.metrics.Record(cfg.ID, usage, runErr)
	if runErr != nil && !errors.Is(runErr, agent.ErrMaxIterations) {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": runErr.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id": body.SessionID,
		"content":    msg.Content,
		"reasoning":  msg.Reasoning,
		"usage":      usage,
		"cost_usd":   s.costOf(cfg, model, usage),
	})
}

// chatStream SSE 流式对话
//
// 断连即取消：r.Context() 取消后循环内 LLM 调用与工具执行一并中断
func (s *Server) chatStream(w http.ResponseWriter, r *http.Request, cfg *provider.ProviderConfig, llm core.LLM, model string, body ChatBody) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// 客户端断连时 r.Context() 取消；写入失败也要主动取消，避免循环空转
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	writeSSE := func(event string, data any) bool {
		payload, err := json.Marshal(data)
		if err != nil {
			return false
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
			cancel()
			return false
		}
		flusher.Flush()
		return true
	}

	onEvent := func(e agent.LoopEvent) {
		switch e.Type {
		case agent.EventDeltaText:
			if !writeSSE("text", map[string]string{"delta": e.Text}) {
				return
			}
		case agent.EventDeltaReasoning:
			if !writeSSE("reasoning", map[string]string{"delta": e.Reasoning}) {
				return
			}
		case agent.EventToolCall:
			if !writeSSE("tool_call", map[string]string{"id": e.Call.ID, "name": e.Call.Name, "arguments": e.Call.Arguments}) {
				return
			}
		case agent.EventToolResult:
			if !writeSSE("tool_result", map[string]any{"id": e.Call.ID, "name": e.Call.Name, "error": errString(e.Err)}) {
				return
			}
		case agent.EventError:
			if !writeSSE("error", map[string]string{"message": errString(e.Err)}) {
				return
			}
		}
	}

	loop := s.loop(llm, model, body.SystemPrompt, onEvent)
	msg, usage, runErr := loop.Run(ctx, body.SessionID, body.Input)
	s.metrics.Record(cfg.ID, usage, runErr)

	status := "done"
	if runErr != nil {
		status = "error"
	}
	writeSSE(status, map[string]any{
		"content":   msg.Content,
		"reasoning": msg.Reasoning,
		"usage":     usage,
		"cost_usd":  s.costOf(cfg, model, usage),
		"error":     errString(runErr),
	})
}

// costOf 按提供商定价估算成本
// returns: 美元成本，模型未配置定价时为 0
func (s *Server) costOf(cfg *provider.ProviderConfig, model string, usage core.Usage) float64 {
	m, ok := cfg.Model(model)
	if !ok {
		return 0
	}
	return m.CostOf(usage)
}

// writeJSON 输出 JSON 响应
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// errString 错误转字符串，nil 返回空串
// returns: 错误信息
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
