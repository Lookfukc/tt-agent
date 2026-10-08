package entry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// ChatBody is the conversation request body.
//
// Exactly one of two modes applies:
//   - Stateful: fill in only session_id + input; history is managed
//     by the server-side memory.
//   - Stateless: fill in messages (full history) + input; the server
//     keeps no state, and the response carries new_messages so the
//     caller can persist them and pass them back verbatim next round.
type ChatBody struct {
	SessionID    string         `json:"session_id"`
	ProviderID   string         `json:"provider_id"`
	Model        string         `json:"model"`
	Input        string         `json:"input"`
	Stream       bool           `json:"stream"`
	SystemPrompt string         `json:"system_prompt"`
	Messages     []core.Message `json:"messages,omitempty"`
}

// handleHealth is the liveness probe.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleMetrics emits a snapshot of runtime metrics.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.metrics.Snapshot())
}

// handleProviders lists registered providers and models.
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

// maxChatBody is the request body size cap.
//
// The endpoint is unauthenticated; without a cap, arbitrarily large
// JSON could be pushed in until OOM.
const maxChatBody = 4 << 20

// handleChat is the conversation entry point; with stream=true it
// returns process events over SSE.
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
	// Stateless mode: messages carries the full history and is
	// mutually exclusive with session_id.
	if len(body.Messages) > 0 {
		if body.SessionID != "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "session_id and messages are mutually exclusive"})
			return
		}
	} else if body.SessionID == "" {
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

	// Stateless non-streaming: no server-side state is kept;
	// new_messages is handed back to the caller.
	if len(body.Messages) > 0 {
		loop := s.statelessLoop(llm, model, body.SystemPrompt, nil)
		res, runErr := loop.RunWithHistory(r.Context(), body.Messages, body.Input)
		s.metrics.Record(cfg.ID, res.Usage, runErr)
		if runErr != nil && !errors.Is(runErr, agent.ErrMaxIterations) {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": runErr.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"mode":         "stateless",
			"content":      res.Message.Content,
			"reasoning":    res.Message.Reasoning,
			"new_messages": res.NewMessages,
			"usage":        res.Usage,
			"cost_usd":     s.costOf(cfg, model, res.Usage),
		})
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

// chatStream is the SSE streaming conversation.
//
// Disconnect cancels: once r.Context() is canceled, both the LLM
// calls and tool executions inside the loop are aborted.
func (s *Server) chatStream(w http.ResponseWriter, r *http.Request, cfg *provider.ProviderConfig, llm core.LLM, model string, body ChatBody) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// r.Context() is canceled when the client disconnects; write
	// failures must also cancel proactively so the loop does not spin idle.
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

	// Stateless streaming: process events are identical to the
	// stateful path; the done event additionally carries new_messages.
	if len(body.Messages) > 0 {
		loop := s.statelessLoop(llm, model, body.SystemPrompt, onEvent)
		res, runErr := loop.RunWithHistory(ctx, body.Messages, body.Input)
		s.metrics.Record(cfg.ID, res.Usage, runErr)
		status := "done"
		if runErr != nil {
			status = "error"
		}
		writeSSE(status, map[string]any{
			"content":      res.Message.Content,
			"reasoning":    res.Message.Reasoning,
			"new_messages": res.NewMessages,
			"usage":        res.Usage,
			"cost_usd":     s.costOf(cfg, model, res.Usage),
			"error":        errString(runErr),
		})
		return
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

// costOf estimates cost from the provider's pricing.
// returns: the cost in USD, or 0 if the model has no pricing configured.
func (s *Server) costOf(cfg *provider.ProviderConfig, model string, usage core.Usage) float64 {
	m, ok := cfg.Model(model)
	if !ok {
		return 0
	}
	return m.CostOf(usage)
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// errString converts an error to a string; nil yields an empty string.
// returns: the error message.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
