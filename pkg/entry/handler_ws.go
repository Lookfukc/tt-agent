package entry

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/Lookfukc/tt-agent/pkg/agent"
)

// handleChatWS is the WebSocket conversation entry point.
//
// Each inbound text frame is an independent conversation request, and
// events are pushed back as JSON text frames;
// connection-level cancellation: the read goroutine cancels the session
// ctx as soon as it detects a disconnect, aborting any in-flight loop
// and tool execution, so no tokens are burned for a disconnected client.
func (s *Server) handleChatWS(w http.ResponseWriter, r *http.Request) {
	if !wsHeaderContains(r.Header.Get("Connection"), "upgrade") ||
		!strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "websocket upgrade required"})
		return
	}
	// Browser cross-site WS is not subject to CORS: when Origin is
	// present it must be same-origin with Host;
	// non-browser clients without an Origin header are allowed through.
	if origin := r.Header.Get("Origin"); origin != "" {
		if u, err := url.Parse(origin); err != nil || !strings.EqualFold(u.Host, r.Host) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "origin not allowed"})
			return
		}
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "hijack unsupported"})
		return
	}
	conn, brw, err := hijacker.Hijack()
	if err != nil {
		return
	}
	// After the hijack, HTTP semantics are over; reads and writes
	// both go over the raw connection.
	ws, err := wsUpgrade(conn, bufio.NewReader(brw.Reader), r.Header.Get("Sec-WebSocket-Key"))
	if err != nil {
		_ = conn.Close()
		return
	}
	defer ws.Close()
	s.trackWS(ws)
	defer s.untrackWS(ws)

	// The session ctx is driven by the read goroutine: any read error
	// (including disconnect, protocol violation, or read-window
	// timeout) cancels it, which in turn cancels the running loop.
	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()

	// N12 keepalive: idle connections send pings periodically; any
	// client frame (including pong) refreshes the 2-minute read
	// window, so active connections are never killed by the window
	// in error; dead connections are cut off by the window as a
	// fallback; when connCtx is canceled the goroutine exits by
	// itself, so nothing leaks.
	go ws.keepalive(connCtx, cancelConn)

	type inbound struct {
		op      int
		payload []byte
	}
	messages := make(chan inbound, 4)
	go func() {
		defer close(messages)
		for {
			op, payload, err := ws.ReadMessage()
			if err != nil {
				// Fragment accumulation over the limit gets a 1009
				// reply per protocol; unmasked frames get 1002.
				if errors.Is(err, wsErrMessageTooBig) {
					_ = ws.WriteCloseStatus(1009)
				} else if errors.Is(err, wsErrProtocol) {
					_ = ws.WriteCloseStatus(1002)
				}
				cancelConn()
				return
			}
			select {
			case messages <- inbound{op: op, payload: payload}:
			case <-connCtx.Done():
				return
			}
		}
	}()

	for {
		var msg inbound
		select {
		case msg = <-messages:
		case <-connCtx.Done():
			return
		}
		if msg.op != wsOpText {
			continue
		}
		var body ChatBody
		if err := json.Unmarshal(msg.payload, &body); err != nil || body.Input == "" {
			_ = wsWriteJSON(ws, map[string]any{"event": "error", "message": "invalid request body"})
			continue
		}
		if body.SessionID == "" {
			body.SessionID = "default"
		}
		cfg, model, err := s.resolveModel(body.ProviderID, body.Model)
		if err != nil {
			_ = wsWriteJSON(ws, map[string]any{"event": "error", "message": err.Error()})
			continue
		}
		llm, err := s.llmFor(cfg.ID)
		if err != nil {
			_ = wsWriteJSON(ws, map[string]any{"event": "error", "message": err.Error()})
			continue
		}

		runCtx, cancelRun := context.WithCancel(connCtx)
		writeFailed := false
		onEvent := func(e agent.LoopEvent) {
			if writeFailed {
				return
			}
			var envelope any
			switch e.Type {
			case agent.EventDeltaText:
				envelope = map[string]any{"event": "text", "delta": e.Text}
			case agent.EventDeltaReasoning:
				envelope = map[string]any{"event": "reasoning", "delta": e.Reasoning}
			case agent.EventToolCall:
				envelope = map[string]any{"event": "tool_call", "name": e.Call.Name, "arguments": e.Call.Arguments}
			case agent.EventToolResult:
				envelope = map[string]any{"event": "tool_result", "name": e.Call.Name, "error": errString(e.Err)}
			case agent.EventError:
				envelope = map[string]any{"event": "error", "message": errString(e.Err)}
			}
			if envelope == nil {
				return
			}
			if err := wsWriteJSON(ws, envelope); err != nil {
				writeFailed = true
				cancelRun()
			}
		}

		loop := s.loop(llm, model, body.SystemPrompt, onEvent)
		msgOut, usage, runErr := loop.Run(runCtx, body.SessionID, body.Input)
		status := "done"
		if runErr != nil && !errors.Is(runErr, agent.ErrMaxIterations) {
			status = "error"
		}
		s.metrics.Record(cfg.ID, usage, runErr)
		fin := map[string]any{
			"event":    status,
			"content":  msgOut.Content,
			"usage":    usage,
			"cost_usd": s.costOf(cfg, model, usage),
		}
		if runErr != nil {
			fin["error"] = errString(runErr)
		}
		_ = wsWriteJSON(ws, fin)
		cancelRun()

		if writeFailed || connCtx.Err() != nil {
			return
		}
	}
}
