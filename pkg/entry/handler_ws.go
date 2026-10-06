package entry

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/Lookfukc/send-agent/pkg/agent"
)

// handleChatWS WebSocket 对话入口
//
// 每条入站文本帧是一次独立对话请求，事件以 JSON 文本帧回推；
// 连接级取消：读 goroutine 检测断连即 cancel 会话 ctx，
// 进行中的循环与工具执行随之中断，不为断连客户端继续烧 token
func (s *Server) handleChatWS(w http.ResponseWriter, r *http.Request) {
	if !wsHeaderContains(r.Header.Get("Connection"), "upgrade") ||
		!strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "websocket upgrade required"})
		return
	}
	// 浏览器跨站 WS 不受 CORS 约束：Origin 存在时必须与 Host 同源，
	// 无 Origin 的非浏览器客户端放行
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
	// 劫持后 HTTP 语义结束，读写都走裸连接
	ws, err := wsUpgrade(conn, bufio.NewReader(brw.Reader), r.Header.Get("Sec-WebSocket-Key"))
	if err != nil {
		_ = conn.Close()
		return
	}
	defer ws.Close()
	s.trackWS(ws)
	defer s.untrackWS(ws)

	// 会话 ctx 由读 goroutine 驱动：任何读错误（含断连、协议违规、
	// 读窗口超时）都会取消它，进而取消正在执行的循环
	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()

	// N12 保活：空闲连接周期发 ping，客户端任何帧（含 pong）都会
	// 刷新 2 分钟读窗口，活跃连接不再被窗口误杀；死连接由窗口兜底
	// 掐断；connCtx 取消时 goroutine 自行退出，不泄漏
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
				// 分片累计超限按协议回 1009，未掩码帧回 1002
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
