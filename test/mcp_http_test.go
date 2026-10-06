package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/tools/mcp"
)

// TestMCPHTTPEchoSession Streamable HTTP：JSON 与 SSE 两种响应、会话头续用
func TestMCPHTTPEchoSession(t *testing.T) {
	var mu sync.Mutex
	var sessions = map[string]bool{}
	var lastSessionHeader string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     int64          `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		mu.Lock()
		lastSessionHeader = r.Header.Get("Mcp-Session-Id")
		mu.Unlock()

		respond := func(result any, asSSE bool) {
			payload := map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}
			if asSSE {
				w.Header().Set("Content-Type", "text/event-stream")
				raw, _ := json.Marshal(payload)
				fmt.Fprintf(w, "event: message\ndata: %s\n\n", raw)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			// initialize 分配会话
			if req.Method == "initialize" {
				w.Header().Set("Mcp-Session-Id", "sess-1")
			}
			_ = json.NewEncoder(w).Encode(payload)
		}

		switch req.Method {
		case "initialize":
			respond(map[string]any{
				"protocolVersion": "2024-11-05",
				"serverInfo":      map[string]any{"name": "http-fake"},
			}, false)
			mu.Lock()
			sessions["initialized"] = true
			mu.Unlock()
		case "tools/list":
			// 用 SSE 形式回复，验证两种编码都能解
			respond(map[string]any{"tools": []any{
				map[string]any{
					"name":        "http_echo",
					"description": "HTTP 回显",
					"inputSchema": map[string]any{"type": "object"},
				},
			}}, true)
		case "tools/call":
			respond(map[string]any{"content": []any{
				map[string]any{"type": "text", "text": "http 回显"},
			}}, false)
		default:
			if req.ID == 0 {
				return // 通知：202 即可
			}
			w.WriteHeader(202)
		}
	}))
	defer srv.Close()

	client := mcp.ConnectHTTP("http-fake", srv.URL, "")
	if err := client.Connect(t.Context()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	specs, err := client.ListTools(t.Context())
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "http_echo" {
		t.Fatalf("specs = %+v", specs)
	}

	res, err := client.CallTool(t.Context(), "http_echo", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.Text != "http 回显" {
		t.Errorf("text = %q", res.Text)
	}

	// initialize 之后所有请求应带上会话头
	mu.Lock()
	header := lastSessionHeader
	mu.Unlock()
	if header != "sess-1" {
		t.Errorf("Mcp-Session-Id header = %q, want sess-1", header)
	}
}

// TestMCPHTTPSSEInitializeSession M-F5 回归：initialize 以 SSE 应答且
// 经 HTTP 响应头分配会话时，会话必须被捕获并续用到后续请求
func TestMCPHTTPSSEInitializeSession(t *testing.T) {
	var mu sync.Mutex
	var lastSessionHeader string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     int64  `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		mu.Lock()
		lastSessionHeader = r.Header.Get("Mcp-Session-Id")
		mu.Unlock()

		switch req.Method {
		case "initialize":
			// SSE 编码 + 响应头分配会话：会话捕获必须在编码格式分支之前完成
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Mcp-Session-Id", "sse-sess-1")
			raw, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": req.ID,
				"result": map[string]any{
					"protocolVersion": "2024-11-05",
					"serverInfo":      map[string]any{"name": "http-sse-fake"},
				},
			})
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", raw)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0", "id": req.ID,
				"result": map[string]any{"tools": []any{}},
			})
		default:
			// 通知（notifications/initialized）：202 即可
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer srv.Close()

	client := mcp.ConnectHTTP("http-sse-fake", srv.URL, "")
	if err := client.Connect(t.Context()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// 追加一次调用，其请求头必须带 initialize（SSE 应答）分配的会话
	if _, err := client.ListTools(t.Context()); err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	mu.Lock()
	header := lastSessionHeader
	mu.Unlock()
	if header != "sse-sess-1" {
		t.Errorf("Mcp-Session-Id header = %q, want sse-sess-1", header)
	}
}

func TestMCPHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, "internal")
	}))
	defer srv.Close()

	client := mcp.ConnectHTTP("bad", srv.URL, "")
	if err := client.Connect(t.Context()); err == nil {
		t.Fatal("want error on http 500")
	} else if !strings.Contains(err.Error(), "http 500") {
		t.Errorf("err = %v", err)
	}
}
