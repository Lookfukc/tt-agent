package test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/send-agent/pkg/tools"
	"github.com/Lookfukc/send-agent/pkg/tools/mcp"
)

// duplex 拼接两条单向管道成双向传输
type duplex struct {
	r io.ReadCloser
	w io.WriteCloser
}

// Read 读
func (d duplex) Read(p []byte) (int, error) { return d.r.Read(p) }

// Write 写
func (d duplex) Write(p []byte) (int, error) { return d.w.Write(p) }

// Close 关闭两端
func (d duplex) Close() error {
	d.r.Close()
	return d.w.Close()
}

// fakeMCPServer 内存中的 MCP 服务器：echo 工具回显，fail 工具报错
type fakeMCPServer struct {
	mu       sync.Mutex
	initSeen bool
}

// serve 在 duplex 上应答 JSON-RPC
func (s *fakeMCPServer) serve(rw io.ReadWriteCloser) {
	scanner := bufio.NewScanner(rw)
	encoder := json.NewEncoder(rw)
	for scanner.Scan() {
		var req struct {
			ID     int64          `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &req) != nil {
			continue
		}
		if req.ID == 0 {
			// 通知
			if req.Method == "notifications/initialized" {
				s.mu.Lock()
				s.initSeen = true
				s.mu.Unlock()
			}
			continue
		}
		result, rpcErr := s.dispatch(req.Method, req.Params)
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		if rpcErr != nil {
			resp["error"] = rpcErr
		} else {
			resp["result"] = result
		}
		_ = encoder.Encode(resp)
	}
}

// fakeRPCError 假服务器的 RPC 错误
type fakeRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// initialized 线程安全地读取握手通知状态
// returns: true 表示已收到 initialized 通知
func (s *fakeMCPServer) initialized() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initSeen
}

// dispatch 按方法路由
// returns: result 或 RPC 错误
func (s *fakeMCPServer) dispatch(method string, params map[string]any) (any, *fakeRPCError) {
	rpcFail := func(msg string) *fakeRPCError {
		return &fakeRPCError{Code: -32000, Message: msg}
	}
	switch method {
	case "initialize":
		return map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "fake"},
		}, nil
	case "tools/list":
		return map[string]any{"tools": []any{
			map[string]any{
				"name":        "echo",
				"description": "回显文本",
				"inputSchema": map[string]any{"type": "object", "properties": map[string]any{
					"text": map[string]any{"type": "string"},
				}},
			},
			map[string]any{
				"name":        "fail",
				"description": "注定失败",
				"inputSchema": map[string]any{"type": "object"},
			},
		}}, nil
	case "tools/call":
		name, _ := params["name"].(string)
		switch name {
		case "echo":
			return map[string]any{"content": []any{
				map[string]any{"type": "text", "text": "回显:hi"},
				map[string]any{"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"},
			}}, nil
		case "fail":
			return map[string]any{
				"content": []any{map[string]any{"type": "text", "text": "内部炸了"}},
				"isError": true,
			}, nil
		default:
			return nil, rpcFail("unknown tool " + name)
		}
	default:
		return nil, rpcFail("unknown method " + method)
	}
}

// newMCPPair 建立客户端与假服务器的内存连接
// returns: 客户端与服务器实例
func newMCPPair(t *testing.T) (*mcp.Client, *fakeMCPServer) {
	t.Helper()
	cRead, cWrite := io.Pipe() // 服务器 → 客户端
	sRead, sWrite := io.Pipe() // 客户端 → 服务器
	server := &fakeMCPServer{}
	go server.serve(duplex{r: sRead, w: cWrite})
	client := mcp.NewClient(duplex{r: cRead, w: sWrite}, "fake")
	return client, server
}

func TestMCPClientLifecycle(t *testing.T) {
	ctx := context.Background()
	client, server := newMCPPair(t)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// notify 是 fire-and-forget 写出，服务器处理存在调度延迟，轮询确认
	deadline := time.Now().Add(2 * time.Second)
	for !server.initialized() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !server.initialized() {
		t.Error("initialized notification not delivered")
	}

	specs, err := client.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(specs) != 2 || specs[0].Name != "echo" || len(specs[0].Parameters) == 0 {
		t.Errorf("specs = %+v", specs)
	}

	res, err := client.CallTool(ctx, "echo", json.RawMessage(`{"text":"hi"}`))
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.Text != "回显:hi" || len(res.Images) != 1 {
		t.Errorf("res = %+v", res)
	}

	if _, err := client.CallTool(ctx, "fail", nil); err == nil {
		t.Error("isError result should surface as error")
	}
	if _, err := client.CallTool(ctx, "nope", nil); err == nil {
		t.Error("unknown tool should fail")
	}
}

func TestMCPRegisterIntoRegistry(t *testing.T) {
	ctx := context.Background()
	client, _ := newMCPPair(t)
	defer client.Close()

	reg := tools.NewRegistry()
	if err := client.Register(ctx, reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if len(reg.Specs()) != 2 {
		t.Fatalf("specs = %d, want 2", len(reg.Specs()))
	}

	tool, err := reg.MustGet("echo")
	if err != nil {
		t.Fatalf("MustGet: %v", err)
	}
	res, err := tool.Execute(ctx, json.RawMessage(`{"text":"hi"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Text != "回显:hi" {
		t.Errorf("text = %q", res.Text)
	}
}
