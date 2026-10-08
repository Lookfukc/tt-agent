package test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/tools"
	"github.com/Lookfukc/tt-agent/pkg/tools/mcp"
)

// duplex joins two one-way pipes into a bidirectional transport.
type duplex struct {
	r io.ReadCloser
	w io.WriteCloser
}

// Read reads from the underlying reader.
func (d duplex) Read(p []byte) (int, error) { return d.r.Read(p) }

// Write writes to the underlying writer.
func (d duplex) Write(p []byte) (int, error) { return d.w.Write(p) }

// Close closes both ends.
func (d duplex) Close() error {
	d.r.Close()
	return d.w.Close()
}

// fakeMCPServer is an in-memory MCP server: the echo tool echoes, the fail tool errors.
type fakeMCPServer struct {
	mu       sync.Mutex
	initSeen bool
}

// serve answers JSON-RPC over the duplex transport.
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
			// notification
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

// fakeRPCError is the fake server's RPC error.
type fakeRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// initialized reads the handshake notification state thread-safely.
// returns: true if the initialized notification has been received.
func (s *fakeMCPServer) initialized() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initSeen
}

// dispatch routes by method.
// returns: a result or an RPC error.
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

// newMCPPair sets up an in-memory connection between the client and the fake server.
// returns: the client and the server instance.
func newMCPPair(t *testing.T) (*mcp.Client, *fakeMCPServer) {
	t.Helper()
	cRead, cWrite := io.Pipe() // server → client
	sRead, sWrite := io.Pipe() // client → server
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
	// The notify is a fire-and-forget write; the server processes it with scheduling delay, so poll to confirm.
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
