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

// fakeMCPServerExt is a programmable fake server: records initialize count, can inject server-initiated requests
type fakeMCPServerExt struct {
	mu        sync.Mutex
	initCalls int
	// ServerRequest injects one server-initiated request (with id colliding with 1) before the first tools/list
	ServerRequest string
}

// serve answers requests and injects server requests as configured
func (s *fakeMCPServerExt) serve(rw io.ReadWriteCloser) {
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
		if req.Method == "initialize" {
			s.mu.Lock()
			s.initCalls++
			calls := s.initCalls
			s.mu.Unlock()
			if calls > 1 {
				// Strict server: a second initialize immediately returns a protocol error
				_ = encoder.Encode(map[string]any{
					"jsonrpc": "2.0", "id": req.ID,
					"error": map[string]any{"code": -32600, "message": "Server already initialized"},
				})
				continue
			}
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0", "id": req.ID,
				"result": map[string]any{
					"protocolVersion": "2024-11-05",
					"serverInfo":      map[string]any{"name": "strict"},
				},
			})
			continue
		}
		if req.ID == 0 {
			continue
		}
		if req.Method == "tools/list" {
			s.mu.Lock()
			inject := s.ServerRequest
			s.mu.Unlock()
			if inject != "" {
				// Server-initiated request: the official SDK's ids also start at 1, colliding with the client's first request id
				_, _ = rw.Write([]byte(inject + "\n"))
			}
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0", "id": req.ID,
				"result": map[string]any{"tools": []any{
					map[string]any{"name": "real_tool", "description": "真工具", "inputSchema": map[string]any{"type": "object"}},
				}},
			})
			continue
		}
		_ = encoder.Encode(map[string]any{
			"jsonrpc": "2.0", "id": req.ID,
			"result": map[string]any{"content": []any{map[string]any{"type": "text", "text": "ok"}}},
		})
	}
}

// initCallsCount reads the handshake count thread-safely
func (s *fakeMCPServerExt) initCallsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initCalls
}

func newMCPPairExt(t *testing.T) (*mcp.Client, *fakeMCPServerExt) {
	t.Helper()
	cRead, cWrite := io.Pipe()
	sRead, sWrite := io.Pipe()
	server := &fakeMCPServerExt{}
	go server.serve(duplex{r: sRead, w: cWrite})
	client := mcp.NewClient(duplex{r: cRead, w: sWrite}, "strict")
	return client, server
}

// TestM_F4ConnectIdempotent verifies Register must not initialize a second time (a strict server would error out)
func TestM_F4ConnectIdempotent(t *testing.T) {
	ctx := context.Background()
	client, server := newMCPPairExt(t)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// The ConnectStdio + Register combination in the old implementation performed an extra handshake
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("second Connect must be idempotent: %v", err)
	}
	if server.initCallsCount() != 1 {
		t.Fatalf("M-F4: initialize called %d times, want 1", server.initCallsCount())
	}

	// Exercise the Register path too (it also calls Connect internally)
	reg := tools.NewRegistry()
	if err := client.Register(ctx, reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if server.initCallsCount() != 1 {
		t.Fatalf("M-F4: Register re-initialized, calls = %d", server.initCallsCount())
	}
}

// TestM_F6ServerRequestNotConsumed verifies a server-initiated request with a colliding id must not be treated as a response
func TestM_F6ServerRequestNotConsumed(t *testing.T) {
	ctx := context.Background()
	client, server := newMCPPairExt(t)
	defer client.Close()

	// Inject a server request with id=1 (roots/list), identical to the client's first request id
	server.mu.Lock()
	server.ServerRequest = `{"jsonrpc":"2.0","id":1,"method":"roots/list"}`
	server.mu.Unlock()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	specs, err := client.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "real_tool" {
		t.Fatalf("M-F6: server request consumed as response, specs = %+v", specs)
	}
}

// TestM_F2ReaderExitWakesWaiters verifies new requests must not hang forever after the read loop exits
func TestM_F2ReaderExitWakesWaiters(t *testing.T) {
	cRead, cWrite := io.Pipe()
	_, sWrite := io.Pipe()
	client := mcp.NewClient(duplex{r: cRead, w: sWrite}, "dead")
	// The server replies nothing; close the read end directly → the read loop exits on EOF
	_ = cWrite.Close()
	_ = sWrite.Close()

	// Give the read loop time to exit
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.ListTools(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("M-F2: request after reader exit unexpectedly succeeded")
		}
		// Expect a fast failure, not hanging until the ctx timeout
	case <-time.After(1 * time.Second):
		t.Fatal("M-F2: request hangs forever after reader loop exited")
	}
}
