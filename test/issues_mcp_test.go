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

// fakeMCPServerExt 可编程假服务器：记录 initialize 次数、可注入服务器请求
type fakeMCPServerExt struct {
	mu        sync.Mutex
	initCalls int
	// ServerRequest 在首个 tools/list 前注入一条服务器主动请求（id 撞 1）
	ServerRequest string
}

// serve 应答并按需注入服务器请求
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
				// 严格服务器：第二次 initialize 直接报协议错误
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
				// 服务器主动请求：官方 SDK 的 id 也从 1 起，与客户端首请求撞号
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

// initCallsCount 线程安全读取握手次数
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

// TestM_F4ConnectIdempotent Register 不得二次 initialize（严格服务器必报错）
func TestM_F4ConnectIdempotent(t *testing.T) {
	ctx := context.Background()
	client, server := newMCPPairExt(t)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// ConnectStdio + Register 组合在旧实现里会再握手一次
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("second Connect must be idempotent: %v", err)
	}
	if server.initCallsCount() != 1 {
		t.Fatalf("M-F4: initialize called %d times, want 1", server.initCallsCount())
	}

	// 走一遍 Register 路径（内部还会调 Connect）
	reg := tools.NewRegistry()
	if err := client.Register(ctx, reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if server.initCallsCount() != 1 {
		t.Fatalf("M-F4: Register re-initialized, calls = %d", server.initCallsCount())
	}
}

// TestM_F6ServerRequestNotConsumed 服务器主动请求撞 id 不得被当响应
func TestM_F6ServerRequestNotConsumed(t *testing.T) {
	ctx := context.Background()
	client, server := newMCPPairExt(t)
	defer client.Close()

	// 注入一条 id=1 的服务器请求（roots/list），与客户端首个请求 id 相同
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

// TestM_F2ReaderExitWakesWaiters 读循环退出后新请求不得永久挂起
func TestM_F2ReaderExitWakesWaiters(t *testing.T) {
	cRead, cWrite := io.Pipe()
	_, sWrite := io.Pipe()
	client := mcp.NewClient(duplex{r: cRead, w: sWrite}, "dead")
	// 服务器什么都不回，直接关闭读端 → 读循环 EOF 退出
	_ = cWrite.Close()
	_ = sWrite.Close()

	// 给读循环退出留时间
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
		// 预期快速失败，而不是挂到 ctx 超时
	case <-time.After(1 * time.Second):
		t.Fatal("M-F2: request hangs forever after reader loop exited")
	}
}
