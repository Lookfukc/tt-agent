// Package mcp 提供外部 MCP 工具服务器的接入能力
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// protocolVersion 握手版本
const protocolVersion = "2024-11-05"

// rpcError JSON-RPC 错误对象
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error 实现 error 接口
func (e *rpcError) Error() string {
	return fmt.Sprintf("rpc %d: %s", e.Code, e.Message)
}

// rpcResponse JSON-RPC 响应帧
type rpcResponse struct {
	ID     int64           `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
	// Method 非空表示这是服务器主动请求而非响应，派发层据此丢弃
	Method string `json:"method,omitempty"`
}

// rpcRequest JSON-RPC 请求帧
type rpcRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      int64          `json:"id,omitempty"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
}

// transport JSON-RPC 传输抽象
//
// stdio 是长连接分帧，HTTP 是逐请求往返，两者在此接口下
// 对 Client 透明；send 必须返回与 req.ID 对应的响应
type transport interface {
	send(ctx context.Context, req rpcRequest) (*rpcResponse, error)
	notify(ctx context.Context, req rpcRequest) error
	close() error
}

// Client MCP 服务器客户端
type Client struct {
	name   string
	tr     transport
	nextID atomic.Int64
	initMu sync.Mutex
	// connected initialize 只允许握手一次，重复握手严格服务器直接报错
	connected bool
}

// NewClient 构造基于长连接分帧传输的客户端
// rw: 按行分帧的双向传输
// name: 服务器名，日志定位用
// returns: 已就绪的客户端，握手前不可调用工具
func NewClient(rw interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
}, name string) *Client {
	return &Client{name: name, tr: newFramedTransport(rw)}
}

// Connect 完成 initialize 握手，重复调用幂等直接返回
// ctx: 握手超时控制
// returns: 握手失败（协议不符/服务器不可用）时返回错误
func (c *Client) Connect(ctx context.Context) error {
	c.initMu.Lock()
	defer c.initMu.Unlock()
	if c.connected {
		return nil
	}
	var result struct {
		ProtocolVersion string `json:"protocolVersion"`
		ServerInfo      struct {
			Name string `json:"name"`
		} `json:"serverInfo"`
	}
	raw, err := c.request(ctx, "initialize", map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "send-agent", "version": "0.1"},
	})
	if err != nil {
		return fmt.Errorf("mcp %s initialize: %w", c.name, err)
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("mcp %s bad initialize result: %w", c.name, err)
	}
	if result.ProtocolVersion == "" {
		return fmt.Errorf("mcp %s: no protocolVersion in initialize result", c.name)
	}
	if err := c.notify(ctx, "notifications/initialized", nil); err != nil {
		return fmt.Errorf("mcp %s initialized notify: %w", c.name, err)
	}
	c.connected = true
	return nil
}

// ListTools 拉取服务器声明的工具
// returns: 工具定义列表
func (c *Client) ListTools(ctx context.Context) ([]core.ToolSpec, error) {
	raw, err := c.request(ctx, "tools/list", nil)
	if err != nil {
		return nil, fmt.Errorf("mcp %s tools/list: %w", c.name, err)
	}
	var result struct {
		Tools []struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			InputSchema json.RawMessage `json:"inputSchema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("mcp %s bad tools/list result: %w", c.name, err)
	}
	specs := make([]core.ToolSpec, 0, len(result.Tools))
	for _, t := range result.Tools {
		specs = append(specs, core.ToolSpec{
			Name: t.Name, Description: t.Description, Parameters: t.InputSchema,
		})
	}
	return specs, nil
}

// CallTool 调用服务器上的工具
// name: 工具名
// args: 参数 JSON，必须是对象
// returns: 文本与图片聚合的工具结果；服务器报 isError 时返回错误
func (c *Client) CallTool(ctx context.Context, name string, args json.RawMessage) (core.ToolResult, error) {
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	raw, err := c.request(ctx, "tools/call", map[string]any{
		"name":      name,
		"arguments": json.RawMessage(args),
	})
	if err != nil {
		return core.ToolResult{}, fmt.Errorf("mcp %s call %s: %w", c.name, name, err)
	}

	var result struct {
		Content []struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			Data     string `json:"data"`
			MimeType string `json:"mimeType"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return core.ToolResult{}, fmt.Errorf("mcp %s bad call result: %w", c.name, err)
	}

	out := core.ToolResult{}
	for _, item := range result.Content {
		switch item.Type {
		case "text":
			if out.Text != "" {
				out.Text += "\n"
			}
			out.Text += item.Text
		case "image":
			out.Images = append(out.Images, item.Data)
		}
	}
	if result.IsError {
		return out, fmt.Errorf("tool %s failed: %s", name, out.Text)
	}
	return out, nil
}

// Close 关闭传输
func (c *Client) Close() error { return c.tr.close() }

// request 发送请求并等待响应
// returns: result 原文；RPC 层或传输层错误
func (c *Client) request(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
	req := rpcRequest{JSONRPC: "2.0", ID: c.nextID.Add(1), Method: method, Params: params}
	resp, err := c.tr.send(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response for %s", method)
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Result, nil
}

// notify 发送通知帧
//
// 必须携带调用方 ctx：服务器停读时写出可能阻塞，
// Background 会让握手卡满传输层超时而无视调用方取消
func (c *Client) notify(ctx context.Context, method string, params map[string]any) error {
	return c.tr.notify(ctx, rpcRequest{
		JSONRPC: "2.0", Method: method, Params: params,
	})
}
