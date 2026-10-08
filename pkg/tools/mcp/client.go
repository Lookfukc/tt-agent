// Package mcp provides integration with external MCP tool servers.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// protocolVersion is the handshake version.
const protocolVersion = "2024-11-05"

// rpcError is the JSON-RPC error object.
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error implements the error interface.
func (e *rpcError) Error() string {
	return fmt.Sprintf("rpc %d: %s", e.Code, e.Message)
}

// rpcResponse is a JSON-RPC response frame.
type rpcResponse struct {
	ID     int64           `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
	// Method being non-empty marks this as a server-initiated request
	// rather than a response; the dispatch layer drops it accordingly
	Method string `json:"method,omitempty"`
}

// rpcRequest is a JSON-RPC request frame.
type rpcRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      int64          `json:"id,omitempty"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
}

// transport is the JSON-RPC transport abstraction.
//
// stdio is a long-lived framed connection while HTTP is per-request
// round-trips; both are transparent to Client under this interface.
// send must return the response corresponding to req.ID.
type transport interface {
	send(ctx context.Context, req rpcRequest) (*rpcResponse, error)
	notify(ctx context.Context, req rpcRequest) error
	close() error
}

// Client is a client for an MCP server.
type Client struct {
	name   string
	tr     transport
	nextID atomic.Int64
	initMu sync.Mutex
	// connected guards initialize to a single handshake; strict servers
	// error out outright on repeated handshakes
	connected bool
}

// NewClient constructs a client over a long-lived framed transport.
// rw: a line-framed bidirectional transport
// name: server name, for log attribution
// returns: a ready client; tools must not be called before the handshake
func NewClient(rw interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
}, name string) *Client {
	return &Client{name: name, tr: newFramedTransport(rw)}
}

// Connect performs the initialize handshake; repeated calls are idempotent
// and return immediately.
// ctx: handshake timeout control
// returns: an error if the handshake fails (protocol mismatch / server unavailable)
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
		"clientInfo":      map[string]any{"name": "tt-agent", "version": "0.1"},
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

// ListTools fetches the tools declared by the server.
// returns: the list of tool definitions
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

// CallTool invokes a tool on the server.
// name: the tool name
// args: argument JSON, must be an object
// returns: the tool result aggregating text and images; an error if the
// server reports isError
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

// Close closes the transport.
func (c *Client) Close() error { return c.tr.close() }

// request sends a request and waits for the response.
// returns: the raw result; an RPC-layer or transport-layer error
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

// notify sends a notification frame.
//
// It must carry the caller's ctx: writes can block when the server stops
// reading, and Background would leave the handshake stuck until the
// transport timeout, ignoring caller cancellation.
func (c *Client) notify(ctx context.Context, method string, params map[string]any) error {
	return c.tr.notify(ctx, rpcRequest{
		JSONRPC: "2.0", Method: method, Params: params,
	})
}
