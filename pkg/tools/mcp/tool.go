package mcp

import (
	"context"
	"encoding/json"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/tools"
)

// mcpTool adapts a server-side tool into a framework tool.
type mcpTool struct {
	client *Client
	spec   core.ToolSpec
}

// Name returns the tool name.
func (t mcpTool) Name() string { return t.spec.Name }

// Description returns the tool description.
func (t mcpTool) Description() string { return t.spec.Description }

// Parameters returns the parameter schema.
func (t mcpTool) Parameters() json.RawMessage { return t.spec.Parameters }

// Execute forwards execution to the MCP server.
func (t mcpTool) Execute(ctx context.Context, args json.RawMessage) (core.ToolResult, error) {
	return t.client.CallTool(ctx, t.spec.Name, args)
}

// Register performs the handshake and registers all tools of this server
// into the Registry.
// reg: the target registry
// returns: an error if the handshake or listing fails
func (c *Client) Register(ctx context.Context, reg *tools.Registry) error {
	if err := c.Connect(ctx); err != nil {
		return err
	}
	specs, err := c.ListTools(ctx)
	if err != nil {
		return err
	}
	for _, spec := range specs {
		reg.Register(mcpTool{client: c, spec: spec})
	}
	return nil
}
