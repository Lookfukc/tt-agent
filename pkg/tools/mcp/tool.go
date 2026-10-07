package mcp

import (
	"context"
	"encoding/json"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/tools"
)

// mcpTool 将服务器侧工具适配为框架工具
type mcpTool struct {
	client *Client
	spec   core.ToolSpec
}

// Name 工具名
func (t mcpTool) Name() string { return t.spec.Name }

// Description 工具描述
func (t mcpTool) Description() string { return t.spec.Description }

// Parameters 参数 schema
func (t mcpTool) Parameters() json.RawMessage { return t.spec.Parameters }

// Execute 转发执行到 MCP 服务器
func (t mcpTool) Execute(ctx context.Context, args json.RawMessage) (core.ToolResult, error) {
	return t.client.CallTool(ctx, t.spec.Name, args)
}

// Register 握手并把这台服务器的全部工具注册进 Registry
// reg: 目标注册表
// returns: 握手或列取失败时返回错误
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
