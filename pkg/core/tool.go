package core

import (
	"context"
	"encoding/json"
)

// ToolResult 工具执行结果，支持多形态以适配 vision 与结构化场景
type ToolResult struct {
	// Text 文本结果，直接作为 tool 消息内容回传
	Text string

	// Data 结构化结果，为空时不回传；适配器负责序列化
	Data any

	// Images 图片结果（URL 或 base64），供 vision 模型消费
	// TODO: Message 支持多模态 content parts 后接入
	Images []string
}

// Render 将结果渲染为回传给模型的字符串
// returns: Data 存在时返回其 JSON 序列化，否则返回 Text
func (r ToolResult) Render() string {
	if r.Data != nil {
		if b, err := json.Marshal(r.Data); err == nil {
			return string(b)
		}
	}
	return r.Text
}

// Tool 工具抽象，框架内所有工具实现此接口
type Tool interface {
	// Name 工具唯一标识
	Name() string

	// Description 供模型理解工具用途的自然语言描述
	Description() string

	// Parameters 参数的 JSON Schema
	Parameters() json.RawMessage

	// Execute 执行工具
	// ctx: 承载超时与取消，长任务必须响应
	// args: 模型给出的参数 JSON，格式合法由实现方校验
	// returns: 执行结果与非 nil 错误；错误会作为工具失败信息回传模型
	Execute(ctx context.Context, args json.RawMessage) (ToolResult, error)
}

// ToolFunc 函数式工具，便于将普通函数快速注册为工具
type ToolFunc func(ctx context.Context, args json.RawMessage) (ToolResult, error)

// Execute 实现 Tool 接口
func (f ToolFunc) Execute(ctx context.Context, args json.RawMessage) (ToolResult, error) {
	return f(ctx, args)
}
