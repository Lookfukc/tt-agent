// Package core 提供多平台 LLM Agent 框架的统一类型与核心契约
package core

import (
	"encoding/json"
	"strings"
)

// Role 消息角色
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ToolCall 模型发起的一次工具调用
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // 原始 JSON 字符串，保持各平台原样透传
}

// UnmarshalArguments 将 Arguments 解析为任意 JSON 值
// returns: 解析后的值，Arguments 为空或非法时返回 nil
func (t ToolCall) UnmarshalArguments() any {
	if t.Arguments == "" {
		return nil
	}
	var v any
	if err := json.Unmarshal([]byte(t.Arguments), &v); err != nil {
		return nil
	}
	return v
}

// ContentPart 多模态内容分片
type ContentPart struct {
	// Type 分片类型：text 或 image
	Type string
	// Text 文本内容，Type 为 text 时有效
	Text string
	// ImageURL 图片地址：http(s) URL 或 data:image/png;base64,xxx 形式的 Data URI
	ImageURL string
}

// ImageData 解析 Data URI 形式的图片
// returns: MIME 类型、base64 数据、是否为合法的 base64 Data URI
func (p ContentPart) ImageData() (mimeType, data string, ok bool) {
	const prefix = "data:"
	if !strings.HasPrefix(p.ImageURL, prefix) {
		return "", "", false
	}
	body := p.ImageURL[len(prefix):]
	meta, payload, found := strings.Cut(body, ",")
	if !found {
		return "", "", false
	}
	// 非 base64 的 Data URI（如 data:text/plain,abc）不是图片载荷，
	// 误报 ok 会把明文塞进 image 块被 API 拒绝
	if !strings.HasSuffix(meta, ";base64") {
		return "", "", false
	}
	mimeType = strings.TrimSuffix(meta, ";base64")
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	return mimeType, payload, true
}

// Message 统一内部消息格式，所有协议适配器负责与其互转
type Message struct {
	Role    Role   `json:"role"`
	Content string `json:"content"`

	// ContentParts 多模态分片，非空时适配器用其替代 Content 构建请求
	ContentParts []ContentPart `json:"content_parts,omitempty"`

	// ToolCalls 仅 assistant 消息携带，表示模型请求的工具调用
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`

	// ToolCallID 仅 tool 角色消息携带，标识本次结果回应哪个调用
	ToolCallID string `json:"tool_call_id,omitempty"`

	// Reasoning 思维链内容，仅用于展示与记账，回传 API 时丢弃
	Reasoning string `json:"reasoning,omitempty"`

	// FinishReason 仅流式聚合产物携带，assistant 历史消息回传时忽略
	FinishReason FinishReason `json:"finish_reason,omitempty"`
}

// UserImage 便捷构造带图 user 消息
// text: 文本说明
// imageURL: 图片 URL 或 Data URI
// returns: 组装好的消息
func UserImage(text, imageURL string) Message {
	return Message{
		Role: RoleUser,
		ContentParts: []ContentPart{
			{Type: "text", Text: text},
			{Type: "image", ImageURL: imageURL},
		},
	}
}

// Text 便捷构造 user 消息
func Text(content string) Message {
	return Message{Role: RoleUser, Content: content}
}

// ToolSpec 暴露给模型的工具定义
type ToolSpec struct {
	Name        string
	Description string
	Parameters  json.RawMessage // JSON Schema
}

// ThinkingConfig 思考模式开关
type ThinkingConfig struct {
	Enabled bool
	// BudgetTokens 思考 token 上限，0 表示由模型自定
	BudgetTokens int64
}

// ResponseFormat 结构化输出约束
type ResponseFormat struct {
	// Name schema 名称，部分协议要求提供
	Name string
	// Schema JSON Schema 定义
	Schema json.RawMessage
}

// ChatRequest 一次对话请求的统一表示
type ChatRequest struct {
	Model       string
	Messages    []Message
	Tools       []ToolSpec
	Temperature *float64
	MaxTokens   int64
	Thinking    *ThinkingConfig

	// ResponseFormat 非空时约束模型输出为符合 Schema 的 JSON
	ResponseFormat *ResponseFormat

	// Extra 提供商特有字段的透传通道，由协议适配器合并进请求体
	Extra map[string]any
}

// FinishReason 生成终止原因
type FinishReason string

const (
	FinishStop          FinishReason = "stop"
	FinishToolCalls     FinishReason = "tool_calls"
	FinishLength        FinishReason = "length"
	FinishContentFilter FinishReason = "content_filter"
)

// Usage token 用量记账
type Usage struct {
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
}

// Total 总输出 token 含思考部分
// returns: OutputTokens 与 ReasoningTokens 之和
func (u Usage) Total() int64 {
	return u.OutputTokens + u.ReasoningTokens
}

// Add 累加另一份用量
func (u *Usage) Add(other Usage) {
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.ReasoningTokens += other.ReasoningTokens
}

// ChatResponse 一次对话的完整响应
type ChatResponse struct {
	ID           string
	Model        string
	Content      string
	Reasoning    string
	ToolCalls    []ToolCall
	FinishReason FinishReason
	Usage        Usage
}

// StreamEventType 流事件类型
type StreamEventType int

const (
	// StreamStart 流开始，保证是首个事件
	StreamStart StreamEventType = iota

	// StreamDeltaText 文本增量
	StreamDeltaText

	// StreamDeltaReasoning 思维链增量
	StreamDeltaReasoning

	// StreamDeltaToolCall 工具调用参数增量，同一次调用按 Index 聚积
	StreamDeltaToolCall

	// StreamUsage 部分提供商在流尾附带用量
	StreamUsage

	// StreamDone 正常终止事件，之后 channel 关闭
	StreamDone

	// StreamError 终止性错误事件，之后 channel 关闭
	StreamError
)

// ToolCallDelta 工具调用增量的分片
type ToolCallDelta struct {
	Index    int
	ID       string // 仅首片携带
	Name     string // 仅首片携带
	ArgsPart string // Arguments 的追加片段
}

// StreamEvent 流式传输事件
//
// channel 语义：生产者负责 close；错误只通过 StreamError 事件传递，不单独开 error channel；
// ctx 取消时发送 StreamError 后 close
type StreamEvent struct {
	Type          StreamEventType
	Text          string
	Reasoning     string
	ToolCallDelta ToolCallDelta
	Usage         Usage
	FinishReason  FinishReason
	Err           error
}
