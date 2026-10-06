package core

import "context"

// LLM 对话能力抽象，协议适配器实现此接口
//
// 能力差异（vision/structured output 等）属于 Model 级信息，
// 由 provider registry 的 ModelConfig 描述，不放在接口上
type LLM interface {
	// Chat 发送单次对话请求并等待完整响应
	Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error)

	// ChatStream 发送流式对话请求
	//
	// channel 语义：返回的 channel 由实现方 close；错误仅通过 StreamError
	// 事件传递；ctx 取消时实现方发送 StreamError 后 close。首个事件之前
	// 发生的错误通过 error 返回值给出，此时尚未创建 channel
	ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error)
}
