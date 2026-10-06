package core

import (
	"strings"
	"sync"
)

// StreamAccumulator 聚合流事件为完整消息
//
// 工具调用分片按 Index 累积参数，消费端无需理解分片协议
type StreamAccumulator struct {
	mu        sync.Mutex
	text      strings.Builder
	reasoning strings.Builder
	toolOrder []int
	toolCalls map[int]*ToolCall
	usage     Usage
	finish    FinishReason
}

// NewStreamAccumulator 构造聚合器
// returns: 可用的聚合器实例
func NewStreamAccumulator() *StreamAccumulator {
	return &StreamAccumulator{toolCalls: make(map[int]*ToolCall)}
}

// Feed 喂入一个事件
func (a *StreamAccumulator) Feed(e StreamEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch e.Type {
	case StreamDeltaText:
		a.text.WriteString(e.Text)
	case StreamDeltaReasoning:
		a.reasoning.WriteString(e.Reasoning)
	case StreamDeltaToolCall:
		tc, ok := a.toolCalls[e.ToolCallDelta.Index]
		if !ok {
			tc = &ToolCall{}
			a.toolCalls[e.ToolCallDelta.Index] = tc
			a.toolOrder = append(a.toolOrder, e.ToolCallDelta.Index)
		}
		if e.ToolCallDelta.ID != "" {
			tc.ID = e.ToolCallDelta.ID
		}
		if e.ToolCallDelta.Name != "" {
			tc.Name = e.ToolCallDelta.Name
		}
		tc.Arguments += e.ToolCallDelta.ArgsPart
	case StreamUsage:
		// 字段级合并而非覆盖：Anthropic 分 message_start/message_delta
		// 两段各带一半用量，后者缺失的字段保留前值
		if e.Usage.InputTokens > a.usage.InputTokens {
			a.usage.InputTokens = e.Usage.InputTokens
		}
		if e.Usage.OutputTokens > a.usage.OutputTokens {
			a.usage.OutputTokens = e.Usage.OutputTokens
		}
		if e.Usage.ReasoningTokens > a.usage.ReasoningTokens {
			a.usage.ReasoningTokens = e.Usage.ReasoningTokens
		}
	case StreamDone:
		if e.FinishReason != "" {
			a.finish = e.FinishReason
		}
	}
}

// Message 产出聚合后的 assistant 消息
// returns: 含 Content/Reasoning/ToolCalls 的消息
func (a *StreamAccumulator) Message() Message {
	a.mu.Lock()
	defer a.mu.Unlock()
	msg := Message{
		Role:         RoleAssistant,
		Content:      a.text.String(),
		Reasoning:    a.reasoning.String(),
		FinishReason: a.finish,
	}
	for _, idx := range a.toolOrder {
		if tc := a.toolCalls[idx]; tc != nil {
			msg.ToolCalls = append(msg.ToolCalls, *tc)
		}
	}
	return msg
}

// Usage 产出累计用量
// returns: 最后一次 StreamUsage 事件的用量
func (a *StreamAccumulator) Usage() Usage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.usage
}

// CollectStream 便捷消费：聚合并返回终止错误
// events: 实现方负责 close 的事件流
// returns: 聚合消息、累计用量、终止性错误（StreamError 事件的 Err）
func CollectStream(events <-chan StreamEvent) (Message, Usage, error) {
	acc := NewStreamAccumulator()
	var lastErr error
	for e := range events {
		if e.Type == StreamError {
			lastErr = e.Err
			continue
		}
		acc.Feed(e)
	}
	return acc.Message(), acc.Usage(), lastErr
}
