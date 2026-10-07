package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// anthropicStreamEvent 流事件载荷
//
// delta 字段在不同事件类型下结构不同（增量 vs 终止原因），
// 合并为一个宽松结构按需取值
type anthropicStreamEvent struct {
	Type string `json:"type"`

	// message_start 的初始 usage
	Message struct {
		Usage anthropicUsage `json:"usage"`
	} `json:"message"`

	// content_block_start 的块元信息
	Index        int            `json:"index"`
	ContentBlock anthropicBlock `json:"content_block"`

	// content_block_delta 与 message_delta 共用的 delta 载荷
	Delta anthropicDelta `json:"delta"`

	// message_delta 的累计输出用量
	Usage anthropicUsage `json:"usage"`

	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// anthropicDelta delta 载荷的并集结构
type anthropicDelta struct {
	Type string `json:"type"`
	Text string `json:"text"`
	// Thinking thinking_delta 内容
	Thinking string `json:"thinking"`
	// PartialJSON input_json_delta 的参数片段
	PartialJSON string `json:"partial_json"`
	// StopReason message_delta 的终止原因
	StopReason string `json:"stop_reason"`
}

// ChatStream 发送流式对话请求
//
// 首事件前的错误同步返回；建立连接后错误走 StreamError 事件
func (p *AnthropicProtocol) ChatStream(ctx context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	body, err := p.buildBody(req, true)
	if err != nil {
		return nil, err
	}
	resp, err := p.post(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return nil, p.httpError(resp.StatusCode, resp.Header.Get("Retry-After"), raw)
	}

	events := make(chan core.StreamEvent, 16)
	go func() {
		defer close(events)
		// 泄漏防线：流读尽后必须关闭响应体，否则每次调用漏一个连接
		defer resp.Body.Close()
		d := &anthropicStreamDecoder{reader: newSSEReader(resp, false)}
		emit := func(e core.StreamEvent) bool {
			select {
			case events <- e:
				return true
			case <-ctx.Done():
				// 契约：取消也必须发终止错误事件，静默关闭会让
				// 半截内容被当成完整回答返回
				select {
				case events <- core.StreamEvent{
					Type: core.StreamError,
					Err:  core.NewError(core.ErrCanceled, p.providerID, ctx.Err()),
				}:
				default:
				}
				return false
			}
		}
		if !emit(core.StreamEvent{Type: core.StreamStart}) {
			return
		}
		for {
			ev, done, err := d.next(ctx)
			if err != nil {
				emit(core.StreamEvent{Type: core.StreamError, Err: err})
				return
			}
			if done {
				emit(core.StreamEvent{Type: core.StreamDone, FinishReason: d.finish})
				return
			}
			if !p.emitEvent(ev, emit) {
				return
			}
		}
	}()
	return events, nil
}

// emitEvent 转换单个流事件，返回 false 表示消费端已取消
func (p *AnthropicProtocol) emitEvent(ev *anthropicStreamEvent, emit func(core.StreamEvent) bool) bool {
	switch ev.Type {
	case "error":
		return emit(core.StreamEvent{
			Type: core.StreamError,
			Err:  core.NewError(core.ErrProviderInternal, p.providerID, fmt.Errorf("stream error: %s", ev.Error.Message)),
		})
	case "message_start":
		return emit(core.StreamEvent{
			Type:  core.StreamUsage,
			Usage: core.Usage{InputTokens: ev.Message.Usage.InputTokens},
		})
	case "message_delta":
		// 终止原因已在 decoder 记录，此处只透传累计输出用量
		return emit(core.StreamEvent{
			Type:  core.StreamUsage,
			Usage: core.Usage{OutputTokens: ev.Usage.OutputTokens},
		})
	case "content_block_start":
		if ev.ContentBlock.Type == "tool_use" {
			// 块起点携带完整 id/name，参数随后以 input_json_delta 到达
			return emit(core.StreamEvent{
				Type: core.StreamDeltaToolCall,
				ToolCallDelta: core.ToolCallDelta{
					Index: ev.Index, ID: ev.ContentBlock.ID, Name: ev.ContentBlock.Name,
				},
			})
		}
		return true
	case "content_block_delta":
		switch ev.Delta.Type {
		case "text_delta":
			return emit(core.StreamEvent{Type: core.StreamDeltaText, Text: ev.Delta.Text})
		case "thinking_delta":
			return emit(core.StreamEvent{Type: core.StreamDeltaReasoning, Reasoning: ev.Delta.Thinking})
		case "input_json_delta":
			return emit(core.StreamEvent{
				Type:          core.StreamDeltaToolCall,
				ToolCallDelta: core.ToolCallDelta{Index: ev.Index, ArgsPart: ev.Delta.PartialJSON},
			})
		}
		return true
	}
	return true
}

// anthropicStreamDecoder Anthropic 流解码器
type anthropicStreamDecoder struct {
	reader *sseReader
	// finish message_delta 记录的终止原因，随 Done 事件发出
	finish core.FinishReason
}

// next 读取下一个事件，message_delta 顺带记录终止原因
// returns: 解码后的事件；done 为 true 表示 message_stop
func (d *anthropicStreamDecoder) next(ctx context.Context) (*anthropicStreamEvent, bool, error) {
	for {
		data, _, err := d.reader.next(ctx)
		if err != nil {
			return nil, false, err
		}
		var ev anthropicStreamEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return nil, false, core.NewError(core.ErrProviderInternal, "",
				fmt.Errorf("decode stream event: %w", err))
		}
		switch ev.Type {
		case "message_stop":
			return nil, true, nil
		case "message_delta":
			if ev.Delta.StopReason != "" {
				d.finish = anthropicFinish(ev.Delta.StopReason)
			}
		}
		return &ev, false, nil
	}
}
