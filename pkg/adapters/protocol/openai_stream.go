package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// streamChunk 单个 SSE data 行对应的 JSON 增量
type streamChunk struct {
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Content   string            `json:"content"`
			Reasoning string            `json:"reasoning_content"`
			ToolCalls []openAIToolDelta `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *openAIUsage `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// openAIToolDelta 工具调用分片
type openAIToolDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// ChatStream 发送流式对话请求
//
// 首事件前的错误（网络/鉴权/4xx）同步返回；建立连接后的所有
// 内容与错误走事件 channel，语义见 core.StreamEvent
func (p *OpenAIProtocol) ChatStream(ctx context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
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
		// decoder 持有跨 chunk 状态，不可复用
		d := newStreamDecoder(resp)
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
		finish := core.FinishStop
		for {
			chunk, done, err := d.next(ctx)
			if err != nil {
				emit(core.StreamEvent{Type: core.StreamError, Err: err})
				return
			}
			if done {
				// 终止事件只此一处：finish_reason chunk 只记原因不发声，
				// usage 在其后到达，首个 Done 即终止的消费者不能丢掉它
				emit(core.StreamEvent{Type: core.StreamDone, FinishReason: finish})
				return
			}
			if !p.emitChunk(chunk, emit) {
				return
			}
			if len(chunk.Choices) > 0 && chunk.Choices[0].FinishReason != nil {
				finish = normalizeFinish(*chunk.Choices[0].FinishReason)
			}
		}
	}()
	return events, nil
}

// emitChunk 将单个增量解码为零或多个事件，返回 false 表示消费端已取消
func (p *OpenAIProtocol) emitChunk(chunk *streamChunk, emit func(core.StreamEvent) bool) bool {
	if chunk.Error != nil {
		return emit(core.StreamEvent{
			Type: core.StreamError,
			Err: core.NewError(core.ErrProviderInternal, p.providerID,
				fmt.Errorf("stream error: %s", chunk.Error.Message)),
		})
	}
	if chunk.Usage != nil {
		if !emit(core.StreamEvent{Type: core.StreamUsage, Usage: chunk.Usage.toCore()}) {
			return false
		}
	}
	for _, choice := range chunk.Choices {
		if choice.Delta.Reasoning != "" {
			if !emit(core.StreamEvent{Type: core.StreamDeltaReasoning, Reasoning: choice.Delta.Reasoning}) {
				return false
			}
		}
		if choice.Delta.Content != "" {
			if !emit(core.StreamEvent{Type: core.StreamDeltaText, Text: choice.Delta.Content}) {
				return false
			}
		}
		for _, tc := range choice.Delta.ToolCalls {
			if !emit(core.StreamEvent{Type: core.StreamDeltaToolCall, ToolCallDelta: core.ToolCallDelta{
				Index: tc.Index, ID: tc.ID, Name: tc.Function.Name, ArgsPart: tc.Function.Arguments,
			}}) {
				return false
			}
		}
	}
	return true
}

// streamDecoder 有状态 SSE 解码器
//
// SSE 事件可能跨多次 TCP read 到达，tool_calls 分片需要按
// index 累积，因此解码必须持有 per-request 状态
type streamDecoder struct {
	reader *sseReader
}

// newStreamDecoder 构造解码器并启用长行缓冲
func newStreamDecoder(resp *http.Response) *streamDecoder {
	return &streamDecoder{reader: newSSEReader(resp, false)}
}

// next 读取下一个增量
// ctx: 用于取消时中断阻塞读
// returns: 解码后的增量；done 为 true 表示流正常结束；err 非 nil 时流已损坏
func (d *streamDecoder) next(ctx context.Context) (*streamChunk, bool, error) {
	for {
		data, done, err := d.reader.next(ctx)
		if err != nil || done {
			return nil, done, err
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return nil, false, core.NewError(core.ErrProviderInternal, "",
				fmt.Errorf("decode stream chunk: %w", err))
		}
		return &chunk, false, nil
	}
}
