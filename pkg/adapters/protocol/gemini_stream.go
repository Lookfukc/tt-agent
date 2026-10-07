package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// ChatStream 发送流式对话请求
//
// Gemini 的 SSE 每行是一个完整响应片段而非增量，functionCall
// 一次性出现在单个片段中
func (p *GeminiProtocol) ChatStream(ctx context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	body, err := p.buildBody(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.post(ctx, body, ":streamGenerateContent?alt=sse")
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
		// 原生 Gemini 流以连接关闭结束，不认 [DONE] 标记
		reader := newSSEReader(resp, true)
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
		toolIdx := 0
		// nameSeq 同名调用计数：合成 ID 用出现序号消歧，跨 chunk 累计
		nameSeq := map[string]int{}
		for {
			data, done, err := reader.next(ctx)
			if err != nil {
				emit(core.StreamEvent{Type: core.StreamError, Err: err})
				return
			}
			if done {
				emit(core.StreamEvent{Type: core.StreamDone, FinishReason: finish})
				return
			}
			if !p.emitChunk(data, &toolIdx, nameSeq, emit) {
				return
			}
			if stopReason := chunkFinishReason(data); stopReason != "" {
				finish = geminiFinish(stopReason)
			}
		}
	}()
	return events, nil
}

// emitChunk 转换单个流片段
//
// toolIdx 是跨 chunk 的工具调用序号：Gemini 的 functionCall 无 index，
// 聚合器按 Index 合并分片，不给独立序号时并行调用会互相覆盖
// returns: false 表示消费端已取消
func (p *GeminiProtocol) emitChunk(data string, toolIdx *int, nameSeq map[string]int, emit func(core.StreamEvent) bool) bool {
	var chunk struct {
		Candidates []struct {
			Content struct {
				Parts []geminiPart `json:"parts"`
			} `json:"content"`
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount     int64 `json:"promptTokenCount"`
			CandidatesTokenCount int64 `json:"candidatesTokenCount"`
			ThoughtsTokenCount   int64 `json:"thoughtsTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		emit(core.StreamEvent{
			Type: core.StreamError,
			Err:  core.NewError(core.ErrProviderInternal, p.providerID, fmt.Errorf("decode stream chunk: %w", err)),
		})
		return false
	}
	if chunk.UsageMetadata.PromptTokenCount > 0 || chunk.UsageMetadata.CandidatesTokenCount > 0 ||
		chunk.UsageMetadata.ThoughtsTokenCount > 0 {
		if !emit(core.StreamEvent{Type: core.StreamUsage, Usage: core.Usage{
			InputTokens:     chunk.UsageMetadata.PromptTokenCount,
			OutputTokens:    chunk.UsageMetadata.CandidatesTokenCount,
			ReasoningTokens: chunk.UsageMetadata.ThoughtsTokenCount,
		}}) {
			return false
		}
	}
	if len(chunk.Candidates) == 0 {
		return true
	}
	for _, part := range chunk.Candidates[0].Content.Parts {
		if part.Thought && part.Text != "" {
			if !emit(core.StreamEvent{Type: core.StreamDeltaReasoning, Reasoning: part.Text}) {
				return false
			}
			continue
		}
		if part.Text != "" {
			if !emit(core.StreamEvent{Type: core.StreamDeltaText, Text: part.Text}) {
				return false
			}
		}
		if part.FunctionCall != nil && part.FunctionCall.Name != "" {
			args, _ := json.Marshal(part.FunctionCall.Args)
			if string(args) == "null" {
				args = []byte("{}")
			}
			idx := *toolIdx
			*toolIdx++
			// 同名并行调用按出现序号消歧（第二次起追加 :<n>），
			// 首次保持旧格式 gemini:<name>，与流式/非流式一致
			seq := nameSeq[part.FunctionCall.Name]
			nameSeq[part.FunctionCall.Name] = seq + 1
			if !emit(core.StreamEvent{
				Type: core.StreamDeltaToolCall,
				ToolCallDelta: core.ToolCallDelta{
					Index:    idx,
					ID:       geminiCallID(part.FunctionCall.Name, seq),
					Name:     part.FunctionCall.Name,
					ArgsPart: string(args),
				},
			}) {
				return false
			}
		}
	}
	return true
}

// chunkFinishReason 提取片段的终止原因
// returns: 终止原因字符串，无则空串
func chunkFinishReason(data string) string {
	var probe struct {
		Candidates []struct {
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
	}
	if json.Unmarshal([]byte(data), &probe) != nil || len(probe.Candidates) == 0 {
		return ""
	}
	return probe.Candidates[0].FinishReason
}
