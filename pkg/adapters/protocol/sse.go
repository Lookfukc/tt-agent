package protocol

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// sseReader SSE 行级读取器
//
// SSE 事件可能跨多次 TCP read 到达，行缓冲是协议正确性的前提；
// 各协议适配器共享此读取器，只负责解析 data 载荷
type sseReader struct {
	scanner *bufio.Scanner
	// eofAsDone 连接关闭是否视为正常结束：
	// OpenAI 系约定以 [DONE] 收尾，EOF 属异常；
	// 原生 Gemini 流以连接关闭结束，无终止标记
	eofAsDone bool
}

// newSSEReader 构造读取器并启用长行缓冲
// eofAsDone: true 时 EOF 视为流正常结束
// returns: 就绪的读取器
func newSSEReader(resp *http.Response, eofAsDone bool) *sseReader {
	s := bufio.NewScanner(resp.Body)
	// 增量行通常很小，但工具长参数可能超默认 64KB，放宽到 1MB
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return &sseReader{scanner: s, eofAsDone: eofAsDone}
}

// next 读取下一个 data 载荷
// ctx: 用于取消时中断阻塞读
// returns: data 载荷原文；done 为 true 表示流正常结束；err 非 nil 时流已损坏
func (r *sseReader) next(ctx context.Context) (string, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", false, core.NewError(core.ErrCanceled, "", err)
		}
		if !r.scanner.Scan() {
			if err := r.scanner.Err(); err != nil {
				if errors.Is(err, bufio.ErrTooLong) {
					// %w 透传 ErrTooLong，调用方可经 core.Error 的
					// Unwrap 链用 errors.Is 程序化识别此类错误
					return "", false, core.NewError(core.ErrNetwork, "",
						fmt.Errorf("sse line exceeds 1MB limit (likely oversized tool arguments): %w", bufio.ErrTooLong))
				}
				// 取消/超时发生在阻塞读内时以 body 读错误的形式浮出，
				// 必须归为不可重试的取消，否则会被当作瞬时网络错误反复重试
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return "", false, core.NewError(core.ErrCanceled, "", err)
				}
				// transport 在 ctx 结束时关闭连接，读端浮出的往往是
				// "use of closed network connection"；此时以 ctx 的结论为准
				if ctxErr := ctx.Err(); ctxErr != nil {
					return "", false, core.NewError(core.ErrCanceled, "", ctxErr)
				}
				return "", false, core.NewError(core.ErrNetwork, "", err)
			}
			if r.eofAsDone {
				return "", true, nil
			}
			return "", false, core.NewError(core.ErrNetwork, "",
				fmt.Errorf("stream closed without termination marker"))
		}
		line := strings.TrimSpace(r.scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			return "", true, nil
		}
		return data, false, nil
	}
}
