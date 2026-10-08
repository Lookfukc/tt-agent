package protocol

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// sseReader is a line-level SSE reader.
//
// SSE events may arrive across multiple TCP reads, so line buffering is a
// precondition for protocol correctness; protocol adapters share this reader,
// which is responsible only for parsing the data payload.
type sseReader struct {
	scanner *bufio.Scanner
	// eofAsDone controls whether connection close counts as a normal end:
	// the OpenAI family terminates with [DONE], so EOF is abnormal;
	// native Gemini streams end with connection close and carry no marker.
	eofAsDone bool
}

// newSSEReader constructs the reader and enables long-line buffering.
// eofAsDone: when true, EOF is treated as a normal end of the stream.
// returns: the ready reader.
func newSSEReader(resp *http.Response, eofAsDone bool) *sseReader {
	s := bufio.NewScanner(resp.Body)
	// Increment lines are usually tiny, but long tool arguments can exceed the
	// default 64KB; relax the limit to 1MB.
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return &sseReader{scanner: s, eofAsDone: eofAsDone}
}

// next reads the next data payload.
// ctx: used to interrupt a blocking read on cancellation.
// returns: the raw data payload; done is true when the stream ended normally; a non-nil err means the stream is broken.
func (r *sseReader) next(ctx context.Context) (string, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", false, core.NewError(core.ErrCanceled, "", err)
		}
		if !r.scanner.Scan() {
			if err := r.scanner.Err(); err != nil {
				if errors.Is(err, bufio.ErrTooLong) {
					// %w passes ErrTooLong through so callers can identify this
					// class of error programmatically via errors.Is on the
					// Unwrap chain of core.Error.
					return "", false, core.NewError(core.ErrNetwork, "",
						fmt.Errorf("sse line exceeds 1MB limit (likely oversized tool arguments): %w", bufio.ErrTooLong))
				}
				// Cancellation/timeout inside a blocking read surfaces as a body
				// read error; it must be classified as non-retryable cancellation,
				// otherwise it would be retried repeatedly as a transient network error.
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return "", false, core.NewError(core.ErrCanceled, "", err)
				}
				// The transport closes the connection when ctx ends, and the read
				// side usually surfaces "use of closed network connection";
				// in that case ctx's conclusion takes precedence.
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
