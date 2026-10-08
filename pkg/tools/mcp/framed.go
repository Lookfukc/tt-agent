package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// framedTransport is a long-lived, line-framed transport (stdio etc.).
//
// After a request is written out, the caller suspends until the read loop
// dispatches the response by ID.
type framedTransport struct {
	rw io.ReadWriteCloser

	writeMu   sync.Mutex
	closeOnce sync.Once
	doneOnce  sync.Once
	pending   sync.Map // id -> chan *rpcResponse
	done      chan struct{}
}

// newFramedTransport constructs the framed transport and starts the read loop.
// rw: a bidirectional framed stream
// returns: the ready transport
func newFramedTransport(rw io.ReadWriteCloser) *framedTransport {
	t := &framedTransport{rw: rw, done: make(chan struct{})}
	go t.readLoop()
	return t
}

// send writes the request out and waits for the matching response.
func (t *framedTransport) send(ctx context.Context, req rpcRequest) (*rpcResponse, error) {
	ch := make(chan *rpcResponse, 1)
	t.pending.Store(req.ID, ch)
	if err := t.writeFrame(req); err != nil {
		t.pending.Delete(req.ID)
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("connection closed while waiting id %d", req.ID)
		}
		return resp, nil
	case <-ctx.Done():
		t.pending.Delete(req.ID)
		return nil, ctx.Err()
	case <-t.done:
		return nil, fmt.Errorf("connection closed")
	}
}

// notify writes a notification frame out.
func (t *framedTransport) notify(_ context.Context, req rpcRequest) error {
	return t.writeFrame(req)
}

// close closes the stream and wakes all waiters.
//
// It must never grab writeMu first: if the peer stops reading and Write
// blocks inside the lock, Close would block with it and Kill would never
// run. Just close the stream directly — closing the underlying connection
// (including killing the process under stdio) unblocks the stuck Write.
func (t *framedTransport) close() error {
	var err error
	t.closeOnce.Do(func() {
		t.shutdownDone()
		err = t.rw.Close()
	})
	return err
}

// shutdownDone closes the done channel, exactly once.
func (t *framedTransport) shutdownDone() {
	t.doneOnce.Do(func() { close(t.done) })
}

// readLoop consumes response frames and dispatches them by ID.
func (t *framedTransport) readLoop() {
	scanner := bufio.NewScanner(t.rw)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		var resp rpcResponse
		// A non-empty method marks a server-initiated request (e.g.
		// roots/list); its id may collide with an in-flight request, and
		// mistaking it for the response would silently swallow the
		// correct result
		if json.Unmarshal(scanner.Bytes(), &resp) != nil || resp.ID == 0 || resp.Method != "" {
			// Notification, server request, or bad frame — not handled
			// on the client side
			continue
		}
		if ch, ok := t.pending.LoadAndDelete(resp.ID); ok {
			ch.(chan *rpcResponse) <- &resp
		}
	}
	// When the read loop exits (including ErrTooLong on overlong lines),
	// done must be closed; otherwise every subsequent request would hang
	// forever with no error at all
	t.shutdownDone()
	// Wake all waiters after the transport is closed
	t.pending.Range(func(_, v any) bool {
		close(v.(chan *rpcResponse))
		return true
	})
}

// writeFrame serializes the writing of one frame.
func (t *framedTransport) writeFrame(req rpcRequest) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if _, err := t.rw.Write(payload); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}
