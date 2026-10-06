package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// framedTransport 长连接按行分帧传输（stdio 等）
//
// 请求写出后挂起等待读循环按 ID 派发响应
type framedTransport struct {
	rw io.ReadWriteCloser

	writeMu   sync.Mutex
	closeOnce sync.Once
	doneOnce  sync.Once
	pending   sync.Map // id -> chan *rpcResponse
	done      chan struct{}
}

// newFramedTransport 构造分帧传输并启动读循环
// rw: 双向分帧流
// returns: 就绪的传输
func newFramedTransport(rw io.ReadWriteCloser) *framedTransport {
	t := &framedTransport{rw: rw, done: make(chan struct{})}
	go t.readLoop()
	return t
}

// send 写出请求并等待对应响应
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

// notify 写出通知帧
func (t *framedTransport) notify(_ context.Context, req rpcRequest) error {
	return t.writeFrame(req)
}

// close 关闭流并唤醒全部等待者
//
// 绝不能先抢 writeMu：对端停读导致 Write 阻塞在锁内时，
// Close 会跟着卡死，Kill 也永远执行不到。直接关流即可，
// 底层连接关闭（stdio 下含 Kill 进程）会解除阻塞的 Write
func (t *framedTransport) close() error {
	var err error
	t.closeOnce.Do(func() {
		t.shutdownDone()
		err = t.rw.Close()
	})
	return err
}

// shutdownDone 关闭 done 通道，只关一次
func (t *framedTransport) shutdownDone() {
	t.doneOnce.Do(func() { close(t.done) })
}

// readLoop 消费响应帧并按 ID 派发
func (t *framedTransport) readLoop() {
	scanner := bufio.NewScanner(t.rw)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		var resp rpcResponse
		// method 非空是服务器主动请求（如 roots/list），id 可能
		// 与在途请求撞号，误当响应会静默吞掉正确结果
		if json.Unmarshal(scanner.Bytes(), &resp) != nil || resp.ID == 0 || resp.Method != "" {
			// 通知、服务器请求或坏帧，客户端侧不处理
			continue
		}
		if ch, ok := t.pending.LoadAndDelete(resp.ID); ok {
			ch.(chan *rpcResponse) <- &resp
		}
	}
	// 读循环退出（含超长行 ErrTooLong）必须关 done，
	// 否则之后的全部请求永久挂起且无任何报错
	t.shutdownDone()
	// 传输关闭后叫醒所有等待者
	t.pending.Range(func(_, v any) bool {
		close(v.(chan *rpcResponse))
		return true
	})
}

// writeFrame 串行化写出一帧
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
