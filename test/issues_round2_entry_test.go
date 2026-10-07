package test

import (
	"encoding/binary"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// writeFragment 发送带掩码、FIN 位可控的客户端帧
//
// 现有 writeMasked 固定 FIN=1，构造分片与控制帧违规形态需要手工拼首字节
func (c *wsTestClient) writeFragment(op int, fin bool, data []byte) {
	n := len(data)
	first := byte(op)
	if fin {
		first |= 0x80
	}
	frame := []byte{first}
	switch {
	case n < 126:
		frame = append(frame, byte(0x80|n))
	case n < 1<<16:
		frame = append(frame, 0x80|126, byte(n>>8), byte(n))
	default:
		frame = append(frame, 0x80|127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(n))
		frame = append(frame, ext...)
	}
	mask := []byte{1, 2, 3, 4}
	frame = append(frame, mask...)
	for i, b := range data {
		frame = append(frame, b^mask[i%4])
	}
	if _, err := c.conn.Write(frame); err != nil {
		panic(err)
	}
}

// readCloseFrame 断言下一帧是携带指定状态码的关闭帧
func (c *wsTestClient) readCloseFrame(t *testing.T, wantCode int) {
	t.Helper()
	op, payload := c.readMessage(t)
	if op != 8 {
		t.Fatalf("opcode = %d, want close(8)", op)
	}
	if len(payload) < 2 || int(binary.BigEndian.Uint16(payload[:2])) != wantCode {
		t.Fatalf("close status = %v, want %d", payload, wantCode)
	}
}

// TestRound2WSFragmentAssembled 多段分片消息在累计上限内必须被拼接投递
func TestRound2WSFragmentAssembled(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	req, _ := json.Marshal(map[string]any{"input": "hi", "session_id": "frag-1"})
	// 首帧 text FIN=0 + 一段中间 continuation + 末段 FIN=1
	client.writeFragment(1, false, req[:5])
	client.writeFragment(0, false, req[5:11])
	client.writeFragment(0, true, req[11:])

	events := client.collectEvents(t)
	done := false
	for _, ev := range events {
		if ev["event"] == "done" {
			done = true
		}
	}
	if !done {
		t.Fatal("fragmented message not assembled/delivered")
	}
}

// TestRound2WSFragmentOverCap 分片累计超上限必须以 1009 关闭连接
func TestRound2WSFragmentOverCap(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	// 8MB + 8MB 恰好压线不触发，再补 1MB 累计 17MB 超过 16MB 上限；
	// 单帧自身均低于 16MB 帧上限，只应由"累计"路径拒绝
	chunk := make([]byte, 8<<20)
	client.writeFragment(1, false, chunk)
	client.writeFragment(0, false, chunk)
	client.writeFragment(0, true, make([]byte, 1<<20))

	client.readCloseFrame(t, 1009)
}

// TestRound2LE2ControlFrameRules 控制帧与分片协议规则违规必须回 1002
func TestRound2LE2ControlFrameRules(t *testing.T) {
	cases := []struct {
		name string
		send func(c *wsTestClient)
	}{
		// 控制帧不容许 FIN=0（RFC6455 §5.5）
		{"ping-fragmented", func(c *wsTestClient) {
			c.writeFragment(9, false, []byte("x"))
		}},
		// 控制帧载荷不得超过 125 字节
		{"ping-oversized", func(c *wsTestClient) {
			c.writeFragment(9, true, make([]byte, 126))
		}},
		// 分片在途时来了新的首个数据帧，不得静默覆盖已累计内容
		{"new-data-during-fragment", func(c *wsTestClient) {
			c.writeFragment(1, false, []byte(`{"inp`))
			c.writeFragment(1, true, []byte(`ut":"hi"}`))
		}},
		// 无在途分片的孤立 continuation 同样是协议违规
		{"stray-continuation", func(c *wsTestClient) {
			c.writeFragment(0, true, []byte("stray"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t, &streamMockLLM{})
			ts := httptest.NewServer(srv.Handler())
			defer ts.Close()

			client := dialWS(t, ts.URL)
			defer client.conn.Close()

			tc.send(client)
			client.readCloseFrame(t, 1002)
		})
	}
}

// TestRound2WSServerPingKeepalive 服务端 60s 保活 ping 无法黑盒观测
//
// ping 周期是 pkg/entry 包内常量（60s），测试包无注入口；
// 等待首个 ping 需 ≥60s，超出"不添加 >2s sleep"的测试时限约束。
// 结构保证：keepalive goroutine 在 connCtx 取消时退出（无泄漏），
// ping 帧经 writeFrame 持写锁写出，与 pong/close/事件帧串行化。
func TestRound2WSServerPingKeepalive(t *testing.T) {
	t.Skip("60s ping 周期无法在测试时限内观测，黑盒无短周期注入口")
}
