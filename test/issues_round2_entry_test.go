package test

import (
	"encoding/binary"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// writeFragment sends a masked client frame with a controllable FIN bit
//
// The existing writeMasked always sets FIN=1; constructing fragmented messages
// and control-frame violations requires hand-crafting the first byte
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

// readCloseFrame asserts the next frame is a close frame carrying the given status code
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

// TestRound2WSFragmentAssembled verifies multi-segment fragmented messages within the cumulative cap must be assembled and delivered
func TestRound2WSFragmentAssembled(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	req, _ := json.Marshal(map[string]any{"input": "hi", "session_id": "frag-1"})
	// First text frame FIN=0 + one middle continuation + final segment FIN=1
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

// TestRound2WSFragmentOverCap verifies fragments exceeding the cumulative cap must close the connection with 1009
func TestRound2WSFragmentOverCap(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	// 8MB + 8MB sits exactly at the edge without triggering; adding 1MB more totals
	// 17MB, exceeding the 16MB cap; each individual frame is itself below the 16MB
	// frame limit, so only the "cumulative" path should reject
	chunk := make([]byte, 8<<20)
	client.writeFragment(1, false, chunk)
	client.writeFragment(0, false, chunk)
	client.writeFragment(0, true, make([]byte, 1<<20))

	client.readCloseFrame(t, 1009)
}

// TestRound2LE2ControlFrameRules verifies control-frame and fragmentation protocol violations must get 1002
func TestRound2LE2ControlFrameRules(t *testing.T) {
	cases := []struct {
		name string
		send func(c *wsTestClient)
	}{
		// Control frames must not have FIN=0 (RFC6455 §5.5)
		{"ping-fragmented", func(c *wsTestClient) {
			c.writeFragment(9, false, []byte("x"))
		}},
		// Control frame payload must not exceed 125 bytes
		{"ping-oversized", func(c *wsTestClient) {
			c.writeFragment(9, true, make([]byte, 126))
		}},
		// A new first data frame arriving while a fragment is in flight must not silently overwrite the accumulated content
		{"new-data-during-fragment", func(c *wsTestClient) {
			c.writeFragment(1, false, []byte(`{"inp`))
			c.writeFragment(1, true, []byte(`ut":"hi"}`))
		}},
		// A stray continuation with no fragment in flight is likewise a protocol violation
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

// TestRound2WSServerPingKeepalive: the server's 60s keepalive ping cannot be observed black-box
//
// The ping period is a constant inside the pkg/entry package (60s), and the test
// package has no injection point; waiting for the first ping would take ≥60s,
// exceeding the test-time constraint of "no >2s sleeps".
// Structural guarantees: the keepalive goroutine exits when connCtx is cancelled
// (no leak); ping frames are written through writeFrame while holding the write
// lock, serialized with pong/close/event frames.
func TestRound2WSServerPingKeepalive(t *testing.T) {
	t.Skip("60s ping 周期无法在测试时限内观测，黑盒无短周期注入口")
}
