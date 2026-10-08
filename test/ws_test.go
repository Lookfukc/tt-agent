package test

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// wsTestClient is a raw-TCP WebSocket client
type wsTestClient struct {
	conn net.Conn
	br   *bufio.Reader
}

// dialWS performs the upgrade handshake manually
// returns: a ready client connection
func dialWS(t *testing.T, url string) *wsTestClient {
	t.Helper()
	addr := strings.TrimPrefix(url, "http://")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	req := "GET /api/chat/ws HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write upgrade: %v", err)
	}

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("status = %s", status)
	}
	var accept string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read header: %v", err)
		}
		if line == "\r\n" {
			break
		}
		if v, ok := strings.CutPrefix(line, "Sec-WebSocket-Accept: "); ok {
			accept = strings.TrimSpace(v)
		}
	}
	// Verify the accept computation
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	want := base64.StdEncoding.EncodeToString(sum[:])
	if accept != want {
		t.Fatalf("accept = %q, want %q", accept, want)
	}
	return &wsTestClient{conn: conn, br: br}
}

// writeMasked sends a masked client frame
func (c *wsTestClient) writeMasked(op int, data []byte) {
	n := len(data)
	frame := []byte{byte(0x80 | op)}
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

// readMessage reads a server frame (server frames are unmasked)
// returns: the frame opcode and payload
func (c *wsTestClient) readMessage(t *testing.T) (int, []byte) {
	t.Helper()
	var hdr [2]byte
	if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
		t.Fatalf("read frame header: %v", err)
	}
	op := int(hdr[0] & 0x0F)
	length := int64(hdr[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		_, _ = io.ReadFull(c.br, ext[:])
		length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		_, _ = io.ReadFull(c.br, ext[:])
		length = int64(binary.BigEndian.Uint64(ext[:]))
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	return op, payload
}

// collectEvents reads frames until a done/error event
// returns: the list of event JSON objects
func (c *wsTestClient) collectEvents(t *testing.T) []map[string]any {
	t.Helper()
	var events []map[string]any
	for i := 0; i < 100; i++ {
		var ev map[string]any
		_, payload := c.readMessage(t)
		if err := json.Unmarshal(payload, &ev); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		events = append(events, ev)
		if ev["event"] == "done" || ev["event"] == "error" {
			return events
		}
	}
	t.Fatal("no terminal event within 100 frames")
	return nil
}

func TestWebSocketChat(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	req, _ := json.Marshal(map[string]any{"input": "hi", "session_id": "ws-1"})
	client.writeMasked(1, req)
	events := client.collectEvents(t)

	var text strings.Builder
	done := false
	for _, ev := range events {
		switch ev["event"] {
		case "text":
			text.WriteString(ev["delta"].(string))
		case "done":
			done = true
		}
	}
	if !done {
		t.Fatalf("no done event: %v", events)
	}
	if text.String() != "你好" {
		t.Errorf("text = %q", text.String())
	}

	// Continue a second round on the same connection
	req2, _ := json.Marshal(map[string]any{"input": "again"})
	client.writeMasked(1, req2)
	events2 := client.collectEvents(t)
	if len(events2) == 0 {
		t.Fatal("no events for second message")
	}
}

func TestWebSocketPing(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	client.writeMasked(9, []byte("ping-data"))
	op, payload := client.readMessage(t)
	// The server should reply pong with the payload echoed verbatim
	if op != 0xA {
		t.Errorf("pong opcode = %#x", op)
	}
	if string(payload) != "ping-data" {
		t.Errorf("pong payload = %q", payload)
	}
}

func TestWebSocketBadBody(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	client.writeMasked(1, []byte("not-json"))
	var ev map[string]any
	_, payload := client.readMessage(t)
	if err := json.Unmarshal(payload, &ev); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ev["event"] != "error" {
		t.Errorf("event = %v", ev)
	}
}
