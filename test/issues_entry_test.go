package test

import (
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// writeUnmasked sends a frame without the mask bit set (a protocol-violating form)
func (c *wsTestClient) writeUnmasked(op int, data []byte) {
	frame := []byte{byte(0x80 | op), byte(len(data))}
	frame = append(frame, data...)
	if _, err := c.conn.Write(frame); err != nil {
		panic(err)
	}
}

// dialRaw makes a raw TCP connection (no WS handshake)
// returns: the raw connection
func dialRaw(t *testing.T, url string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(url, "http://"), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

// TestM_E3UnmaskedFrameRejected verifies an unmasked client frame must be rejected (RFC6455 §5.1)
func TestM_E3UnmaskedFrameRejected(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	client.writeUnmasked(1, []byte(`{"input":"hi"}`))
	// An unmasked frame must fail the connection with a close frame (opcode 8)
	// carrying 1002, rather than being processed as a message
	client.readCloseFrame(t, 1002)
}

// TestM_E4OriginRejected verifies a cross-origin WS handshake must get 403
func TestM_E4OriginRejected(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req := strings.Replace(
		"GET /api/chat/ws HTTP/1.1\r\nHost: HOST\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"+
			"Origin: http://evil.example.com\r\n\r\n",
		"HOST", strings.TrimPrefix(ts.URL, "http://"), 1)
	conn := dialRaw(t, ts.URL)
	defer conn.Close()
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 512)
	n, _ := conn.Read(buf)
	if !strings.Contains(string(buf[:n]), "403") {
		t.Fatalf("M-E4: cross-origin upgrade not rejected: %s", string(buf[:n]))
	}
}

// TestM_E5BodyLimit verifies an over-limit request body must get 400 instead of being swallowed in full
func TestM_E5BodyLimit(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	big := `{"input":"` + strings.Repeat("x", 5<<20) + `"}`
	resp, err := ts.Client().Post(ts.URL+"/api/chat", "application/json", strings.NewReader(big))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("M-E5: status = %d, want 400", resp.StatusCode)
	}
}
