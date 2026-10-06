package test

import (
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Lookfukc/send-agent/pkg/config"
	"github.com/Lookfukc/send-agent/pkg/entry"
)

// writeUnmasked 发送不带掩码位的帧（协议违规形态）
func (c *wsTestClient) writeUnmasked(op int, data []byte) {
	frame := []byte{byte(0x80 | op), byte(len(data))}
	frame = append(frame, data...)
	if _, err := c.conn.Write(frame); err != nil {
		panic(err)
	}
}

// dialRaw 裸 TCP 连接（不做 WS 握手）
// returns: 原始连接
func dialRaw(t *testing.T, url string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(url, "http://"), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

// writeTemp 写临时配置文件
// returns: 文件路径
func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "providers.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

// loadProvidersForTest 配置加载直通
// returns: 加载错误
func loadProvidersForTest(path string) error {
	_, err := config.LoadProviders(path)
	return err
}

// TestM_E3UnmaskedFrameRejected 未掩码客户端帧必须拒绝（RFC6455 §5.1）
func TestM_E3UnmaskedFrameRejected(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := dialWS(t, ts.URL)
	defer client.conn.Close()

	client.writeUnmasked(1, []byte(`{"input":"hi"}`))
	// 未掩码帧必须以携带 1002 的关闭帧（opcode 8）失败连接，
	// 而不是被当作消息处理
	client.readCloseFrame(t, 1002)
}

// TestM_E4OriginRejected 跨源 WS 握手必须 403
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

// TestM_E5BodyLimit 超限请求体必须 400 而非全量吞入
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

// TestM_E7ConfigValidation 协议/scheme/默认模型/内置冲突全部装配期拦截
func TestM_E7ConfigValidation(t *testing.T) {
	cases := map[string]string{
		"protocol":  `{"providers":[{"id":"p1","protocol":"bad","base_url":"http://a","api_key_env":"K","models":[{"id":"m1"}]}]}`,
		"scheme":    `{"providers":[{"id":"p1","protocol":"openai","base_url":"ftp://a","api_key_env":"K","models":[{"id":"m1"}]}]}`,
		"model":     `{"providers":[{"id":"p1","protocol":"openai","base_url":"http://a","api_key_env":"K","default_model":"nope","models":[{"id":"m1"}]}]}`,
		"builtinID": `{"providers":[{"id":"glm","protocol":"openai","base_url":"http://a","api_key_env":"K","models":[{"id":"m1"}]}]}`,
	}
	_ = entry.WithConfig
	for name, raw := range cases {
		if err := loadProvidersForTest(writeTemp(t, raw)); err == nil {
			t.Errorf("M-E7 %s: want validation error", name)
		}
	}
}
