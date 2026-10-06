package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"
)

// httpTransport Streamable HTTP 传输
//
// 每个请求 POST 一次，响应可能是单 JSON 或 SSE 流；
// initialize 响应携带的 Mcp-Session-Id 自动续用到后续请求
type httpTransport struct {
	url    string
	apiKey string
	client *http.Client

	mu        sync.Mutex
	sessionID string
}

// newHTTPTransport 构造 HTTP 传输
// url: MCP 端点地址
// apiKey: 可选的 Bearer 密钥
// returns: 就绪的传输
func newHTTPTransport(url, apiKey string) *httpTransport {
	return &httpTransport{
		url:    url,
		apiKey: apiKey,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

// ConnectHTTP 建立到 Streamable HTTP 服务器的客户端
// name: 服务器名
// url: MCP 端点地址
// apiKey: 可选密钥，空则不携带
// returns: 未握手的客户端，需再调用 Connect
func ConnectHTTP(name, url, apiKey string) *Client {
	return &Client{name: name, tr: newHTTPTransport(url, apiKey)}
}

// send POST 请求并解析响应
func (t *httpTransport) send(ctx context.Context, req rpcRequest) (*rpcResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := t.post(ctx, payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}

	ct := resp.Header.Get("Content-Type")
	mediaType, _, _ := mime.ParseMediaType(ct)
	// 会话头挂在 HTTP 响应头上，两种编码格式都可能分配，
	// 必须在分支之前捕获，否则 initialize 以 SSE 应答时丢会话
	t.captureSession(resp)
	switch {
	case strings.Contains(mediaType, "text/event-stream"):
		return t.readSSE(ctx, resp.Body, req.ID)
	default:
		var out rpcResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		return &out, nil
	}
}

// notify POST 通知帧，响应体丢弃
func (t *httpTransport) notify(ctx context.Context, req rpcRequest) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := t.post(ctx, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}

// close HTTP 无长连接资源，会话由服务器侧过期
func (t *httpTransport) close() error { return nil }

// post 发送一次 POST，带上会话头
func (t *httpTransport) post(ctx context.Context, payload []byte) (*http.Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, strings.NewReader(string(payload)))
	if err != nil {
		return nil, err
	}
	// 双 Accept 是协议要求：服务器二选一回复格式
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	if t.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+t.apiKey)
	}
	t.mu.Lock()
	sid := t.sessionID
	t.mu.Unlock()
	if sid != "" {
		httpReq.Header.Set("Mcp-Session-Id", sid)
	}
	return t.client.Do(httpReq)
}

// captureSession 记录服务器分配的会话 ID
func (t *httpTransport) captureSession(resp *http.Response) {
	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		t.mu.Lock()
		t.sessionID = sid
		t.mu.Unlock()
	}
}

// readSSE 解析 SSE 形式的响应，取到匹配 ID 的帧为止
//
// 流里可能夹带服务器通知，按 ID 过滤
func (t *httpTransport) readSSE(ctx context.Context, body io.Reader, id int64) (*rpcResponse, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line := strings.TrimSpace(scanner.Text())
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "" || data == "[DONE]" {
			continue
		}
		var resp rpcResponse
		if err := json.Unmarshal([]byte(data), &resp); err != nil {
			continue
		}
		// method 非空是服务器主动请求，id 相同也不能当本请求响应
		if resp.Method == "" && resp.ID == id {
			return &resp, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read sse: %w", err)
	}
	return nil, fmt.Errorf("sse stream ended without response for id %d", id)
}
