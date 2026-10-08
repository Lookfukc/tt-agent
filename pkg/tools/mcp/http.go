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

// httpTransport is the Streamable HTTP transport.
//
// Each request is one POST; the response may be a single JSON document or
// an SSE stream. The Mcp-Session-Id carried by the initialize response is
// reused automatically on subsequent requests.
type httpTransport struct {
	url    string
	apiKey string
	client *http.Client

	mu        sync.Mutex
	sessionID string
}

// newHTTPTransport constructs the HTTP transport.
// url: the MCP endpoint address
// apiKey: an optional Bearer key
// returns: the ready transport
func newHTTPTransport(url, apiKey string) *httpTransport {
	return &httpTransport{
		url:    url,
		apiKey: apiKey,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

// ConnectHTTP builds a client for a Streamable HTTP server.
// name: the server name
// url: the MCP endpoint address
// apiKey: an optional key; omitted when empty
// returns: an un-handshaken client; Connect must still be called
func ConnectHTTP(name, url, apiKey string) *Client {
	return &Client{name: name, tr: newHTTPTransport(url, apiKey)}
}

// send POSTs the request and parses the response.
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
	// The session header rides on HTTP response headers and either
	// encoding format may allocate one; it must be captured before the
	// branch, otherwise the session is lost when initialize answers via
	// SSE
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

// notify POSTs a notification frame; the response body is discarded.
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

// close: HTTP holds no long-lived resources; the session expires on the
// server side.
func (t *httpTransport) close() error { return nil }

// post sends one POST, carrying the session header.
func (t *httpTransport) post(ctx context.Context, payload []byte) (*http.Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, strings.NewReader(string(payload)))
	if err != nil {
		return nil, err
	}
	// The dual Accept is a protocol requirement: the server picks one of
	// the two reply formats
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

// captureSession records the server-assigned session ID.
func (t *httpTransport) captureSession(resp *http.Response) {
	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		t.mu.Lock()
		t.sessionID = sid
		t.mu.Unlock()
	}
}

// readSSE parses an SSE-formed response, up to the frame with the
// matching ID.
//
// The stream may interleave server notifications; filter by ID.
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
		// A non-empty method marks a server-initiated request; even with
		// the same id it must not be taken as this request's response
		if resp.Method == "" && resp.ID == id {
			return &resp, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read sse: %w", err)
	}
	return nil, fmt.Errorf("sse stream ended without response for id %d", id)
}
