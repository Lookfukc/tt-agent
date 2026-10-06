package builtin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// maxFetchBytes 响应体上限，超长截断防记忆爆窗
const maxFetchBytes = 64 * 1024

// maxRedirects 重定向跳数上限
const maxRedirects = 5

// Clock 返回当前时间
type Clock struct{}

// NewClock 构造时钟工具
// returns: 可注册的工具实例
func NewClock() *Clock { return &Clock{} }

// Name 工具名
func (Clock) Name() string { return "clock" }

// Description 工具描述
func (Clock) Description() string { return "获取当前日期时间，格式 RFC3339" }

// Parameters 参数 schema
func (Clock) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}

// Execute 返回当前时间
func (Clock) Execute(_ context.Context, _ json.RawMessage) (core.ToolResult, error) {
	now := time.Now().Format(time.RFC3339)
	return core.ToolResult{Data: map[string]any{"now": now}}, nil
}

// HTTPFetch 抓取 URL 文本内容
//
// 默认拒绝环回/私网/链路本地目标：该工具面向模型输出，无过滤即
// SSRF，可被引导抓取云元数据端点。重定向手动逐跳跟随，且私网
// 校验在拨号期强制执行（防 DNS rebinding TOCTOU），两层校验
// 均受 AllowPrivateNetwork 开关控制
type HTTPFetch struct {
	client *http.Client
	// AllowPrivateNetwork 显式放行内网目标（自托管内网场景）
	AllowPrivateNetwork bool
}

// NewHTTPFetch 构造抓取工具，默认启用私网过滤
// returns: 可注册的工具实例
func NewHTTPFetch() *HTTPFetch {
	return NewHTTPFetchWithOptions()
}

// HTTPOption HTTPFetch 功能选项
type HTTPOption func(*HTTPFetch)

// WithAllowPrivateTargets 放行环回/私网目标（自托管内网场景）
//
// 生产默认必须保持严格（false），放行仅用于内网部署与本地测试
func WithAllowPrivateTargets(allow bool) HTTPOption {
	return func(f *HTTPFetch) { f.AllowPrivateNetwork = allow }
}

// NewHTTPFetchWithOptions 按选项构造抓取工具
// opts: 功能选项，见 WithAllowPrivateTargets
// returns: 可注册的工具实例
func NewHTTPFetchWithOptions(opts ...HTTPOption) *HTTPFetch {
	f := &HTTPFetch{
		client: &http.Client{
			Timeout: 15 * time.Second,
			// 重定向手动跟随：每一跳都要重新过 IP 校验
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	// 私网过滤的权威执行点在拨号期（见 dialChecked），而非请求前的
	// 一次性 DNS 解析：两次解析之间的 DNS 切换即 rebinding TOCTOU。
	// 不挂 ProxyFromEnvironment：经代理时拨号目标是代理而非 URL 主机，
	// 私网校验会被整体绕过
	f.client.Transport = &http.Transport{DialContext: f.dialChecked}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Name 工具名
func (HTTPFetch) Name() string { return "http_fetch" }

// Description 工具描述
func (HTTPFetch) Description() string {
	return "发起 HTTP GET 请求并返回文本响应体，适合抓取网页或 API 文本内容"
}

// Parameters 参数 schema
func (HTTPFetch) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"url":{"type":"string","description":"完整 URL"}},"required":["url"]}`)
}

// Execute 抓取 URL
// returns: 截断后的文本内容
func (f *HTTPFetch) Execute(ctx context.Context, args json.RawMessage) (core.ToolResult, error) {
	var in struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return core.ToolResult{}, fmt.Errorf("invalid args: %w", err)
	}
	if !strings.HasPrefix(in.URL, "http://") && !strings.HasPrefix(in.URL, "https://") {
		return core.ToolResult{}, fmt.Errorf("url must be http(s)")
	}

	current := in.URL
	var resp *http.Response
	for hop := 0; hop <= maxRedirects; hop++ {
		if err := f.checkTarget(ctx, current); err != nil {
			return core.ToolResult{}, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, current, nil)
		if err != nil {
			return core.ToolResult{}, err
		}
		req.Header.Set("User-Agent", "send-agent/0.1")
		resp, err = f.client.Do(req)
		if err != nil {
			return core.ToolResult{}, err
		}
		if !isRedirect(resp.StatusCode) {
			break
		}
		location := resp.Header.Get("Location")
		resp.Body.Close()
		if location == "" {
			return core.ToolResult{}, fmt.Errorf("redirect without Location")
		}
		next, err := resolveLocation(current, location)
		if err != nil {
			return core.ToolResult{}, err
		}
		current = next
		if hop == maxRedirects {
			return core.ToolResult{}, fmt.Errorf("too many redirects")
		}
	}
	if resp == nil {
		return core.ToolResult{}, fmt.Errorf("no response")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFetchBytes+1))
	if err != nil {
		return core.ToolResult{}, err
	}
	truncated := false
	if len(body) > maxFetchBytes {
		body = body[:maxFetchBytes]
		truncated = true
	}
	return core.ToolResult{Data: map[string]any{
		"status":    resp.StatusCode,
		"content":   string(body),
		"truncated": truncated,
	}}, nil
}

// checkTarget 请求前的快速失败校验
//
// 权威校验在 dialChecked（拨号期），此处只为给出即时错误信息，
// 避免 IP 字面量目标还要先建连接才被拒
func (f *HTTPFetch) checkTarget(ctx context.Context, rawURL string) error {
	if f.AllowPrivateNetwork {
		return nil
	}
	host := hostOf(rawURL)
	if host == "" {
		return fmt.Errorf("invalid url")
	}
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsGlobalUnicast() || isPrivateIP(ip) {
			return fmt.Errorf("blocked non-public target %s", host)
		}
		return nil
	}
	// 域名逐条解析 A/AAAA，全部地址都放行才放行
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", host, err)
	}
	for _, addr := range addrs {
		if !addr.IP.IsGlobalUnicast() || isPrivateIP(addr.IP) {
			return fmt.Errorf("blocked non-public target %s (%s)", host, addr.IP)
		}
	}
	return nil
}

// dialChecked 拨号期私网校验，关闭 DNS rebinding TOCTOU 窗口
//
// checkTarget 的解析结果与真正拨号之间 DNS 记录可能被切换
// （rebinding 攻击），因此校验权威在此：域名的每条解析结果都必须
// 放行，随后钉扎首个合法 IP 直接拨号，绕开 http.Transport 的二次
// 解析。只改拨号地址、不动 URL，TLS 仍按 URL 主机名做 SNI 与
// 证书校验
func (f *HTTPFetch) dialChecked(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if f.AllowPrivateNetwork {
		return dialer.DialContext(ctx, network, addr)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("httpfetch: split dial address %s: %w", addr, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("httpfetch: resolve %s: %w", host, err)
		}
		// 任一解析结果命中私网段即整体拒绝：rebinding 常用 TTL 极短
		// 的公网/私网交替记录混过抽检
		for _, a := range addrs {
			if !a.IP.IsGlobalUnicast() || isPrivateIP(a.IP) {
				return nil, fmt.Errorf("httpfetch: blocked non-public target %s (%s)", host, a.IP)
			}
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("httpfetch: no address resolved for %s", host)
		}
		ip = addrs[0].IP
	}
	if !ip.IsGlobalUnicast() || isPrivateIP(ip) {
		return nil, fmt.Errorf("httpfetch: blocked non-public target %s", host)
	}
	return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
}

// isPrivateIP 判定私有/特殊网段
//
// IsGlobalUnicast 已覆盖多数，但拨号 VPN 常见的 100.64/10 段
// 与 IsGlobalUnicast 交集需要单独排除
func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	// CGNAT 100.64.0.0/10
	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
		return true
	}
	return false
}

// hostOf 提取 URL 的主机部分（去掉端口）
func hostOf(rawURL string) string {
	s := rawURL
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	if i := strings.LastIndex(s, "@"); i >= 0 {
		s = s[i+1:]
	}
	// 去端口，但不动 IPv6 字面量的冒号
	if strings.HasPrefix(s, "[") {
		if i := strings.Index(s, "]"); i >= 0 {
			return strings.Trim(s[1:i], "[]")
		}
	}
	if i := strings.LastIndex(s, ":"); i >= 0 && strings.Count(s, ":") == 1 {
		s = s[:i]
	}
	return s
}

// resolveLocation 按 RFC 3986 解析重定向目标为绝对 URL
//
// Location 可能是绝对 URL、绝对路径、相对路径或 "../" 形式，
// 手工拼 scheme+host+location 会漏掉无前导斜杠的相对路径，
// 必须用 URL 基准解析
// returns: 绝对地址或错误
func resolveLocation(base, location string) (string, error) {
	b, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse base %q: %w", base, err)
	}
	ref, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parse Location %q: %w", location, err)
	}
	resolved := b.ResolveReference(ref)
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return "", fmt.Errorf("redirect target scheme %q not supported", resolved.Scheme)
	}
	return resolved.String(), nil
}

// isRedirect 判定重定向状态码
func isRedirect(code int) bool {
	return code == 301 || code == 302 || code == 303 || code == 307 || code == 308
}
