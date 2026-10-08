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

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// maxFetchBytes is the response body limit; oversized bodies are truncated
// to prevent memory blowups.
const maxFetchBytes = 64 * 1024

// maxRedirects is the maximum number of redirect hops.
const maxRedirects = 5

// Clock returns the current time.
type Clock struct{}

// NewClock constructs the clock tool.
// returns: a registrable tool instance
func NewClock() *Clock { return &Clock{} }

// Name returns the tool name.
func (Clock) Name() string { return "clock" }

// Description returns the tool description.
func (Clock) Description() string { return "获取当前日期时间，格式 RFC3339" }

// Parameters returns the parameter schema.
func (Clock) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}

// Execute returns the current time.
func (Clock) Execute(_ context.Context, _ json.RawMessage) (core.ToolResult, error) {
	now := time.Now().Format(time.RFC3339)
	return core.ToolResult{Data: map[string]any{"now": now}}, nil
}

// HTTPFetch fetches the text content of a URL.
//
// Loopback/private/link-local targets are rejected by default: this tool
// faces model output, and without filtering it is an SSRF vector that can
// be steered to fetch cloud metadata endpoints. Redirects are followed
// manually hop by hop, and the private-network check is enforced at dial
// time (defending against DNS rebinding TOCTOU); both layers of checking
// are governed by the AllowPrivateNetwork switch.
type HTTPFetch struct {
	client *http.Client
	// AllowPrivateNetwork explicitly permits intranet targets
	// (self-hosted intranet scenarios)
	AllowPrivateNetwork bool
}

// NewHTTPFetch constructs the fetch tool with private-network filtering
// enabled by default.
// returns: a registrable tool instance
func NewHTTPFetch() *HTTPFetch {
	return NewHTTPFetchWithOptions()
}

// HTTPOption is a functional option for HTTPFetch.
type HTTPOption func(*HTTPFetch)

// WithAllowPrivateTargets permits loopback/private targets (self-hosted
// intranet scenarios).
//
// The production default must stay strict (false); allowing is only for
// intranet deployments and local testing.
func WithAllowPrivateTargets(allow bool) HTTPOption {
	return func(f *HTTPFetch) { f.AllowPrivateNetwork = allow }
}

// NewHTTPFetchWithOptions constructs the fetch tool per the options.
// opts: functional options, see WithAllowPrivateTargets
// returns: a registrable tool instance
func NewHTTPFetchWithOptions(opts ...HTTPOption) *HTTPFetch {
	f := &HTTPFetch{
		client: &http.Client{
			Timeout: 15 * time.Second,
			// Redirects are followed manually: every hop must re-pass
			// IP validation
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	// The authoritative enforcement point for private-network filtering is
	// at dial time (see dialChecked), not a one-shot DNS resolution before
	// the request: a DNS switch between the two resolutions is exactly the
	// rebinding TOCTOU. ProxyFromEnvironment is deliberately not set: when
	// going through a proxy the dial target is the proxy rather than the
	// URL host, and private-network validation would be bypassed entirely
	f.client.Transport = &http.Transport{DialContext: f.dialChecked}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Name returns the tool name.
func (HTTPFetch) Name() string { return "http_fetch" }

// Description returns the tool description.
func (HTTPFetch) Description() string {
	return "发起 HTTP GET 请求并返回文本响应体，适合抓取网页或 API 文本内容"
}

// Parameters returns the parameter schema.
func (HTTPFetch) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"url":{"type":"string","description":"完整 URL"}},"required":["url"]}`)
}

// Execute fetches the URL.
// returns: the truncated text content
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
		req.Header.Set("User-Agent", "tt-agent/0.1")
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

// checkTarget is a fail-fast check before the request.
//
// The authoritative check lives in dialChecked (at dial time); this exists
// only to produce an immediate error message, so that IP-literal targets
// are not rejected only after a connection is established.
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
	// Resolve A/AAAA records for the domain; allow only if every address
	// is allowed
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

// dialChecked is the dial-time private-network check, closing the DNS
// rebinding TOCTOU window.
//
// DNS records may be switched between checkTarget's resolution and the
// actual dial (a rebinding attack), hence the authoritative check lives
// here: every resolved address of the domain must be allowed, then the
// first legal IP is pinned and dialed directly, bypassing http.Transport's
// second resolution. Only the dial address is changed; the URL is
// untouched, so TLS still performs SNI and certificate validation against
// the URL hostname.
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
		// Reject as a whole if any resolved address hits a private range:
		// rebinding commonly uses alternating public/private records with
		// extremely short TTLs to slip past spot checks
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

// isPrivateIP determines private/special network ranges.
//
// IsGlobalUnicast already covers most, but the 100.64/10 range common in
// dial-up VPNs intersects IsGlobalUnicast and must be excluded separately.
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

// hostOf extracts the host part of a URL (port stripped).
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
	// Strip the port, but leave IPv6 literal colons untouched
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

// resolveLocation resolves a redirect target into an absolute URL per
// RFC 3986.
//
// Location may be an absolute URL, an absolute path, a relative path, or
// "../" form; hand-concatenating scheme+host+location misses relative
// paths without a leading slash, so base-URL resolution must be used.
// returns: the absolute address or an error
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

// isRedirect determines whether a status code is a redirect.
func isRedirect(code int) bool {
	return code == 301 || code == 302 || code == 303 || code == 307 || code == 308
}
