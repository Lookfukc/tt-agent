package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/tools/builtin"
)

// TestRound2N14CalculatorRejectsNonFinite 非有限结果必须显式报错
//
// 原 bug：溢出得到 Inf/NaN 时静默返回空串，模型拿到空值继续推理
func TestRound2N14CalculatorRejectsNonFinite(t *testing.T) {
	calc := builtin.NewCalculator()
	// 1e308*10 溢出为 +Inf；Inf-Inf 为 NaN
	for _, expr := range []string{"1e308*10", "(1e308*10)-(1e308*10)"} {
		_, err := calc.Execute(context.Background(), json.RawMessage(`{"expression":"`+expr+`"}`))
		if err == nil {
			t.Errorf("N14: %s should fail, got success", expr)
			continue
		}
		if !strings.Contains(err.Error(), "finite") {
			t.Errorf("N14: %s err = %v, want non-finite result message", expr, err)
		}
	}
}

// TestRound2N7RelativeRedirectLocation 无前导斜杠的相对 Location 须按 RFC 3986 解析
//
// 原 bug：scheme+"://"+host+location 拼接，"next-page" 会被接到端口后
// 变成非法 URL
func TestRound2N7RelativeRedirectLocation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dir/page":
			w.Header().Set("Location", "next-page") // 无前导 '/'
			w.WriteHeader(http.StatusFound)
		case "/dir/next-page":
			fmt.Fprint(w, "相对页内容")
		case "/deep/a/b":
			w.Header().Set("Location", "../../root-page")
			w.WriteHeader(http.StatusFound)
		case "/root-page":
			fmt.Fprint(w, "根页内容")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	fetch := builtin.NewHTTPFetchWithOptions(builtin.WithAllowPrivateTargets(true))
	for _, tc := range []struct {
		start     string
		wantPath  string
		wantChunk string
	}{
		{"/dir/page", "/dir/next-page", "相对页内容"},
		{"/deep/a/b", "/root-page", "根页内容"},
	} {
		res, err := fetch.Execute(context.Background(),
			json.RawMessage(`{"url":"`+srv.URL+tc.start+`"}`))
		if err != nil {
			t.Fatalf("N7: %s: %v", tc.start, err)
		}
		if !strings.Contains(res.Render(), tc.wantChunk) {
			t.Errorf("N7: %s content = %s, want %q", tc.start, res.Render(), tc.wantChunk)
		}
	}
}

// TestRound2N7RedirectSchemeRejected 重定向到非 http(s) 协议必须被拒
func TestRound2N7RedirectSchemeRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			w.Header().Set("Location", "ftp://evil.example/x")
			w.WriteHeader(http.StatusFound)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	fetch := builtin.NewHTTPFetchWithOptions(builtin.WithAllowPrivateTargets(true))
	_, err := fetch.Execute(context.Background(),
		json.RawMessage(`{"url":"`+srv.URL+`/start"}`))
	if err == nil {
		t.Fatal("N7: non-http redirect target must be rejected")
	}
	if !strings.Contains(err.Error(), "scheme") {
		t.Errorf("N7: err = %v, want scheme rejection message", err)
	}
}

// TestRound2H8DialTimePrivateBlock 私网拦截必须发生在拨号之前
//
// 即便目标端口上没有任何监听，错误也应明确说 blocked 而非
// connection refused / timeout，证明请求从未真正发出
func TestRound2H8DialTimePrivateBlock(t *testing.T) {
	fetch := builtin.NewHTTPFetch() // 默认严格
	for _, u := range []string{
		"http://127.0.0.1:1/", // 环回 + 无监听端口
		"http://169.254.169.254/latest/meta-data",
		"http://10.0.0.1:8080/",
	} {
		_, err := fetch.Execute(context.Background(), json.RawMessage(`{"url":"`+u+`"}`))
		if err == nil {
			t.Errorf("H8: %s must be blocked", u)
			continue
		}
		if !strings.Contains(err.Error(), "blocked") {
			t.Errorf("H8: %s err = %v, want blocked message rather than dial error", u, err)
		}
	}
}

// TestRound2H8AllowPrivateOption 放行选项走自定义拨号路径仍可正常抓取
//
// 新增拨号期校验不能破坏 AllowPrivateNetwork 放行场景的功能
func TestRound2H8AllowPrivateOption(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "本机内容")
	}))
	defer srv.Close()

	fetch := builtin.NewHTTPFetchWithOptions(builtin.WithAllowPrivateTargets(true))
	res, err := fetch.Execute(context.Background(),
		json.RawMessage(`{"url":"`+srv.URL+`"}`))
	if err != nil {
		t.Fatalf("H8: %v", err)
	}
	if !strings.Contains(res.Render(), "本机内容") {
		t.Errorf("H8: content = %s", res.Render())
	}
	// 默认构造保持严格：同一 URL 必须被拒
	strict := builtin.NewHTTPFetch()
	if _, err := strict.Execute(context.Background(),
		json.RawMessage(`{"url":"`+srv.URL+`"}`)); err == nil {
		t.Error("H8: default constructor must stay strict")
	}
}
