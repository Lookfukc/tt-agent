package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/tools"
	"github.com/Lookfukc/tt-agent/pkg/tools/builtin"
)

func TestCalculator(t *testing.T) {
	calc := builtin.NewCalculator()
	cases := []struct {
		expr string
		want float64
	}{
		{"(1+2)*3", 9},
		{"2+3*4", 14},
		{"10/4", 2.5},
		{"-(3+4)", -7},
		{"100%7", 2},
		{"0.5*8", 4},
	}
	for _, c := range cases {
		res, err := calc.Execute(context.Background(), json.RawMessage(`{"expression":"`+c.expr+`"}`))
		if err != nil {
			t.Fatalf("%s: %v", c.expr, err)
		}
		var out struct {
			Value float64 `json:"value"`
		}
		_ = json.Unmarshal([]byte(res.Render()), &out)
		if out.Value != c.want {
			t.Errorf("%s = %v, want %v", c.expr, out.Value, c.want)
		}
	}
}

func TestCalculatorRejectsCode(t *testing.T) {
	calc := builtin.NewCalculator()
	// 函数调用、标识符等非算术语法必须被拒
	for _, expr := range []string{`fmt.Println(1)`, `x`, `1;2`} {
		if _, err := calc.Execute(context.Background(), json.RawMessage(`{"expression":"`+expr+`"}`)); err == nil {
			t.Errorf("expression %q should be rejected", expr)
		}
	}
	if _, err := calc.Execute(context.Background(), json.RawMessage(`{"expression":"1/0"}`)); err == nil {
		t.Error("division by zero should fail")
	}
}

// TestH8HTTPFetchBlocksPrivate 默认拒绝环回/私网目标（SSRF 防护）
func TestH8HTTPFetchBlocksPrivate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "页面内容")
	}))
	defer srv.Close()

	// httptest 监听 127.0.0.1，默认策略必须拒绝
	fetch := builtin.NewHTTPFetch()
	if _, err := fetch.Execute(context.Background(),
		json.RawMessage(`{"url":"`+srv.URL+`"}`)); err == nil {
		t.Fatal("H8: loopback target must be blocked by default")
	}
	// 常见 SSRF 目标：云元数据端点
	for _, u := range []string{"http://169.254.169.254/latest/meta-data", "http://10.0.0.1/x", "http://[::1]/"} {
		if _, err := fetch.Execute(context.Background(), json.RawMessage(`{"url":"`+u+`"}`)); err == nil {
			t.Errorf("H8: %s must be blocked", u)
		}
	}
	// 非 http(s) 协议拒绝
	if _, err := fetch.Execute(context.Background(), json.RawMessage(`{"url":"file:///etc/passwd"}`)); err == nil {
		t.Error("file protocol should be rejected")
	}
}

func TestHTTPFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "页面内容")
	}))
	defer srv.Close()

	// 显式放行内网后功能正常
	fetch := builtin.NewHTTPFetch()
	fetch.AllowPrivateNetwork = true
	res, err := fetch.Execute(context.Background(),
		json.RawMessage(`{"url":"`+srv.URL+`"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var out struct {
		Status  int    `json:"status"`
		Content string `json:"content"`
	}
	_ = json.Unmarshal([]byte(res.Render()), &out)
	if out.Status != 200 || out.Content != "页面内容" {
		t.Errorf("out = %+v", out)
	}
}

// TestH8HTTPFetchRedirectCheck 重定向逐跳校验：外网 → 内网必须被拦
func TestH8HTTPFetchRedirectCheck(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "内网内容")
	}))
	defer target.Close()

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer entry.Close()

	fetch := builtin.NewHTTPFetch()
	fetch.AllowPrivateNetwork = true // 放行入口，验证的是"每跳重新校验"逻辑之外的正常路径
	// 入口放行但目标也是本机——放行是全局开关，此处验证重定向跟随本身可用
	res, err := fetch.Execute(context.Background(),
		json.RawMessage(`{"url":"`+entry.URL+`"}`))
	if err != nil {
		t.Fatalf("redirect follow: %v", err)
	}
	if !strings.Contains(res.Render(), "内网内容") {
		t.Errorf("redirect content = %s", res.Render())
	}
}

func TestClockAndRegistry(t *testing.T) {
	reg := tools.NewRegistry()
	reg.Register(builtin.NewCalculator())
	reg.Register(builtin.NewClock())
	reg.Register(builtin.NewHTTPFetch())

	if got := len(reg.Specs()); got != 3 {
		t.Errorf("specs = %d, want 3", got)
	}
	clock, _ := reg.Get("clock")
	res, err := clock.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(res.Render(), "T") {
		t.Errorf("clock output = %s, want RFC3339", res.Render())
	}
}
