package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/tools/builtin"
)

// TestRound2N14CalculatorRejectsNonFinite: non-finite results must produce an explicit error
//
// Original bug: overflow to Inf/NaN silently returned an empty string, and the
// model carried on reasoning with an empty value
func TestRound2N14CalculatorRejectsNonFinite(t *testing.T) {
	calc := builtin.NewCalculator()
	// 1e308*10 overflows to +Inf; Inf-Inf is NaN
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

// TestRound2N7RelativeRedirectLocation: a relative Location without a leading slash must be resolved per RFC 3986
//
// Original bug: concatenating scheme+"://"+host+location appended "next-page"
// right after the port, producing an invalid URL
func TestRound2N7RelativeRedirectLocation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dir/page":
			w.Header().Set("Location", "next-page") // no leading '/'
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

// TestRound2N7RedirectSchemeRejected verifies redirects to non-http(s) schemes must be rejected
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

// TestRound2H8DialTimePrivateBlock: the private-network block must happen before dialing
//
// Even with nothing listening on the target port, the error should clearly say
// blocked rather than connection refused / timeout, proving the request was
// never actually sent
func TestRound2H8DialTimePrivateBlock(t *testing.T) {
	fetch := builtin.NewHTTPFetch() // strict by default
	for _, u := range []string{
		"http://127.0.0.1:1/", // loopback + no listener on the port
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

// TestRound2H8AllowPrivateOption: with the allow option, fetching still works normally through the custom dial path
//
// The new dial-time check must not break the AllowPrivateNetwork allow scenario
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
	// The default constructor stays strict: the same URL must be rejected
	strict := builtin.NewHTTPFetch()
	if _, err := strict.Execute(context.Background(),
		json.RawMessage(`{"url":"`+srv.URL+`"}`)); err == nil {
		t.Error("H8: default constructor must stay strict")
	}
}
