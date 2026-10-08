package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// probeBody starts an HTTP server that captures the request body and replies with a fixed response.
//
// The map must be pre-allocated: unmarshaling into a nil map inside the
// closure would allocate a fresh map visible only to the closure, leaving
// the caller with nil
func probeBody(t *testing.T, reply string) (map[string]any, *httptest.Server) {
	t.Helper()
	got := make(map[string]any)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &got)
		fmt.Fprint(w, reply)
	}))
	return got, srv
}

func TestOpenAIResponseFormat(t *testing.T) {
	got, srv := probeBody(t, `{"choices":[{"message":{"content":"{}"}}]}`)
	defer srv.Close()

	p := protocol.NewOpenAI("t", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:          "m",
		ResponseFormat: &core.ResponseFormat{Name: "user", Schema: []byte(`{"type":"object"}`)},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	rf := got["response_format"].(map[string]any)
	if rf["type"] != "json_schema" {
		t.Errorf("type = %v", rf["type"])
	}
	js := rf["json_schema"].(map[string]any)
	if js["name"] != "user" || js["strict"] != true {
		t.Errorf("json_schema = %v", js)
	}
}

func TestGeminiResponseFormat(t *testing.T) {
	got, srv := probeBody(t, `{"candidates":[{"content":{"parts":[{"text":"{}"}]}}]}`)
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:          "m",
		ResponseFormat: &core.ResponseFormat{Schema: []byte(`{"type":"object"}`)},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	gen := got["generationConfig"].(map[string]any)
	if gen["responseMimeType"] != "application/json" {
		t.Errorf("responseMimeType = %v", gen["responseMimeType"])
	}
	if gen["responseSchema"] == nil {
		t.Error("responseSchema missing")
	}
}

func TestResponseFormatAbsent(t *testing.T) {
	got, srv := probeBody(t, `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`)
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "k", protocol.Quirks{})
	if _, err := p.Chat(context.Background(), core.ChatRequest{Model: "m"}); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if got["generationConfig"] != nil {
		t.Errorf("generationConfig should be absent, got %v", got["generationConfig"])
	}
}
