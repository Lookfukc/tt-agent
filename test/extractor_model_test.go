package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/adapters"
	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/ltm"
	"github.com/Lookfukc/tt-agent/pkg/ltm/extractor"
)

// TestEmptyModelResolvesToDefault is the regression guard for a bug the
// demo consumer found: the fact extractor hardcoded Model:"extractor",
// and strict providers (DeepSeek: "The supported API model names are
// ..., but you passed extractor") rejected the request with a 400.
//
// Contract under test: a ChatRequest with an empty Model resolves to
// the provider's DefaultModel at the adapter layer, and the extractor
// relies on exactly that.
func TestEmptyModelResolvesToDefault(t *testing.T) {
	var mu sync.Mutex
	var gotModels []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		gotModels = append(gotModels, body.Model)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"[\"fact\"]"}}]}`))
	}))
	defer srv.Close()

	cfg := &provider.ProviderConfig{
		ID: "capture", Protocol: "openai",
		BaseURL:      srv.URL,
		DefaultModel: "test-default",
		Models:       []provider.ModelConfig{{ID: "test-default"}},
	}
	cfg.SetAPIKey("k")
	llm, err := adapters.NewLLM(cfg)
	if err != nil {
		t.Fatalf("NewLLM: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1) 空模型 → 填默认
	if _, err := llm.Chat(ctx, core.ChatRequest{Messages: []core.Message{
		{Role: core.RoleUser, Content: "hi"}}}); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	// 2) 显式模型 → 原样透传
	if _, err := llm.Chat(ctx, core.ChatRequest{Model: "explicit", Messages: []core.Message{
		{Role: core.RoleUser, Content: "hi"}}}); err != nil {
		t.Fatalf("Chat explicit: %v", err)
	}
	// 3) 抽取器端到端：不再发 "extractor"，发的是默认模型
	if _, err := extractor.New(llm, extractor.Options{}).Extract(ctx, []ltm.Message{
		{Role: "user", Content: "I like Go"},
	}); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(gotModels) != 3 {
		t.Fatalf("captured %d requests, want 3: %v", len(gotModels), gotModels)
	}
	if gotModels[0] != "test-default" {
		t.Fatalf("empty model not defaulted: %q", gotModels[0])
	}
	if gotModels[1] != "explicit" {
		t.Fatalf("explicit model not passed through: %q", gotModels[1])
	}
	if gotModels[2] != "test-default" {
		t.Fatalf("extractor sent %q, want test-default (no more \"extractor\" on the wire)", gotModels[2])
	}
}
