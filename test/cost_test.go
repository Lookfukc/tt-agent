package test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

func TestCostOf(t *testing.T) {
	m := provider.ModelConfig{InputPricePerMtok: 4, OutputPricePerMtok: 16}
	// 1M input + 0.5M output = 4 + 8 = 12 USD.
	cost := m.CostOf(core.Usage{InputTokens: 1_000_000, OutputTokens: 500_000})
	if cost < 11.99 || cost > 12.01 {
		t.Errorf("cost = %f, want ~12", cost)
	}
	// Reasoning tokens are billed on the output side.
	cost = m.CostOf(core.Usage{OutputTokens: 250_000, ReasoningTokens: 250_000})
	if cost < 7.99 || cost > 8.01 {
		t.Errorf("cost with reasoning = %f, want ~8", cost)
	}
	if (provider.ModelConfig{}).CostOf(core.Usage{InputTokens: 100}) != 0 {
		t.Error("unpriced model should be 0")
	}
}

func TestEntryCostField(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/api/chat", "application/json",
		strings.NewReader(`{"input":"hi"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["cost_usd"]; !ok {
		t.Errorf("cost_usd missing: %v", out)
	}
}
