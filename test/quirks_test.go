package test

import (
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

func TestComposeQuirks(t *testing.T) {
	// A nil list returns the zero value.
	q, err := provider.ComposeQuirks(nil, "openai")
	if err != nil {
		t.Fatalf("nil names: %v", err)
	}
	if q.PatchRequest != nil {
		t.Error("nil names should compose zero quirks")
	}

	// Single patch: GLM thinking private field injection.
	q, err = provider.ComposeQuirks([]string{"glm-thinking"}, "openai")
	if err != nil {
		t.Fatalf("glm-thinking: %v", err)
	}
	body := map[string]any{"temperature": 0.7}
	q.PatchRequest(body, core.ChatRequest{Thinking: &core.ThinkingConfig{Enabled: true}})
	th, ok := body["thinking"].(map[string]any)
	if !ok || th["type"] != "enabled" {
		t.Errorf("thinking = %#v", body["thinking"])
	}

	// Composition: both patches run in declaration order.
	q, err = provider.ComposeQuirks([]string{"glm-thinking", "deepseek-reasoner"}, "openai")
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	body = map[string]any{"temperature": 0.7, "top_p": 0.9}
	q.PatchRequest(body, core.ChatRequest{Model: "deepseek-reasoner", Thinking: &core.ThinkingConfig{Enabled: true}})
	if th, ok := body["thinking"].(map[string]any); !ok || th["type"] != "enabled" {
		t.Errorf("thinking = %#v", body["thinking"])
	}
	if _, exists := body["temperature"]; exists {
		t.Error("temperature should be stripped for deepseek-reasoner")
	}
	if _, exists := body["top_p"]; exists {
		t.Error("top_p should be stripped for deepseek-reasoner")
	}

	// Non-reasoner models are unaffected by sampling-parameter stripping.
	body = map[string]any{"temperature": 0.7}
	q.PatchRequest(body, core.ChatRequest{Model: "deepseek-chat"})
	if _, exists := body["temperature"]; !exists {
		t.Error("temperature should survive for deepseek-chat")
	}

	// An unknown name errors and lists available names.
	if _, err := provider.ComposeQuirks([]string{"bogus"}, "openai"); err == nil ||
		!strings.Contains(err.Error(), "glm-thinking") {
		t.Errorf("unknown quirk error = %v", err)
	}

	// A protocol mismatch errors.
	if _, err := provider.ComposeQuirks([]string{"glm-thinking"}, "anthropic"); err == nil {
		t.Error("quirk on wrong protocol should fail")
	}

	names := provider.QuirkNames()
	if len(names) != 2 || names[0] != "deepseek-reasoner" || names[1] != "glm-thinking" {
		t.Errorf("QuirkNames = %v", names)
	}
}
