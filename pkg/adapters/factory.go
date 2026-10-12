// Package adapters provides the assembly entry point for protocol adapters.
package adapters

import (
	"context"
	"fmt"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// NewLLM assembles an LLM instance from the provider configuration.
// cfg: provider configuration; the API key must already be bound via SetAPIKey or LoadAPIKeyFromEnv.
// returns: an adapter implementing the core LLM interface; returns an error if the protocol is unsupported or the key is missing.
func NewLLM(cfg *provider.ProviderConfig) (core.LLM, error) {
	key, ok := cfg.APIKey()
	if !ok || key == "" {
		return nil, fmt.Errorf("api key not configured for provider %s", cfg.ID)
	}
	var llm core.LLM
	switch cfg.Protocol {
	case "openai":
		llm = protocol.NewOpenAI(cfg.ID, cfg.BaseURL, key, cfg.Quirks)
	case "anthropic":
		llm = protocol.NewAnthropic(cfg.ID, cfg.BaseURL, key, cfg.Quirks)
	case "gemini":
		llm = protocol.NewGemini(cfg.ID, cfg.BaseURL, key, cfg.Quirks)
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", cfg.Protocol)
	}
	// Requests without an explicit model get the provider default.
	// Strict providers (DeepSeek among them) reject unknown model
	// names instead of ignoring them, so "no model given" must resolve
	// here rather than travel to the wire.
	if cfg.DefaultModel != "" {
		llm = &defaultModelLLM{LLM: llm, model: cfg.DefaultModel}
	}
	return llm, nil
}

// defaultModelLLM fills an empty ChatRequest.Model with the provider's
// default. Both entry points must agree: a request that streams and
// one that does not resolve to the same model.
type defaultModelLLM struct {
	core.LLM
	model string
}

// Chat implements core.LLM with default-model resolution.
func (d *defaultModelLLM) Chat(ctx context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	if req.Model == "" {
		req.Model = d.model
	}
	return d.LLM.Chat(ctx, req)
}

// ChatStream implements core.LLM with default-model resolution.
func (d *defaultModelLLM) ChatStream(ctx context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	if req.Model == "" {
		req.Model = d.model
	}
	return d.LLM.ChatStream(ctx, req)
}

// NewLLMFromRegistry is a convenience assembly helper: it fetches the configuration by ID from the registry and builds the LLM.
// r: the provider registry.
// id: the provider ID.
// returns: an LLM instance.
func NewLLMFromRegistry(r *provider.Registry, id string) (core.LLM, error) {
	cfg, ok := r.Get(id)
	if !ok {
		return nil, fmt.Errorf("provider not registered: %s", id)
	}
	return NewLLM(cfg)
}
