// Package adapters provides the assembly entry point for protocol adapters.
package adapters

import (
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
	switch cfg.Protocol {
	case "openai":
		return protocol.NewOpenAI(cfg.ID, cfg.BaseURL, key, cfg.Quirks), nil
	case "anthropic":
		return protocol.NewAnthropic(cfg.ID, cfg.BaseURL, key, cfg.Quirks), nil
	case "gemini":
		return protocol.NewGemini(cfg.ID, cfg.BaseURL, key, cfg.Quirks), nil
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", cfg.Protocol)
	}
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
