// Package provider provides multi-vendor model registration and configuration management.
package provider

import (
	"fmt"
	"os"
	"sync"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// ModelCapabilities is a model-level capability declaration.
type ModelCapabilities struct {
	Streaming          bool
	ToolCalls          bool
	Thinking           bool
	Vision             bool
	StructuredOutput   bool
	TemperatureSupport bool
	ContextWindow      int64 // number of tokens
}

// ModelConfig is the configuration and capabilities of a single model.
type ModelConfig struct {
	ID           string
	Name         string
	Capabilities ModelCapabilities
	// InputPricePerMtok is the price per million input tokens, used for cost accounting.
	InputPricePerMtok float64
	// OutputPricePerMtok is the price per million output tokens.
	OutputPricePerMtok float64
}

// ProviderConfig is the provider configuration.
type ProviderConfig struct {
	ID       string
	Name     string
	Protocol string // openai / anthropic / gemini
	BaseURL  string
	// APIKeyEnv is the name of the API key environment variable; keys are never stored in configuration.
	APIKeyEnv    string
	DefaultModel string
	Models       []ModelConfig
	// Quirks are protocol deviation corrections; see protocol.Quirks.
	Quirks protocol.Quirks
}

// SupportsModel reports whether the provider includes the specified model.
// returns: true means supported.
func (c *ProviderConfig) SupportsModel(model string) bool {
	_, ok := c.Model(model)
	return ok
}

// CostOf computes the cost of one usage according to pricing.
// u: the token usage; thinking tokens count toward the output side.
// returns: the cost in dollars; 0 when pricing is not configured.
func (m ModelConfig) CostOf(u core.Usage) float64 {
	cost := float64(u.InputTokens)/1_000_000*m.InputPricePerMtok +
		float64(u.OutputTokens+u.ReasoningTokens)/1_000_000*m.OutputPricePerMtok
	// Zero out anything below half a cent to avoid floating-point trailing noise.
	if cost < 0.005 {
		return 0
	}
	return cost
}

// Model looks up a model configuration.
// returns: the model configuration; ok is false when not registered.
func (c *ProviderConfig) Model(model string) (ModelConfig, bool) {
	for _, m := range c.Models {
		if m.ID == model {
			return m, true
		}
	}
	return ModelConfig{}, false
}

// Registry is the provider registry, safe for concurrent use.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]*ProviderConfig
}

// NewRegistry constructs an empty registry.
//
// Vendors are always registered by the calling code; the framework ships no
// built-in catalog and owns no configuration file format.
// returns: the empty registry.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]*ProviderConfig)}
}

// Register registers a provider; a duplicate ID overwrites.
func (r *Registry) Register(c *ProviderConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[c.ID] = c
}

// Get looks up a provider by ID.
// returns: the provider configuration; ok is false when not registered.
func (r *Registry) Get(id string) (*ProviderConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.providers[id]
	return c, ok
}

// MustGet looks up a provider by ID and panics when not registered; intended only for startup-time static assembly.
// returns: the provider configuration.
func (r *Registry) MustGet(id string) *ProviderConfig {
	c, ok := r.Get(id)
	if !ok {
		panic(fmt.Sprintf("provider not registered: %s", id))
	}
	return c
}

// List lists all provider IDs.
// returns: the list of IDs.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.providers))
	for id := range r.providers {
		ids = append(ids, id)
	}
	return ids
}

// apiKeys is a process-level key cache that avoids reading environment variables on every request.
var apiKeys sync.Map

// APIKey reads the provider's API key.
// returns: the key value; ok is false when it was not bound via SetAPIKey or an environment variable.
func (c *ProviderConfig) APIKey() (string, bool) {
	if v, ok := apiKeys.Load(c.ID); ok {
		return v.(string), true
	}
	// TODO: support loading from yaml / a secret management service
	return "", false
}

// SetAPIKey binds the key; effective within the process.
func (c *ProviderConfig) SetAPIKey(key string) {
	apiKeys.Store(c.ID, key)
}

// LoadAPIKeyFromEnv loads the key from an environment variable.
// returns: an error when loading fails.
func (c *ProviderConfig) LoadAPIKeyFromEnv() error {
	if c.APIKeyEnv == "" {
		return fmt.Errorf("provider %s has no APIKeyEnv", c.ID)
	}
	key := os.Getenv(c.APIKeyEnv)
	if key == "" {
		return fmt.Errorf("env %s not set for provider %s", c.APIKeyEnv, c.ID)
	}
	c.SetAPIKey(key)
	return nil
}

// ThinkingCapability queries whether a model supports thinking mode; the capability belongs to the model, not the LLM interface.
// model: the model ID.
// returns: true means supported.
func (c *ProviderConfig) ThinkingCapability(model string) bool {
	m, ok := c.Model(model)
	return ok && m.Capabilities.Thinking
}
