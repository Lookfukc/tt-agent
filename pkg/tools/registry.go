// Package tools provides tool registration and lookup.
package tools

import (
	"fmt"
	"sort"
	"sync"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// Registry is a concurrency-safe tool registry.
type Registry struct {
	mu    sync.RWMutex
	tools map[string]core.Tool
}

// NewRegistry constructs an empty registry.
// returns: a usable registry instance
func NewRegistry() *Registry {
	return &Registry{tools: make(map[string]core.Tool)}
}

// Register registers a tool; duplicate names overwrite.
func (r *Registry) Register(t core.Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[t.Name()] = t
}

// Get looks up a tool by name.
// returns: the tool instance; ok is false if not registered
func (r *Registry) Get(name string) (core.Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	return t, ok
}

// Specs outputs the definitions of all tools, for assembling ChatRequest.
// returns: a list of tool definitions, stably sorted by name
func (r *Registry) Specs() []core.ToolSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	specs := make([]core.ToolSpec, 0, len(r.tools))
	for _, t := range r.tools {
		specs = append(specs, core.ToolSpec{
			Name: t.Name(), Description: t.Description(), Parameters: t.Parameters(),
		})
	}
	// Map iteration order is random: without sorting, the tool definition
	// order would differ on every request, defeating providers' prefix-based
	// prompt caching mechanisms entirely
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

// MustGet looks up a tool by name, returning an error instead of panicking
// when unregistered, so that a hallucinated tool name from the model does
// not abort the whole loop.
// returns: the tool instance
func (r *Registry) MustGet(name string) (core.Tool, error) {
	t, ok := r.Get(name)
	if !ok {
		return nil, fmt.Errorf("tool not registered: %s", name)
	}
	return t, nil
}
