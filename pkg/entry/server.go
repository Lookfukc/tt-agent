// Package entry provides the HTTP/SSE entry layer, forwarding requests
// into the Agent loop.
package entry

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/Lookfukc/tt-agent/pkg/adapters"
	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/observer"
	"github.com/Lookfukc/tt-agent/pkg/tools"
)

// Config is the server configuration.
type Config struct {
	Addr            string
	DefaultProvider string
	DefaultModel    string
	SystemPrompt    string
	MaxIterations   int
	TokenBudget     int64
}

// defaultConfig returns defaults for unset fields.
func defaultConfig() Config {
	return Config{
		Addr:          ":8080",
		MaxIterations: 16,
		TokenBudget:   32_000,
	}
}

// LLMFactory assembles an LLM by providerID; used by tests to inject mocks.
type LLMFactory func(providerID string) (core.LLM, error)

// Server is the HTTP entry point.
type Server struct {
	cfg      Config
	registry *provider.Registry
	mem      core.Memory
	tools    *tools.Registry
	newLLM   LLMFactory
	metrics  *observer.Metrics

	mu   sync.RWMutex
	llms map[string]core.LLM

	// Hijacked WS connections are outside http.Server.Shutdown's
	// purview; they must be tracked manually so they can be wrapped
	// up during graceful shutdown.
	wsMu   sync.Mutex
	wsCons map[*wsConn]struct{}
}

// NewServer constructs the server.
// registry: the provider registry.
// toolReg: the tool registry; may be nil.
// opts: overrides for the default configuration.
// returns: the server instance.
func NewServer(registry *provider.Registry, toolReg *tools.Registry, opts ...Option) *Server {
	cfg := defaultConfig()
	s := &Server{
		cfg:      cfg,
		registry: registry,
		mem:      defaultMemory(),
		tools:    toolReg,
		llms:     make(map[string]core.LLM),
		metrics:  observer.NewMetrics(),
		wsCons:   make(map[*wsConn]struct{}),
	}
	s.newLLM = func(providerID string) (core.LLM, error) {
		return adapters.NewLLMFromRegistry(registry, providerID)
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.tools == nil {
		s.tools = tools.NewRegistry()
	}
	return s
}

// Option is a server configuration option.
type Option func(*Server)

// WithConfig overrides the server configuration.
func WithConfig(cfg Config) Option {
	return func(s *Server) { s.cfg = cfg }
}

// WithMemory replaces the memory implementation.
func WithMemory(mem core.Memory) Option {
	return func(s *Server) { s.mem = mem }
}

// DefaultMemoryDir returns the default session memory directory.
//
// The fixed subdirectory guarantees sessions survive restarts on the
// same machine; when multiple distinct applications on the same
// machine share the default, they share this directory — for
// production deployments, use WithMemory to designate a dedicated one.
func DefaultMemoryDir() string {
	return filepath.Join(os.TempDir(), "tt-agent-sessions")
}

// defaultMemLoaded is the cap on resident sessions in default memory.
//
// Memory usage depends only on the number of "concurrently active
// sessions", not on the total volume of historical sessions.
const defaultMemLoaded = 1024

// defaultMemory builds the default memory: temp-dir persistence + an
// LRU residency cap.
//
// Process memory is no longer the default (lost on restart, unbounded
// growth); if the temp directory is unusable we panic outright — this
// is an environment error, and exposing it at assembly time beats
// silent degradation at run time.
func defaultMemory() core.Memory {
	p, err := memory.NewPersistentWithLRU(DefaultMemoryDir(), nil, defaultMemLoaded)
	if err != nil {
		panic("tt-agent: init default memory dir: " + err.Error())
	}
	return p
}

// WithLLMFactory replaces LLM assembly; a test injection point.
func WithLLMFactory(f LLMFactory) Option {
	return func(s *Server) { s.newLLM = f }
}

// Handler returns the http.Handler with routes mounted.
// returns: a handler ready to hand to http.Server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/providers", s.handleProviders)
	mux.HandleFunc("GET /api/metrics", s.handleMetrics)
	mux.HandleFunc("POST /api/chat", s.handleChat)
	mux.HandleFunc("GET /api/chat/ws", s.handleChatWS)
	return mux
}

// Metrics exposes the metrics aggregator for external collection.
// returns: the aggregator instance.
func (s *Server) Metrics() *observer.Metrics {
	return s.metrics
}

// trackWS registers a WS connection.
func (s *Server) trackWS(ws *wsConn) {
	s.wsMu.Lock()
	s.wsCons[ws] = struct{}{}
	s.wsMu.Unlock()
}

// untrackWS unregisters a WS connection.
func (s *Server) untrackWS(ws *wsConn) {
	s.wsMu.Lock()
	delete(s.wsCons, ws)
	s.wsMu.Unlock()
}

// CloseWebSockets closes all in-flight WS connections.
//
// Hijacked connections do not end with http.Server.Shutdown; they must
// be closed explicitly before process exit.
func (s *Server) CloseWebSockets() {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for ws := range s.wsCons {
		_ = ws.Close()
	}
	s.wsCons = make(map[*wsConn]struct{})
}

// llmFor returns the cached LLM instance, assembling one if absent.
// providerID: the provider identifier.
// returns: the LLM wrapped with the middleware chain.
func (s *Server) llmFor(providerID string) (core.LLM, error) {
	s.mu.RLock()
	llm, ok := s.llms[providerID]
	s.mu.RUnlock()
	if ok {
		return llm, nil
	}

	// TODO: Retry parameters and budget should be overridable per provider
	bare, err := s.newLLM(providerID)
	if err != nil {
		return nil, err
	}
	// Logging is attached as an LLM-level middleware: the server's
	// main path (the Agent loop) is always streaming, and logging
	// attached only via ChatMiddleware would have no effect on
	// streaming calls at all.
	pipeline := core.NewPipeline(bare, core.Retry(3))
	wrapped := core.StreamRetry(
		core.LoggingLLM(nil)(pipeline), 3,
	)
	s.mu.Lock()
	s.llms[providerID] = wrapped
	s.mu.Unlock()
	return wrapped, nil
}

// resolveModel determines the provider and model to use for this request.
// returns: the provider config and model ID.
func (s *Server) resolveModel(providerID, model string) (*provider.ProviderConfig, string, error) {
	if providerID == "" {
		providerID = s.cfg.DefaultProvider
	}
	cfg, ok := s.registry.Get(providerID)
	if !ok {
		return nil, "", fmt.Errorf("unknown provider: %s", providerID)
	}
	if model == "" {
		model = s.cfg.DefaultModel
	}
	if model == "" {
		model = cfg.DefaultModel
	}
	if !cfg.SupportsModel(model) {
		return nil, "", fmt.Errorf("provider %s has no model %s", providerID, model)
	}
	return cfg, model, nil
}

// loop builds the Agent loop.
// returns: the assembled loop.
func (s *Server) loop(llm core.LLM, model string, systemPrompt string, onEvent func(agent.LoopEvent)) *agent.Loop {
	if systemPrompt == "" {
		systemPrompt = s.cfg.SystemPrompt
	}
	return agent.NewLoop(llm, s.tools, s.mem, agent.Config{
		Model:         model,
		SystemPrompt:  systemPrompt,
		MaxIterations: s.cfg.MaxIterations,
		TokenBudget:   s.cfg.TokenBudget,
		OnEvent:       onEvent,
	})
}

// statelessLoop builds a stateless Agent loop.
//
// No session memory is attached; paired with RunWithHistory: the
// caller brings the full history and the server keeps no session
// state at all.
// returns: the assembled loop.
func (s *Server) statelessLoop(llm core.LLM, model string, systemPrompt string, onEvent func(agent.LoopEvent)) *agent.Loop {
	if systemPrompt == "" {
		systemPrompt = s.cfg.SystemPrompt
	}
	return agent.NewLoop(llm, s.tools, nil, agent.Config{
		Model:         model,
		SystemPrompt:  systemPrompt,
		MaxIterations: s.cfg.MaxIterations,
		TokenBudget:   s.cfg.TokenBudget,
		OnEvent:       onEvent,
	})
}
