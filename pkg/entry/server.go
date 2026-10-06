// Package entry 提供 HTTP/SSE 入口层，将请求转接给 Agent 循环
package entry

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/Lookfukc/send-agent/pkg/adapters"
	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/core"
	"github.com/Lookfukc/send-agent/pkg/memory"
	"github.com/Lookfukc/send-agent/pkg/observer"
	"github.com/Lookfukc/send-agent/pkg/tools"
)

// Config 服务配置
type Config struct {
	Addr            string
	DefaultProvider string
	DefaultModel    string
	SystemPrompt    string
	MaxIterations   int
	TokenBudget     int64
}

// defaultConfig 未填字段的缺省值
func defaultConfig() Config {
	return Config{
		Addr:          ":8080",
		MaxIterations: 16,
		TokenBudget:   32_000,
	}
}

// LLMFactory 按 providerID 装配 LLM，测试注入 mock 用
type LLMFactory func(providerID string) (core.LLM, error)

// Server HTTP 入口
type Server struct {
	cfg      Config
	registry *provider.Registry
	mem      core.Memory
	tools    *tools.Registry
	newLLM   LLMFactory
	metrics  *observer.Metrics

	mu   sync.RWMutex
	llms map[string]core.LLM

	// 劫持的 WS 连接不在 http.Server.Shutdown 管辖内，
	// 需自行登记才能在优雅关闭时收尾
	wsMu   sync.Mutex
	wsCons map[*wsConn]struct{}
}

// NewServer 构造服务
// registry: 提供商注册表
// toolReg: 工具注册表，可为 nil
// opts: 覆盖默认配置
// returns: 服务实例
func NewServer(registry *provider.Registry, toolReg *tools.Registry, opts ...Option) *Server {
	cfg := defaultConfig()
	s := &Server{
		cfg:      cfg,
		registry: registry,
		mem:      memory.NewBuffer(nil),
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

// Option 服务配置项
type Option func(*Server)

// WithConfig 覆盖服务配置
func WithConfig(cfg Config) Option {
	return func(s *Server) { s.cfg = cfg }
}

// WithMemory 替换记忆实现
func WithMemory(mem core.Memory) Option {
	return func(s *Server) { s.mem = mem }
}

// WithLLMFactory 替换 LLM 装配，测试注入点
func WithLLMFactory(f LLMFactory) Option {
	return func(s *Server) { s.newLLM = f }
}

// Handler 返回路由挂载后的 http.Handler
// returns: 可直接交给 http.Server 的处理器
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/providers", s.handleProviders)
	mux.HandleFunc("GET /api/metrics", s.handleMetrics)
	mux.HandleFunc("POST /api/chat", s.handleChat)
	mux.HandleFunc("GET /api/chat/ws", s.handleChatWS)
	return mux
}

// Metrics 暴露指标聚合器，供外部采集
// returns: 聚合器实例
func (s *Server) Metrics() *observer.Metrics {
	return s.metrics
}

// trackWS 登记 WS 连接
func (s *Server) trackWS(ws *wsConn) {
	s.wsMu.Lock()
	s.wsCons[ws] = struct{}{}
	s.wsMu.Unlock()
}

// untrackWS 注销 WS 连接
func (s *Server) untrackWS(ws *wsConn) {
	s.wsMu.Lock()
	delete(s.wsCons, ws)
	s.wsMu.Unlock()
}

// CloseWebSockets 关闭全部在途 WS 连接
//
// 劫持后的连接不随 http.Server.Shutdown 结束，进程退出前需显式关闭
func (s *Server) CloseWebSockets() {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for ws := range s.wsCons {
		_ = ws.Close()
	}
	s.wsCons = make(map[*wsConn]struct{})
}

// llmFor 取缓存的 LLM 实例，无则装配
// providerID: 提供商标识
// returns: 包装了中间件链的 LLM
func (s *Server) llmFor(providerID string) (core.LLM, error) {
	s.mu.RLock()
	llm, ok := s.llms[providerID]
	s.mu.RUnlock()
	if ok {
		return llm, nil
	}

	// TODO: Retry 参数与预算应可按提供商覆盖
	bare, err := s.newLLM(providerID)
	if err != nil {
		return nil, err
	}
	// 日志挂 LLM 级中间件：服务端主路径（Agent 循环）恒为流式，
	// 只挂 ChatMiddleware 的日志对流式调用完全不生效
	pipeline := core.NewPipeline(bare, core.Retry(3))
	wrapped := core.StreamRetry(
		core.LoggingLLM(nil)(pipeline), 3,
	)
	s.mu.Lock()
	s.llms[providerID] = wrapped
	s.mu.Unlock()
	return wrapped, nil
}

// resolveModel 确定本次请求使用的提供商与模型
// returns: 提供商配置与模型 ID
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

// loop 构建 Agent 循环
// returns: 装配好的循环
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
