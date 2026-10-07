// Package provider 提供多厂商模型注册与配置管理能力
package provider

import (
	"fmt"
	"os"
	"sync"

	"github.com/Lookfukc/send-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/send-agent/pkg/core"
)

// ModelCapabilities 模型级能力声明
type ModelCapabilities struct {
	Streaming          bool
	ToolCalls          bool
	Thinking           bool
	Vision             bool
	StructuredOutput   bool
	TemperatureSupport bool
	ContextWindow      int64 // token 数
}

// ModelConfig 单个模型的配置与能力
type ModelConfig struct {
	ID           string
	Name         string
	Capabilities ModelCapabilities
	// InputPricePerMtok 每百万输入 token 价格，用于成本核算
	InputPricePerMtok float64
	// OutputPricePerMtok 每百万输出 token 价格
	OutputPricePerMtok float64
}

// ProviderConfig 提供商配置
type ProviderConfig struct {
	ID       string
	Name     string
	Protocol string // openai / anthropic / gemini
	BaseURL  string
	// APIKeyEnv 密钥环境变量名，密钥不落配置
	APIKeyEnv    string
	DefaultModel string
	Models       []ModelConfig
	// Quirks 协议偏差修正，见 protocol.Quirks
	Quirks protocol.Quirks
}

// SupportsModel 判断提供商是否含指定模型
// returns: true 表示支持
func (c *ProviderConfig) SupportsModel(model string) bool {
	_, ok := c.Model(model)
	return ok
}

// CostOf 按定价计算一次用量的成本
// u: token 用量，思考 token 计入输出侧
// returns: 美元成本，未配置定价时为 0
func (m ModelConfig) CostOf(u core.Usage) float64 {
	cost := float64(u.InputTokens)/1_000_000*m.InputPricePerMtok +
		float64(u.OutputTokens+u.ReasoningTokens)/1_000_000*m.OutputPricePerMtok
	// 半美分以下归零，避免浮点尾噪
	if cost < 0.005 {
		return 0
	}
	return cost
}

// Model 查找模型配置
// returns: 模型配置；ok 为 false 表示未注册
func (c *ProviderConfig) Model(model string) (ModelConfig, bool) {
	for _, m := range c.Models {
		if m.ID == model {
			return m, true
		}
	}
	return ModelConfig{}, false
}

// Registry 提供商注册表，并发安全
type Registry struct {
	mu        sync.RWMutex
	providers map[string]*ProviderConfig
}

// NewRegistry 构造空注册表
//
// 厂商一律由使用方代码注册，框架不内置目录也不拥有配置文件格式
// returns: 空注册表
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]*ProviderConfig)}
}

// Register 注册提供商，ID 重复时覆盖
func (r *Registry) Register(c *ProviderConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[c.ID] = c
}

// Get 按 ID 查找提供商
// returns: 提供商配置；ok 为 false 表示未注册
func (r *Registry) Get(id string) (*ProviderConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.providers[id]
	return c, ok
}

// MustGet 按 ID 查找提供商，未注册时 panic，仅用于启动期静态装配
// returns: 提供商配置
func (r *Registry) MustGet(id string) *ProviderConfig {
	c, ok := r.Get(id)
	if !ok {
		panic(fmt.Sprintf("provider not registered: %s", id))
	}
	return c
}

// List 列出全部提供商 ID
// returns: ID 列表
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.providers))
	for id := range r.providers {
		ids = append(ids, id)
	}
	return ids
}

// apiKeys 进程级密钥缓存，避免每次请求读环境变量
var apiKeys sync.Map

// APIKey 读取提供商密钥
// returns: 密钥值；ok 为 false 表示未通过 SetAPIKey 或环境变量绑定
func (c *ProviderConfig) APIKey() (string, bool) {
	if v, ok := apiKeys.Load(c.ID); ok {
		return v.(string), true
	}
	// TODO: 支持从 yaml/密钥管理服务加载
	return "", false
}

// SetAPIKey 绑定密钥，进程内生效
func (c *ProviderConfig) SetAPIKey(key string) {
	apiKeys.Store(c.ID, key)
}

// LoadAPIKeyFromEnv 从环境变量加载密钥
// returns: 加载失败时返回错误
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

// ThinkingCapability 查询模型是否支持思考模式，能力归属模型而非 LLM 接口
// model: 模型 ID
// returns: true 表示支持
func (c *ProviderConfig) ThinkingCapability(model string) bool {
	m, ok := c.Model(model)
	return ok && m.Capabilities.Thinking
}
