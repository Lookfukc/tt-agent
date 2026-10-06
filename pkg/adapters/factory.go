// Package adapters 提供协议适配器的装配入口
package adapters

import (
	"fmt"

	"github.com/Lookfukc/send-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
	"github.com/Lookfukc/send-agent/pkg/core"
)

// NewLLM 按提供商配置装配 LLM 实例
// cfg: 提供商配置，密钥需已通过 SetAPIKey 或 LoadAPIKeyFromEnv 绑定
// returns: 实现核心 LLM 接口的适配器；协议不支持或密钥缺失时返回错误
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

// NewLLMFromRegistry 便捷装配：注册表按 ID 取配置并构建
// r: 提供商注册表
// id: 提供商 ID
// returns: LLM 实例
func NewLLMFromRegistry(r *provider.Registry, id string) (core.LLM, error) {
	cfg, ok := r.Get(id)
	if !ok {
		return nil, fmt.Errorf("provider not registered: %s", id)
	}
	return NewLLM(cfg)
}
