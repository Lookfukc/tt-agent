// Package config 提供自定义提供商配置文件加载能力
package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
)

// CapabilitiesDTO 模型能力的文件表示
type CapabilitiesDTO struct {
	Streaming          bool  `json:"streaming"`
	ToolCalls          bool  `json:"tool_calls"`
	Thinking           bool  `json:"thinking"`
	Vision             bool  `json:"vision"`
	TemperatureSupport bool  `json:"temperature_support"`
	ContextWindow      int64 `json:"context_window"`
}

// ModelDTO 模型配置的文件表示
type ModelDTO struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Capabilities CapabilitiesDTO `json:"capabilities"`
}

// ProviderDTO 提供商配置的文件表示
//
// Quirks 是代码概念不进配置；自定义厂商接入 OpenAI 兼容端点
// 无需行为修正，需要时写代码注册
type ProviderDTO struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Protocol     string     `json:"protocol"`
	BaseURL      string     `json:"base_url"`
	APIKeyEnv    string     `json:"api_key_env"`
	DefaultModel string     `json:"default_model"`
	Models       []ModelDTO `json:"models"`
}

// FileConfig 配置文件根结构
type FileConfig struct {
	Providers []ProviderDTO `json:"providers"`
}

// LoadProviders 从 JSON 文件加载自定义提供商
// path: 配置文件路径
// returns: 提供商配置切片，字段校验失败即报错
func LoadProviders(path string) ([]provider.ProviderConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var fc FileConfig
	if err := json.Unmarshal(raw, &fc); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	out := make([]provider.ProviderConfig, 0, len(fc.Providers))
	builtinIDs := make(map[string]bool)
	for i := range provider.BuiltinProviders {
		builtinIDs[provider.BuiltinProviders[i].ID] = true
	}
	// 同一文件内的 ID 必须唯一，重复定义会让后者静默顶掉前者
	seenIDs := make(map[string]bool)
	for _, p := range fc.Providers {
		if p.ID == "" || p.BaseURL == "" || p.Protocol == "" || len(p.Models) == 0 {
			return nil, fmt.Errorf("provider %s: id/base_url/protocol/models are required", p.ID)
		}
		if p.APIKeyEnv == "" {
			return nil, fmt.Errorf("provider %s: api_key_env is required", p.ID)
		}
		// 与内置厂商同名会静默覆盖并共享密钥缓存，必须在装配期拦下
		if builtinIDs[p.ID] {
			return nil, fmt.Errorf("provider %s: id conflicts with builtin provider", p.ID)
		}
		// 同文件重复 ID 同样是静默覆盖，装配期点名拒绝
		if seenIDs[p.ID] {
			return nil, fmt.Errorf("provider %s: duplicate id in config file", p.ID)
		}
		seenIDs[p.ID] = true
		// 协议拼错只会在首个请求才 502，装配期即报
		if !supportedProtocols[p.Protocol] {
			return nil, fmt.Errorf("provider %s: unsupported protocol %q (want one of openai/anthropic/gemini)", p.ID, p.Protocol)
		}
		u, err := url.Parse(p.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("provider %s: base_url must be absolute http(s) URL", p.ID)
		}
		cfg := provider.ProviderConfig{
			ID: p.ID, Name: p.Name, Protocol: p.Protocol,
			BaseURL: p.BaseURL, APIKeyEnv: p.APIKeyEnv, DefaultModel: p.DefaultModel,
		}
		for _, m := range p.Models {
			if m.ID == "" {
				return nil, fmt.Errorf("provider %s: model id is required", p.ID)
			}
			cfg.Models = append(cfg.Models, provider.ModelConfig{
				ID: m.ID, Name: m.Name,
				Capabilities: provider.ModelCapabilities{
					Streaming:          m.Capabilities.Streaming,
					ToolCalls:          m.Capabilities.ToolCalls,
					Thinking:           m.Capabilities.Thinking,
					Vision:             m.Capabilities.Vision,
					TemperatureSupport: m.Capabilities.TemperatureSupport,
					ContextWindow:      m.Capabilities.ContextWindow,
				},
			})
		}
		if cfg.DefaultModel == "" {
			cfg.DefaultModel = cfg.Models[0].ID
		} else if !cfg.SupportsModel(cfg.DefaultModel) {
			return nil, fmt.Errorf("provider %s: default_model %q not in models", p.ID, cfg.DefaultModel)
		}
		out = append(out, cfg)
	}
	return out, nil
}

// supportedProtocols factory 支持的协议集合
var supportedProtocols = map[string]bool{
	"openai": true, "anthropic": true, "gemini": true,
}
