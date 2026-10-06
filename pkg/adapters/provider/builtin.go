package provider

import (
	"github.com/Lookfukc/send-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/send-agent/pkg/core"
)

// BuiltinProviders 内置厂商配置
//
// 配置只描述静态事实，厂商行为偏差统一放在 Quirks，
// 新增厂商优先评估能否仅靠配置接入
//
// 定价单位统一为美元每百万 token；国内厂商官方牌价为人民币，
// 按 7.25 汇率折算，避免 cost_usd 高估约 7 倍
var BuiltinProviders = []ProviderConfig{
	{
		ID:           "kimi",
		Name:         "Moonshot AI",
		Protocol:     "openai",
		BaseURL:      "https://api.moonshot.cn/v1",
		APIKeyEnv:    "KIMI_API_KEY",
		DefaultModel: "kimi-k2-0905-preview",
		Models: []ModelConfig{
			{
				ID:           "kimi-k2-0905-preview",
				Name:         "Kimi K2",
				Capabilities: ModelCapabilities{Streaming: true, ToolCalls: true, TemperatureSupport: true, ContextWindow: 256_000},
				// 官方 ¥4/¥16
				InputPricePerMtok: 0.55, OutputPricePerMtok: 2.21,
			},
		},
	},
	{
		ID:           "glm",
		Name:         "Zhipu AI",
		Protocol:     "openai",
		BaseURL:      "https://open.bigmodel.cn/api/paas/v4",
		APIKeyEnv:    "GLM_API_KEY",
		DefaultModel: "glm-4.6",
		Models: []ModelConfig{
			{
				ID:           "glm-4.6",
				Name:         "GLM-4.6",
				Capabilities: ModelCapabilities{Streaming: true, ToolCalls: true, Thinking: true, TemperatureSupport: true, ContextWindow: 200_000},
				// 官方 ¥8/¥16
				InputPricePerMtok: 1.10, OutputPricePerMtok: 2.21,
			},
		},
		Quirks: protocol.Quirks{
			// GLM 用顶层 thinking 字段控制思考模式，标准 OpenAI 协议没有这个字段
			PatchRequest: glmThinkingPatch,
		},
	},
	{
		ID:           "anthropic",
		Name:         "Anthropic",
		Protocol:     "anthropic",
		BaseURL:      "https://api.anthropic.com",
		APIKeyEnv:    "ANTHROPIC_API_KEY",
		DefaultModel: "claude-sonnet-4-5",
		Models: []ModelConfig{
			{
				ID:                "claude-sonnet-4-5",
				Name:              "Claude Sonnet 4.5",
				Capabilities:      ModelCapabilities{Streaming: true, ToolCalls: true, Thinking: true, TemperatureSupport: true, Vision: true, ContextWindow: 200_000},
				InputPricePerMtok: 3, OutputPricePerMtok: 15,
			},
			{
				ID:                "claude-haiku-4-5",
				Name:              "Claude Haiku 4.5",
				Capabilities:      ModelCapabilities{Streaming: true, ToolCalls: true, TemperatureSupport: true, Vision: true, ContextWindow: 200_000},
				InputPricePerMtok: 1, OutputPricePerMtok: 5,
			},
		},
	},
	{
		ID:           "gemini",
		Name:         "Google",
		Protocol:     "gemini",
		BaseURL:      "https://generativelanguage.googleapis.com/v1beta",
		APIKeyEnv:    "GEMINI_API_KEY",
		DefaultModel: "gemini-2.5-flash",
		Models: []ModelConfig{
			{
				ID:                "gemini-2.5-flash",
				Name:              "Gemini 2.5 Flash",
				Capabilities:      ModelCapabilities{Streaming: true, ToolCalls: true, Thinking: true, TemperatureSupport: true, Vision: true, ContextWindow: 1_000_000},
				InputPricePerMtok: 0.3, OutputPricePerMtok: 2.5,
			},
			{
				ID:                "gemini-2.5-pro",
				Name:              "Gemini 2.5 Pro",
				Capabilities:      ModelCapabilities{Streaming: true, ToolCalls: true, Thinking: true, TemperatureSupport: true, Vision: true, ContextWindow: 1_000_000},
				InputPricePerMtok: 1.25, OutputPricePerMtok: 10,
			},
		},
	},
	{
		ID:           "deepseek",
		Name:         "DeepSeek",
		Protocol:     "openai",
		BaseURL:      "https://api.deepseek.com/v1",
		APIKeyEnv:    "DEEPSEEK_API_KEY",
		DefaultModel: "deepseek-chat",
		Models: []ModelConfig{
			{
				ID:           "deepseek-chat",
				Name:         "DeepSeek V3",
				Capabilities: ModelCapabilities{Streaming: true, ToolCalls: true, TemperatureSupport: true, ContextWindow: 64_000},
				// 官方 ¥2/¥8
				InputPricePerMtok: 0.28, OutputPricePerMtok: 1.10,
			},
			{
				ID:           "deepseek-reasoner",
				Name:         "DeepSeek R1",
				Capabilities: ModelCapabilities{Streaming: true, Thinking: true, ContextWindow: 64_000},
				// 官方 ¥4/¥16
				InputPricePerMtok: 0.55, OutputPricePerMtok: 2.21,
			},
		},
		Quirks: protocol.Quirks{
			// reasoner 系模型收到 temperature 会直接 400，必须删除
			PatchRequest: deepSeekReasonerPatch,
		},
	},
}

// glmThinkingPatch 将统一 Thinking 配置转译为 GLM 私有字段
func glmThinkingPatch(body map[string]any, req core.ChatRequest) {
	if req.Thinking == nil {
		return
	}
	if req.Thinking.Enabled {
		body["thinking"] = map[string]any{"type": "enabled"}
	} else {
		body["thinking"] = map[string]any{"type": "disabled"}
	}
}

// deepSeekReasonerPatch 清除 reasoner 模型不接受的采样参数
func deepSeekReasonerPatch(body map[string]any, req core.ChatRequest) {
	if req.Model != "deepseek-reasoner" {
		return
	}
	delete(body, "temperature")
	delete(body, "top_p")
}
