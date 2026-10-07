// Package main 演示从注册表装配 Provider 到 Agent 循环的完整链路
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Lookfukc/send-agent/pkg/adapters"
	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/core"
	"github.com/Lookfukc/send-agent/pkg/memory"
	"github.com/Lookfukc/send-agent/pkg/tools"
)

// 厂商配置即代码：需要几家写几家，行为偏差用命名 quirks，
// 密钥一律走环境变量（APIKeyEnv），不落代码
func buildProviders() []provider.ProviderConfig {
	deepseekQuirks, _ := provider.ComposeQuirks([]string{"deepseek-reasoner"}, "openai")
	glmQuirks, _ := provider.ComposeQuirks([]string{"glm-thinking"}, "openai")
	return []provider.ProviderConfig{
		{
			ID: "deepseek", Name: "DeepSeek", Protocol: "openai",
			BaseURL: "https://api.deepseek.com/v1", APIKeyEnv: "DEEPSEEK_API_KEY",
			DefaultModel: "deepseek-chat", Quirks: deepseekQuirks,
			Models: []provider.ModelConfig{
				{
					ID: "deepseek-chat", Name: "DeepSeek V3",
					Capabilities:      provider.ModelCapabilities{Streaming: true, ToolCalls: true, TemperatureSupport: true, ContextWindow: 64_000},
					InputPricePerMtok: 0.28, OutputPricePerMtok: 1.10,
				},
			},
		},
		{
			ID: "glm", Name: "Zhipu AI", Protocol: "openai",
			BaseURL: "https://open.bigmodel.cn/api/paas/v4", APIKeyEnv: "GLM_API_KEY",
			DefaultModel: "glm-4.6", Quirks: glmQuirks,
			Models: []provider.ModelConfig{
				{
					ID: "glm-4.6", Name: "GLM-4.6",
					Capabilities:      provider.ModelCapabilities{Streaming: true, ToolCalls: true, Thinking: true, TemperatureSupport: true, ContextWindow: 200_000},
					InputPricePerMtok: 1.10, OutputPricePerMtok: 2.21,
				},
			},
		},
	}
}

func main() {
	customs := buildProviders()
	registry := provider.NewRegistry()
	for i := range customs {
		registry.Register(&customs[i])
	}

	// 默认取首个厂商，AGENT_PROVIDER 显式覆盖
	providerID := customs[0].ID
	if v := os.Getenv("AGENT_PROVIDER"); v != "" {
		providerID = v
	}
	cfg, ok := registry.Get(providerID)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown provider: %s\n", providerID)
		os.Exit(1)
	}
	if err := cfg.LoadAPIKeyFromEnv(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	llm, err := adapters.NewLLM(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	wrapped := core.StreamRetry(core.NewPipeline(llm, core.Logging(nil), core.Retry(3)), 3)

	toolReg := tools.NewRegistry()
	toolReg.Register(echoTool{})

	loop := agent.NewLoop(wrapped, toolReg, memory.NewBuffer(nil), agent.Config{
		Model:        cfg.DefaultModel,
		SystemPrompt: "你是一个简洁的助手，必要时使用工具",
		OnEvent: func(e agent.LoopEvent) {
			switch e.Type {
			case agent.EventDeltaText:
				fmt.Print(e.Text)
			case agent.EventDeltaReasoning:
				fmt.Fprintf(os.Stderr, "[think] %s", e.Reasoning)
			case agent.EventToolCall:
				fmt.Fprintf(os.Stderr, "\n[tool] %s(%s)\n", e.Call.Name, e.Call.Arguments)
			case agent.EventError:
				fmt.Fprintf(os.Stderr, "\n[error] %v\n", e.Err)
			}
		},
	})

	input := "帮我调用 echo 工具说一下你好，然后用自己的话复述一遍"
	if len(os.Args) > 1 {
		input = os.Args[1]
	}

	msg, usage, err := loop.Run(context.Background(), "demo-session", input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nrun failed: %v\n", err)
	}
	fmt.Printf("\n\n--- usage: in=%d out=%d reasoning=%d ---\n",
		usage.InputTokens, usage.OutputTokens, usage.ReasoningTokens)
	_ = msg
}

// echoTool 演示用回声工具
type echoTool struct{}

// Name 工具名
func (echoTool) Name() string { return "echo" }

// Description 工具描述
func (echoTool) Description() string { return "原样返回输入的文本" }

// Parameters 参数 schema
func (echoTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"text":{"type":"string","description":"要回显的文本"}},"required":["text"]}`)
}

// Execute 回显输入
func (echoTool) Execute(_ context.Context, args json.RawMessage) (core.ToolResult, error) {
	var in struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return core.ToolResult{}, err
	}
	return core.ToolResult{Text: in.Text}, nil
}
