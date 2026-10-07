# send-agent

基于Go 实现的多协议 LLM Agent 框架，统一内部消息类型，协议适配层兼容 OpenAI / Anthropic / Gemini 三家协议及一切 OpenAI-compatible 提供商（DeepSeek、GLM、Kimi、本地 ollama/vLLM……）。

## 功能总览

| 能力 | 说明 | 章节 |
|---|---|---|
| 多厂商接入 | 代码注册厂商，OpenAI/Anthropic/Gemini 三协议，密钥走环境变量 | [核心用法](#核心用法)、[配置文件](#接入自有配置文件) |
| ReAct Agent 循环 | 工具调用、熔断、token 预算、工具并行执行、流式事件回调 | [核心用法](#核心用法) |
| 自定义/内置工具 | 任意 `core.Tool` 实现；内置 calculator/clock/http_fetch；MCP 服务器接入 | [工具](#自定义工具)、[MCP](#mcp-工具接入) |
| 会话记忆 | 内存 Buffer / JSONL 持久化 / LLM 摘要压缩，按 session 隔离 | [记忆](#记忆) |
| 中间件链 | 日志/重试/限流/主备切换/缓存，流式与非流式分别覆盖 | [中间件](#中间件) |
| HTTP/SSE 服务 | 开箱即用的对话服务端，含 WS 与 gRPC 入口 | [HTTP 服务](#http-服务) |
| 工作流编排 | 多 Agent 路由/监督、人工检查点、断点续跑 | [工作流编排](#工作流编排) |
| 多模态/追踪/指标 | 图片输入三协议映射、span 链路追踪、按厂商指标聚合 | [多模态与追踪](#多模态与追踪) |
| 成本核算 | 模型定价按 token 计费，`cost_usd` 随响应返回 | [中间件](#中间件) |

## 环境要求

- Go ≥ 1.25（见 `go.mod`）
- 任一 LLM 提供商的 API Key（DeepSeek / 智谱 / Anthropic / Google / 本地 ollama 均可）

## 快速开始

本框架是公共依赖库，装进你自己的项目使用：

```bash
# 拉依赖
go get github.com/Lookfukc/send-agent
```

## 核心概念

一次对话自上而下经过这些层，每层都可以单独替换：

```
你的 main
  │  装配厂商配置 ProviderConfig（地址/协议/模型/密钥环境变量名）
  ▼
adapters.NewLLM ──► core.LLM          统一对话接口（Chat / ChatStream）
  │                                     外面包中间件：重试/限流/主备…
  ▼
agent.NewLoop(llm, tools, memory, cfg)  ReAct 循环：LLM ↔ 工具往返直至出结果
  │                                     事件流回调 OnEvent 实时吐增量
  ▼
loop.Run(ctx, sessionID, input)        返回最终消息 + token 用量
```

| 层 | 职责 | 不负责 |
|---|---|---|
| `adapters/provider` | 厂商配置结构、注册表、命名 quirks 库、定价 | 解析任何配置文件（格式由你选） |
| `adapters/protocol` | OpenAI/Anthropic/Gemini 请求构建与 SSE 解码 | 厂商差异（交给 Quirks） |
| `core.Pipeline` | 中间件链（重试、限流、日志、缓存、主备） | 业务逻辑 |
| `agent.Loop` | ReAct 循环、token 预算、工具并行、事件流 | 存储会话（交给 Memory） |
| `memory` | 会话历史按 session 隔离读写 | 压缩策略选择（三种实现任选） |
| `tools` | 工具注册表；builtin 内置工具；mcp 外部工具 | — |

## 核心用法

完整可跑的例子：

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Lookfukc/send-agent/pkg/adapters"
	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/core"
	"github.com/Lookfukc/send-agent/pkg/memory"
	"github.com/Lookfukc/send-agent/pkg/tools"
)

func main() {
	// 第 1 步：厂商配置即代码
	quirks, _ := provider.ComposeQuirks([]string{"glm-thinking"}, "openai")
	cfg := &provider.ProviderConfig{
		ID:           "glm",
		Name:         "Zhipu AI",
		Protocol:     "openai",                               // 协议三选一：openai/anthropic/gemini
		BaseURL:      "https://open.bigmodel.cn/api/paas/v4", // API 根地址，/chat/completions 由适配器拼
		APIKeyEnv:    "GLM_API_KEY",                          // 框架启动时从这个环境变量读密钥
		DefaultModel: "glm-4.6",
		Quirks:       quirks, // GLM 思考模式私有字段补丁，不需要可省
		Models: []provider.ModelConfig{{
			ID: "glm-4.6",
			Capabilities: provider.ModelCapabilities{
				Streaming: true, ToolCalls: true, Thinking: true, ContextWindow: 200_000,
			},
			InputPricePerMtok: 1.10, OutputPricePerMtok: 2.21, // 可选：美元/百万 token，缺省不计成本
		}},
	}
	if err := cfg.LoadAPIKeyFromEnv(); err != nil { // 读 GLM_API_KEY
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// 第 2 步：装配 LLM，套中间件
	llm, err := adapters.NewLLM(cfg) // 按 Protocol 选协议适配器
	if err != nil {
		panic(err)
	}
	wrapped := core.StreamRetry( // 流式路径：首 token 前失败可重试
		core.NewPipeline(llm, // 非流式路径：全中间件链
			core.Logging(nil), // 请求/响应日志，nil 用默认 slog
			core.Retry(3),     // 非流式重试 3 次
		), 3,
	)

	// 第 3 步：工具与记忆
	toolReg := tools.NewRegistry()
	toolReg.Register(myTool{})   // 任意实现 core.Tool 的结构，见「自定义工具」
	mem := memory.NewBuffer(nil) // 内存会话；持久化换 NewPersistent，见「记忆」

	// 第 4 步：ReAct 循环
	loop := agent.NewLoop(wrapped, toolReg, mem, agent.Config{
		Model:         cfg.DefaultModel,
		SystemPrompt:  "你是一个简洁的助手，必要时使用工具",
		MaxIterations: 16,     // 熔断上限：LLM↔工具最多往返 16 轮，默认 16
		TokenBudget:   32_000, // 单次调用输入 token 预算，超限截断最旧历史
		OnEvent: func(e agent.LoopEvent) { // 实时事件流，见下方事件表
			switch e.Type {
			case agent.EventDeltaText:
				fmt.Print(e.Text) // 正文增量，直接打即流式效果
			case agent.EventDeltaReasoning:
				fmt.Fprintf(os.Stderr, "[think] %s", e.Reasoning)
			case agent.EventToolCall:
				fmt.Fprintf(os.Stderr, "\n[tool] %s(%s)\n", e.Call.Name, e.Call.Arguments)
			case agent.EventError:
				fmt.Fprintf(os.Stderr, "\n[error] %v\n", e.Err)
			}
		},
	})

	// 第 5 步：运行
	msg, usage, err := loop.Run(context.Background(), "session-1", "你好")
	//  msg   最终 assistant 消息；usage 累计 token 用量；同一 sessionID 的下次 Run 自动带历史
	_, _ = msg, usage
}

```

`OnEvent` 回调收到的事件类型：

| 事件 | 载荷字段 | 含义 |
|---|---|---|
| `EventIterStart` | `Iter` | 进入第 N 轮 LLM↔工具往返 |
| `EventDeltaText` | `Text` | 正文增量（拼起来即完整回复） |
| `EventDeltaReasoning` | `Reasoning` | 思考链增量（模型支持时才有） |
| `EventToolCall` | `Call` | 模型请求调用工具 |
| `EventToolResult` | `Call`, `Err` | 工具执行完毕（`Err` 非-nil 表示失败） |
| `EventDone` | `Text`, `Usage` | 循环收敛，携带最终文本与累计用量 |
| `EventError` | `Err` | 循环终止于错误 |

注意：`Loop` 本身无状态，同一个 `loop` 可并发服务多个会话；此时 `OnEvent` 会来自多个 goroutine，用 `e.SessionID` 区分路由。

## 自定义工具

实现 `core.Tool` 四个方法即可，参数 schema 用 JSON Schema 表达：

```go
type weatherTool struct{}

func (weatherTool) Name() string { return "get_weather" }
func (weatherTool) Description() string { return "查询指定城市的当前天气" }
func (weatherTool) Parameters() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {"city": {"type": "string", "description": "城市名，如 北京"}},
		"required": ["city"]
	}`)
}
func (weatherTool) Execute(ctx context.Context, args json.RawMessage) (core.ToolResult, error) {
	var in struct {
		City string `json:"city"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return core.ToolResult{}, err // 解析失败：框架把错误回传给模型自行调整
	}
	return core.ToolResult{Text: in.City + " 晴，26℃"}, nil // Text 会作为 tool 消息回传模型
}

// 注册
toolReg := tools.NewRegistry()
toolReg.Register(weatherTool{})
```

要点：**工具失败不要返回 error 终止循环**——把失败原因写进 `ToolResult.Text` 让模型自己重试或换路；只有 LLM 错误和迭代超限才终止循环。同轮多个工具调用并行执行。

内置工具：

| 工具 | 构造 | 说明 |
|---|---|---|
| 计算器 | `builtin.NewCalculator()` | `go/ast` 白名单求值，不执行任意代码 |
| 时钟 | `builtin.NewClock()` | 当前时间/时间换算 |
| HTTP 抓取 | `builtin.NewHTTPFetch()` | 面向模型输出属 SSRF 面，默认内网不可达；`AllowPrivateNetwork = true` 显式放行 |

## 接入自有配置文件

框架**不解析任何配置文件**——格式（yaml / toml / .env / 硬编码）由你选，自己解析后把值填进 `ProviderConfig`。下面给出三种形态的完整接法。

### YAML（`gopkg.in/yaml.v3`）

```bash
go get gopkg.in/yaml.v3
```

`config.yaml`（贴近常见 `ai-chat` 风格，支持多家）：

```yaml
ai-chat:
  # 默认用哪家：切换只改这里，不用注释/反注释整段
  active: deepseek
  providers:
    deepseek:
      address: "https://api.deepseek.com/v1"   # 注意带 /v1，见下方字段映射
      model_name: "deepseek-chat"
      api_key_env: "DEEPSEEK_API_KEY"          # 推荐写 env 变量名而不是 key 本身
    glm:
      address: "https://open.bigmodel.cn/api/paas/v4"
      model_name: "glm-4.6"
      api_key_env: "GLM_API_KEY"
      quirks: ["glm-thinking"]                 # 可选：命名行为补丁
    local-ollama:
      address: "http://127.0.0.1:11434/v1"     # 本地模型同一条路，无需 is_local 分支
      model_name: "qwen3:8b"
      api_key_env: "OLLAMA_KEY"                # ollama 不校验 key 时随便设一个占位
```

`main.go` 完整胶水：

```go
package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
)

type providerYAML struct {
	Address    string   `yaml:"address"`
	ModelName  string   `yaml:"model_name"`
	APIKeyEnv  string   `yaml:"api_key_env"`
	Quirks     []string `yaml:"quirks"`
}

type configYAML struct {
	AIChat struct {
		Active    string                  `yaml:"active"`
		Providers map[string]providerYAML `yaml:"providers"`
	} `yaml:"ai-chat"`
}

func buildProviders(path string) ([]provider.ProviderConfig, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	var c configYAML
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, "", err
	}

	out := make([]provider.ProviderConfig, 0, len(c.AIChat.Providers))
	for id, p := range c.AIChat.Providers {
		cfg := provider.ProviderConfig{
			ID: id, Protocol: "openai",
			BaseURL: p.Address, APIKeyEnv: p.APIKeyEnv,
			DefaultModel: p.ModelName,
			Models: []provider.ModelConfig{{ID: p.ModelName}},
		}
		if len(p.Quirks) > 0 {
			q, err := provider.ComposeQuirks(p.Quirks, cfg.Protocol) // 名字错/协议不匹配在此报错
			if err != nil {
				return nil, "", fmt.Errorf("provider %s: %w", id, err)
			}
			cfg.Quirks = q
		}
		out = append(out, cfg)
	}
	return out, c.AIChat.Active, nil
}

func main() {
	providers, active, err := buildProviders("config.yaml")
	if err != nil {
		panic(err)
	}
	registry := provider.NewRegistry()
	for i := range providers {
		registry.Register(&providers[i])
	}
	// active 即默认厂商；key 各自按 api_key_env 从环境变量读
	cfg := registry.MustGet(active)
	if err := cfg.LoadAPIKeyFromEnv(); err != nil {
		panic(err)
	}
	// ...后续 adapters.NewLLM(cfg)，见「核心用法」
}
```

api_key 直接写进文件也可以（`pc.SetAPIKey(cfg.AIChat.APIKey)`），但务必确认该文件不进 git；更安全的做法是文件里只写 `api_key_env` 名字、key 放环境变量或密钥管理服务。

### .env（框架原生最贴合）

`APIKeyEnv` + `LoadAPIKeyFromEnv` 就是为这种形态设计的：

```bash
go get github.com/joho/godotenv
```

`.env`：

```env
AI_BASE_URL=https://api.deepseek.com/v1
AI_MODEL=deepseek-chat
DEEPSEEK_API_KEY=sk-xxx
```

```go
_ = godotenv.Load() // 可选：把 .env 装进进程环境变量；生产环境直接设系统 env 即可

cfg := &provider.ProviderConfig{
	ID: "main", Protocol: "openai",
	BaseURL:      os.Getenv("AI_BASE_URL"),
	APIKeyEnv:    "DEEPSEEK_API_KEY",
	DefaultModel: os.Getenv("AI_MODEL"),
	Models:       []provider.ModelConfig{{ID: os.Getenv("AI_MODEL")}},
}
_ = cfg.LoadAPIKeyFromEnv()
```

### TOML

同 YAML，解析库换 `BurntSushi/toml`（`go get github.com/BurntSushi/toml`），struct tag 改 `toml:"..."`，填 `ProviderConfig` 的方式不变。

### 旧配置字段 → `ProviderConfig` 映射

| 旧写法（常见） | 本框架 | 说明 |
|---|---|---|
| `address` + `request_address: "/chat/completions"` | `BaseURL` 一个字段 | 根地址（含 `/v1`），endpoint 路径适配器自己拼；拆开传会拼出双份路径 |
| `model_name` | `DefaultModel` + `Models[].ID` | |
| `api_key: "sk-..."` | `APIKeyEnv` + `LoadAPIKeyFromEnv()`，或 `SetAPIKey(v)` | 前者 key 不落文件；后者兼容存量直填 |
| `is_local: true` | 不需要 | ollama/vLLM 就是 openai 协议 + 本地 `BaseURL`，同一条路 |
| `whisper_asr_*` 等非对话字段 | 不映射 | 本框架只管对话链路，留在你自己的配置里 |

## HTTP 服务

`cmd/server` 是单厂商、flag+env 配置的现成服务端，模块内的 cmd 包可直接 go run，无需克隆仓库：

```bash
export DEEPSEEK_API_KEY=sk-xxx
go run github.com/Lookfukc/send-agent/cmd/server@latest --addr :8080 \
  --protocol openai \
  --base-url https://api.deepseek.com/v1 \
  --model deepseek-chat \
  --api-key-env DEEPSEEK_API_KEY

# 可选：--grpc :9090 同时开 gRPC；--prompt 设系统提示词
# --enable-httpfetch 注册 http_fetch 工具（SSRF 面，默认关）
# 多厂商按请求路由：写自己的 main 注册多家后用 entry.NewServer
```

| 端点 | 方法 | 说明 |
|---|---|---|
| `/api/chat` | POST | 对话；`stream: true` 时返回 SSE |
| `/api/chat/ws` | GET | WebSocket 入口，RFC 6455 |
| `/api/providers` | GET | 已注册厂商与模型列表 |
| `/api/metrics` | GET | 按厂商聚合的调用量/错误/token 指标 |
| `/api/health` | GET | 存活探针 |

请求体字段：

| 字段 | 必填 | 说明 |
|---|---|---|
| `input` | 是 | 用户输入 |
| `session_id` | 否 | 会话 ID，同 ID 自动带历史 |
| `stream` | 否 | `true` 走 SSE |
| `provider_id` / `model` | 否 | 多厂商注册时按请求路由/指定模型 |

非流式：

```bash
curl localhost:8080/api/chat -d '{"session_id":"s1","input":"你好"}'
# {"session_id":"s1","content":"你好！有什么可以帮你？","reasoning":"","usage":{"InputTokens":12,"OutputTokens":8,"ReasoningTokens":0},"cost_usd":0.0012}
```

流式（`curl -N` 关缓冲）：

```bash
curl -N localhost:8080/api/chat -d '{"input":"帮我查下天气","stream":true}'

event: text
data: {"delta":"好的，"}

event: text
data: {"delta":"我来查一下天气。"}

event: tool_call
data: {"id":"call_1","name":"get_weather","arguments":"{\"city\":\"北京\"}"}

event: tool_result
data: {"id":"call_1","name":"get_weather","error":""}

event: text
data: {"delta":"北京今天晴，26℃。"}

event: done
data: {"content":"...","reasoning":"","usage":{"InputTokens":123,"OutputTokens":45,"ReasoningTokens":0},"cost_usd":0.0009,"error":""}
```

终止事件为 `done`（成功）或 `error`（失败），载荷同构。客户端断连即取消整条循环（在途工具执行除外）。

## WebSocket 与 gRPC

**WS**（`GET /api/chat/ws`，零依赖 RFC 6455 实现）：入站一个文本帧 = 一个对话请求（同 `/api/chat` 的 JSON），服务端把 Loop 事件序列化成 JSON 帧回推，断连即取消当次循环，支持 ping/pong。

**gRPC**：`Chat` 一元 + `ChatStream` 服务端流。环境无 protoc，运行时构造 descriptor + dynamicpb 动态消息，零生成代码。服务定义在 `pkg/entry/grpc_desc.go` 顶部注释；客户端用 `entry.GRPCChatRequestDesc()` 构造动态消息后原生调用：

```go
conn.Invoke(ctx, "/agentframework.AgentService/Chat", req, resp)
```

## 工作流编排

多步骤、带人工审核、可断点续跑的工作流。每个步骤是一个已注册的 Agent：

```go
store, _ := orchestrator.NewFileRunStore("./runs") // 运行状态 JSONL 落盘，写入前 fsync
orch := orchestrator.New(store)

// Agent 就是普通的 agent.Loop，各自可用不同厂商/模型/提示词
orch.RegisterAgent("writer", writerLoop)
orch.RegisterAgent("reviewer", reviewerLoop)

_ = orch.RegisterWorkflow(&orchestrator.Workflow{
	Name: "review",
	Steps: []orchestrator.Step{
		orchestrator.AgentStep{Agent: "writer", Input: "$input"},
		orchestrator.CheckpointStep{Name: "approve", Prompt: "人工审核草稿"}, // 停下等人
		orchestrator.AgentStep{Agent: "reviewer", Input: "$prev"},
	},
})

run, err := orch.Run(ctx, "review", "写一段导语") // 停在检查点时 err == orchestrator.ErrCheckpoint
run2, err := orch.Resume(ctx, run.ID, "通过")     // 人工输入作为 $prev 喂给下一步
```

模板变量：

| 变量 | 值 |
|---|---|
| `$input` | 工作流初始输入 |
| `$prev` | 上一步输出 / 人工输入 |
| `$step.<name>.output` | 指定步骤输出（`AgentStep.Name`） |

容错语义：进程重启后重新注册工作流即可 `Resume`；`waiting`（检查点）与 `running`（取消/崩溃中断）状态均可续跑，中断续跑按至少一次语义重执行当前步骤。

**多 Agent 路由/监督**两种组合步骤：

```go
_ = orch.RegisterWorkflow(&orchestrator.Workflow{
	Name: "routed",
	Steps: []orchestrator.Step{
		orchestrator.RouterStep{ // 分类 Agent 输出必须是 Candidates 之一，据此选执行者
			Router: "classifier", Candidates: []string{"writer", "reviewer"}, Input: "$input",
		},
		orchestrator.SupervisorStep{ // 监督者每轮首行 WORKER <name> 委派 / DONE 收敛，轮数上限防死循环
			Supervisor: "boss", Workers: []string{"writer", "reviewer"}, Input: "$input",
		},
	},
})
```

## 记忆

三种实现同一接口，按 session 隔离，可直换：

```go
mem := memory.NewBuffer(nil)                          // ① 纯内存，重启即失
mem, _ := memory.NewPersistent("./sessions", nil)     // ② JSONL 落盘，重启自动恢复，坏行容错
mem = memory.NewSummary(mem, cheapLLM)                // ③ 在 ② 外再包摘要：截断丢弃的旧消息
                                                      //    用 LLM 压成前缀，压缩失败退化为纯截断
loop := agent.NewLoop(llm, tools, mem, cfg)           // 接口一致，直接替换
```

| 实现 | 持久化 | 长会话行为 |
|---|---|---|
| `Buffer` | 否 | 超预算截断最旧消息 |
| `Persistent` | JSONL/会话 | 同 Buffer，但重启恢复 |
| `Summary(inner, llm)` | 取决于 inner | 被截断的历史压缩成摘要前缀，不丢上下文 |

## 中间件

中间件分两档：`ChatMiddleware` 只包非流式 `Chat`；`LLMMiddleware` 同时覆盖 `Chat` 与 `ChatStream`。

| 中间件 | 覆盖路径 | 构造 |
|---|---|---|
| `Logging(nil)` | 非流式 | `ChatMiddleware` |
| `Retry(3)` | 非流式 | `ChatMiddleware` |
| `RateLimit(n, window)` | 非流式 | `ChatMiddleware` |
| `Cache(ttl, max)` | 非流式、无工具请求 | `ChatMiddleware` |
| `LoggingLLM(nil)` | 双路径 | `LLMMiddleware` |
| `RateLimitLLM(n, window)` | 双路径令牌桶 | `LLMMiddleware` |
| `FallbackLLM(primary, backup)` | 主备切换（流式仅首内容事件前切换） | 组合两个 LLM |
| `StreamRetry(llm, n)` | 流式首 token 前重试，之后原样透传 | 包装 LLM |

```go
// 服务端主路径（Agent 循环）恒为流式——挂 ChatMiddleware 的中间件对流式不生效，按需选 LLM 版
wrapped := core.StreamRetry(
	core.LoggingLLM(nil)(
		core.RateLimitLLM(10, time.Second)(
			core.NewPipeline(llm, core.Retry(3)),
		),
	), 3,
)
```

**结构化输出**：`ChatRequest.ResponseFormat = &core.ResponseFormat{Name, Schema}`（openai/gemini 已映射，anthropic 显式报错——该协议无原生支持）。

**成本核算**：`ModelConfig` 的 `InputPricePerMtok`/`OutputPricePerMtok` 声明定价（美元/百万 token，缺省不计），`m.CostOf(usage)` 计算（思考 token 计入输出侧），`/api/chat` 响应与 done 事件携带 `cost_usd`。

## 多模态与追踪

```go
// 图片输入：URL 或 Data URI 均可，三协议自动映射（openai image_url / anthropic base64 / gemini inlineData）
messages := []core.Message{core.UserImage("这张图是什么", "data:image/png;base64,...")}

// 链路追踪：observer.MemoryTracer 内存实现 core.Tracer 接口
tracer := observer.NewMemoryTracer()
loop := agent.NewLoop(llm, tools, mem, agent.Config{Model: "m", Tracer: tracer})
// span 树：agent.run → agent.iter → llm.stream / tool.exec；编排层另有 workflow.run → workflow.step

// 指标：按厂商聚合调用量/错误/token
metrics := observer.NewMetrics()
metrics.Record("glm", usage, err) // entry 服务端已内置
snap := metrics.Snapshot()        // map[providerID]Stats
```

## 命名 Quirks

厂商协议偏差不走子类，统一走命名补丁库，代码按名引用：

```go
// deepseek-reasoner 收到 temperature 会 400，请求前删除
quirks, _ := provider.ComposeQuirks([]string{"deepseek-reasoner"}, "openai")
cfg.Quirks = quirks
```

| 名字 | 效果 | 适用协议 |
|---|---|---|
| `glm-thinking` | 统一 Thinking 配置转译为 GLM 私有 `thinking` 字段 | openai |
| `deepseek-reasoner` | `deepseek-reasoner` 模型请求删除 `temperature`/`top_p` | openai |

多个名字按声明顺序组合执行；名字未知或协议不匹配时 `ComposeQuirks` 返回错误（错误信息列出全部可用名）。

## 架构分层

```
entry               HTTP/SSE/WS/gRPC 入口：POST /api/chat、GET /api/providers、/api/metrics
orchestrator        工作流编排：AgentStep + Router/Supervisor + 人工检查点，断点续跑
agent.Loop          ReAct 循环：MaxIterations 熔断、token 预算、工具并行执行
core.Pipeline       中间件链：Logging / Retry / RateLimit / Fallback / Cache
core.LLM            统一对话接口（能力信息在 ModelConfig，不进接口）
adapters/protocol   OpenAI / Anthropic / Gemini 三协议，stateful SSE 解码 + Quirks hook
adapters/provider   注册表（厂商一律由使用方代码注册）+ 命名 quirks 库 + 定价
memory              Buffer / Persistent(JSONL) / Summary(LLM 压缩)
tools/builtin       calculator(go/ast 白名单求值) / clock / http_fetch
tools/mcp           MCP 服务器接入（stdio / Streamable HTTP）
observer            提供商级调用量/错误/token 指标聚合 + 内存 span 追踪
```

## MCP 工具接入

把外部 MCP 服务器的工具全量挂进注册表，模型即可像内置工具一样调用：

```go
// stdio：本地进程
client, err := mcp.ConnectStdio(ctx, "my-tools", "npx", "-y", "some-mcp-server")
if err != nil {
	panic(err)
}
defer client.Close()
if err := client.Register(ctx, toolReg); err != nil { // 握手 + tools/list + 全量注册为 core.Tool
	panic(err)
}

// Streamable HTTP：远程服务器（可带 bearer token）
client = mcp.ConnectHTTP("remote", "https://mcp.example.com/mcp", "bearer-token")
```

## 关键设计决策

**流 channel 语义钉死。** 生产者 close；错误只走 `StreamError` 终止事件；`ChatStream` 的 error 返回值仅用于建连前失败。

**流式重试只发生在首 token 前。** `StreamRetry` 缓冲至首个内容事件，之后失败原样透传，避免重复输出。

**Memory 按 session 隔离。** Agent Loop 无状态可复用，同一 Loop 可并发服务多个会话。

**工具失败不熔断循环。** 错误文本作为 tool 消息回传，模型自行调整；只有 LLM 错误和迭代超限才终止。同轮多个工具调用并行执行。`LoopEvent` 携带 `SessionID`；单个 Run 内回调来自单一 goroutine，**同一 Loop 并发服务多会话时回调来自多个 goroutine**，需按 SessionID 路由。

## 测试

测试统一放在独立的 `test/` 模块，全部黑盒测试，只依赖导出 API：

```
test/
├── openai_test.go            SSE golden fixtures、错误映射、Quirks 注入
├── anthropic_test.go         Messages API 块结构、content_block 流解码
├── gemini_test.go            role 映射、functionCall 无 ID 的合成与还原
├── agent_test.go             ReAct 循环、熔断、工具失败回传
├── agent_parallel_test.go    工具并行执行与结果顺序
├── pipeline_test.go          Retry 语义、流式首 token 前重试
├── memory_test.go            持久化恢复、坏行容错、截断一致性
├── orchestrator_test.go      顺序链、模板、检查点暂停/重启续跑
├── entry_test.go             HTTP/SSE 端点
└── quirks_test.go            命名 quirks 组合与校验
```

```bash
go test ./test/       # 只跑框架测试
go test ./...         # 全量
go test -race ./test/
```

白盒测试（需要访问未导出符号时）例外，按 Go 惯例留在源码包内的 `xxx_test.go`。
