# tt-agent

> **中文** | [English](README.en.md)

[![CI](https://github.com/Lookfukc/tt-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/Lookfukc/tt-agent/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Lookfukc/tt-agent.svg)](https://pkg.go.dev/github.com/Lookfukc/tt-agent)

基于 Go 实现的多协议 LLM Agent 框架。统一内部消息类型，协议适配层兼容 **OpenAI / Anthropic / Gemini** 三家协议及一切 OpenAI-compatible 提供商（DeepSeek、GLM、Kimi、本地 ollama / vLLM……）。

- [功能总览](#功能总览)
- [环境要求](#环境要求)
- [安装](#安装)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [教程：从零装配一个 Agent](#教程从零装配一个-agent)
- [API 参考](#api-参考)
  - [core 包](#core-包)｜[adapters 包](#adapters-包)｜[agent 包](#agent-包)｜[memory 包](#memory-包)｜[tools 包](#tools-包)｜[entry 包](#entry-包)｜[orchestrator 包](#orchestrator-包)｜[observer 包](#observer-包)
- [完整示例集](#完整示例集)
- [接入自有配置文件](#接入自有配置文件)
- [关键设计决策](#关键设计决策)
- [错误处理与重试](#错误处理与重试)
- [常见问题 FAQ](#常见问题-faq)
- [测试](#测试)

---

## 功能总览

| 能力 | 说明 | 入口 |
|---|---|---|
| 多厂商接入 | 代码注册厂商，三协议（openai/anthropic/gemini），密钥走环境变量 | [`adapters.NewLLM`](#adapters-包) |
| ReAct Agent 循环 | 工具调用、迭代熔断、token 预算、工具并行、流式事件回调 | [`agent.NewLoop`](#agent-包) |
| 无状态模式 | 调用方自带全量历史，框架只做组装，服务端零存储 | [`Loop.RunWithHistory`](#runwithhistory) |
| 会话记忆 | JSONL 持久化 + LRU 驻留上限 + LLM 摘要压缩 + 空闲逐出 | [`memory`](#memory-包) |
| 自定义 / 内置工具 | 任意 `core.Tool` 实现；内置 calculator / clock / http_fetch；MCP 接入 | [`tools`](#tools-包) |
| 中间件链 | 日志 / 重试 / 限流 / 主备切换 / 缓存，流式与非流式分别覆盖 | [`core.Pipeline`](#中间件与-pipeline) |
| HTTP / SSE 服务 | 开箱即用对话服务端，含 WS 与 gRPC 入口 | [`entry.NewServer`](#entry-包) |
| 工作流编排 | 多 Agent 路由 / 监督、人工检查点、断点续跑 | [`orchestrator`](#orchestrator-包) |
| 多模态 / 追踪 / 指标 | 图片输入三协议映射、span 链路追踪、按厂商指标聚合 | [`observer`](#observer-包) |
| 成本核算 | 模型定价按 token 计费，`cost_usd` 随响应返回 | [`ModelConfig.CostOf`](#modelconfig-与-modelcapabilities) |

---

## 环境要求

| 项 | 要求 |
|---|---|
| Go | **≥ 1.24**（以 `go.mod` 的 `go` 指令为准） |
| LLM 凭据 | 任一提供商的 API Key（DeepSeek / 智谱 / Anthropic / Google / 本地 ollama 均可） |
| 外部依赖 | 仅 `google.golang.org/grpc`（gRPC 入口用）。核心链路零第三方依赖 |

> 提示：若 `go.mod` 声明的工具链版本高于本机版本，Go 会自动下载新工具链；在 `GOSUMDB=off` 环境下该下载会被拒绝。此时可用 `GOTOOLCHAIN=local` 或先手动安装对应版本。

---

## 安装

```bash
go get github.com/Lookfukc/tt-agent
```

想直接跑现成服务端（无需克隆仓库）：

```bash
go run github.com/Lookfukc/tt-agent/cmd/server@latest \
  --base-url https://api.deepseek.com/v1 \
  --model deepseek-chat \
  --api-key-env DEEPSEEK_API_KEY
```

---

## 快速开始

一段 40 行内、可直接 `go run` 的最小对话程序：

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Lookfukc/tt-agent/pkg/adapters"
	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/memory"
)

func main() {
	// 1. 厂商配置（密钥从环境变量 DEEPSEEK_API_KEY 读）
	cfg := &provider.ProviderConfig{
		ID:           "deepseek",
		Protocol:     "openai",
		BaseURL:      "https://api.deepseek.com/v1",
		APIKeyEnv:    "DEEPSEEK_API_KEY",
		DefaultModel: "deepseek-chat",
		Models:       []provider.ModelConfig{{ID: "deepseek-chat"}},
	}
	if err := cfg.LoadAPIKeyFromEnv(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// 2. 装配 LLM
	llm, err := adapters.NewLLM(cfg)
	if err != nil {
		panic(err)
	}

	// 3. 会话记忆（JSONL 落盘，重启可恢复）
	mem, err := memory.NewPersistent("./sessions", nil)
	if err != nil {
		panic(err)
	}

	// 4. 循环
	loop := agent.NewLoop(llm, nil, mem, agent.Config{
		Model:        cfg.DefaultModel,
		SystemPrompt: "你是一个简洁的助手",
	})

	// 5. 运行（同一 sessionID 的下一次 Run 自动带历史）
	msg, usage, err := loop.Run(context.Background(), "session-1", "你好")
	if err != nil {
		panic(err)
	}
	fmt.Println(msg.Content)
	fmt.Printf("tokens: in=%d out=%d\n", usage.InputTokens, usage.OutputTokens)
}
```

```bash
export DEEPSEEK_API_KEY=sk-xxx
go run main.go
```

---

## 核心概念

一次对话自上而下经过这些层，每层都可单独替换：

```
你的 main
  │  装配厂商配置 ProviderConfig（地址 / 协议 / 模型 / 密钥环境变量名）
  ▼
adapters.NewLLM ──► core.LLM          统一对话接口（Chat / ChatStream）
  │                                     外面可包中间件：重试 / 限流 / 主备…
  ▼
agent.NewLoop(llm, tools, memory, cfg)  ReAct 循环：LLM ↔ 工具往返直至出结果
  │                                     事件流回调 OnEvent 实时吐增量
  ▼
loop.Run(ctx, sessionID, input)        返回最终消息 + token 用量
  或 loop.RunWithHistory(ctx, history, input)   无状态模式
```

| 层 | 职责 | 不负责 |
|---|---|---|
| `adapters/provider` | 厂商配置结构、注册表、命名 quirks 库、定价 | 解析任何配置文件（格式由你选） |
| `adapters/protocol` | OpenAI / Anthropic / Gemini 请求构建与 SSE 解码 | 厂商差异（交给 Quirks） |
| `core.Pipeline` | 中间件链（重试、限流、日志、缓存、主备） | 业务逻辑 |
| `agent.Loop` | ReAct 循环、token 预算、工具并行、事件流 | 存储会话（交给 Memory） |
| `memory` | 会话历史按 session 隔离读写 | 压缩策略选择（多种实现任选） |
| `tools` | 工具注册表；builtin 内置工具；mcp 外部工具 | — |
| `entry` | HTTP / SSE / WS / gRPC 入口 | 业务鉴权与用户体系 |
| `orchestrator` | 多 Agent 工作流、检查点、断点续跑 | LLM 调用细节（复用 agent.Loop） |

---

## 教程：从零装配一个 Agent

### 第 1 步：定义厂商

```go
cfg := &provider.ProviderConfig{
	ID:           "glm",                                  // 唯一标识，用于路由与指标
	Name:         "Zhipu AI",                            // 展示名
	Protocol:     "openai",                              // 三选一：openai / anthropic / gemini
	BaseURL:      "https://open.bigmodel.cn/api/paas/v4", // API 根地址
	APIKeyEnv:    "GLM_API_KEY",                         // 密钥所在的环境变量名
	DefaultModel: "glm-4.6",
	Models: []provider.ModelConfig{{
		ID: "glm-4.6",
		Capabilities: provider.ModelCapabilities{
			Streaming: true, ToolCalls: true, Thinking: true, ContextWindow: 200_000,
		},
		InputPricePerMtok: 1.10, OutputPricePerMtok: 2.21, // 可选，用于成本核算
	}},
}
if err := cfg.LoadAPIKeyFromEnv(); err != nil {
	log.Fatal(err) // 环境变量未设置或为空
}
```

**`BaseURL` 只填根地址**，`/chat/completions` 之类的 endpoint 由适配器按协议拼接。填成完整 endpoint 会拼出双份路径。

### 第 2 步：装配 LLM 与中间件

```go
llm, err := adapters.NewLLM(cfg) // 按 cfg.Protocol 选适配器
if err != nil {
	return err
}

wrapped := core.StreamRetry(                  // 流式：首 token 前失败可重试
	core.NewPipeline(llm,                     // 非流式：完整中间件链
		core.Logging(nil),                    // 请求/响应日志，nil 用默认 slog
		core.Retry(3),                        // 非流式重试 3 次
	), 3,
)
```

> ⚠️ **关键**：Agent 循环恒走**流式**路径。只挂在 `core.NewPipeline` 上的中间件（`Logging` / `Retry` / `RateLimit` / `Cache`）对流式调用**不生效**；需要覆盖流式请用 LLM 版（`LoggingLLM` / `RateLimitLLM` / `FallbackLLM` / `StreamRetry`）。详见[中间件与 Pipeline](#中间件与-pipeline)。

### 第 3 步：定义工具

实现 `core.Tool` 四个方法：

```go
type weatherTool struct{}

// Name 工具唯一标识，模型按此名调用
func (weatherTool) Name() string { return "get_weather" }

// Description 供模型理解用途，写清适用场景能显著提升调用准确率
func (weatherTool) Description() string { return "查询指定城市的当前天气" }

// Parameters 参数 JSON Schema
func (weatherTool) Parameters() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {"city": {"type": "string", "description": "城市名，如 北京"}},
		"required": ["city"]
	}`)
}

// Execute 执行工具；args 是模型给出的原始 JSON
func (weatherTool) Execute(ctx context.Context, args json.RawMessage) (core.ToolResult, error) {
	var in struct {
		City string `json:"city"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return core.ToolResult{}, err // 框架把错误文本回传模型，让它自行调整
	}
	return core.ToolResult{Text: in.City + " 晴，26℃"}, nil
}
```

注册：

```go
toolReg := tools.NewRegistry()
toolReg.Register(weatherTool{})
```

> **设计原则**：工具失败**不要**返回 error 去终止循环——错误信息会作为 tool 消息回传给模型，让它自己重试或换路。只有 LLM 错误和迭代超限才终止循环。同轮多个工具调用**并行执行**。

### 第 4 步：装配循环

```go
loop := agent.NewLoop(wrapped, toolReg, mem, agent.Config{
	Model:         cfg.DefaultModel,     // 必填
	SystemPrompt:  "你是一个简洁的助手",   // 系统提示词
	MaxIterations: 16,                   // 熔断上限，默认 16
	TokenBudget:   32_000,               // 输入 token 预算，默认 32000
	Temperature:   nil,                  // *float64，nil 用提供商默认
	Thinking:      &core.ThinkingConfig{Enabled: true, BudgetTokens: 4096},
	OnEvent: func(e agent.LoopEvent) {   // 实时事件流
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
	Tracer: tracer, // 可选，见 observer 包
})
```

### 第 5 步：运行

```go
msg, usage, err := loop.Run(ctx, "session-1", "北京天气怎么样")
// msg   最终 assistant 消息（Content / Reasoning / ToolCalls）
// usage 本轮累计 token 用量
// err   终止性错误；ErrMaxIterations 表示达到熔断上限
```

同一 `sessionID` 的下一次 `Run` 会自动带上历史——这是**有状态模式**。若想让调用方自己存历史，用[无状态模式](#runwithhistory)。

---

## API 参考

### core 包

`github.com/Lookfukc/tt-agent/pkg/core`

#### LLM 接口

```go
type LLM interface {
	// Chat 非流式对话，返回完整响应
	Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error)

	// ChatStream 流式对话
	// 生产者负责 close channel；错误只通过 StreamError 事件传递；
	// error 返回值仅用于建连前失败
	ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error)
}
```

#### 消息与请求

| 类型 | 说明 |
|---|---|
| `Message` | 统一内部消息格式 |
| `ChatRequest` | 一次对话请求 |
| `ChatResponse` | 一次对话的完整响应 |
| `Usage` | token 用量记账 |
| `Role` | 角色：`RoleSystem` / `RoleUser` / `RoleAssistant` / `RoleTool` |

**`Message` 字段**

| 字段 | 类型 | 说明 |
|---|---|---|
| `Role` | `Role` | 角色 |
| `Content` | `string` | 文本内容 |
| `ContentParts` | `[]ContentPart` | 多模态分片，非空时适配器用其替代 `Content` |
| `ToolCalls` | `[]ToolCall` | 仅 assistant 消息携带，表示模型请求的工具调用 |
| `ToolCallID` | `string` | 仅 tool 角色携带，标识本次结果回应哪个调用 |
| `Reasoning` | `string` | 思维链，仅用于展示与记账，回传 API 时丢弃 |
| `FinishReason` | `FinishReason` | 仅流式聚合产物携带，历史回传时忽略 |

便捷构造：

```go
core.Text("你好")                                   // 构造 user 文本消息
core.UserImage("这张图是什么", "data:image/png;base64,...") // 构造带图 user 消息
```

**`ChatRequest` 字段**

| 字段 | 类型 | 说明 |
|---|---|---|
| `Model` | `string` | 模型 ID |
| `Messages` | `[]Message` | 消息列表 |
| `Tools` | `[]ToolSpec` | 暴露给模型的工具定义 |
| `Temperature` | `*float64` | nil 用提供商默认 |
| `MaxTokens` | `int64` | 输出上限，0 用提供商默认 |
| `Thinking` | `*ThinkingConfig` | 思考模式开关与预算 |
| `ResponseFormat` | `*ResponseFormat` | 非空时约束输出为符合 Schema 的 JSON |
| `Extra` | `map[string]any` | 提供商特有字段透传通道 |

**`Usage`**

```go
type Usage struct {
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64 // 思考 token，计成本时计入输出侧
}

usage.Total()        // OutputTokens + ReasoningTokens
usage.Add(other)     // 累加另一份用量（指针接收者）
```

#### 流式事件

| 类型 | 说明 |
|---|---|
| `StreamEventType` | `StreamStart` / `StreamDeltaText` / `StreamDeltaReasoning` / `StreamDeltaToolCall` / `StreamUsage` / `StreamDone` / `StreamError` |
| `StreamEvent` | 单个流事件 |
| `ToolCallDelta` | 工具调用参数分片：`Index` / `ID`（仅首片）/ `Name`（仅首片）/ `ArgsPart` |
| `StreamAccumulator` | 把事件流聚合成完整 `Message` + `Usage` |

```go
acc := core.NewStreamAccumulator()
for e := range events {
	acc.Feed(e)
}
msg, usage := acc.Message(), acc.Usage()

// 或一次性收集
msg, usage, err := core.CollectStream(events)
```

#### 工具

```go
type Tool interface {
	Name() string
	Description() string
	Parameters() json.RawMessage
	Execute(ctx context.Context, args json.RawMessage) (ToolResult, error)
}
```

`ToolResult`：

| 字段 | 说明 |
|---|---|
| `Text` | 文本结果，直接作为 tool 消息内容回传 |
| `Data` | 结构化结果，非空时 `Render()` 返回其 JSON 序列化 |
| `Images` | 图片结果（URL 或 base64），供 vision 模型消费 |

函数式工具（`ToolFunc` 只实现 `Execute`，需包一层才能注册）：

```go
// ToolFunc 签名：func(ctx, args) (ToolResult, error)
// 它只实现 Execute，Name/Description/Parameters 仍需结构体提供，
// 因此要用一个通用包装器把它接入注册表
type funcTool struct {
	name, desc string
	schema     json.RawMessage
	fn         core.ToolFunc
}

func (t funcTool) Name() string                { return t.name }
func (t funcTool) Description() string         { return t.desc }
func (t funcTool) Parameters() json.RawMessage { return t.schema }
func (t funcTool) Execute(ctx context.Context, args json.RawMessage) (core.ToolResult, error) {
	return t.fn(ctx, args)
}

reg.Register(funcTool{
	name: "ping",
	desc: "返回 pong",
	schema: json.RawMessage(`{"type":"object","properties":{}}`),
	fn: func(ctx context.Context, args json.RawMessage) (core.ToolResult, error) {
		return core.ToolResult{Text: "pong"}, nil
	},
})
```

「工具失败不熔断循环」的完整语义：`Execute` 返回的 error 会被框架转成文本 `tool error: <原因>`，作为 tool 消息回传模型，由模型决定重试或换路。只有 LLM 调用错误与迭代超限才会终止循环。

#### Memory 与 TokenCounter

```go
type Memory interface {
	// Add 追加消息，按 sessionID 隔离
	Add(ctx context.Context, sessionID string, msgs ...Message) error

	// Recent 取回不超预算的最近消息
	// 实现方负责从新到旧截断，并保证系统消息保留
	Recent(ctx context.Context, sessionID string, budget int64) ([]Message, error)

	// Clear 清空会话
	Clear(ctx context.Context, sessionID string) error
}

type TokenCounter interface {
	Count(msgs []Message) int64
}
```

#### 中间件与 Pipeline

两档中间件：

| 类型 | 签名 | 覆盖 |
|---|---|---|
| `ChatMiddleware` | `func(next ChatHandler) ChatHandler` | 只包非流式 `Chat` |
| `LLMMiddleware` | `func(next LLM) LLM` | 同时覆盖 `Chat` 与 `ChatStream` |

| 构造 | 覆盖路径 | 说明 |
|---|---|---|
| `core.Logging(logger)` | 非流式 | 请求/响应日志，`nil` 用默认 slog |
| `core.Retry(maxAttempts)` | 非流式 | 按 `ErrorKind.Retryable()` 决定是否重试，指数退避 |
| `core.RateLimit(n, window)` | 非流式 | 令牌桶；`n<=0` 或 `window<=0` 视为不限流 |
| `core.Cache(ttl, maxEntries)` | 非流式、**无工具**请求 | 相同请求命中缓存直接返回；带工具的请求可能产生副作用，直接穿透；条目数超上限整体清空 |
| `core.LoggingLLM(logger)` | 双路径 | 同上，覆盖流式 |
| `core.RateLimitLLM(n, window)` | 双路径 | 同上，共享一个令牌桶 |
| `core.FallbackLLM(primary, alternate)` | 双路径 | 主失败切备；流式仅在首个内容事件前可切 |
| `core.StreamRetry(llm, maxAttempts)` | 流式 | 缓冲至首个内容事件，之前失败可重试，之后原样透传 |

```go
type Pipeline struct{ /* ... */ }
func NewPipeline(llm LLM, middlewares ...ChatMiddleware) *Pipeline

// 组合示例（服务端主路径恒为流式，按需选 LLM 版）
wrapped := core.StreamRetry(
	core.LoggingLLM(nil)(
		core.RateLimitLLM(10, time.Second)(
			core.FallbackLLM(primaryLLM, backupLLM),
		),
	), 3,
)
```

#### 错误类型

```go
type ErrorKind int

const (
	ErrInvalidRequest   // 请求参数错误，重试无意义
	ErrAuth             // 鉴权失败，需换 key
	ErrPermission       // 配额或权限不足
	ErrRateLimited      // 限流，可延迟后重试
	ErrProviderInternal // 提供商服务端错误，可重试
	ErrNetwork          // 网络层错误，可重试
	ErrCanceled         // 调用方主动取消
	ErrUnsupported      // 能力不支持，属配置错误
	ErrExhausted        // 重试次数耗尽
)

func NewError(kind ErrorKind, providerID string, err error) *Error
func ErrorKindOf(err error) ErrorKind // 非 *Error 返回 ErrInvalidRequest
func Retryable(err error) bool        // 便捷判断
func (k ErrorKind) Retryable() bool   // 仅 RateLimited / ProviderInternal / Network 为 true
func (e *Error) Error() string
func (e *Error) Unwrap() error        // 可用 errors.Is / errors.As 穿透
```

> 适配器会把 HTTP 状态码与网络错误映射成上述类别（401→`ErrAuth`、429→`ErrRateLimited`、5xx→`ErrProviderInternal`、超时/连接失败→`ErrNetwork`），重试中间件据此决策。

#### 追踪

```go
type Span interface {
	SetAttr(key string, value any)
	RecordError(err error)
	End()
}

type Tracer interface {
	StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, Span)
}

func NoopTracer() Tracer                          // 空实现
func SpanFromContext(ctx context.Context) Span    // 取当前 span
func WithSpan(ctx context.Context, s Span) context.Context
```

### adapters 包

#### `ProviderConfig`

```go
type ProviderConfig struct {
	ID           string          // 唯一标识
	Name         string          // 展示名
	Protocol     string          // "openai" / "anthropic" / "gemini"
	BaseURL      string          // API 根地址（如 https://api.deepseek.com/v1）
	APIKeyEnv    string          // 密钥环境变量名（密钥不落代码）
	DefaultModel string          // 默认模型
	Models       []ModelConfig   // 可用模型清单
	Quirks       protocol.Quirks // 协议偏差补丁（可选）
}
```

| 方法 | 签名 | 说明 |
|---|---|---|
| `LoadAPIKeyFromEnv` | `() error` | 从 `APIKeyEnv` 读密钥并绑定；变量名为空或未设置时返回错误 |
| `APIKey` | `() (string, bool)` | 取当前密钥，`ok=false` 表示尚未绑定 |
| `SetAPIKey` | `(key string)` | 直接设置密钥（兼容存量配置），进程内生效 |
| `SupportsModel` | `(model string) bool` | 是否注册了该模型 |
| `Model` | `(model string) (ModelConfig, bool)` | 查模型配置 |
| `ThinkingCapability` | `(model string) bool` | 该模型是否支持思考模式 |

#### `ModelConfig` 与 `ModelCapabilities`

```go
type ModelConfig struct {
	ID                 string
	Name               string
	Capabilities       ModelCapabilities
	InputPricePerMtok  float64 // 美元/百万输入 token
	OutputPricePerMtok float64 // 美元/百万输出 token
}

type ModelCapabilities struct {
	Streaming          bool
	ToolCalls          bool
	Thinking           bool
	Vision             bool
	StructuredOutput   bool
	TemperatureSupport bool
	ContextWindow      int64 // token 数
}

// CostOf 按定价算成本；思考 token 计入输出侧；低于 0.5 美分归零
func (m ModelConfig) CostOf(u core.Usage) float64
```

#### `provider.Registry`

```go
reg := provider.NewRegistry()
reg.Register(&provider.ProviderConfig{ /* ... */ }) // ID 重复时覆盖

cfg, ok := reg.Get("deepseek")   // 查配置，ok=false 表示未注册
cfg = reg.MustGet("deepseek")    // 未注册时 panic，仅用于启动期静态装配
ids := reg.List()                // 全部厂商 ID
```

#### 装配 LLM

```go
// 直接从单份配置装配
func NewLLM(cfg *provider.ProviderConfig) (core.LLM, error)

// 从注册表按 ID 装配
func NewLLMFromRegistry(r *provider.Registry, id string) (core.LLM, error)
```

#### 命名 Quirks

厂商协议偏差不走子类继承，统一走**命名补丁库**：

```go
quirks, err := provider.ComposeQuirks([]string{"glm-thinking", "deepseek-reasoner"}, "openai")
if err != nil {
	return err // 名字未知或协议不匹配，错误信息会列出全部可用名
}
cfg.Quirks = quirks

names := provider.QuirkNames() // 列出全部可用补丁名
```

| 名字 | 效果 | 适用协议 |
|---|---|---|
| `glm-thinking` | 把统一 `Thinking` 配置转译为 GLM 私有 `thinking` 字段 | openai |
| `deepseek-reasoner` | 请求 `deepseek-reasoner` 时删除 `temperature` / `top_p`（否则 400） | openai |

多个名字按声明顺序组合执行。

#### 协议适配器（一般无需直接用）

```go
func NewOpenAI(providerID, baseURL, apiKey string, quirks Quirks) *OpenAIProtocol
func NewAnthropic(providerID, baseURL, apiKey string, quirks Quirks) *AnthropicProtocol
func NewGemini(providerID, baseURL, apiKey string, quirks Quirks) *GeminiProtocol
```

### agent 包

#### `Config`

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `SystemPrompt` | `string` | 空 | 系统提示词 |
| `Model` | `string` | — | 模型 ID |
| `Temperature` | `*float64` | nil | nil 用提供商默认 |
| `Thinking` | `*core.ThinkingConfig` | nil | 思考模式配置 |
| `MaxIterations` | `int` | **16** | LLM↔工具往返上限，`<=0` 取默认 |
| `TokenBudget` | `int64` | **32000** | 单次 LLM 调用输入 token 预算，`<=0` 取默认 |
| `OnEvent` | `func(LoopEvent)` | nil | 过程回调；**回调阻塞会拖慢整个循环** |
| `Tracer` | `core.Tracer` | 空实现 | 链路追踪 |

```go
func NewLoop(llm core.LLM, toolReg *tools.Registry, mem core.Memory, cfg Config) *Loop
```

`mem` 传 `nil` 表示纯无状态使用（此时 `Run` 返回错误，请用 `RunWithHistory`）。

#### `Run`

```go
func (l *Loop) Run(ctx context.Context, sessionID, input string) (core.Message, core.Usage, error)
```

| 参数 | 说明 |
|---|---|
| `ctx` | 取消时中断当前 LLM 调用或工具执行 |
| `sessionID` | 会话标识，历史与新消息都落在此会话 |
| `input` | 用户输入 |

返回最终 assistant 消息、全轮累计用量、终止性错误。

#### `RunWithHistory`

无状态模式：调用方持有历史，框架内部完成组装。

```go
func (l *Loop) RunWithHistory(ctx context.Context, history []core.Message, input string) (RunResult, error)

type RunResult struct {
	Message     core.Message   // 最终 assistant 回答
	NewMessages []core.Message // 本轮新增的全部消息（含输入），调用方落库
	Usage       core.Usage     // 全轮累计用量
}
```

| 参数 | 说明 |
|---|---|
| `ctx` | 取消时中断当前 LLM 调用或工具执行 |
| `history` | 调用方存储的全量历史，原顺序；可为 `nil` 开启全新对话 |
| `input` | 用户输入 |

行为要点：

- 历史只读——框架**不会**修改你传入的切片
- 超预算历史自动按原子组截断（系统消息保留），调用方无需自己控制长度
- `assistant(tool_calls)` 与其 `tool` 结果**同进同退**，不会产生孤儿 tool 消息（那会导致 400）
- 事件回调的 `SessionID` 恒为 `"stateless"`
- 出错时 `NewMessages` 仍携带已产生的消息，调用方可选择落库部分结果或整体放弃
- **运行过程不触碰 `Loop` 上挂的任何记忆**

典型用法：

```go
// 从你的数据库读历史
history, _ := db.LoadMessages(ctx, conversationID)

res, err := loop.RunWithHistory(ctx, history, userInput)
if err != nil {
	return err
}
// 落库本轮新增
_ = db.AppendMessages(ctx, conversationID, res.NewMessages)
fmt.Println(res.Message.Content)
```

#### Loop 事件

```go
type LoopEvent struct {
	SessionID string      // 事件归属会话
	Iter      int         // 第几轮
	Type      LoopEventType
	Text      string      // 文本增量或工具名
	Reasoning string
	Call      *core.ToolCall
	Usage     *core.Usage // 仅 EventDone 携带累计用量
	Err       error
}
```

| 事件 | 载荷字段 | 含义 |
|---|---|---|
| `EventIterStart` | `Iter` | 进入第 N 轮 LLM↔工具往返 |
| `EventDeltaText` | `Text` | 正文增量（拼起来即完整回复） |
| `EventDeltaReasoning` | `Reasoning` | 思考链增量（模型支持时才有） |
| `EventToolCall` | `Call` | 模型请求调用工具 |
| `EventToolResult` | `Call`, `Err` | 工具执行完毕（`Err` 非 nil 表示失败） |
| `EventDone` | `Text`, `Usage` | 循环收敛，携带最终文本与累计用量 |
| `EventError` | `Err` | 循环终止于错误 |

```go
var ErrMaxIterations = errors.New("agent loop: max iterations exceeded")
```

> **并发注意**：`Loop` 本身无状态，同一个 `loop` 可并发服务多个会话；此时 `OnEvent` 会来自**多个 goroutine**，回调必须并发安全并用 `e.SessionID` 路由。

### memory 包

#### 接口

```go
// core.Memory：三种实现都满足
type Memory interface {
	Add(ctx context.Context, sessionID string, msgs ...Message) error
	Recent(ctx context.Context, sessionID string, budget int64) ([]Message, error)
	Clear(ctx context.Context, sessionID string) error
}

// 可选能力接口：装饰器据此启用高级功能
type Splitter interface {       // Persistent / memorytest.Buffer 实现
	Split(ctx context.Context, sessionID string, budget int64) (kept, dropped []Message, err error)
}
type Trimmer interface {        // Persistent / memorytest.Buffer 实现
	Trim(ctx context.Context, sessionID string, n int) error
}
type SummaryStore interface {   // Persistent 实现，摘要随会话落盘
	SaveSummary(ctx context.Context, sessionID string, covered int, text string) error
	LoadSummary(ctx context.Context, sessionID string) (text string, covered int, err error)
}
```

#### 实现选型

| 实现 | 持久化 | 内存占用 | 多实例共享 | 适用场景 |
|---|---|---|---|---|
| `Persistent` | JSONL / 会话 | 有界（LRU 驻留） | ❌ 单机 | **单机默认，零依赖** |
| `sqlite.Driver` | 单文件数据库 | 无（直查） | ✅ 同机多进程 | 单节点多进程、需要 SQL 查询 |
| `redis.Driver` | Redis | 无（直查） | ✅ 跨机 | **多实例部署、原生 TTL** |
| `postgres.Driver` | PostgreSQL | 无（直查） | ✅ 跨机 | 已有 PG、需要事务/审计 |
| `Summary(inner, llm)` | 取决于 inner | 同 inner | 同 inner | 长上下文不丢信息 |
| `TTL(inner, ...)` | 取决于 inner | 有界 | 同 inner | 空闲逐出会话 |
| `memorytest.Buffer` | 否 | 无界 | ❌ | **仅测试** |

> 进程内纯内存实现已从公开 API 移除（易被误用于生产：重启即失、无界驻留）。测试与临时演示请用 `pkg/memory/memorytest` 子包。

**所有后端共用同一套语义**（预算截断、tool_calls 原子组配对、系统消息保留），因为它们复用同一个 `internal/sessionlog` 实现，并由同一套契约测试覆盖——换后端不会改变对话行为。

### 外部存储后端

三种外部后端共享同一套驱动契约：`pkg/memory/memorystore` 定义 `Driver`（只负责存取字节），预算计算与截断由 `Store` 统一完成。因此驱动实现不需要理解 token 预算，也不会各自跑偏。

```go
// Redis：多实例共享 + 原生 TTL（空闲会话由 Redis 自己过期，无需清扫协程）
d, err := redis.NewFromURL(ctx, "redis://localhost:6379/0", redis.Options{
	SessionTTL: 30 * time.Minute,
})
if err != nil {
	return err
}
defer d.Close()
mem := d.Memory(memorystore.Options{})

// SQLite：单文件、多进程安全、纯 Go 无 CGO
d, err := sqlite.New("./sessions.db", sqlite.Options{})
if err != nil {
	return err
}
defer d.Close()
if err := d.Migrate(ctx); err != nil { // 建表，幂等
	return err
}
mem := d.Memory(memorystore.Options{})

// PostgreSQL：已有 PG 时复用；需要先建 schema 再 Migrate
d, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: "agent"})
if err != nil {
	return err
}
defer d.Close()
if err := d.Migrate(ctx); err != nil {
	return err
}
mem := d.Memory(memorystore.Options{})
```

三种后端返回的 `*memorystore.Store` 同时实现 `core.Memory` + `Splitter` + `Trimmer` + `SummaryStore`，所以可以直接叠装饰器：

```go
mem = memory.NewCompactingSummary(mem, cheapLLM) // 摘要压缩照常可用
mem = memory.NewTTL(ctx, mem, 30*time.Minute, 5*time.Minute)
```

| 后端 | 键/表结构 | 清理方式 |
|---|---|---|
| Redis | `agent:mem:{id}` (LIST)、`:system` (LIST)、`:sum` (STRING) | 原生 `EXPIRE`（`SessionTTL`） |
| SQLite | `agent_messages` / `agent_summaries` 两表 | `PruneIdleSessions(idle)` 定时调用 |
| Postgres | 同 SQLite（按 schema 隔离） | `PruneIdleSessions(idle)` 定时调用 |

> **为什么 Redis 用两个 LIST**：`system` 平行列表记录「哪条是系统消息」。这样 `Trim` 就能在不解密、不解析全部消息的前提下定位最旧的**非系统**消息（系统消息永不删除）。代价是写入走 pipeline 保证两个列表等长。
>
> **Trim 的原子性**：整个修剪作为 Lua 脚本在 Redis 服务端单线程执行。客户端"读→重建→改名"的实现存在丢失更新竞态——LRANGE 与 RENAME 之间别的进程追加的消息会被改名覆盖吞掉。服务端脚本执行期间没有其他命令能插入，并发 `Add` 要么完整落在修剪前、要么完整落在修剪后。

### 加密存储

加密挂在**编解码层**，不在驱动层——因此三个后端外加本地 JSONL 全部自动获得加密能力，而密钥从不进入驱动代码：

```go
codec, err := memorystore.NewEncryptedCodec([]byte(os.Getenv("MEMORY_KEY")), nil)
if err != nil {
	return err
}
mem := d.Memory(memorystore.Options{Codec: codec}) // 任何驱动都一样
```

- 算法：**AES-256-GCM**，密钥任意长度（内部 SHA-256 派生为 32 字节）
- 记录格式：`base64("ttae1" || nonce(12) || ciphertext || tag)`，带版本前缀便于将来演进
- nonce 同时作为 GCM 的 additional data，防止密文被换 nonce 重组
- **向后兼容**：无版本前缀的记录按明文读取，所以已加密的数据集可以就地开启加密，无需重写
- 密钥错误或数据被篡改时按「损坏记录」跳过（`ErrDecrypt`），不会让整个会话崩溃

> ⚠️ 这保护的是**静态数据**（数据库文件、Redis 快照、备份泄露）。传输层仍需 TLS，密钥本身需要 KMS 或环境变量管理，不要写进代码。

### 快照与回滚（时间旅行）

外部驱动器存的是追加日志：能追加、能截断，但无法回答「三轮之前这个会话是什么样」，也无法撤销一次糟糕的回合。`pkg/memory/snapshot` 补上这一层，且**不改动 `core.Memory` 接口**：

```go
snaps := snapshot.New(driver, snapshot.Options{}) // 直接包 Driver，与 memory 视图共享同一存储

entry, err := snaps.Capture(ctx, "session-1", "before-tool-call")
// ... 模型跑了一轮，结果不理想 ...
if _, err := snaps.Rollback(ctx, "session-1", entry.ID); err != nil {
	return err
}

list, err := snaps.List(ctx, "session-1")  // 快照列表，最新在前
err = snaps.Delete(ctx, "session-1", entry.ID)
```

| 参数/方法 | 说明 |
|---|---|
| `New(driver, Options{Codec, MaxPerSession, Prefix, Observer, Backend, LockWait})` | `MaxPerSession` 默认 20，超出淘汰最旧；`LockWait` 默认 5s |
| `Capture(ctx, sessionID, label)` | 记录当前全部消息，返回 `Entry{ID, Label, MessageCount, CreatedAt}` |
| `Rollback(ctx, sessionID, snapshotID)` | **破坏性**：用快照内容替换会话日志；目标不存在返回 `ErrNoSnapshot` |
| `RollbackSafe(ctx, sessionID, snapshotID, safetyLabel)` | **非破坏性**：先把当前状态存为安全快照再回滚，返回 `(restored, safety)` 两个 Entry——后悔了用 `safety.ID` 再滚一次即撤销本次回滚 |
| `List` / `Delete` | 枚举（最新在前）与删除单条 |

`RollbackSafe` 的两条保证：

- **目标 ID 写错时不产生垃圾快照**：先校验目标存在，不存在直接 `ErrNoSnapshot`，快照日志不被污染
- **捕获与回滚同锁串行**：两步之间不可能插入并发写入，被丢弃的状态必然完整保存在安全快照里——这正是该方法存在的意义

**跨进程串行**：驱动实现 `memorystore.SessionLocker` 时（Redis 用 `SET NX PX` + token 释放脚本；Postgres 用事务级咨询锁，连接死亡即自动释放），快照操作额外持有该锁，「捕获+回滚」序列对共享同一存储的其他进程同样原子——测试覆盖了双进程并发 `RollbackSafe` 与持锁者崩溃后的锁自动回收。SQLite/文件后端无此能力，保证仅限单进程内（单机部署本来也只有一个进程）。

要点：

- 快照通过同一个 `Codec` 编码，**加密时快照也是密文**（测试覆盖）
- 快照存在派生的 `snapshot:<sessionID>` 键下，**不会混入对话消息**
- 回滚是破坏性的（这正是它的目的）；想保留当前状态就先 `Capture` 一次
- 恢复时逐字节透传原始载荷，不重新编码

### 长期记忆与向量检索

前面所有内容解决的是「这次对话记得住」；`pkg/ltm` 解决「下次还记得你」。两者刻意分开：会话历史是**有序日志按时间读**，长期记忆是**无序事实按相关性读**。

```go
store := ltm.NewMemoryStore(ltm.MemoryOptions{MaxFactsPerNamespace: 1000})
mem := ltm.New(store, ltm.Options{
	Embedder:  myEmbedder,   // 可选：不配则退化为关键词匹配
	Extractor: myExtractor,  // 可选：不配则只支持显式 Remember
})

// 显式记住一条事实
fact, err := mem.Remember(ctx, userID, "用户对花生过敏", map[string]string{"source": "session-42"})

// 按相关性召回
facts, err := mem.Recall(ctx, userID, "推荐个餐厅", 5)
if prompt := ltm.Prompt(facts); prompt != "" {
	// 作为 system 消息注入，模型即可看到长期上下文
	messages = append(messages, core.Message{Role: core.RoleSystem, Content: prompt})
}

// 对话结束后自动抽取事实（配合 Extractor 使用）
learned, err := mem.Learn(ctx, userID, conversation)

// 管理接口
err = mem.Forget(ctx, userID, fact.ID)
list, err := mem.List(ctx, userID, 20)
err = store.Clear(ctx, userID)
```

| 概念 | 说明 |
|---|---|
| `Fact` | 一条事实：`ID` / `Namespace` / `Text` / `Metadata` / 时间戳 / `Score` |
| `Namespace` | 隔离维度（通常 user ID）；**跨命名空间永远不可见**，空命名空间直接报错 |
| `Store` | 存储接口：`Upsert` / `Search` / `List` / `Delete` / `Clear` |
| `MemoryStore` | 内置进程内实现，带向量检索 + 容量上限 |
| `pgvector.Store` | PostgreSQL + pgvector 实现，跨进程共享的真·向量检索 |
| `Embedder` | 文本转向量；`EmbedderFunc` 可一行适配任意厂商 |
| `Extractor` | 对话蒸馏成事实；`ExtractorFunc` 同理 |
| `Prompt(facts)` | 把召回结果渲染成 system 提示片段 |
| `KeywordScore` | 导出的关键词评分函数，供外部 Store 实现做一致的降级排序 |

#### 开箱即用的 Embedder（`pkg/ltm/embeddings`）

三个适配器共享一个 HTTP 内核，只差认证头，直接实现 `ltm.Embedder`：

```go
// OpenAI 兼容：OpenAI / DeepSeek / GLM / 本地 ollama、vLLM（Bearer 认证）
emb := embeddings.NewOpenAICompatible("https://api.openai.com/v1", apiKey, "text-embedding-3-small")

// Anthropic（Voyage 驱动的 /v1/embeddings，x-api-key + anthropic-version 认证）
emb = embeddings.NewAnthropic(apiKey, "voyage-3-large")

// Google Gemini（:embedContent 端点，x-goog-api-key 认证）
emb = embeddings.NewGemini(apiKey, "gemini-embedding-001")

mem := ltm.New(store, ltm.Options{Embedder: emb})
```

| 选项 | 说明 |
|---|---|
| `WithHTTPClient(hc)` | 注入自定义 HTTP 客户端（代理 / 测试） |
| `WithBaseURL(base)` | 覆盖默认 API 根地址（网关 / 本地代理） |
| `WithDimensions(n)` | 请求指定输出维度（OpenAI text-embedding-3 系与 Voyage 系支持；Gemini 按模型固定，忽略此项） |

行为要点：

- 错误信息**始终携带 provider 响应体片段**——预览版 API 字段漂移时不需要抓包就能定位
- 空文本直接返回 nil（不发起请求），与 ltm 门面的降级语义对齐
- 30 秒默认超时，context 取消即时生效；并发安全
- ⚠️ **Anthropic 适配器未对线上端点实测**：编写环境无法访问 Anthropic 域名，实现基于其公开的预览版 API 描述（端点形状已由密闭测试锁定；认证头为 `x-api-key` + `anthropic-version`）。首次真连若字段有漂移，错误信息会带服务端原文，可即时定位
- ⚠️ **换 Embedder 通常意味着换向量维度**，配合 pgvector 时需新建表（`Dim` 建表时固定）

#### 内置 LLM Extractor（`pkg/ltm/extractor`）

基于框架自己的 `core.LLM`——协议适配、中间件、重试全部继承，指向 Agent 正在用的 LLM 或更廉价的模型均可：

```go
ext := extractor.New(llm, extractor.Options{
    MaxMessages: 100,           // 只取最近 N 条参与抽取
    Lenient:     true,          // LLM/解析失败返回空而非报错——学习是锦上添花，不该打断对话流
})
mem := ltm.New(store, ltm.Options{Embedder: emb, Extractor: ext})

learned, err := mem.Learn(ctx, userID, conversation) // 对话结束后的自动事实抽取
```

解析器容忍模型给 JSON 包 markdown 围栏和前后废话（首 `[` 到末 `]` 切片解析）；空串事实被丢弃；空对话不发起 LLM 调用。默认严格模式（解析失败报错），`Lenient: true` 变为静默降级。

#### pgvector 后端（PostgreSQL 向量检索）

`pkg/ltm/pgvector` 把长期记忆放进带 pgvector 扩展的 PostgreSQL——跨进程共享、持久化、SQL 级余弦排序。向量以 pgvector 文本格式（`'[1,2,3]'::vector`）写入，**零额外客户端依赖**：

```go
store, err := pgvector.NewFromURL(ctx, "postgres://user:pass@host/db", pgvector.Options{
	Dim:    1536, // 向量维度，建表时固定；换 embedding 厂商需新建表
	Schema: "agent",
})
if err != nil {
	return err
}
defer store.Close()
if err := store.Migrate(ctx); err != nil { // 建扩展 + 建表，幂等；连接角色需有建扩展权限
	return err
}
mem := ltm.New(store, ltm.Options{Embedder: myEmbedder})
```

行为规则与进程内实现严格对齐：

- **维度不匹配降级为 NULL 向量**：换 embedding 厂商时旧事实仍可关键词召回，写入不失败
- **非有限值（NaN/Inf）同样存 NULL**：pgvector 会拒绝，宁可丢排序不能丢事实
- **向量检索在 SQL 内完成**（`ORDER BY embedding <=> $query`），关键词降级在 Go 侧用共享的 `KeywordScore` 排序——与内置实现同一套评分
- 连接角色必须是库 owner 或有 `CREATE EXTENSION` 权限（`Migrate` 会给出明确错误提示）

设计要点：

- **去重靠稳定 ID**：同一事实重复学习只会更新一行（`namespace + 归一化文本` 的哈希），不需要额外调用 LLM 做 merge 决策
- **降级不丢数据**：embedding 失败时事实仍然存储、仍可关键词召回——丢掉用户的偏好比排序变差更糟
- **单一策略排序**：一次调用内要么全用向量相似度、要么全用关键词，避免两种分数混在一起不可比
- **容量上限**：`MaxFactsPerNamespace` 默认 1000，按插入序淘汰（不用时间戳，同纳秒写入会排序不稳定）
- **维度不匹配不 panic**：换 embedding 厂商导致维度变化时相似度为 0，该条被过滤

#### 实现选型

| 实现 | 持久化 | 内存占用 | 长会话行为 | 适用场景 |
|---|---|---|---|---|
| `Persistent` | JSONL / 会话 | 有界（LRU 驻留） | 超预算截断最旧 | **生产默认** |
| `Summary(inner, llm)` | 取决于 inner | 同 inner | 截断部分压缩成摘要前缀 | 需要长上下文不丢信息 |
| `TTL(inner, ...)` | 取决于 inner | 有界 | 空闲逐出会话 | 长驻进程防堆积 |
| `memorytest.Buffer` | 否 | 无界 | 超预算截断 | **仅测试** |

> 进程内纯内存实现已从公开 API 移除（易被误用于生产：重启即失、无界驻留）。测试与临时演示请用 `pkg/memory/memorytest` 子包；已有数据库的调用方，实现 `core.Memory` 三方法接口即可接入（见[示例 15](#示例-15自定义记忆后端)）。

#### `Persistent`

```go
// 构造：dir 不存在则创建；counter 为 nil 时用内置粗估
func NewPersistent(dir string, counter core.TokenCounter) (*Persistent, error)

// 带内存驻留上限：超限按 LRU 卸载最久未访问的会话（只卸内存，数据在盘上）
func NewPersistentWithLRU(dir string, counter core.TokenCounter, maxLoaded int) (*Persistent, error)
```

| 参数 | 说明 |
|---|---|
| `dir` | 会话文件目录，每会话一个 `<sessionID>.jsonl` |
| `counter` | token 估算器，`nil` 用内置字符粗估（偏保守，宁少勿超） |
| `maxLoaded` | 内存驻留会话上限，`0` 表示不限；建议 1024 量级 |

特性：

- **写穿透**：每条消息追加落盘，进程崩溃最多丢最后一条
- **惰性加载**：首访问才读文件，万级历史会话不拖垮启动与内存
- **坏行容错**：损坏行与超长行（>4MB）跳过，不中断恢复
- **新旧格式兼容**：新格式带 `{ts, msg}` 信封，旧格式裸消息，同文件混排均可读，零迁移
- **原子重写**：`Trim` 走 `temp + rename`，崩溃不会留下半截状态
- **路径安全**：`sessionID` 含 `/`、`\`、`..` 或超 128 字符一律拒绝
- **摘要持久化**：实现 `SummaryStore`，摘要存 `<sessionID>.summary`，`Clear` 联动删除

#### `Summary` 系列

```go
func NewSummary(inner core.Memory, llm core.LLM) *Summary
func NewSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary
func NewCompactingSummary(inner core.Memory, llm core.LLM) *Summary
func NewCompactingSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary
```

| 构造 | 旧消息处理 | 摘要持久化 |
|---|---|---|
| `NewSummary` | **不删**，完整保留（审计友好，占用无界） | 内层实现 `SummaryStore` 时落盘 |
| `NewCompactingSummary` | 摘要成功后**物理删除**（占用随摘要收敛） | 同上 |
| `...WithCounter` | 同左 | 额外注入内外层一致的 token 估算器 |

| 参数 | 说明 |
|---|---|
| `inner` | 实际存储，如 `Persistent`（也可再套 `TTL`） |
| `llm` | 用于压缩的模型，**建议用廉价小模型** |
| `c` | token 估算器；内外层口径必须一致时使用 |

行为要点：

- 摘要由「已折入条数」驱动缓存：同一截断点不重复压缩，截断点前进只压缩增量并与旧摘要滚动合并
- 压缩失败（LLM 报错）时**退化为纯截断**，不写缓存，下轮重试
- 摘要作为一条 system 消息插在系统消息之后、保留历史之前
- 压缩请求单条消息截断至 2KB、最多 200 条，控制压缩本身成本

#### `TTL`

```go
func NewTTL(ctx context.Context, inner core.Memory, idle, sweep time.Duration) *TTL
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `ctx` | — | janitor 生命周期，取消后不再逐出 |
| `inner` | — | 实际存储 |
| `idle` | **30 分钟** | 空闲多久逐出，`<=0` 取默认 |
| `sweep` | **5 分钟** | 检查周期，`<=0` 取默认，应小于 `idle` |

按**最后活跃时间**判空闲（长会话不会中途被杀），单个后台 janitor 周期清理；对 `Persistent` 内层，`Clear` 会同步删除磁盘文件——这是内存与磁盘无限增长的统一出口。

#### 生产推荐组合

```go
inner, _ := memory.NewPersistentWithLRU("./sessions", nil, 1024)
mem := memory.NewTTL(ctx, inner, 30*time.Minute, 5*time.Minute)
mem = memory.NewCompactingSummary(mem, cheapLLM)

// 或直接用服务端默认（临时目录 + LRU 1024）
srv := entry.NewServer(reg, toolReg)
```

### tools 包

#### `tools.Registry`

```go
reg := tools.NewRegistry()

reg.Register(tool)              // 注册，同名覆盖
t, ok := reg.Get("calculator")  // 查工具
specs := reg.Specs()            // 导出全部 ToolSpec（框架内部构造请求用）
t, err := reg.MustGet("x")      // 不存在返回错误
```

#### 内置工具

| 工具 | 构造 | 参数 | 返回 | 说明 |
|---|---|---|---|---|
| `calculator` | `builtin.NewCalculator()` | `expression` (string) | `{"value": 3}` | `go/ast` 白名单求值，不执行任意代码；支持 `+ - * / %`，不支持 `^` |
| `clock` | `builtin.NewClock()` | 无 | `{"now": "2026-01-01T00:00:00Z"}` | 当前时间 RFC3339 |
| `http_fetch` | `builtin.NewHTTPFetch()` | `url` (string) | 响应文本（截断至 64KB） | **SSRF 面**，默认拒绝环回/私网/链路本地目标 |

```go
// http_fetch 的显式放行（仅内网部署或本地测试）
fetch := builtin.NewHTTPFetchWithOptions(builtin.WithAllowPrivateTargets(true))

reg.Register(builtin.NewCalculator())
reg.Register(builtin.NewClock())
if enableFetch {
	reg.Register(fetch) // 生产默认不注册
}
```

> `http_fetch` 的私网校验在**拨号期**强制执行（防 DNS rebinding TOCTOU），重定向手动逐跳跟随且每跳重新校验，最多 5 跳，超时 15 秒。

#### MCP 工具接入

把外部 MCP 服务器的工具全量挂进注册表，模型即可像内置工具一样调用：

```go
// stdio：本地进程
func ConnectStdio(ctx context.Context, name, command string, args ...string) (*Client, error)

// Streamable HTTP：远程服务器（apiKey 可为空）
func ConnectHTTP(name, url, apiKey string) *Client

// 自定义传输（任何按行分帧的双向流）
func NewClient(rw interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
}, name string) *Client

func (c *Client) Connect(ctx context.Context) error            // initialize 握手，幂等
func (c *Client) ListTools(ctx context.Context) ([]core.ToolSpec, error)
func (c *Client) CallTool(ctx context.Context, name string, args json.RawMessage) (core.ToolResult, error)
func (c *Client) Register(ctx context.Context, reg *tools.Registry) error // 握手 + tools/list + 全量注册
func (c *Client) Close() error
```

```go
client, err := mcp.ConnectStdio(ctx, "my-tools", "npx", "-y", "some-mcp-server")
if err != nil {
	panic(err)
}
defer client.Close()

if err := client.Register(ctx, toolReg); err != nil { // 握手 + tools/list + 注册
	panic(err)
}
```

### entry 包

#### `Server` 与 `Config`

```go
type Config struct {
	Addr            string // 监听地址，默认 ":8080"
	DefaultProvider string // 默认厂商 ID
	DefaultModel    string // 默认模型（空则用厂商的 DefaultModel）
	SystemPrompt    string // 默认系统提示词
	MaxIterations   int    // 默认 16
	TokenBudget     int64  // 默认 32000
}

func NewServer(registry *provider.Registry, toolReg *tools.Registry, opts ...Option) *Server

type Option func(*Server)
func WithConfig(cfg Config) Option                       // 覆盖配置
func WithMemory(mem core.Memory) Option                  // 替换记忆实现
func WithLLMFactory(f LLMFactory) Option                 // 替换 LLM 装配（测试注入点）

// LLMFactory 按 providerID 装配 LLM
type LLMFactory func(providerID string) (core.LLM, error)

func DefaultMemoryDir() string                           // 缺省记忆目录（临时目录下 tt-agent-sessions）
func (s *Server) Handler() http.Handler                  // 挂载路由后的处理器
func (s *Server) Metrics() *observer.Metrics             // 指标聚合器
func (s *Server) CloseWebSockets()                       // 关闭全部在途 WS 连接
func (s *Server) GRPCRegister(gs *grpc.Server)           // 注册 gRPC 服务
```

**缺省记忆行为**：不传 `WithMemory` 时，服务端默认使用 `Persistent` + LRU 1024，落在 `os.TempDir()/tt-agent-sessions`。同机重启可恢复会话；**正式部署请用 `WithMemory` 指定专属目录**，避免与其他应用共用。

#### `cmd/server` 命令行服务

`cmd/server` 是单厂商、flag + env 配置的现成服务端，可直接 `go run`：

```bash
export DEEPSEEK_API_KEY=sk-xxx
go run github.com/Lookfukc/tt-agent/cmd/server@latest \
  --base-url https://api.deepseek.com/v1 \
  --model deepseek-chat \
  --api-key-env DEEPSEEK_API_KEY
```

| flag | 类型 | 默认 | 说明 |
|---|---|---|---|
| `--addr` | string | `:8080` | HTTP 监听地址 |
| `--grpc` | string | 空 | gRPC 监听地址，空则不启用 |
| `--provider` | string | `main` | 厂商 ID 标签（用于路由与指标聚合） |
| `--protocol` | string | `openai` | `openai` / `anthropic` / `gemini` |
| `--base-url` | string | **必填** | API 根地址，须为绝对 http(s) URL |
| `--api-key-env` | string | **必填** | 密钥所在环境变量名（密钥不落命令行） |
| `--model` | string | **必填** | 模型 ID |
| `--prompt` | string | 空 | 默认系统提示词 |
| `--enable-httpfetch` | bool | `false` | 注册 `http_fetch` 工具（SSRF 面，默认关） |
| `--httpfetch-allow-private` | bool | `false` | 放行 `http_fetch` 访问内网 |
| `--memory-type` | string | `file` | 会话记忆后端：`file` / `sqlite` / `redis` / `postgres` |
| `--memory-dir` | string | 空 | `file` 后端：会话目录；空则用共享临时目录 |
| `--memory-max-loaded` | int | `1024` | `file` 后端：内存驻留会话上限，超出按 LRU 卸载 |
| `--memory-dsn` | string | 空 | `sqlite` 传文件路径；`redis` 传 `redis://host:port/db`；`postgres` 传连接串 |
| `--memory-redis-prefix` | string | 空 | `redis` 后端键前缀（默认 `agent:mem:`） |
| `--memory-ttl` | duration | `0` | 会话保留期：`redis` 原生过期；`sqlite`/`postgres` 由清扫协程执行（`0` = 永久保留） |
| `--memory-prune-every` | duration | `5m` | `sqlite`/`postgres`：空闲会话清扫周期 |

多厂商按请求路由的场景，写自己的 `main` 注册多家后用 `entry.NewServer`。

#### HTTP 端点

| 端点 | 方法 | 说明 |
|---|---|---|
| `/api/chat` | POST | 对话；`stream: true` 时返回 SSE |
| `/api/chat/ws` | GET | WebSocket 入口（RFC 6455） |
| `/api/providers` | GET | 已注册厂商与模型列表 |
| `/api/metrics` | GET | 按厂商聚合的调用量 / 错误 / token |
| `/api/health` | GET | 存活探针 |

#### `POST /api/chat` 请求字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `input` | string | **是** | 用户输入 |
| `session_id` | string | 否 | 会话 ID，同 ID 自动带历史（**有状态模式**） |
| `messages` | []Message | 否 | 全量历史（**无状态模式**），与 `session_id` **互斥** |
| `stream` | bool | 否 | `true` 走 SSE |
| `provider_id` | string | 否 | 指定厂商（多厂商注册时按请求路由） |
| `model` | string | 否 | 指定模型 |
| `system_prompt` | string | 否 | 覆盖默认系统提示词 |

请求体上限 4MB。

**有状态模式响应**：

```json
{
  "session_id": "s1",
  "content": "你好！有什么可以帮你？",
  "reasoning": "",
  "usage": {"InputTokens": 12, "OutputTokens": 8, "ReasoningTokens": 0},
  "cost_usd": 0.0012
}
```

**无状态模式响应**：

```json
{
  "mode": "stateless",
  "content": "...",
  "reasoning": "",
  "new_messages": [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}],
  "usage": {"InputTokens": 12, "OutputTokens": 8, "ReasoningTokens": 0},
  "cost_usd": 0.0012
}
```

#### SSE 事件

```bash
curl -N localhost:8080/api/chat -d '{"input":"帮我查下天气","stream":true}'
```

| event | data 字段 | 说明 |
|---|---|---|
| `text` | `delta` | 正文增量 |
| `reasoning` | `delta` | 思考链增量 |
| `tool_call` | `id`, `name`, `arguments` | 模型请求调用工具 |
| `tool_result` | `id`, `name`, `error` | 工具执行完毕 |
| `error` | `message` | 循环内错误 |
| `done` / `error` | `content`, `reasoning`, `usage`, `cost_usd`, `error`，无状态模式额外含 `new_messages` | 终止事件，载荷同构 |

终止事件为 `done`（成功）或 `error`（失败）。客户端断连即取消整条循环（在途工具执行除外）。

#### WebSocket

`GET /api/chat/ws`，零依赖 RFC 6455 实现。入站一个文本帧 = 一个对话请求（同 `/api/chat` 的 JSON），服务端把 Loop 事件序列化成 JSON 帧回推，断连即取消当次循环，支持 ping/pong（60 秒心跳）。

#### gRPC

服务定义在 `pkg/entry/grpc_desc.go`，等价 .proto 契约：

```proto
package agentframework;

service AgentService {
  rpc Chat(ChatRequest) returns (ChatResponse);
  rpc ChatStream(ChatRequest) returns (stream ChatResponse);
}

message ChatRequest {
  string session_id    = 1;
  string provider_id   = 2;
  string model         = 3;
  string input         = 4;
  string system_prompt = 5;
}

message ChatResponse {
  string event            = 1; // start / text / reasoning / tool_call / tool_result / done / error
  string delta            = 2;
  string content          = 3;
  string error            = 4;
  int64  input_tokens     = 5;
  int64  output_tokens    = 6;
  int64  reasoning_tokens = 7;
  double cost_usd         = 8;
}
```

环境无 protoc 时，框架在运行时构造 descriptor + `dynamicpb` 动态消息，**零生成代码**；消息只含标量类型，无 WKT 依赖：

```go
conn.Invoke(ctx, "/agentframework.AgentService/Chat", req, resp)

// 客户端可用框架导出的 descriptor 构造动态消息
desc := entry.GRPCChatRequestDesc()
msg := dynamicpb.NewMessage(desc)
```

### orchestrator 包

#### 构造与注册

```go
func New(store RunStore) *Orchestrator

func (o *Orchestrator) RegisterAgent(name string, loop *agent.Loop)
func (o *Orchestrator) RegisterWorkflow(wf *Workflow) error
func (o *Orchestrator) Run(ctx context.Context, wfName, input string) (*RunState, error)
func (o *Orchestrator) Resume(ctx context.Context, runID, humanInput string) (*RunState, error)
func (o *Orchestrator) Get(runID string) (*RunState, bool)
```

| 方法 | 参数 | 返回 |
|---|---|---|
| `New` | `store`: 运行状态存储，`nil` 则不持久化（重启丢状态） | 编排器 |
| `RegisterAgent` | `name`: 唯一名（`AgentStep.Agent` 引用）；`loop`: 普通 `agent.Loop` | — |
| `RegisterWorkflow` | `wf`: 工作流定义；名为空报错，重名覆盖 | error |
| `Run` | `wfName`, `input`（步骤模板 `$input` 引用此值） | 运行状态；**停在检查点时返回 `ErrCheckpoint`** |
| `Resume` | `runID`, `humanInput`（作为 `$prev` 喂给下一步） | 续跑后的运行状态 |
| `Get` | `runID` | 状态 + 是否存在 |

```go
var ErrCheckpoint = errors.New("workflow paused at checkpoint")
```

`Orchestrator` 还有可选的 `Tracer core.Tracer` 字段（产生 `workflow.run` / `workflow.step` span）。

#### 步骤类型

```go
type Step interface{ stepKind() } // 封闭 sum type
```

**`AgentStep`**

| 字段 | 说明 |
|---|---|
| `Name` | 步骤名，供 `$step.<name>.output` 引用；空则取 Agent 名 |
| `Agent` | 已注册的 Agent 名 |
| `Input` | 输入模板 |

**`CheckpointStep`**

| 字段 | 说明 |
|---|---|
| `Name` | 检查点名 |
| `Prompt` | 给审核人的提示说明 |

**`RouterStep`**（多 Agent 路由）

| 字段 | 说明 |
|---|---|
| `Name` | 步骤名 |
| `Router` | 分类 Agent 名，其输出**必须是** `Candidates` 之一，否则步骤失败（不做模糊匹配） |
| `Candidates` | 可选执行 Agent 名单 |
| `Input` | 交给被选 Agent 的输入模板 |

**`SupervisorStep`**（监督者委派）

| 字段 | 说明 |
|---|---|
| `Name` | 步骤名 |
| `Supervisor` | 监督 Agent 名 |
| `Workers` | 可委派的工人 Agent 名单 |
| `Input` | 初始任务模板 |
| `MaxRounds` | 最大轮数，`0` 取默认 8 |

监督协议为纯文本：监督者输出首行 `WORKER <name>` 时委派（余下为任务说明），首行 `DONE` 时收敛（余下为最终答案）。

#### 模板变量

只做**整串**模板匹配（不做子串插值，避免引入转义规则）：

| 变量 | 值 |
|---|---|
| `$input` | 工作流初始输入 |
| `$prev` | 上一步输出 / 人工输入 |
| `$step.<name>.output` | 指定步骤输出 |

#### `RunState`

| 字段 | 类型 | 说明 |
|---|---|---|
| `ID` | string | 运行 ID（`run-<毫秒时间戳>-<序号>`） |
| `Workflow` | string | 工作流名 |
| `StepIdx` | int | 当前步骤下标 |
| `Input` | string | 初始输入 |
| `Prev` | string | 上一步输出 |
| `Outputs` | map[string]string | 各步骤输出 |
| `Status` | Status | `running` / `waiting` / `done` / `failed` |
| `Err` | string | 错误信息 |
| `Usage` | core.Usage | 累计用量 |
| `CreatedAt` / `UpdatedAt` | time.Time | 时间戳 |

#### 断点续跑语义

| 状态 | 含义 | `Resume` 行为 |
|---|---|---|
| `waiting` | 停在人工检查点 | `humanInput` 作为 `$prev`，步骤下标 +1，继续执行 |
| `running` | 进程崩溃 / 取消遗留 | 从当前步骤**重跑**（至少一次语义） |
| `done` / `failed` | 已结束 | 返回错误，无内容可续 |

- 状态**先落盘再执行**，进程崩溃后仍可 `Resume`
- 进程重启后**重新注册工作流**即可续跑
- 同 run 的 `Run` / `Resume` 由运行级锁串行化，避免双份执行双倍花费
- 调用方取消（`ctx` 取消）不是步骤失败：状态保持 `running`，不标 `failed`，保留续跑路径

#### `RunStore`

```go
type RunStore interface {
	Save(run *RunState) error
	Get(id string) (*RunState, bool)
}

// 每运行一个 JSON 文件，写入走临时文件 + 原子改名
func NewFileRunStore(dir string) (*FileRunStore, error)
```

### observer 包

#### `Metrics`

```go
type Stats struct {
	Calls           int64 `json:"calls"`
	Errors          int64 `json:"errors"`
	InputTokens     int64 `json:"input_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

func NewMetrics() *Metrics
func (m *Metrics) Record(providerID string, usage core.Usage, err error) // 每次调用记一次
func (m *Metrics) Snapshot() map[string]Stats                            // 导出副本
```

`entry.Server` 已内置 `Metrics` 并通过 `/api/metrics` 暴露。

#### `MemoryTracer`

```go
func NewMemoryTracer() *MemoryTracer
func NewMemoryTracerWithLimit(n int) *MemoryTracer // 默认上限 10000 个 span

// TraceSpan 已结束或进行中的 span 快照
type TraceSpan struct {
	TraceID    string         `json:"trace_id"`
	SpanID     string         `json:"span_id"`
	ParentID   string         `json:"parent_span_id,omitempty"`
	Name       string         `json:"name"`
	StartedAt  time.Time      `json:"started_at"`
	EndedAt    time.Time      `json:"ended_at"`
	Attributes map[string]any `json:"attributes,omitempty"`
}
func (s *TraceSpan) Duration() time.Duration // 未结束的 span 返回「至今」

func (t *MemoryTracer) StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, core.Span)
func (t *MemoryTracer) Spans() []TraceSpan
func (t *MemoryTracer) TraceCount() int
```

Span 树结构：`agent.run` → `agent.iter` → `llm.stream` / `tool.exec`；编排层另有 `workflow.run` → `workflow.step`。

#### 记忆事件与 Webhook

记忆子系统全链路可观测：会话存储、快照、长期记忆都支持注入 `Observer`，变更即发事件。

```go
// 第一层：进程内订阅（异步分发，绝不阻塞记忆写路径）
bus := observer.NewMemoryEventBus(1024, observer.MemoryObserverFunc(func(e observer.MemoryEvent) {
    log.Printf("[%s] %s session=%s detail=%v", e.Backend, e.Kind, e.Session, e.Detail)
}))
defer bus.Close()

store := d.Memory(memorystore.Options{Observer: bus})     // 任意驱动
snaps := snapshot.New(driver, snapshot.Options{Observer: bus})
mem := ltm.New(ltmStore, ltm.Options{Observer: bus})

// 第二层：转发到 HTTP（实现同一个接口，直接挂进总线或单独注入）
fwd := webhook.New(webhook.Config{
    URL:         "https://ops.example.com/agent-memory",
    Secret:      "hmac-key",   // 每个请求带 X-TT-Agent-Signature（HMAC-SHA256）
    MaxRetries:  3,            // 指数退避重试；5xx 重试、4xx 立即放弃
    QueueSize:   256,          // 有界队列，满载丢最旧并计数
})
defer fwd.Close()
bus2 := observer.NewMemoryEventBus(64, fwd) // 或直接 fwd 当 Observer 注入
```

**事件清单**：

| Kind | 来源 | Detail 携带 |
|---|---|---|
| `messages_appended` | 会话存储 | `count` |
| `session_trimmed` | 会话存储 | `count` |
| `session_cleared` | 会话存储 | — |
| `summary_saved` | 会话存储 | `covered` |
| `snapshot_captured` | 快照 | `snapshot_id` / `label` / `messages` |
| `session_rolled_back` | 快照 | `target`（RollbackSafe 另带 `safety`） |
| `fact_remembered` | 长期记忆 | `fact_id` |
| `fact_forgotten` | 长期记忆 | `fact_id` |

三条设计纪律：

1. **事件绝不携带消息内容**——只有计数与 ID。Webhook 出进程时不会把对话泄露给配置错误的 URL
2. **发射永不阻塞写路径**——慢观察者（测试里用挂起的 handler 验证）不拖慢 `Add`；总线满载丢弃并计数（`bus.Drops()`），Webhook 队列满载丢最旧
3. **观察者 panic 不杀分发器**——单个坏 sink 被隔离，其余观察者照常收到事件

Webhook 接收方验签示例：用相同 Secret 对**原始请求体**做 HMAC-SHA256，与 `X-TT-Agent-Signature` 头比对。

---

## 完整示例集

### 示例 1：最小可运行对话

见[快速开始](#快速开始)。

### 示例 2：终端流式对话

```go
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/Lookfukc/tt-agent/pkg/adapters"
	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
)

func main() {
	cfg := &provider.ProviderConfig{
		ID: "deepseek", Protocol: "openai",
		BaseURL: "https://api.deepseek.com/v1",
		APIKeyEnv: "DEEPSEEK_API_KEY", DefaultModel: "deepseek-chat",
		Models: []provider.ModelConfig{{ID: "deepseek-chat"}},
	}
	if err := cfg.LoadAPIKeyFromEnv(); err != nil {
		panic(err)
	}
	llm, _ := adapters.NewLLM(cfg)
	mem, _ := memory.NewPersistent("./sessions", nil)

	loop := agent.NewLoop(core.StreamRetry(llm, 3), nil, mem, agent.Config{
		Model:        cfg.DefaultModel,
		SystemPrompt: "你是一个简洁的助手",
		OnEvent: func(e agent.LoopEvent) {
			switch e.Type {
			case agent.EventDeltaText:
				fmt.Print(e.Text) // 边收边打
			case agent.EventDeltaReasoning:
				fmt.Fprintf(os.Stderr, "\033[2m%s\033[0m", e.Reasoning) // 思考链灰色
			case agent.EventDone:
				fmt.Printf("\n[tokens] in=%d out=%d\n", e.Usage.InputTokens, e.Usage.OutputTokens)
			}
		},
	})

	// 交互式多轮，同一 sessionID 自动带历史
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("\n> ")
		if !scanner.Scan() {
			break
		}
		if _, _, err := loop.Run(context.Background(), "cli-session", scanner.Text()); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}
}
```

### 示例 3：多工具与并行执行

```go
type addTool struct{}

func (addTool) Name() string             { return "add" }
func (addTool) Description() string      { return "两数相加" }
func (addTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}},"required":["a","b"]}`)
}
func (addTool) Execute(_ context.Context, args json.RawMessage) (core.ToolResult, error) {
	var in struct{ A, B float64 }
	if err := json.Unmarshal(args, &in); err != nil {
		return core.ToolResult{}, err
	}
	return core.ToolResult{Data: map[string]any{"sum": in.A + in.B}}, nil
}

type mulTool struct{}

func (mulTool) Name() string        { return "mul" }
func (mulTool) Description() string { return "两数相乘" }
func (mulTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}},"required":["a","b"]}`)
}
func (mulTool) Execute(_ context.Context, args json.RawMessage) (core.ToolResult, error) {
	var in struct{ A, B float64 }
	if err := json.Unmarshal(args, &in); err != nil {
		return core.ToolResult{}, err
	}
	return core.ToolResult{Data: map[string]any{"product": in.A * in.B}}, nil
}

reg := tools.NewRegistry()
reg.Register(addTool{})
reg.Register(mulTool{})

// 模型一轮内请求 add 与 mul 时，两者并行执行；结果按调用顺序落记忆
// 通过事件观察执行过程：
loop := agent.NewLoop(llm, reg, mem, agent.Config{
	Model: "deepseek-chat",
	OnEvent: func(e agent.LoopEvent) {
		if e.Type == agent.EventToolCall {
			fmt.Printf("[iter %d] 请求调用 %s(%s)\n", e.Iter, e.Call.Name, e.Call.Arguments)
		}
		if e.Type == agent.EventToolResult {
			fmt.Printf("[iter %d] %s 完成，err=%v\n", e.Iter, e.Call.Name, e.Err)
		}
	},
})
```

### 示例 4：记忆的三种生产配置

```go
// ① 单机最小可用：JSONL 落盘，重启恢复
mem, err := memory.NewPersistent("./sessions", nil)

// ② 长会话不丢上下文：摘要压缩（旧消息保留，审计友好）
mem, _ = memory.NewPersistent("./sessions", nil)
mem = memory.NewSummary(mem, cheapLLM) // cheapLLM 用廉价模型，如 glm-4-flash

// ③ 生产推荐：内存有界 + 空闲逐出 + 物理压缩
inner, _ := memory.NewPersistentWithLRU("./sessions", nil, 1024)
mem = memory.NewTTL(ctx, inner, 30*time.Minute, 5*time.Minute)
mem = memory.NewCompactingSummary(mem, cheapLLM)
```

三种都直接传给 `agent.NewLoop(llm, tools, mem, cfg)`，接口一致。

### 示例 5：无状态模式（调用方自管历史）

```go
// 框架不持有任何会话状态；历史由你的数据库管理
loop := agent.NewLoop(llm, toolReg, nil, agent.Config{ // mem 传 nil
	Model:        "deepseek-chat",
	SystemPrompt: "你是一个简洁的助手",
	TokenBudget:  32_000, // 超预算历史自动截断，你无需自己控制长度
})

// 一次请求
history, err := db.LoadMessages(ctx, convID) // 从你的存储读全量历史
if err != nil {
	return err
}

res, err := loop.RunWithHistory(ctx, history, userInput)
if err != nil {
	return err
}

// 落库本轮新增（含输入与所有工具往返消息）
if err := db.AppendMessages(ctx, convID, res.NewMessages); err != nil {
	return err
}

return res.Message.Content
```

多实例部署时无需共享任何存储——每个实例都是纯粹的请求处理器。

### 示例 6：多厂商注册与按请求路由

```go
reg := provider.NewRegistry()

deepseek := &provider.ProviderConfig{
	ID: "deepseek", Protocol: "openai",
	BaseURL: "https://api.deepseek.com/v1",
	APIKeyEnv: "DEEPSEEK_API_KEY", DefaultModel: "deepseek-chat",
	Models: []provider.ModelConfig{{ID: "deepseek-chat"}},
}
glm := &provider.ProviderConfig{
	ID: "glm", Protocol: "openai",
	BaseURL: "https://open.bigmodel.cn/api/paas/v4",
	APIKeyEnv: "GLM_API_KEY", DefaultModel: "glm-4.6",
	Models: []provider.ModelConfig{{ID: "glm-4.6"}},
}
reg.Register(deepseek)
reg.Register(glm)
for _, id := range reg.List() {
	if err := reg.MustGet(id).LoadAPIKeyFromEnv(); err != nil {
		log.Fatalf("provider %s: %v", id, err)
	}
}

// HTTP 层：请求体带 provider_id 即路由到对应厂商
srv := entry.NewServer(reg, toolReg, entry.WithConfig(entry.Config{
	Addr:            ":8080",
	DefaultProvider: "deepseek", // 不指定 provider_id 时用它
}))
```

```bash
curl localhost:8080/api/chat -d '{"provider_id":"glm","model":"glm-4.6","input":"你好"}'
```

### 示例 7：中间件全量组合

```go
primary, _ := adapters.NewLLM(deepseekCfg)
backup, _ := adapters.NewLLM(glmCfg)

wrapped := core.StreamRetry( // 最外层：流式首 token 前重试
	core.RateLimitLLM(60, time.Minute)( // 全局限速 60 次/分钟
		core.FallbackLLM(primary, backup), // 主备切换
	),
	3, // 最多重试 3 次
)

// 非流式路径需要显式套 Pipeline
pipeline := core.NewPipeline(primary,
	core.Logging(nil),
	core.Retry(3),
	core.Cache(5*time.Minute, 1000), // 相同请求 5 分钟内命中缓存
)
```

### 示例 8：结构化输出

```go
// 约束模型输出为符合 JSON Schema 的 JSON
// openai / gemini 已映射；anthropic 无原生支持会显式报错
schema := json.RawMessage(`{
	"type": "object",
	"properties": {
		"name": {"type": "string"},
		"age": {"type": "integer"}
	},
	"required": ["name", "age"]
}`)

resp, err := llm.Chat(ctx, core.ChatRequest{
	Model:    "deepseek-chat",
	Messages: []core.Message{core.Text("提取：张三今年 30 岁")},
	ResponseFormat: &core.ResponseFormat{
		Name:   "person",
		Schema: schema,
	},
})
```

### 示例 9：多模态图片输入

```go
// URL 或 Data URI 均可，三协议自动映射
// （openai image_url / anthropic base64 块 / gemini inlineData）
messages := []core.Message{
	core.UserImage("这张图里有什么", "https://example.com/cat.jpg"),
	core.UserImage("这张呢", "data:image/png;base64,iVBORw0KGgo..."),
}

resp, err := llm.Chat(ctx, core.ChatRequest{
	Model:    "gpt-4o",
	Messages: messages,
})
```

### 示例 10：追踪与指标

```go
tracer := observer.NewMemoryTracer()
metrics := observer.NewMetrics()

loop := agent.NewLoop(llm, tools, mem, agent.Config{
	Model:  "deepseek-chat",
	Tracer: tracer, // 产生 agent.run / agent.iter / llm.stream / tool.exec span
})

msg, usage, err := loop.Run(ctx, "s1", "你好")
metrics.Record("deepseek", usage, err)

// 查看 span
for _, sp := range tracer.Spans() {
	fmt.Printf("%s %v\n", sp.Name, sp.Duration())
}
fmt.Println("traces:", tracer.TraceCount())

// 指标快照
for providerID, st := range metrics.Snapshot() {
	fmt.Printf("%s: calls=%d errors=%d in=%d out=%d\n",
		providerID, st.Calls, st.Errors, st.InputTokens, st.OutputTokens)
}
```

### 示例 11：HTTP 服务端完整流程

```go
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/entry"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/tools"
	"github.com/Lookfukc/tt-agent/pkg/tools/builtin"
)

func main() {
	reg := provider.NewRegistry()
	reg.Register(&provider.ProviderConfig{
		ID: "deepseek", Protocol: "openai",
		BaseURL: "https://api.deepseek.com/v1",
		APIKeyEnv: "DEEPSEEK_API_KEY", DefaultModel: "deepseek-chat",
		Models: []provider.ModelConfig{{ID: "deepseek-chat"}},
	})
	if err := reg.MustGet("deepseek").LoadAPIKeyFromEnv(); err != nil {
		log.Fatal(err)
	}

	toolReg := tools.NewRegistry()
	toolReg.Register(builtin.NewCalculator())
	toolReg.Register(builtin.NewClock())

	// 生产记忆：专属目录 + LRU + TTL
	mem, err := memory.NewPersistentWithLRU("./sessions", nil, 1024)
	if err != nil {
		log.Fatal(err)
	}

	srv := entry.NewServer(reg, toolReg,
		entry.WithConfig(entry.Config{
			Addr:            ":8080",
			DefaultProvider: "deepseek",
			SystemPrompt:    "你是一个简洁的助手",
			MaxIterations:   16,
			TokenBudget:     32_000,
		}),
		entry.WithMemory(mem),
	)

	httpSrv := &http.Server{
		Addr:              ":8080",
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 30 * time.Second,
	}
	log.Println("listening on :8080")
	log.Fatal(httpSrv.ListenAndServe())
}
```

```bash
# 有状态多轮
curl localhost:8080/api/chat -d '{"session_id":"s1","input":"我叫张三"}'
curl localhost:8080/api/chat -d '{"session_id":"s1","input":"我叫什么"}'

# 无状态
curl localhost:8080/api/chat -d '{"messages":[{"role":"user","content":"我叫张三"}],"input":"我叫什么"}'

# 流式
curl -N localhost:8080/api/chat -d '{"input":"你好","stream":true}'

# 指标与健康
curl localhost:8080/api/metrics
curl localhost:8080/api/health
```

### 示例 12：工作流编排（检查点 + 续跑）

```go
store, err := orchestrator.NewFileRunStore("./runs")
if err != nil {
	log.Fatal(err)
}
orch := orchestrator.New(store)

// Agent 就是普通 agent.Loop，各自可用不同厂商 / 模型 / 提示词
orch.RegisterAgent("writer", writerLoop)
orch.RegisterAgent("reviewer", reviewerLoop)

err = orch.RegisterWorkflow(&orchestrator.Workflow{
	Name: "review",
	Steps: []orchestrator.Step{
		orchestrator.AgentStep{Agent: "writer", Input: "$input"},
		orchestrator.CheckpointStep{Name: "approve", Prompt: "人工审核草稿"},
		orchestrator.AgentStep{Agent: "reviewer", Input: "$prev"},
	},
})
if err != nil {
	log.Fatal(err)
}

// 执行：停在检查点
run, err := orch.Run(ctx, "review", "写一段导语")
if errors.Is(err, orchestrator.ErrCheckpoint) {
	// 状态已落盘：StepIdx 指向检查点，Status 为 waiting
	log.Printf("停在检查点：run=%s step=%d status=%s", run.ID, run.StepIdx, run.Status)

	// 人工审核通过后继续（进程重启后重新注册工作流即可 Resume）
	// humanInput 会作为 $prev 喂给检查点的下一步
	run, err = orch.Resume(ctx, run.ID, "通过")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("完成，状态=%s，输出=%+v", run.Status, run.Outputs)
}
```

### 示例 13：多 Agent 路由与监督

```go
err := orch.RegisterWorkflow(&orchestrator.Workflow{
	Name: "routed",
	Steps: []orchestrator.Step{
		// 分类 Agent 输出必须是 Candidates 之一，据此选执行者
		orchestrator.RouterStep{
			Name:       "route",
			Router:     "classifier",
			Candidates: []string{"writer", "reviewer"},
			Input:      "$input",
		},
		// 监督者每轮首行 WORKER <name> 委派 / DONE 收敛
		orchestrator.SupervisorStep{
			Name:       "supervise",
			Supervisor: "boss",
			Workers:    []string{"writer", "reviewer"},
			Input:      "$step.route.output", // 把路由结果作为任务
			MaxRounds:  8,
		},
	},
})
```

监督者的输出协议：

```
WORKER writer
请写一段关于春天的导语，200 字以内
```

或收敛：

```
DONE
春天来了，万物复苏……
```

### 示例 14：MCP 工具接入

```go
// stdio：拉起本地 MCP 服务器进程
client, err := mcp.ConnectStdio(ctx, "filesystem", "npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp")
if err != nil {
	log.Fatal(err)
}
defer client.Close()

// 握手 + tools/list + 全量注册为 core.Tool
if err := client.Register(ctx, toolReg); err != nil {
	log.Fatal(err)
}

// 之后模型即可调用该服务器的全部工具，与内置工具无差别
loop := agent.NewLoop(llm, toolReg, mem, cfg)

// 远程 Streamable HTTP
remote := mcp.ConnectHTTP("remote-tools", "https://mcp.example.com/mcp", "bearer-token")
if err := remote.Register(ctx, toolReg); err != nil {
	log.Fatal(err)
}
```

### 示例 15：自定义记忆后端

实现 `core.Memory` 三个方法即可接入你自己的数据库（20~30 行）：

```go
// RedisSessionMemory 用 Redis 存会话历史
type RedisSessionMemory struct {
	rdb *redis.Client
}

func (m *RedisSessionMemory) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	pipe := m.rdb.Pipeline()
	for _, msg := range msgs {
		b, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		pipe.RPush(ctx, "mem:"+sessionID, b)
	}
	pipe.Expire(ctx, "mem:"+sessionID, 7*24*time.Hour) // 天然 TTL
	_, err := pipe.Exec(ctx)
	return err
}

func (m *RedisSessionMemory) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	// 取最近 N 条后本地按预算装填（可复用框架的截断语义）
	raw, err := m.rdb.LRange(ctx, "mem:"+sessionID, -200, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]core.Message, 0, len(raw))
	for _, b := range raw {
		var msg core.Message
		if err := json.Unmarshal([]byte(b), &msg); err != nil {
			continue // 坏数据跳过
		}
		out = append(out, msg)
	}
	return out, nil
}

func (m *RedisSessionMemory) Clear(ctx context.Context, sessionID string) error {
	return m.rdb.Del(ctx, "mem:"+sessionID).Err()
}

// 接入
srv := entry.NewServer(reg, toolReg, entry.WithMemory(&RedisSessionMemory{rdb: rdb}))
```

**实现契约**（务必遵守）：

1. `Recent` 必须保证**系统消息保留**，且 `assistant(tool_calls)` 与其 `tool` 结果**同进同退**——切破配对会导致提供商 400，会话永久损坏
2. `Recent` 返回的消息保持原时间顺序
3. 三个方法都要**并发安全**（同一 `Loop` 会并发服务多个会话）
4. `Add` 落盘失败要返回错误（调用方据此感知），但不要把消息只留在内存里假装成功

> 若还想支持摘要压缩，额外实现 `Splitter` + `Trimmer`（甚至 `SummaryStore`）即可被 `Summary` 装饰器识别。

---

## 接入自有配置文件

框架**不解析任何配置文件**——格式（YAML / TOML / .env / 硬编码）由你选，自己解析后把值填进 `ProviderConfig`。

### YAML

```bash
go get gopkg.in/yaml.v3
```

`config.yaml`：

```yaml
ai-chat:
  active: deepseek # 默认用哪家：切换只改这一行
  providers:
    deepseek:
      address: "https://api.deepseek.com/v1" # 注意带 /v1
      model_name: "deepseek-chat"
      api_key_env: "DEEPSEEK_API_KEY" # 推荐写 env 变量名而不是 key 本身
    glm:
      address: "https://open.bigmodel.cn/api/paas/v4"
      model_name: "glm-4.6"
      api_key_env: "GLM_API_KEY"
      quirks: ["glm-thinking"]
    local-ollama:
      address: "http://127.0.0.1:11434/v1" # 本地模型同一条路，无需 is_local 分支
      model_name: "qwen3:8b"
      api_key_env: "OLLAMA_KEY" # ollama 不校验 key 时随便设一个占位
```

```go
type providerYAML struct {
	Address   string   `yaml:"address"`
	ModelName string   `yaml:"model_name"`
	APIKeyEnv string   `yaml:"api_key_env"`
	Quirks    []string `yaml:"quirks"`
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
			Models:       []provider.ModelConfig{{ID: p.ModelName}},
		}
		if len(p.Quirks) > 0 {
			q, err := provider.ComposeQuirks(p.Quirks, cfg.Protocol) // 名字错在此报错
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
	cfg := registry.MustGet(active)
	if err := cfg.LoadAPIKeyFromEnv(); err != nil {
		panic(err)
	}
	// ... 后续 adapters.NewLLM(cfg)
}
```

### .env

`APIKeyEnv` + `LoadAPIKeyFromEnv` 就是为这种形态设计的：

```bash
go get github.com/joho/godotenv
```

```env
AI_BASE_URL=https://api.deepseek.com/v1
AI_MODEL=deepseek-chat
DEEPSEEK_API_KEY=sk-xxx
```

```go
_ = godotenv.Load() // 可选：把 .env 装进进程环境变量；生产直接设系统 env

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

同 YAML，解析库换 `BurntSushi/toml`，struct tag 改 `toml:"..."`，填 `ProviderConfig` 的方式不变。

### 旧配置字段映射

| 旧写法（常见） | 本框架 | 说明 |
|---|---|---|
| `address` + `request_address: "/chat/completions"` | `BaseURL` 一个字段 | 只填根地址（含 `/v1`），endpoint 路径适配器自己拼 |
| `model_name` | `DefaultModel` + `Models[].ID` | |
| `api_key: "sk-..."` | `APIKeyEnv` + `LoadAPIKeyFromEnv()`，或 `SetAPIKey(v)` | 前者 key 不落文件 |
| `is_local: true` | 不需要 | ollama / vLLM 就是 openai 协议 + 本地 `BaseURL` |
| `whisper_asr_*` 等非对话字段 | 不映射 | 本框架只管对话链路 |

---

## 关键设计决策

**流 channel 语义钉死。** 生产者负责 close；错误只走 `StreamError` 终止事件；`ChatStream` 的 error 返回值仅用于建连前失败。

**流式重试只发生在首 token 前。** `StreamRetry` 缓冲至首个内容事件，之后失败原样透传，避免重复输出。

**Memory 按 session 隔离。** `Loop` 无状态可复用，同一 Loop 可并发服务多个会话。

**工具失败不熔断循环。** 错误文本作为 tool 消息回传，模型自行调整；只有 LLM 错误和迭代超限才终止。同轮多个工具调用并行执行。

**历史组装与存储分离。** 预算截断、`tool_calls` 原子组配对、悬空调用修补由框架在**组装请求时**完成（`sanitizeHistory`），存储只存事实——这使无状态模式与有状态模式共享同一套正确性保证。

**内存永远是缓存，不是真相源。** 缺省记忆落盘；`Persistent` 的惰性加载 + LRU 使内存占用只与「同时活跃的会话数」相关，与历史会话总量无关。

**装饰器优于子类。** 记忆的持久化 / 压缩 / 逐出是三个正交能力，通过 `Memory` + 可选能力接口（`Splitter` / `Trimmer` / `SummaryStore`）组合，而非继承树。

---

## 错误处理与重试

```go
msg, usage, err := loop.Run(ctx, "s1", "你好")
switch {
case err == nil:
	// 成功
case errors.Is(err, agent.ErrMaxIterations):
	// 达到熔断上限：可能是工具反复失败或问题过于复杂
case errors.Is(err, context.Canceled):
	// 调用方取消了 ctx
case core.Retryable(err):
	// 可重试类别（限流 / 提供商 5xx / 网络）
	kind := core.ErrorKindOf(err)
	_ = kind
default:
	// 其他终止性错误
}
```

| 错误 | 来源 | 处理建议 |
|---|---|---|
| `agent.ErrMaxIterations` | 循环达到 `MaxIterations` | 提高上限、简化问题、检查工具是否反复失败 |
| `orchestrator.ErrCheckpoint` | 工作流停在检查点 | 属**正常流程**，调 `Resume` 继续 |
| `core.ErrAuth` | 密钥错误 | 换 key，不要重试 |
| `core.ErrRateLimited` | 触发限流 | `Retry` 中间件会自动退避重试 |
| `core.ErrProviderInternal` | 提供商 5xx | 可重试；持久失败考虑 `FallbackLLM` |
| `core.ErrNetwork` | 网络层 | 可重试 |
| `core.ErrUnsupported` | 能力不支持 | 配置错误，如 anthropic + 结构化输出 |

---

## 常见问题 FAQ

**Q：为什么我的中间件对流式不生效？**
Agent 循环恒走 `ChatStream`。`core.NewPipeline` 上的 `Logging` / `Retry` / `RateLimit` / `Cache` 只包非流式 `Chat`。需要覆盖流式请用 `LoggingLLM` / `RateLimitLLM` / `FallbackLLM` / `StreamRetry`。

**Q：服务重启后会话历史为什么没了？**
检查是否用了 `WithMemory` 指定了固定目录。默认记忆落在系统临时目录，同机重启可恢复；换机或容器重建则不可。多实例部署需共享存储后端（见[示例 15](#示例-15自定义记忆后端)）。

**Q：会话数据会无限增长吗？**
`Persistent` 默认不限会话数（但内存只驻留活跃会话）。生产建议用 `NewPersistentWithLRU` + `NewTTL` + `NewCompactingSummary` 三件套，见[示例 4](#示例-4记忆的三种生产配置)。

**Q：模型调用工具时反复失败怎么办？**
检查两点：① `Parameters()` 的 JSON Schema 描述是否清晰（`description` 写清楚能显著提升准确率）；② `Execute` 返回的错误信息是否足够指导模型修正。工具错误会回传模型，它会自行调整。

**Q：怎么让模型输出 JSON？**
用 `ChatRequest.ResponseFormat`，见[示例 8](#示例-8结构化输出)。注意 anthropic 协议无原生支持，会显式报错。

**Q：如何并发服务多个会话？**
同一个 `Loop` 直接并发调用即可。注意 `OnEvent` 回调会来自多个 goroutine，必须并发安全并按 `e.SessionID` 路由。

**Q：`MaxIterations` 设多少合适？**
默认 16。简单问答 3~5 足够；复杂多步任务 16~32。设太小会导致任务未完成就熔断，设太大则失控时烧钱更多。

**Q：`TokenBudget` 和模型的 `ContextWindow` 什么关系？**
`TokenBudget` 是框架侧**输入 token 预算**，超限时截断最旧历史。应设为模型 `ContextWindow` 的一定比例（如 60~80%），给输出和系统提示留出余量。粗估偏保守（宁少勿超）。

**Q：能不用任何存储吗？**
可以。`RunWithHistory` 完全不碰存储，历史由调用方管理（见[示例 5](#示例-5无状态模式调用方自管历史)）。这也是多实例部署最省事的方式。

**Q：`http_fetch` 为什么不默认注册？**
它面向**模型输出**，无过滤即为 SSRF 面（可被引导抓取云元数据端点）。默认不注册；显式注册后仍默认拒绝环回/私网目标，需要访问内网要再开 `WithAllowPrivateTargets(true)`。

---

## 性能基准

机器：Intel Core Ultra 9 185H（22 线程），Windows，SQLite/文件走临时目录，Redis 为 **miniredis（进程内，无网络往返）**，Postgres 为 Docker 本机真实实例。数字用于**后端间相对比较**，不是绝对承诺。复现：`go test ./pkg/memory/... ./pkg/ltm/... -run '^$' -bench . -benchtime 2s`（Postgres 需设 `TEST_POSTGRES_DSN`）。

**写入吞吐（Add，单条 ~60 字符消息）**

| 后端 | ns/op | 说明 |
|---|---:|---|
| Redis (miniredis) | 122K | 无网络口径；真实 Redis 加一次 RTT |
| File (JSONL) | 255K | 追加写 + fsync |
| SQLite | 458K | 单事务插入 |
| Postgres（真实） | 1,584K | 本机 Docker；跨网络更高 |

**读取（Recent，全窗口物化）随会话长度线性增长**

| 后端 | 100 条 | 1,000 条 | 5,000 条 |
|---|---:|---:|---:|
| File | — | 151K | — |
| SQLite | 236K | 2,159K | 4,753K |
| Redis (miniredis) | 397K | 2,908K | 14,304K |
| Postgres（真实） | — | 3,857K | — |

线性增长正是 `maxScan=2000` 默认值存在的原因：它把单次请求组装的最坏耗时钉在毫秒量级；5,000 条的窗口在 Redis 上要 14ms——超长会话的老消息留在窗口外是设计取舍，不是缺陷。读取触到窗口上限不再静默：`store.CappedReads()` 计数每次出窗读取，运维可以据此发现"会话长过头了"并调大 `Options.MaxScan` 或做压缩。

**Trim（1,000 条会话删 10 条）**：SQLite 606K / Postgres 1,049K / Redis 1,957K（Lua 服务端整段执行）

**加密开销（AES-256-GCM，每条消息）**：明文编码 535ns → 加密编码 979ns；明文解码 1,137ns → 加密解码 1,476ns。**约 +0.5µs/条**，对比存储 I/O（122µs~1.6ms）是纯噪音——加密是免费的。

**长期记忆（进程内实现）**

| 操作 | 100 事实 | 1,000 事实 | 5,000 事实 |
|---|---:|---:|---:|
| 关键词召回 | 71K | 989K | 6,322K |
| 向量召回（64 维） | — | 1,184K | 8,382K |

`Remember` 写入 6.5µs。千级事实召回在 1ms 内；到五千条逼近 10ms 时，换 `pgvector.Store`（SQL 内排序，量级更稳）。

**快照**：Capture 100 条会话 ≈ 9ms；1,000 条 ≈ 112ms——随会话长度线性（快照是全量副本），配合 `MaxPerSession`（默认 20）封顶总占用。

---

## 测试

测试统一放在独立的 `test/` 模块，全部黑盒测试，只依赖导出 API：

```
test/
├── openai_test.go            SSE golden fixtures、错误映射、Quirks 注入
├── anthropic_test.go         Messages API 块结构、content_block 流解码
├── gemini_test.go            role 映射、functionCall 无 ID 的合成与还原
├── agent_test.go             ReAct 循环、熔断、工具失败回传
├── agent_parallel_test.go    工具并行执行与结果顺序
├── stateless_test.go         无状态模式、历史归还、HTTP messages 入口
├── pipeline_test.go          Retry 语义、流式首 token 前重试
├── middleware_test.go        限流、缓存、主备切换
├── memory_test.go            持久化恢复、坏行容错、截断一致性
├── memory_growth_test.go     增长控制、LRU 逐出、摘要压缩与持久化
├── summary_test.go           摘要滚动合并、LLM 失败退化
├── orchestrator_test.go      顺序链、模板、检查点暂停/重启续跑
├── multi_agent_test.go       Router / Supervisor 步骤
├── entry_test.go             HTTP/SSE 端点
├── ws_test.go                WebSocket 帧协议
├── grpc_test.go              gRPC 动态 descriptor 一元/流式
├── mcp_test.go               MCP 握手、工具列举与调用（stdio/HTTP）
├── builtin_tools_test.go     calculator / clock / http_fetch
├── multimodal_test.go        图片输入三协议映射
├── structured_output_test.go 结构化输出
├── metrics_test.go           指标聚合
├── trace_test.go             链路追踪
├── cost_test.go              成本核算
├── quirks_test.go            命名 quirks 组合与校验
└── issues_*_test.go          历史回归用例（按轮次与编号归档）
```

```bash
go test ./test/       # 只跑框架测试
go test ./...         # 全量
go test -race ./test/ # 竞态检测
```

存储驱动测试需要外部服务时按环境变量启用，未设置则优雅跳过：

| 环境变量 | 作用 |
|---|---|
| `TEST_POSTGRES_DSN` | 启用 Postgres 驱动契约测试 + pgvector 长期记忆测试（如 `postgres://user:pass@localhost:5432/db?sslmode=disable`），CI 应始终设置，使四后端一致性保证覆盖全部实现 |

**CI**（`.github/workflows/ci.yml`）：push/PR 触发，跑 gofmt 检查 → vet → build → **全量测试（`-race`，带真实 pgvector 服务容器）**，并守护 `go.mod` 的 `go 1.24` 版本下限不被依赖悄悄抬高。

白盒测试（需要访问未导出符号时）按 Go 惯例留在源码包内的 `xxx_test.go`。

---

## 目录结构

```
cmd/server/          开箱即用的服务端（flag + env 配置）
examples/            可运行示例
pkg/
├── core/            LLM / Memory / Tool / Pipeline / 错误 / 追踪 / 流式类型
├── adapters/
│   ├── provider/    厂商配置、注册表、命名 quirks、定价
│   └── protocol/    OpenAI / Anthropic / Gemini 三协议实现
├── agent/           ReAct 循环、无状态模式
├── memory/          会话记忆
│   ├── memorystore/ 驱动契约（Codec / Driver / Store）+ 加密编解码
│   ├── redis/       Redis 驱动（原生 TTL）
│   ├── sqlite/      SQLite 驱动（单文件、多进程、纯 Go）
│   ├── postgres/    PostgreSQL 驱动（schema 隔离）
│   ├── snapshot/    快照与回滚（时间旅行）
│   └── memorytest/  测试专用进程内实现
├── ltm/             长期记忆：事实抽取 + 向量检索 + 命名空间隔离
│   ├── embeddings/  开箱即用 Embedder（OpenAI 兼容 / Anthropic / Gemini）
│   ├── extractor/   内置 LLM 事实抽取器（基于 core.LLM）
│   └── pgvector/    PostgreSQL + pgvector 向量存储后端
├── tools/           工具注册表
│   ├── builtin/     calculator / clock / http_fetch
│   └── mcp/         MCP 客户端（stdio / Streamable HTTP）
├── entry/           HTTP / SSE / WS / gRPC 入口
├── orchestrator/    工作流编排、检查点、断点续跑
├── observer/        指标聚合、内存追踪、记忆事件总线
│   └── webhook/     事件 HTTP 转发（HMAC 签名 + 重试）
test/                黑盒测试
```

---

## License

见 [LICENSE](LICENSE)。
