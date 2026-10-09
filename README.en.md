# tt-agent

> [中文](README.md) | **English**

[![CI](https://github.com/Lookfukc/tt-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/Lookfukc/tt-agent/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Lookfukc/tt-agent.svg)](https://pkg.go.dev/github.com/Lookfukc/tt-agent)

A multi-protocol LLM agent framework implemented in Go. It unifies internal message types, and its protocol adaptation layer is compatible with **OpenAI / Anthropic / Gemini** as well as any OpenAI-compatible provider (DeepSeek, GLM, Kimi, local ollama / vLLM, …).

- [Features Overview](#features-overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Tutorial: Assembling an Agent from Scratch](#tutorial-assembling-an-agent-from-scratch)
- [API Reference](#api-reference)
  - [core package](#core-package)｜[adapters package](#adapters-package)｜[agent package](#agent-package)｜[memory package](#memory-package)｜[tools package](#tools-package)｜[entry package](#entry-package)｜[orchestrator package](#orchestrator-package)｜[observer package](#observer-package)
- [Complete Example Collection](#complete-example-collection)
- [Integrating Your Own Configuration File](#integrating-your-own-configuration-file)
- [Key Design Decisions](#key-design-decisions)
- [Error Handling and Retries](#error-handling-and-retries)
- [FAQ](#faq)
- [Testing](#testing)

---

## Features Overview

| Capability | Description | Entry point |
|---|---|---|
| Multi-provider access | Providers are registered in code; three protocols (openai/anthropic/gemini); API keys come from environment variables | [`adapters.NewLLM`](#adapters-package) |
| ReAct agent loop | Tool calls, iteration cap, token budget, parallel tools, streaming event callbacks | [`agent.NewLoop`](#agent-package) |
| Stateless mode | The caller supplies the full history; the framework only assembles the request; zero server-side storage | [`Loop.RunWithHistory`](#runwithhistory) |
| Session memory | JSONL persistence + LRU resident cap + LLM summary compression + idle eviction | [`memory`](#memory-package) |
| Custom / built-in tools | Any `core.Tool` implementation; built-in calculator / clock / http_fetch; MCP integration | [`tools`](#tools-package) |
| Middleware chain | Logging / retry / rate limiting / failover / caching, covering the streaming and non-streaming paths separately | [`core.Pipeline`](#middleware-and-pipeline) |
| HTTP / SSE service | Ready-to-run chat server, plus WS and gRPC entry points | [`entry.NewServer`](#entry-package) |
| Workflow orchestration | Multi-agent routing / supervision, human checkpoints, resume from checkpoint | [`orchestrator`](#orchestrator-package) |
| Multimodal / tracing / metrics | Image input mapped across all three protocols, span-based tracing, per-provider metric aggregation | [`observer`](#observer-package) |
| Cost accounting | Model pricing billed by token; `cost_usd` returned with the response | [`ModelConfig.CostOf`](#modelconfig-and-modelcapabilities) |

---

## Requirements

| Item | Requirement |
|---|---|
| Go | **≥ 1.24** (per the `go` directive in `go.mod`) |
| LLM credentials | An API key from any provider (DeepSeek / Zhipu / Anthropic / Google / local ollama all work) |
| External dependencies | Only `google.golang.org/grpc` (for the gRPC entry point). The core path has zero third-party dependencies |

> Tip: if the toolchain version declared in `go.mod` is newer than the one installed locally, Go downloads the newer toolchain automatically; under `GOSUMDB=off` that download is rejected. In that case use `GOTOOLCHAIN=local`, or install the matching version manually first.

---

## Installation

This framework is a public library meant to be installed into your own project:

```bash
go get github.com/Lookfukc/tt-agent
```

To run the ready-made server directly (no need to clone the repository):

```bash
go run github.com/Lookfukc/tt-agent/cmd/server@latest \
  --base-url https://api.deepseek.com/v1 \
  --model deepseek-chat \
  --api-key-env DEEPSEEK_API_KEY
```

---

## Quick Start

A minimal chat program under 40 lines that you can `go run` as is:

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
	// 1. Provider config (the API key is read from the env var DEEPSEEK_API_KEY)
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

	// 2. Assemble the LLM
	llm, err := adapters.NewLLM(cfg)
	if err != nil {
		panic(err)
	}

	// 3. Session memory (persisted to JSONL on disk, recoverable after a restart)
	mem, err := memory.NewPersistent("./sessions", nil)
	if err != nil {
		panic(err)
	}

	// 4. The loop
	loop := agent.NewLoop(llm, nil, mem, agent.Config{
		Model:        cfg.DefaultModel,
		SystemPrompt: "你是一个简洁的助手",
	})

	// 5. Run (the next Run with the same sessionID automatically carries history)
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

## Core Concepts

A conversation flows top-down through these layers, and every layer can be replaced independently:

```
Your main
  │  build ProviderConfig (address / protocol / model / API key env var name)
  ▼
adapters.NewLLM ──► core.LLM          unified chat interface (Chat / ChatStream)
  │                                    can be wrapped in middleware: retry / rate limit / fallback…
  ▼
agent.NewLoop(llm, tools, memory, cfg)  ReAct loop: LLM ↔ tools until a result comes out
  │                                     event-stream callback OnEvent emits deltas in real time
  ▼
loop.Run(ctx, sessionID, input)        returns the final message + token usage
  or loop.RunWithHistory(ctx, history, input)   stateless mode
```

| Layer | Responsibility | Not responsible for |
|---|---|---|
| `adapters/provider` | Provider config structs, registry, named quirks library, pricing | Parsing any config file (the format is up to you) |
| `adapters/protocol` | OpenAI / Anthropic / Gemini request building and SSE decoding | Provider differences (delegated to Quirks) |
| `core.Pipeline` | Middleware chain (retry, rate limit, logging, cache, failover) | Business logic |
| `agent.Loop` | ReAct loop, token budget, parallel tools, event stream | Storing conversations (delegated to Memory) |
| `memory` | Reading and writing conversation history isolated per session | Choosing a compression strategy (several implementations to pick from) |
| `tools` | Tool registry; builtin tools; mcp external tools | — |
| `entry` | HTTP / SSE / WS / gRPC entry points | Business authentication and user management |
| `orchestrator` | Multi-agent workflows, checkpoints, resume from checkpoint | LLM call details (reuses agent.Loop) |

---

## Tutorial: Assembling an Agent from Scratch

### Step 1: Define a Provider

```go
cfg := &provider.ProviderConfig{
	ID:           "glm",                                  // unique identifier, used for routing and metrics
	Name:         "Zhipu AI",                            // display name
	Protocol:     "openai",                              // one of three: openai / anthropic / gemini
	BaseURL:      "https://open.bigmodel.cn/api/paas/v4", // API root address
	APIKeyEnv:    "GLM_API_KEY",                         // name of the env var holding the API key
	DefaultModel: "glm-4.6",
	Models: []provider.ModelConfig{{
		ID: "glm-4.6",
		Capabilities: provider.ModelCapabilities{
			Streaming: true, ToolCalls: true, Thinking: true, ContextWindow: 200_000,
		},
		InputPricePerMtok: 1.10, OutputPricePerMtok: 2.21, // optional, used for cost accounting
	}},
}
if err := cfg.LoadAPIKeyFromEnv(); err != nil {
	log.Fatal(err) // the env var is unset or empty
}
```

**`BaseURL` takes only the root address**; endpoints such as `/chat/completions` are appended by the adapter according to the protocol. Supplying a full endpoint produces a duplicated path.

### Step 2: Assemble the LLM and Middleware

```go
llm, err := adapters.NewLLM(cfg) // picks the adapter based on cfg.Protocol
if err != nil {
	return err
}

wrapped := core.StreamRetry(                  // streaming: retry before the first token
	core.NewPipeline(llm,                     // non-streaming: the full middleware chain
		core.Logging(nil),                    // request/response logging; nil uses the default slog
		core.Retry(3),                        // retry non-streaming calls 3 times
	), 3,
)
```

> ⚠️ **Critical**: the agent loop always takes the **streaming** path. Middleware attached only to `core.NewPipeline` (`Logging` / `Retry` / `RateLimit` / `Cache`) has **no effect** on streaming calls; to cover streaming, use the LLM variants (`LoggingLLM` / `RateLimitLLM` / `FallbackLLM` / `StreamRetry`). See [Middleware and Pipeline](#middleware-and-pipeline).

### Step 3: Define a Tool

Implement the four methods of `core.Tool`:

```go
type weatherTool struct{}

// Name is the tool's unique identifier; the model calls it by this name
func (weatherTool) Name() string { return "get_weather" }

// Description lets the model understand the purpose; a clear statement of when to use it markedly improves call accuracy
func (weatherTool) Description() string { return "查询指定城市的当前天气" }

// Parameters is the parameter JSON Schema
func (weatherTool) Parameters() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {"city": {"type": "string", "description": "城市名，如 北京"}},
		"required": ["city"]
	}`)
}

// Execute runs the tool; args is the raw JSON produced by the model
func (weatherTool) Execute(ctx context.Context, args json.RawMessage) (core.ToolResult, error) {
	var in struct {
		City string `json:"city"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return core.ToolResult{}, err // the framework sends the error text back to the model to let it adjust
	}
	return core.ToolResult{Text: in.City + " 晴，26℃"}, nil
}
```

Registering it:

```go
toolReg := tools.NewRegistry()
toolReg.Register(weatherTool{})
```

> **Design principle**: a failing tool must **not** return an error that terminates the loop — the error text is sent back to the model as a tool message so it can retry or take another route. Only LLM errors and iteration overflow terminate the loop. Multiple tool calls in the same round are **executed in parallel**.

### Step 4: Assemble the Loop

```go
loop := agent.NewLoop(wrapped, toolReg, mem, agent.Config{
	Model:         cfg.DefaultModel,     // required
	SystemPrompt:  "你是一个简洁的助手",   // system prompt
	MaxIterations: 16,                   // iteration cap, 16 by default
	TokenBudget:   32_000,               // input token budget, 32000 by default
	Temperature:   nil,                  // *float64; nil uses the provider default
	Thinking:      &core.ThinkingConfig{Enabled: true, BudgetTokens: 4096},
	OnEvent: func(e agent.LoopEvent) {   // real-time event stream
		switch e.Type {
		case agent.EventDeltaText:
			fmt.Print(e.Text) // text delta; printing it directly gives streaming output
		case agent.EventDeltaReasoning:
			fmt.Fprintf(os.Stderr, "[think] %s", e.Reasoning)
		case agent.EventToolCall:
			fmt.Fprintf(os.Stderr, "\n[tool] %s(%s)\n", e.Call.Name, e.Call.Arguments)
		case agent.EventError:
			fmt.Fprintf(os.Stderr, "\n[error] %v\n", e.Err)
		}
	},
	Tracer: tracer, // optional, see the observer package
})
```

### Step 5: Run

```go
msg, usage, err := loop.Run(ctx, "session-1", "北京天气怎么样")
// msg   the final assistant message (Content / Reasoning / ToolCalls)
// usage cumulative token usage for this round
// err   terminating error; ErrMaxIterations means the iteration cap was reached
```

The next `Run` with the same `sessionID` automatically carries the history — this is **stateful mode**. To keep the history on the caller's side, use [stateless mode](#runwithhistory).

---

## API Reference

### core package

`github.com/Lookfukc/tt-agent/pkg/core`

#### LLM Interface

```go
type LLM interface {
	// Chat is a non-streaming call that returns the complete response
	Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error)

	// ChatStream is a streaming call.
	// The producer is responsible for closing the channel; errors are delivered only through StreamError events;
	// the error return value is used solely for failures before the connection is established
	ChatStream(ctx context.Context, req ChatRequest) (<-chan StreamEvent, error)
}
```

#### Messages and Requests

| Type | Description |
|---|---|
| `Message` | Unified internal message format |
| `ChatRequest` | A single chat request |
| `ChatResponse` | The complete response to a chat request |
| `Usage` | Token usage accounting |
| `Role` | Role: `RoleSystem` / `RoleUser` / `RoleAssistant` / `RoleTool` |

**`Message` fields**

| Field | Type | Description |
|---|---|---|
| `Role` | `Role` | Role |
| `Content` | `string` | Text content |
| `ContentParts` | `[]ContentPart` | Multimodal parts; when non-empty the adapter uses them instead of `Content` |
| `ToolCalls` | `[]ToolCall` | Carried only by assistant messages; the tool calls the model requested |
| `ToolCallID` | `string` | Carried only by tool-role messages; identifies which call this result answers |
| `Reasoning` | `string` | Chain of thought, used only for display and accounting, and dropped when sent back to the API |
| `FinishReason` | `FinishReason` | Carried only by aggregated streaming output, and ignored when history is sent back |

Convenience constructors:

```go
core.Text("你好")                                   // build a user text message
core.UserImage("这张图是什么", "data:image/png;base64,...") // build a user message with an image
```

**`ChatRequest` fields**

| Field | Type | Description |
|---|---|---|
| `Model` | `string` | Model ID |
| `Messages` | `[]Message` | Message list |
| `Tools` | `[]ToolSpec` | Tool definitions exposed to the model |
| `Temperature` | `*float64` | nil uses the provider default |
| `MaxTokens` | `int64` | Output cap; 0 uses the provider default |
| `Thinking` | `*ThinkingConfig` | Thinking-mode switch and budget |
| `ResponseFormat` | `*ResponseFormat` | When non-empty, constrains the output to Schema-conforming JSON |
| `Extra` | `map[string]any` | Pass-through channel for provider-specific fields |

**`Usage`**

```go
type Usage struct {
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64 // thinking tokens, counted on the output side for costing
}

usage.Total()        // OutputTokens + ReasoningTokens
usage.Add(other)     // accumulate another usage value (pointer receiver)
```

#### Streaming Events

| Type | Description |
|---|---|
| `StreamEventType` | `StreamStart` / `StreamDeltaText` / `StreamDeltaReasoning` / `StreamDeltaToolCall` / `StreamUsage` / `StreamDone` / `StreamError` |
| `StreamEvent` | A single stream event |
| `ToolCallDelta` | Tool-call argument fragment: `Index` / `ID` (first fragment only) / `Name` (first fragment only) / `ArgsPart` |
| `StreamAccumulator` | Aggregates an event stream into a complete `Message` + `Usage` |

```go
acc := core.NewStreamAccumulator()
for e := range events {
	acc.Feed(e)
}
msg, usage := acc.Message(), acc.Usage()

// or collect everything in one shot
msg, usage, err := core.CollectStream(events)
```

#### Tools

```go
type Tool interface {
	Name() string
	Description() string
	Parameters() json.RawMessage
	Execute(ctx context.Context, args json.RawMessage) (ToolResult, error)
}
```

`ToolResult`:

| Field | Description |
|---|---|
| `Text` | Text result, sent back directly as the tool message content |
| `Data` | Structured result; when non-empty, `Render()` returns its JSON serialization |
| `Images` | Image results (URL or base64) for vision models to consume |

Functional tools (`ToolFunc` implements only `Execute`, so it needs a wrapper before it can be registered):

```go
// ToolFunc signature: func(ctx, args) (ToolResult, error)
// It implements only Execute; Name/Description/Parameters still have to come
// from a struct, so a generic wrapper is needed to plug it into the registry
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

The full semantics of "tool failures do not break the loop": an error returned by `Execute` is turned by the framework into the text `tool error: <reason>` and sent back to the model as a tool message, leaving the model to decide whether to retry or change course. Only an LLM call error or iteration overflow terminates the loop.

#### Memory and TokenCounter

```go
type Memory interface {
	// Add appends messages, isolated by sessionID
	Add(ctx context.Context, sessionID string, msgs ...Message) error

	// Recent returns the most recent messages within budget
	// Implementations truncate from newest to oldest and must preserve system messages
	Recent(ctx context.Context, sessionID string, budget int64) ([]Message, error)

	// Clear empties the session
	Clear(ctx context.Context, sessionID string) error
}

type TokenCounter interface {
	Count(msgs []Message) int64
}
```

#### Middleware and Pipeline

Two tiers of middleware:

| Type | Signature | Coverage |
|---|---|---|
| `ChatMiddleware` | `func(next ChatHandler) ChatHandler` | Wraps only the non-streaming `Chat` |
| `LLMMiddleware` | `func(next LLM) LLM` | Covers both `Chat` and `ChatStream` |

| Constructor | Covered path | Description |
|---|---|---|
| `core.Logging(logger)` | Non-streaming | Request/response logging; `nil` uses the default slog |
| `core.Retry(maxAttempts)` | Non-streaming | Decides whether to retry via `ErrorKind.Retryable()`, with exponential backoff |
| `core.RateLimit(n, window)` | Non-streaming | Token bucket; `n<=0` or `window<=0` means no rate limiting |
| `core.Cache(ttl, maxEntries)` | Non-streaming, **tool-free** requests | Identical requests hit the cache and return immediately; requests carrying tools may have side effects and pass straight through; when the entry count exceeds the cap the whole cache is cleared |
| `core.LoggingLLM(logger)` | Both paths | As above, but covering streaming |
| `core.RateLimitLLM(n, window)` | Both paths | As above, sharing one token bucket |
| `core.FallbackLLM(primary, alternate)` | Both paths | Switches to the alternate when the primary fails; streaming can switch only before the first content event |
| `core.StreamRetry(llm, maxAttempts)` | Streaming | Buffers up to the first content event; failures before it are retried, events after it pass through unchanged |

```go
type Pipeline struct{ /* ... */ }
func NewPipeline(llm LLM, middlewares ...ChatMiddleware) *Pipeline

// composition example (the server's main path is always streaming; pick the LLM variants as needed)
wrapped := core.StreamRetry(
	core.LoggingLLM(nil)(
		core.RateLimitLLM(10, time.Second)(
			core.FallbackLLM(primaryLLM, backupLLM),
		),
	), 3,
)
```

#### Error Types

```go
type ErrorKind int

const (
	ErrInvalidRequest   // bad request parameters; retrying is pointless
	ErrAuth             // authentication failed; the key must be replaced
	ErrPermission       // quota or permission insufficient
	ErrRateLimited      // rate limited; may be retried after a delay
	ErrProviderInternal // provider-side server error; retryable
	ErrNetwork          // network-layer error; retryable
	ErrCanceled         // cancelled by the caller
	ErrUnsupported      // capability unsupported; a configuration error
	ErrExhausted        // retry attempts exhausted
)

func NewError(kind ErrorKind, providerID string, err error) *Error
func ErrorKindOf(err error) ErrorKind // returns ErrInvalidRequest for non-*Error values
func Retryable(err error) bool        // convenience check
func (k ErrorKind) Retryable() bool   // true only for RateLimited / ProviderInternal / Network
func (e *Error) Error() string
func (e *Error) Unwrap() error        // can be traversed with errors.Is / errors.As
```

> The adapters map HTTP status codes and network errors onto the categories above (401→`ErrAuth`, 429→`ErrRateLimited`, 5xx→`ErrProviderInternal`, timeout/connection failure→`ErrNetwork`), and the retry middleware decides accordingly.

#### Tracing

```go
type Span interface {
	SetAttr(key string, value any)
	RecordError(err error)
	End()
}

type Tracer interface {
	StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, Span)
}

func NoopTracer() Tracer                          // no-op implementation
func SpanFromContext(ctx context.Context) Span    // fetch the current span
func WithSpan(ctx context.Context, s Span) context.Context
```

### adapters package

#### `ProviderConfig`

```go
type ProviderConfig struct {
	ID           string          // unique identifier
	Name         string          // display name
	Protocol     string          // "openai" / "anthropic" / "gemini"
	BaseURL      string          // API root address (e.g. https://api.deepseek.com/v1)
	APIKeyEnv    string          // name of the API key env var (the key never lands in code)
	DefaultModel string          // default model
	Models       []ModelConfig   // list of available models
	Quirks       protocol.Quirks // protocol deviation patches (optional)
}
```

| Method | Signature | Description |
|---|---|---|
| `LoadAPIKeyFromEnv` | `() error` | Reads the API key from `APIKeyEnv` and binds it; returns an error when the variable name is empty or unset |
| `APIKey` | `() (string, bool)` | Returns the current API key; `ok=false` means it has not been bound yet |
| `SetAPIKey` | `(key string)` | Sets the key directly (for legacy configs); takes effect within the process |
| `SupportsModel` | `(model string) bool` | Whether the model is registered |
| `Model` | `(model string) (ModelConfig, bool)` | Looks up a model config |
| `ThinkingCapability` | `(model string) bool` | Whether the model supports thinking mode |

#### `ModelConfig` and `ModelCapabilities`

```go
type ModelConfig struct {
	ID                 string
	Name               string
	Capabilities       ModelCapabilities
	InputPricePerMtok  float64 // USD per million input tokens
	OutputPricePerMtok float64 // USD per million output tokens
}

type ModelCapabilities struct {
	Streaming          bool
	ToolCalls          bool
	Thinking           bool
	Vision             bool
	StructuredOutput   bool
	TemperatureSupport bool
	ContextWindow      int64 // number of tokens
}

// CostOf computes the cost from the pricing; thinking tokens count on the output side; anything below 0.5 cents is rounded to zero
func (m ModelConfig) CostOf(u core.Usage) float64
```

#### `provider.Registry`

```go
reg := provider.NewRegistry()
reg.Register(&provider.ProviderConfig{ /* ... */ }) // an existing ID is overwritten

cfg, ok := reg.Get("deepseek")   // look up a config; ok=false means it is not registered
cfg = reg.MustGet("deepseek")    // panics when absent; use it only for static startup wiring
ids := reg.List()                // every provider ID
```

#### Assembling the LLM

```go
// assemble directly from a single config
func NewLLM(cfg *provider.ProviderConfig) (core.LLM, error)

// assemble from a registry by ID
func NewLLMFromRegistry(r *provider.Registry, id string) (core.LLM, error)
```

#### Named Quirks

Provider protocol deviations are not handled through subclass inheritance; they all go through the **named patch library**:

```go
quirks, err := provider.ComposeQuirks([]string{"glm-thinking", "deepseek-reasoner"}, "openai")
if err != nil {
	return err // unknown name or protocol mismatch; the error lists every available name
}
cfg.Quirks = quirks

names := provider.QuirkNames() // list every available patch name
```

| Name | Effect | Applicable protocol |
|---|---|---|
| `glm-thinking` | Translates the unified `Thinking` config into GLM's private `thinking` field | openai |
| `deepseek-reasoner` | Removes `temperature` / `top_p` when requesting `deepseek-reasoner` (otherwise 400) | openai |

Multiple names are composed and applied in declaration order.

#### Protocol Adapters (generally not used directly)

```go
func NewOpenAI(providerID, baseURL, apiKey string, quirks Quirks) *OpenAIProtocol
func NewAnthropic(providerID, baseURL, apiKey string, quirks Quirks) *AnthropicProtocol
func NewGemini(providerID, baseURL, apiKey string, quirks Quirks) *GeminiProtocol
```

### agent package

#### `Config`

| Field | Type | Default | Description |
|---|---|---|---|
| `SystemPrompt` | `string` | empty | System prompt |
| `Model` | `string` | — | Model ID |
| `Temperature` | `*float64` | nil | nil uses the provider default |
| `Thinking` | `*core.ThinkingConfig` | nil | Thinking-mode config |
| `MaxIterations` | `int` | **16** | Upper bound on LLM↔tool round trips; `<=0` uses the default |
| `TokenBudget` | `int64` | **32000** | Input token budget for a single LLM call; `<=0` uses the default |
| `OnEvent` | `func(LoopEvent)` | nil | Progress callback; **a blocking callback slows the whole loop down** |
| `Tracer` | `core.Tracer` | no-op | Tracing |

```go
func NewLoop(llm core.LLM, toolReg *tools.Registry, mem core.Memory, cfg Config) *Loop
```

Passing `nil` for `mem` means purely stateless use (in that case `Run` returns an error — use `RunWithHistory` instead).

#### `Run`

```go
func (l *Loop) Run(ctx context.Context, sessionID, input string) (core.Message, core.Usage, error)
```

| Parameter | Description |
|---|---|
| `ctx` | Cancelling it interrupts the current LLM call or tool execution |
| `sessionID` | Session identifier; both history and new messages land in this session |
| `input` | User input |

Returns the final assistant message, the cumulative usage for the whole round, and a terminating error.

#### `RunWithHistory`

Stateless mode: the caller holds the history, and the framework does the assembly internally.

```go
func (l *Loop) RunWithHistory(ctx context.Context, history []core.Message, input string) (RunResult, error)

type RunResult struct {
	Message     core.Message   // the final assistant answer
	NewMessages []core.Message // every message added this round (including the input), for the caller to persist
	Usage       core.Usage     // cumulative usage for the whole round
}
```

| Parameter | Description |
|---|---|
| `ctx` | Cancelling it interrupts the current LLM call or tool execution |
| `history` | The caller's full history in its original order; may be `nil` to start a brand-new conversation |
| `input` | User input |

Behavior notes:

- History is read-only — the framework **never** mutates the slice you pass in
- Over-budget history is truncated automatically by atomic groups (system messages are preserved), so the caller need not control the length
- `assistant(tool_calls)` and its `tool` results **come and go together**, so no orphan tool messages are produced (those would cause a 400)
- The `SessionID` in event callbacks is always `"stateless"`
- On error, `NewMessages` still carries the messages produced so far; the caller may persist partial results or discard the whole round
- **The run never touches any memory attached to the `Loop`**

Typical usage:

```go
// read the history from your database
history, _ := db.LoadMessages(ctx, conversationID)

res, err := loop.RunWithHistory(ctx, history, userInput)
if err != nil {
	return err
}
// persist the messages added this round
_ = db.AppendMessages(ctx, conversationID, res.NewMessages)
fmt.Println(res.Message.Content)
```

#### Loop Events

```go
type LoopEvent struct {
	SessionID string      // the session this event belongs to
	Iter      int         // which round
	Type      LoopEventType
	Text      string      // text delta or tool name
	Reasoning string
	Call      *core.ToolCall
	Usage     *core.Usage // only EventDone carries cumulative usage
	Err       error
}
```

| Event | Payload fields | Meaning |
|---|---|---|
| `EventIterStart` | `Iter` | Entering round N of the LLM↔tool round trip |
| `EventDeltaText` | `Text` | Text delta (concatenated, it is the full reply) |
| `EventDeltaReasoning` | `Reasoning` | Thinking-chain delta (only when the model supports it) |
| `EventToolCall` | `Call` | The model requests a tool call |
| `EventToolResult` | `Call`, `Err` | Tool execution finished (a non-nil `Err` means failure) |
| `EventDone` | `Text`, `Usage` | The loop converged, carrying the final text and cumulative usage |
| `EventError` | `Err` | The loop terminated with an error |

```go
var ErrMaxIterations = errors.New("agent loop: max iterations exceeded")
```

> **Concurrency note**: `Loop` itself is stateless, so a single `loop` can serve multiple sessions concurrently; in that case `OnEvent` arrives from **multiple goroutines**, so the callback must be concurrency-safe and route by `e.SessionID`.

### memory package

#### Interfaces

```go
// core.Memory: all three implementations satisfy it
type Memory interface {
	Add(ctx context.Context, sessionID string, msgs ...Message) error
	Recent(ctx context.Context, sessionID string, budget int64) ([]Message, error)
	Clear(ctx context.Context, sessionID string) error
}

// optional capability interfaces: decorators use them to enable advanced features
type Splitter interface {       // implemented by Persistent / memorytest.Buffer
	Split(ctx context.Context, sessionID string, budget int64) (kept, dropped []Message, err error)
}
type Trimmer interface {        // implemented by Persistent / memorytest.Buffer
	Trim(ctx context.Context, sessionID string, n int) error
}
type SummaryStore interface {   // implemented by Persistent; summaries are persisted with the session
	SaveSummary(ctx context.Context, sessionID string, covered int, text string) error
	LoadSummary(ctx context.Context, sessionID string) (text string, covered int, err error)
}
```

#### Choosing an Implementation

| Implementation | Persistence | Memory footprint | Shared across instances | Use case |
|---|---|---|---|---|
| `Persistent` | JSONL / session | Bounded (LRU resident) | ❌ single machine | **Single-node default, zero dependencies** |
| `sqlite.Driver` | single-file database | None (direct queries) | ✅ multi-process on one host | Single node, several processes, SQL queries |
| `redis.Driver` | Redis | None (direct queries) | ✅ across machines | **Multi-instance deployments, native TTL** |
| `postgres.Driver` | PostgreSQL | None (direct queries) | ✅ across machines | Teams already running Postgres, transactions/auditing |
| `Summary(inner, llm)` | Depends on inner | Same as inner | Same as inner | When long context must not lose information |
| `TTL(inner, ...)` | Depends on inner | Bounded | Same as inner | Evicts idle sessions |
| `memorytest.Buffer` | No | Unbounded | ❌ | **Testing only** |

> The pure in-process memory implementation has been removed from the public API (it is easy to misuse in production: gone on restart, unbounded residency). Use the `pkg/memory/memorytest` subpackage for tests and temporary demos.

**Every backend shares one set of semantics** — budget truncation, atomic `tool_calls` grouping, system-message retention — because they reuse the same `internal/sessionlog` implementation and are covered by the same contract test suite. Swapping a backend never changes conversation behavior.

### External Storage Backends

All three external backends share one driver contract: `pkg/memory/memorystore` defines `Driver` (which only stores and retrieves bytes) while budget accounting and truncation live in `Store`. A driver therefore never needs to understand token budgets, and they cannot drift apart.

```go
// Redis: shared across instances + native TTL (idle sessions expire
// inside Redis, with no sweeper goroutine in your process)
d, err := redis.NewFromURL(ctx, "redis://localhost:6379/0", redis.Options{
	SessionTTL: 30 * time.Minute,
})
if err != nil {
	return err
}
defer d.Close()
mem := d.Memory(memorystore.Options{})

// SQLite: one file, multi-process safe, pure Go with no CGO
d, err := sqlite.New("./sessions.db", sqlite.Options{})
if err != nil {
	return err
}
defer d.Close()
if err := d.Migrate(ctx); err != nil { // creates tables; idempotent
	return err
}
mem := d.Memory(memorystore.Options{})

// PostgreSQL: reuse an existing server; create the schema before Migrate
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

The `*memorystore.Store` returned by all three implements `core.Memory` plus `Splitter`, `Trimmer` and `SummaryStore`, so the decorators apply unchanged:

```go
mem = memory.NewCompactingSummary(mem, cheapLLM) // summary compaction still works
mem = memory.NewTTL(ctx, mem, 30*time.Minute, 5*time.Minute)
```

| Backend | Keys / tables | Retention |
|---|---|---|
| Redis | `agent:mem:{id}` (LIST), `:system` (LIST), `:sum` (STRING) | Native `EXPIRE` (`SessionTTL`) |
| SQLite | `agent_messages` / `agent_summaries` tables | Call `PruneIdleSessions(idle)` on a timer |
| Postgres | Same tables (isolated per schema) | Call `PruneIdleSessions(idle)` on a timer |

> **Why Redis keeps two lists:** the parallel `system` list records which entries are system messages, so `Trim` can locate the oldest *non-system* messages without decrypting or parsing every message (system messages are never trimmed). The cost is that writes go through a pipeline to keep both lists the same length.
>
> **Trim atomicity:** the whole trim runs as a Lua script on the Redis server's single thread. A client-side read-rebuild-rename implementation has a lost-update race — a message appended by another process between the LRANGE and the RENAME is silently swallowed by the rename. While a script executes, no other command can interleave, so a concurrent `Add` lands entirely before or entirely after a trim.

### Encryption at Rest

Encryption lives at the **codec boundary**, not inside drivers — so all three backends plus the local JSONL store gain it automatically, and no driver ever handles a key:

```go
codec, err := memorystore.NewEncryptedCodec([]byte(os.Getenv("MEMORY_KEY")), nil)
if err != nil {
	return err
}
mem := d.Memory(memorystore.Options{Codec: codec}) // identical for any driver
```

- Algorithm: **AES-256-GCM**; the key may be any length (SHA-256 derives 32 bytes internally)
- Record format: `base64("ttae1" || nonce(12) || ciphertext || tag)`, version-prefixed so the format can evolve
- The nonce doubles as the GCM additional data, so ciphertext cannot be recombined with a different nonce
- **Backward compatible**: records without the version prefix are read as plaintext, so an existing dataset can be encrypted in place with no rewrite
- A wrong key or tampered data is treated as a corrupt record (`ErrDecrypt`), which never takes a whole session down

> ⚠️ This protects **data at rest** (database files, Redis snapshots, leaked backups). Transport still needs TLS, and the key itself belongs in a KMS or environment variable — never in code.

### Snapshots and Rollback (Time Travel)

External drivers store an append-only log: you can append and truncate, but you cannot ask "what did this session look like three turns ago", nor undo a bad turn. `pkg/memory/snapshot` adds that layer **without changing the `core.Memory` contract**:

```go
snaps := snapshot.New(driver, snapshot.Options{}) // wraps the Driver, shares the same storage

entry, err := snaps.Capture(ctx, "session-1", "before-tool-call")
// ... the model takes a turn that does not go well ...
if _, err := snaps.Rollback(ctx, "session-1", entry.ID); err != nil {
	return err
}

list, err := snaps.List(ctx, "session-1")  // newest first
err = snaps.Delete(ctx, "session-1", entry.ID)
```

| Parameter / method | Description |
|---|---|
| `New(driver, Options{Codec, MaxPerSession, Prefix, Observer, Backend})` | `MaxPerSession` defaults to 20; older snapshots are evicted |
| `Capture(ctx, sessionID, label)` | Records all current messages; returns `Entry{ID, Label, MessageCount, CreatedAt}` |
| `Rollback(ctx, sessionID, snapshotID)` | **Destructive**: replaces the session log with the snapshot; `ErrNoSnapshot` when missing |
| `RollbackSafe(ctx, sessionID, snapshotID, safetyLabel)` | **Non-destructive**: captures the current state as a safety snapshot first, then rolls back, returning `(restored, safety)` — rolling back to `safety.ID` undoes the rollback |
| `List` / `Delete` | Enumeration (newest first) and single-snapshot removal |

`RollbackSafe` guarantees two things:

- **A mistyped target ID leaves no junk snapshot**: the target is validated first; a missing ID returns `ErrNoSnapshot` without polluting the snapshot log
- **Capture and rollback are serialized under one lock**: no concurrent write can land between the two steps, so the discarded state is guaranteed to be intact inside the safety snapshot — which is the entire point of the method

**Cross-process serialization**: when the driver implements `memorystore.SessionLocker` (Redis via `SET NX PX` plus a token-checked release script; Postgres via transaction-scoped advisory locks that evaporate when the connection dies; SQLite via a lock table — token-checked release plus lease expiry reclamation, with the table created lazily on first use in older databases), snapshot operations additionally hold that lock, making the capture-then-rollback sequence atomic against other processes sharing the storage — dual-process concurrent `RollbackSafe` and crash-recovery of the lock are covered by tests on all three backends. Only the local JSONL file backend lacks the capability; its guarantee holds within one process.

Key points:

- Snapshots are encoded through the same `Codec`, so **snapshots are ciphertext when encryption is on** (covered by tests)
- Snapshots live under a derived `snapshot:<sessionID>` key and **never mix into the conversation**
- Rollback is destructive by design; `Capture` first if the current state should stay recoverable
- Restore passes the recorded payloads through verbatim rather than re-encoding them

### Long-Term Memory and Vector Search

Everything above answers "does it remember this conversation"; `pkg/ltm` answers "does it still remember *you* next time". The two are deliberately separate: session history is an **ordered log read chronologically**, long-term memory is an **unordered set of facts read by relevance**.

```go
store := ltm.NewMemoryStore(ltm.MemoryOptions{MaxFactsPerNamespace: 1000})
mem := ltm.New(store, ltm.Options{
	Embedder:  myEmbedder,   // optional: falls back to keyword matching
	Extractor: myExtractor,  // optional: without it only explicit Remember works
})

// Store a fact explicitly
fact, err := mem.Remember(ctx, userID, "The user is allergic to peanuts", map[string]string{"source": "session-42"})

// Recall by relevance
facts, err := mem.Recall(ctx, userID, "recommend a restaurant", 5)
if prompt := ltm.Prompt(facts); prompt != "" {
	// Inject as a system message so the model sees durable context
	messages = append(messages, core.Message{Role: core.RoleSystem, Content: prompt})
}

// Mine facts from a finished conversation (requires an Extractor)
learned, err := mem.Learn(ctx, userID, conversation)

// Management
err = mem.Forget(ctx, userID, fact.ID)
list, err := mem.List(ctx, userID, 20)
err = store.Clear(ctx, userID)
```

| Concept | Description |
|---|---|
| `Fact` | One statement: `ID` / `Namespace` / `Text` / `Metadata` / timestamps / `Score` |
| `Namespace` | Isolation dimension (usually a user ID); **never visible across namespaces**, and an empty namespace is rejected |
| `Store` | Storage interface: `Upsert` / `Search` / `List` / `Delete` / `Clear` |
| `MemoryStore` | Built-in in-process implementation with vector search and a size cap |
| `pgvector.Store` | PostgreSQL + pgvector implementation: shared, durable, true vector search |
| `Embedder` | Text to vector; `EmbedderFunc` adapts any provider in one line |
| `Extractor` | Distills conversations into facts; `ExtractorFunc` likewise |
| `Prompt(facts)` | Renders recalled facts as a system-prompt fragment |
| `KeywordScore` | Exported keyword-scoring function so external Store implementations degrade with consistent ranking |

#### Ready-to-Use Embedders (`pkg/ltm/embeddings`)

Three adapters share one HTTP kernel and differ only in authentication; each implements `ltm.Embedder` directly:

```go
// OpenAI-compatible: OpenAI / DeepSeek / GLM / local ollama, vLLM (Bearer auth)
emb := embeddings.NewOpenAICompatible("https://api.openai.com/v1", apiKey, "text-embedding-3-small")

// Anthropic (Voyage-powered /v1/embeddings; x-api-key + anthropic-version auth)
emb = embeddings.NewAnthropic(apiKey, "voyage-3-large")

// Google Gemini (:embedContent endpoint; x-goog-api-key auth)
emb = embeddings.NewGemini(apiKey, "gemini-embedding-001")

mem := ltm.New(store, ltm.Options{Embedder: emb})
```

| Option | Description |
|---|---|
| `WithHTTPClient(hc)` | Inject a custom HTTP client (proxies / tests) |
| `WithBaseURL(base)` | Override the default API root (gateways / local proxies) |
| `WithDimensions(n)` | Request a specific output dimension (supported by OpenAI text-embedding-3 and Voyage models; Gemini fixes dimension per model and ignores it) |

Behavior notes:

- Error messages **always carry a snippet of the provider's response body** — a schema drift in a preview API is diagnosable without a packet capture
- Empty text returns nil without a request, matching the facade's degrade semantics
- 30-second default timeout, immediate context cancellation, safe for concurrent use
- ⚠️ **The Anthropic adapter has not been tested against the live endpoint**: the build environment cannot reach Anthropic domains, and the implementation follows their published preview-API description (endpoint shape locked by hermetic tests; auth via `x-api-key` + `anthropic-version`). If fields have drifted, the first real call's error will carry the server's response for immediate diagnosis
- ⚠️ **Switching embedders usually means switching vector dimensions** — with pgvector that means a new table (`Dim` is fixed at creation)

#### Built-in LLM Extractor (`pkg/ltm/extractor`)

Built on the framework's own `core.LLM` — protocol adapters, middleware and retry all inherited; point it at the LLM your agent already uses, or a cheaper one:

```go
ext := extractor.New(llm, extractor.Options{
    MaxMessages: 100,   // only the most recent N messages are considered
    Lenient:     true,  // LLM/parse failures return no facts instead of erroring — learning is a nicety and must not break the flow that triggered it
})
mem := ltm.New(store, ltm.Options{Embedder: emb, Extractor: ext})

learned, err := mem.Learn(ctx, userID, conversation) // automatic fact extraction after a conversation
```

The parser tolerates models wrapping JSON in markdown fences and prose (slices from the first `[` to the last `]`); blank facts are dropped; an empty conversation makes no LLM call. Strict by default (a parse failure errors); `Lenient: true` degrades silently.

#### The pgvector Backend (PostgreSQL Vector Search)

`pkg/ltm/pgvector` puts long-term memory into PostgreSQL with the pgvector extension — shared across processes, durable, with cosine ranking in SQL. Vectors travel in pgvector's text format (`'[1,2,3]'::vector`), so there is **no extra client-side dependency**:

```go
store, err := pgvector.NewFromURL(ctx, "postgres://user:pass@host/db", pgvector.Options{
	Dim:    1536, // fixed at table creation; switching embedding providers means a new table
	Schema: "agent",
})
if err != nil {
	return err
}
defer store.Close()
if err := store.Migrate(ctx); err != nil { // extension + table; idempotent; needs CREATE EXTENSION rights
	return err
}
mem := ltm.New(store, ltm.Options{Embedder: myEmbedder})
```

Behavior aligns strictly with the in-process implementation:

- **Dimension mismatch degrades to a NULL vector**: after switching embedding providers, old facts stay reachable by keyword and writes never fail
- **Non-finite values (NaN/Inf) also store NULL**: pgvector would reject them; losing ranking beats losing the fact
- **Vector ranking happens in SQL** (`ORDER BY embedding <=> $query`); the keyword fallback ranks in Go with the shared `KeywordScore` — the same scoring the built-in store uses
- The connecting role must own the database or hold `CREATE EXTENSION` privileges (Migrate reports this clearly)

Design notes:

- **Deduplication via stable IDs**: re-learning the same fact updates one row (a hash of `namespace + normalized text`) instead of piling up duplicates, with no extra LLM "merge or add" call
- **Degrade without losing data**: when embedding fails the fact is still stored and still reachable by keyword — losing a user's preference is worse than ranking it poorly
- **One ranking strategy per call**: either all vector similarity or all keyword overlap, so scores stay comparable
- **Size cap**: `MaxFactsPerNamespace` defaults to 1000 and evicts by insertion order, not timestamps (same-nanosecond writes would order unstably)
- **Dimension mismatch never panics**: switching embedding providers yields zero similarity, so those facts are filtered rather than crashing a live request

#### `Persistent`

```go
// New: creates dir if it does not exist; a nil counter uses the built-in rough estimate
func NewPersistent(dir string, counter core.TokenCounter) (*Persistent, error)

// With an in-memory residency cap: over the limit, the least recently used sessions are unloaded (memory only; the data stays on disk)
func NewPersistentWithLRU(dir string, counter core.TokenCounter, maxLoaded int) (*Persistent, error)
```

| Parameter | Description |
|---|---|
| `dir` | Session file directory; one `<sessionID>.jsonl` per session |
| `counter` | Token estimator; `nil` uses the built-in character-based estimate (conservative — better under than over) |
| `maxLoaded` | Cap on sessions resident in memory; `0` means unlimited; on the order of 1024 is recommended |

Features:

- **Write-through**: every message is appended to disk, so a process crash loses at most the last one
- **Lazy loading**: files are read on first access, so tens of thousands of historical sessions do not slow down startup or inflate memory
- **Corrupt-line tolerance**: corrupt lines and over-long lines (>4MB) are skipped without interrupting recovery
- **Old/new format compatibility**: the new format carries a `{ts, msg}` envelope while the old one stores bare messages; both may be mixed in one file with zero migration
- **Atomic rewrite**: `Trim` uses `temp + rename`, so a crash never leaves half-written state
- **Path safety**: a `sessionID` containing `/`, `\`, `..`, or longer than 128 characters is always rejected
- **Summary persistence**: implements `SummaryStore`; summaries are stored in `<sessionID>.summary` and deleted together by `Clear`

#### The `Summary` Family

```go
func NewSummary(inner core.Memory, llm core.LLM) *Summary
func NewSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary
func NewCompactingSummary(inner core.Memory, llm core.LLM) *Summary
func NewCompactingSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary
```

| Constructor | Old-message handling | Summary persistence |
|---|---|---|
| `NewSummary` | **Does not delete**; fully retained (audit-friendly, unbounded footprint) | Persisted when the inner implementation provides `SummaryStore` |
| `NewCompactingSummary` | **Physically deletes** after a successful summary (the footprint shrinks along with the summary) | Same as above |
| `...WithCounter` | Same as the left column | Additionally injects one token estimator consistent across the inner and outer layers |

| Parameter | Description |
|---|---|
| `inner` | The actual store, e.g. `Persistent` (which may itself be wrapped in `TTL`) |
| `llm` | The model used for compression; **a cheap small model is recommended** |
| `c` | Token estimator; use it when the inner and outer layers must agree |

Behavior notes:

- The summary is cached keyed on the "number of messages folded in": the same truncation point is never compressed twice, and as the truncation point advances only the increment is compressed and merged into the previous summary
- If compression fails (the LLM errors), it **degrades to plain truncation**, writes no cache, and retries on the next round
- The summary is inserted as a system message after the system message and before the retained history
- Compression requests truncate each message to 2KB and cap at 200 messages, to control the cost of compression itself

#### `TTL`

```go
func NewTTL(ctx context.Context, inner core.Memory, idle, sweep time.Duration) *TTL
```

| Parameter | Default | Description |
|---|---|---|
| `ctx` | — | Janitor lifetime; once cancelled, no further eviction happens |
| `inner` | — | The actual store |
| `idle` | **30 minutes** | How long idle before eviction; `<=0` uses the default |
| `sweep` | **5 minutes** | Check interval; `<=0` uses the default, and it should be less than `idle` |

Idleness is judged by **last active time** (long sessions are not killed midway), and a single background janitor sweeps periodically; for a `Persistent` inner store, `Clear` also deletes the on-disk file — this is the unified exit for unbounded growth in both memory and disk.

#### Recommended Production Combination

```go
inner, _ := memory.NewPersistentWithLRU("./sessions", nil, 1024)
mem := memory.NewTTL(ctx, inner, 30*time.Minute, 5*time.Minute)
mem = memory.NewCompactingSummary(mem, cheapLLM)

// or simply use the server defaults (temp dir + LRU 1024)
srv := entry.NewServer(reg, toolReg)
```

### tools package

#### `tools.Registry`

```go
reg := tools.NewRegistry()

reg.Register(tool)              // register; the same name overwrites
t, ok := reg.Get("calculator")  // look up a tool
specs := reg.Specs()            // export every ToolSpec (used internally to build requests)
t, err := reg.MustGet("x")      // returns an error when absent
```

#### Built-in Tools

| Tool | Constructor | Parameters | Returns | Description |
|---|---|---|---|---|
| `calculator` | `builtin.NewCalculator()` | `expression` (string) | `{"value": 3}` | `go/ast` whitelist evaluation that executes no arbitrary code; supports `+ - * / %`, not `^` |
| `clock` | `builtin.NewClock()` | none | `{"now": "2026-01-01T00:00:00Z"}` | Current time in RFC3339 |
| `http_fetch` | `builtin.NewHTTPFetch()` | `url` (string) | Response text (truncated to 64KB) | **SSRF surface**; loopback/private/link-local targets are rejected by default |

```go
// explicit allow-listing for http_fetch (internal-network deployments or local tests only)
fetch := builtin.NewHTTPFetchWithOptions(builtin.WithAllowPrivateTargets(true))

reg.Register(builtin.NewCalculator())
reg.Register(builtin.NewClock())
if enableFetch {
	reg.Register(fetch) // not registered by default in production
}
```

> `http_fetch`'s private-network checks are enforced **at dial time** (guarding against DNS-rebinding TOCTOU); redirects are followed manually hop by hop and re-validated on every hop, up to 5 hops, with a 15-second timeout.

#### MCP Tool Integration

Attach every tool of an external MCP server to the registry, and the model can call them just like built-in tools:

```go
// stdio: a local process
func ConnectStdio(ctx context.Context, name, command string, args ...string) (*Client, error)

// Streamable HTTP: a remote server (apiKey may be empty)
func ConnectHTTP(name, url, apiKey string) *Client

// custom transport (any line-framed bidirectional stream)
func NewClient(rw interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
}, name string) *Client

func (c *Client) Connect(ctx context.Context) error            // initialize handshake, idempotent
func (c *Client) ListTools(ctx context.Context) ([]core.ToolSpec, error)
func (c *Client) CallTool(ctx context.Context, name string, args json.RawMessage) (core.ToolResult, error)
func (c *Client) Register(ctx context.Context, reg *tools.Registry) error // handshake + tools/list + register everything
func (c *Client) Close() error
```

```go
client, err := mcp.ConnectStdio(ctx, "my-tools", "npx", "-y", "some-mcp-server")
if err != nil {
	panic(err)
}
defer client.Close()

if err := client.Register(ctx, toolReg); err != nil { // handshake + tools/list + registration
	panic(err)
}
```

### entry package

#### `Server` and `Config`

```go
type Config struct {
	Addr            string // listen address, default ":8080"
	DefaultProvider string // default provider ID
	DefaultModel    string // default model (falls back to the provider's DefaultModel when empty)
	SystemPrompt    string // default system prompt
	MaxIterations   int    // 16 by default
	TokenBudget     int64  // 32000 by default
}

func NewServer(registry *provider.Registry, toolReg *tools.Registry, opts ...Option) *Server

type Option func(*Server)
func WithConfig(cfg Config) Option                       // override configuration
func WithMemory(mem core.Memory) Option                  // replace the memory implementation
func WithLLMFactory(f LLMFactory) Option                 // replace LLM assembly (test injection point)

// LLMFactory assembles an LLM by providerID
type LLMFactory func(providerID string) (core.LLM, error)

func DefaultMemoryDir() string                           // default memory directory (tt-agent-sessions under the temp dir)
func (s *Server) Handler() http.Handler                  // handler with routes mounted
func (s *Server) Metrics() *observer.Metrics             // metrics aggregator
func (s *Server) CloseWebSockets()                       // close every in-flight WS connection
func (s *Server) GRPCRegister(gs *grpc.Server)           // register the gRPC service
```

**Default memory behavior**: without `WithMemory`, the server uses `Persistent` + LRU 1024 under `os.TempDir()/tt-agent-sessions`. Sessions survive a restart on the same machine; **for real deployments pass `WithMemory` with a dedicated directory** so it is not shared with other applications.

#### `cmd/server` Command-Line Service

`cmd/server` is a ready-made server for a single provider, configured with flags + env, and can be `go run` directly:

```bash
export DEEPSEEK_API_KEY=sk-xxx
go run github.com/Lookfukc/tt-agent/cmd/server@latest \
  --base-url https://api.deepseek.com/v1 \
  --model deepseek-chat \
  --api-key-env DEEPSEEK_API_KEY
```

| flag | Type | Default | Description |
|---|---|---|---|
| `--addr` | string | `:8080` | HTTP listen address |
| `--grpc` | string | empty | gRPC listen address; empty leaves it disabled |
| `--provider` | string | `main` | Provider ID label (used for routing and metric aggregation) |
| `--protocol` | string | `openai` | `openai` / `anthropic` / `gemini` |
| `--base-url` | string | **required** | API root address; must be an absolute http(s) URL |
| `--api-key-env` | string | **required** | Name of the env var holding the API key (the key never goes on the command line) |
| `--model` | string | **required** | Model ID |
| `--prompt` | string | empty | Default system prompt |
| `--enable-httpfetch` | bool | `false` | Register the `http_fetch` tool (an SSRF surface, off by default) |
| `--httpfetch-allow-private` | bool | `false` | Allow `http_fetch` to reach internal networks |
| `--memory-type` | string | `file` | Session memory backend: `file` / `sqlite` / `redis` / `postgres` |
| `--memory-dir` | string | empty | `file` backend: session directory; empty uses the shared temp directory |
| `--memory-max-loaded` | int | `1024` | `file` backend: cap on sessions resident in memory; beyond it, LRU unloads |
| `--memory-dsn` | string | empty | `sqlite`: file path; `redis`: `redis://host:port/db`; `postgres`: connection string |
| `--memory-redis-prefix` | string | empty | `redis` backend: key prefix (default `agent:mem:`) |
| `--memory-ttl` | duration | `0` | Session retention: `redis` expires natively; `sqlite`/`postgres` sweep it every `--memory-prune-every` (0 = keep forever) |
| `--memory-prune-every` | duration | `5m` | `sqlite`/`postgres`: how often the idle-session sweep runs |

For the multi-provider per-request routing scenario, write your own `main`, register several providers, and then use `entry.NewServer`.

#### HTTP Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/chat` | POST | Chat; returns SSE when `stream: true` |
| `/api/chat/ws` | GET | WebSocket entry point (RFC 6455) |
| `/api/providers` | GET | Registered providers and their model lists |
| `/api/metrics` | GET | Per-provider call counts / errors / tokens |
| `/api/health` | GET | Liveness probe |

#### `POST /api/chat` Request Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `input` | string | **yes** | User input |
| `session_id` | string | no | Session ID; the same ID automatically carries history (**stateful mode**) |
| `messages` | []Message | no | Full history (**stateless mode**); **mutually exclusive** with `session_id` |
| `stream` | bool | no | `true` uses SSE |
| `provider_id` | string | no | Select a provider (routes per request when several are registered) |
| `model` | string | no | Select a model |
| `system_prompt` | string | no | Override the default system prompt |

The request body limit is 4MB.

**Stateful-mode response**:

```json
{
  "session_id": "s1",
  "content": "你好！有什么可以帮你？",
  "reasoning": "",
  "usage": {"InputTokens": 12, "OutputTokens": 8, "ReasoningTokens": 0},
  "cost_usd": 0.0012
}
```

**Stateless-mode response**:

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

#### SSE Events

```bash
curl -N localhost:8080/api/chat -d '{"input":"帮我查下天气","stream":true}'
```

| event | data fields | Description |
|---|---|---|
| `text` | `delta` | Text delta |
| `reasoning` | `delta` | Thinking-chain delta |
| `tool_call` | `id`, `name`, `arguments` | The model requests a tool call |
| `tool_result` | `id`, `name`, `error` | Tool execution finished |
| `error` | `message` | Error inside the loop |
| `done` / `error` | `content`, `reasoning`, `usage`, `cost_usd`, `error`; stateless mode additionally carries `new_messages` | Terminal event; the payload shape is identical |

The terminal event is `done` (success) or `error` (failure). A client disconnect cancels the entire loop (except for in-flight tool execution).

#### WebSocket

`GET /api/chat/ws`, a zero-dependency RFC 6455 implementation. One inbound text frame = one chat request (the same JSON as `/api/chat`); the server serializes Loop events into JSON frames and pushes them back. A disconnect cancels the current loop, and ping/pong is supported (60-second heartbeat).

#### gRPC

The service is defined in `pkg/entry/grpc_desc.go`, with this equivalent .proto contract:

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

When the environment has no protoc, the framework builds the descriptor plus `dynamicpb` messages at runtime, with **zero generated code**; the messages contain only scalar types and have no WKT dependencies:

```go
conn.Invoke(ctx, "/agentframework.AgentService/Chat", req, resp)

// clients can build dynamic messages from the framework-exported descriptor
desc := entry.GRPCChatRequestDesc()
msg := dynamicpb.NewMessage(desc)
```

### orchestrator package

#### Construction and Registration

```go
func New(store RunStore) *Orchestrator

func (o *Orchestrator) RegisterAgent(name string, loop *agent.Loop)
func (o *Orchestrator) RegisterWorkflow(wf *Workflow) error
func (o *Orchestrator) Run(ctx context.Context, wfName, input string) (*RunState, error)
func (o *Orchestrator) Resume(ctx context.Context, runID, humanInput string) (*RunState, error)
func (o *Orchestrator) Get(runID string) (*RunState, bool)
```

| Method | Parameters | Returns |
|---|---|---|
| `New` | `store`: the run-state store; `nil` means no persistence (state is lost on restart) | The orchestrator |
| `RegisterAgent` | `name`: a unique name (referenced by `AgentStep.Agent`); `loop`: an ordinary `agent.Loop` | — |
| `RegisterWorkflow` | `wf`: the workflow definition; an empty name is an error, the same name overwrites | error |
| `Run` | `wfName`, `input` (referenced by the step template `$input`) | Run state; **returns `ErrCheckpoint` when it stops at a checkpoint** |
| `Resume` | `runID`, `humanInput` (fed to the next step as `$prev`) | The run state after resuming |
| `Get` | `runID` | State plus whether it exists |

```go
var ErrCheckpoint = errors.New("workflow paused at checkpoint")
```

`Orchestrator` also has an optional `Tracer core.Tracer` field (producing `workflow.run` / `workflow.step` spans).

#### Step Types

```go
type Step interface{ stepKind() } // closed sum type
```

**`AgentStep`**

| Field | Description |
|---|---|
| `Name` | Step name, referenced by `$step.<name>.output`; falls back to the agent name when empty |
| `Agent` | Name of a registered agent |
| `Input` | Input template |

**`CheckpointStep`**

| Field | Description |
|---|---|
| `Name` | Checkpoint name |
| `Prompt` | Prompt text for the human reviewer |

**`RouterStep`** (multi-agent routing)

| Field | Description |
|---|---|
| `Name` | Step name |
| `Router` | Name of the classifier agent; its output **must be** one of `Candidates`, otherwise the step fails (no fuzzy matching) |
| `Candidates` | List of agents eligible to execute |
| `Input` | Input template handed to the selected agent |

**`SupervisorStep`** (supervisor delegation)

| Field | Description |
|---|---|
| `Name` | Step name |
| `Supervisor` | Name of the supervisor agent |
| `Workers` | List of worker agents available for delegation |
| `Input` | Initial task template |
| `MaxRounds` | Maximum number of rounds; `0` uses the default of 8 |

The supervision protocol is plain text: when the supervisor emits `WORKER <name>` on the first line it delegates (the rest is the task description), and when the first line is `DONE` it converges (the rest is the final answer).

#### Template Variables

Only **whole-string** template matching is performed (no substring interpolation, to avoid introducing escaping rules):

| Variable | Value |
|---|---|
| `$input` | The workflow's initial input |
| `$prev` | The previous step's output / human input |
| `$step.<name>.output` | The output of the named step |

#### `RunState`

| Field | Type | Description |
|---|---|---|
| `ID` | string | Run ID (`run-<millisecond timestamp>-<sequence>`) |
| `Workflow` | string | Workflow name |
| `StepIdx` | int | Index of the current step |
| `Input` | string | Initial input |
| `Prev` | string | Previous step's output |
| `Outputs` | map[string]string | Output of each step |
| `Status` | Status | `running` / `waiting` / `done` / `failed` |
| `Err` | string | Error message |
| `Usage` | core.Usage | Cumulative usage |
| `CreatedAt` / `UpdatedAt` | time.Time | Timestamps |

#### Resume Semantics

| Status | Meaning | `Resume` behavior |
|---|---|---|
| `waiting` | Stopped at a human checkpoint | `humanInput` becomes `$prev`, the step index increments, and execution continues |
| `running` | Left over from a crash / cancellation | **Re-runs** from the current step (at-least-once semantics) |
| `done` / `failed` | Already finished | Returns an error; there is nothing to resume |

- State is **persisted to disk before execution**, so `Resume` still works after a process crash
- After a process restart, **re-registering the workflow** is all it takes to resume
- `Run` / `Resume` for the same run are serialized by a run-level lock, avoiding double execution and double cost
- A caller cancellation (`ctx` cancelled) is not a step failure: the status stays `running` and is not marked `failed`, preserving the resume path

#### `RunStore`

```go
type RunStore interface {
	Save(run *RunState) error
	Get(id string) (*RunState, bool)
}

// one JSON file per run, written via a temp file + atomic rename
func NewFileRunStore(dir string) (*FileRunStore, error)
```

### observer package

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
func (m *Metrics) Record(providerID string, usage core.Usage, err error) // one record per call
func (m *Metrics) Snapshot() map[string]Stats                            // export a copy
```

`entry.Server` already embeds `Metrics` and exposes it via `/api/metrics`.

#### `MemoryTracer`

```go
func NewMemoryTracer() *MemoryTracer
func NewMemoryTracerWithLimit(n int) *MemoryTracer // default cap of 10000 spans

// TraceSpan is a snapshot of a finished or in-flight span
type TraceSpan struct {
	TraceID    string         `json:"trace_id"`
	SpanID     string         `json:"span_id"`
	ParentID   string         `json:"parent_span_id,omitempty"`
	Name       string         `json:"name"`
	StartedAt  time.Time      `json:"started_at"`
	EndedAt    time.Time      `json:"ended_at"`
	Attributes map[string]any `json:"attributes,omitempty"`
}
func (s *TraceSpan) Duration() time.Duration // an unfinished span returns "so far"

func (t *MemoryTracer) StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, core.Span)
func (t *MemoryTracer) Spans() []TraceSpan
func (t *MemoryTracer) TraceCount() int
```

Span tree structure: `agent.run` → `agent.iter` → `llm.stream` / `tool.exec`; the orchestration layer additionally has `workflow.run` → `workflow.step`.

#### Memory Events and Webhooks

The memory subsystem is observable end to end: session stores, snapshots and long-term memory all accept an `Observer` and emit events on change.

```go
// Layer 1: in-process subscription (async fan-out; never blocks memory writes)
bus := observer.NewMemoryEventBus(1024, observer.MemoryObserverFunc(func(e observer.MemoryEvent) {
    log.Printf("[%s] %s session=%s detail=%v", e.Backend, e.Kind, e.Session, e.Detail)
}))
defer bus.Close()

store := d.Memory(memorystore.Options{Observer: bus})   // any driver
snaps := snapshot.New(driver, snapshot.Options{Observer: bus})
mem := ltm.New(ltmStore, ltm.Options{Observer: bus})

// Layer 2: forward over HTTP (implements the same interface)
fwd := webhook.New(webhook.Config{
    URL:        "https://ops.example.com/agent-memory",
    Secret:     "hmac-key",  // each request carries X-TT-Agent-Signature (HMAC-SHA256)
    MaxRetries: 3,           // exponential backoff; 5xx retries, 4xx gives up immediately
    QueueSize:  256,         // bounded queue; drops the oldest when full and counts it
})
defer fwd.Close()
bus2 := observer.NewMemoryEventBus(64, fwd) // or inject fwd as the Observer directly
```

**Event catalog**:

| Kind | Source | Detail carries |
|---|---|---|
| `messages_appended` | session store | `count` |
| `session_trimmed` | session store | `count` |
| `session_cleared` | session store | — |
| `summary_saved` | session store | `covered` |
| `snapshot_captured` | snapshots | `snapshot_id` / `label` / `messages` |
| `session_rolled_back` | snapshots | `target` (RollbackSafe adds `safety`) |
| `fact_remembered` | long-term memory | `fact_id` |
| `fact_forgotten` | long-term memory | `fact_id` |

Three design rules:

1. **Events never carry message content** — only counts and IDs. A webhook leaving the process cannot leak conversations to a misconfigured URL
2. **Emission never blocks the write path** — a slow observer (verified in tests with a hanging handler) does not slow `Add`; a saturated bus drops and counts (`bus.Drops()`), a saturated webhook queue drops the oldest
3. **A panicking observer cannot kill the dispatcher** — one bad sink is isolated; the rest keep receiving events

Receivers verify the signature by HMAC-SHA256'ing the **raw request body** with the same secret and comparing against the `X-TT-Agent-Signature` header.

---

## Complete Example Collection

### Example 1: Minimal Runnable Chat

See [Quick Start](#quick-start).

### Example 2: Streaming Terminal Chat

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
				fmt.Print(e.Text) // print as it arrives
			case agent.EventDeltaReasoning:
				fmt.Fprintf(os.Stderr, "\033[2m%s\033[0m", e.Reasoning) // thinking chain in gray
			case agent.EventDone:
				fmt.Printf("\n[tokens] in=%d out=%d\n", e.Usage.InputTokens, e.Usage.OutputTokens)
			}
		},
	})

	// interactive multi-turn; the same sessionID automatically carries history
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

### Example 3: Multiple Tools and Parallel Execution

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

// when the model requests add and mul in one round, both run in parallel; results are written to memory in call order
// observe the execution through events:
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

### Example 4: Three Production Memory Configurations

```go
// ① Minimal single-machine setup: JSONL persisted to disk, recovered after a restart
mem, err := memory.NewPersistent("./sessions", nil)

// ② Long sessions without losing context: summary compression (old messages retained, audit-friendly)
mem, _ = memory.NewPersistent("./sessions", nil)
mem = memory.NewSummary(mem, cheapLLM) // cheapLLM should be a cheap model, e.g. glm-4-flash

// ③ Production recommendation: bounded memory + idle eviction + physical compaction
inner, _ := memory.NewPersistentWithLRU("./sessions", nil, 1024)
mem = memory.NewTTL(ctx, inner, 30*time.Minute, 5*time.Minute)
mem = memory.NewCompactingSummary(mem, cheapLLM)
```

All three are passed straight to `agent.NewLoop(llm, tools, mem, cfg)` and share the same interface.

### Example 5: Stateless Mode (Caller-Managed History)

```go
// the framework holds no session state at all; history is managed by your database
loop := agent.NewLoop(llm, toolReg, nil, agent.Config{ // pass nil for mem
	Model:        "deepseek-chat",
	SystemPrompt: "你是一个简洁的助手",
	TokenBudget:  32_000, // over-budget history is truncated automatically; you need not control the length
})

// a single request
history, err := db.LoadMessages(ctx, convID) // read the full history from your store
if err != nil {
	return err
}

res, err := loop.RunWithHistory(ctx, history, userInput)
if err != nil {
	return err
}

// persist the messages added this round (including the input and all tool round-trip messages)
if err := db.AppendMessages(ctx, convID, res.NewMessages); err != nil {
	return err
}

return res.Message.Content
```

For multi-instance deployments, no shared storage is needed at all — every instance is a pure request handler.

### Example 6: Multi-Provider Registration and Per-Request Routing

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

// HTTP layer: a provider_id in the request body routes to that provider
srv := entry.NewServer(reg, toolReg, entry.WithConfig(entry.Config{
	Addr:            ":8080",
	DefaultProvider: "deepseek", // used when provider_id is not specified
}))
```

```bash
curl localhost:8080/api/chat -d '{"provider_id":"glm","model":"glm-4.6","input":"你好"}'
```

### Example 7: Full Middleware Composition

```go
primary, _ := adapters.NewLLM(deepseekCfg)
backup, _ := adapters.NewLLM(glmCfg)

wrapped := core.StreamRetry( // outermost: retry before the first streaming token
	core.RateLimitLLM(60, time.Minute)( // global rate limit of 60 requests/minute
		core.FallbackLLM(primary, backup), // failover
	),
	3, // retry up to 3 times
)

// the non-streaming path needs an explicit Pipeline
pipeline := core.NewPipeline(primary,
	core.Logging(nil),
	core.Retry(3),
	core.Cache(5*time.Minute, 1000), // identical requests hit the cache within 5 minutes
)
```

### Example 8: Structured Output

```go
// constrain the model's output to JSON conforming to a JSON Schema
// openai / gemini are mapped; anthropic has no native support and raises an explicit error
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

### Example 9: Multimodal Image Input

```go
// either a URL or a Data URI works; all three protocols are mapped automatically
// (openai image_url / anthropic base64 block / gemini inlineData)
messages := []core.Message{
	core.UserImage("这张图里有什么", "https://example.com/cat.jpg"),
	core.UserImage("这张呢", "data:image/png;base64,iVBORw0KGgo..."),
}

resp, err := llm.Chat(ctx, core.ChatRequest{
	Model:    "gpt-4o",
	Messages: messages,
})
```

### Example 10: Tracing and Metrics

```go
tracer := observer.NewMemoryTracer()
metrics := observer.NewMetrics()

loop := agent.NewLoop(llm, tools, mem, agent.Config{
	Model:  "deepseek-chat",
	Tracer: tracer, // produces agent.run / agent.iter / llm.stream / tool.exec spans
})

msg, usage, err := loop.Run(ctx, "s1", "你好")
metrics.Record("deepseek", usage, err)

// inspect the spans
for _, sp := range tracer.Spans() {
	fmt.Printf("%s %v\n", sp.Name, sp.Duration())
}
fmt.Println("traces:", tracer.TraceCount())

// metrics snapshot
for providerID, st := range metrics.Snapshot() {
	fmt.Printf("%s: calls=%d errors=%d in=%d out=%d\n",
		providerID, st.Calls, st.Errors, st.InputTokens, st.OutputTokens)
}
```

### Example 11: Complete HTTP Server Flow

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

	// production memory: dedicated directory + LRU + TTL
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
# stateful multi-turn
curl localhost:8080/api/chat -d '{"session_id":"s1","input":"我叫张三"}'
curl localhost:8080/api/chat -d '{"session_id":"s1","input":"我叫什么"}'

# stateless
curl localhost:8080/api/chat -d '{"messages":[{"role":"user","content":"我叫张三"}],"input":"我叫什么"}'

# streaming
curl -N localhost:8080/api/chat -d '{"input":"你好","stream":true}'

# metrics and health
curl localhost:8080/api/metrics
curl localhost:8080/api/health
```

### Example 12: Workflow Orchestration (Checkpoint + Resume)

```go
store, err := orchestrator.NewFileRunStore("./runs")
if err != nil {
	log.Fatal(err)
}
orch := orchestrator.New(store)

// an agent is just an ordinary agent.Loop; each may use a different provider / model / prompt
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

// execute: stops at the checkpoint
run, err := orch.Run(ctx, "review", "写一段导语")
if errors.Is(err, orchestrator.ErrCheckpoint) {
	// the state is already persisted: StepIdx points at the checkpoint and Status is waiting
	log.Printf("停在检查点：run=%s step=%d status=%s", run.ID, run.StepIdx, run.Status)

	// continue after human approval (after a process restart, re-register the workflow and Resume)
	// humanInput is fed to the step after the checkpoint as $prev
	run, err = orch.Resume(ctx, run.ID, "通过")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("完成，状态=%s，输出=%+v", run.Status, run.Outputs)
}
```

### Example 13: Multi-Agent Routing and Supervision

```go
err := orch.RegisterWorkflow(&orchestrator.Workflow{
	Name: "routed",
	Steps: []orchestrator.Step{
		// the classifier agent's output must be one of Candidates; the executor is chosen accordingly
		orchestrator.RouterStep{
			Name:       "route",
			Router:     "classifier",
			Candidates: []string{"writer", "reviewer"},
			Input:      "$input",
		},
		// each round the supervisor delegates with WORKER <name> on the first line, or converges with DONE
		orchestrator.SupervisorStep{
			Name:       "supervise",
			Supervisor: "boss",
			Workers:    []string{"writer", "reviewer"},
			Input:      "$step.route.output", // use the routing result as the task
			MaxRounds:  8,
		},
	},
})
```

The supervisor's output protocol:

```
WORKER writer
请写一段关于春天的导语，200 字以内
```

Or convergence:

```
DONE
春天来了，万物复苏……
```

### Example 14: MCP Tool Integration

```go
// stdio: spawn a local MCP server process
client, err := mcp.ConnectStdio(ctx, "filesystem", "npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp")
if err != nil {
	log.Fatal(err)
}
defer client.Close()

// handshake + tools/list + register everything as core.Tool
if err := client.Register(ctx, toolReg); err != nil {
	log.Fatal(err)
}

// afterwards the model can call every tool of that server, no different from built-in tools
loop := agent.NewLoop(llm, toolReg, mem, cfg)

// remote Streamable HTTP
remote := mcp.ConnectHTTP("remote-tools", "https://mcp.example.com/mcp", "bearer-token")
if err := remote.Register(ctx, toolReg); err != nil {
	log.Fatal(err)
}
```

### Example 15: Custom Memory Backend

Implementing the three methods of `core.Memory` is all it takes to plug in your own database (20–30 lines):

```go
// RedisSessionMemory stores session history in Redis
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
	pipe.Expire(ctx, "mem:"+sessionID, 7*24*time.Hour) // natural TTL
	_, err := pipe.Exec(ctx)
	return err
}

func (m *RedisSessionMemory) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	// fetch the most recent N entries, then fill locally by budget (the framework's truncation semantics can be reused)
	raw, err := m.rdb.LRange(ctx, "mem:"+sessionID, -200, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]core.Message, 0, len(raw))
	for _, b := range raw {
		var msg core.Message
		if err := json.Unmarshal([]byte(b), &msg); err != nil {
			continue // skip bad data
		}
		out = append(out, msg)
	}
	return out, nil
}

func (m *RedisSessionMemory) Clear(ctx context.Context, sessionID string) error {
	return m.rdb.Del(ctx, "mem:"+sessionID).Err()
}

// wiring it in
srv := entry.NewServer(reg, toolReg, entry.WithMemory(&RedisSessionMemory{rdb: rdb}))
```

**Implementation contract** (must be followed):

1. `Recent` must guarantee that **system messages are preserved** and that `assistant(tool_calls)` and its `tool` results **come and go together** — splitting the pairing causes provider 400s and permanently corrupts the session
2. The messages returned by `Recent` keep their original chronological order
3. All three methods must be **concurrency-safe** (one `Loop` serves multiple sessions concurrently)
4. `Add` must return an error when persisting to disk fails (so the caller notices), rather than keeping messages in memory only and pretending success

> To also support summary compression, additionally implement `Splitter` + `Trimmer` (or even `SummaryStore`) and the `Summary` decorator will recognize it.

---

## Integrating Your Own Configuration File

The framework **parses no configuration file at all** — the format (YAML / TOML / .env / hard-coded) is your choice; parse it yourself and put the values into `ProviderConfig`.

### YAML

```bash
go get gopkg.in/yaml.v3
```

`config.yaml`:

```yaml
ai-chat:
  active: deepseek # which provider is the default: switching it changes only this line
  providers:
    deepseek:
      address: "https://api.deepseek.com/v1" # note the /v1
      model_name: "deepseek-chat"
      api_key_env: "DEEPSEEK_API_KEY" # prefer the env var name over the key itself
    glm:
      address: "https://open.bigmodel.cn/api/paas/v4"
      model_name: "glm-4.6"
      api_key_env: "GLM_API_KEY"
      quirks: ["glm-thinking"]
    local-ollama:
      address: "http://127.0.0.1:11434/v1" # local models take the same path, no is_local branch needed
      model_name: "qwen3:8b"
      api_key_env: "OLLAMA_KEY" # set any placeholder when ollama does not validate the key
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
			q, err := provider.ComposeQuirks(p.Quirks, cfg.Protocol) // a wrong name errors here
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
	// ... then adapters.NewLLM(cfg)
}
```

### .env

`APIKeyEnv` + `LoadAPIKeyFromEnv` were designed exactly for this shape:

```bash
go get github.com/joho/godotenv
```

```env
AI_BASE_URL=https://api.deepseek.com/v1
AI_MODEL=deepseek-chat
DEEPSEEK_API_KEY=sk-xxx
```

```go
_ = godotenv.Load() // optional: load .env into the process environment; in production set the system env directly

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

Same as YAML, except the parsing library is `BurntSushi/toml` and the struct tags become `toml:"..."`; the way you fill `ProviderConfig` is unchanged.

### Legacy Config Field Mapping

| Legacy form (common) | This framework | Description |
|---|---|---|
| `address` + `request_address: "/chat/completions"` | A single `BaseURL` field | Supply only the root address (including `/v1`); the adapter appends the endpoint path itself |
| `model_name` | `DefaultModel` + `Models[].ID` | |
| `api_key: "sk-..."` | `APIKeyEnv` + `LoadAPIKeyFromEnv()`, or `SetAPIKey(v)` | The former keeps the key out of files |
| `is_local: true` | Not needed | ollama / vLLM are simply the openai protocol with a local `BaseURL` |
| Non-chat fields such as `whisper_asr_*` | Not mapped | This framework only covers the chat path |

---

## Key Design Decisions

**Streaming channel semantics are pinned down.** The producer is responsible for closing; errors travel only through the `StreamError` terminal event; `ChatStream`'s error return value is used solely for failures before the connection is established.

**Streaming retries happen only before the first token.** `StreamRetry` buffers up to the first content event; failures after that pass through unchanged to avoid duplicated output.

**Memory is isolated per session.** `Loop` is stateless and reusable, so one Loop can serve multiple sessions concurrently.

**Tool failures do not break the loop.** Error text is sent back as a tool message and the model adjusts by itself; only LLM errors and iteration overflow terminate the loop. Multiple tool calls in the same round run in parallel.

**History assembly is separated from storage.** Budget truncation, `tool_calls` atomic-group pairing, and dangling-call repair are done by the framework **when assembling the request** (`sanitizeHistory`); storage only stores facts — this lets stateless and stateful modes share the same correctness guarantees.

**Memory is always a cache, never the source of truth.** The default memory persists to disk; `Persistent`'s lazy loading + LRU make the memory footprint depend only on "the number of concurrently active sessions", not on the total number of historical sessions.

**Decorators beat subclassing.** Persistence / compression / eviction of memory are three orthogonal capabilities, composed through `Memory` plus optional capability interfaces (`Splitter` / `Trimmer` / `SummaryStore`) rather than an inheritance tree.

---

## Error Handling and Retries

```go
msg, usage, err := loop.Run(ctx, "s1", "你好")
switch {
case err == nil:
	// success
case errors.Is(err, agent.ErrMaxIterations):
	// the iteration cap was reached: tools may be failing repeatedly, or the problem is too complex
case errors.Is(err, context.Canceled):
	// the caller cancelled ctx
case core.Retryable(err):
	// a retryable category (rate limit / provider 5xx / network)
	kind := core.ErrorKindOf(err)
	_ = kind
default:
	// other terminating errors
}
```

| Error | Source | Suggested handling |
|---|---|---|
| `agent.ErrMaxIterations` | The loop hit `MaxIterations` | Raise the cap, simplify the problem, or check whether a tool keeps failing |
| `orchestrator.ErrCheckpoint` | The workflow stopped at a checkpoint | This is a **normal flow**; call `Resume` to continue |
| `core.ErrAuth` | Bad API key | Replace the key; do not retry |
| `core.ErrRateLimited` | A rate limit was triggered | The `Retry` middleware backs off and retries automatically |
| `core.ErrProviderInternal` | Provider 5xx | Retryable; consider `FallbackLLM` for persistent failures |
| `core.ErrNetwork` | Network layer | Retryable |
| `core.ErrUnsupported` | Unsupported capability | A configuration error, e.g. anthropic + structured output |

---

## FAQ

**Q: Why does my middleware have no effect on streaming?**
The agent loop always takes `ChatStream`. `Logging` / `Retry` / `RateLimit` / `Cache` on `core.NewPipeline` wrap only the non-streaming `Chat`. To cover streaming, use `LoggingLLM` / `RateLimitLLM` / `FallbackLLM` / `StreamRetry`.

**Q: Why is the session history gone after a server restart?**
Check whether `WithMemory` was used to specify a fixed directory. By default memory lands in the system temp directory, which survives a restart on the same machine but not a machine change or a container rebuild. Multi-instance deployments need a shared storage backend (see [Example 15](#example-15-custom-memory-backend)).

**Q: Will session data grow without bound?**
`Persistent` does not limit the number of sessions by default (but only active sessions are resident in memory). In production, use the `NewPersistentWithLRU` + `NewTTL` + `NewCompactingSummary` trio; see [Example 4](#example-4-three-production-memory-configurations).

**Q: What if the model's tool calls keep failing?**
Check two things: ① whether the JSON Schema descriptions in `Parameters()` are clear (a well-written `description` markedly improves accuracy); ② whether the error message returned by `Execute` gives the model enough guidance to correct itself. Tool errors are sent back to the model, which adjusts on its own.

**Q: How do I make the model output JSON?**
Use `ChatRequest.ResponseFormat`; see [Example 8](#example-8-structured-output). Note that the anthropic protocol has no native support and raises an explicit error.

**Q: How do I serve multiple sessions concurrently?**
Just call the same `Loop` concurrently. Note that `OnEvent` callbacks arrive from multiple goroutines, so they must be concurrency-safe and routed by `e.SessionID`.

**Q: What is a sensible `MaxIterations` value?**
16 by default. 3–5 is enough for simple Q&A; 16–32 for complex multi-step tasks. Too small and the task is cut off before it finishes; too large and a runaway loop burns more money.

**Q: How does `TokenBudget` relate to the model's `ContextWindow`?**
`TokenBudget` is the framework-side **input token budget**; when exceeded, the oldest history is truncated. It should be set to a fraction of the model's `ContextWindow` (e.g. 60–80%), leaving headroom for the output and the system prompt. The rough estimate is conservative (better under than over).

**Q: Can I do without any storage?**
Yes. `RunWithHistory` never touches storage; the caller manages the history (see [Example 5](#example-5-stateless-mode-caller-managed-history)). It is also the least troublesome way to deploy multiple instances.

**Q: Why isn't `http_fetch` registered by default?**
It faces **model output**, and without filtering it is an SSRF surface (a model can be steered into fetching cloud metadata endpoints). It is not registered by default; even once registered explicitly, loopback/private targets are rejected by default, and reaching internal networks requires `WithAllowPrivateTargets(true)`.

---

## Benchmarks

Machine: Intel Core Ultra 9 185H (22 threads), Windows; SQLite and the file backend use temp directories; Redis numbers use **miniredis (in-process, no network round-trip)**; Postgres is a real Docker instance on localhost. Numbers are for **relative comparison between backends**, not absolute promises. Reproduce with `go test ./pkg/memory/... ./pkg/ltm/... -run '^$' -bench . -benchtime 2s` (Postgres needs `TEST_POSTGRES_DSN`).

**Write throughput (Add, one ~60-char message)**

| Backend | ns/op | Note |
|---|---:|---|
| Redis (miniredis) | 122K | no-network figure; real Redis adds an RTT |
| File (JSONL) | 255K | append + fsync |
| SQLite | 458K | single-transaction insert |
| Postgres (real) | 1,584K | localhost Docker; higher across a network |

**Reads (Recent, full-window materialization) grow linearly with session length**

| Backend | 100 msgs | 1,000 msgs | 5,000 msgs |
|---|---:|---:|---:|
| File | — | 151K | — |
| SQLite | 236K | 2,159K | 4,753K |
| Redis (miniredis) | 397K | 2,908K | 14,304K |
| Postgres (real) | — | 3,857K | — |

The linear growth is exactly why `maxScan=2000` exists as the default: it pins the worst-case request assembly at milliseconds. A 5,000-message window costs 14ms on Redis — leaving older messages outside the window is a deliberate trade-off, not an oversight. Reads that hit the window cap are no longer silent: `store.CappedReads()` counts every capped read, so operators can notice sessions that outgrew their window and raise `Options.MaxScan` or compact them.

**Trim (10 removed from a 1,000-message session)**: SQLite 606K / Postgres 1,049K / Redis 1,957K (the whole Lua script runs server-side).

**Encryption overhead (AES-256-GCM, per message)**: plain encode 535ns → encrypted 979ns; plain decode 1,137ns → encrypted 1,476ns. **~+0.5µs per message** — pure noise next to storage I/O (122µs–1.6ms). Encryption is free.

**Long-term memory (in-process store)**

| Operation | 100 facts | 1,000 facts | 5,000 facts |
|---|---:|---:|---:|
| Keyword recall | 71K | 989K | 6,322K |
| Vector recall (64-dim) | — | 1,184K | 8,382K |

`Remember` writes at 6.5µs. Recall stays under 1ms at a thousand facts; when five thousand approaches 10ms, move to `pgvector.Store` (SQL-side ranking holds its scale better).

**Snapshots**: capturing a 100-message session ≈ 9ms; 1,000 messages ≈ 112ms — linear in session length (a snapshot is a full copy), with `MaxPerSession` (default 20) bounding the total.

---

## Testing

Tests live in a separate `test/` module and are all black-box, relying only on exported APIs:

```
test/
├── openai_test.go            SSE golden fixtures, error mapping, Quirks injection
├── anthropic_test.go         Messages API block structure, content_block stream decoding
├── gemini_test.go            role mapping, synthesis and restoration of functionCall without an ID
├── agent_test.go             ReAct loop, iteration cap, tool-failure feedback
├── agent_parallel_test.go    parallel tool execution and result ordering
├── stateless_test.go         stateless mode, history hand-back, HTTP messages entry point
├── pipeline_test.go          Retry semantics, retry before the first streaming token
├── middleware_test.go        rate limiting, caching, failover
├── memory_test.go            persistence recovery, corrupt-line tolerance, truncation consistency
├── memory_growth_test.go     growth control, LRU eviction, summary compression and persistence
├── summary_test.go           rolling summary merge, degradation on LLM failure
├── orchestrator_test.go      sequential chains, templates, checkpoint pause/restart resume
├── multi_agent_test.go       Router / Supervisor steps
├── entry_test.go             HTTP/SSE endpoints
├── ws_test.go                WebSocket frame protocol
├── grpc_test.go              gRPC dynamic descriptor, unary/streaming
├── mcp_test.go               MCP handshake, tool listing and calling (stdio/HTTP)
├── builtin_tools_test.go     calculator / clock / http_fetch
├── multimodal_test.go        image input mapped across the three protocols
├── structured_output_test.go structured output
├── metrics_test.go           metric aggregation
├── trace_test.go             tracing
├── cost_test.go              cost accounting
├── quirks_test.go            named quirks composition and validation
└── issues_*_test.go          historical regression cases (filed by round and number)
```

```bash
go test ./test/       # framework tests only
go test ./...         # everything
go test -race ./test/ # race detection
```

Storage-driver tests that need an external server enable it through an environment variable and skip gracefully when unset:

| Environment variable | Purpose |
|---|---|
| `TEST_POSTGRES_DSN` | Enables the Postgres driver contract suite plus the pgvector long-term-memory tests (e.g. `postgres://user:pass@localhost:5432/db?sslmode=disable`); CI should always set it so the four-backend parity guarantee covers every implementation |

**CI** (`.github/workflows/ci.yml`): runs on push/PR — gofmt check → vet → build → **full tests (`-race`, with a real pgvector service container)**, and guards the `go 1.24` floor in `go.mod` against being silently raised by a dependency.

White-box tests (for when unexported symbols must be reached) stay in `xxx_test.go` inside the source package, per Go convention.

---

## Directory Structure

```
cmd/server/          ready-to-run server (flag + env configuration)
examples/            runnable examples
pkg/
├── core/            LLM / Memory / Tool / Pipeline / errors / tracing / streaming types
├── adapters/
│   ├── provider/    provider configs, registry, named quirks, pricing
│   └── protocol/    OpenAI / Anthropic / Gemini protocol implementations
├── agent/           ReAct loop, stateless mode
├── memory/          session memory
│   ├── memorystore/ driver contract (Codec / Driver / Store) + encrypting codec
│   ├── redis/       Redis driver (native TTL)
│   ├── sqlite/      SQLite driver (one file, multi-process, pure Go)
│   ├── postgres/    PostgreSQL driver (schema-isolated)
│   ├── snapshot/    snapshots and rollback (time travel)
│   └── memorytest/  test-only in-process implementation
├── ltm/             long-term memory: fact extraction + vector search + namespaces
│   ├── embeddings/  ready-to-use embedders (OpenAI-compatible / Anthropic / Gemini)
│   ├── extractor/   built-in LLM fact extractor (on core.LLM)
│   └── pgvector/    PostgreSQL + pgvector vector store backend
├── tools/           tool registry
│   ├── builtin/     calculator / clock / http_fetch
│   └── mcp/         MCP client (stdio / Streamable HTTP)
├── entry/           HTTP / SSE / WS / gRPC entry points
├── orchestrator/    workflow orchestration, checkpoints, resume from checkpoint
├── observer/        metric aggregation, in-memory tracing, memory event bus
│   └── webhook/     event forwarding over HTTP (HMAC signing + retries)
test/                black-box tests
```

---

## License

See [LICENSE](LICENSE).
