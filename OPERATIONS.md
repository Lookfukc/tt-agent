# tt-agent 操作文档（OPERATIONS.md）

> 面向使用者的完整操作手册：环境准备 → 零代码跑服务 → **Gin 完整集成（从零搭出 agent-demo）** → 四种记忆后端 → 快照时间旅行 → 长期记忆 → 观测 → 运维排查。
> 姊妹文档：[README.md](README.md)（框架总览与 API 参考）。
> 配套实战工程：agent-demo（Gin + Redis + pgvector + swaggo，本文第 4 章逐步搭出的就是它）。

**目录**

- [1. 环境准备](#1-环境准备)
- [2. 零代码跑服务](#2-零代码跑服务)
- [3. 服务端配置详解](#3-服务端配置详解)
- [4. Gin 完整集成教程（从零搭出 agent-demo）](#4-gin-完整集成教程从零搭出-agent-demo)
- [5. 对话：有状态 / 无状态 / SSE 流式 / 记忆注入](#5-对话有状态--无状态--sse-流式--记忆注入)
- [6. 会话记忆与时间旅行（快照/回滚）](#6-会话记忆与时间旅行快照回滚)
- [7. 长期记忆（pgvector + ollama）](#7-长期记忆pgvector--ollama)
- [8. 可观测性](#8-可观测性)
- [9. 故障排查（踩坑实录）](#9-故障排查踩坑实录)
- [10. agent-demo 完整接口清单](#10-agent-demo-完整接口清单)

---

## 1. 环境准备

| 项 | 要求 | 检查命令 |
|---|---|---|
| Go | ≥ 1.24（以 `go.mod` 为准） | `go version` |
| LLM 凭据 | 任一提供商 API Key（DeepSeek / 智谱 / Anthropic / Google / 本地 ollama） | — |
| Redis（可选） | ≥ 6.0，会话记忆/快照后端 | `redis-cli ping` |
| PostgreSQL（可选） | ≥ 14 + pgvector 扩展，长期记忆 | `psql -c 'select 1'` |
| ollama（可选） | 向量模型服务，长期记忆用 | `ollama list` |

> 工具链提示：`GOSUMDB=off` 环境下 Go 自动下载新工具链会被拒绝，用 `GOTOOLCHAIN=local`
> 或先安装对应版本。

Docker 一键起依赖：

```bash
# Redis
docker run -d --name tt-redis -p 6379:6379 redis:7

# PostgreSQL + pgvector
docker run -d --name tt-pg \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 pgvector/pgvector:pg16
docker exec tt-pg psql -U postgres -c 'create extension if not exists vector'

# ollama 向量模型（长期记忆用）
ollama pull nomic-embed-text
```

---

## 2. 零代码跑服务

不写一行代码，`go run @latest` 直接起（HTTP/SSE/WS 全有）：

```bash
export DEEPSEEK_API_KEY=sk-xxx
go run github.com/Lookfukc/tt-agent/cmd/server@latest \
  --base-url https://api.deepseek.com/v1 \
  --model deepseek-chat \
  --api-key-env DEEPSEEK_API_KEY
```

验证：

```bash
curl http://localhost:8080/api/health
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" -d '{"input":"你好"}'
```

源码构建（改代码/加 gRPC 时）：

```bash
git clone https://github.com/Lookfukc/tt-agent.git
cd tt-agent
go build -o server ./cmd/server
./server --base-url https://api.deepseek.com/v1 \
         --model deepseek-chat \
         --api-key-env DEEPSEEK_API_KEY \
         --memory-type redis --memory-dsn redis://127.0.0.1:6379
```

需要自定义接口/业务逻辑时，回到第 4 章的完整集成教程。

---

## 3. 服务端配置详解

`cmd/server` 全部参数（`--help` 看完整列表）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--addr` | `:8080` | HTTP 监听地址 |
| `--grpc` | 空 | gRPC 监听地址（如 `:9090`，空=关闭） |
| `--protocol` | openai | openai / anthropic / gemini |
| `--base-url` | 必填 | API 根地址（**不带** /chat/completions） |
| `--api-key-env` | 必填 | **环境变量的名字**（不是 key 本身！） |
| `--model` | 必填 | 默认模型 |
| `--prompt` | 空 | 默认 system prompt |
| `--memory-type` | file | file / sqlite / redis / postgres |
| `--memory-dir` | 系统临时目录 | file 后端目录 |
| `--memory-dsn` | — | sqlite 路径 / redis URL / postgres 连接串 |
| `--memory-ttl` | 0 | 会话保留期（0=永久；redis 原生过期，sqlite/pg 后台清扫） |
| `--memory-prune-every` | 5m | 清扫周期 |
| `--httpfetch` | 关 | 注册 http_fetch 工具（注意 SSRF 面） |

> ⚠️ **最常见的坑**：`--api-key-env` 填 `DEEPSEEK_API_KEY`（变量名），且启动进程的环境里
> 要真的能读到它。填成 key 本身会报 `api key not configured`。

**作为库引入**：

```bash
go get github.com/Lookfukc/tt-agent
```

最小组合：`pkg/agent`（ReAct 循环，带 core/tools）+ `pkg/adapters`（连真实 LLM）+
`pkg/memory`（会话记忆）。其余按需。

---

## 4. Gin 完整集成教程（从零搭出 agent-demo）

以 Gin + viper + Redis + pgvector 为例，完整搭一个分层服务。全部代码取自
agent-demo 实测工程，目录结构：

```
my-agent/
├── main.go                    # 入口
├── configs/
│   ├── config.yaml            # 基础配置
│   └── config.local.yaml      # 本地覆盖（APP_ENV=local 时合并）
├── internal/
│   ├── config/                # viper 加载
│   ├── global/                # 全局句柄 + 事件环
│   ├── core/                  # Init 链：日志→总线→Redis→PG→记忆→快照→LTM
│   │   └── initializer/       # logrus / redis / postgres 初始化器
│   ├── models/dto/            # 请求 DTO
│   ├── services/              # agent / memory / ltm 业务逻辑
│   ├── handler/               # Gin handler
│   └── router/                # container（装配）+ 路由
└── pkg/result/                # 统一响应信封
```

### 4.1 配置文件 configs/config.yaml

```yaml
system:
  host: "0.0.0.0"
  port: 7950
  env: dev

logger:
  level: debug
  prefix: "[my-agent]"
  log_in_console: true

# redis：会话记忆 + 快照
redis:
  address: 127.0.0.1:6379
  db: 3

# postgres（pgvector）：长期记忆
postgres:
  host: 127.0.0.1
  port: 5432
  db: ai_ops
  username: postgres
  password: postgres

# 对话模型（OpenAI 兼容协议；换 DeepSeek/GLM/Kimi/ollama 只改这三行）
ai-chat:
  address: "https://api.deepseek.com"   # 根地址，不带 /chat/completions
  model_name: "deepseek-chat"
  api_key: "sk-xxx"                      # 生产用环境变量 AI_CHAT_API_KEY 注入

# 向量模型（ollama，OpenAI 兼容 /v1/embeddings；换模型必须同步改 dim）
ai-embedding:
  address: "http://127.0.0.1:11434"
  model_name: "nomic-embed-text"
  api_key: "ollama"
  dim: 768

agent:
  memory_prefix: "myapp:mem:"
  session_ttl_hours: 24
  pg_schema: "my_agent"
  system_prompt: "你是一个简洁的助手。"
  max_iterations: 16
  token_budget: 32000
  snapshot_max_per_session: 20

webhook:
  url: ""        # 留空只走进程内事件环
  secret: ""
```

环境差异写 `config.<env>.yaml` 覆盖；敏感项可用环境变量注入（优先级最高）。

### 4.2 配置加载 internal/config/config.go

```go
package config

import (
	"fmt"
	"os"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type config struct {
	System      system            `yaml:"system"`
	Redis       redis             `yaml:"redis"`
	Postgres    postgres          `yaml:"postgres"`
	AiChat      aiChat            `yaml:"ai-chat"`
	AiEmbedding aiEmbeddingConfig `yaml:"ai-embedding"`
	Agent       agentConfig       `yaml:"agent"`
	Webhook     webhook           `yaml:"webhook"`
}

// ...（各段 struct 字段与 yaml 一一对应，完整定义见 agent-demo）

var Config *config

func init() {
	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "local"
	}
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./configs")
	if err := v.ReadInConfig(); err != nil {
		panic(fmt.Errorf("读取基础配置失败: %w", err))
	}
	v.SetConfigName("config." + env)
	_ = v.MergeInConfig() // 环境覆盖文件，可缺省

	// 环境变量注入敏感项，优先级最高，生产不落盘
	if key := os.Getenv("AI_CHAT_API_KEY"); key != "" {
		v.Set("ai-chat.api_key", key)
	}

	Config = &config{}
	if err := v.Unmarshal(Config, func(c *mapstructure.DecoderConfig) {
		c.TagName = "yaml"
	}); err != nil {
		panic(fmt.Errorf("配置反序列化失败: %w", err))
	}
}
```

### 4.3 初始化链 internal/core/core.go（核心装配）

启动顺序：日志 → 事件总线 → Redis → Postgres → 会话记忆 → 快照 → 长期记忆。
**降级策略**：Redis 失败时会话记忆降级进程内（告警）；pgvector 失败只影响长期记忆。

```go
package core

import (
	"context"
	"time"

	"my-agent/internal/config"
	"my-agent/internal/global"

	ttcore "github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/ltm"
	"github.com/Lookfukc/tt-agent/pkg/ltm/embeddings"
	"github.com/Lookfukc/tt-agent/pkg/ltm/pgvector"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/redis"
	"github.com/Lookfukc/tt-agent/pkg/memory/snapshot"
	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// Init 启动初始化链
func Init() {
	global.Log = initializer.InitLogger()
	global.RecentEvents = global.NewEventRing(500)
	global.EventBus = buildEventBus()

	ctx := context.Background()
	initSessionMemory(ctx)
	initSnapshots()
	initLongTermMemory(ctx)
}

// 会话记忆：Redis 驱动 + Store（预算截断/配对语义在 Store 层保证）
func initSessionMemory(ctx context.Context) {
	ac := config.Config.Agent
	opts := memorystore.Options{Observer: global.EventBus, Backend: "redis"}
	ttl := time.Duration(ac.SessionTTLHours) * time.Hour

	if global.Redis != nil {
		driver := redis.New(global.Redis, redis.Options{
			Prefix: ac.MemoryPrefix, SessionTTL: ttl,
		})
		probe := memorystore.New(driver, opts)
		// 启动即自检：配置错误在这里暴露，而不是等第一次请求
		if err := probe.Add(ctx, "startup-probe",
			ttcore.Message{Role: ttcore.RoleUser, Content: "ping"}); err == nil {
			_ = probe.Clear(ctx, "startup-probe")
			global.MemoryDriver = driver
			global.SessionMem = probe
			global.Log.Infof("会话记忆: redis prefix=%s ttl=%s", ac.MemoryPrefix, ttl)
			return
		} else {
			global.Log.Warnf("Redis 记忆驱动自检失败: %v", err)
		}
	}
	global.SessionMem = memorytest.NewBuffer(nil) // 降级：进程内，重启丢
	global.Log.Warn("会话记忆降级为进程内存储，请检查 Redis")
}

// 快照：Redis 驱动自带跨进程锁
func initSnapshots() {
	if global.MemoryDriver == nil {
		return
	}
	global.Snapshots = snapshot.New(global.MemoryDriver, snapshot.Options{
		Observer:      global.EventBus,
		Backend:       "redis",
		MaxPerSession: config.Config.Agent.SnapshotMaxPerSession,
		LockWait:      5 * time.Second,
	})
}

// 长期记忆：pgvector 向量库 + ollama 向量模型
func initLongTermMemory(ctx context.Context) {
	emb := config.Config.AiEmbedding
	var embedder ltm.Embedder
	if emb.Address != "" && emb.ModelName != "" {
		embedder = embeddings.NewOpenAICompatible(emb.Address+"/v1", emb.ApiKey, emb.ModelName)
	}
	store, err := pgvector.New(global.PgPool, pgvector.Options{
		Dim: emb.Dim, Schema: config.Config.Agent.PgSchema,
	})
	if err == nil {
		_, _ = global.PgPool.Exec(ctx,
			`CREATE SCHEMA IF NOT EXISTS "`+config.Config.Agent.PgSchema+`"`)
		err = store.Migrate(ctx) // 内部 CREATE EXTENSION vector + 建表
	}
	backend := "pgvector"
	if err != nil {
		global.Log.Warnf("pgvector 不可用，降级进程内: %v", err)
		store, backend = ltm.NewMemoryStore(ltm.MemoryOptions{}), "memory"
	}
	global.LongTerm = ltm.New(store, ltm.Options{
		Embedder:     embedder,
		DefaultLimit: 5,
		Observer:     global.EventBus,
		Backend:      backend,
	})
}
```

### 4.4 对话服务 internal/services/agent_service.go

```go
package services

import (
	"context"
	"fmt"
	"sync"

	"my-agent/internal/config"
	"my-agent/internal/global"

	"github.com/Lookfukc/tt-agent/pkg/adapters"
	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/tools"
	"github.com/Lookfukc/tt-agent/pkg/tools/builtin"
)

// AgentService 模型懒加载 + 每请求构造 Loop
type AgentService struct{}

var (
	llmOnce sync.Once
	llmInit error
)

// llm 首次调用时按 ai-chat 段装配，避免拖慢启动。
// 换模型只改配置：协议适配层兼容 OpenAI 系（DeepSeek/GLM/Kimi/ollama）。
func (s *AgentService) llm() (core.LLM, string, error) {
	llmOnce.Do(func() {
		ai := config.Config.AiChat
		if ai.Address == "" || ai.ModelName == "" || ai.ApiKey == "" {
			llmInit = fmt.Errorf("ai-chat 配置不完整")
			return
		}
		cfg := &provider.ProviderConfig{
			ID: "ai-chat", Protocol: "openai",
			BaseURL:      ai.Address, // 根地址即可，/chat/completions 由适配器拼
			DefaultModel: ai.ModelName,
			Models:       []provider.ModelConfig{{ID: ai.ModelName}},
		}
		cfg.SetAPIKey(ai.ApiKey)
		l, err := adapters.NewLLM(cfg)
		if err != nil {
			llmInit = err
			return
		}
		global.LLM, global.LLMModel = l, ai.ModelName
	})
	return global.LLM, global.LLMModel, llmInit
}

// tools 注册内置工具，Loop 才能走 ReAct
func (s *AgentService) tools() *tools.Registry {
	reg := tools.NewRegistry()
	reg.Register(builtin.NewCalculator())
	reg.Register(builtin.NewClock())
	reg.Register(builtin.NewHTTPFetch())
	return reg
}

// Chat 有状态对话：历史落 Redis，同 session_id 连续对话
func (s *AgentService) Chat(ctx context.Context, sessionID, input, model string) (*ChatResult, error) {
	if _, _, err := s.llm(); err != nil {
		return nil, err
	}
	if sessionID == "" {
		sessionID = NewSessionID()
	}
	loop := agent.NewLoop(global.LLM, s.tools(), global.SessionMem, agent.Config{
		Model:         model,
		SystemPrompt:  config.Config.Agent.SystemPrompt,
		MaxIterations: config.Config.Agent.MaxIterations,
		TokenBudget:   config.Config.Agent.TokenBudget,
	})
	msg, usage, err := loop.Run(ctx, sessionID, input)
	if err != nil {
		return nil, err
	}
	return &ChatResult{SessionID: sessionID, Content: msg.Content,
		Reasoning: msg.Reasoning, Usage: usage}, nil
}

// ChatStateless 无状态：不落任何存储，历史调用方携带并回收增量
func (s *AgentService) ChatStateless(ctx context.Context, history []core.Message,
	input, model string) (*StatelessResult, error) {
	if _, _, err := s.llm(); err != nil {
		return nil, err
	}
	// memory 传 nil + RunWithHistory：服务端零会话状态
	loop := agent.NewLoop(global.LLM, s.tools(), nil, agent.Config{
		Model: model, SystemPrompt: config.Config.Agent.SystemPrompt,
		MaxIterations: config.Config.Agent.MaxIterations,
		TokenBudget:   config.Config.Agent.TokenBudget,
	})
	res, err := loop.RunWithHistory(ctx, history, input)
	if err != nil {
		return nil, err
	}
	return &StatelessResult{Content: res.Message.Content,
		NewMessages: res.NewMessages, Usage: res.Usage}, nil
}
```

### 4.5 DTO internal/models/dto/agent_dto.go

```go
package dto

import "github.com/Lookfukc/tt-agent/pkg/core"

// AgentChatDTO 有状态对话入参
type AgentChatDTO struct {
	Input     string `json:"input" binding:"required"`
	SessionID string `json:"session_id"` // 缺省新建会话；同 ID 复用上下文
	Model     string `json:"model"`      // 可选，覆盖配置模型
}

// AgentStatelessDTO 无状态对话入参
type AgentStatelessDTO struct {
	Input    string         `json:"input" binding:"required"`
	Messages []core.Message `json:"messages"` // 调用方自己的历史
	Model    string         `json:"model"`
}
```

`core.Message` 是框架的统一消息类型，JSON 直接反序列化（`role`/`content`/
`tool_calls`/`tool_call_id` 与各协议通用格式一致），无需自己定义。

### 4.6 Handler（REST + SSE）internal/handler/agent_handler.go

```go
// Chat 有状态对话，一次性返回
func (h *AgentHandler) Chat(c *gin.Context) {
	req, ok := utils.BindJSON[dto.AgentChatDTO](c)
	if !ok {
		return
	}
	res, err := h.svc.Chat(c.Request.Context(), req.SessionID, req.Input, req.Model)
	if err != nil {
		result.FailErr(c, result.LLM_ERROR, err)
		return
	}
	result.OkData(gin.H{
		"session_id": res.SessionID,
		"content":    res.Content,
		"reasoning":  res.Reasoning,
		"usage": gin.H{"input_tokens": res.Usage.InputTokens,
			"output_tokens": res.Usage.OutputTokens},
	}, c)
}

// ChatStream 有状态对话，SSE 流式返回
func (h *AgentHandler) ChatStream(c *gin.Context) {
	req, ok := utils.BindJSON[dto.AgentChatDTO](c)
	if !ok {
		return
	}
	// SSE 头必须在写 body 前设置；X-Accel-Buffering 防 nginx 缓冲吞增量
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("X-Accel-Buffering", "no")

	writable := true
	writeSSE := func(event string, data any) {
		if !writable {
			return
		}
		payload, _ := json.Marshal(data)
		if _, err := fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event, payload); err != nil {
			writable = false // 客户端断连后停写；循环靠请求上下文取消
			return
		}
		c.Writer.Flush()
	}

	onEvent := func(e agent.LoopEvent) {
		switch e.Type {
		case agent.EventDeltaText:
			writeSSE("text", gin.H{"delta": e.Text})
		case agent.EventDeltaReasoning:
			writeSSE("reasoning", gin.H{"delta": e.Reasoning})
		case agent.EventToolCall:
			writeSSE("tool_call", gin.H{"name": e.Call.Name, "arguments": e.Call.Arguments})
		case agent.EventToolResult:
			writeSSE("tool_result", gin.H{"name": e.Call.Name})
		}
	}

	res, err := h.svc.ChatStream(c.Request.Context(), req.SessionID, req.Input, req.Model, onEvent)
	if err != nil {
		writeSSE("error", gin.H{"error": err.Error()})
		return
	}
	writeSSE("done", gin.H{"session_id": res.SessionID, "content": res.Content})
}
```

### 4.7 装配与路由 internal/router/

```go
// container.go：service → handler 统一装配
func newDependencies() *HandlerDeps {
	agentSvc := services.NewAgentService()
	return &HandlerDeps{
		Agent:  handler.NewAgentHandler(agentSvc),
		Memory: handler.NewMemoryHandler(services.NewMemoryService()),
		LTM:    handler.NewLTMHandler(services.NewLTMService()),
	}
}

// router.go：全部公开
func Register(r *gin.Engine) {
	deps := newDependencies()
	api := r.Group("/v1")
	agent := api.Group("/agent")
	agent.POST("/chat", deps.Agent.Chat)
	agent.POST("/chat/stream", deps.Agent.ChatStream)
	agent.POST("/chat/stateless", deps.Agent.ChatStateless)
	// memory / ltm / events / system 同理（完整见第 10 章）
}
```

### 4.8 启动后用 HTTP 调（curl）

**非流式**：

```bash
curl -X POST http://127.0.0.1:7950/v1/agent/chat \
     -H "Content-Type: application/json" \
     -d '{"input":"帮我计算 23*7+1"}'
```

响应（统一信封）：

```json
{
  "code": 0,
  "data": {
    "session_id": "550e8400-e29b-41d4-a716-446655440000",
    "content": "23*7+1 = 162",
    "reasoning": "",
    "usage": { "input_tokens": 156, "output_tokens": 23 }
  },
  "msg": "ok"
}
```

**流式（SSE）**——注意 `-N` 关 curl 缓冲：

```bash
curl -N -X POST http://127.0.0.1:7950/v1/agent/chat/stream \
     -H "Content-Type: application/json" \
     -d '{"input":"计算 23*7+1"}'
```

```
event: text
data:{"delta":"我来算一下"}

event: tool_call
data:{"name":"calculator","arguments":"{\"expression\":\"23*7+1\"}"}

event: tool_result
data:{"name":"calculator"}

event: done
data:{"session_id":"...","content":"23*7+1 = 162"}
```

**无状态**（带全量历史，返回 `new_messages`）：

```bash
curl -X POST http://127.0.0.1:7950/v1/agent/chat/stateless \
     -H "Content-Type: application/json" \
     -d '{"messages":[{"role":"user","content":"我叫小明"}],"input":"我叫什么"}'
```

**会话消息查询**：

```bash
curl http://127.0.0.1:7950/v1/memory/sessions/550e8400.../messages
```

swaggo 文档：`go install github.com/swaggo/swag/cmd/swag@latest && swag init`
后访问 `http://127.0.0.1:7950/swagger/index.html`。

---

## 5. 对话：有状态 / 无状态 / SSE 流式 / 记忆注入

### 有状态（服务端记忆）

```go
msg, usage, err := loop.Run(ctx, "user-42", "你好")
// msg.Content  最终回答；msg.Reasoning 思考内容
// usage        累计 token（InputTokens/OutputTokens/ReasoningTokens）
```

每轮只传 `sessionID + input`，历史由 Memory 管理；重启后会话仍在（持久化后端）。

### 无状态（调用方持有历史）

```go
loop := agent.NewLoop(llm, tools, nil, cfg)   // memory 为 nil

res, _ := loop.RunWithHistory(ctx, history, "我叫小明，喜欢 Go")
saveToDb(res.NewMessages)                     // 自己存库

res2, _ := loop.RunWithHistory(ctx, loadFromDb(), "我喜欢什么？")
fmt.Println(res2.Message.Content)             // → "Go"
```

框架内部零存储；超长历史传入也安全（预算截断与工具配对仍生效）。

### 记忆注入对话（长期记忆真正产生价值的路径）

```go
// 先按输入召回事实，渲染成提示词注入系统人设，再走有状态对话
prompt, facts, _ := longTerm.RecallPrompt(ctx, "user-42", input, 5)
sysPrompt := prompt + "\n\n" + basePrompt
loop := agent.NewLoop(llm, tools, mem, agent.Config{SystemPrompt: sysPrompt})
```

### 事件全表

`agent.LoopEvent` 全部 7 种事件（`OnEvent` 回调与 SSE 转发共用一套语义）：

| Type | SSE 事件名 | 携带 | 说明 |
|---|---|---|---|
| `EventIterStart` | —（内部） | Iter 序号 | 一轮推理开始 |
| `EventDeltaText` | `text` | `e.Text` | 回答正文流式片段 |
| `EventDeltaReasoning` | `reasoning` | `e.Reasoning` | 思考模型推理过程 |
| `EventToolCall` | `tool_call` | `e.Call.Name/Arguments` | 模型请求调用工具 |
| `EventToolResult` | `tool_result` | `e.Call.Name` | 工具返回 |
| `EventDone` | `done` | `e.Usage` | 本轮收敛 |
| `EventError` | `error` | `e.Err` | 本轮失败 |

> 回调可能来自多个 goroutine（一个 Loop 并发服务多会话），按 `e.SessionID` 路由；
> 回调阻塞会拖慢整个循环。

---

## 6. 会话记忆与时间旅行（快照/回滚）

### 后端切换

| 后端 | 构造 | 特点 |
|---|---|---|
| file | `memory.NewPersistent(dir, nil)` | 零依赖、JSONL 追加写、LRU 上限 |
| sqlite | `sqlite.New(dsn, sqlite.Options{})` → `Migrate` | 事务、多进程单文件 |
| redis | `redis.New(client, redis.Options{Prefix, SessionTTL})` | 原生 TTL、跨进程锁、集群安全 |
| postgres | `postgres.NewFromURL(ctx, dsn, ...)` → `Migrate` | Schema 隔离、清扫 |

**Redis key 布局**（`{}` hash tag 保证同会话 key 同 slot，集群安全）：

```
<prefix>{sessionId}           消息列表
<prefix>{sessionId}:system    标志列表
<prefix>{sessionId}:sum       摘要
<prefix>{sessionId}:lock      跨进程锁
```

**文件后端布局**（`<sessionId>.jsonl` 一行一消息 + 可选 `.summary`）：

```
sessions/
├── user-42.jsonl
└── user-42.summary
```

### 快照与安全回滚

```go
snaps := snapshot.New(driver, snapshot.Options{
	MaxPerSession: 20,              // 每会话快照上限
	LockWait:      5 * time.Second, // 跨进程锁等待
})

entry, _ := snaps.Capture(ctx, "user-42", "改动前") // 建快照
list, _ := snaps.List(ctx, "user-42")               // 新→旧
_ = snaps.Delete(ctx, "user-42", entry.ID)

// RollbackSafe 非破坏性回滚：先把当前状态存一份安全快照，再恢复到目标；
// 目标不存在不留垃圾，且回滚本身可以再回滚（时间旅行）
safe, restored, _ := snaps.RollbackSafe(ctx, "user-42", entry.ID, "回滚前")
```

---

## 7. 长期记忆（pgvector + ollama）

```go
// 向量库（先建 schema 再 Migrate）
store, _ := pgvector.New(pool, pgvector.Options{Dim: 768, Schema: "my_app"})
pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS "my_app"`)
store.Migrate(ctx)

// 向量模型（OpenAI 兼容 /v1/embeddings；换模型必须同步改 Dim）
embedder := embeddings.NewOpenAICompatible(
	"http://127.0.0.1:11434/v1", "ollama", "nomic-embed-text")

ltmMem := ltm.New(store, ltm.Options{
	Embedder: embedder, DefaultLimit: 5,
})

ltmMem.Remember(ctx, "user-42", "用户喜欢 Go", nil)  // 显式记一条
facts, _ := ltmMem.Recall(ctx, "user-42", "Go", 5)   // 向量+关键词混合召回
prompt := ltm.Prompt(facts)                           // 渲染成 system prompt 片段
ltmMem.Forget(ctx, "user-42", factID)

// 从对话抽取事实（复用对话模型，无需额外配置）
ext := extractor.New(chatLLM, extractor.Options{})
texts, _ := ext.Extract(ctx, msgs)
for _, t := range texts {
	ltmMem.Remember(ctx, "user-42", t, map[string]string{"source": "learn"})
}
```

> ⚠️ ollama 换向量模型必须同步改 `Dim`（nomic-embed-text=768、bge-m3=1024），
> 维度不匹配时写入静默降级为仅关键词可召回。

---

## 8. 可观测性

```go
// 指标：按厂商聚合
metrics := observer.NewMetrics()
defer metrics.Record("deepseek", usage, err)()
metrics.Snapshot() // map[providerID]Stats{Calls,Errors,InputTokens,...}

// 追踪：传给 agent.Config.Tracer，产出四级 span
tracer := observer.NewMemoryTracer()
loop := agent.NewLoop(llm, tools, mem, agent.Config{Tracer: tracer})
spans := tracer.Export() // agent.run → agent.iter → llm.stream / tool.exec

// 事件总线：记忆生命周期（异步非阻塞，观察者 panic 隔离）
bus := observer.NewMemoryEventBus(256, sink)
// 接到 memorystore.Options{Observer: bus} / snapshot.Options / ltm.Options

// webhook：HMAC-SHA256 签名转发 + 指数退避重试
hook := webhook.New(webhook.Config{
	URL: "https://ops.example/hook", Secret: "s3cret",
	Timeout: 5 * time.Second, MaxRetries: 3, QueueSize: 256,
})
```

---

## 9. 故障排查（踩坑实录）

### 9.1 `api key not configured`

- `--api-key-env` / `cfg.SetAPIKey` 二选一；env 方式填的是**变量名**
- 环境变量必须在启动进程里可见（`export` 与 `go run` 同一窗口）

### 9.2 `http 401 ... api key: ****xxxx is invalid`

LLM 官方拒绝：key 无效（删除/欠费/复制不全）。去平台重新生成。

### 9.3 免费档模型 429（码 1305 之类）

该档位不支持 function calling（如 glm-4.7-flash 免费档），带 tools 的请求直接被
网关拒。换支持 tools 的模型。

### 9.4 SSE 走 nginx 没有增量

nginx 默认缓冲会吞 SSE 增量。响应头加 `X-Accel-Buffering: no`（见 4.6 代码），
或 nginx 侧 `proxy_buffering off`。

### 9.5 Windows 下 `InvalidPathException`（会话 ID 含 `:`）

文件后端的平台保留字符问题；最新版已内置百分号编码修复，升级即可。

### 9.6 ollama 向量维度不匹配

换 embedding 模型没改 `Dim` → 写入向量维度对不上 → 静默降级为仅关键词召回。
检查 `ai-embedding.dim` 与模型实际维度一致。

### 9.7 Redis Cluster 报 CROSSSLOT

确保用框架驱动构造的会话 key（自带 `{sessionId}` hash tag）；不要绕开框架直接
操作会话相关的多个 key。

### 9.8 SSE 出错时消费侧无感知

Go 版流内错误通过 `StreamEvent.Error` 事件送达后关流——流正常关闭不代表本轮成功，
消费侧要检查终态事件类型。

---

## 10. agent-demo 完整接口清单

完整工程（Gin + Redis + pgvector + swaggo），22 个公开接口：

```
# Agent 对话
POST /agent/chat              有状态对话（历史存 Redis）
POST /agent/chat/stream       有状态 SSE 流式
POST /agent/chat/stateless    无状态（历史调用方携带）
POST /agent/chat/recall       带长期记忆注入的对话

# 会话记忆 / 时间旅行
GET  /memory/stats                          记忆统计（后端/出窗/事件丢弃）
GET  /memory/sessions/:id/messages          会话消息查询
POST /memory/sessions/:id/messages          追加消息（导入历史/联调）
DELETE /memory/sessions/:id                 清空会话
POST /memory/sessions/:id/snapshots         创建快照
GET  /memory/sessions/:id/snapshots         快照列表（新→旧）
DELETE /memory/sessions/:id/snapshots/:sid  删除快照
POST /memory/sessions/:id/rollback          安全回滚（非破坏，可撤销）

# 长期记忆
POST /ltm/facts               记住事实
GET  /ltm/facts               事实列表
GET  /ltm/facts/search        向量+关键词混合召回
DELETE /ltm/facts/:id         遗忘
POST /ltm/facts/learn         从对话抽取事实
POST /ltm/prompt              渲染召回片段为 system prompt
GET  /ltm/stats               LTM 统计

# 观测 / 系统
GET  /events                  事件流（进程内事件环）
GET  /system/health           健康检查
GET  /system/config           配置脱敏视图
```

**快速开始**：

```bash
# 前置：Redis(6379) + PostgreSQL+pgvector(5432)
go run .
# swagger: http://127.0.0.1:7950/swagger/index.html
```

**接口测试**（真实基础设施）：

```bash
APP_ENV=local go test ./test/ -count=1 -v            # 基础套件（不调模型，免费）
APP_ENV=local TEST_LLM=1 go test ./test/ -count=1 -v # 完整套件（含真实对话/流式/抽取）
```

覆盖：健康/配置脱敏、参数校验信封、会话增删查、快照→坏轮次→安全回滚→撤销→删快照、
长期记忆全链路、事件流、swagger 可达，以及真实模型的连续对话记忆、SSE 流式、
无状态模式、长期记忆注入、对话事实抽取。
