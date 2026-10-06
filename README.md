# send-agent

Go 实现的多平台 LLM Agent 框架。统一内部消息类型，协议适配层兼容多家 OpenAI-compatible 提供商，流式为一等公民。

## 快速开始

```bash
export DEEPSEEK_API_KEY=sk-xxx
go run ./examples/simple_chat "你好"
```

切换提供商：`AGENT_PROVIDER=kimi KIMI_API_KEY=sk-xxx go run ./examples/simple_chat`

## 核心用法

```go
registry := provider.NewRegistry()          // 内置 kimi/glm/deepseek
cfg := registry.MustGet("glm")
cfg.LoadAPIKeyFromEnv()                     // 读 GLM_API_KEY

llm, _ := adapters.NewLLM(cfg)
wrapped := core.StreamRetry(                // 流式首 token 前可重试
    core.NewPipeline(llm,                   // 非流式全中间件链
        core.Logging(nil),
        core.Retry(3),
    ), 3,
)

toolReg := tools.NewRegistry()
toolReg.Register(myTool{})

loop := agent.NewLoop(wrapped, toolReg, memory.NewBuffer(nil), agent.Config{
    Model:         cfg.DefaultModel,
    SystemPrompt:  "你是一个助手",
    MaxIterations: 16,       // 熔断上限，默认 16
    TokenBudget:   32_000,   // 单次调用输入 token 预算，超限截断历史
    OnEvent: func(e agent.LoopEvent) { ... },  // 流式增量/工具事件回调
})

msg, usage, err := loop.Run(ctx, "session-id", "用户输入")
```

## HTTP 服务

```bash
export DEEPSEEK_API_KEY=sk-xxx
go run ./cmd/server --addr :8080 --provider deepseek

# 追加自定义厂商（OpenAI 兼容网关），零代码接入
go run ./cmd/server --config config/providers.example.json
```

| 端点 | 说明 |
|---|---|
| `POST /api/chat` | 对话；`stream: true` 时返回 SSE |
| `GET /api/providers` | 提供商与模型列表 |
| `GET /api/metrics` | 按提供商聚合的调用量/错误/token 指标 |
| `GET /api/health` | 存活探针 |

```bash
curl -N localhost:8080/api/chat -d '{
  "session_id": "s1",
  "input": "帮我查下天气",
  "stream": true
}'
```

SSE 事件：`text` / `reasoning`（增量）→ `tool_call` / `tool_result` → `done`（含 content 与累计 usage）或 `error`。客户端断连即取消整条循环。

## 架构分层

```
entry               HTTP/SSE 入口：POST /api/chat、GET /api/providers、/api/metrics
orchestrator        工作流编排：AgentStep + 人工检查点，断点续跑
agent.Loop          ReAct 循环：MaxIterations 熔断、token 预算、工具并行执行
core.Pipeline       中间件链：Logging / Retry / RateLimit / Fallback / Cache
core.LLM            统一对话接口（能力信息在 ModelConfig，不进接口）
adapters/protocol   OpenAI / Anthropic / Gemini 三协议，stateful SSE 解码 + Quirks hook
adapters/provider   注册表：kimi / glm / anthropic / gemini / deepseek，配置驱动 + 定价
config              JSON 配置文件注册自定义厂商
memory              Buffer / Persistent(JSONL) / Summary(LLM 压缩)
tools/builtin       calculator(go/ast 白名单求值) / clock / http_fetch
tools/mcp           MCP 服务器接入（stdio，JSON-RPC 2.0）
observer            提供商级调用量/错误/token 指标聚合
```

## MCP 工具接入

```go
client, err := mcp.ConnectStdio(ctx, "my-tools", "npx", "-y", "some-mcp-server")
defer client.Close()
err = client.Register(ctx, toolReg)   // 握手 + tools/list + 全量注册为 core.Tool

// Streamable HTTP 服务器
client = mcp.ConnectHTTP("remote", "https://mcp.example.com/mcp", "bearer-token")
```

## 多 Agent 模式

```go
_ = orch.RegisterWorkflow(&orchestrator.Workflow{
    Name: "routed",
    Steps: []orchestrator.Step{
        orchestrator.RouterStep{                      // 分类后选择执行者
            Router: "classifier", Candidates: []string{"writer", "reviewer"}, Input: "$input",
        },
        orchestrator.SupervisorStep{                  // 监督者分解委派直至 DONE
            Supervisor: "boss", Workers: []string{"writer", "reviewer"}, Input: "$input",
        },
    },
})
```

Supervisor 协议：监督 Agent 每轮首行 `WORKER <name>`（委派）或 `DONE`（收敛），轮数上限防死循环。

## 多模态与追踪

```go
messages := []core.Message{core.UserImage("这张图是什么", "data:image/png;base64,...")}
loop := agent.NewLoop(llm, tools, mem, agent.Config{Model: "m", Tracer: tracer})
```

三协议 vision 请求已映射（openai image_url / anthropic base64 source / gemini inlineData）。
`core.Tracer`/`Span` 接口 + `observer.MemoryTracer` 内存实现，span 树：`agent.run → agent.iter → llm.stream / tool.exec`，编排层另有 `workflow.run → workflow.step`。

## WebSocket 与 gRPC 入口

`GET /api/chat/ws`（RFC 6455 零依赖实现）：入站文本帧为对话请求，事件 JSON 帧回推，断连即取消当次循环；支持 ping/pong。

gRPC：`Chat` 一元 + `ChatStream` 服务端流。环境无 protoc，采用运行时构造 descriptor + dynamicpb 动态消息，零生成代码——服务定义写在 `pkg/entry/grpc_desc.go` 顶部注释里，客户端用 `entry.GRPCChatRequestDesc()` 构造动态消息、原生 `Invoke`/`NewStream` 调用：

```go
conn.Invoke(ctx, "/agentframework.AgentService/Chat", req, resp)
```

启动：`go run ./cmd/server --grpc :9090`

## 关键设计决策

**协议优先于厂商。** 厂商偏差不走子类，统一走 `protocol.Quirks`：

```go
// deepseek-reasoner 收到 temperature 会 400，请求前删除
Quirks: protocol.Quirks{
    PatchRequest: deepSeekReasonerPatch,
}
```

**流 channel 语义钉死。** 生产者 close；错误只走 `StreamError` 终止事件；`ChatStream` 的 error 返回值仅用于建连前失败。

**流式重试只发生在首 token 前。** `StreamRetry` 缓冲至首个内容事件，之后失败原样透传，避免重复输出。

**Memory 按 session 隔离。** Agent Loop 无状态可复用，同一 Loop 可并发服务多个会话。

**工具失败不熔断循环。** 错误文本作为 tool 消息回传，模型自行调整；只有 LLM 错误和迭代超限才终止。同轮多个工具调用并行执行。`LoopEvent` 携带 `SessionID`；单个 Run 内回调来自单一 goroutine，**同一 Loop 并发服务多会话时回调来自多个 goroutine**，需按 SessionID 路由。

## 工作流编排

```go
store, _ := orchestrator.NewFileRunStore("./runs")
orch := orchestrator.New(store)

orch.RegisterAgent("writer", writerLoop)
orch.RegisterAgent("reviewer", reviewerLoop)
_ = orch.RegisterWorkflow(&orchestrator.Workflow{
    Name: "review",
    Steps: []orchestrator.Step{
        orchestrator.AgentStep{Agent: "writer", Input: "$input"},
        orchestrator.CheckpointStep{Name: "approve", Prompt: "人工审核草稿"},
        orchestrator.AgentStep{Agent: "reviewer", Input: "$prev"},
    },
})

run, err := orch.Run(ctx, "review", "写一段导语")   // 停在检查点，err == ErrCheckpoint
run2, err := orch.Resume(ctx, run.ID, "通过")       // 人工输入喂给下一步
```

模板变量：`$input`（初始输入）、`$prev`（上一步输出/人工输入）、`$step.<name>.output`（指定步骤输出）。运行状态 JSONL 落盘（写入前 fsync），进程重启后重新注册工作流即可 `Resume`；`waiting`（检查点）与 `running`（取消/崩溃中断）状态均可续跑，中断续跑按至少一次语义重执行当前步骤。

## 持久化记忆

```go
mem, _ := memory.NewPersistent("./sessions", nil)  // 重启自动恢复，坏行容错
loop := agent.NewLoop(llm, tools, mem, cfg)         // 接口与 Buffer 一致，直接替换
```

摘要压缩：截断丢弃的旧消息用 LLM 压缩成前缀，按丢弃数量缓存，压缩失败退化为纯截断：

```go
mem := memory.NewSummary(inner, cheapLLM)
```

## 中间件

```go
// LLM 级中间件同时覆盖 Chat 与 ChatStream 两条路径；
// 服务端主路径（Agent 循环）恒为流式，挂 ChatMiddleware 的中间件对流式不生效
wrapped := core.StreamRetry(
    core.LoggingLLM(nil)(          // 双路径日志
        core.RateLimitLLM(10, time.Second)(   // 双路径令牌桶限流
            core.NewPipeline(llm, core.Retry(3)),  // Retry 仅非流式
        ),
    ), 3,
)
// 其余：FallbackLLM 主备切换（流式仅首内容事件前）；Cache 响应缓存（仅非流式、无工具请求）
```

结构化输出：`ChatRequest.ResponseFormat = &core.ResponseFormat{Name, Schema}`（openai/gemini 已映射，anthropic 显式报错——该协议无原生支持）。成本核算：`ModelConfig.CostOf(usage)`（美元口径，国内厂商按 7.25 汇率折算），`/api/chat` 响应与 done 事件携带 `cost_usd`。

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
└── config_test.go            配置文件加载与校验
```

```bash
go test ./test/       # 只跑框架测试
go test ./...         # 全量
go test -race ./test/
```

白盒测试（需要访问未导出符号时）例外，按 Go 惯例留在源码包内的 `xxx_test.go`
