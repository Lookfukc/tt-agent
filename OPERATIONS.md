# tt-agent 操作文档（OPERATIONS.md）

> 面向使用者的完整操作手册：环境准备 → 跑服务 → 调接口 → 配置 → 运维排查。
> 姊妹文档：[README.md](README.md)（框架总览与 API 参考）｜配套实战示例 [agent-demo](https://github.com/Lookfukc/agent-demo)（Gin + Redis + pgvector 完整消费者）。

**目录**

- [1. 环境准备](#1-环境准备)
- [2. 五分钟跑起来（零代码）](#2-五分钟跑起来零代码)
- [3. 服务配置详解](#3-服务配置详解)
- [4. 接口操作手册](#4-接口操作手册)
- [5. 用 Docker 起依赖](#5-用-docker-起依赖)
- [6. 记忆后端切换](#6-记忆后端切换)
- [7. 长期记忆（pgvector）](#7-长期记忆pgvector)
- [8. 观测与运维](#8-观测与运维)
- [9. 故障排查](#9-故障排查)
- [10. 示例项目：agent-demo](#10-示例项目agent-demo)

---

## 1. 环境准备

| 项 | 要求 | 检查命令 |
|---|---|---|
| Go | ≥ 1.24（以 `go.mod` 为准） | `go version` |
| LLM 凭据 | 任一提供商 API Key（DeepSeek / 智谱 / Anthropic / Google / ollama） | — |
| Redis（可选） | ≥ 6.0，会话记忆/快照后端 | `redis-cli ping` |
| PostgreSQL（可选） | ≥ 14 + pgvector 扩展，长期记忆 | `psql -c 'select 1'` |

> 工具链提示：`GOSUMDB=off` 环境下 Go 自动下载新工具链会被拒绝，用 `GOTOOLCHAIN=local`
> 或先安装对应版本。

---

## 2. 五分钟跑起来（零代码）

不需要克隆仓库，一条命令起服务（HTTP/SSE/WS 全有）：

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
  -H "Content-Type: application/json" \
  -d '{"input":"你好"}'
```

从源码构建（要改代码/加 gRPC 时）：

```bash
git clone https://github.com/Lookfukc/tt-agent.git
cd tt-agent
go build -o server ./cmd/server
./server --base-url https://api.deepseek.com/v1 \
         --model deepseek-chat \
         --api-key-env DEEPSEEK_API_KEY \
         --memory-type redis --memory-dsn redis://127.0.0.1:6379
```

---

## 3. 服务配置详解

`cmd/server` 全部命令行参数（完整列表见 `--help`）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--addr` | `:8080` | HTTP 监听地址 |
| `--grpc` | 空 | gRPC 监听地址（如 `:9090`，空=关闭） |
| `--protocol` | openai | openai / anthropic / gemini |
| `--base-url` | 必填 | API 根地址（不带 /chat/completions） |
| `--api-key-env` | 必填 | **环境变量的名字**（不是 key 本身！） |
| `--model` | 必填 | 默认模型 |
| `--prompt` | 空 | 默认 system prompt |
| `--memory-type` | file | file / sqlite / redis / postgres |
| `--memory-dir` | 系统临时目录 | file 后端目录 |
| `--memory-dsn` | — | sqlite 路径 / redis URL / postgres 连接串 |
| `--memory-ttl` | 0 | 会话保留期（0=永久；redis 原生过期，sqlite/pg 后台清扫） |
| `--memory-prune-every` | 5m | 清扫周期 |
| `--httpfetch` | 关 | 注册 http_fetch 工具（注意 SSRF 面） |

> ⚠️ **最常见的坑**：`--api-key-env` 填 `DEEPSEEK_API_KEY`（变量名），启动进程的环境里
> 要真的能读到这个变量。填成 key 本身会报 `api key not configured`。

---

## 4. 接口操作手册

### 4.1 REST

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/health` | 存活探针 |
| GET | `/api/providers` | 已注册厂商与模型（含能力/定价） |
| GET | `/api/metrics` | 指标快照（按厂商聚合调用数/错误数/token） |
| POST | `/api/chat` | 对话；`stream=true` 返回 SSE |

`POST /api/chat` 请求体：

```json
{
  "session_id": "user-42",
  "provider_id": "",
  "model": "",
  "input": "你好",
  "stream": false,
  "system_prompt": ""
}
```

两种模式二选一：`session_id`（有状态，历史由服务端 memory 管理）或
`messages`（无状态，带全量历史，响应返回 `new_messages` 供你持久化）。

响应（有状态非流式）：

```json
{
  "session_id": "user-42",
  "content": "你好！有什么可以帮你？",
  "reasoning": "",
  "usage": { "input_tokens": 12, "output_tokens": 8 },
  "cost_usd": 0.0000124
}
```

### 4.2 SSE 流式

`"stream": true` 时返回 `text/event-stream`：

```
event: text
data:好的

event: tool_call
data:{"id":"c1","name":"calculator","arguments":"{\"expression\":\"23*7+1\"}"}

event: tool_result
data:{"id":"c1","name":"calculator","error":""}

event: done
data:{"event":"done","content":"...","usage":{...},"cost_usd":0.0001}
```

curl 记得加 `-N`（关缓冲）：

```bash
curl -N -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"session_id":"s1","input":"计算 23*7+1","stream":true}'
```

### 4.3 WebSocket

`/api/chat/ws`——每帧一个独立请求（JSON 同上），事件以 JSON 文本帧推回；
断开自动取消进行中的循环与工具执行。

### 4.4 gRPC

服务 `agentframework.AgentService`（`Chat` 一元 + `ChatStream` 服务端流）：

```protobuf
message ChatRequest  { string session_id=1; string provider_id=2; string model=3;
                       string input=4; string system_prompt=5; }
message ChatResponse { string event=1; string delta=2; string content=3; string error=4;
                       int64 input_tokens=5; int64 output_tokens=6; int64 reasoning_tokens=7;
                       double cost_usd=8; }
```

启动加 `--grpc :9090`，客户端无需 protoc（运行时 descriptor，见 `entry` 包）。

---

## 5. 用 Docker 起依赖

```bash
# Redis
docker run -d --name tt-redis -p 6379:6379 redis:7

# PostgreSQL + pgvector（长期记忆用）
docker run -d --name tt-pg \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 pgvector/pgvector:pg16
docker exec tt-pg psql -U postgres -c 'create extension if not exists vector'
```

服务接上：

```bash
./server --memory-type redis --memory-dsn redis://127.0.0.1:6379 \
  --memory-ttl 24h \
  --base-url https://api.deepseek.com/v1 \
  --model deepseek-chat --api-key-env DEEPSEEK_API_KEY
```

---

## 6. 记忆后端切换

| 后端 | DSN 形态 | 特点 |
|---|---|---|
| file | `--memory-dir /data/sessions` | 零依赖、JSONL 追加写；单机 |
| sqlite | `--memory-dsn /data/memory.db` | 事务、多进程单文件 |
| redis | `--memory-dsn redis://127.0.0.1:6379` | 原生 TTL、跨进程锁、集群安全（hash tag） |
| postgres | `--memory-dsn postgres://u:p@host:5432/db` | Schema 隔离、清扫、可挂 pgvector |

Redis key 布局（`{}` hash tag 保证同会话 key 同 slot）：

```
<prefix>{sessionId}           消息列表
<prefix>{sessionId}:system    标志列表
<prefix>{sessionId}:sum       摘要
<prefix>{sessionId}:lock      跨进程锁
```

文件后端布局（`<sessionId>.jsonl` 一行一消息 + 可选 `.summary`；特殊字符百分号编码）：

```
sessions/
├── user-42.jsonl
└── user-42.summary
```

---

## 7. 长期记忆（pgvector）

作为库使用时（服务端 `cmd/server` 未内置 LTM，接入方式见 agent-demo）：

```go
vecStore, err := pgvector.NewFromURL(ctx,
    "postgres://postgres:postgres@localhost:5432/postgres",
    pgvector.Options{Dim: 768, Schema: "public", MaxPerNamespace: 1000})
vecStore.Migrate(ctx)

embedder := embeddings.NewOpenAICompatible("http://localhost:11434/v1", "ollama")

ltmMem := ltm.NewMemory(ctx, vecStore, embedder,
    ltm.NewLLMExtractor(chatLLM),
    ltm.Options{Namespace: "user-42", Backend: "demo"})

ltmMem.Learn(ctx, msgs)          // 从对话抽取事实
facts, _ := ltmMem.Recall(ctx, "Java", 5)
prompt := ltm.Prompt(facts)      // 拼进 system prompt
```

---

## 8. 观测与运维

- **指标**：`GET /api/metrics` 按厂商聚合（calls/errors/input_tokens/output_tokens）
- **追踪**：`observer.NewMemoryTracer()` 传给 `agent.Config.Tracer`，产出
  `agent.run → agent.iter → llm.stream / tool.exec` 四级 span
- **事件总线**：`observer.NewMemoryEventBus(256, sink)` 观察记忆生命周期
  （messages_appended / session_trimmed / fact_remembered ...），异步非阻塞
- **webhook**：`webhook.New(url, secret)` HMAC-SHA256 签名转发，指数退避重试
- **会话清理**：redis 后端 `--memory-ttl` 原生过期；sqlite/pg 由
  `--memory-prune-every` 周期清扫空闲会话

---

## 9. 故障排查

| 症状 | 原因与处理 |
|---|---|
| `api key not configured` | `--api-key-env` 填的是**变量名**；确认启动进程能读到该变量 |
| `http 401 ... api key: ****xxxx is invalid` | LLM 官方拒绝：key 无效（删除/欠费/复制不全），去平台重新生成 |
| Windows 下 `InvalidPathException`（会话 ID 含 `:`） | 文件后端保留字符问题；确保使用最新版（已内置百分号编码修复） |
| SSE 出错时日志刷 `HttpMessageNotWritableException` | 自建服务时的坑：SSE 错误必须作为事件发回，不能走全局异常处理器写 JSON |
| redis 跨进程并发报错 | 确认使用框架提供的驱动（内置 MULTI/EXEC 串行化与 Lua trim），不要绕开直连操作会话 key |
| gRPC 客户端接不上 | 客户端用任意原生 protobuf 按上面的 proto 契约调用即可，无需 protoc 生成 |
| 代理网关 429（码 1305 之类） | 该模型档位不支持 function calling（如 glm-4.7-flash 免费档），换支持 tools 的模型 |

---

## 10. 示例项目：agent-demo

完整消费者工程（Gin + Redis + pgvector + swaggo），把本框架的记忆子系统全部能力
暴露为公开接口，是本文档的最佳实操参考：

**技术栈**：DeepSeek/GLM 对话（OpenAI 兼容）｜Redis 会话记忆 + 快照回滚｜
PostgreSQL+pgvector 长期记忆（ollama 向量）｜记忆事件总线｜swaggo 文档。

**接口全景**（全部公开）：

```
POST /agent/chat              有状态对话（历史存 Redis）
POST /agent/chat/stream       有状态 SSE 流式
POST /agent/chat/stateless    无状态（历史调用方携带）
POST /agent/chat/recall       带长期记忆注入的对话
GET  /memory/stats            记忆统计
GET  /memory/sessions/:id/messages      会话消息查询
POST /memory/sessions/:id/messages      追加消息
DELETE /memory/sessions/:id             清空会话
POST /memory/sessions/:id/snapshots     创建快照
GET  /memory/sessions/:id/snapshots     快照列表
DELETE /memory/sessions/:id/snapshots/:sid  删除快照
POST /memory/sessions/:id/rollback      回滚到快照（非破坏性，可撤销）
POST /ltm/facts               记住事实
GET  /ltm/facts               事实列表
GET  /ltm/facts/search        向量+关键词混合召回
DELETE /ltm/facts/:id         遗忘
POST /ltm/facts/learn         从对话抽取事实
POST /ltm/prompt              渲染召回片段为 system prompt
GET  /events                  事件流（进程内事件环）
GET  /system/health           健康检查
GET  /system/config           配置脱敏视图
```

**快速开始**：

```bash
# 前置：Redis(6379) + PostgreSQL+pgvector(5432)
git clone https://github.com/Lookfukc/agent-demo.git
cd agent-demo
go run .
# swagger: http://127.0.0.1:7950/swagger/index.html
```

配置在 `configs/config.yaml`（`ai-chat` 段配对话模型、`redis`/`postgres` 段配存储、
`agent` 段配记忆参数），环境差异写 `config.<env>.yaml` 覆盖。

接口测试（真实基础设施）：

```bash
APP_ENV=local go test ./test/ -count=1 -v          # 基础套件（不调模型，免费）
APP_ENV=local TEST_LLM=1 go test ./test/ -count=1 -v # 完整套件（含真实对话/流式/抽取）
```
