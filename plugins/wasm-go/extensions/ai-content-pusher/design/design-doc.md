# AI 内容推送插件（ai-content-pusher）设计文档

> 状态：设计稿（待 issue-spec 评审）
> 日期：2026-10-16
> 关联插件：ai-statistics（配置变更）、log-request-response（先例参考）

## 1. 概述

针对 LLM API 调用（典型报文：1MB 请求体 + 1MB 响应体）的日志归档场景，在 Higress 网关侧捕获全量请求/响应内容，**绕开 access log 通道**直接写入 Doris，与 ai-statistics 的统计链路（短行 access log）组成双车道架构。

核心约束（按优先级）：

1. **合规红线**：access log 链路（含采集器、日志服务）不允许出现任何用户内容片段
2. **调用方零改造**：内容捕获在网关侧完成，调用方只需携带两个 header
3. **零新增中间件**：不引入 Kafka；只依赖已有 Doris（及冷热分层所需的对象存储）
4. **不影响主请求**：归档失败只丢日志，绝不影响请求转发

## 2. 方案决策记录

设计过程中评估并否决的备选方案（详细论证见文末参考资料）：

| 备选方案 | 否决原因 |
|---|---|
| 全量内容写入 access log（含 as_separate_log_field） | 2MB+ 行触发容器运行时 16KB P/F 拆行 → 采集器 512KB 二次拆段 → JSON 报废；且违反合规红线 |
| 调用方直写 Doris/对象存储 | 调用方多且不可控，改造与审计成本高；网关是唯一同时握有完整双边报文的位置 |
| 插件前置 Kafka | 官方建议 Stream Load 单批 100MB~1GB，wasm 实例内存预算 200MB 撑不起客户端攒批；Kafka 集群本身是新的运维组件。**Group Commit（Doris 2.1+）把攒批下沉到服务端**，Kafka 的唯一价值被替代 |
| 直推对象存储（无 content 表） | 仅当环境已有 OSS 且放弃 SQL 点查时才更优（需在插件内实现 S3 v4 签名）；作为演进路径保留 |
| SLS 承载内容 | 索引前缀默认 2KB/最高 16KB（maxTextLen），logtail 单行默认 512KB；检索价值与存储成本严重不对等 |
| 单表（官方日志 recipe 原样） | 统计与内容保留期分化（180 天 vs 30 天），单表动态分区 TTL 无法独立 |

**先例参考**：`log-request-response` 插件已实现"请求/响应体写入 access log"（FILTER_STATE + maxSize 默认 10KB）。本插件的差异在于目标通道（绕开 access log）与量级（2MB，为其默认上限的 200 倍）。

## 3. 总体架构

```
 客户端 ── LLM 请求（x-request-id / x-conversation-id）──→ Higress 网关
                                                           │
             ┌─────────────────────────────────────────────┤
             │                                             │
  ┌──────────▼───────────┐                      ┌───────────▼───────────┐
  │    ai-statistics     │                      │   ai-content-pusher   │
  │  纯统计，零内容提取    │                      │  缓冲全量请求/响应体    │
  └──────────┬───────────┘                      └───────────┬───────────┘
             │                                              │
  车道1：ai_log 短行(<2KB)                         车道2：全量内容(2MB/请求)
  统计字段 + 业务键，零用户内容                     {request_id, bodies}
             │                                              │
  ┌──────────▼───────────┐                      Stream Load PUT（2MB/次）
  │  access log(<16KB/行) │                      直连 BE:8040
  └──────────┬───────────┘                      group_commit=async_mode
             │                                  fire-and-forget + 丢弃计数
  ┌──────────▼───────────┐                              │
  │  FluentBit（默认配置） │                              │
  └──────────┬───────────┘                              │
             │                                          ▼
             │              ┌────────────────────────────────────────┐
             └─────────────→│         Doris（已有组件）                │
                            │  llm_call_stats      llm_call_content  │
                            │  180天·全程热存        30天·3天冷却S3    │
                            └────────────────────┬───────────────────┘
                                                 │ 3天后自动冷却
                                                 ▼
                                          ┌──────────────┐
                                          │   S3/OSS     │
                                          └──────────────┘
```

排障路径：`conversation_id → stats 表定位异常请求 → request_id 点查 content 表看全量`。30 天后 stats 仍有记录而 content 已过期，属正常分支，需返回"内容已过保留期"。

## 4. 插件设计

### 4.1 职责边界

**做**：缓冲全量请求体/响应体；按采样规则过滤；响应结束时异步推送 Doris；失败计数。

**不做**：不修改请求/响应（纯旁路）；不做攒批（Group Commit 服务端负责）；不做复杂重试（默认不重试）；不影响任何主请求语义。

与 ai-statistics 保持**独立插件**：两者失败语义不同（统计丢失影响大盘，内容丢失影响排障，但都不允许拖垮请求），独立演进互不牵连。

### 4.2 配置 schema

```yaml
sink:
  type: doris_stream_load
  endpoints:
    - "http://be-1:8040"        # 直连 BE，绕开 FE 的 307 重定向（wasm HttpCall 不保证跟随）
    - "http://be-2:8040"
  database: log_db
  table: llm_call_content
  username: ""
  password: ""                  # 建议从 KMS/Secret 注入
  groupCommit: async            # off | async | sync
  timeoutMs: 3000
sampling:
  ratio: 100                    # 采样百分比，100=全量
buffer:
  maxRequestBodyBytes: 2097152   # 超限截断并打标 truncated
  maxResponseBodyBytes: 2097152
```

### 4.3 请求处理流程

```
OnHttpRequestBody        累积请求体（上限 buffer.maxRequestBodyBytes）
OnHttpResponseBody       累积流式响应块（append，上限 maxResponseBodyBytes）
endOfStream=true         组装消息 → 异步 Stream Load（fire-and-forget）
OnDone / stream done     释放 context 资源
```

消息体（JSON，单次 Stream Load body）：

```json
{
  "request_id": "req-abc123",
  "conversation_id": "conv-789",
  "user_id": "u-42",
  "ts": "2026-10-16T12:00:00+08:00",
  "model": "qwen-max",
  "status_code": 200,
  "request_body": "{...原始请求 JSON...}",
  "response_body": "...原始响应（流式为各 chunk 原样拼接）...",
  "truncated": false
}
```

归档原则：**原样存报文**，不做语义化重组——排障时看到的就是线上真实字节；语义化字段（model/token 等）由车道 1 承担。

Stream Load 参数：

| Header | 值 | 说明 |
|---|---|---|
| `label` | `pusher-{request_id}` | 以 request_id 做标签，天然幂等去重 |
| `group_commit` | `async` | 服务端攒批，快速返回；WAL 兜底崩溃 |
| `format` | `json` + `read_json_by_line` | 单条 JSON 即一行 |
| `timeout` | `3000` | 与配置一致 |

### 4.4 失败语义

- 任何失败（连接失败、超时、Doris 返回错误）→ 计数器 `pusher_dropped_total` +1，**不影响主请求**
- 默认不重试（`retry: 0` 可配），丢失窗口对排障日志可接受
- 采样在缓冲**之前**判定，未命中的请求零缓冲开销

### 4.5 内存预算

- 每个在途请求 ≈ 3~4MB（请求体 1MB + 流式响应累积 1MB + 消息序列化 1~2MB）
- 并发 1000 时网关内存增量 GB 级——**这是网关侧捕获的固有代价**，容量规划必须计入
- wasm 实例内存上限（wrapper `WithRebuildMaxMemBytes`）需单独评估，不能沿用 ai-statistics 的 200MB 配置照抄

## 5. Doris 侧设计

### 5.1 建表（关键参数，完整 DDL 见运维手册）

公共：`DUPLICATE KEY(ts)` · RANGE 按天动态分区 · `DISTRIBUTED BY RANDOM` · `compression=zstd` · `compaction_policy=time_series`

| | llm_call_stats | llm_call_content |
|---|---|---|
| 保留 | `dynamic_partition.start="-180"` | `dynamic_partition.start="-30"` |
| 冷却 | 无（全程热存） | `storage_policy` cooldown_ttl=259200（3天） |
| 倒排索引 | request_id / model / consumer | 仅 request_id |

### 5.2 BE 配置变更（一次）

```
string_type_length_soft_limit_bytes = 10485760   # 官方日志教程值；默认 1MB 不够，需滚动重启
```

### 5.3 ai-statistics 配置（车道 1，配合变更）

```yaml
# 不开 use_default_attributes —— 默认列表含 question/answer，会把内容写进 ai_log
attributes:
  - key: conversation_id
    value_source: request_header
    value: x-conversation-id
    apply_to_log: true
  - key: user_id
    value_source: request_header
    value: x-user-id
    apply_to_log: true
```

model/token/ttft/duration 由 tokenusage 包直接写入 ai_log，不依赖属性列表（已对 main.go 逐行确认）。其余合规口子：log_format 模板无 body 占位符、网关日志级别 ≥ info、FluentBit 默认配置不做 body 捕获。

## 6. 验证计划（实现前必须完成）

| # | 风险点 | 验证方法 | 通过标准 |
|---|---|---|---|
| V1 | endOfStream 时刻 HttpContext 生命周期 | 最小原型：在 OnHttpResponseBody(endOfStream=true) 发起 proxywasm DispatchHttpCall | 调用可达 Doris；若 context 提前销毁，评估 pause/resume 方案的尾延迟代价并记录决策 |
| V2 | 直连 BE 的 Stream Load 链路 | curl 验证 BE:8040 + group_commit=async + label 幂等；wasm 原型发真实 2MB 消息 | 200/OK；重复 label 被去重；async 模式返回延迟 < 100ms |
| V3 | 真实负载压测 | 真实 2MB 行 × 目标 QPS 压 Group Commit 导入；观测 BE 内存/compaction score/导入延迟 | 无内存超限、compaction score 无持续攀升（对照 issue #43550 场景） |
| V4 | 网关内存水位 | 并发压测网关，观测 wasm 实例内存与 P99 尾延迟 | 内存增量符合 3~4MB/并发预估；P99 尾延迟增量可接受（目标 < 5ms） |
| V5 | 合规红线 | 在 129 测试集群按第 5.3 节配置跑真实对话 | ai_log 及 access log 中 grep 不到任何请求/响应内容片段 |

## 7. 演进路径

- **写入吞吐不足** → sink 增加 `type: kafka`，前置 Kafka，Routine Load 接管攒批；表结构不变
- **确认无需 SQL 点查内容** → content 表退役，sink 改推对象存储（复用冷却桶路径规范 `{date}/{request_id}.json.gz`），BE 的 10MB 配置变更随之取消
- **存储成本敏感** → `sampling.ratio` 下调，全量归档改采样归档
- **合规升级** → 车道 2 消息体加密（信封加密，密钥不入 Doris）

## 8. 参考资料

- [Doris 日志存储与分析（官方教程）](https://doris.apache.org/zh-CN/docs/4.x/observability/log)——建表参数、Group Commit、资源估算的出处
- [Doris STRING 类型](https://doris.apache.org/docs/3.x/sql-manual/basic-element/sql-data-types/string-type/STRING)——`string_type_length_soft_limit_bytes` 默认 1MB
- [Doris issue #43550](https://github.com/apache/doris/issues/43550)——大字段 Stream Load 内存超限案例
- 仓库内先例：`plugins/wasm-go/extensions/log-request-response`（体→access log + maxSize 上限）、`ai-load-balancer/design/design-doc.md`（文档格式参照）

## 9. 贡献路径说明

本仓库 `.cursor/rules/plugin-development.mdc` 规定：AI 参与的上游贡献须走 issue-spec 流程（Proposal Issue → maintainer 批准的 Design Issue），本地 `design/` 目录仅作为工作稿，不能替代已批准的 Design Issue。若本插件走上游贡献，本文档内容应转入 issue-spec 的 Design Issue；若为内部部署，本文档即设计基线。
