---
title: 集群并发数限制
keywords: [higress, concurrency-limit]
description: 集群并发数限制插件配置参考
---

## 功能说明

`cluster-concurrency-limit` 插件基于 Redis 实现**集群级并发数限制**（Gauge 语义），适用于需要跨多个 Higress Gateway 实例进行**全局一致并发控制**的场景，典型如 LLM API 的流式（SSE）请求并发保护。

### 与窗口限流的语义差异（Gauge vs 窗口计数）

- `cluster-key-rate-limit` 是**窗口计数**（Counter）语义：统计固定/滑动时间窗口内的请求**次数**，窗口结束自动清零，无需请求结束回调。
- 本插件是**并发数**（Gauge）语义：请求进入时对计数器 **INCR**（占位），请求结束（流关闭）时 **DECR**（释放）。计数器表示"此刻正在处理中的请求数"，超过并发上限的请求立即被拒绝（默认 429）。

对 LLM 流式场景，一条 SSE 请求可能持续数分钟，窗口限流无法表达"同时只有 N 条生成中请求"；本插件的时间窗口就是请求自身的生命周期，天然匹配该需求。

### 流式与非流式一视同仁

插件不区分流式/非流式请求：请求头阶段统一 INCR 占位，`onHttpStreamDone`（响应流结束/断开）统一 DECR 释放。流式请求会在其**整个流持续期间**占用并发槽位，非流式请求占用时间即其请求处理时长。

### `cluster-` 前缀与 Redis 强依赖

- `cluster-` 前缀表示计数器存储在 Redis 中，跨多个 Higress Gateway 实例共享，实现集群级一致性。
- Redis 是**强依赖**：Redis 不可用时插件采用 **fail-open** 策略（放行请求并记录 `concurrency_limit_redis_error` 指标），不会导致网关拒绝所有流量，但此时并发限制失效。
- 请求开始时的占位在**单次原子 Lua EVAL** 中完成：对所有命中计数器统一 INCR，任一超限则**全部回滚**（all-or-nothing），不存在部分占位导致的计数漂移。

### 多规则匹配语义（all-match OR）

与 `cluster-key-rate-limit` 当前行为一致：`global_threshold` 与 `rule_items` 可混合配置，**所有命中**的规则都会被评估，规则之间是"或"关系，**任一触发即拒绝**。规则的执行顺序不影响最终结果。

### 运行时行为要点

- **执行阶段：认证阶段之后**。本插件必须在 `key-auth` / `jwt-auth` / `basic-auth` 等认证插件**之后**执行，否则 `limit_by_consumer` / `limit_by_per_consumer` 读不到认证插件注入的 `x-mse-consumer` 请求头，consumer 维度限流将失效。
- **Redis key 格式**（`{rule_name}` 带 hash tag，兼容 Redis Cluster 按规则聚合到同一 slot）：
  - 全局：`higress-concurrency-limit:{rule_name}:global_threshold`
  - 规则项：`higress-concurrency-limit:{rule_name}:{limit_type}:{limit key 名}:{实际值}`
- **租约自动回收（lease_ttl_seconds）**：每个计数器首次 INCR 时设置 TTL，且每次成功占位都会为本请求命中的全部计数器续租（持续有成功放行请求的计数器不会因 TTL 过期被误删）。若网关进程异常退出（如 `kill -9`、OOM）导致 `onHttpStreamDone` 未执行，泄漏的计数会在 TTL 到期后由 Redis 自动清零，无需人工干预。单个请求在飞期间没有专属续期，因此 **TTL 必须大于目标场景的最长请求时长**（LLM 流式场景建议大于最大生成时长），否则长请求占位可能被提前回收。

## 运行属性

插件执行阶段：`默认阶段`（UNSPECIFIED_PHASE，实际执行位于认证插件之后）
插件执行优先级：`610`

> ⚠️ `limit_by_consumer` / `limit_by_per_consumer` 依赖认证插件（key-auth / jwt-auth / basic-auth 等）注入的 `x-mse-consumer` 请求头。pilot 按 AUTHN → AUTHZ → STATS → 默认阶段 的顺序装配 filter 链，phase 保持默认即可保证本插件在认证之后执行；若将 phase 显式改为 `认证阶段`（AUTHN）且排在认证插件之前，consumer 维度规则将读不到该请求头而失效。

## 配置说明

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
| ------------------------ | ------ | ---- | ------ | ---- |
| rule_name | string | 是 | - | 限流规则名称，根据限流规则名称 + 限流类型 + 限流 key 名称 + 限流 key 对应的实际值来拼装 redis key |
| global_threshold | object | 否，`global_threshold` 与 `rule_items` 至少配置一项，可同时配置 | - | 对整个自定义规则组进行并发限制 |
| rule_items | array of object | 否，同上 | - | 限流规则项，最多支持 **10 条**。所有满足匹配条件的 `rule_item` 都会参与限流，规则之间是"或"关系，任一触发即拒绝；规则的执行顺序不影响最终结果。详见下方展开。 |
| rejected_code | int | 否 | 429 | 请求被限流时，返回的 HTTP 状态码 |
| rejected_msg | string | 否 | concurrency limit exceeded | 请求被限流时，返回的响应体 |
| show_limit_quota_header | bool | 否 | false | 响应头中是否显示 `X-Concurrency-Limit`（并发上限）和 `X-Concurrency-Remaining`（剩余可用并发数）。注意：这两个响应头**仅在请求被拒绝时**返回，且此时 `X-Concurrency-Remaining` 恒为 0（与 cluster-key-rate-limit 在成功时也返回头部不同） |
| lease_ttl_seconds | int | 否 | 600 | 并发计数器租约时长（秒），必须为正整数。仅在网关进程异常退出导致释放逻辑未执行时兜底清零泄漏计数；**必须大于目标场景的最长请求时长**（LLM 流式场景建议大于最大生成时长） |
| redis | object | 是 | - | redis 相关配置 |

`global_threshold` 中每一项的配置字段说明。

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
| ----------- | ---- | ---- | ------ | ---- |
| concurrency | int | 是 | - | 允许的最大并发请求数，必须为正整数 |

`rule_items` 中每一项的配置字段说明。

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
| --------------------- | ------ | ---- | ------ | ---- |
| limit_by_header | string | 否，`limit_by_*` 中选填一项 | - | 配置获取限流键值的来源 HTTP 请求头名称 |
| limit_by_param | string | 否，`limit_by_*` 中选填一项 | - | 配置获取限流键值的来源 URL 参数名称 |
| limit_by_consumer | string | 否，`limit_by_*` 中选填一项 | - | 根据 consumer 名称进行限流，无需添加实际值。依赖认证插件注入的 `x-mse-consumer` 请求头，本插件需配置在认证插件之后 |
| limit_by_cookie | string | 否，`limit_by_*` 中选填一项 | - | 配置获取限流键值的来源 Cookie 中 key 名称 |
| limit_by_per_header | string | 否，`limit_by_*` 中选填一项 | - | 按请求头值分别计算并发；`limit_keys` **不可填字面名，仅接受 `*` 或 `regexp:...`**。精确匹配请改用 `limit_by_header`（去掉 `per_`） |
| limit_by_per_param | string | 否，`limit_by_*` 中选填一项 | - | 按 URL 参数值分别计算并发；`limit_keys` **不可填字面名，仅接受 `*` 或 `regexp:...`**。精确匹配请改用 `limit_by_param`（去掉 `per_`） |
| limit_by_per_consumer | string | 否，`limit_by_*` 中选填一项 | - | 按 consumer 分别计算并发；`limit_keys` **不可填字面名，仅接受 `*` 或 `regexp:...`**。精确匹配请改用 `limit_by_consumer`（去掉 `per_`） |
| limit_by_per_cookie | string | 否，`limit_by_*` 中选填一项 | - | 按 Cookie 值分别计算并发；`limit_keys` **不可填字面名，仅接受 `*` 或 `regexp:...`**。精确匹配请改用 `limit_by_cookie`（去掉 `per_`） |
| limit_by_per_ip | string | 否，`limit_by_*` 中选填一项 | - | 选择客户端 IP 来源：`from-header-<header名>` 或 `from-remote-addr`；IP/CIDR 必须写在 `limit_keys[].key` 中 |
| limit_keys | array of object | 是 | - | 配置匹配键值后的并发上限 |

`limit_keys` 中每一项的配置字段说明。

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
| ---- | ------ | ---- | ------ | ---- |
| key | string | 是 | - | 非 `per_` 类型接受**精确字面值**或 `regexp:` 前缀的正则表达式（按实际值匹配）；注意**裸 `*` 在非 per 类型下不生效**（会被当作字面值 `"*"` 且永远匹配不到），兜底请使用 `regexp:.*`；`limit_by_per_header`、`limit_by_per_param`、`limit_by_per_consumer`、`limit_by_per_cookie` **仅接受 `*` 或 `regexp:...`**；`limit_by_per_ip` 接受 IP 地址或 CIDR |
| concurrency | int | 是 | - | 允许的最大并发请求数，必须为正整数 |

`redis` 中每一项的配置字段说明。

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
| ------------ | ------ | ---- | ------ | ---- |
| service_name | string | 是 | - | redis 服务名称，带服务类型的完整 FQDN 名称，例如 my-redis.dns、redis.my-ns.svc.cluster.local |
| service_port | int | 否 | 服务类型为固定地址（static service）默认值为 80，其他为 6379 | 输入 redis 服务的服务端口 |
| username | string | 否 | - | redis 用户名 |
| password | string | 否 | - | redis 密码 |
| timeout | int | 否 | 1000 | redis 连接超时时间，单位毫秒 |
| database | int | 否 | 0 | 使用的数据库 id，例如配置为 1，对应 `SELECT 1` |

## 配置示例

### consumer 级并发限制

```yaml
rule_name: llm-consumer-concurrency-rule
rule_items:
  - limit_by_consumer: ''
    limit_keys:
      - key: consumer1
        concurrency: 5
      - key: consumer2
        concurrency: 20
  - limit_by_per_consumer: ''
    limit_keys:
      # 正则兜底：其余所有 consumer 每个最多 3 并发
      - key: "*"
        concurrency: 3
rejected_code: 429
rejected_msg: '{"code":-1,"msg":"concurrency limit exceeded"}'
lease_ttl_seconds: 1800  # LLM 流式场景，建议大于最大生成时长
redis:
  service_name: redis.static
show_limit_quota_header: true
```

### 识别请求参数 apikey，进行区别并发限制

```yaml
rule_name: llm-apikey-concurrency-rule
rule_items:
  - limit_by_param: apikey
    limit_keys:
      - key: 9a342114-ba8a-11ec-b1bf-00163e1250b5
        concurrency: 10
      - key: a6a6d7f2-ba8a-11ec-bec2-00163e1250b5
        concurrency: 50
  - limit_by_per_param: apikey
    limit_keys:
      # 正则表达式，匹配以 a 开头的所有 apikey，每个最多 2 并发
      - key: "regexp:^a.*"
        concurrency: 2
      # 正则兜底：其余所有 apikey 每个最多 1 并发
      # 注意：此处是 per_ 类型，允许用 *；非 per 类型的兜底请用 regexp:.*
      - key: "*"
        concurrency: 1
redis:
  service_name: redis.static
show_limit_quota_header: true
```

### global 全局兜底 + 规则细分的混合配置

```yaml
rule_name: llm-mixed-concurrency-rule
# 全局兜底：整个规则组（该路由）总并发不超过 100
global_threshold:
  concurrency: 100
rule_items:
  # 按 consumer 细分
  - limit_by_consumer: ''
    limit_keys:
      - key: consumer1
        concurrency: 10
      - key: consumer2
        concurrency: 10
  # 按请求头 x-api-key 中的精确 key 细分
  - limit_by_header: x-api-key
    limit_keys:
      - key: 102234
        concurrency: 5
      # 正则兜底：其他所有 key 值每个最多 2 并发
      - key: "regexp:.*"
        concurrency: 2
lease_ttl_seconds: 1800
redis:
  service_name: redis.static
show_limit_quota_header: true
```

所有命中的规则会同时评估：consumer 达到上限、或 x-api-key 达到上限、或全组总并发达到 100，任一触发即拒绝。

## 常见错误

### 在非 per 类型下使用裸 `*` 兜底

```yaml
# ❌ 错误：limit_by_header 是非 per 类型，key "*" 被当作字面值 "*"，
#    只有当请求头 x-api-key 的值恰好是 "*" 时才会命中，永远不会兜底
rule_items:
  - limit_by_header: x-api-key
    limit_keys:
      - key: "*"
        concurrency: 2

# ✅ 正确：非 per 类型的兜底使用 regexp:.*
rule_items:
  - limit_by_header: x-api-key
    limit_keys:
      - key: "regexp:.*"
        concurrency: 2
```

### 在 per 类型下填写字面名称

```yaml
# ❌ 错误：per_consumer 的 limit_keys 仅接受 * 或 regexp:...，
#    配置解析会直接失败并提示应改用的非 per 类型
rule_items:
  - limit_by_per_consumer: ''
    limit_keys:
      - key: "alice"
        concurrency: 5

# ✅ 正确：精确匹配名称时去掉 per_
rule_items:
  - limit_by_consumer: ''
    limit_keys:
      - key: "alice"
        concurrency: 5
```

### 把 CIDR 填到 `limit_by_per_ip`

```yaml
# ❌ 错误：limit_by_per_ip 选择 IP 来源，不接受 CIDR
rule_items:
  - limit_by_per_ip: "0.0.0.0/0"
    limit_keys:
      - key: "regexp:.*"
        concurrency: 100

# ✅ 正确：CIDR 写入 limit_keys
rule_items:
  - limit_by_per_ip: "from-remote-addr"
    limit_keys:
      - key: "0.0.0.0/0"
        concurrency: 1000
```

### 遗漏 `limit_keys` 或 `redis`

```yaml
# ❌ 错误 1：rule_item 缺少 limit_keys，整个配置会被拒绝
rule_items:
  - limit_by_per_ip: "from-remote-addr"

# ❌ 错误 2：缺少 redis 配置，本插件为 Redis 强依赖，配置会被拒绝
rule_name: demo
global_threshold:
  concurrency: 10

# ✅ 正确：每条 rule_item 都提供 limit_keys，且配置 redis
rule_name: demo
global_threshold:
  concurrency: 10
redis:
  service_name: redis.static
```

### `lease_ttl_seconds` 小于最长请求时长

若 `lease_ttl_seconds` 小于 LLM 最长生成时长，长时间流式请求的占位会在流未结束时被 Redis 回收，导致实际并发数超过配置上限。应将 `lease_ttl_seconds` 设置为**大于目标场景最长请求时长**（默认 600 秒）。

## 诊断

### 查看 Redis 中的实时并发 Gauge

```bash
# 查看所有并发计数器（含当前正在处理中的请求数）
redis-cli KEYS "higress-concurrency-limit:*"
# 查看某条规则的全局并发
redis-cli GET "higress-concurrency-limit:{llm-mixed-concurrency-rule}:global_threshold"
# 查看某条规则项（按请求头 x-api-key 维度）的实时并发，sk-aaa111 为该 header 的实际取值
redis-cli GET "higress-concurrency-limit:{my-rule}:limit_by_header:x-api-key:sk-aaa111"
```

> 注意：key 中 `{rule_name}` 是 Redis Cluster 的 hash tag，集群模式下请使用 `redis-cli -c`。生产环境请用 `SCAN` 迭代替代 `KEYS`（`KEYS` 会阻塞 Redis 主线程，仅限开发环境使用）。

### 查看插件指标

在 Gateway `/stats` 中过滤 `concurrency_limit`：

| 指标 | 含义 |
| --- | --- |
| `concurrency_limit_rejected` | 因并发超限被拒绝的请求数（按 route/cluster/consumer 维度） |
| `concurrency_limit_redis_error` | Redis 调用失败次数；持续增长说明 Redis 连接异常 |

### Fail-open 行为

Redis 不可用时，插件**放行请求**（fail-open）并递增 `concurrency_limit_redis_error`，不会导致网关拒绝所有流量，但此时并发限制失效。若业务对并发上限敏感（如成本保护），应监控该指标并保障 Redis 可用性。

### 网关日志排查

```bash
kubectl -n higress-system logs <gateway-pod> --tail=2000 \
  | grep -Ei 'cluster-concurrency-limit|missing limit_keys|must start with|regexp'
```

`config_dump` 中能看到配置只说明控制面已下发，不能证明数据面插件已成功加载；需要结合 `/stats` 和网关日志判断。

## 流式说明

### SSE 流式请求的占位与释放

对 LLM 流式（SSE）请求，插件在**请求头阶段**即占位（INCR），并在**响应流结束或断开**（客户端主动断开、上游结束、超时）时通过 `onHttpStreamDone` 释放（DECR）。流式请求会在整个生成期间占用并发槽位——这正是 LLM 并发保护所期望的语义。

### 进程崩溃的恢复路径（lease_ttl_seconds）

释放依赖网关进程正常执行 `onHttpStreamDone`。若网关进程异常退出（`kill -9`、OOM、节点故障），已占用的计数无法被释放，会形成"泄漏"。`lease_ttl_seconds` 租约机制兜底：每个计数器首次 INCR 时写入 TTL，泄漏计数在 TTL 到期后由 Redis 自动清零恢复。

配置建议：

- `lease_ttl_seconds` 必须**大于目标场景的最长请求时长**；LLM 流式场景建议大于最大生成时长（例如生成最长 20 分钟则设为 1500 以上）。
- TTL 越小，崩溃后恢复越快，但长请求被提前回收的风险越大；TTL 越大越安全，但崩溃后泄漏槽位恢复越慢。
