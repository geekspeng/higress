# 集群并发数限制插件设计文档

## 1. 概述

`cluster-concurrency-limit` 基于 Redis 实现跨网关实例共享的**集群级并发数限制**（Gauge 语义），典型场景是 LLM API 的流式（SSE）请求并发保护：一条 SSE 请求可能持续数分钟，需要表达"此刻同时只有 N 条生成中请求"，这是窗口限流无法描述的。

插件从 `ai-token-ratelimit` / `cluster-key-rate-limit` 蓝本移植而来（配置解析、`limit_by_*` 维度提取逻辑与蓝本一致），核心差异在于计数语义与生命周期管理：

| | cluster-key-rate-limit / ai-token-ratelimit | 本插件 |
|---|---|---|
| 计数语义 | Counter（窗口内请求**次数**） | Gauge（**此刻**处理中的请求数） |
| 计数时机 | 请求进入时 +1，窗口结束清零 | 请求头阶段 +1（占位），`onHttpStreamDone` -1（释放） |
| "窗口" | 固定/滑动时间窗口 | 请求自身的生命周期 |
| 结束回调 | 不需要（窗口自动清零） | 必须（否则计数泄漏） |
| 溢出行为 | 窗口内累计超限即拒 | 超过并发上限的请求立即拒（默认 429） |

插件不区分流式/非流式：非流式请求占用槽位的时间即其处理时长，流式请求在整个流持续期间占用槽位——后者正是 LLM 并发保护期望的语义。

## 2. 核心机制

### 2.1 原子占位与 all-or-nothing 回滚（CheckScript）

一个请求可能同时命中多条规则（`global_threshold` + 多条 `rule_items`，all-match 语义，任一超限即拒）。所有命中计数器在**单次 Lua EVAL** 中完成检查+占用，保证跨规则的原子性（多 key 分散调用会产生部分占位的中间态）：

```lua
for i = 1, #KEYS do
    local current = redis.call('incr', KEYS[i])
    if current == 1 then redis.call('expire', KEYS[i], ttl) end
    if current > threshold then
        for j = 1, i do redis.call('decr', KEYS[j]) end  -- 回滚本次全部 INCR
        return {0, current, i}      -- 拒绝：{allowed=0, current(INCR 后、回滚前的值，仅用于响应展示), 超限下标}
    end
end
for i = 1, #KEYS do redis.call('expire', KEYS[i], ttl) end -- 成功：全 key 续租
return {1, 0, 0}
```

设计要点：

- **回滚范围是 `1..i`（含已通过的前缀 key）**：第 i 个 key 超限时，前面 key 已经 INCR，必须一并撤回，否则每次拒绝都会给全局计数器留下 +1 的幽灵占位，拒绝流量本身会把全局计数器推满（自我 DoS）。
- **回滚的拒绝不触发续租**：只有全部通过才进入第二个 EXPIRE 循环。
- **拒绝请求不写释放上下文**：`LimitContext` 仅在全部通过后写入，被拒请求的 stream done 不会对已回滚的 key 发起释放——否则会把其他请求合法持有的槽位误减。
- **每次成功占位即续租**（"最后一次成功 INCR 即最后一次续租"）：某 key 持续有**成功放行**的请求命中时，其计数器不会因 TTL 过期被误删；纯被拒流量不续租，被拒绝的流量无法维持泄漏计数器的租约。
- Lua 脚本在 Redis 单线程内原子执行，跨网关实例（多副本）并发 INCR 无竞态——E2E 在双副本网关上验证无超卖。

### 2.2 释放与防负数（ReleaseScript）

`onHttpStreamDone`（响应流正常结束、客户端断开、上游结束/超时均会触发）对请求占用的全部 key 执行：

```lua
local current = tonumber(redis.call('get', KEYS[i]) or "0")
if current > 0 then redis.call('decr', KEYS[i]) end
```

`> 0` 守卫防止重复释放把计数打成负数（如 lease TTL 已回收计数后又收到迟到的释放）。脚本原子执行，GET+DECR 无竞态。释放是尽力而为：失败只记日志，由租约 TTL 兜底。

### 2.3 租约 TTL（lease_ttl_seconds）与崩溃恢复

释放依赖网关进程存活并执行 `onHttpStreamDone`。网关进程异常退出（kill -9、OOM、节点故障）时已占用的计数无法释放，形成泄漏。租约机制兜底：

- 每个 key **首次 INCR**（current==1）时写入 TTL（续租规则见 §2.1：每次成功占位为全部命中 key 续租；单个请求自身在飞期间没有专属续期）；
- 泄漏计数在 TTL 到期后由 Redis 自动清零，服务自愈，无需人工干预；
- TTL 必须**大于目标场景的最长请求时长**（LLM 流式建议大于最大生成时长），否则长请求的占位会在流未结束时被提前回收，导致实际并发超过上限——这是配置约束而非实现缺陷（默认 600s）。

### 2.4 上下文传递

占位成功后，已占用的 key 列表存入 `HttpContext`（`LimitContext`），供 `onHttpStreamDone` 取出释放。未命中任何规则的请求不发起 Redis 调用、不存上下文，零开销直通。

## 3. 执行阶段与优先级

README 声明"执行阶段：认证阶段之后，优先级 610"。实现上 **phase 留空（UNSPECIFIED_PHASE）**，依赖 pilot 的 filter 链装配顺序保证执行先后（`istio/pilot/pkg/networking/core/listener.go`）：

```
AUTHN → AUTHZ → STATS → UNSPECIFIED_PHASE
```

key-auth / jwt-auth / basic-auth 属于 AUTHN phase，必然先于本插件执行，`limit_by_consumer` / `limit_by_per_consumer` 才能读到认证插件注入的 `x-mse-consumer` 请求头。若显式配置 AUTHN phase 且 priority 小于认证插件，consumer 维度将失效——phase 留空是更稳的默认。

`priority: 610` 只影响同 phase 内的相对顺序（如与同为 UNSPECIFIED 的其他限流插件）。

## 4. Redis key 设计

```
higress-concurrency-limit:{rule_name}:global_threshold
higress-concurrency-limit:{rule_name}:{limit_type}:{limit key 名}:{实际值}
```

`{rule_name}` 是 Redis Cluster 的 **hash tag**：同一规则的所有计数器（global + 各维度）聚合到同一 slot，多 key EVAL 无需担心 CROSSSLOT 错误；不同规则天然分散到不同 slot，规则间不互相争抢。

多维度组合命中时，去重后的 key 列表按「global → rule_items 数组顺序」拼接，单次 EVAL 携带 `[threshold1, ttl1, threshold2, ttl2, ...]` 参数。

## 5. 多维限流与 key 匹配

`limit_by_*` 九种类型与蓝本一致（header / param / consumer / cookie / per_header / per_param / per_consumer / per_cookie / per_ip）。实现上刻意让 **key 匹配与规则类型解耦**（`findMatchingItem` 按 `ConfigType` 统一分发：Exact / Regexp / All / IpNet），由此产生两条配置约定：

- **非 per 类型同样支持 `regexp:` 前缀**（蓝本只给 per 类型正则能力），因此非 per 类型的兜底写 `regexp:.*` 而非 `*`——裸 `*` 在非 per 类型下是字面值，永远匹配不到，这是最常见的配置错误；
- **per 类型仅接受 `*` 或 `regexp:...`**，字面名直接在解析期报错并提示改用的非 per 类型（如 `limit_by_per_consumer` 配 "alice" → 提示改用 `limit_by_consumer`），把配置错误从运行期静默不命中提前到加载期显式失败。

`limit_by_per_ip` 的 `from-header-<name>` / `from-remote-addr` 决定 IP 来源，IP/CIDR 写在 `limit_keys[].key`（iptree 匹配）。

## 6. 失败模式与容错

| 故障 | 行为 | 设计取向 |
|---|---|---|
| Redis 不可用/超时/应答异常 | **fail-open** 放行，递增 `concurrency_limit_redis_error` | 网关可用性优先于限流精度；成本敏感场景需监控该指标 |
| Redis 应答缺字段/下标非法 | 记错误日志后放行 | 同上 |
| 释放调用失败 | 记日志，TTL 兜底回收 | 释放不可丢失流量，只可能短暂多占 |
| 配置非法（阈值非正、per 类型字面名、缺 limit_keys/redis 等） | 解析期拒绝 → envoy "Unable to create Wasm plugin" → 该路由请求**绕过插件**直通 | 共享网关上坏配置只影响本路由，其他路由不受牵连（E2E 实证） |

指标名带 route/cluster/consumer 维度：`wasmcustom.route.<route>.upstream.<cluster>.consumer.<consumer>.metric.<name>`，`name ∈ {concurrency_limit_rejected, concurrency_limit_redis_error}`。拒绝响应额外写入 `concurrency_limit_status=limited` 用户属性日志（AI 日志键）。

## 7. E2E 验证记录（2026-11-09，129 集群）

单节点集群、Higress 部署于 `aiap` 命名空间、gateway 双副本（天然验证跨实例共享计数）、redis-stack-server、测试后端 `/delay/N`（可控时长）与 `/stream/N`（chunked 流）：

| # | 场景 | 预期 | 实测 |
|---|---|---|---|
| T1 | 单请求完成后 gauge 回收 + 租约 TTL | 0 / TTL=60 | ✅ |
| T2 | 6 并行、全局上限 3 | 3×429 + 3×200，持有期 gauge=3，结束回 0 | ✅ |
| T3 | exact 桶(2) 与 regexp 桶(1) 并行 | 各自独立拒/放，桶间零干扰 | ✅ |
| T4 | 拒绝响应头 | `X-Concurrency-Limit: <阈值>`、`X-Concurrency-Remaining: 0` | ✅ |
| T5 | 默认拒绝码/体 | 429 + `concurrency limit exceeded` | ✅ |
| T6 | 流式占位与客户端断开释放 | 流期间 gauge=1，`--max-time` 断开后归 0 | ✅ |
| T7 | 多 key all-or-nothing 回滚 | per-key 超限拒绝时 global 占位同步回滚（1→1，无幽灵计数） | ✅ |
| T8 | 指标 | `concurrency_limit_rejected` 与全程拒绝数精确一致（9） | ✅ |
| T9 | Redis 认证失败 fail-open | 请求全放行，`redis_error` 双 pod 共 +3 | ✅ |
| T10 | 非法配置 | envoy 拒绝创建插件、路由 fail-open、恢复后重新生效 | ✅ |

单元测试（proxy-wasm host 模拟）覆盖：占位/拒绝/无规则直通零 Redis 调用、多 key 单 EVAL 合并、拒绝 429、stream done 触发 DECR EVAL、Redis 故障 fail-open，见 `main_test.go`。

## 8. 已知限制

- TTL 不感知流式长请求：占位租约不会随流持续自动续期（仅成功占位时续租），`lease_ttl_seconds` 必须大于最长请求时长，否则长请求占位被提前回收、并发上限失真。
- gauge 精度依赖 Redis 可用性：fail-open 期间并发不受控（设计取向，见 §6）。
- 计数器以规则+维度+值拼 key，维度取值无界（如按 per_ip 限公网流量）时 key 数量与请求源数量同阶，需留意 Redis 内存与 `KEYS` 排查方式（生产用 SCAN）。
- **Redis 驱逐策略是正确性前提**：gauge 依赖 key 不被意外删除，Redis 若配置 `allkeys-lru` 等驱逐策略，计数被驱逐即静默超卖——承载本插件的 Redis 应禁用对计数 key 的驱逐（容量规划 + `noeviction`/volatile 类策略）。
