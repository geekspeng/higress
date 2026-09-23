# AI 编码提示记录

本文档记录使用 AI 工具（Claude Code）开发 `cluster-concurrency-limit` 插件过程中给出的主要指令与产出。开发跨越多个会话，按任务阶段归纳。

## 1. 蓝本分析与脚手架

> 参考 ai-token-ratelimit / cluster-key-rate-limit 插件，开发一个基于 Redis 的集群级并发数限制插件（Gauge 语义）：请求进入时占位、流结束时释放，与窗口限流的区别参考两个蓝本的结构。先搭脚手架（go.mod、util 包）。

产出：模块脚手架与 `util/` 包（cookie 提取、IP 解析、header 转换、route/cluster/consumer 取值），从蓝本复制通用逻辑并裁剪 window 相关代码。

## 2. 配置解析

> 实现配置解析：rule_name、global_threshold、rule_items（九种 limit_by_*）、limit_keys（concurrency 字段代替 window）、rejected_code/msg、show_limit_quota_header、redis 配置。校验规则对齐 cluster-key-rate-limit：global 与 rule_items 至少一项、rule_items 最多 10 条、并发数必须为正整数。

产出：`config/config.go`。设计决策：key 匹配按 ConfigType（Exact/Regexp/All/IpNet）统一分发，非 per 类型同样支持 `regexp:` 前缀；per 类型仅接受 `*`/`regexp:`，字面名在解析期报错并提示对应的非 per 类型；`lease_ttl_seconds` 必须为正整数。

## 3. 核心限流逻辑

> 实现占位与释放：请求头阶段对所有命中计数器单次原子 Lua 检查+占用，任一超限全部回滚（all-or-nothing）；流结束（onHttpStreamDone）释放本请求占用的全部计数器，释放要防负数；Redis 故障 fail-open；拒绝时返回 429 与 X-Concurrency-* 头并记录指标。

产出：`main.go` 的 `CheckScript`/`ReleaseScript`、`collectMatchedRules`（global 优先、按数组顺序、按 key 去重）、`LimitContext` 上下文传递、fail-open 分支、`rejected()`（SendHttpResponseWithDetail + 用户属性日志 + 维度化指标）。

## 4. 评审补充测试

> 评审指出多 key 合并 EVAL 与 stream-done 释放路径缺少测试覆盖，补上。

产出：`TestOnHttpRequestHeaders_MultiKeyRollback`（global+consumer 双规则合并单次 EVAL、第 2 个 key 超限返回 429）、`TestOnHttpStreamDone_ReleasesViaRedisEval`（CompleteHttp 后断言新发起的 EVAL 含 DECR）。发现并记录 host 模拟器特性：已应答的 callout 会从 `GetRedisCalloutAttributes` 移除，释放 callout 须作为 CompleteHttp 后的新条目断言。

## 5. 文档与注册信息

> 按 higress 插件文档规范写 README（中文），说明 Gauge 与窗口限流的语义差异、配置参考、常见配置错误（非 per 类型裸 `*`、per 类型字面名、CIDR 位置、lease_ttl_seconds 过小）、Redis key 诊断方法；补 VERSION。

产出：`README.md`、`README_EN.md`、`VERSION`（1.0.0）。

## 6. 修正轮

> 修正：占位成功时应刷新所有命中 key 的租约（最后一次成功 INCR 即最后一次续租）；拒绝时写入用户属性日志；lease_ttl_seconds 校验与文档表述对齐。

产出：`CheckScript` 第二个 EXPIRE 循环（成功路径全 key 续租）、`ctx.SetUserAttribute("concurrency_limit_status", "limited")` + `WriteUserAttributeToLogWithKey(AILogKey)`、配置校验与 README 一致性修正。

## 7. E2E 验证（129 集群）

> 在内部 129 测试集群上对本插件做 E2E 验证：构建 wasm、经 plugin-server 分发、独立 WasmPlugin + 独立 Ingress 隔离测试，验证占位/释放、全局与维度限流、多 key 回滚、流式占位与断开释放、fail-open、指标与非法配置行为。

产出：10 项 E2E 全部通过（记录见 `design-doc.md` §7）；测试基建 `ccl-delay-server`（`/delay/N`、`/stream/N`）留在集群；实证 pilot 的 phase 装配顺序 AUTHN → AUTHZ → STATS → UNSPECIFIED_PHASE，本插件 phase 留空即可保证在认证插件之后执行。

## 8. 设计文档补录

> 按 CLAUDE.md 插件开发规范补 `design/` 目录（本文档与 design-doc.md），记录设计决策与验证结论后提交。

产出：`design/design-doc.md`、本文件。
