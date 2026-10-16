# AI 编码提示记录（ai-content-pusher）

本插件设计由 AI（Claude Code）协助完成，按仓库 agent-assisted contribution 政策记录关键提示过程。

## 会话脉络（2026-10-15 ~ 2026-10-16）

设计从一次 ai-statistics 插件的配置咨询演进而来，关键决策点及对应提问：

1. **起点**：询问 ai-statistics 的 `as_separate_log_field` 配置语义 → 引出"值挂在哪个 filter-state 键"的机制分析
2. **问题发现**：追问"输入输出各 1MB 写日志会不会有问题" → 逐层验证出运行时 16KB 拆行、logtail 512KB 分段、SLS 索引前缀 2KB/16KB、日志计费等完整问题链
3. **存储选型**：要求核对 Doris 官方日志文档后再下结论 → 修正了两个早期判断（官方单表 recipe、RANDOM 分桶），确认 Group Commit 可替代 Kafka
4. **需求收敛**：用户先后排除全文检索需求（仅 request_id 点查）、排除调用方直写（改网关侧捕获）、排除 Kafka（成本）、排除内容进日志（合规红线不允许）
5. **架构定稿**：双车道（统计走短行 access log / 内容直连 Stream Load）+ Doris 双表（180 天 stats / 30 天 content + 3 天冷却）+ 独立新插件 ai-content-pusher

## 关键提示词摘录

- "对于大模型调用日志，包含 1M 请求体和 1M 响应体，并包含其他一些统计数据，适合使用 doris 进行存储吗？是分成两个表？"
- "content 表为什么不存完整的请求和响应呢？doris 不是支持冷热分层吗？"（修正了对外置方案的过度推荐）
- "https://doris.apache.org/zh-CN/docs/4.x/observability/log 这个是官方文档，看一下再给结论呢"（触发对官方文档的全文阅读与两处结论修正）
- "可以不引入 Kafka 吗，这个增加了太多的成本"（引出 Group Commit 替代方案）
- "不允许"（合规红线：日志链路零用户内容，确定 header 业务键替代内容预览）
- "写入设计文档呢"（本文档）

## AI 辅助产出

- `design/design-doc.md` 全文（架构图、配置 schema、消息契约、验证计划）
- 代码事实核验：ai-statistics main.go 的属性写入路径、tokenusage 独立于属性列表的结论、wasm 内存预算 200MB
- 外部事实核验：Doris/SLS/logtail/Kafka 的限制值均经官方文档二次确认
