# ADR-0005：无 Key Dry-run、Mock 与显式 Secret 注入

- 状态：Accepted
- 日期：2026-07-30
- 关联：FLO-124 AC-01/02、安全验收

## 决策

1. 缺少任意模型 Key 不阻止应用、项目、Plan、Prompt draft、预算规则和 Mock 启动；
2. Live capability 分别显示 `NOT_CONFIGURED`，付费入口变为生成调用计划；
3. Mock 固定响应覆盖 success、timeout、401/403、429/5xx、quota、content、region/model、callback、cancel、recovery；
4. Secret 只从显式 `VIDEO_*` 环境变量或 Secret Store reference 注入；
5. 禁止扫描/复制 Claude Code 私密配置；
6. 数据库只保存 provider profile、credential reference/fingerprint，不保存值；
7. frontend/log/trace/error/fixture/Manifest/outbox 结构化脱敏。

## 后果

- M0 不等待 Key；
- QA 不产生真实费用；
- Mock 只能证明契约和恢复，不能证明真实模型质量/权限/价格；
- Key 到位后必须执行 operations 中的最小实测，才能进入 M1。
