# ADR-0001：Temporal + PostgreSQL + CAS 控制面

- 状态：Accepted
- 日期：2026-07-30
- 关联：FLO-108、FLO-124、FR-P0-08～24

## 背景

远程视频任务可持续数分钟，包含付费提交、人工 Gate、callback/polling、未知结果、进程重启、取消竞态、产物下载和成本结算。单 HTTP 请求、内存队列或供应商 task list 都不能表达产品 revision、预算、审计和恢复。

## 决策

1. PostgreSQL 是可查询产品真相和 outbox；
2. Temporal 是持久执行 history、Activity retry、人工 signal 和 cancellation；
3. CAS 保存不可变大文件；
4. ProviderJob 保存本地 job 与 upstream request/task ID；
5. 外部调用采用 intent → call → reconcile，不在 DB transaction 内调用；
6. Activity retry 复用同一 ProviderJob/幂等键；`UNKNOWN` 继续 poll，不盲目创建；
7. Workflow input 只含 ID/hash/route/budget snapshot，不含 Secret 或完整媒体。

## 后果

正向：

- 进程重启后可以继续原上游任务，避免重复计费；
- 人工 G2/G3 可以等待数小时而不占 goroutine；
- Run/Attempt/ProviderJob/Artifact/Cost 分层可审计；
- adapter 替换不改变 Workflow 数据结构。

成本：

- 需要 PostgreSQL/Temporal 的运维与 schema/workflow versioning；
- Activity 必须严格幂等；
- callback、polling 和 outbox 均为 at-least-once，要去重；
- 产品 projection 与 Temporal history 需要 reconciliation。

## 拒绝方案

| 方案 | 原因 |
|---|---|
| 上游 Provider task list 作真相 | 无版本/Gate/预算/多 provider/权限语义 |
| 进程内 goroutine/cron | 崩溃后 history、timer、人工等待丢失 |
| 仅 PostgreSQL job polling | 可实现但人工 signal、复杂补偿和版本演进成本高 |
| 本地 GPU/ComfyUI queue | FLO-124 明确无 GPU，且仍缺产品控制面语义 |
