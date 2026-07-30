# API、事件、错误与版本约定

机器契约：

- Public REST：`contracts/openapi.yaml`
- Domain events：`contracts/asyncapi.yaml`
- Internal adapter：`internal/providercontract`
- Mock HTTP fixture：`internal/videopipeline/mockprovider`

## 1. API namespace

独立仓库的产品接口统一位于 `/api/v1/*`：

| 分组 | 代表接口 | 用途 |
|---|---|---|
| System | `GET /system/info`, `/providers/status` | 无 Key/Dry-run/版本状态 |
| Providers | capabilities、connection-test、provider-jobs、callbacks | 路由与远程任务 |
| Plans | `POST /generation-plans` | 不付费的任务图/估算/预算门 |
| Series | series/source/impacts | 项目、小说 revision、stale |
| Production | production-batch、shot run、cancel/resume | Temporal 长任务 |
| Approvals | `POST /approvals` | G1/G2/Q1/G3 精确绑定 |
| Lineage | manifests | Provider/输入/输出/成本/许可谱系 |

所有 mutation：

- `Idempotency-Key` 必填；
- 修改已有 aggregate 时 `If-Match` 必填；
- 长任务返回 `202 + Location`；
- 错误为 `application/problem+json`；
- UI 轮询本地 projection，绝不等待 provider。

## 2. Generation plan → submit

### Dry-run plan

`POST /api/v1/generation-plans` 输入镜头 revision、候选数、route snapshot、预算上限。输出：

```text
shot count
provider call count
actual model/endpoint snapshot
estimated unit range
estimated amount range OR explicitly unknown
pricing rule version + expiry
budget decision
immutable plan hash
```

Dry-run 不创建上游任务。金额未知不是 0；默认要求人工确认。

### Provider submit

`POST /api/v1/provider-jobs` 必须引用：

```text
confirmed generation_plan_id
generation_attempt_id
capability alias
input_hash
route/model/capability snapshot
budget reservation
secret-free request snapshot
```

内部 adapter 使用稳定 JobID 作为上游幂等键。返回 task ID 前本地已保存 submit intent；超时进入 `UNKNOWN`。

## 3. Provider callback

Callback URL 不含凭证。必需：

- provider profile path；
- callback ID；
- timestamp/nonce（adapter-specific）；
- signature；
- provider task/request ID；
- monotonic sequence 或可比较 provider update time；
- payload hash。

响应语义：

| 情况 | HTTP | 效果 |
|---|---:|---|
| 首次有效 callback | 202 | applied |
| 完全重复 | 202 | applied=false, duplicate |
| 旧 sequence/终态后回退 | 202 | applied=false, stale |
| 验签失败 | 401 | 不写业务状态，写 security audit |
| 未知 task | 202/404（adapter policy） | 隔离，不自动绑定 |

Polling 与 callback 可并存；任一端先确认终态后，另一端只能补充相同终态的用量/费用，不能回退。

## 4. 错误映射

| errorCode | HTTP | retryable | requires action |
|---|---:|---:|---:|
| `unauthenticated` | 401 | 否 | 配置/更新 Secret |
| `forbidden` | 403 | 否 | 模型/Endpoint/区域权限 |
| `rate_limited` | 429 | 是 | 等 `Retry-After` |
| `quota_exceeded` | 402/422 | 否 | 配额/余额/资源包 |
| `budget_exceeded` | 422 | 否 | 缩减计划或重新批准 |
| `content_blocked` | 422 | 否 | 人工修改内容 |
| `region_unavailable` | 422 | 否 | 新 attempt 选择区域 |
| `model_unavailable` | 422 | 否 | 更新 route snapshot |
| `invalid_request` | 400/422 | 否 | 修正参数/拆镜 |
| `provider_unavailable` | 503 | 是 | 原 JobID 最多 3 次 |
| `timeout` | 202 projection | 否 | reconciliation，不重新创建 |

Public problem response 只包含稳定类别、可操作建议和 trace ID，不回显 provider 原始 body、请求 header 或 signed URL。

## 5. 事件

Outbox 是发布来源，at-least-once delivery，consumer 按 `eventId` 去重：

| 事件 | 生产事务 | 主要消费者 |
|---|---|---|
| `video.revision.created.v1` | revision + dependency | UI projection、stale resolver |
| `video.series.created.v1` | series + operation | UI projection |
| `video.generation-plan.created.v1` | plan + budget decision | UI、budget |
| `video.production.requested.v1` | operation + G2 bindings | Temporal、UI |
| `video.workflow-step.completed.v1` | Activity journal result | recovery、ops |
| `video.approval.decided.v1` | Gate decision/bindings | Temporal、policy |
| `video.run.state-changed.v1` | Run transition | UI、metrics |
| `video.provider-job.state-changed.v1` | ProviderJob transition | poll scheduler、UI、ops |
| `video.cost-ledger.recorded.v1` | ledger append | budget/cost views |
| `video.artifact.committed.v1` | CAS binding | QC/Manifest |
| `video.qc.completed.v1` | QC report | Q1 review |
| `video.dependency.stale.v1` | impact transaction | UI/production blocker |
| `video.manifest.locked.v1` | G3 lock | export/archive |

事件 payload 只放 ID、hash、state、route/model snapshot、units/cost 和 reason code。小说全文、完整 Prompt、Secret、signed URL 不进入事件。

## 6. 版本升级

- Public API：major 放 URL (`/v1`)；可选字段向后兼容增加；
- Events：事件名带 `.v1`；破坏性变更新事件名，旧消费者并行迁移；
- Internal provider envelope：`schemaVersion=v1`；adapter 必须拒绝未知 major；
- Model route：`route_version + capability_hash`；模型替换不等于 API 版本升级；
- Workflow：固定注册名 `video.production.episode.v1`，破坏性行为用新 workflow 名/version marker；
- Database：expand → dual-read/write → backfill → contract，不原地重解释历史 JSON；
- Config：route/pricing/capability 每次生效都生成不可变 snapshot。

## 7. 回滚

1. 应用回滚不回滚已提交 ProviderJob；
2. 旧 worker 能读取旧 envelope/event，遇到未知版本不消费；
3. route 配置回滚只影响新 attempt，历史 snapshot 不变；
4. DB 只在确认旧应用兼容扩展列后回滚；
5. Provider 任务继续由 reconciliation 读取 task ID；
6. Artifact/Manifest 均内容寻址，可回退业务引用而不覆盖文件。

本增量的 schema 回滚顺序是先停写入，再执行 migration v2 down；down 只移除不可变 trigger、Manifest Secret/lock check 和 Artifact retention 元数据，不删除任何 revision、Audit、Manifest 或 CAS 字节。仍需不可变保证时不得单独回滚 v2 后继续写流量。

## 8. QA 契约边界

Mock 可确定性验收 API/编排：

- success、slow success、timeout/unknown；
- 401/403、429、5xx、quota、content、region/model unavailable；
- Idempotency conflict；
- duplicate/out-of-order callback；
- cancellation race；
- process restart/recovery；
- CAS checksum 与临时 URL 不入历史。

真实火山 Key 才能验收：

- 账号实际模型/区域/Endpoint；
- callback 签名、polling SLA、取消语义；
- ratio/duration/resolution/reference/tail-frame；
- QPM/TPM、配额、价格/计费；
- 输出 URL/Base64、MIME/媒体元数据；
- 真实错误 payload 到稳定 errorCode 的映射。
