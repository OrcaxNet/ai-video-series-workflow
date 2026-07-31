# UI 状态、权限与回滚边界

本文定义 FLO-101 操作台如何读取当前 OpenAPI/AsyncAPI。PostgreSQL/control-plane
projection 是业务真相，Temporal history 与 Provider SDK 不是 UI 查询源。前端 Mock
用于演练交互，不构成可提交给真实控制面的 revision、route、预算、权利或产物绑定。

## 1. 人工闸门

产品界面呈现三道创作者闸门：

| UI | API Gate | 可审核前置 | 决策绑定 |
|---|---|---|---|
| 内容与资产 | G1 | 内容结构、资产、许可/同意快照齐全 | world/character/asset revision + content hash |
| 剧本与分镜 | G2 | G1 APPROVED，脚本、分镜、Prompt/context snapshot 无 stale | episode script/storyboard/prompt revision + content hash |
| 成片锁版 | G3 | G2 APPROVED，当前 attempts 全部成功，QC/Manifest/artifact 就绪 | episode revision/QC/Manifest/artifact + content hash |

后端的 Q1 是逐镜头质量复核，UI 在任务/QC 视图中显示，但不替代三道剧集级闸门。任何角色都不能绕过许可、stale 或预算策略。

审批按钮只在前置完成时可用；这只是即时反馈，控制面仍必须再次执行 RBAC、Gate、freshness、license、budget 和 ETag 校验。409/422 显示稳定 `errorCode`、建议动作与 `traceId`，不回显上游错误体。`REVISION_CONFLICT` 的对账 revision 只读取冻结 Error schema 的 `affectedObjects[].currentRevision`；前端不得自行发明 Gate 查询路由。

Mock control plane 同样执行 G3 真相校验，不依赖 React disabled：它自持每个 shot 的
current Job 与状态，仅在全部 current attempts 为 `SUCCEEDED`，且请求精确匹配当前
episode revision、QC report、Manifest、artifact 的 revision/hash 时接受批准。缺失、
重复或漂移 binding，以及未完成、FAILED、CANCELLED attempt 均在 API 边界拒绝。

## 2. Provider 与运行状态

| 后端状态/事实 | UI 文案 | 可执行动作 |
|---|---|---|
| QUEUED | 排队中 | 取消 |
| RUNNING | 生成中 | 查看进度、取消 |
| retryable error + scheduled retry | 重试中 | 查看 Retry-After；达到 3 次前沿用同 Job ID |
| CANCEL_REQUESTED | 取消确认中 | 等待 poll/callback 对账；不宣称已取消 |
| CANCELLED | 已取消 | 查看可能费用与 orphan 产物 |
| FAILED | 失败 | 按稳定类别修复；创意问题创建新 attempt |
| SUCCEEDED | 已完成 | 查看 CAS、QC、Manifest |
| UNKNOWN / RECONCILING | 状态未知 / 对账中 | 复用 upstream task ID；禁止盲目新 submit |
| REQUIRES_ACTION | 需要处理 | 按 auth/permission/quota/budget/content/region/model 类别处理 |

派生的 `RETRYING` 和 `CANCEL_REQUESTED` 是面向用户的本地 projection，不改写冻结 ProviderJob enum；来源分别是错误+调度信息与 GenerationRun/Operation 的取消状态。

`SUCCEEDED`、`FAILED`、`CANCELLED` 是不可回退终态。晚到/乱序 callback、重复事件、异常演练以及“完成批次”只能推进非终态 Job；遇到既有终态必须 no-op 并保留原更新时间、错误与费用证据。

终态创作重做遵循下列映射：

| 操作 | 旧 Job | 新 Job / attempt | G3 计算 |
|---|---|---|---|
| FAILED / CANCELLED → 创建新 attempt | 保持原 ID、终态、时间、错误/费用并转为历史只读行 | Mock 新建 Provider Job；live 必须以精确 Prompt/route/budget/policy 绑定创建 GenerationRun | 新 attempt 成为该镜头当前运行；未成功前持续阻断 |
| 429 / 5xx 基础设施 retry | 原 Job ID 上递增 retryCount | 不创建新 attempt | 仍由同一当前 Job 决定 |
| 当前批次完成 | 历史行 no-op | 只推进各镜头当前的非终态 Job | 所有当前 attempt SUCCEEDED 且 G2 APPROVED 才解锁 |

前端投影用 `isCurrentAttempt` 标识每个镜头参与当前批次判定的唯一行，并用
`supersedesJobId` / `supersededByJobId` 呈现替代关系。Mock API 对新 attempt 同样执行
Idempotency-Key 与严格递增校验。真实控制面不公开直接 Provider Job submit；唯一写
入口是 `POST /api/v1/shots/{shotId}/runs`，且必须提交 PostgreSQL 中存在的
`shotSpecRevisionId`、`promptSnapshotId`、generation profile/plan、冻结
`routeSnapshot`、`budgetApprovalId` 与 `executionPolicy`。PoC 未加载这些真实绑定时
必须 fail closed，不能把 Mock 行转换为 live 请求。

这一 live 门禁适用于所有从 Mock state 派生的 mutation，不只 Job attempt：Series
创建、G1/G2/G3、Mock 完成、revision 重生成和并发注入都在 `fetch` 前返回
`LIVE_PROJECTION_BINDINGS_REQUIRED`。只有 provider discovery GET 保持启用。

### 2.1 新 attempt 创作意图幂等

- 意图键为 `project + sourceJobId + nextAttempt`。前端在第一次异步操作前同步写入
  in-flight set，因此同一事件循环内的重复激活只会进入一次 API。
- `generationAttemptId` 和 `Idempotency-Key` 由意图键稳定派生并缓存在内存 command
  ledger；请求已受理但响应丢失时，再次操作会复用同一请求身份，由控制面返回原 Job。
- Mock 请求快照携带非敏感 `creativeIntentKey = sourceJobId:nextAttempt`，用于演练
  审计与唯一性对账；不包含 Secret。live GenerationRun 的幂等身份由公开 Run 命令及
  服务端事务、稳定 Workflow ID 和不可变输入绑定共同保证，不向当前 OpenAPI 注入
  `requestSnapshot` 扩展字段。
- Mock 服务端维护每个 shot 的 current Job、每个 Job 的 attempt，以及唯一
  `(sourceJobId, nextAttempt)`。同键同 payload replay 原结果；不同键重放已落库意图返回
  `ATTEMPT_ALREADY_EXISTS`；历史 source 返回 `ATTEMPT_SOURCE_SUPERSEDED`。客户端传入的
  `isCurrentAttempt` 不作为服务端真相。

## 3. 错误映射

| HTTP/错误 | UI 行为 |
|---|---|
| 401 `unauthenticated` | 提示在服务端配置 Secret；不提供浏览器 Key 输入 |
| 403 `forbidden` | 提示核对模型、Endpoint 与区域权限 |
| 429 `rate_limited` | 显示 Retry-After，同 Job ID 退避 |
| 5xx `provider_unavailable` | 最多 3 次基础设施重试，不新建创作 attempt |
| timeout | 进入 UNKNOWN/reconciliation，保留 task ID |
| quota/budget | 阻断 submit，重新规划或批准预算 |
| content safety | 人工修改输入并创建新 attempt |
| duplicate callback | applied=false，不重复提示、不重复结算 |
| out-of-order callback | ignored，终态和序列不得回退 |
| cancel race | 最先确认的合法终态优先，费用继续对账 |

## 4. 证据标签

- `mock_only`：固定 fixture；只证明契约和编排行为。
- `pending_key` / `pending_key_validation`：真实账号测试尚未完成；不得展示真实 SLA 或价格结论。
- `live_provider_call`：必须同时有 provider/model/request/task、输入输出 hash、usage/cost 与测试时间窗证据。
- `verified=false`：金额是估算或 Provider 未返回可核实账单，不能呈现为实付。

Mock 任务即使 SUCCEEDED 也保持 `mock_only`，不会升级为真实质量证据。

## 5. Revision、diff 与回滚

1. 上游内容、资产、上下文和 Prompt 都只新增 revision。
2. Gate decision 精确绑定 revision ID 和 content hash。
3. 新 revision 通过 dependency graph 将消费者标为 stale，不覆盖已批准 revision。
4. Prompt/资产“选择”创建新的业务引用；资产引用变化同时创建新的不可变 asset-set/G1 snapshot，更新精确 binding 和 hash，使 G1 重新待审并阻断 G2/G3。
5. 受资产变化影响的镜头标记 stale；旧 G2 revision 不得再次批准，必须创建新的 G2 revision 吸收 Prompt/镜头影响。
6. 回滚选择历史 revision，不删除较新 revision、CAS artifact、cost ledger、audit 或 Manifest。
7. 已提交 ProviderJob 不随应用回滚而撤销；reconciliation 继续使用原 task ID。
8. G3 锁版后的修改产生新的 episode cut/Manifest revision，旧导出仍可验证。
9. 真实 G3 决策必须绑定精确 Manifest/QC；只有
   `POST /api/v1/runs/{runId}/publication-lock` 成功后才形成不可变发布锁。Mock G3
   不代表 artifact 已提交或作品已发布。
10. 生成和后期付费提交前，控制面必须重新验证 Consent/License、预算保守预占、
    Prompt/route 绑定与 artifact commit；UI 的 enabled 状态不替代服务端 fail-closed。

## 6. 浏览器安全边界

浏览器允许：

- bearer 会话由部署层提供时的受控 API 请求；
- 非敏感 ID、mask/fingerprint、状态、稳定错误、trace、hash 与费用投影；
- 本地内存中的表单草稿。

浏览器禁止：

- Provider Key、Authorization、secret store value；
- 读取开发者目录或本地 Agent 配置；
- 把 Secret 写入 localStorage/sessionStorage/IndexedDB；
- 记录原始 Provider 请求/错误体、cookie 或临时签名地址；
- 直接调用 Provider、PostgreSQL、Temporal 或 CAS 文件卷。

PoC API client 明确使用 `credentials: "omit"`，且没有任何持久化调用。生产鉴权集成时由同源控制面负责会话和 CSRF 策略，仍不把 Provider Secret 发往前端。
