# Studio Agent Plan 单镜主链路

`FLO-161` 提供一个窄的、订阅专用的真实单镜后端入口。浏览器只提交标题、场景文本、画幅和权利声明；Series、Source、Episode、Scene、ShotSpec、Prompt、Profile、上下文、Safety、Route 与额度绑定都由服务端生成并冻结。

## 安全开关

控制面只有同时满足以下条件才接受确认：

- `VIDEO_LIVE_CALLS_ENABLED=true`；
- `VIDEO_PROVIDER_SERVICE_AUTH_SECRET` 至少 32 字节；
- 通过带 HMAC 的内部请求读取 Adapter `/v1/capabilities`；
- `video.primary` 为 `configured=true`、`enabled=true`、`mode=live`；
- `limits.billingMode=subscription`，且确认时 capability hash 与计划完全一致。

本地默认值为关闭。`make video-orbstack-up` 是已授权本地 live 栈入口，它把同一内部认证 Secret 只注入控制面、worker 和 Adapter。浏览器不会收到 ARK key、内部 Secret、宿主机路径或 Provider 临时下载 URL。

## API 顺序

1. `POST /api/v1/creator/live-shot-plans`，携带 UUID `Idempotency-Key` 和 `schemaVersion=v1`。该请求只做 Adapter capability GET、CAS 写入和 PostgreSQL 计划事务；计划内 `providerCallCount=1`，实际 `providerSubmitCount=0`。
2. 在 15 分钟内调用 `POST /api/v1/creator/live-shot-plans/{planId}/confirm`。Body 必须包含 `schemaVersion=v1`、`confirmed=true` 与原 `planHash`，`If-Match` 必须是同一 hash 的双引号形式。
3. 用返回的 run URL 轮询；响应支持 `ETag` / `If-None-Match`。
4. 成功后从同源 artifact URL 播放或下载 MP4；GET/HEAD 与 `Range` 均受支持。Manifest URL 返回 `live_provider_call` 证据。

所有 mutation 都要求 UUID `Idempotency-Key`。计划事务会同时写入规范化 GenerationPlan operation、subscription token 预算审批、rights/safety bindings 与完整 Prompt 谱系；`planHash` 绑定计划时的项目累计额度/并发快照，额度变化后必须重新计划。确认事务先写确定性的 operation、run、provider job、request hash 与 token reservation，再启动 Temporal；双击、响应丢失和进程重启不会产生第二次 Provider POST。

## 固定约束与成本语义

- 单镜：1 个任务、最多 1,000,000 video tokens、5 秒、720p、24fps、1 个候选、无音频；
- Studio 项目（当前认证 actor）：累计最多 3 个任务和 3,000,000 video tokens；保守并发为 1，`UNKNOWN` / `RECONCILING` 也占用并发；
- 只允许 subscription；paygo、unknown billing 和任何正现金收费声明都 fail closed；
- subscription 的现金证据必须为 `amountMicros: null`、`verified: false`，绝不把未知费用伪装成已验证的 0；
- `SUCCEEDED` 只会在 MP4 已存在于 CAS 且 Manifest 与 hash 同事务提交后出现。

迁移为 `000012_creator_live_shot`。部署顺序仍为 migrate → Adapter/worker → control-plane；升级后 `/api/v1/providers/status` 会展示真实 Adapter snapshot 与独立 live 开关状态。
