# 需求追踪与验证边界

FLO-108 将 FLO-96 的 24 条 P0 制片需求与 FLO-124 的纯模型 API 约束合并。最新约束优先：无 GPU、无本地生成模型、火山优先、无 Key 可 Dry-run/Mock。状态：

- `RUNNABLE`：本骨架可直接运行/测试；
- `CONTRACT`：API/Event/SQL/Workflow 边界已冻结；
- `REAL-KEY`：必须等真实火山/语音 Key 才能验收；
- `FRONTEND`：前端按 OpenAPI 独立实施，本后端 issue 不改 UI。

## 1. FR-P0-01～24

| FR | 组件 | 接口/事件 | 数据 | 验证 |
|---|---|---|---|---|
| 01 剧集/Profile | Control Plane | `POST /series` | series/generation_profiles | CONTRACT；profile 只含能力 alias/CPU 媒体规则，无硬件/模型权重 |
| 02 小说摄取 | Ingestion | sources API + revision event | source_revisions/evidence | CONTRACT；hash/rights/offset/重复上传 |
| 03 设定抽取 | text Provider Adapter | text job + revision | entity_revisions/evidence | Mock schema；REAL-KEY structured output/usage |
| 04 基础原画 | image Provider Adapter | plan/provider job/artifact | assets/versions/provider_jobs | Mock success/error；REAL-KEY reference/size/seed |
| 05 G1 资产门 | Approval/Policy | approvals | decisions/bindings/license | CONTRACT；未批准/无权利不可下游 |
| 06 剧本/拆集 | text Provider Adapter | operation + revision | episode_script_revisions | Mock/Dry-run；REAL-KEY 结构和事实质量 |
| 07 分镜/ShotSpec | Storyboard/Capability validator | revision + capabilities | storyboard/shot_spec | CONTRACT；按当前 snapshot 校验时长/比例，不硬截断 |
| 08 G2 剧本门/批次 | Approval + Workflow | production-batches | G2 binding/operation | RUNNABLE Workflow 强制 G2 |
| 09 四层上下文 | Context resolver | Workflow input | context/effective snapshot | RUNNABLE 精确四层解析、来源记录与稳定 hash |
| 10 Prompt 编译 | Prompt compiler | Activity | prompt_snapshots | RUNNABLE 不可变 Prompt/effective-context snapshot 与依赖证据 |
| 11 Plan/预算/幂等 | Plan/Run services | generation-plans/provider-jobs | idempotency/budget/run | RUNNABLE Mock replay；0 重复 external task |
| 12 远程生成执行 | Provider Adapter | submit/poll/callback/cancel | provider profile/cap/job/callback | RUNNABLE Mock 全故障；REAL-KEY 火山 |
| 13 自动 QC | QC Activity | qc event | qc_reports | RUNNABLE 结构 QC；REAL-KEY 媒体/一致性/安全 |
| 14 受控重试/换模型 | Workflow/Router | same job retry / new attempt | attempts/jobs/model snapshot | RUNNABLE infra max3；自动跨 provider=false |
| 15 中断恢复 | Temporal/Reconciler | resume/poll | upstream task/state | RUNNABLE slow/unknown；REAL-KEY kill/restart 0 重复 |
| 16 取消/补偿 | Control Plane/Adapter | cancel | cancel state/orphan/cost | RUNNABLE cancel race；REAL-KEY cancel semantics |
| 17 TTS/授权 | speech Adapter/Policy | provider job/artifact | voice asset/consent | CONTRACT+Mock；REAL-KEY voice/timestamps |
| 18 字幕/口型 | Subtitle/CPU media | operation/revision/artifact | subtitle/audio/QC | CONTRACT；REAL-KEY timestamp；口型可选 |
| 19 拼接/导出 | CPU Media Worker | delivery operation | MP4/SRT/VTT/audio/manifest | 无 GPU；后续 FFmpeg fixture E2E |
| 20 Q1/G3 锁版 | Review/Policy/Manifest | approvals + G3 signal | review/decision/manifest | RUNNABLE Q1/G3 review task、精确绑定与 `LOCKED` |
| 21 immutable/stale | Revision graph | impacts API/event | dependencies/freshness | CONTRACT；新 asset v3 不改 v2 引用 |
| 22 谱系/Manifest | Manifest builder | manifest GET/event | generation_manifests | RUNNABLE provider/model/request/task/usage/cost/实际媒体规格/hash 挂载 |
| 23 权限/审计/Secret | Auth/RBAC/Redaction | all mutations | audit/provider profile | CONTRACT；Secret scan 0；禁止扫 Claude Code |
| 24 观测/成本熔断 | OTel/Budget/Cost | provider/cost events | budget/cost/outbox | CONTRACT；plan confirmation、unknown price、probe shot |

## 2. FLO-124 验收

| AC | 实现锚点 | 当前验证 |
|---|---|---|
| AC-01 无 Key 启动/Dry-run | Compose、`/providers/status` | RUNNABLE smoke：四类 live false，Mock/Dry-run true |
| AC-02 Key 连接测试/脱敏 | OpenAPI connection-test、Secret policy | CONTRACT；REAL-KEY |
| AC-03 参数与能力冲突 | capability snapshot + Shot validator | CONTRACT；Mock capability fixture |
| AC-04 重复点击只一任务 | provider job unique/idempotent mock | RUNNABLE unit+smoke |
| AC-05 重启后恢复 task ID | PG provider_jobs + Temporal heartbeat | CONTRACT/Mock recovery；REAL-KEY kill test |
| AC-06 临时 URL 归档 | Adapter→CAS | RUNNABLE CAS；REAL-KEY URL expiry |
| AC-07 timeout→unknown | Provider state/reconciler | RUNNABLE Mock timeout |
| AC-08 429/5xx max3 | error mapping/Temporal policy | RUNNABLE Mock category；REAL-KEY Retry-After |
| AC-09 auth/quota/content action | stable errors | RUNNABLE failure matrix |
| AC-10 asset v2/v3 | immutable revisions/dependencies | CONTRACT |
| AC-11 四层 Prompt 来源 | snapshots/data model | CONTRACT |
| AC-12 批量提交前估算 | generation plan/budget | CONTRACT + Mock estimate |
| AC-13 预算不足 0 submit | reservation gate | SQL/API CONTRACT |
| AC-14 单镜头新 attempt | Run/Attempt dependency | CONTRACT |
| AC-15 3 镜 MP4/SRT/VTT/Manifest | CPU media boundary | 后续 M1 REAL-KEY |

## 3. 被 FLO-124 覆盖的 FLO-96 假设

| 旧假设 | 状态 | 当前决策 |
|---|---|---|
| Wan2.2 TI2V-5B 固定主线 | 覆盖 | `video.primary` → 实际 model/endpoint snapshot |
| 本地 24GB GPU 最低硬件 | 删除 | 本地 GPU 不存在也必须工作 |
| ComfyUI 必需执行面 | 覆盖 | 非默认可替换远程 adapter，不能成为前置 |
| CUDA/PyTorch/VRAM Manifest | 覆盖 | provider/model/request/task/usage/cost/input-output hash |
| GPU 小时预算 | 覆盖 | Provider units/金额/价格版本/配额/预算预占 |
| 本地模型 benchmark 激活 | 覆盖 | 真实 provider capability/quality/cost/SLA 实测 |
| GPU OOM fallback | 覆盖 | capability mismatch、quota/rate/region/model/outage 明确分类 |

仍不变：

- 四层上下文；
- 资产/Prompt/产物不可变 revision 与 stale；
- G1/G2/Q1/G3 人工质量边界；
- 短镜头拼集；
- 授权/许可/AI 标识；
- 完整谱系、可恢复与幂等；
- 确定性 CPU 后期处理。

## 4. 架构决策追踪

| 决策 | ADR/契约 | 失败路径 |
|---|---|---|
| 云生成、本地编排 | ADR-0001/0003 | Provider outage→same JobID retry/reconcile |
| 不可变 revision/CAS | ADR-0002 | stale/impact/rollback |
| 能力 alias + snapshot | ADR-0003 | model unavailable→new route/attempt |
| Gate/权利/Manifest | ADR-0004 | missing license/consent→block |
| 无 Key/Secret policy | ADR-0005 | live disabled，Dry-run/Mock 可用 |
| 先 plan 后消费 | OpenAPI + budget tables | over budget→0 external calls |
| no auto cross-provider | Config/provider contract | user-confirmed new attempt |

## 5. QA 执行矩阵

| 层 | 命令/fixture | 通过 |
|---|---|---|
| Unit | `go test -race ./...` | FLO-110 契约、CAS、status、provider failures、callback、cancel、Workflow |
| Contract | `go test ./video-pipeline/contracts` | YAML + required operations/events/errors + forbidden legacy assumptions |
| Static | `go vet ./...` | 0 issue |
| Compose | `docker compose ... config --quiet` | no GPU service/image/device |
| M0 E2E | `make video-up && make video-smoke` | no Key status、idempotency、CAS、migration、Temporal LOCKED |
| Security | secret pattern scan | response/log/trace/PG/Manifest/export 0 hit |
| M1 Provider | operations 真实实测清单 | 四能力、恢复、错误、成本、3 镜成片 |

## 6. 当前骨架证据

| 能力 | 文件 |
|---|---|
| Provider-neutral interface/types/errors | `internal/providercontract` |
| Fixed-response Mock + fault injection | `internal/videopipeline/mockprovider` |
| No-key status | `internal/videopipeline/controlplane` |
| Temporal provider reconciliation/G3 | `internal/videopipeline/orchestration` |
| CAS atomic immutable commit | `internal/videopipeline/artifactstore` |
| Provider/route/job/callback/budget/cost schema | migration |
| Public/async contracts | OpenAPI/AsyncAPI |
| no-GPU deployment | Dockerfile/Compose/CI |

本交付不把 Mock 通过等同于真实模型质量通过；真实 Key 缺失是明确的 M1 实测边界，不阻塞 M0 架构和契约冻结。
