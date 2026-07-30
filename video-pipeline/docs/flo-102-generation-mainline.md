# FLO-102 内容编译与模型 API 生成主链路

## 交付边界

本切片实现了可独立测试的后端领域主链路，代码位于
`internal/videopipeline/production`。它只依赖
`internal/providercontract` 的统一接口，不依赖 GPU、ComfyUI、模型权重或单一供应商字段。

```text
授权小说 revision
  -> text.primary 结构化输出
  -> 世界/角色/关系/场景/道具 revision
  -> 分集/场景/4-6 秒 ShotSpec revision
  -> series/episode/scene/shot 四层上下文解析
  -> 精确资产 revision + 上一 Prompt/尾帧解析
  -> 不可变 PromptSnapshot
  -> G1/G2 + 授权 + 预算 + 能力发现
  -> Provider-neutral GenerationRequest
  -> submit/poll/cancel/retry/idempotency
  -> 临时 URL 下载、SHA-256 校验、CAS 提交
  -> Run/Attempt + Generation Manifest
  -> QC 通过 + G3 绑定 Manifest hash 后创建 publication lock
```

## 关键实现

### 内容与不可变修订

- `ContentCompiler` 在调用模型前校验来源授权、改编权限和有效期。
- `ProviderContentGenerator` 通过 `providercontract.Provider` 调用
  `text.primary`，不读取开发机 Claude Code 配置。若以后接 Claude，专用
  adapter 必须使用官方 Anthropic Go SDK 并只接收显式运行时 Secret。
- 模型必须返回严格的 `CompilationDraft v1` JSON；未知字段、空输出、非 JSON
  或多值输出失败关闭。
- EvidenceSpan 使用授权来源文本的字节区间和 SHA-256，防止模型把无来源内容伪装为原文事实。
- `RevisionStore` 采用 compare-and-append；修改、回滚都生成新 revision，
  历史 revision 永不覆盖。`DependencyGraph` 返回下游 stale 闭包。

### 四层上下文、资产与 Prompt

- `ContextResolver` 要求 series、episode、scene、shot 各一个精确 revision，
  按固定顺序合并。
- 每层只有白名单字段可写或覆盖；例如 shot 不能改系列视觉风格或输出比例。
- 资产只能引用已授权、已审批、ACTIVE、`cas://sha256/...` 的精确 revision；
  `latest`、失效引用、未授权 alias 冲突均被阻断。
- `PromptCompiler` 冻结 ShotSpec hash、四层 Context hash、资产 hash、
  上一 Prompt hash、尾帧 hash、模板、生成 Profile 和输出规格。
- 编译结果同时包含稳定 Prompt、negative constraints、字幕时间轴、
  provider-neutral model payload、输入 hash 表和 Prompt diff。
- 付费提交前，`GenerationRunner` 必须从持久化 `PromptSnapshotSource` 按 ID
  取回记录，重算 `normalized_input_hash` 与 `content_hash`，并逐字段核对调用方
  输入；旧 ID/hash 下的篡改或未入库的新 snapshot 均失败关闭。

### Provider 执行、CAS 与谱系

- `GenerationRunner` 在任何付费调用前校验来源授权、G1、G2、预算预留、
  路由快照和运行时能力发现。预算预留绑定 run、Prompt content hash、模型
  路由和估算包络，且金额必须覆盖当前 estimate、不得超过请求上限；
  `ReservedMicros` 仅表示其他未结预留。Provider 返回的实际费用超过已批准
  预留时，Run 以 `budget_exceeded` 失败关闭，不能进入成功 Manifest/G3。
- 不确定 submit 使用原 idempotency key 重试；poll 错误不会创建新的创作
  attempt；同 key 不同输入返回 conflict。
- 401/403、429、5xx、超时、配额、内容安全、地区和模型不可用使用
  `providercontract.ErrorCode` 分类，Run ledger 只允许一个终态。
- `DownloadingCASCommitter` 以有界流读取 provider 临时 URL，计算/核验
  SHA-256 并只返回 CAS URI。签名 URL 不进入 Manifest。
- 成功与 provider 终态生成不可变 Manifest，记录请求、上下文、输入/输出
  资产、provider/model/region/request/job、attempt、用量、费用和实际输出规格。
- 通过的 QC 报告和 G3 必须同时绑定该 Manifest 的 ID 与 hash，才能建立
  publication lock。

## Temporal、FLO-101 与 FLO-103 接口

Temporal `CompilePrompt` Activity 已支持注入 `PromptSource`，将完整 Prompt、
ContextRefs、资产与输出规格传给 Provider Job。无数据库注入时只使用明确标记的
mock-only fallback，以维持无 Key Compose smoke；live 环境不得用该 fallback。

FLO-101 前端使用以下稳定对象，不接收 provider Secret 或临时 URL：

- 内容编辑：source/entity/episode/scene/shot revision 与 stale impacts；
- Prompt 预览：effective context、input revision hashes、Prompt diff、字幕时间轴；
- 运行与审核：GenerationRun、Attempt、QC、Manifest、G1/G2/G3 binding。

FLO-103 控制面按 `contracts/openapi.yaml` 实现：

- `POST /sources/{sourceRevisionId}/compilations`
- `POST /shots/{shotId}/prompt-snapshots`
- `POST /shots/{shotId}/runs`
- `POST /runs/{runId}/publication-lock`

PostgreSQL migration `000006_generation_mainline` 增加内容编译记录、Prompt 输入/
资产关联、上一 Prompt、输出规格、输入 hash，以及同时绑定 QC/G3 的 publication
lock。事务写入时应同时写 `revision_dependencies`、`idempotency_records`、
`audit_events` 和 outbox。

## 测试与证据边界

```bash
go test ./internal/videopipeline/production
go test -run TestGoldenThirtyShots_CompleteMockLineage ./internal/videopipeline/production
go test ./...
make video-test
```

`testdata/golden_30_shots.json` 固定包含：

- 单角色 10 镜；
- 双角色/道具 10 镜；
- 运动/运镜 10 镜。

30 镜测试逐镜完成内容 revision、Prompt 编译、异步 Fake Provider、CAS、
Manifest 和 G3 lock；另有授权、上下文覆盖、引用冲突、Prompt diff、stale、
回滚、幂等、恢复和 API 错误分类测试。负向预检同时断言篡改/未持久化
PromptSnapshot 与低于估算的预算预留都在 Provider `Submit` 调用计数为 0 时
失败。

Fake/fixture 证据仅为 `mock_only`，不能证明真实画质、p95、成功率、模型可用性
或费用。火山 Key 到位前这些指标保持 `pending_key`。真实运行需要：

1. 运行时发现并冻结账号实际 region/model/endpoint/capability hash；
2. 用真实 Provider Adapter 和 `DownloadingCASCommitter`；
3. 执行 FLO-110 live-test 清单并写 `live_provider_call` Manifest；
4. 真实质量、时延、成功率、费用通过后再放行 G3。
