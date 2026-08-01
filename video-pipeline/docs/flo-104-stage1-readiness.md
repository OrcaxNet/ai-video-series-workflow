# FLO-104 阶段 1 无费用 readiness

本阶段只冻结并验证执行合同，不调用真实 TTS 或视频 Provider。运行：

```bash
make video-stage1-readiness
```

固定合同位于 `video-pipeline/config/flo104-stage1-readiness.json`：正式样片只允许
`doubao-seedance-2.0` 的 10 个主 job 与最多 1 个经批准的受控重试；视频总量不超过
1,200,000 tokens；月度总量不超过 38,000 AFP；非订阅调用不超过 20 CNY；对白
总文本不超过 600 个 Unicode 字符（按 135 milli-AFP/字符归因，最大 81 AFP）。

`internal/videopipeline/stage1` 的 v3 batch ledger 在 Provider submit 前预留 10+1、video
token、AFP、现金和安全熔断额度并持久化 idempotency key；它不再接收调用方自报的
License/Consent/Gate/预算/安全布尔值。正式入口先依据只含 ID/hash 的 execution package
定位 PostgreSQL Run/PromptSnapshot/Plan/审批，再由 `PrepareProviderJob` 在同一
`SERIALIZABLE` 产品真相事务里锁定并复查 G2、SAFETY、Plan 的 episode/shot 集合、完整
资产 License/Consent（含地域、商用范围和有效期）、Provider capability、VIDEO
approval 的 Plan/scope/amount/currency，并写入预算 reservation 与 Provider job。任一漂移
均在 Adapter POST 前失败，且不会写 Stage 1 新 attempt。execution package 的完整
ProductTruth/envelope 期望也随请求进入同一事务，在任何 reservation、Provider job、cost
ledger insert 前与锁定事实比较；因此冻结包偏离时 PostgreSQL 付费副作用为零。PostgreSQL prepare 回调与 v3
ledger reservation 共用 Stage 1 跨进程文件锁：第二个 runner 不能在产品真相事务与本地
额度落盘之间插入另一笔授权；拒绝的 prepare 不产生 Provider POST。

terminal completion 按不可变序号冻结；相同结果任意重放是幂等
no-op，不同结果返回 conflict，token/AFP/现金与连续安全失败均从完整终态序列重新推导。
Provider 在创建 upstream task 前返回 `requires_action/content_blocked` 时仍会冻结为
`TERMINAL_FAILED`，并记录内容安全失败及证据不完整，阻断后续提交；这是唯一允许终态
记录没有 task ID 的情况。同幂等 `submit` 重放会返回不可重试 `content_blocked`，绝不把
空 task 当作恢复成功；任何其他无 task 的 Adapter 恢复结果也 fail-closed。已冻结终态
优先于 Adapter 的 recover 结果，同幂等 `submit` 返回 conflict。`AMBIGUOUS` 只允许由
`PREPARED` 进入，重复标记为 no-op，
任何迟到的 terminal→ambiguous 写入都返回 conflict，不能降级不可变终态。
未知 submit 结果在重启后也只能 recover，不能再次 submit。受控重试使用独立、只可绑定
一次的 extension package，必须冻结新的 Run/Attempt/idempotency、原终态失败分类、审批 ID、
“无重复任务”证据以及只替换失败 Run 的后期输入；同一 `retry` 重放和 ambiguous 结果仍只
recover，不会产生第二次 POST。任一 PostgreSQL 许可/Consent/Gate/预算/内容安全
检查失效、连续两次内容安全失败、既有证据不完整、额度或 10% AFP 漂移越界都会
fail-closed。

Agent Plan TTS 由同一 Live Adapter 提供 `speech.primary`：只读取运行时
`ARK_API_KEY`，固定 `X-Api-Resource-Id: seed-tts-2.0`，要求 Provider 返回 usage
tokens，并把唯一 request/connect ID 和 `X-Tt-Logid` 写入后期 Manifest 的 speech
attempt。

## speech-v2 单镜 canary 修订

已成功的十个视频 Run、旧 VOICE 版本以及旧 speech job/reconciliation 永久只读。新的
speech-v2 包通过 `video-stage1-revoice` 从当前包和当前 VOICE 派生一个 APPROVED child
version、对应
ALLOWED license snapshot、`agent-plan-large-tts-v2` capability 与新的 execution package；
命令不接收 Adapter URL、没有 Provider client，并断言运行前后 `provider_jobs` 数量不变。
FLO-104 当前只接受 `zh_female_vv_uranus_bigtts`（vivi 2.0）；旧 Mars speaker 或任意其他
speaker 在配置加载、物化输入和 Adapter 构造阶段失败关闭。
当已有失败 canary 时，新 package 的 VOICE `parentAssetVersionId` 必须精确等于当前 package
的 VOICE version；PostgreSQL 递归验证这条 APPROVED 谱系最终回到镜头原始批准 VOICE，
既不允许跳过当前 VOICE，也不允许跨 asset 分叉。

```bash
make video-stage1-revoice-test

VIDEO_POSTGRES_DSN=postgres://... VIDEO_ARTIFACT_ROOT=/var/lib/video-pipeline/artifacts \
go run ./cmd/video-stage1-revoice \
  --plan video-pipeline/config/flo104-stage1-readiness.json \
  --parent flo104-sample1-execution-package.json \
  --revision approved-speech-v2-revision.json \
  --approval-comment <approval-comment-uuid> \
  --approval-actor <approving-agent-uuid> \
  --approval-valid-until <RFC3339> \
  --output flo104-sample1-execution-package-speech-v2.json \
  --report flo104-sample1-speech-v2-revoice-report.json
```

新 job 语义为
`speech-v2-<sha256(canonical(episodeRevisionId, subtitleContentHash, cueId, voiceAssetVersionId, routeVersion, resourceId, speaker))[:32]>`。
FLO-104 的包只授权 `cue-001`、`MaxAttempts=1`、10% hard cap `2228 milli-AFP`、
非订阅现金 `0`。PostgreSQL 在 Prepare 与付费事务内重新锁定 VOICE parent/child、license、
Provider profile/capability；Adapter 再匹配 job/input/cue/VOICE/license allowlist，并从 CAS
复算 VOICE descriptor digest、精确核对 parent/child version 与 license/route 身份。CAS
缺失、损坏、descriptor schema 或谱系漂移均在 Provider 调用前失败关闭。成功生成
一段 canary 后，Finalize 会在媒体合成、Manifest 与 G3 前返回需人工检查的非重试冲突，
不会继续提交其余 cue。跨进程相同 job 通过原子 intent/replay 保证至多一次 Provider submit。

speech-v2 execution package 还必须冻结 `parentExecutionPackageHash`，且 Runner 必须同时读取
`VIDEO_STAGE1_PARENT_EXECUTION_PACKAGE_PATH` 指向的完整只读父包。Runner 会先复算父包 hash，
再构造唯一允许的 child 投影；只允许 Speech route/profile、VOICE binding、授权 cue、AFP/现金/
attempt canary 上限以及固定 `-speech-v2` trace 后缀变化。PrimaryJobs、Episode/Run/Plan、视频
route/profile/预算审批、基础 speech 预算、字幕、背景音和其余后期目标必须逐字节保持不变。
父包只给 hash、不提供完整 artifact，或任何非语音字段漂移，均在首次 ledger 写入和 Provider
调用前失败关闭。父/子文件必须使用不同路径，不能用 child 覆盖父包。

通过完整父子投影验证后，Runner 只会在当前 package 的十个视频记录全部为
`TERMINAL_SUCCEEDED` 且证据完整、主 job 身份与额度逐项未变、
不存在 controlled retry 时，于同一文件锁内把 ledger 线性提升到 child package；立即父 hash
写入 `supersededExecutionPackageHash`，完整有序历史写入
`supersededExecutionPackageHashes`。同一 child 可幂等重放；同一 parent 的竞争 child、分叉、
缺镜、失败/证据不全、Attempt 漂移或错误 parent 均在启动 TTS 前失败关闭。只有新的明确批准
才能从当前 child 再派生一个 child。这样 VOICE 修订不会要求
重提已完成视频，也不能把任意后期包替换到已有视频证据上。物化事务同时写入独立的
`stage1.execution_package.revision_bound` 审计，以 parent/child package hash、VOICE version
和批准评论固定这次只改后期语音合同的派生关系；旧 VOICE 修订审计保持只读。

```bash
VIDEO_STAGE1_EXECUTION_PACKAGE_PATH=/var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-execution-package-speech-v2.json \
VIDEO_STAGE1_PARENT_EXECUTION_PACKAGE_PATH=/var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-execution-package.json \
docker compose --profile stage1 run --rm stage1-runner finalize-input
```

在连接 ledger、PostgreSQL 或 Adapter 前可独立执行同一父子投影验证：

```bash
go run ./cmd/video-stage1-package verify-revision \
  video-pipeline/config/flo104-stage1-readiness.json \
  flo104-sample1-execution-package.json \
  flo104-sample1-execution-package-speech-v2.json
```

Provider 失败证据保留精确 HTTP status、纯数字 Provider code、脱敏
`X-Api-Message` 分类与 `X-Tt-Logid`。401/403、非 Plan endpoint、usage/log 缺失、AFP/现金
越界均失败关闭；仅 `55000000` 分类为已知 resource-or-speaker unavailable，其他未知
`55*` 不推断根因。真实 canary 只能在新 SHA 的无费用 QA 完成后单独授权。

正式 Stage 1 视频 job 只能通过 `video-stage1-runner`：该命令固定构造
`PostgreSQL product truth + Gate + AdapterSubmitter + Executor`，completion 会从内部 Adapter 重新读取 terminal
task、request、用量、费用、错误分类与 CAS artifact 后再调用 `Gate.Complete`。它每次只
从 stdin 接受一个 `submit`、`retry` 或 `complete` JSON，不含自动重试循环；`submit` 与
`retry` 都只接受
`{"shotId":"sample1-shot-01"}`，调用方无法覆盖 Prompt、Run、route、预算、资产或授权。
真实 submit 前必须
显式注入至少 32 字节的内部服务认证。默认 Compose 与 `make video-stage1-readiness`
仍保持 Mock/纯校验，不会启动 runner 或产生 Provider 调用。

PromptSnapshot 与数据库始终只保存完整、精确的 `cas://sha256/<digest>` 资产引用。Live
Adapter 在外部 Provider submit 前复查 URI、digest、声明/实际 bytes，并用 PNG/JPEG
格式 allowlist 解码实际媒体；任何非 CAS URI、大小漂移或伪装 MIME 均失败关闭。校验
通过后才仅在内存中把可视媒体转换为 Provider 可读取的 data URL；临时字节不会进入
job registry、日志或证据包。用于
License/Consent 与 TTS 谱系的 `audio/x-voice-profile+json` 语音画像不会被误发为
Seedance 音频输入。CAS 缺失、损坏、类型不匹配会在 upstream POST 前失败关闭。

正式执行前必须由当前 PostgreSQL 产品真相冻结单独的 execution package。管理员批准的
product-input/source/safety/visual 四个固定附件只能通过 `video-stage1-materialize` 导入：
命令先逐文件校验 bytes/SHA-256，把原始附件写入 CAS，再在一个 `SERIALIZABLE` 事务中
写入精确 UUID 的 Series/Episode/Scene/Shot、四层 Context、资产/License、G1/G2/SAFETY、
VIDEO/SPEECH budget、Plan、Prompt 和十个 VALIDATED Run。它没有 Adapter URL、认证客户端
或 submit 操作，离线报告还会断言本包的 reservation/provider_job/cost ledger 均为 0。
外部 product hash、每镜原始 hash 和 ADMIN comment/validUntil 保存在不可变 audit 中；数据库
canonicalizer 派生的 Prompt/Run hash 单独写入，不会静默覆盖原始证据。普通编译器的
derived Prompt UUID 合同保持不变，只有带完整导入 audit 的 `stage1-product-input-v1`
记录允许使用管理员预留 UUID。

每个导入 Run 还必须有与普通工作流一致的 `generation_run.created` 审计，并在 payload 中
精确绑定冻结 `generationPlanId`。旧物化包重放只可在 Run/Attempt 与原封印包完全一致、且
reservation/provider job/cost ledger 全为零时补齐该审计；错 Plan、重复审计或已经跨过
付费边界均失败关闭。first attempt 必须仍为 `VALIDATED`、sequence=1，kind 与 creative
attempt 一致，`input_hash` 必须等于冻结 Run digest，route 也必须逐字段一致；terminal、hash、
kind、sequence 或 route 漂移均不得补写 Plan provenance。`PrepareProviderJob` 不从
materialization report 或调用方输入猜测 Plan，并在任何 reservation/job/cost 写入前独立
复查相同 Attempt 身份。只有已存在且请求快照、reservation 完全一致的 Provider job 可以幂等
恢复；Run/Attempt 已终态时也只能走这条精确恢复路径。不存在既有 job 时，Run 必须非终态且
first Attempt 必须仍为 `VALIDATED`（控制面可在 Activity 开始前把 Run 生命周期推进为
`RUNNING`，但不会提前推进 Attempt）。

示例（路径仅为操作占位，必须换成 issue 中四个固定附件；不传任何 Secret）：

```bash
VIDEO_POSTGRES_DSN=postgres://... VIDEO_ARTIFACT_ROOT=/var/lib/video-pipeline/artifacts \
go run ./cmd/video-stage1-materialize \
  --product flo-104-sample1-product-input-v1.json \
  --source flo-104-sample1-source-v1.txt \
  --safety flo-104-sample1-safety-evidence-v1.json \
  --visual flo-104-sample1-visual-reference-board-v1.png \
  --plan video-pipeline/config/flo104-stage1-readiness.json \
  --approval-comment <ADMIN-comment-uuid> --approval-actor <ADMIN-agent-uuid> \
  --approval-valid-until <RFC3339> \
  --output flo104-sample1-execution-package.json \
  --report flo104-sample1-materialization-report.json
```

输出文件只含十个
Run、ShotSpecRevision、PromptSnapshot ID/hash、Plan、VIDEO approval、route 和额度，以及
同一顺序的完整 `FinalizeEpisodeInput`（TTS route/approval、UTF-8 字幕、对白轨、720p/24fps
成片、Manifest/SBOM）；不得包含 Prompt 文本、Secret 或 signed URL。独立 QA 继续用
同一 verifier 复核：

```bash
go run ./cmd/video-stage1-package verify \
  video-pipeline/config/flo104-stage1-readiness.json \
  flo104-sample1-execution-package.json
```

Runner 启动时和每次新 submit 都验证 package；v3 ledger 还永久绑定 package 的
`contentHash`，并在首条 PREPARED 之前立即原子写盘、fsync 文件与目录；prepare 被拒后
跨重启也不能换包。缺文件、hash 漂移、十镜/顺序/后期包不完整，
或任一引用在 PostgreSQL 中不存在/已漂移时均在 Provider POST 前停止。已存在 Provider task
仅走 recover，不因事后授权漂移重复 POST。

主 job 具有证据完整的终态失败后，先把审批、失败分类、去重证据、新 Run/Attempt 和替换
Run 后的完整后期输入写入 retry draft，再单独封印；它不会改写基础十镜包：

```bash
go run ./cmd/video-stage1-package seal-retry \
  video-pipeline/config/flo104-stage1-readiness.json \
  /var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-execution-package.json \
  approved-retry-package.draft.json \
  > /var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-retry-package.json
```

QA readiness 只构建 runner：

```bash
make video-stage1-runner-build
```

readiness QA 通过后，正式执行才可按单次审批输入调用（两个命令均无默认输入）：

```bash
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --build --rm -T stage1-runner submit \
  < approved-shot-selection.json
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --rm -T stage1-runner poll \
  < approved-poll-selection.json
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --rm -T stage1-runner complete \
  < approved-completion.json
VIDEO_STAGE1_RETRY_PACKAGE_PATH=/var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-retry-package.json \
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --rm -T stage1-runner retry \
  < approved-retry-shot-selection.json
VIDEO_STAGE1_RETRY_PACKAGE_PATH=/var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-retry-package.json \
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --rm -T stage1-runner finalize-input
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --rm -T stage1-runner finalize </dev/null
```

`finalize` 与 submit 使用相同冻结包和 ledger；只有十个主 Run（或唯一受控重试替换）
均为证据完整的成功终态后，才以绑定 package hash 的稳定 Workflow ID 启动或恢复
`video.production.stage1-finalization.v1`。该 Workflow 只执行 `FinalizeEpisode → CreateGate3`，
不会重新编译 Prompt、创建 Run 或提交视频 job。Live Worker 的视频与 TTS 客户端共享同一
短期签名服务认证，submit/poll/cancel 均不能绕过 Adapter 信任边界。

`poll` 只接受已进入冻结 ledger 的 idempotency key，只执行带服务认证的 Adapter GET，
不会创建 Provider job 或写 terminal ledger。确认 Adapter 已返回终态并取得独立套餐增量后
才调用 `complete`。`complete` 的 AFP 增量必须来自本次 job 的独立套餐用量证据；video tokens、现金、task/
request ID、内容安全分类和 artifact 不接受 stdin 覆盖，只从已认证 Adapter 回读。现有
通用 Temporal worker 不属于本批正式提交入口，不能替代上述 runner。TTS 则仍由后期
worker 的 `VIDEO_SPEECH_PROVIDER_ADAPTER_URL` 显式指向内部 Live Adapter；默认值保持
Mock。

调用 `finalize` 前必须以同一个内存注入的 service-auth secret 启动 Live Adapter 与
后期 Worker，并显式选择内部 TTS 路由；不得把 Adapter 的 8091 端口发布到宿主机：

```bash
VIDEO_SPEECH_PROVIDER_ADAPTER_URL=http://volcengine-provider:8091 \
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live up --build --wait \
  postgres migrate temporal volcengine-provider orchestrator-worker
```

未设置该路由时 Worker 保持 Mock，Live `FinalizeEpisode` 会因冻结 route/证据不匹配
失败关闭，不会把 Mock 结果提升为正式证据。

回滚只需停止阶段 1 runner 并恢复上一镜像。无数据库迁移；本轮复用既有 PostgreSQL
Run/Prompt/Plan/approval/reservation/provider_job 表，v1/v2 readiness ledger 不会被
v3 runner 接受。已导入的产品记录和 CAS 是不可变审计证据，不在代码回滚时删除；旧镜像
无法执行 `stage1-product-input-v1` Prompt，会 fail-closed。本阶段尚无真实 Stage 1 job，readiness QA 应先归档任何无费用旧版
ledger；v3 runner 会在首次绑定基础 package 时原子创建新 ledger，retry extension hash
同样只可绑定一次。ledger 与 CAS 均保留
为审计证据，不删除历史任务；任何 `AMBIGUOUS` 记录必须人工恢复。
