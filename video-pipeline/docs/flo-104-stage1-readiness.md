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

正式 Stage 1 视频 job 只能通过 `video-stage1-runner`：该命令固定构造
`PostgreSQL product truth + Gate + AdapterSubmitter + Executor`，completion 会从内部 Adapter 重新读取 terminal
task、request、用量、费用、错误分类与 CAS artifact 后再调用 `Gate.Complete`。它每次只
从 stdin 接受一个 `submit`、`retry` 或 `complete` JSON，不含自动重试循环；`submit` 与
`retry` 都只接受
`{"shotId":"sample1-shot-01"}`，调用方无法覆盖 Prompt、Run、route、预算、资产或授权。
真实 submit 前必须
显式注入至少 32 字节的内部服务认证。默认 Compose 与 `make video-stage1-readiness`
仍保持 Mock/纯校验，不会启动 runner 或产生 Provider 调用。

正式执行前必须由当前 PostgreSQL 产品真相冻结单独的 execution package。该文件只含十个
Run、ShotSpecRevision、PromptSnapshot ID/hash、Plan、VIDEO approval、route 和额度，以及
同一顺序的完整 `FinalizeEpisodeInput`（TTS route/approval、UTF-8 字幕、对白轨、720p/24fps
成片、Manifest/SBOM）；不得包含 Prompt 文本、Secret 或 signed URL。先准备未带
`contentHash` 的 draft，再无费用封印并验证：

```bash
go run ./cmd/video-stage1-package seal \
  video-pipeline/config/flo104-stage1-readiness.json approved-execution-package.draft.json \
  > /var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-execution-package.json
go run ./cmd/video-stage1-package verify \
  video-pipeline/config/flo104-stage1-readiness.json \
  /var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-execution-package.json
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
  --profile live --profile stage1 run --rm -T stage1-runner complete \
  < approved-completion.json
VIDEO_STAGE1_RETRY_PACKAGE_PATH=/var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-retry-package.json \
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --rm -T stage1-runner retry \
  < approved-retry-shot-selection.json
VIDEO_STAGE1_RETRY_PACKAGE_PATH=/var/lib/video-pipeline/artifacts/stage1/flo104-sample-1-retry-package.json \
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --rm -T stage1-runner finalize-input
```

`complete` 的 AFP 增量必须来自本次 job 的独立套餐用量证据；video tokens、现金、task/
request ID、内容安全分类和 artifact 不接受 stdin 覆盖，只从已认证 Adapter 回读。现有
通用 Temporal worker 不属于本批正式提交入口，不能替代上述 runner。TTS 则仍由后期
worker 的 `VIDEO_SPEECH_PROVIDER_ADAPTER_URL` 显式指向内部 Live Adapter；默认值保持
Mock。

回滚只需停止阶段 1 runner 并恢复上一镜像。无数据库迁移；本轮复用既有 PostgreSQL
Run/Prompt/Plan/approval/reservation/provider_job 表，v1/v2 readiness ledger 不会被
v3 runner 接受。本阶段尚无真实 Stage 1 job，readiness QA 应先归档任何无费用旧版
ledger；v3 runner 会在首次绑定基础 package 时原子创建新 ledger，retry extension hash
同样只可绑定一次。ledger 与 CAS 均保留
为审计证据，不删除历史任务；任何 `AMBIGUOUS` 记录必须人工恢复。
