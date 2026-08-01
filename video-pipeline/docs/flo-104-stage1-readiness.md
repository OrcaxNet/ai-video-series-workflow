# FLO-104 阶段 1 无费用 readiness

本阶段只冻结并验证执行合同，不调用真实 TTS 或视频 Provider。运行：

```bash
make video-stage1-readiness
```

固定合同位于 `video-pipeline/config/flo104-stage1-readiness.json`：正式样片只允许
`doubao-seedance-2.0` 的 10 个主 job 与最多 1 个经批准的受控重试；视频总量不超过
1,200,000 tokens；月度总量不超过 38,000 AFP；非订阅调用不超过 20 CNY；对白
总文本不超过 600 个 Unicode 字符（按 135 milli-AFP/字符归因，最大 81 AFP）。

`internal/videopipeline/stage1` 的 v2 ledger 在 Provider submit 前原子预留额度并持久化
idempotency key。terminal completion 按不可变序号冻结；相同结果任意重放是幂等
no-op，不同结果返回 conflict，token/AFP/现金与连续安全失败均从完整终态序列重新推导。
未知 submit 结果在重启后也只能 recover，不能再次 submit。受控重试必须绑定原终态
失败、失败分类、审批 ID 与“无重复任务”证据。任一许可/Consent/Gate/预算/内容安全
检查失效、连续两次内容安全失败、既有证据不完整、额度或 10% AFP 漂移越界都会
fail-closed。

Agent Plan TTS 由同一 Live Adapter 提供 `speech.primary`：只读取运行时
`ARK_API_KEY`，固定 `X-Api-Resource-Id: seed-tts-2.0`，要求 Provider 返回 usage
tokens，并把唯一 request/connect ID 和 `X-Tt-Logid` 写入后期 Manifest 的 speech
attempt。

正式 Stage 1 视频 job 只能通过 `video-stage1-runner`：该命令固定构造
`Gate + AdapterSubmitter + Executor`，completion 会从内部 Adapter 重新读取 terminal
task、request、用量、费用、错误分类与 CAS artifact 后再调用 `Gate.Complete`。它每次只
从 stdin 接受一个 `submit` 或 `complete` JSON，不含自动重试循环；真实 submit 前必须
显式注入至少 32 字节的内部服务认证。默认 Compose 与 `make video-stage1-readiness`
仍保持 Mock/纯校验，不会启动 runner 或产生 Provider 调用。

QA readiness 只构建 runner：

```bash
make video-stage1-runner-build
```

readiness QA 通过后，正式执行才可按单次审批输入调用（两个命令均无默认输入）：

```bash
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --build --rm -T stage1-runner submit \
  < approved-submit.json
docker compose --env-file video-pipeline/.env.video -f video-pipeline/compose.yaml \
  --profile live --profile stage1 run --rm -T stage1-runner complete \
  < approved-completion.json
```

`complete` 的 AFP 增量必须来自本次 job 的独立套餐用量证据；video tokens、现金、task/
request ID、内容安全分类和 artifact 不接受 stdin 覆盖，只从已认证 Adapter 回读。现有
通用 Temporal worker 不属于本批正式提交入口，不能替代上述 runner。TTS 则仍由后期
worker 的 `VIDEO_SPEECH_PROVIDER_ADAPTER_URL` 显式指向内部 Live Adapter；默认值保持
Mock。

回滚只需停止阶段 1 runner 并恢复上一镜像。无数据库迁移；v1 readiness ledger 不会被
v2 runner 接受。本阶段尚无真实 Stage 1 job，readiness QA 应先归档任何无费用 v1
ledger；v2 runner 会在首个获批 reservation 时原子创建新 ledger。ledger 与 CAS 均保留
为审计证据，不删除历史任务；任何 `AMBIGUOUS` 记录必须人工恢复。
