# FLO-104 阶段 1 无费用 readiness

本阶段只冻结并验证执行合同，不调用真实 TTS 或视频 Provider。运行：

```bash
make video-stage1-readiness
```

固定合同位于 `video-pipeline/config/flo104-stage1-readiness.json`：正式样片只允许
`doubao-seedance-2.0` 的 10 个主 job 与最多 1 个经批准的受控重试；视频总量不超过
1,200,000 tokens；月度总量不超过 38,000 AFP；非订阅调用不超过 20 CNY；对白
总文本不超过 600 个 Unicode 字符（按 135 milli-AFP/字符归因，最大 81 AFP）。

`internal/videopipeline/stage1` 的门禁在 Provider submit 前原子预留额度并持久化
idempotency key。未知 submit 结果在重启后也只能 recover，不能再次 submit。受控重试
必须绑定原终态失败、失败分类、审批 ID 与“无重复任务”证据。任一许可/Consent/Gate/
预算/内容安全检查失效、连续两次内容安全失败、既有证据不完整、额度或 10% AFP 漂移
越界都会 fail-closed。

Agent Plan TTS 由同一 Live Adapter 提供 `speech.primary`：只读取运行时
`ARK_API_KEY`，固定 `X-Api-Resource-Id: seed-tts-2.0`，要求 Provider 返回 usage
tokens，并把唯一 request/connect ID 和 `X-Tt-Logid` 写入后期 Manifest 的 speech
attempt。真实阶段 1 启动时，worker 的 `VIDEO_SPEECH_PROVIDER_ADAPTER_URL` 必须显式
指向内部 `http://volcengine-provider:8091`；默认 Compose 仍保持 Mock 路径。

回滚只需停止阶段 1 runner 并恢复上一镜像。门禁 ledger 与 CAS 均为追加/不可变证据，
不需要数据库迁移，也不删除历史任务；任何 `AMBIGUOUS` ledger 记录必须人工恢复。
