# FLO-154 Provider 原生音频优先后期

## 冻结策略与付费边界

新编译的视频 Prompt 固定 `audioStrategy=native_preferred`、`generateAudio=true`、
`audioDelivery=native_mix`。只有两个显式降级会触发 TTS：

- `tts_required`：兼容历史请求，对全部 cue 生成 TTS；
- `hybrid`：仅生成 `cueFallbacks` 中冻结的失败 cue，每条必须记录原因、原始
  Native Mix SHA-256 和 replacement revision。

`PrepareProviderJob` 在写预算 reservation、Provider job 和调用 Adapter 之前检查冻结的
Capability snapshot。缺少 `supportsNativeAudio=true`、真实
`nativeAudioDelivery`，或把 `native_mix` 冒充 `native_stems`，都会返回
`CAPABILITY_UNAVAILABLE`。Volcengine 与契约 Mock Adapter 都透传
`generate_audio`；Provider 返回的原始 MP4 先进入 CAS 并标记
`provider_original`，是对白、环境声、音效和音乐的不可变来源。

## 后期产物与确定性混音

每个 Provider MP4 的音轨提取为 `native_mix`，不会被标成独立 Dialogue stem。
只有实际存在 TTS fallback 时才生成 `dialogue_audio`。FFmpeg 固定执行：

1. 48 kHz stereo 重采样与轻量 -23 LUFS 镜头归一化；
2. 镜头边界 50 ms 包络，禁止对混合对白做破坏性 time-stretch；
3. hybrid fallback 仅在对应 cue 窗口进入独立 Dialogue，并对 Native bed 做固定
   sidechain ducking；
4. Program Mix 最终归一为 -16±1 LUFS、True Peak ≤ -1 dBTP；
5. 原生通过路径输出一条 Program 音轨；存在真实 fallback 时才额外交付 Dialogue 音轨。

CAS/Manifest/BOM 明确记录 Provider original、逐镜头 Native Mix、可选 Dialogue、
Final Mix、Final Video、字幕、Audio QC report、命令计划 hash、工具版本、策略、fallback、模型与费用证据。
相同冻结输入和已完成 Provider attempt 重放不会再次提交 TTS；completed-only crash replay
也必须逐条验证 Job、route、usage、零现金证据和 cue 覆盖。

## ASR、字幕、口型与环境声门禁

Worker 可通过 `VIDEO_AUDIO_ANALYZER_COMMAND` 配置本地、已冻结的分析器。命令接收
`input.json analysis.json` 两个参数；输入只含 CAS 媒体路径/hash、cue 时间窗、run
时间窗、Scene Context ambience identity/version/continuity、镜头口型要求和 FLO-104 frozen
ASR 配置，不提供参考字幕正文，避免 ASR
“照抄答案”。输出是严格 JSON，未知字段、多 JSON、来源 hash 漂移或超限大小均失败关闭；
Adapter 负责生成最终 evidence hash。

G3 前必须满足：标准 CER ≤2%、字幕边界 p95 ≤250 ms、音画起点 p95 ≤120 ms、
正面近景/特写是否需要口型由冻结的 `cinematography.lipSyncRequired` 明示，或由
front/frontal + close-up/近景/特写组合推导；分析器不得自行把它降级为 `false`。门禁要求
口型 p95 ≤80 ms、连续环境声硬静音 ≤250 ms 且响度差 ≤1 LUFS、成片
-16±1 LUFS、True Peak ≤-1 dBTP。口型失败返回精确 `blockedRunIds`，建议只重生成受影响
镜头或使用已审批 audio-driven video；不会整集重新 TTS。

`mock_only` 只验证契约、CAS、幂等和 FFmpeg 媒体结构，状态为
`AUDIO_CONTRACT_PASSED` 且保留人工门禁，不能升级为真实质量证据。真实 Provider 样片、
金标 ASR/CV 报告和实际费用必须在单独的 Secret、预算与调用次数审批后执行。

## 兼容与回滚

旧 `prompt-compiler-v2` / `control-plane-compiler-v1` 快照继续按原字段解释：`generateAudio=false` 为
`tts_required`，不会被读取路径改写。FLO-154 没有 SQL schema migration；新增字段均位于
版本化 JSON、Manifest 和 media spec 中。回滚应用版本不会删除 Provider original、Native
Mix 或 Final Mix；操作员可冻结新的 `tts_required` revision 走旧后期，但不得修改已接受的
Prompt、fallback 或质量证据。

验证命令：

```bash
go test ./...
make video-postproduction-integration-test
```

第二条需要本机 FFmpeg/FFprobe；不会调用任何外部 Provider。
