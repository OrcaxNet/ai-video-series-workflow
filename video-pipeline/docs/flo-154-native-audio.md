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

Worker 只接受同时配置的 `VIDEO_AUDIO_ANALYZER_COMMAND`、
`VIDEO_AUDIO_ANALYZER_ROOT`、`VIDEO_AUDIO_ANALYZER_SEAL`。启动时先校验封印、可执行文件、
模型/Tokenizer/Normalizer/VAD/人脸口部/音画同步/FFmpeg/FFprobe/许可证快照的 SHA-256，
运行时再校验执行包里的 analyzer seal，任一漂移均失败关闭。命令接收
`input.json analysis.json` 两个参数；输入只含 CAS 媒体路径/hash、cue 时间窗、run
时间窗、Scene Context ambience identity/version/continuity、镜头口型要求和 frozen
ASR 配置，不提供参考字幕正文，避免 ASR
“照抄答案”。输出是严格 JSON，未知字段、多 JSON、来源 hash 漂移或超限大小均失败关闭；
来源 hash 同时绑定 Final Video、Final Mix、逐镜 Native Mix 和可选 Dialogue，音画起点
测量必须逐镜完整覆盖并以绝对偏移参与 p95。Adapter 负责生成最终 evidence hash。

G3 前必须满足：标准 CER ≤2%、字幕边界 p95 ≤250 ms、音画起点 p95 ≤120 ms、
正面近景/特写是否需要口型由冻结的 `cinematography.lipSyncRequired` 明示，或由
front/frontal + close-up/近景/特写组合推导；分析器不得自行把它降级为 `false`。
按字幕 cue 与逐镜时间窗的最大唯一重叠冻结 cue→run，分析器输入携带该绑定与有效交集；
跨 run 重绑、required cue 遗漏、重复 cue 或超出 cue/run 交集的时间戳均失败关闭。门禁要求
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
tools/flo154-analyzer/build.sh "$PWD/artifacts/flo154-analyzer/build" \
  <本地已冻结的 faster-whisper 模型目录>

go run ./cmd/video-stage1-readiness \
  video-pipeline/config/flo154-native-stage1-readiness.json

# 先提交全部受跟踪源码，再以该 SHA 和 materializer 二进制哈希生成独立执行包。
go run ./cmd/video-stage1-materialize \
  -plan video-pipeline/config/flo154-native-stage1-readiness.json \
  -product <flo154-product.json> -source <source.json> -safety <flo154-safety.json> \
  -visual <visual.json> -analyzer-root <analyzer-build> -analyzer-seal <analyzer-build>/analyzer-seal.json \
  -code-commit <40位固定SHA> -build-sha256 <materializer二进制SHA-256> \
  -approval-comment <审批评论UUID> -approval-actor <审批者UUID> \
  -approval-valid-until <RFC3339> \
  -output <execution-package.json> -report <materialization-report.json>

# 该命令没有 Provider client；它重算代码/二进制/输入/分析器哈希，执行无参考字幕的
# 离线 fixture，并从 ledger/audit 断言 Provider/TTS/job/reservation/cost 全部为零。
go run ./cmd/video-flo154-preflight \
  -plan video-pipeline/config/flo154-native-stage1-readiness.json \
  -package <execution-package.json> -product <flo154-product.json> \
  -source <source.json> -safety <flo154-safety.json> -visual <visual.json> \
  -analyzer-root <analyzer-build> -analyzer-seal <analyzer-build>/analyzer-seal.json \
  -repo-root . -build <materializer-binary> -fixture-input <offline-fixture-input.json> \
  -output <preflight-report.json>

go test ./...
make video-postproduction-integration-test
```

两个物化/预检命令都通过 `VIDEO_POSTGRES_DSN` 指向同一隔离测试库，物化还通过
`VIDEO_ARTIFACT_ROOT` 指向隔离 CAS。分析器构建只消费调用者明确提供的本地模型快照，
不下载权重；分析器 fixture 与后期集成
测试需要本机 FFmpeg/FFprobe。上述命令均不会调用任何外部 Provider。新的真实 Provider
样片必须等待 QA 固定版本验证通过及 Deep Research 重新签发授权，旧授权不可复用。当前
native materializer 只创建 `OPEN` 的 VIDEO budget review；`PrepareProviderJob` 在任何
reservation/job/cost 写入前要求它已被单独批准，因此离线执行包本身不构成付费授权。
