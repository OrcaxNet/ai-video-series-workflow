# FLO-104 音频、字幕与确定性成片

## 1. 交付边界

本切片在每个镜头均完成结构 QC 和 Q1 后、创建 G3 前，新增一个持久的单集后期 Activity：

```text
G1/G2 approved inputs
  → exact successful shot runs
  → immutable prompt/context/video hashes
  → provider-neutral speech.primary jobs
  → immutable UTF-8 SRT revision
  → deterministic CPU FFmpeg
  → MP4 + external SRT + independent dialogue WAV
  → post-production Manifest + Service BOM in CAS
  → G3 bound to post-production manifest hash
```

生成模型仍全部位于 Provider Adapter 后面。本地只执行确定性媒体处理，不需要 GPU、模型权重或 ComfyUI。真实凭证只能由运行时 Secret 注入；请求、Temporal history、数据库、日志、Manifest、测试和证据包均不保存凭证。

## 2. 启动生产请求

`POST /api/v1/episodes/{episodeRevisionId}/production-runs` 的
`postProduction` 为可选增量。启用时必须冻结独立的 `speech.primary`
路由、预算审批和证据等级：

```json
{
  "postProduction": {
    "enabled": true,
    "evidence": "mock_only",
    "speechRouteSnapshot": {
      "capabilityAlias": "speech.primary",
      "provider": "mock",
      "modelId": "fixture-speech-v1",
      "routeVersion": "route-v1",
      "capabilityHash": "64-lowercase-hex",
      "verification": "mock_only"
    },
    "speechBudgetApprovalId": "00000000-0000-4000-8000-000000000000",
    "speechBudgetLimit": {
      "amountMicros": 1000000,
      "currency": "CNY"
    },
    "subtitleLanguage": "zh-CN",
    "burnSubtitles": true,
    "enforcePoCDuration": true
  }
}
```

`enforcePoCDuration=true` 会在任何语音付费提交前拒绝总镜头时长不在
45～60 秒的单集。可选背景音必须引用已有、获许可且带
`background_audio` 产物的 AssetVersion；不接收临时 URL 或未追踪的本地文件。
后端还会交叉检查证据等级和 ProviderProfile mode：`mock_only` 只能使用
`MOCK`，`live_provider_call` 只能使用 `LIVE/READY`，`pending_key` 只能使用
`DRY_RUN/LIVE`，因此 Mock 结果不能通过请求字段被提升为真实证据。

### 对白来源

每个不可变 `shot_spec_revision.narrative` 可包含相对该镜头起点的对白：

```json
{
  "dialogue": [
    {
      "id": "cue-001",
      "speaker": "角色稳定 ID 或显示名",
      "text": "对白正文",
      "voiceRef": "已审批 voice asset/profile 引用",
      "startMillis": 500,
      "endMillis": 2400
    }
  ]
}
```

准备 Activity 按固定镜头顺序转换为单集绝对时间轴，并从内容生成
`SubtitleRevision.contentHash`。编辑字幕必须创建子 revision；已批准的父
revision 不会被覆盖。SRT 使用 UTF-8、LF 和无 BOM 的规范输出。
`live_provider_call` 的每条对白都必须将 `voiceRef` 设为该镜头已绑定的
VOICE/AUDIO AssetVersion UUID；后端重新检查系列归属、审批、许可和可选
Consent。`mock_only` 可省略 `voiceRef` 并使用明确标注的合成 tone。

## 3. 幂等、恢复与门禁

- 语音 JobID 由 episode revision、subtitle hash 和 cue ID 确定，相同输入重放使用相同 idempotency key。
- Provider submit/poll/cancel 复用统一 Job 契约；超时或 Worker 取消会尝试补偿 cancel。
- Temporal Activity journal 只在 CAS 产物和 PostgreSQL 谱系事务提交后标记完成。Activity 重放读取已提交结果，不重复生成最终资产。
- 提交事务重新检查每个 Run 仍为成功且 QC 通过，避免准备后状态漂移。
- G3 只在后期 Manifest 已关联到所有贡献 Run 后创建。缺失 G1/G2、许可、预算、路由、成功镜头、QC 或 Manifest 均 fail closed。
- `evidence=pending_key` 在语音 Provider 边界之前返回不可重试的
  `PENDING_KEY`，不会伪造样片、质量或费用。
- 结构 QC 只验证容器、720p/24fps、时长和轨道；它不会冒充 CER 或时间边界测量，因此始终保留 `manualTimingRequired=true`，直到真实金标报告满足门槛。

单镜重新生成仍沿用现有 Run/Attempt/Impact 机制。新的镜头、Prompt、
上下文、授权或字幕 revision 会形成新的 hash；旧的 CAS 产物和已批准
revision 保持不可变，受 stale 影响的内容不能进入新的 G3。

## 4. FFmpeg 输出

默认成片规格为 1280×720、16:9、24fps、H.264/AAC、48 kHz stereo：

- 每个视频先缩放/补边、定帧率、裁剪到冻结时长并加无声轨；
- 按镜头顺序 concat；
- 每段 TTS 根据 cue 起点 `adelay`，统一 resample/loudness 后混音；
- 可选获许可背景音以固定增益混入；
- 可选烧录字幕，同时始终交付外挂 SRT；
- MP4 包含 Program Mix 和独立 Dialogue 两条音轨，以及 AI 内容标记；
- 命令不经过 shell，文本不插入 FFmpeg 参数；
- 命令计划、FFmpeg/FFprobe 版本、输入/输出 SHA-256 写入 Manifest 和 Service BOM。

Orchestrator 镜像单独安装 FFmpeg 与 Noto CJK 字体。`/tmp` 是有界
1 GiB tmpfs，用于 45～60 秒的规范化片段、PCM 和最终 MP4；CAS 仍是唯一持久产物目录。

## 5. Mock 与真实证据

证据等级不能相互升级：

| 能力 | `mock_only` | `live_provider_call` | 当前状态 |
|---|---|---|---|
| submit/poll/cancel、幂等与预算边界 | 固定 Provider fixture | 真实 Provider request/task ID | Mock 已自动化；Live `pending_key` |
| MP4/SRT/对白轨/Manifest/BOM | 确定性 fixture | 真实生成输入的确定性后期 | Mock 已实现；3 次 Live `pending_key` |
| CER、字幕边界、音画起点 | 不宣称 | 金标实测 3 次完整统计 | `pending_key` |
| p50/p95、成功率、错误、重试 | 仅测试场景 | 真实队列/轮询/回调/端到端 | `pending_key` |
| 用量与实际费用 | `null`/不适用 | Provider 返回和账单对账 | `pending_key` |

契约 Mock 的媒体 artifact 保存的是确定性 JSON，而不是伪装成真实模型输出的
媒体字节。`mock_only` FFmpeg 计划从这些 artifact hash 派生固定 color/sine
测试源，同时保留原 Provider job/artifact 谱系；`live_provider_call` 则必须
解码 Provider 实际媒体，不能走该降级分支。

生成三套 48 秒、720p、24fps 的独立 `mock_only` 媒体证据包：

```bash
make video-flo104-mock-evidence
```

输出位于被 Git 忽略的 `artifacts/flo104-mock/mock-run-{1,2,3}`，每套包含
MP4、UTF-8 SRT、独立对白 WAV、输入、fixture 审批、结构 QC、Manifest、
Service BOM、usage/cost 边界、FFprobe 规格和 `SHA256SUMS`。该脚本不调用
Provider，也不代表真实模型质量。运行环境包含 libass 时同时烧录字幕；
否则明确记录 `burnSubtitles=false`，外挂 SRT 仍始终交付。

验证真实 FFmpeg 的 CAS 和重复执行稳定性：

```bash
make video-postproduction-integration-test
```

宿主 FFmpeg 若不含可选 libass，集成测试仍验证外挂 SRT；生产镜像包含
libass 依赖并执行烧录路径。命令计划的烧录分支由单元测试固定。

## 6. 真实 Key 到位后的执行单

1. 仅由 Secret manager 注入语音/视频凭证，完成脱敏 connection test。
2. 冻结实际地区、endpoint、model/version、能力、并发、价格和配额 snapshot。
3. 以 `live_provider_call` 运行同一代表性授权输入三次，不挑选最好一次。
4. 每次导出 MP4、SRT、Dialogue、输入配置、日志、G1/G2/G3、QC、Manifest、Service BOM、usage/cost 和复现说明。
5. 汇总 Provider queue/poll/callback/generation/E2E p50/p95、成功率、错误分类、重试、存储量与人工分钟。
6. 使用金标测量 CER ≤2%、字幕边界 p95 ≤250 ms、音画起点 p95 ≤120 ms；不达标则通过新的字幕 revision 人工校时并重跑后期，原 revision 不变。
7. 演练 401/403、429、5xx、timeout、quota、content/region/model 阻断、响应丢失、乱序/重复 callback、取消竞态和 Worker 重启；确认重复付费任务和重复最终资产均为 0。

三次真实运行与完整统计完成前，FLO-104 不能置为 `done`。

## 7. 数据库与回滚

本切片不新增 migration。它复用现有 `artifacts`、`run_artifacts`、
`generation_manifests`、`provider_jobs`、`cost_records`、`approvals`、
`audit_events` 和 `outbox_events`，以新的 `media_spec.kind` 和事件类型保存后期产物。

回滚顺序：

1. 新请求省略 `postProduction` 或设 `enabled=false`；
2. 部署上一版 Control Plane/Worker；
3. 不删除 CAS、审计、Provider job、费用或已提交 Manifest；
4. 未完成的 Activity 由 Temporal 保留，可在兼容 Worker 恢复或显式取消。

回滚不需要 down migration，也不会覆盖或删除历史 revision。已知风险是
FFmpeg/libass 版本差异、真实 TTS 时间戳与费用能力未知，以及当前真实 Key
缺失；这些都必须通过固定镜像 digest 和第 6 节实测关闭。
