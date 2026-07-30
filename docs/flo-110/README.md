# FLO-110 火山引擎模型 API 预检与无密钥开发基线

资料核验日期：2026-07-30。当前证据状态：`official_docs_pending_key` + `mock_only`。

本基线只走云端 API，不依赖本地 GPU、ComfyUI 或模型权重。由于当前未提供火山方舟 API Key，本提交没有发起真实请求，也不声称账号内模型、地域、额度、限流、账单或生成质量已验证。密钥到位后的唯一真实证据必须来自 `live-auth` 预检和受预算保护的 15 镜头计划。

## 1. 候选模型与 API 矩阵

| 类别 | 首选候选 | 输入 → 输出 | 调用形态 | 当前公开能力 | 区域/额度/限流 | 公开结算价（人民币） | 当前结论 |
|---|---|---|---|---|---|---|---|
| 剧本、上下文压缩、分镜提示词 | Doubao Seed 2.1 Pro；吞吐优先可切 Turbo | 文本/图片/音频/视频 → 文本、工具调用 | Ark Responses API，`POST /api/v3/responses`；同步响应，可按官方接口启用流式 | 多模态理解、结构化生成和工具调用 | 基线端点为 `cn-beijing`；账号可见的完整模型 ID、RPM/TPM、免费额度均为 `pending_key`，不填写猜测值；统一处理 429 | Pro：输入 6 元/百万 token、输出 30 元/百万 token；Turbo：输入 3、输出 15 | API 契约可实现，账号可用版本待预检 |
| 人物、场景、道具原画 | Doubao Seedream 5.0 Lite；回退 4.5/4.0 | 文本 + 单/多张参考图 → 单/多张图片 | Ark Image Generations，`POST /api/v3/images/generations`；同步/流式参数以具体模型版本为准 | 文生图、编辑、参考图、多图输入/输出、组图中的角色与风格连续性 | `cn-beijing` 基线；账号模型 ID、并发和日配额为 `pending_key`；处理 `QuotaExceeded` 与内容拦截 | 5.0 Lite：0.22 元/张；4.5：0.25 元/张；4.0：0.20 元/张 | 资产生产能力原生；一致性质量待受控样本验证 |
| 分镜视频 | Doubao Seedance 2.0；成本回退 Fast/Mini | 文本/图片/音频/视频参考 → 视频，可返回尾帧 | 异步任务：`POST /api/v3/contents/generations/tasks`，GET 轮询、回调、DELETE 取消 | 480p/720p/1080p、4–15 秒、多模态参考、视频续写、可返回 PNG 尾帧；任务状态含 queued/running/succeeded/failed/cancelled | `cn-beijing` 基线；真实模型 ID、并发、token 公式及账号额度为 `pending_key`；官方取消只保证 queued 任务 | 2.0：有视频输入 28 元/百万 token，无视频输入 46；Fast 22/37；Mini 14/23 | PoC 规格原生覆盖；质量、延迟、成功率和真实成本待 15 镜头验证 |
| 旁白与对白 | 火山语音合成（独立语音产品） | 文本 → PCM/WAV/MP3/Opus 音频 | 短文本在线接口；长文本最多 10 万字符并异步生成 | 声线、语速、音高、情感参数；在线单次文本上限 1024 bytes（文档约 300 汉字） | 语音产品的 AppID/集群/凭证与 Ark Key 分离；地域、并发、音色授权和额度为 `pending_key` | 标准 TTS 5 元/万字符；复刻音色 8 元/万字符 | 需单独接入 Provider；音轨与视频合成属于后处理 |

公开产品页上的价格可能随合同、活动和模型版本变化。执行真实计划前必须以账号控制台与当日价格重新确认，并把确认时间和模型完整 ID 写入 Generation Manifest。

## 2. PoC 目标规格分类

| 要求 | 分类 | 约束 |
|---|---|---|
| 16:9 | Provider 原生 | Seedance 任务 prompt 参数映射为 `--ratio 16:9`；结果仍需读取返回元数据校验 |
| 720p | Provider 原生 | 映射为 `--resolution 720p`；精确 1280×720 在下载后校验，不满足则失败而非静默拉伸 |
| 24 fps | Provider 原生输出、不可由当前任务参数强制 | 官方技术资料说明主流 Seedance 可直出 720p/1080p、24 FPS；Manifest 必须记录实际 fps |
| 单镜头 4–6 秒 | Provider 原生 | Seedance 2.0 支持 4–15 秒；基线固定 5 秒，允许测试边界 4–6 秒 |
| 参考图驱动 I2V | Provider 原生 | `content` 中传 `image_url` 和角色；资产必须引用不可变 revision、hash 与授权记录 |
| 角色/场景/道具连续性 | Provider 原生能力 + 编排策略，效果 `pending_key` | 使用多模态参考、固定 prompt snapshot、上一镜尾帧与资产 revision；不能把“支持参考图”误写成“质量已达标” |
| 精确编码、字幕、旁白混音、镜头拼接 | 后处理 | Provider 输出下载后由媒体流水线校验/转码/烧录/混音；不塞入领域对象或 Provider 专有字段 |

## 3. Provider-neutral 契约

`internal/providercontract` 是与 FLO-108 对齐的隔离接缝，不修改 Series/Episode/Scene/Shot 的领域模型：

- `GenerationRequest` 只接受请求 ID、幂等键、模态、冻结的 prompt/context snapshot、不可变资产 revision、期望输出和预算信封。
- `Provider` 统一为 `Discover / Submit / Poll / Cancel`。同步文本/图片响应也归一成终态 `Job`，异步视频保留 provider job/request/model 证据。
- `Job.Output` 记录输出资产和用量；落库前应下载临时 URL、计算 SHA-256，并以真实 hash 替换 `pending_download`。
- 供应商字段只存在于 `volcengine.go` 映射层。业务表只保存 provider-neutral ID 和 manifest 引用。
- TTS 是同一接口的 `audio` 模态，但因它属于独立语音产品，本 PR 不把 Ark Key 误用于语音接口；后续实现独立适配器。

### 生命周期与一致性规则

1. 编排器先以 `(provider, idempotency_key)` 建唯一记录，再提交 Provider。火山 API 未在本次公开资料中确认跨请求幂等头，因此网络超时后的自动重提必须先查本地记录/Provider 任务，不能盲目双发。
2. `queued → running → succeeded|failed|cancelled` 只允许单调转换。终态不可由晚到回调回退。
3. 回调以持久化 `event_id` 去重；当前公开资料没有提供可依赖的回调签名契约，因此回调只作为唤醒信号，GET 轮询结果才是权威状态。
4. 401/403 不重试；429 rate-limit 遵守 Retry-After 并受最大尝试数约束；quota/content/model/region 错误不自动重试；5xx/timeout 只对可证明幂等的操作退避重试。
5. 取消与完成竞争时，Provider 已完成的终态优先；官方视频取消限制在 queued 任务。
6. 原始 Provider 错误体不进入日志、API 响应或 manifest。仅保留标准错误码、安全摘要、HTTP 状态和 provider request ID。

## 4. 无密钥 Mock/Fake 覆盖

`FakeProvider` 是确定性、并发安全的测试替身，证据标签固定为 `mock_only`。它覆盖：

- 成功、幂等重放；
- 401、403、429、5xx、timeout、quota、content block；
- 重复回调去重；
- queued 取消与“完成/取消”竞争；
- Provider 已接收但客户端收到 5xx 的不确定提交恢复。

这些测试只证明本地控制流和错误归一化，不证明火山服务可用、性能达标或画质达标。

## 5. 密钥到位后的实测计划

`live-test-plan.json` 固定 3 类×5 镜头：

- `character_dialogue`：人物面部、服装、对白动作连续性；
- `action_continuity`：行走、取物、交接、开门和上一镜尾帧衔接；
- `scene_prop_continuity`：场景布局、空间方向、唯一道具形状和数量。

全部镜头固定 16:9、1280×720 目标、24 fps、5 秒、最多 2 次尝试，并引用资产 revision。必须记录 cold/hot latency、成功率、重试数、人工质量评分、usage token、微元成本和 manifest hash。

以公开的 Seedance 2.0“无视频输入”上限价 46 元/百万 video token 做保守预算：

```text
每次上限 = 500,000 token × 46 / 1,000,000 = 23 CNY
每镜头上限 = 23 × 2 attempts = 46 CNY
15 镜头上限 = 690 CNY
软阈值 = 560 CNY；硬阈值 = 700 CNY
```

达到软阈值停止新批次并人工复核；任何会越过硬阈值的提交在本地拒绝。模型实际 token 计算、账户折扣、失败任务计费与参考视频输入分类均需在首个真实请求后校准。

### 一键预检

无密钥模式不会访问网络：

```bash
./scripts/flo110-preflight.sh
```

输出依次区分 `static_scan`、`plan_only_pending_key`、`mock_only` 和最终的 `live_provider_call: pending_key`。

密钥由 CI/容器 Secret 在进程启动时注入，另注入账号控制台显示的完整 `ARK_LLM_MODEL`。显式设置 `FLO110_LIVE=1` 后，同一脚本才会执行一次最小、可能计费的 Responses API 请求：

```bash
FLO110_LIVE=1 ./scripts/flo110-preflight.sh
```

`live-auth` 输出仅包含 Provider、模型、job/request ID、状态与 usage，不输出密钥或 prompt。视频 15 镜头批次还需 FLO-108 编排器提供已授权、可访问的资产 URL，并在执行前再次确认 700 元硬预算。

## 6. Claude Code 与运行时密钥边界

- 已配置的 Claude Code/Anthropic 凭证不读取、不导出、不复用为火山凭证。现有后端的 `ANTHROPIC_API_KEY` / `ANTHROPIC_AUTH_TOKEN` 仍只服务现有 Anthropic SDK 客户端。
- 火山适配器只从进程运行时接收 `ARK_API_KEY` 和账号内完整模型 ID；不写 `.env`、数据库、fixture、日志、trace、issue 或 PR。
- 语音凭证必须由独立 Secret 引用注入，不能假设 `ARK_API_KEY` 同时授权语音产品。
- CI 先执行仓库静态密钥扫描，再执行测试；Provider 原始响应最多读取 2 MiB，错误体在适配层丢弃。
- 开发者若只拥有 Claude Code 会话而没有火山密钥，正确结果就是 `pending_key`，不得借助本机配置探测或复制凭证。

## 7. 安全、版权与使用条款

- 所有输入资产必须带授权记录；人物肖像应使用平台可信肖像/授权机制并保留校验结果。
- 生成内容按平台要求添加 AI 标识。平台素材的免费/付费可商用范围以具体素材标签为准，不能由技术基线概括授权。
- 面向公众提供生成式服务前，内容审核、算法备案/安全评估等义务需要法务和合规确认。
- watermark 保持开启；关闭只能作为显式、审计过的产品决策。
- 内容拦截视为终态业务错误，不用改写规避审核，也不把被拦截 prompt 写入普通日志。

## 8. 风险与待验证项

| 风险 | 当前控制 | 解锁条件 |
|---|---|---|
| 账号看不到候选模型或模型 ID 漂移 | 模型 ID 仅运行时注入，不提交占位 ID | API Key 到位后执行 `live-auth` 并记录完整 ID |
| 官方未公开统一 RPM/TPM | 429 归一化、退避、并发上限外置 | 查询账号控制台/配额接口并落配置 |
| Seedance token 计算/失败计费不确定 | 23 元/次保守上限，硬预算拦截 | 首批真实 usage 与账单对账 |
| 参考图能用但角色连续性不足 | 15 镜头分层样本、冻结资产/prompt/尾帧 | 人工评分阈值与真实结果评审 |
| 临时输出 URL 过期 | 终态后立即下载、hash、转存 | FLO-108 资产存储接入 |
| 回调来源真实性未知 | 回调仅唤醒，轮询为权威 | 官方签名文档或网关级鉴权确定 |
| TTS 凭证/地域与 Ark 分离 | 保持独立 audio adapter seam | 语音产品配置与授权到位 |
| 价格或条款更新 | 文档带核验日期，执行前二次确认 | 真实批次审批 |

此前基于本地 GPU/ComfyUI 的实现已关闭且不作为本基线依赖；可复用的只有“不可变资产 revision、prompt snapshot、Generation Manifest、尾帧衔接”这些 Provider-neutral 概念。

## 9. 官方资料

- [火山方舟产品与当前模型价格](https://www.volcengine.com/product/ark)
- [Seedance 2.0 规格与套餐](https://www.volcengine.com/activity/seedance2)
- [Ark Responses API 快速开始](https://www.volcengine.com/docs/82379/1795150)
- [Ark Responses API 工具调用](https://www.volcengine.com/docs/82379/1958524?lang=zh)
- [视频生成：创建任务 API](https://api.volcengine.com/api-docs/view?action=CreateContentsGenerationsTasks&serviceCode=ark&version=2024-01-01)
- [视频生成：查询任务 API](https://api.volcengine.com/api-docs/view?action=GetContentsGenerationsTask&serviceCode=ark&version=2024-01-01)
- [图片生成 API](https://api.volcengine.com/api-docs/view?action=ImageGenerations&serviceCode=ark&version=2024-01-01)
- [Seedream 5.0 Lite / 4.5 / 4.0 能力说明](https://www.volcengine.com/docs/82379/1829186)
- [Seedance 2.0 API 上线说明](https://developer.volcengine.com/articles/7628567056649125942)
- [Seedance 原生 720p/1080p、24 FPS 技术说明](https://developer.volcengine.com/articles/7611443299467722779)
- [语音合成 API](https://www.volcengine.com/docs/6561/79817?lang=zh)
- [语音技术服务 SLA](https://www.volcengine.com/docs/6561/107349?lang=zh)
- [素材版权与 AI 标识要求](https://www.volcengine.com/docs/82379/2525200?lang=zh)
- [模型服务协议与内容安全义务](https://www.volcengine.com/docs/82379/1142195)
- [可信人物素材与肖像授权](https://www.volcengine.com/docs/82379/2315856?lang=en)
