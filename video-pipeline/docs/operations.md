# 运维、安全与真实 Provider 激活

## 1. 本地 M0

```bash
make video-up
make video-smoke
make video-down
```

不要求 GPU、CUDA、模型权重、ComfyUI 或模型 API Key。启动链：

```text
PostgreSQL healthy
  → migration v7 clean
Temporal healthy
Mock Provider healthy
  → Orchestrator Worker registered
  → Control Plane ready
```

### v5 → v6/v7 数据升级

`000006_generation_mainline` 创建 v6 主链路表；`000007_generation_mainline_upgrade`
专门覆盖已经应用过旧版 `000006` 的数据库。生产发布顺序：

1. 停止旧 worker 消费 task queue，控制面暂停创建新 Run；
2. 备份 PostgreSQL、Temporal 与 CAS，执行 migration 到 `000007`；
3. 让已有 ProviderJob 用旧 worker 收敛；无法收敛的 Run 显式取消；
4. 对仍需执行的镜头重新编译 Prompt，并创建新 Run，不原地改写 v5
   request snapshot；
5. 启动新 worker。它会在注册 task queue 前检查所有活跃 Run 的 Prompt 输入/
   资产谱系与 Provider reservation snapshot；任一 v5 活跃记录会使启动失败；
6. worker ready 后再开放控制面写流量。

回滚边界：若新 worker 尚未消费，可停止新 binary 并恢复旧 worker，但保留已升级
schema。若已经产生 `GENERATION_PROFILE` lineage 或一 Manifest 对应多个
publication lock，不允许执行 migration down 或让 v5 worker接管；应先停止写入、
保留数据库证据并以前向修复恢复。`000007` down 只用于没有这些数据的可丢弃测试
环境，并会在不安全时主动失败。

`golang-migrate` 会在执行 down SQL 前先把目标版本写为 dirty。因此，如果上述安全
闸门拒绝 `000007 down`，即使第一条保护检查已在任何 DDL 前失败，迁移状态也会显示
为 `6/dirty`，后续 migration 会被阻断。误触后的恢复流程如下：

1. 继续停止控制面写入和新旧 worker，不重试 down；保留失败日志与数据库备份。
2. 使用由 Secret 注入、不会打印到日志的 `VIDEO_POSTGRES_DSN` 检查状态：

   ```bash
   migrate -path ./video-pipeline/db/migrations \
     -database "$VIDEO_POSTGRES_DSN" version
   psql "$VIDEO_POSTGRES_DSN" -Atc \
     "SELECT version, dirty FROM public.schema_migrations;"
   ```

3. 只有当错误明确是
   `cannot roll back generation mainline upgrade while GENERATION_PROFILE lineage exists`
   且以下两项仍保持 v7 形态时，才能判定没有执行部分 down DDL：

   ```sql
   SELECT pg_get_constraintdef(oid)
   FROM pg_constraint
   WHERE conrelid = 'video_pipeline.prompt_snapshot_inputs'::regclass
     AND conname = 'prompt_snapshot_inputs_input_type_check';
   -- 结果必须仍包含 GENERATION_PROFILE

   SELECT COUNT(*)
   FROM pg_constraint
   WHERE conrelid = 'video_pipeline.publication_locks'::regclass
     AND conname = 'publication_locks_manifest_id_key';
   -- v7 结果必须为 0
   ```

   同时确认触发保护的 lineage/lock 数据仍存在，且表、列和约束与操作前快照一致。
4. 满足上述全部核验后，只清理迁移工具的 dirty 标记并重新验证：

   ```bash
   migrate -path ./video-pipeline/db/migrations \
     -database "$VIDEO_POSTGRES_DSN" force 7
   migrate -path ./video-pipeline/db/migrations \
     -database "$VIDEO_POSTGRES_DSN" up
   psql "$VIDEO_POSTGRES_DSN" -Atc \
     "SELECT version, dirty FROM public.schema_migrations;"
   # 必须为 7|f
   ```

若错误来自创建 `publication_locks_manifest_id_key`，或任一 v7 约束/数据快照发生
变化，说明 down 可能已部分执行：**不得 `force 7`**。保持停写，从备份恢复或使用
经审核的前向修复迁移恢复完整 v7 schema。可丢弃环境的自动化证据由
`make video-migration-v7-rollback-guard-test` 提供；它会断言保护失败产生
`6/dirty`、相关 DDL/数据未变，再执行 `force 7 → up` 并验证 `7/clean`。

## 2. 配置

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `VIDEO_CONTROL_PLANE_HTTP_ADDRESS` | `:8080` | Control Plane listen |
| `VIDEO_POSTGRES_ADDRESS` | `postgres:5432` | 仅 host:port；凭证由独立 DSN/Secret 提供 |
| `VIDEO_POSTGRES_DSN` | 未设置 | 可选的完整 DSN；生产从 Secret 注入且禁止记录 |
| `VIDEO_TEMPORAL_ADDRESS` | `temporal:7233` | Temporal |
| `VIDEO_TEMPORAL_NAMESPACE` | `default` | Workflow namespace |
| `VIDEO_TEMPORAL_TASK_QUEUE` | `video-production-v1` | 控制面与 worker 共享 queue |
| `VIDEO_AUTH_HMAC_SECRET` | 本地未设置 | 生产必填、至少 32 bytes；只从 Secret manager 注入 |
| `VIDEO_AUTH_AUDIENCE` | `video-control-plane` | JWT `aud` 精确值 |
| `VIDEO_PROVIDER_ADAPTER_URL` | `http://mock-provider:8090` | Provider-neutral adapter |
| `VIDEO_SPEECH_PROVIDER_ADAPTER_URL` | `http://mock-provider:8090` | 可与真实视频 Adapter 分离的语音 Adapter |
| `VIDEO_ARTIFACT_ROOT` | `/var/lib/video-pipeline/artifacts` | 本地 CAS |
| `ARK_API_KEY` | 未设置 | 火山方舟；只显式注入 |
| `VIDEO_PROVIDER_SERVICE_AUTH_SECRET` | 未设置 | Live Adapter 内部 HMAC 服务认证；至少 32 bytes，只通过 Secret manager 注入 |
| `VIDEO_VOLCENGINE_BASE_URL` | `https://ark.cn-beijing.volces.com/api/plan/v3` | Agent Plan 数据面；不得写入 Manifest/BOM |
| `VIDEO_VOLCENGINE_VIDEO_MODEL` | `doubao-seedance-2.0` | 冻结真实视频模型 |
| `VIDEO_VOLCENGINE_TTS_ENDPOINT` | Agent Plan HTTP TTS URL | 只接受套餐专属 V3 HTTP Chunked `/api/v3/plan/tts/unidirectional`；标准 `/api/v3/tts/...`、query、尾斜杠或 host 漂移均在网络调用前失败关闭；Resource ID 固定为 `seed-tts-2.0` |
| `VIDEO_VOLCENGINE_TTS_SPEAKER` | `zh_female_tianmeitaozi_mars_bigtts` | FLO-104 speech-v2 冻结音色；不是 Secret |
| `VIDEO_VOLCENGINE_TTS_CANARY_*` | 未设置 | 单次 TTS canary 的 job/input/cue、VOICE/license hash、AFP/现金上限完整 allowlist；必须全量配置，`MAX_CASH_MICROS=0`，且不能与旧 job reconciliation 同时启用 |
| `VIDEO_VOLCENGINE_PLAN` | `agent-plan-large` | 套餐计费模式标识 |
| `ANTHROPIC_BASE_URL` | 未设置 | 预留 Claude adapter 的显式 endpoint；M0 不启用 |
| `ANTHROPIC_API_KEY` | 未设置 | 文本备用 Secret |
| `ANTHROPIC_MODEL` | 未设置 | 文本备用 model |

禁止：

- 自动读取 `~/.claude`、shell history、浏览器或 IDE 配置；
- 将 Secret 写入 `.env.video.example`、数据库、日志、trace、error、fixture、Manifest；
- 通过 UI 返回 Secret；
- 将 signed download URL 写入 outbox/history。

生产业务 API 要求 HS256 Bearer JWT，并校验 `sub`、RBAC `role`、`aud`、`exp`；mutation body 的 `actor` 必须与签名 claims 完全一致，不能由调用方自报身份。`local/development/test` 且未配置认证 Secret 时才允许无 token 的开发模式；`VIDEO_ENVIRONMENT=production` 缺少 Secret 会启动失败。健康、系统信息和脱敏 Provider 状态保持公开。

`.env.video` 已被 `.gitignore` 忽略，且只能用于非生产本地值。生产使用 Secret manager/容器 secret injection。

### Agent Plan Large 单次探针

真实 Adapter 是 Compose 的 opt-in `live` profile；默认 `make video-up` 仍只启动
Mock，Mock/Live 证据边界不变。Adapter 同时兼容 Agent Plan 顶层
`output_url` 与平台旧式 `content.video_url`，但两者都只在内存中用于一次 GET
下载；成功响应仅返回 `cas://sha256/...`。

```bash
# ARK_API_KEY 与 VIDEO_PROVIDER_SERVICE_AUTH_SECRET 均由运行环境注入，
# 不写入 .env.video、命令输出、Manifest 或日志。
make video-live-provider-up
VIDEO_BUILD_VERSION=<fixed-sha> make video-live-probe
```

Live Adapter 不发布宿主机端口，只在 Compose `video-backplane` 内通过服务名访问。
除 `/health/live` 与 `/health/ready` 外，所有 `/v1/*` 请求必须携带 2 分钟有效、
body hash 与随机 nonce 绑定的 HMAC 服务签名；缺失、过期、篡改或重放请求会在任何
Provider submit/poll/cancel 前返回 `401 unauthenticated`。该签名 Secret 与
`ARK_API_KEY` 分离，Worker/单次探针只持有前者，只有 Adapter 持有后者。
nonce 会保留到签名时间戳加完整允许偏差的闭区间末端；客户端时钟领先时也不存在
按首次接收时间提前释放的重放窗口。

FLO-104 speech-v2 使用独立的新 VOICE 版本，不修改旧音色或旧
`speech-b3f099e406a02073ed1ab94b2fe5bd94` 记录。新 job ID 绑定 episode revision、
subtitle content hash、cue、VOICE version、route、resource 与 speaker；Adapter 还必须匹配
完整 `VIDEO_VOLCENGINE_TTS_CANARY_*` allowlist，才会执行一个 `MaxAttempts=1` 请求。
其中 parent VOICE version 不是仅做非空检查：Adapter 会在付费边界打开 allowlist hash
指向的 `audio/x-voice-profile+json` CAS descriptor，复算 digest，并精确核对 parent/child
version、license snapshot、provider/model/resource/speaker/route 与内部 MVP 标志。CAS
缺失、损坏、schema 不合法或任一谱系漂移都统一失败关闭，descriptor 本身不会发送给 Provider。
跨进程竞争先原子提交同一 job intent，只有一个进程能越过 TTS 边界；重放只读取持久记录。
HTTP status、纯数字 Provider code、脱敏后的 `X-Api-Message` 分类及 `X-Tt-Logid` 会分别保存，
未知 `55*` 只标为未分类 unavailable，不推断为 resource/speaker mismatch。

`video-live-probe` 固定为一个 1280×720、24 fps、5 秒、无同步音频的原创抽象
镜头，`maxAttempts=1`。输出目录带原子单次提交锁；即使任务失败，也不能在同一
目录重提。成功后会把 Manifest 声明的每个 CAS output 都复制到证据目录；当前包括
MP4 与 `last-frame.jpg`，并在 probe result 的 `files`/`artifacts` 和 Service BOM 的
`artifacts` 中记录文件名、角色、媒体类型、字节数与 SHA-256。
Adapter 在返回成功前使用 ffprobe 实测分辨率/FPS/时长并提交 CAS；任务 ID、
请求 ID、token 用量和套餐成本语义进入证据。Agent Plan 未返回逐任务货币价格时，
成本记录为 `subscription_included`、实际增量 `0 CNY`、
`provider_reported=false`，不得伪造按次价格。

Adapter 会先把不含 prompt/Secret/URL 的提交意图原子写入共享 CAS volume 下的
`provider-jobs/`，再调用远端；重启后以同一 `jobId` 重放已知 task/result。若进程
在远端接收与 task ID 落盘之间崩溃，记录转为需人工对账且绝不自动重提，避免重复
付费任务。回滚不涉及数据库 migration：停止 live profile，将视频路由切回 Mock，
保留 `provider-jobs/`、CAS 与证据目录用于审计；不得删除在途 task 的对账信息。

`make video-test` 和 CI 使用 `git ls-files --cached --others --exclude-standard` 枚举仓库根目录下全部 tracked 与未忽略文件，再执行 fail-closed Secret 扫描。新增根目录文件、隐藏目录或未来子目录不需要维护扫描白名单；未忽略的潜在凭证会直接使门禁失败。

## 3. 健康与就绪

| Probe | 含义 |
|---|---|
| `/health/live` | 进程 event loop 可服务 |
| `/health/ready` | PostgreSQL、Temporal、CAS、Provider Adapter 可用 |
| `/providers/status` | 每个 live capability 的 Secret/权限配置状态；无 Key 不影响 Dry-run |
| Provider `/v1/capabilities` | adapter 可服务和当前能力 snapshot |

“进程 ready”不等于“真实 provider ready”。Live capability 只有在连接测试通过、目标 model/endpoint 有权限、能力 snapshot 有效、价格/配额已知或风险已确认后才是 ready。

## 4. 容器安全

三个 Go service 镜像：

- multi-stage、`CGO_ENABLED=0`、`-trimpath`；
- Alpine runtime；基础服务仅 ca-certs/wget，Orchestrator 额外安装 FFmpeg 与 Noto CJK 字体；
- non-root `10001:10001`；
- read-only root FS；
- `/tmp` tmpfs；Orchestrator 为 45～60 秒后期处理单独限制为 1 GiB；
- `no-new-privileges`；
- drop all Linux capabilities；
- CAS volume 只挂到需要的服务。

生产建议：

- adapter 独立 service account；
- egress allowlist 到火山/明确的 Claude API host；
- callback ingress 单独 WAF/rate limit/body limit；
- PostgreSQL/Temporal/CAS 不暴露公网；
- Secret rotation 不重写历史；
- TLS、request timeout、response size 上限和 MIME allowlist。

## 5. 观测

Trace chain：

```text
series → episode → shot → run → attempt → provider_job
→ upstream request/task → artifact → QC → Gate → manifest
```

日志 allowlist：

```text
trace_id
aggregate IDs
capability alias
provider profile ID
model/endpoint ID
route/capability snapshot hash
provider request/task ID
state/error code/retry count
duration, units, amount, pricing version, verified
artifact hash (not signed URL)
```

必须脱敏：

```text
Authorization, Proxy-Authorization, API key, token, cookie
credential, signed URL query, raw provider body
novel text, full prompt, voice/reference binary
```

Metrics：

- ProviderJob queue/running/unknown/requires_action/terminal；
- submit/poll/callback/download/CAS latency；
- retry count、Retry-After、reconciliation age；
- duplicate callback、state-regression reject、cancel race；
- estimate/reservation/actual/release、unverified cost；
- per provider/model/capability success/429/5xx；
- CAS bytes/disk headroom、Temporal backlog、outbox lag；
- Activity journal incomplete age、input-hash conflict；
- QC pass、Q1/Gate cycle time、3-shot episode completion。
- post-production/TTS cue latency、FFmpeg duration、output bytes、manual timing required 与 G3 manifest binding。

Alerts：

- oldest `UNKNOWN` 超 provider SLA；
- provider 401/403、quota、content block burst；
- 429 持续、5xx circuit open；
- budget reservation leak；
- provider terminal success 无 CAS artifact；
- callback signature failure；
- Secret scanner hit；
- disk/PG/Temporal/outbox critical。

## 6. 备份与恢复

备份：

- PostgreSQL schema/data（不含可用 Secret）；
- CAS + hash inventory；
- Temporal persistence；
- Secret Store 独立备份/rotation policy。

恢复验证：

1. Restore PG/CAS/Temporal；
2. 查 `UNKNOWN/QUEUED/RUNNING` ProviderJob；
3. 对已有 CAS hash 先核验；
4. 用已保存 upstream task ID poll；
5. 对 incomplete Activity journal 使用同一 JobID reconcile；
6. 禁止批量 create replacement jobs；
7. reconcile callback receipts/cost；
8. 校验 manifests 和 revision dependencies。

RPO/RTO 需在部署环境 SLO 中冻结；M0 只保证流程可演练。

## 7. 火山 Provider 激活流程

Key 到位后不改领域/Workflow：

1. 通过 Secret manager 注入 `ARK_API_KEY`；Agent Plan TTS 复用该 Key，Resource ID 固定为 `seed-tts-2.0`；
2. 配置 allowlisted Ark/Speech base URL 与区域；
3. Provider Adapter 增量的 connection-test 只允许返回 fingerprint/masked identity；控制面当前不公开占位路由；
4. discover 实际 text/image/video/speech model/endpoint；
5. 保存 capability snapshot（ratio/duration/resolution/reference/callback/concurrency）；
6. 配置 pricing version、quota、QPM/TPM；
7. route `*.primary` 从 Mock 切到 Volcengine，新 attempt 生效；
8. 先执行无费用/最低费用连接与估算测试；
9. 单 probe shot 通过后再启用批量。

不得把 Seedance/Seedream/具体日期 model ID 写成永久产品规则。model ID 只存在配置与 attempt snapshot。

## 8. 最小真实实测清单

| 检查 | 通过条件 |
|---|---|
| Text | structured JSON + usage + request ID；解析失败只修复 1 次 |
| Image | URL/Base64 下载、MIME/尺寸/hash、reference/seed 实际能力 |
| Video | create/poll/callback/cancel、4–15s 等实际范围从 snapshot 验证 |
| Speech | voice 权限、格式、时长、sentence/word timestamp 能力 |
| Recovery | kill worker 后继续原 task，duplicate paid create = 0 |
| Errors | 401/403/429/5xx/quota/content/region/model 全部稳定分类 |
| Cost | estimate/reserve/actual/release；未知值不伪造 |
| Artifact | 临时 URL 过期后 CAS 可访问并 checksum 一致 |
| Secret | 响应/日志/trace/PG backup/Manifest/export 0 命中 |
| E2E | 1 集 ≥3 镜，TTS+SRT+VTT+CPU MP4+Manifest |

FLO-104 的成片运行、证据分层和三次真实 Key 执行单见
`docs/flo-104-postproduction.md`。真实 CER、字幕边界、音画起点、
p50/p95、成功率与费用在 Key 到位前必须保持 `pending_key`。

## 9. 已知风险

| 风险 | 当前边界 | 关闭条件 |
|---|---|---|
| 无真实 Key | M0 仅 Mock/Dry-run | 完成第 8 节 |
| 模型/区域/价格变化 | snapshot + effective time | 上线前刷新并审批 |
| callback 能力未知 | polling 为必需 fallback | 实测签名/SLA |
| 成本返回不完整 | units + unverified ledger | 账单对账 |
| 跨镜一致性 | 资产/Prompt/尾帧版本化 + Q1 | 真实 3 镜质量验收 |
| 本地磁盘 | stream + CAS + headroom alert | 容量/GC/恢复演练 |
| 可选 adapter 风格漂移 | 禁止自动跨供应商 | 人工新 attempt |
