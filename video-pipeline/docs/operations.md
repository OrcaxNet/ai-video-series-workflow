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
  → migration v5 clean
Temporal healthy
Mock Provider healthy
  → Orchestrator Worker registered
  → Control Plane ready
```

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
| `VIDEO_ARTIFACT_ROOT` | `/var/lib/video-pipeline/artifacts` | 本地 CAS |
| `ARK_API_KEY` | 未设置 | 火山方舟；只显式注入 |
| `ANTHROPIC_BASE_URL` | 未设置 | 预留 Claude adapter 的显式 endpoint；M0 不启用 |
| `ANTHROPIC_API_KEY` | 未设置 | 文本备用 Secret |
| `ANTHROPIC_MODEL` | 未设置 | 文本备用 model |
| `DOUBAO_TTS_APP_ID` | 未设置 | 豆包语音 |
| `DOUBAO_TTS_ACCESS_TOKEN` | 未设置 | 豆包语音 Secret |

禁止：

- 自动读取 `~/.claude`、shell history、浏览器或 IDE 配置；
- 将 Secret 写入 `.env.video.example`、数据库、日志、trace、error、fixture、Manifest；
- 通过 UI 返回 Secret；
- 将 signed download URL 写入 outbox/history。

生产业务 API 要求 HS256 Bearer JWT，并校验 `sub`、RBAC `role`、`aud`、`exp`；mutation body 的 `actor` 必须与签名 claims 完全一致，不能由调用方自报身份。`local/development/test` 且未配置认证 Secret 时才允许无 token 的开发模式；`VIDEO_ENVIRONMENT=production` 缺少 Secret 会启动失败。健康、系统信息和脱敏 Provider 状态保持公开。

`.env.video` 已被 `.gitignore` 忽略，且只能用于非生产本地值。生产使用 Secret manager/容器 secret injection。

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

1. 通过 Secret manager 注入 `ARK_API_KEY`；语音凭证独立注入；
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
