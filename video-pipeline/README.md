# AI 视频剧集 API-first 骨架

这是 FLO-108 的隔离后端切片，按 FLO-124 的最新约束实现：本地没有 GPU，也不运行任何生成式模型。文本、图片、视频和语音通过统一 Provider Adapter 调用远程 API；火山引擎是首选路由，但不是领域层硬依赖。当前无真实 Key 时使用 Dry-run 和固定响应 Mock Provider。

当前可运行：

- PostgreSQL 控制面真相、Temporal 持久编排、内容寻址产物存储；
- 四类能力别名：`text.primary`、`image.primary`、`video.primary`、`speech.primary`；
- 无密钥状态发现、确定性估算/任务、幂等 replay、polling、取消竞态与回调去重；
- Episode Workflow：G2 输入校验、逐镜头远程任务、结构 QC、人工复核、G3 signal；
- OpenAPI、AsyncAPI、数据模型、状态机、ADR、FR-P0-01～24 追踪；
- 无 GPU/无模型 Key 的 Compose、smoke 与 CI。

当前不声称已实现真实火山调用、前端页面或生成质量。`mock-provider` 只生成确定性 fixture 产物；真实 Key 到位后，由火山 Provider Adapter 实现相同领域契约并完成文末实测清单。

## 一键启动

要求：Docker Compose、Go 1.26。无需 GPU、模型权重、ComfyUI 或模型 API Key。

```bash
make video-up
make video-smoke
```

停止服务并保留本地数据卷：

```bash
make video-down
```

服务：

| 地址 | 用途 |
|---|---|
| `http://localhost:18080/health/ready` | PostgreSQL、Temporal、CAS、Provider Adapter 联合就绪 |
| `http://localhost:18080/api/v1/system/info` | 纯远程生成基线 |
| `http://localhost:18080/api/v1/providers/status` | 不泄密的四类能力配置状态 |
| `http://localhost:8090/v1/capabilities` | Mock 能力快照 |
| `http://localhost:8090/v1/jobs` | Mock Provider 任务协议 |
| `localhost:7233` | Temporal gRPC |
| `localhost:55432` | PostgreSQL |

启用 Temporal UI：

```bash
make video-up-tools
```

## 验证

```bash
make video-test
make video-secret-scan
go test ./...
go vet ./...
```

`make video-smoke` 验证：

1. 无 Key 时四类 live capability 均为 `liveConfigured=false`，Dry-run/Mock 可用；
2. 相同 JobID/输入只得到一个上游任务；
3. Mock 任务经 polling 归档到 `cas://sha256/...`；
4. PostgreSQL migration clean 且 Provider/成本表存在；
5. Temporal Workflow 提交 Provider job、通过结构 QC，并在 G3 signal 后进入 `LOCKED`。

## 目录

```text
cmd/
  video-control-plane/          健康、无 Key 与系统发现
  video-mock-provider/          四能力固定响应 Provider fixture
  video-orchestrator-worker/    Temporal Workflow/Activities
internal/videopipeline/
  artifactstore/                SHA-256 CAS 与原子提交
  controlplane/                 隔离 HTTP surface
  mockprovider/                 场景注入、异步任务、callback/cancel
  orchestration/                持久流程、预算确认、provider reconciliation
  runtimeconfig/                仅显式环境变量配置
internal/providercontract/      FLO-110 供应商中立契约、预算、错误和路由快照
video-pipeline/
  config/default.yaml           路由、重试、预算、Secret 与 Mock 策略
  contracts/                    OpenAPI / AsyncAPI
  db/migrations/                `video_pipeline` PostgreSQL schema
  docs/                         架构、状态、ER、ADR、追踪和运维
  scripts/smoke.sh              无 GPU/无 Key E2E
```

## Secret 边界

项目不扫描或复制 Claude Code 配置文件。真实凭证只允许通过运行时环境变量或 Secret Store 引用显式注入：

| 能力 | 显式引用 |
|---|---|
| 火山方舟文本/图片/视频 | `ARK_API_KEY` |
| Claude 文本备用（预留；实现时必须使用官方 Anthropic Go SDK） | `ANTHROPIC_BASE_URL`、`ANTHROPIC_API_KEY` 或 `ANTHROPIC_AUTH_TOKEN`、`ANTHROPIC_MODEL` |
| 豆包语音 | `DOUBAO_TTS_APP_ID`、`DOUBAO_TTS_ACCESS_TOKEN` |

当前 Compose 不传入这些变量，也不启用任何 live adapter；Claude adapter 在 M0 只保留路由与 Secret 契约。前端、数据库、日志、trace、错误、fixture 与 Manifest 只保存 provider profile ID、不可逆凭证指纹、model/endpoint 快照、request/task ID、用量/费用和输入输出 hash，禁止保存 Authorization、Key、token、cookie 或临时签名 URL。

## 真实火山 Key 到位后的最小实测

1. 连接测试：区域、模型/Endpoint 权限、掩码身份和凭证指纹；
2. 能力探测：四类实际 model/endpoint、比例、时长、分辨率、参考图/尾帧、并发和 callback/polling；
3. 错误映射：401/403、429 + `Retry-After`、余额/配额、内容安全、地区/模型不可用、5xx；
4. 视频恢复：保存 upstream task ID，杀进程后继续 poll，确认 0 次重复创建；
5. 产物归档：临时 URL 过期后 CAS 仍可访问，MIME/尺寸/时长/checksum 正确；
6. 成本：价格规则版本、预估上界预占、实际用量/费用和失败/取消费用；
7. Secret 扫描：HTTP 响应、日志、trace、数据库备份、Manifest 和导出包 0 明文命中；
8. 至少 1 集 3 镜头完成视频、TTS、SRT/VTT、CPU FFmpeg 拼接和可追溯 Manifest。

ComfyUI 仅允许以后作为一个非默认、远程部署的 Provider Adapter；它不能成为本地启动或生产运行前置。
