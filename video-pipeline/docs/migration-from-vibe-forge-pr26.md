# 旧跨项目 PR #26 迁移清单

`OrcaxNet/vibe-forge` PR #26 只作为迁移来源，保持不合并，不计入 FLO-108 验收。本仓库不读取、不导入、不链接其工作树、Go module、数据库或运行时资源。

## 结论

| 旧内容 | 本仓库结论 | 目标/理由 |
|---|---|---|
| SHA-256 CAS 与测试 | 迁移 | `internal/videopipeline/artifactstore`；纯领域无关、原子写、不可变 |
| 健康/Provider status 控制面 | 迁移后改写 | `internal/videopipeline/controlplane`；API 改为 `/api/v1`，Secret 名称对齐 FLO-110 |
| Temporal Workflow/Activities | 迁移后改写 | `internal/videopipeline/orchestration`；路由、预算、错误全部使用 `internal/providercontract` |
| 第二套 `internal/videopipeline/provider` | 废弃 | FLO-110 的 `internal/providercontract` 是唯一 Provider-neutral 契约 |
| HTTP Mock Adapter | 重写 | `internal/videopipeline/mockprovider`；复用 FLO-110 状态、错误、预算、GenerationRequest 和 `mock_only` |
| 42 表 PostgreSQL migration/down | 迁移后改写 | `video-pipeline/db/migrations`；金额统一为 FLO-110 的 CNY micros |
| OpenAPI/AsyncAPI | 迁移后改写 | `/api/v1`、FLO-110 小写错误码、Provider evidence/Manifest 挂载点 |
| 架构、ER、状态、ADR、追踪 | 迁移后改写 | 删除跨项目叙述，增加 FLO-110 兼容边界与本清单 |
| Docker/Compose/Make/smoke | 迁移后改写 | 独立 module/image/network/volume 名称；无 GPU、无 Key |
| 旧仓库 README、SQLite、前端与业务 API | 废弃 | 不属于 AI 视频独立仓库，未迁入 |
| CUDA、GPU、ComfyUI、Wan 权重假设 | 废弃 | 已被 FLO-124 覆盖；ComfyUI 只允许成为非默认远程 adapter |

## 兼容性约束

- 原有 `providercontract.Provider`、`GenerationRequest`、`Job`、预算、错误、Fake、Volcengine 映射和 `pending_key` 语义保留。
- 新增的 `CapabilityAlias`、`ModelSnapshot`、`BudgetReservation`、`JobRequest/JobResponse` 是控制面到 Adapter 的 additive envelope，不替代原接口。
- Secret 继续只由显式运行时环境或 Secret manager 注入；不扫描 Claude Code、用户目录或开发者配置。
- `liveConfigured=true` 仅表示检测到显式配置；连接、模型权限、能力和价格/配额证据通过前，`liveCallsEnabled` 仍为 `false`。
- Mock/Fake 证据始终是 `mock_only`，真实 Provider 证据在 Key 到位前始终是 `pending_key`。

## 独立性验证

```bash
test "$(go list -m)" = "github.com/OrcaxNet/ai-video-series-workflow"
! git grep -n "github.com/OrcaxNet/vibe-forge" -- '*.go' go.mod go.sum Dockerfile.video-pipeline video-pipeline/compose.yaml
go test -race ./...
docker compose --env-file video-pipeline/.env.video.example -f video-pipeline/compose.yaml config --quiet
make video-up
make video-smoke
make video-down
```

回滚本 PR 时只需执行 migration down、停止本仓库 Compose 并回退本仓库提交；不会触碰旧仓库、旧 PR 或任何跨项目数据。
