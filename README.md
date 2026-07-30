# AI Video Series Workflow

面向 AI 视频剧集生产的 API-first 工程基线。剧集、单集、场景、分镜四层上下文，以及人物、场景、道具和 Prompt 资产，都以不可变 revision/snapshot 进入可追溯、可复用、可替换 Provider 的生产流程。

当前 M0 基线：

- `internal/providercontract` 是唯一 Provider-neutral 契约，覆盖文本、图像、视频和音频，保留 FLO-110 的预算、错误、Secret 和 `pending_key` 边界；
- 火山引擎优先；Claude 文本备用目前只保留显式配置的路由契约，尚未启用 live adapter；本地 GPU、模型权重和 ComfyUI 均不是运行前置；
- PostgreSQL 保存产品真相，Temporal 保存持久编排 history，SHA-256 CAS 保存不可变媒体；
- 无 Key Fake/Mock 覆盖成功、超时、401/403、429、5xx、配额、预算、内容阻断、地区/模型不可用、重复回调、取消竞态和恢复；
- OpenAPI、AsyncAPI、ER/数据字典、状态机、ADR、24 条 P0 追踪及迁移/回滚说明均已冻结。

无密钥一键启动与验证：

```bash
make video-up
make video-smoke
make video-down
```

只验证 Provider 契约和 `pending_key` 预检：

```bash
./scripts/flo110-preflight.sh
```

真实调用必须由 Secret manager 在进程启动时显式注入，并按 `video-pipeline/docs/operations.md` 完成连接、模型权限、能力/价格快照和账单对账；检测到密钥本身不会自动开启真实调用。

- FLO-110 Provider 调研与实测计划：`docs/flo-110/README.md`
- FLO-108 架构入口：`video-pipeline/README.md`
- 旧跨项目 PR #26 迁移结论：`video-pipeline/docs/migration-from-vibe-forge-pr26.md`
