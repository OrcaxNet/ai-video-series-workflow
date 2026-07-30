# 场记：AI 剧集创作操作台

`web/` 是 FLO-101 的桌面端优先 React/TypeScript PoC。它以 `video-pipeline/contracts/openapi.yaml` 为 API 真相源，展示内容、资产、剧本、分镜、Provider 任务、三道人工闸门、版本差异与生成谱系。

默认启动在无 Key 的 `mock_only` 模式。Mock 用于验证交互、状态机、幂等、异常映射和闸门顺序，不代表真实模型质量、时延、成功率或费用。

## 运行

要求 Node.js 22+。不需要 GPU、模型权重、ComfyUI 或模型 API Key。

```bash
cd web
npm ci
npm run dev
```

浏览器打开 `http://127.0.0.1:4173`。

验证：

```bash
npm run build
npm run test:unit
npx playwright install chromium
npm run test:e2e
```

如已在 `localhost:18080` 启动控制面，可使用同源开发代理读取真实 `/api/v1/providers/status`：

```bash
VITE_STUDIO_MODE=live npm run dev
```

当前后端只运行 system/provider discovery 子集；其余 mutation 仍按冻结 OpenAPI 作为增量实施边界。界面不会把 contract-only 操作显示为已经由真实控制面完成。

## 页面

| 页面 | 用户工作 | 主要真相 |
|---|---|---|
| 片场总览 | 查看本集进度与下一步 | Gate / Job projection |
| 内容与资产 | 选择资产 revision、权利核验、G1 | assets / asset versions / approval bindings |
| 剧本与分镜 | 镜头拆分、四层上下文、G2 | script / storyboard / effective context / prompt snapshot |
| 任务中心 | submit/poll/webhook/cancel/retry/timeout 与预算 | ProviderJob / Operation / cost ledger |
| 成片审核 | 画面、字幕、音轨、AI 标识、G3 | episode cut / QC / Manifest |
| 版本与谱系 | diff、选择、回滚和上下游影响 | immutable revisions / dependencies / Manifest |

## 交互不变量

- G1 未批准时 G2 不可审核；G2 与全部镜头未完成时 G3 不可审核。
- 批准、退回和重生成都绑定精确 revision/hash；重生成只新增 revision。
- Mock API 使用 `Idempotency-Key` 语义；同键同请求 replay，同键不同请求返回 409。
- 审核携带 ETag；`REVISION_CONFLICT` 通过冻结 Error schema 的
  `affectedObjects[].currentRevision` 对账，不调用未定义的 Gate 查询路由。
- 资产引用切换会创建新的不可变 G1/asset-set snapshot，使 G1 重新待审并阻断
  G2/G3；同一 G2 revision 不能批准 stale 镜头。
- 基础设施重试沿用同一 Provider Job；`FAILED` / `CANCELLED` 的创作重做通过
  `POST /api/v1/provider-jobs` 创建递增 attempt 和全新 Job ID。旧终态行、时间、错误与
  费用证据只读保留，并显示新旧替代关系。
- `SUCCEEDED`、`FAILED`、`CANCELLED` 是单调终态；晚到 callback、异常注入和批次
  完成均不得覆盖；批次完成和 G3 只计算每个镜头标记为当前的 attempt。
- Prompt“回滚”只切换下一次使用的 revision，不删除历史产物、费用或 Manifest。

## Secret 与权限边界

- API client 使用 `credentials: "omit"`，不读取或写入浏览器持久化存储。
- UI 只展示 provider profile、capability alias、model/route snapshot、task/request ID、trace、用量、费用及 hash。
- Key、Authorization、cookie、原始 provider error body 和临时签名地址不进入 fixture、组件状态、错误、截图或 Manifest。
- `pending_key`、`mock_only` 与 `live_provider_call` 是互斥证据级别；费用另有 `verified` 标志。

详细状态映射和回滚说明见 `video-pipeline/docs/ui-state-boundary.md`。
