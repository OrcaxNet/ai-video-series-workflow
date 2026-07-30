# AI Video Series Workflow

面向 AI 视频剧集生产的 API-first 工程基线。目标是把剧集、单集、场景和分镜上下文，以及人物、场景、道具、Prompt 等资产，组织成可复用、可追溯、可替换模型供应商的生产流程。

当前 FLO-110 基线以火山引擎 API 为首选：

- Provider-neutral 生成请求、任务、能力、错误和预算契约；
- Ark Responses、Image Generations 与异步 Video Generations 适配；
- 无密钥 Fake Provider、固定夹具和完整故障路径；
- 运行时 Secret 边界、脱敏和静态凭据扫描；
- 3 类 × 5 镜头的 `pending_key` 实测计划；
- LLM、图像、视频、TTS 能力/费用/安全/许可报告。

无密钥环境执行：

```bash
./scripts/flo110-preflight.sh
```

真实调用必须由 Secret 管理器在进程运行时注入配置，并显式开启 `FLO110_LIVE=1`。没有火山 API Key 时，正确结果始终是 `pending_key`。

详细设计、官方来源、成本模型和风险见 `docs/flo-110/README.md`。
