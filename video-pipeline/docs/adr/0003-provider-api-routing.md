# ADR-0003：能力别名驱动的纯 Provider API 路由

- 状态：Accepted
- 日期：2026-07-30
- 关联：FLO-124、FR-P0-03～04、07、12、17、24

## 背景

运行环境无 GPU，火山 Key 尚未提供，实际模型/Endpoint/限制会变化。将 Wan、Seedance 或某日期型号写入业务对象会导致供应商替换和模型升级扩散到整个工作流。

## 决策

1. 本地不执行生成式推理；
2. 领域只引用 `text/image/video/speech.primary`；
3. Router 将 alias 解析为不可变 provider/model/endpoint/capability snapshot；
4. 火山引擎为默认优先路由；Claude 备用使用官方 Anthropic SDK，且只允许显式文本配置；
5. 无 Key 时 live disabled，Dry-run 与 Mock 可用；
6. 模型/限制/价格是带版本和生效时间的 snapshot，不是永久常量；
7. MVP 不自动跨供应商 failover；显式换路由产生新 attempt；
8. FFmpeg CPU 处理不是生成式推理，允许本地执行；
9. ComfyUI 只能作为以后非默认远程 adapter，不能成为依赖。

## Provider 边界

统一方法：discover、estimate、generate text/image、submit/get/cancel video、synthesize speech。统一错误覆盖 auth、permission、rate、quota、budget、content、region、model、outage、invalid、unknown。

## 后果

- 无 Key/无 GPU 可以完成 M0；
- 真实火山 adapter 接入不改领域实体；
- 历史任务保留真实 model/endpoint；
- 用户显式承担换 provider 的风格变化；
- 需要 adapter contract tests 与真实 Key 最小实测。

## 覆盖的旧决策

Wan2.2 主线、本地 24GB GPU、ComfyUI 必需、CUDA/VRAM Manifest、GPU 小时预算全部被 FLO-124 覆盖；替换为 route snapshot、provider units/cost 和 API SLA。
