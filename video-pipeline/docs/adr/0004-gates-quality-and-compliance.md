# ADR-0004：G1/G2/Q1/G3、权利门与 Manifest

- 状态：Accepted
- 日期：2026-07-30

## 决策

- G1：角色/场景/道具/音色资产、权利与同意锁定；
- G2：剧本、时长、人物出入场和分镜输入批准；
- Q1：逐镜主体、风格、连续性、内容与技术质量；
- G3：时间线、字幕、音画、总时长、导出和 Manifest 锁版。

所有 Gate 都绑定 revision/hash/actor/reason。任何角色（包括管理员）不能绕过版权、肖像/声音同意、stale 或预算门。Provider 内容安全失败保留稳定类别，禁止自动改写规避审核。

Manifest 必须包含来源、四层上下文、资产、Prompt、provider/model/endpoint/request/task、用量/费用、输入输出 hash、QC、Gate、许可、AI 标识；不得含 Secret 或临时 signed URL。

## 后果

- 付费调用前发现资产/预算问题；
- 质量不合格只重做受影响镜头；
- 对真人声音/形象和第三方素材有明确阻断；
- MVP 不是无人审核自动发布。
