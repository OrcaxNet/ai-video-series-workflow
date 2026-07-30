# ADR-0002：不可变 Revision、Snapshot 与内容寻址产物

- 状态：Accepted
- 日期：2026-07-30
- 关联：FR-P0-02～10、21～22

## 背景

剧集风格一致性依赖来源小说、四层上下文、资产、Prompt、route/model 和上一镜头尾帧。覆盖任一被引用内容都会让旧镜头不可复现，也无法解释实际费用为何发生。

## 决策

1. Source/Entity/Context/Asset/Script/Storyboard/ShotSpec/Prompt 均新增 child revision，不覆盖历史；
2. 每个 Provider attempt 冻结 input hash、provider/model/endpoint/route/capability snapshot；
3. 媒体用 SHA-256 CAS URI，临时 provider URL 不进入历史；
4. `revision_dependencies` 记录 producer→consumer，变更生成 freshness impact；
5. Gate/Manifest 绑定精确 revision/hash；
6. rollback 修改“当前引用”，不修改历史对象或 Artifact；
7. 同 content hash 去重；被引用对象不物理删除。

## 后果

- 可追溯到输入、模型、请求/task、费用、QC 与审批；
- asset v3 不影响仍引用 v2 的旧镜头；
- 局部重生成和时间线重建不会重复调用其他镜头；
- 需要索引、归档、GC 和 stale UX；
- Provider 同 seed 不保证像素级复现，故“可复现”指输入/执行谱系可复核。
