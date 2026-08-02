# FLO-104 双轨 CER 评测协议

FLO-104 样片使用两个不可混写的指标：

- `standardCer` 是诊断指标。它保持普通字符 Levenshtein 成本，不豁免同音字；
- `mandarinToneAwareCer` 是产品发布门禁，阈值为 `≤2%`。它只把标准对齐中、
  两个 Han 单字在冻结 `kMandarin` 数据里存在完全相同含声调读音的 substitution
  成本改为 0。

因此标准 CER 超限不能写成“标准 CER 已通过”。评审必须同时报告两个分数，例如：
“标准 CER 3.0303%（诊断）/ tone-aware CER 1.5152%（产品门禁通过）”。

## 冻结合同

ASR 必须使用：

- model：`mobiuslabsgmbh/faster-whisper-large-v3-turbo`；
- revision：`0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf`；
- `faster-whisper=1.2.1`、CPU/int8、`language=zh`、`beam_size=5`、
  `vad_filter=true`；
- 最终完整 Dialogue 轨，`referencePrompt=null`；
- 禁止项目词典、参考字幕提示、样片专用同音白名单或事后改写识别文本。

两路文本依次执行 NFKC、OpenCC `t2s`、删除 Unicode 标点和空白，再以 Unicode
code point 为字符执行单位代价 Levenshtein。回溯 tie-break 固定为
`match → substitution → deletion → insertion`。先得到不改成本的标准 edit ops，
再执行 tone-aware 重分类；插入、删除、异调同音、非 Han 替换或任一侧无读音仍为 1。

普通话读音只来自 Unicode 17.0.0 `Unihan_Readings.txt` 的完整 `kMandarin`
子集。仓库内文件
`internal/videopipeline/cerevaluation/data/Unihan_kMandarin-17.0.0.txt` 的 SHA-256
固定为 `07892add965a30e0a67b5003b8c08cd8d50a4c12f56c2ffab7ccd6c15464df70`；
上游 `Unihan.zip` 固定 SHA-256 为
`f7a48b2b545acfaa77b2d607ae28747404ce02baefee16396c5d2d7a8ef34b5e`。
如需审计重建，先从
`https://www.unicode.org/Public/17.0.0/ucd/Unihan.zip` 下载，再执行：

```text
./video-pipeline/scripts/generate-unihan-kmandarin.sh \
  ./Unihan-17.0.0.zip \
  ./new-Unihan_kMandarin-17.0.0.txt
```

脚本拒绝覆盖既有文件并先验证上游压缩包 hash。任何模型、依赖、标准化、
Unihan 版本或数据 hash 变化都形成新协议，不得与本证据混用。

## 无 Provider 评测入口

先准备严格 JSON 输入，再绑定最终 Dialogue 文件生成一次性证据：

```text
go run ./cmd/video-cer-evidence \
  ./cer-input.json \
  ./dialogue.wav \
  ./new-cer-evidence.json
```

命令会独立计算 Dialogue SHA-256，拒绝 ASR 参数漂移、非零 Provider submit、
参考提示/项目词典/白名单，并以独占创建方式防止覆盖旧证据。输出记录原始和规范化
文本、全部标准 edit ops、逐 substitution 的两侧/共享读音、tone-aware 成本、
两种分数、数据来源与证据内容 hash。

定向回归不启动容器、不访问 Provider：

```text
make video-cer-test
```
