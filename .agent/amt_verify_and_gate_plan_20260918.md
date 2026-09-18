# AMT 16叉单树 pathDB 版：开发校验 + 性能门禁推进计划（2026-09-18）

> 状态：**已确认（2026-09-18）**，四项裁定如下：
> - Q1 论文路径：`C:\Users\Merlin\Documents\ASCT_Paper_latex`（4.Design.tex 在其下）
> - Q2 D1：走**路线 B**（fork 节点结构，逐条贴合论文）
> - Q3：选 **E-a**——B3 之前必须补齐 P4/P5（epoch 位图 + fork 节点结构 + stub 挂载），否则跑出来没有意义
> - Q4：优化循环 2–3 轮不达标即带证据升级汇报
> - **C5（新增，赎回校验口径）**：ECMH 校验随 P4/P5 一起实现（stub 承诺进 root 后才有锚点），门控开关——开发/审计短跑开启以证明实现正确，正式实验关闭，只保留廉价自检（长度/指纹）；校验耗时不计入状态树生成路径计时（沿用 9/16 裁定：验证属交易池收集阶段）。
> 上位文档：`amt_pathdb_singletree_plan_20260917.md`（开发计划+S0/S1 执行记录）、
> `amt_experiment_plan_20260907.md`（阶段/门 G1–G4/发车纪律）。本计划是两者的
> 「校验 + 门禁推进」执行层，不改变任何既定裁定（C1–C4、配对纪律、kill gate）。

---

## 0. 现状盘点（2026-09-18 代码级核查，全部有证据）

| # | 事实 | 证据 |
|---|---|---|
| R1 | P0–P3 已落地：单树 16 叉、pathDB 接线、nibble 前缀域、value inline。但**改动还在工作区未提交**（mpt.go 重写 1094 行、ethdb_adapter.go/bench_test.go 新增） | `git status`：4 文件 M；`git log` 停在 69a96bc4f |
| R2 | S1（=B1 无归档结构探针）**已跑完**：file 9 重载段 393,216,000 ops，末段 10 窗 **r=0.594 < 0.7**，已触发 C4 停线 | 9/17 计划文档 §9.5 |
| R3 | 裁剪=**整域封存**：轮转到某个域就把该域全部活跃叶子封进一个 bucket，**不是论文的 per-key epoch 位图**；代码注释自承"P4 待做" | `mpt.go:713-719` |
| R4 | 裁剪后**无 stub 挂回**：bucket 承诺不进 root，$C_{root}$ 只覆盖活跃状态（P2 中间态，D4 已论证必须等 P4 fork 节点结构） | `mpt.go:752-756` |
| R5 | 冷读自动赎回**已实现**：Get → 热树未命中 → 域归档查找 → Cuckoo 过滤 → 从桶删除 → 回插热树 | `mpt.go:607-640`（`ActivateArchivedKeyOnRead` 分支） |
| R6 | 赎回路径**未见 ECMH 校验**（grep `ECMH|verify` 无命中）；论文口径是"Cuckoo 过滤 → ECMH 校验 → 回插"。是否缺失待 A 阶段对照论文确认 | 本次 grep |
| R7 | flag 默认值：`-traceStressHotLayer` 已是 `"mpt"`（C3 已落地）；`-traceStressTrieBackend` 默认 `"hashdb"`，正式跑必须显式传 `pathdb` | `trace_stress_test.go:57,59` |
| R8 | 两个 metric 派生字段 bug 未修：归档轮次按 2^20 算（实际 16^4=65536 域）、archive_period_ops 错 16 倍 | 9/17 计划文档 §9.6 |
| R9 | G2 现有数字不可采信（pathDB 脏缓冲不在 State_Bytes 里），须用 `PathStats()` 三项相加口径重算 | 9/17 计划文档 §8.2、§9.7.3 |
| R10 | 论文源文件 `ASCT_Paper_latex/4.Design.tex` **本机未找到**（/d、OneDrive 已搜），审计前需用户指认路径 | 本次 find |

**结论：用户说的"半成品"属实且可精确定位**——归档/赎回骨架在，裁剪是占位实现，
论文一致性缺口集中在 P4（epoch 位图+fork 节点结构+stub 挂载），且 D1（fork vs 旁路索引）尚未拍板。

---

## 阶段 A：开发校验（只审计、不改行为，0.5–1 天）

目的：给用户一张"符合/不符"对照表，每条带 file:line 证据，不看口头。

### A1 对照论文三机制（前置：用户给论文路径，见 R10）
| 机制 | 核对点 | 预判（待审计坐实） |
|---|---|---|
| 归档 | bucket 记录格式（key/ref/版本）、Cuckoo filter 参数与重建时机、v2 bitmap 索引 | 骨架在，需逐字段对 `encodeArchive`（`mpt.go:519`） |
| 裁剪 | Algorithm 1 四阶段：子树跳过 CanSkip → 叶子剥离 → 压实挂载（CanAppend/≥M 强压/MountAtDeepest）→ 路径收缩 | **不符**：现为整域封存（R3），无 epoch、无 CanSkip、无 stub 挂载（R4） |
| 赎回 | Cuckoo 过滤 → **ECMH 校验** → 回插热树 → 桶 Delete | Cuckoo+回插+桶删有（R5）；**ECMH 校验疑似缺失**（R6），若论文要求则记为缺口 |

### A2 对照用户三项改造要求（9/17 计划 §5 验收矩阵原样执行）
| 要求 | 机检判据 | 预判 |
|---|---|---|
| ① 16 叉 MPT 改造版 AMT | `trace_stress_test.go:57` 默认值 `"mpt"`；正式跑 metadata `hot_layer=mpt` | ✅ 已满足 |
| ② 走 pathDB | metadata `trie_backend=pathdb`；`ethdb_adapter.go` 存在；`TestMPTPathDBBackendReload` 绿 | ✅ 已实现，但默认值是 hashdb（R7），每次发车要盯命令行 |
| ③ 不分片、按路径裁剪 | 无 `shards map`/`aggregate`；裁剪按 nibble 前缀定位 | ✅ 单树已成立（`mpt.go:28-30`）；但"按路径找子树裁剪"当前是**key 区间扫描**（`domainEntriesLocked` 全扫），不是子树定位+CanSkip，**只算部分满足** |
| ④ 冷读自动赎回 | Get 未命中 → 归档 → 回插热树 | ✅ 已满足（R5） |

### A3 性能审查（代码级，不接归因实验）
- 热路径走查：`Get/PutBatch/CommitToBatch/pruneNextDomainLocked` 每操作包装层（Cuckoo 探测、域定位、valueRef 构造、记账）有无明显浪费；
- 重点疑点：`pruneNextDomainLocked` 的**全域迭代扫描**（`NodeIterator` 扫整个域）在 65536 域、上亿 key 下的成本；`rebuildFilter` 每次全量重建 Cuckoo；
- 产出：可疑点清单（带 file:line + 预估量级），留给阶段 C 的 T4 实测验证，**不先动手改**。

### A4 产出物
- 审计报告 `.agent/amt_audit_20260918.md`：三机制对照表 + 四要求对照表 + 性能疑点清单，
  每项标【符合 / 部分符合+差距 / 不符+补齐路径】；
- 问题分级：**阻断正式实验**（口径 bug、未提交代码）/ **影响论文一致性**（P4/P5、ECMH）/ **可延后**。

---

## 阶段 B：收口与基线修复（0.5 天，审计后立即做）

- B1 修 R8 两个派生字段 bug（域数改读 `DomainNibbles` 实际值，archive_period_ops 同步）；
- B2 **提交工作区改动**（P0–P3 至今未 commit，版本不可追溯是风险）；提交前跑 `go vet ./trie/archive/...` + mpt 包全部测试 + `go test -c` 编译门；
- B3 检查所有 `.agent/run_remote_*.py` 上传清单含 `ethdb_adapter.go`、`bench_test.go`（launcher 不同步=远端跑旧码，老坑）；
- B4 G2 口径改 `PathStats()` 三项相加，重算 S1 已有数据的 G2 参考值。

---

## 阶段 C：性能归因 → 优化 → B1 复测循环（C4 停线已触发，核心阶段）

S1 r=0.594，距 0.7 差 18%。按 C4：**不烧 B3，先深度归因**。

- C1 **T4 拆账**：把 Operations_ms 那 66.8% 拆开——Cuckoo 探测 / 域定位 / valueRef 构造 /
  冷热判断记账逐项计时。诊断计数器+CSV 列，环境变量门控，关掉时零开销（沿用 vref dashcam 模式）。
- C2 按拆账结果**逐项优化**，每项一次循环：本地测试绿 → 远端 B1 复测（file 9，同预算 393,216,000 ops，
  参照仍是 fresh 起跑的 B0b，禁止拿全量 B0 晚窗当参照）。
- C3 **出循环判据**：末段 10 窗 r ≥ 0.70 → 进阶段 D；
  若 2–3 轮优化后仍 <0.7 → 带完整证据表（归因拆分 + 每轮 r 曲线）向用户汇报，由用户决定继续优化/调整门槛/调整论文口径，**不擅自发车 B3**。
- C4 红线：归因显示 Operations_ms/comparative ≥70% 才算树遍历瓶颈，否则**不许换结构救非遍历瓶颈**（手册 §4.1）。

---

## 阶段 D：B2 域深度重标定（r 达标后）

- 候选 **{3, 4, 5} nibble**（4096 / 65536 / 1048576 域），旧 D15 结论作废（9/17 计划 D3）；
- 每档 file 9 跑 1.5 归档轮，metadata 标 `domain_depth_nibbles`；
- 选值优先级沿手册 §4.2：热命中 ≥90%（G3）→ FPR ≤1%（G4）→ State_Bytes 最低（G2，用 B4 新口径）→ ops/s 不劣化 >15%；
- 产出：标定对照表 + 选定值，用户确认后进 E。

## 阶段 E：B3 全量（20,242,112,513 ops）

- 前提：G1 达标（阶段 C/D）+ 用户明确拍板以下两者之一：
  - **E-a**：P4/P5（epoch 位图 + fork 节点结构 + stub 挂载 + ECMH 补齐）先做完再跑 B3 —— 论文一致性零缺口，但 D1 要先拍板、工期 +4–6 天；
  - **E-b**：以当前形态（整域封存、无 stub 承诺进 root）先跑 B3 拿性能数据，论文一致性后补 —— 快，但跑出来的系统**不是论文所述系统**，结论页必须注明。
- 参数：`stem_mode=false`、读激活开、Cuckoo 32×4、NodeCache 512MB、`-traceStressTrieBackend=pathdb`、显式 `-traceStressHotLayer` 核对 metadata；
- 跑完 `summary.json` last_root + 存扫（G2 权威口径）；
- 发车纪律：每次正式 run 前向用户确认；launcher 上传清单核对（B3）。

---

## 决策记录（2026-09-18 已全部拍板，见文首状态区）

## 不做（本轮）
状态根与主网一致性校验；Verkle；论文正文改动（C1 裁定：S3 之后再结）；并行跑引擎；删 ASCT/stem 旧码。

---

## 执行进展（2026-09-18 下午，改造清单批准后）

清单口径（用户在审计后拍板的编号：B1=metric 派生字段、B2=读激活计时接入、B3=归档桶常驻止血(P-1)、B4=G2 口径、B5=提交+清单核对、T4=拆账）：

| 项 | 状态 | 落点 |
|---|---|---|
| B1 | 已实现 | `trace_stress_test.go`：`archiveRoundDomains` 按热层实际域数（mpt=16^nibble，amt=2^ShardDepth），`archive_period_ops` 乘裁剪 cadence；summary 新增 `archive_round_domains` |
| B2 | 已实现 | `diagnostics.go` 导出 `RecordMPTReadPromotion`（语义对齐二叉 activateValueRef：只计晋升动作本身）；`mpt.Get` 赎回分支接线，CSV 列名未动 |
| B3 | 已实现 | `mpt.go`：桶 entries LRU 驱逐回盘（filter+count 常驻，负查不落盘）；脏桶与 MRU 桶不驱逐；`ArchiveResidentEntries` 默认 4M 条，flag `-traceStressArchiveResidentEntries` + metadata `archive_resident_entries`；新测试 `TestMPTArchiveBucketEviction`（含驱逐后赎回、脏桶保护）。修掉两个实现 bug：MRU 按位置保护被 dirty 轮转击穿（改按元素身份）、扫描预算随 Len() 收缩（改入口快照） |
| B4 | 已实现 | `writeCounter` 加累计 totals（`take()` 原为排空语义，PathStats 此前只报最近一次 commit 尾巴）；summary 新增 `path_hot_written_bytes`/`path_hot_writes`/`path_hot_buffered_bytes`/`path_hot_diff_bytes`/`active_layer_bytes_total`；CSV 不动 |
| T4 | 已实现 | 新文件 `mpt/op_trace.go`：`MPT_OP_TRACE=1` 门控，拆 hot/probe/load/remove 四段计时+计数，经 `archive.OpTraceProbe` 挂钩输出到独立 `op_trace.csv`（主 CSV schema 不动），summary 带 `op_trace_total` |
| B5 | 进行中 | launcher 核对结论：现役 run_remote_s1_pathdb_pair_20260918.py 按目录 glob 自动含新文件；老显式清单 launcher 均为旧一次性探针，复跑前会另写新 launcher |

验证门基线修正：`go test ./trie/archive` 默认参数下 `TestArchiveTrieStress` 是手动 soak（stressItems 默认 52.4 亿条），永远跑不完——预存现象，非本次回归。验证门改为 `-skip 'TestArchiveTrieStress'`。

下一步：V 门全绿后 commit（B5 收尾）→ 写 S1' B1 复测 launcher（MPT_OP_TRACE=1，file 9 同预算）→ 远端复测拿 T4 拆账。

**S1' 第一轮（20260918_133727，已收）**：exit 0、18 分钟、40 窗。拆账：热树本体（geth 十六叉 Get/Update/Delete）占 Operations 的 **79.2%** 且每窗 ns/op 从 516 涨到 1629（3 倍退化）；归档探测 9.8%、写路径归档清理 2.5%、桶磁盘加载 0%（393M 规模未触发驱逐，loads/evictions=0）；r(S1'/MPT 参考) 末 10 窗 0.577，与 S1 的 0.594 一致（插桩开销约 3%）。异常点：探测总时长 ÷ 抽样推断的归档读次数 ≈ 每次 190µs，与纯内存路径不符——第二轮（140933）加 probes/hot_misses/lock_ns 计数器复跑定位（若实际热未命中率远高于抽样的 0.06%，则 G3 的 Hot_Read_Hit_Rate 抽样口径本身可疑）。另确认：path 后端设计上不用 nodeCache（MPT_NodeCache_*=0 非 bug）；S1' 快照 path_hot_written=0 / buffered=242MB（顶到 256MB 写缓冲上限，FlushEveryBatches=0 全程未 flush——热树退化的头号嫌疑）。

**S1' 第二轮（20260918_140933，已收）+ G3 口径修复**：新计数器裁决——probes=hot_misses=2.62 亿（66.7% 的操作读不存在的键，主网轨迹固有特征），单次探测仅 0.1–0.3µs，"190µs/次"是分母用错的算术假象；锁等待仅 1.2%（排除锁争用）。顺带坐实指标 bug：`mpt.GetValueRef` 对完全缺失的键返回 `(nil,false,nil)` 无错误，抽样器把全部缺失读记成热命中（Sampled_Read_Missing 恒 0、Hot_Read_Hit_Rate 虚高 99.9%）。已修：GetValueRef 返回 ErrNotFound + 适配层翻译 + `TestMPTGetValueRefErrorContract` 契约测试（commit `1df939b5d`）。**S1/S1' 旧 CSV 的 Hot_Read_Hit_Rate 列作废**。

**C2 第一刀（fl25，20260918_144442，已收）——flush 假设坐实，G1/G3 双双过门**：每 100K ops 强制 flush 后 hot ns/op 从 526→1629 压平为 304→603，hot 段总时长 447s→195s；**r(AMT/MPT) 末 10 窗 0.891、均值 1.039（S1 为 0.594）**，整墙钟 18→13 分钟。修复后的 G3 口径真实可信：Hot_Read_Hit_Rate 末 10 窗 99.57%（≥90% ✓），Sampled_Read_Missing 6172–7206/窗（修复前恒 0）。代价：Commit_ms 均值 4.6s（flush 落在提交段，不进 Operations/r）。病灶结论：pathdb diff 层堆积（FlushEveryBatches=0 时 9.8 万批不 flush）造成读放大，非归档 wrapper。已发 fl250 边界探测（150413）确定 B3 用档。

**fl250 边界探测（20260918_150413，已收）——不过门**：r 末 10 窗 0.633、均值 0.763，hot ns/op 退化回 1175–1390。临界点在 25–250 批之间。**C 阶段收官结论：B3 正式参数采用 FlushEveryBatches=25（每 100K ops flush）**，余量 27%（0.891/0.7）；优化循环第 1 刀即达标，止损规则下不再追加轮次。待办提醒：Q3 裁定 B3 前必须补 P4/P5（fork 节点/epoch 位图/四阶段裁剪/stub/ECMH/桶容量 100 分裂/O(1) 更新）；G4 的 mpt 桶 FPR 遥测缺口随 P5 桶重做补齐。
