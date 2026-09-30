# AMT 真实轨迹压测 · 实验手册（2026-09-07，持续修订）

读者：只负责跑实验的人（实验线程）。本文件自足，不需要读其它文档就能发车。
代码改动见 [amt_dev_plan_20260907.md](/D:/go_workspace/go-ethereum/.agent/amt_dev_plan_20260907.md)；
校验与门禁推进见 [amt_verify_and_gate_plan_20260918.md](/D:/go_workspace/go-ethereum/.agent/amt_verify_and_gate_plan_20260918.md)。

范围：只做 **AMT（非 stem，`stem_mode=false`）** 与 MPT、Verkle 的对比。纯压测，不重放、不校验状态根。

---

## 修订摘要（最新进展，细节在后文各节）

- **2026-09-16**：B0/B0b/B1/B2r/B3/B3m 均已跑，G1 在所有诚实配对下不达标，见 §4 状态修订。
- **2026-09-18**：P4/P5 实现补完（epoch 位图 + fork 节点结构 + stub 挂载 + ECMH 门控，E-a 前置清零）；
  性能归因坐实病灶为 pathdb diff 层堆积，**fl25（每 100K ops flush）过门：末 10 窗 r=0.891**。
- **2026-09-20**：D 阶段（域深度重标定 {3,4,5}，取代旧 D∈{15,16,17}，旧 D15 结论作废）首轮冒烟暴露
  四个规模级正确性缺陷，全部修复；R1 三档标定通过（当时推荐 nib4，**已被 9/23 裁定取代**）。
- **2026-09-23**：公平 nib5（file-0 全 trace）生产崩溃暴露**第五洞（cuckoo 饱和）**，已修复。
  **用户拍板（同日两轮）**：①预实验结论=归档周期 3–6 个月合适 → 先改定 nib5；②生命周期口径纠正（epoch 两轮剥离，
  key 生命周期 ∈ [1 轮, 2 轮]：nib5 = 6–12 个月，**超出窗口**；nib4 = 11–22 天太快）→ **分片粒度改比特（2^D，
  2 倍步进，`bca5bef10`）**，首选 **D=19（524,288 域，一轮 ≈86 天 → 生命周期 3–5.7 个月，命中窗口）**。
  在跑 nib5（=D=20）转为重载段性能对照；当前障碍=重写入段性能（末 10 窗 r=0.668<0.7，随库增大衰减）。
  R1 nib5 赎回 0 的真因=epoch 周期未跑完（一轮 4.19B ops>trace 2.98B，无 key 被真正剥离）。
  **B3 前两个缺口：G4 FPR 分母计数器未接线；MPT 参考臂缺 PathStats，G2 无法同口径对比。**

---

## 0. 目标与判定线

| 编号 | 目标 | 主判据 | 同时报告 |
|---|---|---|---|
| G1 | 性能（不含 prune） | B3 末段（最后 10 窗口）`Operations_Per_Sec` 池化均值 ≥ B0 MPT 同 block 区间池化均值 | 最后 5 / 最后 20 窗口均值、全程总值、最低窗口值、低于地板线的窗口数 |
| G2 | 存储（不含归档层） | `StateGain = 1 − AMT活跃层 / min(MPT, Verkle) ≥ 0.50` | AMT `ActiveOnlyLogicalBytes`、`State_Bytes`、MPT/Verkle 同预算 `State_Bytes` 曲线 |
| G3 | 热命中 ≈ 90% | B3 末段 10 窗口 `Hot_Read_Hit_Rate` 均值 ≥ 90% | 全程累计、按 block 区间的曲线 |
| G4 | Cuckoo 假阳性低 | B3 末段 10 窗口 `Runtime_Filter_FPR` 均值 ≤ 1% | 全程累计、`Archive_Filter_Lookups/Negatives/Positives/False_Positives` 原始计数 |

判定纪律（四条，不许临时改动）：

1. G1 是先决门。G1 不过，G2–G4 不进正式表。
2. 一切判定用窗口均值或累计值，不看单窗口；偶发掉落只记录，不改判。池化均值 = `Σ Window_Operations / Σ Comparative_Wall_ms`。
3. 配对比较必须同一批 op：三引擎共用 `-traceCompare/traceStressStartFile` + 同一 ops 预算 + 同一窗口常量（10M），因此第 k 行就是同一段 op。跨段比较（如 B3 末段 vs B0 末段）必须用 `First_Block/Last_Block` 对齐 block 区间。
4. 严格串行，一次只跑一个阶段（单机，并行会污染 CPU/IO/页缓存）。

**r 判门口径（2026-09-23 校准，取代一切旧算法）**：逐窗 `Comparative_Wall_ms ÷ Measured_Wall_ms`
（进程内 MPT 对照臂），判**末段 10 窗均值**；全程均值仅供参考。校准锚点：fl25 run 144442
复算 = 官方记录 0.891/1.039，分毫不差。此前报告的"后 1/3 聚合 r"等中间数值全部作废。

**存储口径**：G2 权威 = summary.json 存扫（AMT 侧 = PathStats 三项相加 `active_layer_bytes_total`）；
CSV `State_Bytes` 为过程口径（不含 pathdb 脏缓冲）；**跨档绝对值不可比**（ops 总量不同）；
**跨段绝对值不可比**（file-0 段 ~91B/op vs file-9 段 ~1.1B/op，相差两个量级）。
内存为一级对比维度：B3 数百 GB 规模须盯 RSS/db 比与分配器滞后。

## 1. 实验机、凭据、路径

| 项 | 值 |
|---|---|
| 主机 | `192.168.2.230`（hostname `titanide`），20 核，59 GiB RAM，`/` 1.9 TB ext4（2026-09-07 实测） |
| 凭据 | 本地 `D:\go_workspace\go-ethereum\.agent\asct_remote_servers.local.json` → 键 `asct_mpt` 的 `ssh_user` / `ssh_password`（paramiko，密码登录）。**该 JSON 的 `host` 字段写的是 `192.168.3.51`，已失效，主机一律以本表为准**；密码绝不打印 |
| 源码 | `/root/asct_codex/go-ethereum-trace`（由 launcher 上传本地 `trie/archive/*.go`（**必须含 `mpt/hx/*.go`、`mpt/ecmh/*.go` 子目录，旧 glob `mpt/*.go` 不含子目录——已踩过**）+ `core/tree_test/trace_compare_test.go`，不 git pull） |
| Go | `/usr/local/go/bin/go`（go1.26.0 linux/amd64） |
| 环境 | `GOCACHE=/root/asct_codex/go-build-cache`，`GOTMPDIR=/root/asct_codex/trace-tmp`（发车前 `mkdir -p`） |
| 结果根 | `/root/asct_codex/results/{trace_stress,trie_compare}` |
| 磁盘守卫 | `/root/asct_codex/full_replay_disk_guard.sh`（源：`.agent/asct_trace_stress_disk_guard.sh`），阈值 `MIN_FREE_BYTES=8000000000`，触发即 `kill -TERM` 并 exit=2 |

发车前必查（本地执行，输出不含密码）：

```powershell
cd D:\go_workspace\go-ethereum
python .agent/remote_full_replay_preflight.py   # 核数/内存/go 版本/磁盘/数据集/源码/残留进程
```

要求：`PROCESSES` 段除自身外为空；`df` 空闲 ≥ 8 GB（守卫阈值），B3 发车前建议 ≥ 120 GB。空间不足时报旧结果目录清单给用户确认，**不擅自删除**。

**磁盘账本（2026-09-23 实测）**：1.9T 盘构成——trace 原始数据 392GB（在跑不能动）、results 残留（旧 run 的
state_db 可清，实验记录必须保留：日志/元数据/CSV）、`/home/root`、fabric/snz（别项目不动，用户裁定）、
`ethdata` 区块/交易 CSV 300GB（转储不用，已 pigz 压缩至 ~40GB）。file-0 段 AMT 库增速 ~91B/op，
B3 发车前必须按段重估磁盘需求。

## 2. 数据集（唯一输入）

`/root/asct_codex/mainnet_state_access_trace/range_10m/`，`du -sb` 实测 411,266,874,333 字节（384 GiB）：`manifest.json`、`SHA256SUMS`、10 个 `state_access_trace_<起block>_<止block>.csv.gz`、生成时留下的 `trace_state_db/`（只读，不要动）。

block 46,147 → 10,000,000；9,953,854 块（1,455,084 空块）；697,373,173 笔交易；**20,242,112,513 次操作**（read 15.26 B / write 4.02 B / touch 841 M / create 77 M / delete 44 M）。

列 `block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len`；key 由 `trieutils.BinaryTree{BasicData,StorageSlot,CodeChunk}Key` 派生，`read,touch→get`、`write,create→put`、`delete→delete`，值是等长合成 payload。

key 随机性（结论，细节不复述）：位置哈希随机、无地址聚簇，shard 路由均衡；同账户关联 key 共享 248 位前缀（单前缀 ≤256 key），AMT 下各自独立归档；**执行顺序刻意不打乱**。

负载极偏：**前 4 片（46k–4M）占 4.5% 字节，后 6 片（4M–10M）占 95.5%**；且**分段负载密度差两个量级**
（file-0 段 ~91B/op——2016 DoS 期大值写入，file-9 段 ~1.1B/op）。因此所有短阶段（B0b/B1/B2）都从
`-traceStressStartFile=9` / `-traceCompareStartFile=9`（block 9,046,147 起，本片约占 14.4%）起跑，
让探针与标定落在真实重负载段；正式长阶段（B0/B3/B4）从 file 0 跑完整轨迹。论文叙述只在 ≥4M 区间做等预算比较。
**注意：file-9 段成绩不能外推到 file-0 段**（公平 nib5 实证：同配置 file-9 r≈1.0，file-0 末 10 窗 r 跌破 0.7）。

## 3. 公共口径

窗口 = **10,000,000 次操作**（`BatchSize=4000` × `MetricsBatches=2500`，三引擎常量一致）。全量 20,242,112,513 → 2024 个满窗 + 1 个残窗（2,112,513 ops，残窗不进任何均值）。

性能分子（比较口径，两边都不含 prune/冷维护）：

| 引擎 | `Comparative` 构成 | 冷维护 |
|---|---|---|
| AMT | `Operations_ms + Commit_ms + DB_Write_ms` | `Cold_Maintenance_ms` 单列，不进分子 |
| MPT / Verkle | `Operations_ms + Root_ms + DB_Write_ms` | 无 |

AMT 读激活的耗时计入分子（走 `Archive_Read_Promotion_ms`，驱动不减它）。

产物命名（注意：AMT 与 ASCT 共用同一个驱动，CSV 文件名固定 `asct_trace_stress.csv`，靠 `metadata.json` 的 `stem_mode=false` 区分）：

| 驱动 | 产物 |
|---|---|
| `TestArchiveStemTraceStress`（AMT） | `<baseDir>/metadata.json`、`<baseDir>/results/asct_trace_stress.csv`、`<baseDir>/results/summary.json` |
| `TestArchiveStemTraceStorageStats` | `<baseDir>/results/storage_breakdown_20260820.json`（含 `ActiveOnlyLogicalBytes`；文件名固定，`analyze_trie_compare.py:118` 按这个名字找） |
| `TestTrieTraceCompare` | `<baseDir>/metadata.json`、`<baseDir>/results/{mpt,verkle}_trace_stress.csv`、`<baseDir>/results/summary.json` |

目录命名统一 `results/<trace_stress|trie_compare>/<阶段>_<ops>_<关键参数>_<STAMP>/`，同一批实验共用一个 `<STAMP>`。

分析：先把各阶段的 `<baseDir>/results/` 与 `metadata.json` 拉到本地同一目录结构，再跑（一次喂三引擎的本地目录）：

```powershell
python .agent/analyze_trie_compare.py --asct <AMT的baseDir> --mpt <B0的baseDir> --verkle <B4的baseDir> --output-dir <本地输出目录>
```

它按 10 窗口一组（= 100M ops）重算池化 `Operations_Per_Sec`，正好是 §0 的末段粒度；`--asct` 参数名喂的是 AMT 目录。

拉回本地：launcher 需带 `--pull <baseDir>` 模式（paramiko `open_sftp()` 把 `<baseDir>/results/` 与 `metadata.json` 递归拉到本地同名目录，层级不变）。查状态：`python .agent/remote_full_replay_status.py`（它写死了三引擎旧目录名，AMT 版要把那三条 `results/...` 路径模式改成新命名，约 3 行）。

正式阶段关键参数（写死，metadata 必须能反推）：`stem_mode=false`、`ShardDepth=D`（B2 标定值）、`BatchSize=4000`、`MetricsBatches=2500`、`PruneEveryBatches=1`、`AccessSampleEvery=1000`、`CuckooBuckets=32`、`CuckooSlots=4`（→ 桶容量 100，与论文一致）、`NodeCacheMB=512`、`CommitWorkers=16`、`AsyncPrune=true`、`DestructiveCommit=true`、`activate_archived_key_on_read=true`、**`FlushEveryBatches=25`（fl25，2026-09-18 拍板，见 §4.2 性能不达标尝试）**。

## 4. 阶段（门控串行）

> **2026-09-16 状态修订（权威）**：B0/B0b/B1/B2r/B3/B3m 均已跑。G1 在所有诚实配对下不达标：全量早期 r≈0.36→0.15；重载段严格配对（B0b fresh 同预算 vs B2r，196.6M ops）r=0.44→0.25、均值≈0.27。此前"B2≈1.0"作废——那是拿全量 B0 末窗（MPT 背着 22GB 老库）当参照的假象，分段比较必须 fresh 同预算起跑配对（脚本 `run_remote_b0b_mpt_file9_20260916.py`）。B3 全量已在 2.57B ops 处停线；A6 16 叉已接线实测（B3m）反而 r≈0.06，根因是每批次提交（聚合根全量重建+提交不合流），见开发文档 A6 调优节与 `.agent/amt_perf_handoff_20260916.md`。B1 r≈1.71 仅证明无归档时二叉不是瓶颈，不能当 G1 达标证据。B4/B5 未跑，是否值得跑取决于 A6 调优后复测的 r。

| 阶段 | 引擎/配置 | 预算 | 预估 | 产出 / 门 |
|---|---|---:|---:|---|
| B0 | MPT 全量（`B0_mpt_20242112513_20260908_085221`） | 20,242,112,513 | 12–20 h | 终局参照：末段 10 窗均值 `M_late`、最低窗 `M_min`、`State_Bytes` 曲线 |
| B0b | MPT，file 9 | 500,000,000 | 0.3–0.5 h | B1 的配对参照（同段、fresh 建树） |
| B1 | AMT 无归档（`-traceStressDisableArchive=true`），file 9 | 500,000,000（50 窗） | 2–4 h | 结构探针比值 `r`，见 §4.1 |
| B2 | AMT 开归档，file 9，**域深度 nibbles ∈ {3,4,5} 各 1.5 归档轮（2026-09-18 重定义，旧 D∈{15,16,17} 作废）** | 24.6M / 393.2M / 6.29B | 6–9 h | 定深度，见 §4.2 |
| B3 | AMT 全量（选定深度，读激活开） | 20,242,112,513 | 70–140 h | G1/G2/G3/G4 主数据 + 终态存储扫描 |
| B4 | Verkle 全量 | 20,242,112,513 | 20–30 h | G1/G2 的第二参照（可提到 B3 之前，不改判定） |
| B5 | AMT 读激活消融（`…KeyOnRead=false`），预算待定（短） | 待 G1 达标后定 | 1–3 h | 读激活对 G1/G3 的净贡献 |

串行合计 ≈ 6–8 天，长极在 B3、B4。B1/B2 任一触门提前停，则 1–2 天。

### 4.1 B1 结构探针（G1 的 kill test）

`r` = 窗口 6–50（跳过暖机）上，AMT(B1) 的池化 ops/s ÷ MPT(B0b) 同窗口的池化 ops/s。

| r | 结论 | 动作 |
|---:|---|---|
| ≥ 0.90 | 树形不是瓶颈 | 进 B2 |
| 0.70–0.90 | 结构有代价但可救 | 进 B2，并在结论页记录风险 |
| < 0.70 | 二叉热层本身太慢 | **停线**，启动开发文档 A6（16 叉热层 + MPT 实现做热层）；不进 B2/B3 |

归因规则：判 A6 之前先看 `Operations_ms / Comparative_Wall_ms`。≥ 70% 才算树遍历瓶颈，否则先查 commit / DB 写路径。

参照锚点（ASCT 历史，仅用于预期，不进正式表）：2 B ops 时 54,922 ops/s、热命中 78.51%、活跃层 5.13 GB；1 B 段 MPT 601 k–1.24 M ops/s、Verkle 228 k–279 k ops/s。按此量级 `r` 很可能落在 < 0.70，B1 因此排在长任务之前。

### 4.2 B2 深度标定

三跑只差域深度（nibble 数）。一轮 = `16^nibbles × 4000` 次操作，跑到 1.5 轮为止（按轮归一，否则热命中率不可比）。选值规则，按序取第一优先：

1. 1.5 轮处 `Hot_Read_Hit_Rate` 最高（三档都 <90% 时取最高档并记为待优化，不硬停）；
2. `Runtime_Filter_FPR` ≤ 1%；
3. 末窗 `State_Bytes` 最低（G2 权威口径 = 存扫，见 §0 存储口径）；
4. 池化 ops/s 不明显劣化（相对最优档低 >15% 视为劣化）。

升级顺序（三档全不达标才动，一次只动一个）：`BatchSize` 4000→2000/8000 → `NodeCacheMB` → Cuckoo 形状（32×4 为基线，16×4 桶容量 60 只作诊断）。

**归档周期换算口径**（12s/块、~3,400 ops/块、每批 4,000 ops 裁 1 域）：key 生命周期 ∈ [1 轮, 2 轮]（epoch 两轮剥离，
代码实证 `mpt.go` epochForKeyLocked+rollover 翻转）。16 叉：nib3 一轮 ≈0.7 天（生命周期 1.4 天）、nib4 ≈11.2 天
（22 天）、nib5 ≈172 天（6–12 个月）。**2026-09-23 改比特粒度 2^D（`bca5bef10`）：D=19 一轮 ≈86 天（生命周期
3–5.7 个月，命中 3–6 个月窗口）**、D=18 ≈43 天/轮、D=20 ≈172 天/轮。归档周期是设计参数，还可提高裁剪预算加速。

**性能不达标尝试汇总（末 10 窗 r，校准口径）**：

| 尝试 | 段/规模 | r | 结局与归因 |
|---|---|---:|---|
| S1（=B1 无归档探针） | file-9 / 393M | **0.594** | 触发 C4 停线。归因：pathdb diff 层堆积（FlushEveryBatches=0，9.8 万批不 flush）→ 读放大，hot ns/op 516→1629 |
| S1' 第一轮（optrace） | file-9 / 393M | **0.577** | 拆账：热树本体占 Operations 79.2%（geth 十六叉固有成本），探测 9.8%；插桩开销 ~3%。"190µs/探测"为分母用错假象（实际 0.1–0.3µs） |
| fl250 边界探测 | file-9 / 393M | **0.633** | flush 临界点坐实在 25–250 批之间；B3 定档 fl25 |
| **公平 nib5 sf0（在跑，未终审）** | file-0 / 6.29B | **0.668**（1.6B ops 时） | 2016 DoS 期高写入密度段：深度 5 归档维护+饱和轮换成本随库增大持续放大（速率 100K→47K ops/s） |

**达标记录**：fl25（run 144442）末 10 窗 0.891、均值 1.039，C 阶段收官（余量 27%）。

**配对纪律（2026-09-16 补，血的教训）**：B2 与 MPT 比较时，MPT 参照必须是同段 fresh 起跑的 B0b（同 file 9、同预算、空库建树），禁止拿全量 B0 的任意窗口当参照——B0 末窗 MPT 背着 22GB 历史库（~20 万 ops/s），会把 B2 假性抬到 r≈1.0。B0b launcher 模板：`.agent/run_remote_b0b_mpt_file9_20260916.py`（MPT 侧走 `core/tree_test/trace_compare_test.go`，产出列名是 `Root_ms` 而非 `Commit_ms`，对比时注意列名映射）。

**R1 三档成绩（file-9，fl25，校准口径）**：nib3 末10窗 0.917（G3 99.74%、赎回 137、4 轮）；
nib4 末10窗 0.936（G3 99.99%、G4 ≈2.4 FP/M ops、赎回 342、1.5 轮、RSS 2.71→2.23G、存扫 693.7MB）；
nib5 0.997 但归档 0 轮、赎回 0 次（真因=一轮 4.19B ops>trace 2.98B，epoch 周期未跑完，无 key 被真正剥离）。
**深度定档（2026-09-23 用户拍板，两轮裁定）：归档周期目标=状态生命周期 3–6 个月（预实验结论）**。epoch 两轮剥离 →
key 生命周期 ∈ [1 轮, 2 轮]：nib5（=D=20）6–12 个月超窗，nib4 11–22 天太快 → **分片粒度改比特 2^D（`bca5bef10`），
首选 D=19（一轮 ≈86 天 → 生命周期 3–5.7 个月）**，备选 D=18（≈43 天/轮）/D=20（≈172 天/轮）。在跑公平 nib5
（sf0 全 trace 1.5 轮）转为 D=20 重载段对照（跑过 4.19B ops 后赎回才开始）；障碍=重写入段性能（末 10 窗 r=0.668<0.7）。

### 4.3 命令

先设公共变量（`<S>` = STAMP）：

```bash
cd /root/asct_codex/go-ethereum-trace
export GOCACHE=/root/asct_codex/go-build-cache GOTMPDIR=/root/asct_codex/trace-tmp
mkdir -p "$GOTMPDIR"
TRACE=/root/asct_codex/mainnet_state_access_trace/range_10m
RES=/root/asct_codex/results
```

B0 MPT 全量：

```bash
/usr/local/go/bin/go test ./core/tree_test -run '^TestTrieTraceCompare$' -count=1 -timeout 0 -v -args \
  -traceCompareInputDir=$TRACE -traceCompareEngine=mpt \
  -traceCompareBaseDir=$RES/trie_compare/B0_mpt_20242112513_$S \
  -traceCompareOps=20242112513 -traceCompareStartFile=0 \
  -traceCompareBatchSize=4000 -traceCompareMetricsBatches=2500 -traceCompareTimingSampleEvery=1000
```

B0b MPT 配对段（把 `Engine=mpt`、`Ops=500000000`、`StartFile=9`、目录 `B0b_mpt_tail500m_$S`）。B4 同理换 `-traceCompareEngine=verkle`、目录 `B4_verkle_20242112513_$S`。

B1 AMT 无归档探针：

```bash
/usr/local/go/bin/go test ./trie/archive -run '^TestArchiveStemTraceStress$' -count=1 -timeout 0 -v -args \
  -traceStressInputDir=$TRACE \
  -traceStressBaseDir=$RES/trace_stress/B1_amt_noarchive_500m_$S \
  -traceStressOps=500000000 -traceStressStartFile=9 \
  -traceStressBatchSize=4000 -traceStressMetricsBatches=2500 \
  -traceStressStemMode=false -traceStressDisableArchive=true \
  -traceStressShardDepth=16 -traceStressCuckooBuckets=32 -traceStressCuckooSlots=4 \
  -traceStressAccessSampleEvery=1000 -traceStressFinalStats=false
```

B2 标定（每个深度一次，`Ops` 按 §4 表）：在 B1 基础上改 `-traceStressDisableArchive=false`、`-traceStressActivateArchivedKeyOnRead=true`、域深度参数、`-traceStressFinalStats=true`，目录 `B2_amt_nib<N>_<ops>_$S`。**公平深度审判用 `-traceStressStartFile=0`（launcher `D2_START_FILE` 开关，从 file 0 用全部 10 分片，让 1.5 轮归档真正跑完）**。

B3 AMT 全量：`-traceStressOps=20242112513 -traceStressStartFile=0`、选定域深度，其余同 B2，目录 `B3_amt_20242112513_nib<N>_$S`。跑完取 `results/summary.json` 的 `last_root` 追加一次存储扫描（G2 权威口径）：

```bash
ROOT=$(python3 -c "import json;print(json.load(open('$RES/trace_stress/B3_amt_20242112513_nib<N>_$S/results/summary.json'))['last_root'])")
/usr/local/go/bin/go test ./trie/archive -run '^TestArchiveStemTraceStorageStats$' -count=1 -timeout 0 -v -args \
  -traceStatsBaseDir=$RES/trace_stress/B3_amt_20242112513_nib<N>_$S -traceStatsRoot=$ROOT \
  -traceStatsOutput=$RES/trace_stress/B3_amt_20242112513_nib<N>_$S/results/storage_breakdown_20260820.json \
  -traceStressShardDepth=<选定深度>
```

B5 消融：复制 B3 配置，`-traceStressActivateArchivedKeyOnRead=false` + 短预算，目录 `B5_amt_noactivation_<ops>_$S`。

实际发车不手敲上面命令：用 launcher（§5）包成 nohup 后台任务，一次一个阶段。

### 4.4 正确性缺陷清单（规模级，全部已修复并回归）

| # | 缺陷 | 根因 | 修复 commit / 回归 |
|---|---|---|---|
| 1 | hx shrinkFull wrap 误删子节点路径（悬挂引用） | wrap 短节点仍引用 childPath 却对其发 onDelete；哈希后端从不真删而长期掩盖 | `8c0178428` / TestMPTPathDBPruneStressDanglingRegression |
| 2 | prune 变更拷贝未打脏标（5 处） | 干净缓存哈希致 hasher 跳过重算 → 幽灵叶/哈希错配 | 同上 |
| 3 | 归档探测短路 | 首个 filter 命中桶无货即返回，分裂兄弟桶被布谷假阳性遮蔽；同时影响 Put/Delete 去重 | 同上 |
| 4 | oversized embed（98B 短节点） | prune 合并造 short→short 链（违反上游"相邻短节点不存在"不变式）+ committer 只塌 fullNode 型 Val | `2d10b88d8`：foldShortChain 级联压平 + 短 Val 递归塌 / TestHXCommitShortShortChainNoOversizedEmbed |
| 5 | **cuckoo 饱和崩溃**（公平 nib5 batch 276,386 生产事故） | 布谷插入在 ~7/8 负载以上**概率性**失败（32×4 配置 M=100 ≈78% 负载 append），append/create 把插入失败当致命；设计本有同挂载点兄弟桶轮换（强制压实）但失败路径未接线 | `06470b64c`：饱和→哨兵→兄弟桶轮换（前缀保留、create 按过滤器实际容量裁剪、失败后 rebuildFilter 防探测假阴性）/ TestMPTCuckooSaturationRotatesSiblingBuckets、TestMPTBucketAppendSaturationKeepsPrefix |

**已排查排除的隐患**：重复追加致 ECMH 承诺双计——结构上不可能（赎回与 Put 覆盖均先 `bucketDeleteLocked`）。

**方法论**：悬挂引用零污染走查 `DebugCheckDangling`（仅内存树，键源必须确定性 rng）；
仪表零解析原则（全树走查会污染 tracer accessList）；复现键源禁用 crypto/rand。

## 5. 发车与状态

launcher：以 `.agent/run_remote_full_three_20260904.py` 为模板改出 AMT 版（本地运行，paramiko 上传源码+守卫、跑回归、写 `run_full_replay_<STAMP>.sh` 后 nohup）。远端 launcher 文件名**保持 `run_full_replay_` 前缀**，状态脚本才能按 stamp 找到它；阶段名（B0/B1/…）写进 `status.log`。它已实现的能力必须保留：发车前 preflight（目录不存在、无残留进程、磁盘 ≥ 阈值）、上传文件后 SHA256 记录、`status.log` / `<run>.exit` / `<run>.log` 三件套、磁盘守卫随进程存活、阶段失败置 `overall=1`、守卫 exit=2 时后续阶段 `skip`（记 129）、只跑被批准的阶段（B0/B0b/B1 先发车，B2 之后每轮由人确认）。

查状态 / 停任务：

```powershell
python .agent/remote_full_replay_status.py      # launcher 状态、磁盘、进程、最新 CSV 行
```

停止：`pkill -TERM -f 'run_full_replay_<STAMP>.sh'` 后再 `pkill -TERM -f 'archive.test|tree.test'`（顺序不能反，否则守卫会误判）。

## 6. 失败处理

`*.exit` 非 0 → 读同目录 `.log` 尾部定位；`exit=2` 是磁盘守卫停机，先处理空间再从该阶段整段重跑；进程消失但无 `.exit` → 视为失败同样整段重跑。**不做断点续跑**（预算已按"跑 4 天就说明不达标"处理）。任何阶段的产物目录都不复用：失败目录改名加 `_failed` 后缀留档。清理远端数据时实验记录（日志/元数据/CSV）必须保留，只删可再生的大件（state_db、压缩原始转储）。

## 7. 不做与已知限制

不做：重放/状态根校验；prune 绝对耗时对 G1 的影响；归档层自身的存储优化；ASCT 全量与 stem A/B；并行跑引擎；合成 Cuckoo FP 标定；`TestArchiveStemTraceFilterFPStats`。

限制（论文须写）：值是合成等长 payload 不是真实 RLP；计时构成两边不对称（`Commit_ms` vs `Root_ms`）且冷维护不入分子；G3/G4 对 MPT/Verkle 天然 N/A；单机、串行、无重复实验；三引擎都从创世重建，不是主网快照的真实状态分布；分段负载密度差两个量级（file-0 vs file-9），短阶段成绩不可外推到全量。

## 8. sized-batch 修复与 file-9 验证跑（2026-09-28）

**根因**（commit `502e94816`）：E2 Commit 61% 的主因是 `kvStoreAdapter.NewBatchWithSize` 丢弃 pathdb 的尺寸提示（`triedb/pathdb/buffer.go:136` 每次 flush 都调用），无尺寸 goleveldb batch 按 batch.go grow 每次增长全量拷贝，256MB/约250万条 flush 变 ~50s memmove；B0 走原版 rawdb 有预分配故无此病。修复=适配链透传尺寸提示（ethdb_adapter.go + stressDBAdapter + levelStore 补精确签名方法）。微基准实证：无尺寸 270 万条 Put=51.7s（19µs/条），batch.Write 仅 3.1s。

**验证跑** `D2_amt_bits19_3145728000_fl25_sf9_20260928_112540`（file-9、1.42 轮耗尽、exit 0、1h55m vs 旧档 2h06m）：同 op 数逐窗对齐旧档 `20260924_120346`——Commit 1554s→963s（占比 28%→19%，-38%），flush 窗附加 ~20s→~4.5s，墙钟 5540→4949s（1.12x），吞吐 537.6K→601.8K ops/s。机制指标逐项一致：复活 2402=2402、热命中 99.9846%=99.9846%、State 3.355G≈3.356G、RSS 2.58G≈2.59G；FPR 0.002077%（旧档该值因分母未接线为 0，本档为首个有效 file-9 FPR，与 E2 的 0.0054% 同量级）。CSV 已存档本地同名目录。

**r vs B0 逐轮裁定**（E2 旧档 vs b0_mpt.csv 同区块段池化，B0 墙钟=Parse+Ops+Root+Write 四分量，其 Measured_Wall 列有计时 bug 不可用）：轮1 r=1.38、轮2 0.82、轮3 0.39、轮4-8 0.20-0.27、全程 0.284；修复投影全程 ~0.44，第3轮后仍不过 0.70 → 用户裁定 E2 暂缓。退化解剖：每突变 staged 字节 23→459B（20 倍，读路径不重写叶子已实证，主因=状态扩散后 pathdb 去重衰减+19元素节点/epoch 叶子放大）、ops 路径 1.8x B0、RSS 2→9.5G。下一杠杆：hx 对齐原版（decodeNode 不设 flags.hash、hasher 简化版丢缓存语义）、RSS/GOGC 治理；验证跑法=sf0 跑 3 轮（6.3B ops ~4h）覆盖失败域。

## 9. fl1 提交节拍对齐（2026-09-28）

**动机**：代码对比发现 B0 每批 triedb.Update+Commit（trace_compare_test.go:821-826，diff 栈恒 1-2 层），AMT fl25 每 25 批才 Commit（mpt.go:1499-1505，栈 25 层），pathdb 读节点须穿透全部 diff 层（pprof diffLayer.node 占 CPU 28%）→ ops 路径 1.8-2.6x 与 RSS 的主嫌疑。fl1=节拍对齐 B0，发射脚本加 D2_FLUSH_BATCHES 旋钮。

**file-9 A/B**（D2_amt_bits19_3145728000_fl1_sf9_20260928_135010，exit 0，1h46m）：与 fl25 档（1h55m）同负载 38 窗——wall 4949→4411s（1.12x，601.8K→675.2K ops/s），Commit 占比 19%→23%（每批 Commit 代价符合预期），RSS 2.6→2.5G，复活 2402=2402，**终态根哈希与 fl25 逐位一致**（0xaef5ba48…）。同段前 197M ops vs B0b 原版 MPT（541.3K ops/s）：fl25 r=2.03、fl1 r=2.25（轻载早段，AMT 结构性占优，不可外推）。hx 疑嫌疑更正：hx/enc.go:205/258 解码即设 flags.hash、hasher.go:59 先查缓存、committer.go:47 干净子树 O(1) 短路——与原版对齐，无料可修。

**file-0 口径裁定（用户）**：file-9 空态起步只覆盖轻载早段、与带全量状态的 B0 后段不可比；统一回 file-0 起跑 vs B0 全程同段池化。已发射 D2_amt_bits19_6291456000_fl1_sf0_20260928_153921（sf0、fl1、修复 build、3 轮=6.29B ops、80 窗，预计 7-9h），分母 b0_mpt.csv，判据末 10 窗 r≥0.70。

## 10. sf0 三轮判分：修复+fl1 vs B0（2026-09-29）

run `D2_amt_bits19_6291456000_fl1_sf0_20260928_153921`（exit 0，10.8h，80 窗，6.29B ops，file-0 起跑）。分母 b0_mpt.csv 同区块段池化（Parse+Ops+Root+Write）：
- R1：AMT 527.1K / B0 290.5K，r=1.814（修复前 1.38），commit 19%
- R2：AMT 207.3K / B0 202.8K，r=1.022（修复前 0.82），commit 32%
- R3：AMT 99.6K / B0 218.1K，r=0.457（修复前 0.39），commit 52%
- **末 10 窗官方判据：r=0.390，FAIL（门 0.70）**，commit 56%；R3 窗内趋势仍在衰减（145K→80K ops/s，commit 43%→58%）
- 机制正常：热命中 99.51%、FPR 0.0039%、复活 179、State 9.04G、RSS 5.7G
- 结论：修复大幅改善 R1-R2，但 R3 稳态退化机理未被触动（commit/window 478→411s 仅 -14%）——瓶颈已不在批次拷贝，在 staged 写量增长（去重衰减）与 commit 路径其余构成。CSV 已存档本地同名目录。

## 11. 归档时间口径裁定（用户，2026-09-29）

归档维护时间（摘除/挂桶/桶序列化/ECMH/桶落盘，及 Commit 中"裁剪写脏"部分）**不计入 r 的分子**；分子=关键路径时间（Parse+业务读写含复活 servicing+Commit 中业务写脏与热节点部分）。归档总耗时作为独立后台预算指标照报，不得隐藏。当前同步持锁裁剪违反此口径（修复 A 异步化即对齐）。已报的 r=0.39（末10窗）为含裁剪的保守下界；staged 业务/裁剪分账是口径落地的必要统计，历史 CSV 无此拆分，正式 r 需带新统计重跑。

### 11.1 每批流水线模型（用户裁定 2026-09-29，已记入论文 repo EXPERIMENT_PLAN.md §5）

MPT=执行读写→生成树根；AMT=分片裁剪→**生成中间树根**→执行读写→生成树根。计耗时=读写+树根；不计=裁剪+中间树根（作为归档维护预算逐窗照报）。中间树根必须真实生成——否则下一次树根计算的批量写入被裁剪写脏放大且两账无法分离。工程红利：两次提交天然分账（中间树根提交=staged 裁剪账，树根提交=staged 业务账），无需脏路径快照启发式。

### 11.2 流水线模型落地实现（2026-09-29，门禁全绿）

按 §11.1 裁定实施，HEAD=502e94816 之上的未提交改动：

- **harness 每批流水线**（trace_stress_test.go）：分片裁剪 → 中间树根（独立 batch 提交+落盘，计时归入归档维护预算，不计入耗时分子）→ 业务读写 → 最终树根。开关 `-traceStressMPTIntermediateRoot`（默认 true，仅 mpt 热层生效）。计耗时=读写+最终树根（Comparative_Wall_ms 列语义自然修正）；真实墙钟 Measured_Wall_ms 仍含全部阶段。
- **mpt 侧**：pruneNextDomainLocked 在挂桶成功后置脏标记（原有缺口：追加进既有桶时无 createBucket 不置脏，写少批次会把裁剪脏推迟到业务提交——违背中间树根设计）；裁剪分相统计（摘出叶子数/解析节点数/摘除与挂桶分相）；Commit 四分量计时（hx 哈希收集/节点暂存/强制 pathdb Commit/归档桶与调度表循环）。
- **staged 物理分账**：按提交发生阶段归因——中间树根提交的 staged=裁剪账，最终树根提交的 staged=业务账（替代原计划的脏路径快照估算，后者已废弃）。
- **修复 A 预取**：mpt.Config.AsyncPrune 生效（此前被忽略）。裁完第 N 分片后单flight后台协程用独立只读 hx 树从最近已提交根预算化预热第 N+1 分片子树（预算 8192 节点），锁内摘除从磁盘读变缓存命中。仅 path 后端。
- **新 CSV 列**（追加尾部，既有列名不变）：Archive_Maint_ms、Intermediate_Root_ms、Prune_Extracted、Prune_Resolved_Nodes、Prune_Extract_ms、Prune_Mount_ms、Commit_Hx_ms、Commit_Stage_ms、Commit_TdbCommit_ms、Commit_ArchiveLoop_ms、Staged_Prune_Bytes、Staged_Ops_Bytes。
- **门禁**：gofmt/vet/build 干净；archive+mpt+hx 包测试全绿；新增神谕测试 TestMPTIntermediateCommitPreservesRoot（单提交 vs 双提交流水线终态根逐位一致+抽样读一致）、TestMPTPrefetchPreservesRoot（预取开/关终态根一致，-race 干净）。
- **下一步**：远端 file-0 验证档（口径裁定 2026-09-29：一律 file-0 起跑，不再用 file-9 短档；终态根须逐位等于 sf0 三轮档旧代码根 `0x5040277f10052051c3c23e20df5f6a5718cdcec38432866cfce5ec09ad206ef9`，新统计列同时出数）→ sf0 三轮正式档，出按新口径的正式 r（分子=Comparative_Wall_ms 口径，归档维护预算逐窗照报）。

### 11.3 file-0 三轮验证档发射（2026-09-29 11:24）

- run：`D2_amt_bits19_6291456000_fl1_sf0_20260929_112408`（D=19、fl1、file-0 起跑、3 轮=6,291,456,000 ops、AsyncPrune=true 经 mpt.Config 首次真实生效、中间树根默认开、ECMH 校验关）；远端编译门禁 2s 过后开跑，发射时磁盘余量 671.9GB。
- 上传文件即本地未提交工作区（diagnostics.go / trace_stress_test.go / trace_stress_hotlayer_test.go / mpt.go / pipeline_oracle_test.go / hx/trie.go，与门禁通过版本一致）。
- **验收**：①终态根逐位等于 `0x5040277f10052051c3c23e20df5f6a5718cdcec38432866cfce5ec09ad206ef9`（sf0 旧代码三轮档）；②新统计列出数（归档维护预算、中间树根耗时、裁剪分相、Commit 四分量、staged 分账）；③逐窗 r 曲线对照 0928 档（R2/R3 的 Prune_Launch 90s/窗尖峰应被预取压掉）。
- 预计 ~11h（0928 档旧代码 10.8h）。

### 11.4 归档循环全表扫描缺陷定位+修复（2026-09-29，验证档在跑中读取新列定位）

- **现象**：新统计列显示 R2 尾~R3 计耗时路径里 Commit_ArchiveLoop（桶序列化段）41s→190s/窗陡增，复活 1K→38K/窗。
- **根因**：commitToBatchLocked 的归档循环 `for range t.buckets` 全表扫描找脏桶；桶句柄（含 Cuckoo 过滤器）从不从 map 删除（淘汰只丢 payload），累计桶数随轮次单调增长（w55 ≈ 22 万个）；每批一次提交扫一遍 ≈ 10ms/批（w44→w55 增量拟合 ~37ns/桶=Go map 迭代速率，拟合严密）。桶脏了之后的真实处理（增量 ECMH O(改动条数)+整桶编码）仅 µs 级。**是 O(累计桶数)/提交 的实现缺陷，非论文机制成本**；中间提交同样扫一遍（其 207s/窗里约一半是扫描）。
- **修复（零语义改动）**：Trie 新增 dirtyBuckets 清单，三处置脏点（建桶/追加/删条目）改走 markBucketDirtyLocked 入列，提交只遍历清单 O(脏桶数)。本地门禁全绿：gofmt/vet/build、archive+mpt+ecmh+hx 包测试、两个流水线神谕（终态根等价）过。
- **生效时机**：在跑的 112408 验证档用的是修复前二进制（上传于修复前），其数据仍有效（神谕+账单）；修复随下一档生效。预计计耗时路径的 ArchiveLoop 从 ~10ms/批 降到 µs 级，严格口径 r 的 R3 投影从 0.40-0.45 大幅抬升。

### 11.5 验证档换修复版重跑（2026-09-29 17:32）

- 旧档 `..._20260929_112408`（修复前二进制，70% 进度）已人工终止：其验根价值被修复档覆盖（清单修复零语义、同神谕），账单价值因扫描缺陷失真。杀掉后远端干净、磁盘 662GB。
- 新档 `D2_amt_bits19_6291456000_fl1_sf0_20260929_173153`：同配置（D=19/fl1/file-0/3轮/ECMH关），仅增量上传 mpt.go（脏桶清单修复）。编译门禁 2s 过，首窗 1.24M ops/s。
- 验收不变：终态根=`0x5040277f10052051c3c23e20df5f6a5718cdcec38432866cfce5ec09ad206ef9`；末10窗新口径 r；归档维护预算账单。预计 ~8-9h（R3 不再背全表扫描）。
- 教训记录：pkill -f 的模式会匹配发起命令自身的 bash 命令行（自杀式无输出）；远端脚本里模式须写 'archive[.]test' 形式。

### 11.6 终态根分歧定位：过滤器重建的 map 序泄漏进状态根（2026-09-30）

- **现象**：修复档 `..._173153` 跑完（7.85h，末 10 窗 r=1.02 过门），但终态根 `0xb783240c…` ≠ 神谕 `0x5040277f…`。输入流全等（6.29B ops、增删读计数、复活 2,057,584 次、热读命中率 16 位小数全等）；两档 state_db 的 927,878 个归档桶载荷**逐字节全等**（68,447,655 条目零差异）。
- **排除链（全部实跑）**：挂桶置脏修复（单测证明旧码该路径本已置脏）；预取/中间树根/脏桶清单（D=4/200万 ops 五配置、D=13/1亿 ops 三配置根全一致）；hx 并行哈希竞态（全配置 -race 零告警）；中间树根×pathdb 陈旧聚合（单元神谕 pathdb 变体 PASS）。
- **根因**：`rebuildFilter`（桶创建/追加饱和时重建布谷鸟过滤器）用 `for key := range map` 遍历——Go 每次 range 顺序随机。实测：同一组 112 个键按三种顺序插入，序列化字节三种结果（槽位布局依赖插入序）。布局经 `b.stub().Filter` 序列化进 stubList，stub 在节点 blob 里，**blob 是状态根承诺的对象** → 每次重建都是一次随机掷骰。桶创建饱和率实测 0.01%/个 × 全程 92.7 万次创建 ≈ 每档 ~90 次重建 → **每档终态根都是一次独立抽奖**。
- **关键含义**：09-28 神谕 `0x5040…ef9` 本身就是随机 rollout，不能作验收基准；0928 与 173153 两档无优劣之分，两侧操作语义完全正确（这也正是"操作结果全等、归档载荷全等、只有根不同"的原因）。此前 0925/0928 未曾有两档完成态同配置对比，"根可复现"从未被真正验证过。
- **修复**：`rebuildFilter` 先 `sort.Strings` 键再插入（规范序）。修复后根重新成为操作流的确定性函数（饱和事件本身由操作流决定，确定性）。审计其余 map 遍历：encodeBucket 先排序 ✓、ECMH 为无序点加和 ✓、桶条目导出先排序 ✓、其余皆切片遍历 ✓——无第二处泄漏。
- **验证**：新增 `TestMPTRootDeterministicUnderFilterRebuild`（小过滤器几何强制饱和重建，同 workload 两次建根比对）：修复前 **5/5 FAIL**（每次根都不同），修复后 5/5 PASS。`TestFilterPlacementOrderDependence`（cuckoo 包）留档证明布局泄漏机制。门禁：四包+ecmh+hx 全绿，两神谕+桶 ECMH 单测全 PASS。
- **后续**：修复版重跑 6.29B（档 `..._20260930_*`）建立新神谕；判分口径不变（新根不应对齐 0x5040，对齐"自身可复现+计数器全等"）。

### 11.7 确定性修复档收档：验收通过，新神谕确立（2026-10-01）

- **档卡片**：`D2_amt_bits19_6291456000_fl1_sf0_20260930_143714`；09-30 14:37 发射，22:31 跑完，exit=0，全程 7.9h（与 173153 的 7.85h 持平——排序修复零开销实证）。
- **①计数器全等 ✓**：gets/puts/deletes/增删读写触五计数/热读命中率（16 位小数）/布谷鸟假阳性数（150,743）与 173153 档逐项全等（过滤器查找数差 18/35 亿=预取时序噪声）。
- **②新神谕根**：`0x7d5f09c7e3f6462f991c878c74f45e217b82084f437b8886b1fde57f454d556e`。旧神谕 0x5040（旧代码随机 rollout）与 173153 的 0xb783 均作废。可复现性由 `TestMPTRootDeterministicUnderFilterRebuild` 钉死；铁证需第二档同根复现（待用户裁定要不要烧）。
- **③正式 r（Comparative 口径，末 10 窗）= 1.02 ≥ 0.70 过门 ✓**：R1 1.80 / R2 1.50 / R3 1.11；末 10 窗 AMT 220.8K vs B0 217.2K ops/s。与 173153 读数差 <0.5%（噪声）。
- **④归档预算账单**：Archive_Maint 合计 4367s = 墙钟 18.3%（173153 为 4355s/18.4%，持平）。
- **验收三条件总评**：①终态根=新神谕（确立）✓ ②新统计列出数 ✓ ③裁剪尖峰被预取压掉 ✓。**file-0 三轮验证档全绿，E2 是否发射待用户裁定。**
- 备注：验收 cron（57cc360e09ba）23:30 触发但 error（产物已拉齐到本地后出错，与前一晚同款）；本次判分为人工本地完成。

## 12. Archive_Bytes 统计落地 + 计划口径收口（2026-10-01）

- **实现**：mpt 增 archiveBytes 增量计数（建桶 +=桶记录编码尺寸(9 头+Σ8+k+v)、追加 +=8+k+v/条、删条目 -=同、销毁 -=9 头），调度记录升 v3（22B，追加 8 字节计数）持久化跨重载；commit 时发布诊断 gauge；CSV 尾部追加 Archive_Bytes/System_Total_Bytes 两列、summary.json 同步（system_total=state_db du，原型单库故与 state_bytes 等值，单列以便将来分库口径不变）。既有列名/语义不变。
- **门禁**：gofmt/vet/build/五包测试全绿；新增 TestMPTArchivePayloadBytesAccounting（gauge=Σlen(encodeBucket) 逐字节对拍+重载持久化+复活全归零）与 TestMPTArchivePayloadBytesAppendSplit（250 条强制跨桶轮转合计口径）。
- **EXPERIMENT_PLAN.md 已更新（论文 repo，未提交，用户自行提交）**：§0 新增 4 条（fl1 口径成文、0x5040 神谕作废标注+0x7d5f 现行神谕、Archive_Bytes 落地、manifest 纪律修订=远端非 git 故以本地冻结提交+台账对应 run↔commit）；§2 补三字节口径定义（含 State−Archive 近似式的压缩口径差注明）；§5.2 fl25→fl1+预取开；§5 窗口指标补两列。
- **遗留讨论项（待用户裁定）**：RSS 2.70× 按计划字面判 F（GOGC 治理挂账 vs 阈值口径重议）；论文 K=2²⁰ vs 实验 2¹⁹；E3 逐次裁剪统计粒度（CSV 仅窗聚合）；archive-first 持久化不原子。
