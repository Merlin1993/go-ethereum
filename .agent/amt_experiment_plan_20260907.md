# AMT 真实轨迹压测 · 实验手册（2026-09-07）

读者：只负责跑实验的人（实验线程）。本文件自足，不需要读其它文档就能发车。代码改动见 [amt_dev_plan_20260907.md](/D:/go_workspace/go-ethereum/.agent/amt_dev_plan_20260907.md)。

范围：只做 **AMT（非 stem，`stem_mode=false`）** 与 MPT、Verkle 的对比。纯压测，不重放、不校验状态根。

发车前提：A1–A4 完成 + 本地 `go test ./trie/archive` 全绿 + §1 环境复核通过 + 用户确认本文件。

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

## 1. 实验机、凭据、路径

| 项 | 值 |
|---|---|
| 主机 | `192.168.2.230`（hostname `titanide`），20 核，59 GiB RAM，`/` 1.9 TB ext4（2026-09-07 实测） |
| 凭据 | 本地 `D:\go_workspace\go-ethereum\.agent\asct_remote_servers.local.json` → 键 `asct_mpt` 的 `ssh_user` / `ssh_password`（paramiko，密码登录）。**该 JSON 的 `host` 字段写的是 `192.168.3.51`，已失效，主机一律以本表为准**；密码绝不打印 |
| 源码 | `/root/asct_codex/go-ethereum-trace`（由 launcher 上传本地 `trie/archive/*.go` + `core/tree_test/trace_compare_test.go`，不 git pull） |
| Go | `/usr/local/go/bin/go`（go1.26.0 linux/amd64） |
| 环境 | `GOCACHE=/root/asct_codex/go-build-cache`，`GOTMPDIR=/root/asct_codex/trace-tmp`（发车前 `mkdir -p`） |
| 结果根 | `/root/asct_codex/results/{trace_stress,trie_compare}` |
| 磁盘守卫 | `/root/asct_codex/full_replay_disk_guard.sh`（源：`.agent/asct_trace_stress_disk_guard.sh`），阈值 `MIN_FREE_BYTES=8000000000`，触发即 `kill -TERM` 并 exit=2 |

发车前必查（本地执行，输出不含密码）：

```powershell
cd D:\go_workspace\go-ethereum
python .agent/remote_full_replay_preflight.py   # 核数/内存/go 版本/磁盘/数据集/源码/残留进程
```

要求：`PROCESSES` 段除自身外为空；`df` 空闲 ≥ 8 GB（守卫阈值），B3 发车前建议 ≥ 120 GB（2026-09-07 实测空闲 355 GB，`results` 已占 31 GB）。空间不足时报旧结果目录清单给用户确认，**不擅自删除**。

## 2. 数据集（唯一输入）

`/root/asct_codex/mainnet_state_access_trace/range_10m/`，`du -sb` 实测 411,266,874,333 字节（384 GiB）：`manifest.json`、`SHA256SUMS`、10 个 `state_access_trace_<起block>_<止block>.csv.gz`、生成时留下的 `trace_state_db/`（只读，不要动）。

block 46,147 → 10,000,000；9,953,854 块（1,455,084 空块）；697,373,173 笔交易；**20,242,112,513 次操作**（read 15.26 B / write 4.02 B / touch 841 M / create 77 M / delete 44 M）。

列 `block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len`；key 由 `trieutils.BinaryTree{BasicData,StorageSlot,CodeChunk}Key` 派生，`read,touch→get`、`write,create→put`、`delete→delete`，值是等长合成 payload。

key 随机性（结论，细节不复述）：位置哈希随机、无地址聚簇，shard 路由均衡；同账户关联 key 共享 248 位前缀（单前缀 ≤256 key），AMT 下各自独立归档；**执行顺序刻意不打乱**。

负载极偏：前 4 片（46 k–4 M）占 4.5% 字节，后 6 片（4 M–10 M）占 95.5%。因此**所有短阶段（B0b/B1/B2）都从 `-traceStressStartFile=9` / `-traceCompareStartFile=9`（block 9,046,147 起，本片约占 14.4%）起跑**，让探针与标定落在真实重负载段；正式长阶段（B0/B3/B4）从 file 0 跑完整轨迹。论文叙述只在 ≥4 M 区间做等预算比较。

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

正式阶段关键参数（写死，metadata 必须能反推）：`stem_mode=false`、`ShardDepth=D`（B2 标定值）、`BatchSize=4000`、`MetricsBatches=2500`、`PruneEveryBatches=1`、`AccessSampleEvery=1000`、`CuckooBuckets=32`、`CuckooSlots=4`（→ 桶容量 100，与论文一致）、`NodeCacheMB=512`、`CommitWorkers=16`、`AsyncPrune=true`、`DestructiveCommit=true`、`activate_archived_key_on_read=true`。

## 4. 阶段（门控串行）

| 阶段 | 引擎/配置 | 预算 | 预估 | 产出 / 门 |
|---|---|---:|---:|---|
| B0 | MPT 全量 **进行中**（2026-09-08 08:52 发车，`B0_mpt_20242112513_20260908_085221`） | 20,242,112,513 | 12–20 h | 终局参照：末段 10 窗均值 `M_late`、最低窗 `M_min`、`State_Bytes` 曲线 |
| B0b | MPT，file 9 | 500,000,000 | 0.3–0.5 h | B1 的配对参照（同段、fresh 建树） |
| B1 | AMT 无归档（`-traceStressDisableArchive=true`），file 9 | 500,000,000（50 窗） | 2–4 h | 结构探针比值 `r`，见 §4.1 |
| B2 | AMT 开归档，file 9，D ∈ {15,16,17} 各 1.5 归档轮 | 196,608,000 / 393,216,000 / 786,432,000（合计 1.38 B） | 6–9 h | 定 D，见 §4.2 |
| B3 | AMT 全量（选定 D，读激活开） | 20,242,112,513 | 70–140 h | G1/G2/G3/G4 主数据 + 终态存储扫描 |
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

### 4.2 B2 D 标定

三跑只差 `-traceStressShardDepth`。一轮 = `2^D × 4000` 次操作，跑到 1.5 轮为止（按轮归一，否则热命中率不可比）。选值规则，按序取第一优先：

1. 1.5 轮处 `Hot_Read_Hit_Rate` 最高（三档都 <90% 时取最高档并记为待优化，不硬停）；
2. `Runtime_Filter_FPR` ≤ 1%；
3. 末窗 `State_Bytes` 最低；
4. 池化 ops/s 不明显劣化（相对最优 D 低 >15% 视为劣化）。

升级顺序（三档全不达标才动，一次只动一个）：`BatchSize` 4000→2000/8000 → `NodeCacheMB` → Cuckoo 形状（32×4 为基线，16×4 桶容量 60 只作诊断）。

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

B2 标定（每个 D 一次，`Ops` 按 §4 表）：在 B1 基础上改 `-traceStressDisableArchive=false`、`-traceStressActivateArchivedKeyOnRead=true`、`-traceStressShardDepth=<D>`、`-traceStressFinalStats=true`，目录 `B2_amt_D<D>_<ops>_$S`。

B3 AMT 全量：`-traceStressOps=20242112513 -traceStressStartFile=0`、`-traceStressShardDepth=<选定 D>`，其余同 B2，目录 `B3_amt_20242112513_D<D>_$S`。跑完取 `results/summary.json` 的 `last_root` 追加一次存储扫描（G2 权威口径）：

```bash
ROOT=$(python3 -c "import json;print(json.load(open('$RES/trace_stress/B3_amt_20242112513_D<D>_$S/results/summary.json'))['last_root'])")
/usr/local/go/bin/go test ./trie/archive -run '^TestArchiveStemTraceStorageStats$' -count=1 -timeout 0 -v -args \
  -traceStatsBaseDir=$RES/trace_stress/B3_amt_20242112513_D<D>_$S -traceStatsRoot=$ROOT \
  -traceStatsOutput=$RES/trace_stress/B3_amt_20242112513_D<D>_$S/results/storage_breakdown_20260820.json \
  -traceStressShardDepth=<选定 D>
```

B5 消融：复制 B3 配置，`-traceStressActivateArchivedKeyOnRead=false` + 短预算，目录 `B5_amt_noactivation_<ops>_$S`。

实际发车不手敲上面命令：用 launcher（§5）包成 nohup 后台任务，一次一个阶段。

## 5. 发车与状态

launcher：以 `.agent/run_remote_full_three_20260904.py` 为模板改出 AMT 版（本地运行，paramiko 上传源码+守卫、跑回归、写 `run_full_replay_<STAMP>.sh` 后 nohup）。远端 launcher 文件名**保持 `run_full_replay_` 前缀**，状态脚本才能按 stamp 找到它；阶段名（B0/B1/…）写进 `status.log`。它已实现的能力必须保留：发车前 preflight（目录不存在、无残留进程、磁盘 ≥ 阈值）、上传文件后 SHA256 记录、`status.log` / `<run>.exit` / `<run>.log` 三件套、磁盘守卫随进程存活、阶段失败置 `overall=1`、守卫 exit=2 时后续阶段 `skip`（记 129）、只跑被批准的阶段（B0/B0b/B1 先发车，B2 之后每轮由人确认）。

查状态 / 停任务：

```powershell
python .agent/remote_full_replay_status.py      # launcher 状态、磁盘、进程、最新 CSV 行
```

停止：`pkill -TERM -f 'run_full_replay_<STAMP>.sh'` 后再 `pkill -TERM -f 'archive.test|tree.test'`（顺序不能反，否则守卫会误判）。

## 6. 失败处理

`*.exit` 非 0 → 读同目录 `.log` 尾部定位；`exit=2` 是磁盘守卫停机，先处理空间再从该阶段整段重跑；进程消失但无 `.exit` → 视为失败同样整段重跑。**不做断点续跑**（预算已按"跑 4 天就说明不达标"处理）。任何阶段的产物目录都不复用：失败目录改名加 `_failed` 后缀留档。

## 7. 不做与已知限制

不做：重放/状态根校验；prune 绝对耗时对 G1 的影响；归档层自身的存储优化；ASCT 全量与 stem A/B；并行跑引擎；合成 Cuckoo FP 标定；`TestArchiveStemTraceFilterFPStats`。

限制（论文须写）：值是合成等长 payload 不是真实 RLP；计时构成两边不对称（`Commit_ms` vs `Root_ms`）且冷维护不入分子；G3/G4 对 MPT/Verkle 天然 N/A；单机、串行、无重复实验；三引擎都从创世重建，不是主网快照的真实状态分布。
