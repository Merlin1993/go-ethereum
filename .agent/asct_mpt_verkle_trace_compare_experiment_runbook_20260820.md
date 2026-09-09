# ASCT / MPT / Verkle trace 对比实验操作手册

日期：2026-08-20  
正式实验日期：2026-08-18  
适用目的：在完全相同的有序 mainnet state-access trace 上比较 ASCT、MPT、Verkle 的结构层访问性能。缓存开关不是本实验的主变量；ASCT 使用固定正常配置。

## 1. 实验目标

本实验回答的问题是：

> 同一条 10 亿操作 mainnet state-access trace、相同 32-byte structural key、相同 value 映射、相同 4000-op commit 节奏下，当前仓库中的 ASCT、MPT、Verkle 各自吞吐、耗时分布、内存和状态库大小如何？

它不回答以下问题：

- 不测量生产客户端完整 EVM replay 吞吐。
- 不比较 ASCT cache on/off 的最终性能结论；cache 数据只用于诊断热点。
- 不把 MPT 结果解释为 canonical secure-MPT account/storage layout 的生产性能。
- 不要求三个引擎输出相同 root；三棵树的编码和 commitment 不同。

正确性对齐标准是：相同 trace 文件、相同操作顺序、相同 key/value/delete 映射、相同 batch 边界、相同操作计数，并且每个 run 正常完成。

## 2. 固定实验条件

### 2.1 输入 trace

```text
/root/asct_codex/mainnet_state_access_trace/range_10m/state_access_trace_009046147_010000000.csv.gz
```

范围和规模：

```text
blocks          9,046,147 .. 9,372,346
operations      1,000,000,000
batches         250,000
batch size      4,000 operations
metrics window  2,500 batches = 10,000,000 operations
```

CSV header：

```text
block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len
```

### 2.2 Key / value / delete 映射

三引擎统一使用 32-byte BinaryTree/Verkle structural key：

```text
account: trieutils.BinaryTreeBasicDataKey(address)
storage: trieutils.BinaryTreeStorageSlotKey(address, slot)
code:    trieutils.BinaryTreeCodeChunkKey(address, chunk)
```

value 规则：

```text
优先使用 trace 中 32-byte value_hash；
缺失时使用确定性 Keccak(key, operation)。
```

特殊 delete 规则：

```text
storage write 且 value_hash 等于 zero storage hash 时，执行 delete。
```

该规则使 executed delete 从 trace 的 2,972 条变成 74,586 条。三个引擎完全使用同一规则。

### 2.3 主机

```text
host     titanide / 192.168.2.230
CPU      13th Gen Intel Core i5-13600K, 20 logical CPUs
RAM      58 GiB
disk     NVMe
kernel   Linux 6.8
Go       /usr/local/go/bin/go, go1.26.0 linux/amd64
```

远程源码：

```text
/root/asct_codex/go-ethereum-trace
```

远程结果根目录：

```text
/root/asct_codex/results/trace_stress
/root/asct_codex/results/trie_compare
```

本机访问不要依赖 plain `ssh` key。已有凭据在：

```text
D:\go_workspace\go-ethereum\.agent\asct_remote_servers.local.json
```

使用其中 `asct_mpt` entry，并把 host 覆盖为 `192.168.2.230`。文档中不复制密码。

## 3. 驱动入口

### 3.1 ASCT

源码：

```text
D:\go_workspace\go-ethereum\trie\archive\trace_stress_test.go
```

入口：

```text
TestArchiveStemTraceStress
```

正式配置：

```text
ShardDepth             20
StemMode               true
Stem cache             65,536 entries / 128 MiB
Node cache             4,194,304 entries / 512 MiB
CommitWorkers          16
AsyncPrune              true
PruneEveryBatches       1
DestructiveCommit       true
PhysicalDelete          false
batch                   4,000
metrics window          2,500 batches
```

ASCT 正式 1B 命令：

```bash
cd /root/asct_codex/go-ethereum-trace
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp
mkdir -p "$GOTMPDIR" /root/asct_codex/results/trace_stress

run=/root/asct_codex/results/trace_stress/formal_1b_cache_on_depth20_batch4000_20260818_0832
test ! -e "$run"

/usr/local/go/bin/go test ./trie/archive \
  -run '^TestArchiveStemTraceStress$' \
  -count=1 \
  -timeout 0 \
  -v \
  -args \
    -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \
    -traceStressBaseDir="$run" \
    -traceStressOps=1000000000 \
    -traceStressBatchSize=4000 \
    -traceStressMetricsBatches=2500 \
    -traceStressStartFile=9 \
    -traceStressFinalStats=true \
  2>&1 | tee "${run}.log"
```

`final_stats=true` 会额外统计最终结构，耗时计入 end-to-end elapsed，但不进入 comparative throughput。

计时口径：

```text
Comparative_Wall_ms = Operations_ms + Commit_ms + DB_Write_ms
```

ASCT 的 `Prune_Launch_ms` 是独立诊断字段，不计入 `Commit_ms`，也不计入 comparative throughput。end-to-end elapsed 仍包含 prune，用于运行管理，不用于引擎性能对比。

2026-08-18 formal run 的原始 summary 生成于口径修正前，其 raw `measured_ops_per_s` 包含 1.158s prune launch。分析脚本已按上式重算；后续新 driver 直接输出 `Comparative_Wall_ms`、comparative batch quantiles 和排除 prune 的 summary throughput。

### 3.2 MPT / Verkle

源码：

```text
D:\go_workspace\go-ethereum\core\tree_test\trace_compare_test.go
```

入口：

```text
TestTrieTraceCompare
```

固定条件：

```text
engine                 mpt 或 verkle
LevelDB cache          512 MiB
open files             256
node storage           path scheme
PathDB diff layers     0，强制每批 diffToDisk
PathDB clean cache     256 MiB
PathDB write buffer    256 MiB
snapshot limit         disabled
Verkle point cache     1,024 entries
batch                  4,000
metrics window         2,500 batches
```

正式 1B launcher：

```text
D:\go_workspace\go-ethereum\.agent\run_trie_compare_1b_forcepath_20260818.sh
```

在远端执行：

```bash
cd /root/asct_codex/go-ethereum-trace
bash .agent/run_trie_compare_1b_forcepath_20260818.sh 20260818_1215
```

等价单引擎命令示例：

```bash
cd /root/asct_codex/go-ethereum-trace
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp

engine=mpt   # 第二轮改为 verkle
run=/root/asct_codex/results/trie_compare/formal_1b_forcepath_${engine}_batch4000_20260818_1215
test ! -e "$run"

/usr/local/go/bin/go test ./core/tree_test \
  -run '^TestTrieTraceCompare$' \
  -count=1 \
  -timeout 0 \
  -v \
  -args \
    -traceCompareInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \
    -traceCompareEngine="$engine" \
    -traceCompareBaseDir="$run" \
    -traceCompareOps=1000000000 \
    -traceCompareBatchSize=4000 \
    -traceCompareMetricsBatches=2500 \
    -traceCompareStartFile=9 \
  >"${run}.log" 2>&1
```

MPT 和 Verkle formal run 必须串行执行，避免互相抢 NVMe、page cache 和 CPU。

## 4. 跑前准备

### 4.1 源码和构建

在远端确认：

```bash
cd /root/asct_codex/go-ethereum-trace
git status --short
/usr/local/go/bin/go version
df -h /root/asct_codex
```

至少需要约 30 GiB 可用空间。正式三引擎的状态库合计约 3.6 GiB，但日志、Go build cache、trace 解压缓冲和后续 profile 需要更多余量。

构建缓存：

```bash
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp
mkdir -p "$GOCACHE" "$GOTMPDIR"
```

### 4.2 smoke run

先各跑 10M，确认入口、trace header、key 映射、输出目录和 DB 打开逻辑：

```bash
go test ./trie/archive \
  -run '^TestArchiveStemTraceStress$' -count=1 -timeout 0 -v \
  -args \
    -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \
    -traceStressBaseDir=/root/asct_codex/results/trace_stress/smoke_asct_10m \
    -traceStressOps=10000000 \
    -traceStressBatchSize=4000 \
    -traceStressMetricsBatches=2500 \
    -traceStressStartFile=9 \
    -traceStressFinalStats=false

go test ./core/tree_test \
  -run '^TestTrieTraceCompare$' -count=1 -timeout 0 -v \
  -args \
    -traceCompareInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \
    -traceCompareEngine=mpt \
    -traceCompareBaseDir=/root/asct_codex/results/trie_compare/smoke_mpt_10m \
    -traceCompareOps=10000000 \
    -traceCompareBatchSize=4000 \
    -traceCompareMetricsBatches=2500 \
    -traceCompareStartFile=9
```

把 engine 和输出目录替换后，同样跑 Verkle。smoke 结果不进入正式对比。

### 4.3 100M validation pilot

正式 1B 前先跑 100M，重点确认：

- MPT/Verkle 能完成含 delete 的 trace。
- PathDB 每批 durable commit 后可 reload root。
- 100M 内无 panic、无 root reload 失败、无操作计数漂移。
- ASCT 与 MPT/Verkle 的 trace classification 计数一致。

正式归档中的 validation pilots：

```text
F:\codex_asct\plot_data\20260818_trie_trace_compare_1b\validation_pilots
```

## 5. 正式跑监控

ASCT：

```bash
tail -f /root/asct_codex/results/trace_stress/formal_1b_cache_on_depth20_batch4000_20260818_0832.log
du -sh /root/asct_codex/results/trace_stress/formal_1b_cache_on_depth20_batch4000_20260818_0832/state_db
pgrep -af 'archive.test|TestArchiveStemTraceStress'
```

MPT/Verkle：

```bash
tail -f /root/asct_codex/results/trie_compare/formal_1b_forcepath_mpt_batch4000_20260818_1215.log
tail -f /root/asct_codex/results/trie_compare/formal_1b_forcepath_verkle_batch4000_20260818_1215.log
cat /root/asct_codex/results/trie_compare/formal_1b_forcepath_mpt_batch4000_20260818_1215.exit
cat /root/asct_codex/results/trie_compare/formal_1b_forcepath_verkle_batch4000_20260818_1215.exit
```

每 10M window 会输出一行 rate 和 state DB 大小。若 RSS 或 DB 增长异常，先保存当时的 CSV 和日志，不要直接删除 run 目录。

## 6. 正确性验收

每个 formal run 必须满足：

```text
exit / test status       PASS 或 0
summary.status           completed
operations               1,000,000,000
batches                  250,000
CSV rows                 100
last CSV Total_Operations 1,000,000,000
last CSV Total_Batches    250,000
read failures             ASCT final_stats 中为 0
```

三引擎 trace 分类计数必须一致：

```text
reads          793,382,002
touches         33,440,025
writes         170,668,785
creates           2,506,216
trace deletes         2,972
```

三引擎 executed 计数必须一致：

```text
executed gets     826,822,027
executed puts     173,103,387
executed deletes      74,586
```

最终 root：

```text
ASCT    0x5b64116749d52c3a84e3af29bd2ed474119e91871e5c411bc2d7cb38ae1d9acf
MPT     0xc2ff315fdc575fb2c887f75d185791a3653e74a766896433fbe4083378ad45d5
Verkle  0x08c669f205ff5e68c4583a892e4b1f44984ab46198f8c3cba22227e8be825536
```

这些 root 不应跨引擎相等。相同 root 只用于同一引擎、同一输入、不同物理 run 的复现校验。

## 7. 必需修复

正式 MPT/Verkle 结果依赖以下三处修复。若在新 checkout 重跑，必须确认它们存在。

### 7.1 PathDB 每批落盘

问题：仓库中 `common.VerkleLayerCount` 为 1，PathDB 会保留 diff layer，MPT/Verkle 并非每批 durable commit，和 ASCT 节奏不一致。

修复：`TestTrieTraceCompare` 临时把：

```go
common.VerkleLayerCount = 0
```

并在 defer 中恢复原值。位置：

```text
core/tree_test/trace_compare_test.go:349
```

### 7.2 Verkle suffix-half delete panic

问题：`LeafNode.Delete` 删除某 half 的最后一个 value 时把 `c1` 或 `c2` 置 nil，后续 `BatchSerialize` panic。

修复：置为 identity point，并加入：

```text
go-verkle TestDeleteLastValueInSuffixHalf
```

相关文件：

```text
trie/verkle.go
go-verkle/tree.go
go-verkle/tree_test.go
```

### 7.3 MPT clean batch panic

问题：clean batch 或 root unchanged 时 `nodes=nil`，直接走 PathDB diff insertion 会 panic。

修复：nodes 为空或 root 未变化时跳过 diff insertion，但仍保存 root 并 reload trie。

位置：

```text
core/tree_test/trace_compare_test.go
```

## 8. 本地回归命令

正式跑前已完成：

```powershell
go test ./core/tree_test -run '^TestTrieTraceCompare$' -count=1
go test ./trie -run '^$' -count=1
Push-Location go-verkle
go test . -run '^TestDeleteLastValueInSuffixHalf$' -count=1
Pop-Location
python -m py_compile .agent/analyze_trie_compare.py
```

`go-verkle` 全量测试当时已有 unrelated proof / compatibility / timing failures，不作为本实验验收标准。

## 9. 汇总分析和存储拆分

分析脚本：

```text
D:\go_workspace\go-ethereum\.agent\analyze_trie_compare.py
```

在小产物归档后运行：

```powershell
python .agent\analyze_trie_compare.py `
  --asct  F:\codex_asct\plot_data\20260818_trie_trace_compare_1b\asct_1b `
  --mpt   F:\codex_asct\plot_data\20260818_trie_trace_compare_1b\mpt_1b_forcepath `
  --verkle F:\codex_asct\plot_data\20260818_trie_trace_compare_1b\verkle_1b_forcepath `
  --output-dir F:\codex_asct\plot_data\20260818_trie_trace_compare_1b\analysis
```

输出：

```text
engine_summary.csv
stage_metrics_100m.csv
stage_metrics_100m_wide.csv
```

指标口径：

- `Measured_Ops_Per_Sec`：统一输出为 operations / comparative wall；即 operations / (operations + commit/root + DB write)。
- 2026-08-18 formal CSV 已按 `Operations_ms + Commit_ms/Root_ms + DB_Write_ms` 后验重算，ASCT `Prune_Launch_ms` 被排除。
- `Prune_Launch_s`：ASCT 裁剪启动时间，只作诊断，不进对比。
- `Core_Wall_s`：elapsed - parse - final stats，仅用于核对，可能包含少量调度/收尾差异。
- `Operations_s`：Get/Update/Delete 循环本身。
- `Commit_Or_Root_s`：ASCT Commit_ms；MPT/Verkle Root_ms。
- `DB_Write_s`：LevelDB batch write / PathDB commit 和 root 保存。
- `Stage_End_Ops`：每 10 个 10M window 聚合成 100M。

### 9.1 ASCT active/archive 存储拆分

磁盘对比只使用 ASCT 非 archive 部分；archive payload 是 ASCT 附加能力，不进入 MPT/Verkle 性能对比。正式 1B run 后用 final root 对既有 DB 做只读 exact scan：

```bash
cd /root/asct_codex/go-ethereum-trace
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp

/usr/local/go/bin/go test ./trie/archive \
  -run '^TestArchiveStemTraceStorageStats$' \
  -count=1 -timeout 0 -v \
  -args \
    -traceStatsBaseDir=/root/asct_codex/results/trace_stress/formal_1b_cache_on_depth20_batch4000_20260818_0832 \
    -traceStatsRoot=0x5b64116749d52c3a84e3af29bd2ed474119e91871e5c411bc2d7cb38ae1d9acf \
    -traceStatsOutput=/root/asct_codex/results/trace_stress/formal_1b_cache_on_depth20_batch4000_20260818_0832/results/storage_breakdown_20260820.json \
    -traceStressShardDepth=20
```

结果：

```text
ActiveOnlyLogicalBytes       1,408,837,104 bytes  1.312 GiB
ArchivedPayloadLogicalBytes    134,715,885 bytes  0.125 GiB
ReachableLogicalBytes        1,543,552,989 bytes  1.438 GiB
StorageBreakdownValid        true
ReadFailures                 0
```

解释边界：

- `ActiveOnlyLogicalBytes` 是非归档对比口径。
- `ArchivedPayloadLogicalBytes` 只诊断归档能力，不进入性能对比。
- logical bytes 统计 reachable record 的 key + value 长度，不包含 LevelDB WAL、obsolete versions、SST metadata 和 compaction overhead。
- MPT/Verkle 表中的 state size 是 LevelDB 物理目录大小，不能和 ASCT logical bytes 直接算精确百分比。
- `State_Bytes` / `state_bytes` 对 ASCT 是未拆分总目录，只作运行监控，不作为跨引擎磁盘结论。
- 后验 JSON 里的 `state_bytes` 是重新打开 LevelDB 后的目录大小，可能因后台 compaction 与 run 结束时 summary 的 `state_bytes` 不同；拆分结论只看 logical byte counters。

### 9.2 归档收益边界

formal 1B run 的归档诊断不能当作归档收益达标证据：

```text
Archived KV records                      479,685
Archived logical suffix values             820,808
Active logical suffix values             6,696,539
Archived logical-value share                 10.92%
Archived payload / reachable bytes           8.73%
Bucket average fill                      2.62 / 60
Bucket P50 / P95 / max                    2 / 6 / 16
Root StubList buckets                    143,745
Ordinary child buckets                        0
Prune cursor advances / depth-20 shards  250,000 / 1,048,576
Prune ring coverage                         23.84%
```

因此该 run 只是跨引擎性能对比，不是稳态归档能力验证；没有完成一个 rolling-epoch shard ring，也没有 archive-on/archive-off 的存储节省基线。后续归档专项实验应至少完成一个完整 prune cycle，并输出 bucket fill / placement、cold-read、proof、RSS、active logical bytes 和 compaction 后 physical bytes。归档专项结果不得混入 MPT/Verkle 性能排名。

## 10. ASCT 10M CPU profile

profile 只用于热点定位，不作为正式性能成绩。10M 处于冷启动阶段，不能代表 1B 后期工作集。

命令：

```bash
cd /root/asct_codex/go-ethereum-trace
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp

/usr/local/go/bin/go test ./trie/archive \
  -run '^TestArchiveStemTraceStress$' \
  -count=1 -timeout 0 -v \
  -cpuprofile=/root/asct_codex/results/trace_stress/asct_profile_10m_20260820.prof \
  -args \
    -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \
    -traceStressBaseDir=/root/asct_codex/results/trace_stress/profile_10m_cache_on_20260820 \
    -traceStressOps=10000000 \
    -traceStressBatchSize=4000 \
    -traceStressMetricsBatches=2500 \
    -traceStressStartFile=9 \
    -traceStressFinalStats=false
```

查看 profile：

```bash
cd /root/asct_codex/go-ethereum-trace
go tool pprof -top -cum -nodecount=50 \
  /root/asct_codex/results/trace_stress/asct_profile_10m_20260820.prof

go tool pprof -peek 'StemTrie\.(Get|Put)|loadStoredStemWithCache|GetFlatValue|cloneStem' \
  /root/asct_codex/results/trace_stress/asct_profile_10m_20260820.prof
```

本地归档：

```text
F:\codex_asct\plot_data\20260818_trie_trace_compare_1b\profile\asct_10m_20260820
```

包含：

```text
asct_profile_10m_20260820.prof
metadata.json
run_status.json
results/summary.json
results/asct_trace_stress.csv
pprof_top_cumulative.txt
pprof_archive_functions.txt
```

注意：CPU profile 的 `Total samples` 可能超过 wall duration，因为 GC/commit worker 并行采样。百分比按 CPU samples 解释，不能把 sample 秒数直接当 wall 秒数。

## 11. 小产物归档

本地归档：

```text
F:\codex_asct\plot_data\20260818_trie_trace_compare_1b
```

只归档：

```text
asct_1b/
mpt_1b_forcepath/
verkle_1b_forcepath/
validation_pilots/
analysis/
profile/
source/
README.md
SHA256SUMS
```

每个 formal run 归档：

```text
metadata.json
run_status.json
go_test.log 或 *.log
go_test.exit 或 *.exit
results/summary.json
results/*.csv
results/storage_breakdown_*.json   # ASCT formal run
```

不归档远端 LevelDB state DB。远端正式 state DB 保留在：

```text
/root/asct_codex/results/trace_stress/formal_1b_cache_on_depth20_batch4000_20260818_0832/state_db
/root/asct_codex/results/trie_compare/formal_1b_forcepath_mpt_batch4000_20260818_1215/state_db
/root/asct_codex/results/trie_compare/formal_1b_forcepath_verkle_batch4000_20260818_1215/state_db
```

更新 checksum 时在归档根目录执行，输出使用 ASCII、不带 BOM：

```powershell
$files = Get-ChildItem -Recurse -File |
  Where-Object { $_.Name -ne 'SHA256SUMS' }
$lines = foreach ($f in $files) {
  $h = (Get-FileHash -LiteralPath $f.FullName -Algorithm SHA256).Hash.ToLower()
  $rel = [IO.Path]::GetRelativePath((Get-Location).Path, $f.FullName)
  "$h  $rel"
}
Set-Content -LiteralPath SHA256SUMS -Value $lines -Encoding ASCII
```

不要用通配删除 state DB 或结果目录；清理远端大目录前必须逐个确认完整绝对路径。

## 12. 结果边界

引用结果时必须说明：

1. 这是 structural-layer trace benchmark，不是 full EVM replay。
2. MPT 使用 raw 32-byte structural key，不是 canonical secure-MPT layout。
3. value 是 `value_hash` 或确定性替代值，不是完整 account/storage RLP payload。
4. 每批 4000 op、每批 durable commit，是对 commit 频率的 stress 设定。
5. 每引擎一台机器、一次 formal run；报告的是本次配置下的相对表现。
6. ASCT 裁剪耗时和归档存储只作诊断/能力评估，不进入跨引擎性能对比。

## 13. 2026-08-25 操作数口径与 trace 规模修正

### 13.1 正式口径仍是按操作数回放

2026-08-25 早先的“trace 只覆盖约 132.5 天 / 1B run 撑不到一个 depth16 归档周期”是错误结论。错误原因是没有列出 `range_10m` 的全部 10 个分片，只查看了 `startFile=9` 的最后一个分片。

manifest 中的完整输入规模：

```text
trace 目录         /root/asct_codex/mainnet_state_access_trace/range_10m
区块范围           46,147 .. 10,000,000
区块数             9,953,854
总操作数           20,242,112,513
读 read             15,262,192,418
touch                  840,688,828
write                 4,018,660,452
create                  76,502,187
delete                   44,068,628
gzip 分片数         10
```

第 10 个分片本身是：

```text
文件               state_access_trace_009046147_010000000.csv.gz
区块范围           9,046,147 .. 10,000,000
操作数             2,978,237,640
```

正式性能与结构实验继续使用 operation-count replay：

```text
-traceStressStartFile=0
-traceStressOps=20242112513
-traceCompareStartFile=0
-traceCompareOps=20242112513
```

ASCT 与 MPT/Verkle driver 都允许最后一批少于 4,000 operations。完整 formal run 必须从第 0 个分片开始并读取全部 10 个分片；不能使用 `startFile=9` 或只重放第 10 个分片。

区块信息只用于解释 trace 的来源、时间跨度与分布，不作为 formal run 的截断条件。新加的 start/end/blocks 控制项保留为诊断工具，可用于对齐一个完整文件或排查某个区块区间，但不应替代用户指定的操作数量级。

### 13.2 操作数覆盖的归档轮次

当前 depth16 prune cadence 是每 4,000-operation batch 推进一个 shard：

```text
depth16 shard 数       65,536
一个 ring 的 batch 数   65,536
一个 ring 的操作数      262,144,000
```

因此旧 depth16 1B-op run 实际覆盖：

```text
1,000,000,000 / 262,144,000 = 3.8147 个 ring
```

它不是“撑不到一轮归档”，而是已经完成 3 个多 depth16 ring。相对地，depth20 一个 ring 是：

```text
1,048,576 batches * 4,000 = 4,194,304,000 operations
```

所以旧 depth20 1B run 只覆盖约 23.84% 个 ring；depth16 才是旧配置下能看到多轮归档的选择。

按操作数设计的参考档位：

```text
786,432,000 ops    3 个完整 depth16 ring；最小多轮验证
1,000,000,000 ops  3.81 个 ring；旧 depth16 已有结果
2,978,237,640 ops  11.37 个 ring；完整最后一个分片
20,242,112,513 ops 77.25 个 ring；全数据集，成本很高
```

如果继续比较 MPT/Verkle，三个引擎必须在同一 `startFile` 和同一操作数下串行重放。不要让 ASCT 用 block range、MPT/Verkle 用 op count。

区块区间模式只作为诊断保留：

```text
-traceStressOps=0
-traceStressStartBlock=<inclusive first block>
-traceStressEndBlock=<inclusive last block>   # 或 -traceStressBlocks=N
```

该模式包含区间内所有 operations，并在 trace 早于请求终点时失败。它不是 formal op-count 实验。

MPT/Verkle driver 也有同样诊断语义：

```text
-traceCompareOps=0
-traceCompareStartBlock=<inclusive first block>
-traceCompareEndBlock=<inclusive last block>  # 或 -traceCompareBlocks=N
```

### 13.3 留存解释边界

当前 `PruneNextShard()` 是 shard-ring 轮转：

```text
depth16 一个 ring = 65,536 shards
depth20 一个 ring = 1,048,576 shards
```

它不表示真实“未访问 3/6 个月”策略；3/6 个月只是用户给出的经验参考，不是本轮 formal 实验必须实现的时间留存。按操作数回放时，应先报告 rolling ring 下性能、active storage、hot/archive 访问比例和 FPR 的趋势。

如果后续要研究访问年龄，需要另行为 stem/subtree 记录 last access，并把归档条件从轮转游标改为年龄阈值；那是独立实验，不能混入当前 op-count 对比。

### 13.4 指标口径

性能排名：

```text
Comparative_Wall_ms =
    Operations_ms + Commit_ms/Root_ms + DB_Write_ms
```

其中 ASCT 的 parse、prune launch、访问采样、同步 cold archive maintenance、final stats 均不进入排名。当前 driver 已单独输出 `Access_Sample_ms` 与 `Cold_Maintenance_ms`。

ASCT 还按逻辑分类输出读写删采样：

```text
Sampled_Read_Hot/Archived/Missing
Sampled_Write_Hot/Archived/Missing
Sampled_Delete_Hot/Archived/Missing
```

每个采样在执行前判定 hot/archive/missing，再对同一 op 计执行耗时；分类耗时独立记录为 `Access_Sample_ms`，不进入 `Operations_ms`。summary 中的 `access_average_ns` 是按类别的平均执行延迟。MPT/Verkle 对应输出 `timing_sample` 与 `timing_average_ns`，采样周期也默认 1,000 operations。

注意：当前实现的 cold write 仍会同步加载并复活整个 stem。`Cold_Maintenance_ms` 是精确的显式删除旧归档 entry 耗时；`cold_maintenance_estimate_ms` 再用冷/热 mutation 平均延迟差乘以 cold mutation 数，估计未拆出的加载、重建和复活成本。这两个值都是诊断，前者已从 `Operations_ms` 与 comparative wall 排除，后者不直接修改实测计时。要把这些成本真正移出主路径，需要 hot overlay/tombstone 与异步归档维护。

热命中率：

```text
Hot_Read_Hit_Rate =
    sampled_read_hot / (sampled_read_hot + sampled_read_archived)
```

分母排除 `read_missing`。`StemCache_Hits` 和 `StemGet_Hot_Hits` 是路径/服务命中，不代表逻辑热数据比例；尤其归档 stem 被读入 decoded cache 后，后续读取会被计入 hot path。

存储：

```text
主对比 = ActiveOnlyLogicalBytes / non-archive active storage
诊断   = ArchivedPayloadLogicalBytes
诊断   = LevelDB physical bytes after compaction
```

`PhysicalDelete=false` 时物理 DB 不会因归档立即缩小；不能把 state DB 目录大小当作 active-only 存储收益。

Cuckoo：

```text
runtime FPR = filter-positive 且 exact-entry miss / filter-positive exact lookups
synthetic FPR = known non-member false positives / known non-member queries
```

运行期 CSV/summary 输出 `Archive_Filter_Lookups`、`Negatives`、`Positives`、`False_Positives` 与 FPR，并扣除逻辑分类采样自己触发的 lookup；这些计数只代表真实 workload。运行期 FPR 不能替代合成负样本。depth16 旧 run 的 20 negative samples/bucket 结果为 `138 / 1,990,820 = 0.0069318%`。

### 13.5 Block-boundary 诊断 pilot

2026-08-25 远程 pilot：

```text
/root/asct_codex/results/trace_stress/block_pilot_depth16_20260825_113441
```

配置：

```text
blocks               9,046,147 .. 9,047,146
operations           3,530,160
batches              883
depth                16
prune cadence        1 shard / 10 blocks，仅驱动验证
access sample every  1,000 operations
```

结果：

```text
access samples       3,531
read hot             332
read archived        0
read missing         2,600
write hot            543
write archived       0
write missing        56
errors               0
access sample time   7.985 ms
cold maintenance     0.008516 ms
```

该 pilot 只证明 block-boundary 诊断模式、在线分类采样和采样计时不进入性能口径；它不是 formal 实验，其 10-block prune cadence 不代表正式策略，热命中率 100% 也没有产品意义。归档分类本身由本地 `TestTraceStressAccessClassifier` 覆盖。

### 13.6 当前读写语义与目标语义的差距

目标读路径：

```text
flat hit -> return
flat miss -> hot tree
hot miss -> Cuckoo
Cuckoo miss -> definitely absent
Cuckoo hit -> exact cold lookup
```

当前实现：

```text
Stem decoded cache
outer hot/archive membership
stem metadata
all present suffix payloads
stem commitment rebuild and root verify
archive bucket prefix + Cuckoo + exact scan + verify
flat value
```

当前 Get 不是 flat-first short-circuit；一次 suffix 读可能物化整个 Stem。`PhysicalDelete=false` 还可能留下旧 flat payload，直接把任意 flat hit 当作当前值会有正确性风险，需要 bitmap/tombstone/版本约束。

目标写路径：

```text
remove/update cold membership bookkeeping
write hot/flat directly
maintain cold copy asynchronously, outside commit timing
```

当前实现会同步检查并删除旧 archive entry，然后加载/更新/提交整个 stem，把它复活为 hot。不能只删除 Cuckoo fingerprint：filter 删除需要精确 membership/count/tombstone 语义，否则同 fingerprint 的其他 key 会受影响。

## 14. 2026-08-25 full op-count replay

启动前的 3,000,000-op validation pilot 三个引擎均通过：

```text
ASCT     3,000,000 ops / 750 batches / exit 0
MPT      3,000,000 ops / 750 batches / exit 0
Verkle   3,000,000 ops / 750 batches / exit 0
```

pilot 使用 `startFile=9` 只验证 operation-count、partial batch 与统计输出，不作为 formal 性能样本。ASCT summary 已包含 hot/archive/missing 读写删分类、`cold_maintenance_ms`、`cold_maintenance_estimate_ms`、workload-only Cuckoo runtime counters；MPT/Verkle summary 已包含 read/write/delete timing sample。

formal run 已按以下口径启动：

```text
operations       20,242,112,513
shards           startFile=0，全部 10 个分片
batch size       4,000
metrics window   10,000,000 operations
ASCT depth       16
ASCT prune       1 shard / batch
采样周期          access/timing 每 1,000 operations
执行顺序          ASCT -> storage stats -> synthetic FPR -> MPT -> Verkle
磁盘保护          free space < 8,000,000,000 bytes 时终止当前进程
```

远端路径：

```text
launcher  /root/asct_codex/run_full_replay_20260825_140122.sh
ASCT      /root/asct_codex/results/trace_stress/formal_20242112513_depth16_20260825_140122
MPT       /root/asct_codex/results/trie_compare/formal_20242112513_mpt_20260825_140122
Verkle    /root/asct_codex/results/trie_compare/formal_20242112513_verkle_20260825_140122
```

ASCT 完成后会自动用 final root 做 `storage_breakdown.json` 和 `filter_fp_20_per_bucket.json`；这两个产物只读 state DB，不改变 final root。MPT/Verkle 会在 ASCT 及其后处理完成后串行启动，避免共享 NVMe/CPU 干扰。
