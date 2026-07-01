# ASCT Round 3 Optimization Brief

本文是第三波实验线程的任务卡，不是逻辑说明。必须保留的 ASCT Plus 语义以
`trie/archive/doc/ARCHIVE_CODE_PLUS.md` 为准。

第三波优化的目标不是继续调参，而是先判断瓶颈来自设计、数据分布，还是工程实现。

## 1. 目标

```text
性能：root/commit 长尾不能明显差于 MPT
路径：MaxBucketsPath 必须受控
误报：false positive 不能成为主要成本
聚合：bucket 不能长期停留在大量 1-item / tiny bucket
语义：保持 ASC plus 模型，Flat KV 是执行真相库
```

先读：

```text
trie/archive/doc/ARCHIVE_CODE_PLUS.md
```

## 2. 当前最重要的判断

已有结果说明问题不是单一参数能解决：

```text
path pressure 强：
  MaxBucketsPath 可压到 14
  BucketItemsAvg 约 1.5
  结论：路径短，但 bucket 聚合很差

聚合/跳过下沉强：
  BucketItemsAvg 可升到约 4
  MaxBucketsPath 可涨到 4879
  结论：聚合好一些，但路径不可接受

commitdiag 长尾：
  shardCommit 约 50.8s
  batchWrite 约 13.9s
  topTree 约 12ms
  rawBytes 约 171MB
  rawOps 约 363万
  结论：长尾主要不在 top tree，也不像 proof/filter 问题
```

优先假设：

```text
固定 prefix shard + storage key = address + slot
=> 大合约 storage 可能集中到单个 hot shard
=> prune/commit 在少数 shard 上形成长尾
```

## 3. 第三波必须补的资料

### 3.1 Per-shard 分布

每个 reporting window 至少记录：

```text
shardID
leaf count
archive item count
bucket count
MaxBucketsPath in shard
dirty node count
pending flat puts/deletes
stale deletes
commit time
raw ops / raw bytes
prune time
```

目的：确认是不是少数 shard 承担了大部分数据和提交成本。

### 3.2 Raw batch 分类

长尾 block 必须拆出：

```text
ASC path node puts/deletes: BPN1 / BPT1
Flat KV puts/deletes: BFV1
archive bucket node puts/deletes
archive filter / entry metadata writes
other raw writes
```

目的：确认 171MB raw batch 主要来自节点、Flat KV、旧 value blob，还是 stale deletes。

### 3.3 Hot shard key attribution

对最重的 shard 统计：

```text
account key count
storage key count
top address by storage slot count
top address by raw bytes
top address by archived item count
```

目的：确认是否存在 hot contract super-shard。

### 3.4 路由模拟

不改语义，离线模拟几种 shard routing：

```text
current: prefix(logicalKey)
hash all: H(domain || logicalKey)
storage hash: H(address || slot)
hybrid: address prefix + H(slot)
adaptive: hot address virtual sub-shards
```

输出：

```text
per-shard count P50/P95/P99/max
top shard share
estimated dirty shard fanout
estimated prune work distribution
```

目的：先证明改 routing 是否值得做。

### 3.5 Prune 工作量预算

记录并模拟：

```text
items pruned per call
nodes visited per call
archive buckets built per call
raw bytes generated per call
wall time per call
```

目的：判断是否需要 budgeted/resumable prune，而不是一次 prune 完一个重 shard。

### 3.6 对齐 MPT 基线

同一 block range 比较：

```text
Avg_Root_Pipeline_Time_ms
Max_Root_Pipeline_Time_ms
Avg_State_Commit_Time_ms
Max_State_Commit_Time_ms
storage bytes
GC / heap counters
```

目的：确认“比 MPT 差不太多”的判断不是跨 run、跨配置比较。

## 4. 建议实验顺序

```text
Step 1: 只加诊断，不改语义
  目标：拿到 per-shard / raw batch / hot address 数据

Step 2: 离线 routing simulation
  目标：判断固定 prefix shard 是否是根因

Step 3: budgeted prune simulation
  目标：判断长尾是否能靠摊平 prune 解决

Step 4: bucket placement A/B
  目标：在 MaxBucketsPath 和 BucketItemsAvg 之间找可接受区域

Step 5: 再做语义级改动
  候选：hash routing、storage sub-sharding、resumable prune
```

不要先做：

```text
继续微调 70% threshold
继续堆 cache
继续调 goroutine 数
把 FP 当第一目标
```

这些只能改善局部，不能解释 hot shard / raw batch 长尾。

## 5. 推荐开启参数

mainnet replay 诊断 run 建议开启：

```text
-binaryNodeStorage=path
-binaryPathDiagnostics=true
-binaryPruneShardMetrics=true
-binaryCommitWatchdogSec=10
-binaryPhysicalDelete=false
-binaryCommitWorkers=16
-archiveBucketSize=100
-cuckooBuckets=16
-cuckooSlots=4
```

`ShardDepth` 建议至少覆盖：

```text
8   baseline / fast debug
16  default scale
20  target stress case
```

## 6. 关键阅读入口

```text
当前实现说明: trie/archive/doc/ARCHIVE_CODE_PLUS.md
归档核心包: trie/archive/
外层适配: trie/archive_trie.go
实验入口: core/tree_test/processor_expire_state_test.go
实验说明: EXPERIMENT_README.md
```

## 7. 关键结果目录

```text
results/mainnet/asct/run_newver_depth20_pathpressure_20260621_150911_allfiles
results/mainnet/asct/run_newver_depth20_skipattack_20260621_115623_allfiles
results/mainnet/asct/run_newver_depth20_commitdiag_20260621_085317_allfiles
results/mainnet/mpt/run_full_diag_20260529_135303_blocks5000000
```

## 8. 新线程起手问题

```text
先不要改 bucket placement。
先回答三个问题：

1. 长尾 block 的 raw batch 主要由什么类型写入组成？
2. 最重 shard 是否由少数 storage address 主导？
3. current prefix routing 和 hash routing 的 per-shard max 差多少？
```

如果这三个问题没有答案，第三波优化不应该进入实现阶段。
