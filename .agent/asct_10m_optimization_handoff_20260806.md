# ASCT 1000 万区块回放优化移交报告

生成时间：2026-08-06

本文供后续性能和存储优化线程使用。目标是给出可追溯的数据、已经证实的瓶颈、仍需验证的推测，以及下一轮 A/B 实验应满足的验收条件。本文不把逻辑字节推算成物理磁盘结果，也不把最新一个窗口的 PASS 覆盖掉历史尾部异常。

## 1. 执行摘要

本轮实验完整结束，100 个 epoch 全部生成，`go test` 退出码为 0，测试 PASS。

| 产品目标 | 本轮结论 | 核心证据 |
|---|---|---|
| 正确性 | 通过 | 703,522,087 笔交易中 4 笔失败，均与 MPT/Verkle 对齐；无 panic、并发写错误和逻辑值读取失败 |
| 后期性能接近或超过 MPT | 不通过 | 99 个对齐窗口中，ASCT Charged Root 平均是 MPT 的 2.72x、Verkle 的 1.32x |
| 去除归档数据后改善至少 50% | 逻辑字节通过，物理磁盘未证明 | 最终可达逻辑字节减少 68.287%；没有 active-only 物理数据库 |
| 误报率低 | 通过 | 141 / 2,078,696，FPR 0.0067831%，假阴性 0 |
| Proof 大小和验证开销可接受 | 典型值可接受，尾部不通过 | 平均 2,606.75 B、Item Max 3,828 B；完整 Proof 最大 1,241,901 B，验证最大 199.6593 ms |
| Root bucket 聚合约束 | 通过 | `MaxRootStubListBuckets=1` |

当前优化优先级应是：

1. Stem 懒加载和 NodeCache miss。
2. Account Update / Stem Apply 热路径及 6.68 秒尖峰。
3. active-only 物理导出和压实验证。
4. 完整 Proof 大包及验证长尾。
5. 最终报告生成顺序和性能观测隔离。

归档裁剪本身不是当前 Root 性能主瓶颈，不建议优先重写 prune。

## 2. 实验身份

### 2.1 运行目录

```text
F:\codex_asct\results\mainnet\asct\run_local_asct_stembatch_productaudit_baselinealigned_workers16_depth20_20260803_140859_10m
```

关键文件：

```text
asct_mainnet_metrics.csv
asct_filter_fp_metrics.csv
asct_final_storage_breakdown.json
product_audit.csv
product_audit_latest.json
go_test.out.log
go_test.err.log
go_test.exit.txt
metadata.json
run_command.ps1
guard.ps1
```

### 2.2 代码和参数

```text
git_head=318e4791e8c249696106c8ddc13e75a3075c7f6f
commit=fix(trie): serialize lazy shard path loading
shardDepth=20
archiveBucketSize=100
effectiveBucketCap=60
binaryCommitWorkers=16
binaryAsyncPrune=true
binaryPhysicalDelete=false
binaryNodeStorage=path
binaryNodeCacheLimit=1048576
binaryNodeCacheBytesLimitMB=512
archiveOverlapBudgetMs=8000
statsInterval2=100000
fullTrieStatsInterval=2000000
blocks=10000000
```

注意：本轮虽然传入 `archiveBucketSize=100`，运行 metadata 明确记录 `effectiveBucketCap=60`。因此本轮验证的是单 bucket 最大 60 的实现，不是单 bucket 最大 100 的策略。

### 2.3 基线

```text
MPT:    F:\codex_asct\plot_data\20260625_plot_workspace\replay\data\formal_ethereum_replay_mpt_verkle_20260625\merkle\asct_mainnet_metrics.csv
Verkle: F:\codex_asct\plot_data\20260625_plot_workspace\replay\data\formal_ethereum_replay_mpt_verkle_20260625\verkle\asct_mainnet_metrics.csv
```

MPT 和 Verkle 最后一行 `Block_End=9,953,853`，ASCT 主 CSV 最后一行 `Block_End=9,999,999`。外部 guard 只能对齐前 99 个 100k 窗口，ASCT 最后窗口没有精确匹配的基线行。

## 3. 指标口径

### 3.1 性能主指标

产品性能使用：

```text
Avg_Root_Compute_Charged_Time_ms
```

该指标排除：

- State DB 写入时间。
- 异步归档等待的前 8 秒。

交易执行使用独立的 `Avg_Tx_Execution_ms`，不得并入 Root 计算。

本轮所有窗口：

```text
Avg_Archive_Wait_Over_Budget_ms=0
Max_Archive_Wait_Over_Budget_ms=0
```

因此性能差距不是由“把 8 秒以内异步归档等待算进 Root”造成的。

### 3.2 墙钟时间

```text
总墙钟时间                 65.816 h
最终存储全量扫描            0.653 h
5 次周期精确结构扫描合计     1.036 h
排除上述观测扫描后的近似时间 64.127 h
```

周期精确扫描耗时：

| 精确扫描点 | Trie Stats |
|---:|---:|
| 2m | 15.304 s |
| 4m | 345.790 s |
| 6m | 960.250 s |
| 8m | 1,289.007 s |
| 10m | 1,118.967 s |

因此，65.816 小时不能直接当作纯交易和 Root 性能。对树实现做性能判断时优先使用 Charged Root；比较端到端墙钟时，至少应剔除这些扫描。

另一个待验证点是：结构统计和 Filter 采样通过 stats view 复用进程级 `nodeCache`。扫描不计入窗口 Root 时间，但可能污染或预热 LRU。需要用 `fullTrieStatsInterval=0` 的性能专用 A/B 验证影响，不能默认其对后续窗口完全无扰动。

## 4. 性能分析

### 4.1 与 MPT / Verkle 的分段对齐

以下为相同 100k 窗口的算术平均：

| 区间 | ASCT Charged Root | MPT Root | ASCT/MPT | Verkle Root | ASCT/Verkle |
|---|---:|---:|---:|---:|---:|
| 0-2m | 0.731 ms | 0.238 ms | 3.08x | 1.317 ms | 0.55x |
| 2-4m | 3.785 ms | 1.498 ms | 2.53x | 3.710 ms | 1.02x |
| 4-6m | 21.240 ms | 8.278 ms | 2.57x | 14.799 ms | 1.44x |
| 6-8m | 22.437 ms | 7.221 ms | 3.11x | 15.263 ms | 1.47x |
| 8m-对齐终点 | 19.005 ms | 7.469 ms | 2.54x | 15.827 ms | 1.20x |
| 全部 99 个对齐窗口 | 13.383 ms | 4.915 ms | 2.72x | 10.126 ms | 1.32x |

结论：

- 8m 后 ASCT 从 6-8m 的 22.437 ms 恢复到约 19 ms，存在后期改善。
- 后期逐渐接近 Verkle，但仍未超过 Verkle。
- 全程没有出现“后期接近或超过 MPT”的目标形态。
- 自动 guard 的灾难性停止条件没有触发，但产品性能目标仍然失败。

### 4.2 PreCommit 分解

分段平均：

| 区间 | PreCommit | PostCommit | Account Update | Storage Update | Charged Root | DB Write |
|---|---:|---:|---:|---:|---:|---:|
| 0-2m | 0.405 ms | 0.802 ms | 0.226 ms | 0.037 ms | 0.731 ms | 0.480 ms |
| 2-4m | 2.761 ms | 2.793 ms | 1.907 ms | 0.120 ms | 3.785 ms | 1.716 ms |
| 4-6m | 17.463 ms | 10.244 ms | 13.697 ms | 1.059 ms | 21.240 ms | 6.531 ms |
| 6-8m | 18.766 ms | 9.918 ms | 14.930 ms | 1.209 ms | 22.437 ms | 6.286 ms |
| 8-10m | 16.033 ms | 8.578 ms | 13.005 ms | 0.674 ms | 19.169 ms | 5.449 ms |

最终窗口：

```text
PreCommit Avg        18.65 ms
Account Update Avg   15.304 ms
Storage Update Avg    0.656 ms
Hash Avg              2.688 ms
PostCommit Avg         9.79 ms
DB Write Avg           6.20 ms
Charged Root Avg      22.27 ms
```

最终窗口中 Account Update 占 PreCommit 约 82%。这是可直接从诊断数据得到的事实。

### 4.3 Stem 和 NodeCache 随状态增长恶化

| 区间 | Account us/account | Stem Apply us/stem | NodeCache DB Get us/get |
|---|---:|---:|---:|
| 0-2m | 34.43 | 34.65 | 10.08 |
| 2-4m | 62.73 | 65.73 | 37.17 |
| 4-6m | 92.85 | 112.12 | 82.86 |
| 6-8m | 127.11 | 139.90 | 99.45 |
| 8-10m | 110.98 | 114.74 | 92.87 |

最终累计 NodeCache：

```text
hits=331,322,864
misses=4,664,710,384
hit rate=6.632%
miss rate=93.368%
evictions=233,850,513
entries=1,047,316 / 1,048,576
serialized bytes≈78 MiB / 512 MiB
```

石锤事实：

- entry 数量几乎碰到上限。
- 缓存序列化字节只使用约 78 MiB，远低于 512 MiB 字节上限。
- 生命周期 miss 率超过 93%。
- 6-8m 的单次 DB Get 和 Stem Apply 成本均接近早期的 4 倍。

合理推测：当前 1,048,576 entry 上限先于 512 MiB 字节上限生效，导致大量可缓存的小 path node 被逐出，并放大 Stem 懒加载的 DB 读取。该推测必须通过只调整 entry cap 的 A/B 证实，不能只凭相关性直接提交为结论。

最终窗口还有：

```text
NodeCache window misses       81,290,108
NodeCache window DB gets      81,290,107
aggregate DB-get time          7,974,969 ms
aggregate loaded bytes        62,027,062,040 B
lock wait                          8,108 ms
```

DB-get 聚合时间远大于 node-cache lock wait，当前优先级应是减少 miss 和重复加载，不是先重写 64-way cache lock。

### 4.4 关键尖峰

#### 尖峰 A：Account Update

```text
block=5,904,615
Max Root Compute Charged=6.680 s
Max State PreCommit=6.671 s
Max Account Updates=6.660653 s
```

该尖峰几乎完整落在 Account Update，不是 archive wait、DB write 或 account wipe。

#### 尖峰 B：Storage Update

```text
block=6,448,357
Max Storage Updates=3.797868 s
```

这是独立于 Account Update 尖峰的稀有慢路径，需要按 storage object、slot 数、Stem 数和 lazy loads 进一步拆分。

#### 其他极值

```text
最大窗口 Charged Root Avg  37.96 ms，窗口结束 5,599,999
最大 DB Write              2.573 s，block 2,469,010（不计入 Charged Root）
最大 Archive Compute       302.220 ms，6.4m 窗口
最大 Archive Over Budget   0
```

### 4.5 已排除的主瓶颈

归档路径窗口平均最高约 1.672 ms，单次最大 302.220 ms，且从未产生超过 8 秒预算的等待。它可能造成 CPU/IO 竞争，但现有数据不支持“prune 是 Root 慢的首要原因”。

Hash 在最终窗口平均 2.688 ms，明显小于 Account Update 15.304 ms。Hash 仍可优化，但不是第一优先级。

## 5. 存储分析

### 5.1 整体归档率曲线

以下为每 2m 的精确、唯一逻辑值统计，不是累计归档事件代理：

| Block | Active | Archived | Archived share |
|---:|---:|---:|---:|
| 2m | 1,034,774 | 383,173 | 27.023% |
| 4m | 19,333,521 | 15,031,066 | 43.740% |
| 6m | 62,780,951 | 78,604,839 | 55.596% |
| 8m | 40,357,450 | 152,884,559 | 79.116% |
| 10m | 34,279,275 | 200,662,163 | 85.409% |

6m 后数量归档率超过 50%，10m 达到 85.409%。

### 5.2 最终逻辑字节

最终存储扫描：

```text
valid=true
read_failures=0
scan_duration=39.17 min
reachable_logical_bytes=36.029 GB
active_only_logical_bytes=11.426 GB
archived_payload_logical_bytes=24.603 GB
logical reduction after removing archive payload=68.287%
```

归档 payload 组成：

| 组成 | GB |
|---|---:|
| Archive buckets | 5.503 |
| Archived Stem metadata | 6.112 |
| Archived suffix values | 12.988 |
| 合计 | 24.603 |

active-only 组成：

| 组成 | GB | active-only 占比 |
|---|---:|---:|
| Root branch | 0.079 | 0.69% |
| Hot tree nodes | 2.812 | 24.61% |
| Active Stem metadata | 1.380 | 12.08% |
| Active suffix values | 2.162 | 18.92% |
| Archive index | 4.993 | 43.70% |
| 合计 | 11.426 | 100% |

Archive index 已占 active-only 逻辑字节的 43.70%，是后续存储优化的主要对象之一，但删除或压缩它不能破坏赎回定位和正确性。

### 5.3 物理磁盘口径

```text
ASCT shared DB physical=41.885 GB=39.008 GiB
MPT final physical=22.507 GiB
Verkle final physical=30.139 GiB
ASCT physical / MPT=1.733x
ASCT physical / Verkle=1.294x
physical bytes 比 reachable logical bytes 高 16.25%
```

当前共享 DB 物理大小不达标。

active-only 逻辑值为 10.641 GiB，数值上比 MPT 物理值少 52.72%，比 Verkle 少 64.69%。这是逻辑/物理交叉比较，只能说明潜力，不能作为产品证明。

必须从现有 DB 只读导出 required active keys 到新的独立数据库，执行 flush/compaction 后测量 active-only physical bytes。禁止删除或修改本轮原数据库。

## 6. Filter 和 Proof

### 6.1 Filter

```text
negative queries=2,078,696
false positives=141
FPR=0.0067831%
positive queries=2,078,696
true positives=2,078,696
false negatives=0
```

FPR 明显低于 1% 目标，Filter 正确性通过。

### 6.2 Proof 典型值

最终窗口：

```text
Avg Proof Size=2,606.75 B
Item P95=3,324 B
Item P99=3,639 B
Item Max=3,828 B
Avg Verify=0.3221 ms
```

典型 Item proof 低于实验前声明的 4 KiB 平均和 8 KiB Item 上限。

### 6.3 Proof 尾部

```text
Max complete proof=1,241,901 B
block=9,187,426
Max verify=199.6593 ms
window end=6,599,999
```

`product_audit_latest.json` 只评价最后窗口，因此最后窗口显示 Proof PASS，但这不能覆盖历史 1.18 MiB 大包和 199.7 ms 验证长尾。

当前 guard 只检查平均大小、Item 大小和验证时间，没有完整 Proof 最大值 SLA。优化线程需要：

1. 为完整 Proof 增加单独的非致命告警阈值。
2. 记录每个完整 Proof 包含的 bucket 数、item 数、路径数和去重前后节点数。
3. 判断 1.18 MiB 是合法的大批量请求、重复 sibling/path，还是编码没有去重。
4. 在定义产品请求粒度之前，不要擅自给完整 Proof 设置“通过”结论。

## 7. Bucket 结构

| Block | Bucket Avg | P95 | Max | MaxBucketsPath | MaxRootStubListBuckets |
|---:|---:|---:|---:|---:|---:|
| 2m | 1.09 | 2 | 4 | 1 | 1 |
| 4m | 12.47 | 23 | 44 | 1 | 1 |
| 6m | 34.45 | 47 | 60 | 2 | 1 |
| 8m | 39.75 | 57 | 60 | 3 | 1 |
| 10m | 39.18 | 51 | 60 | 3 | 1 |

最终结构：

```text
Total buckets=2,079,975
Root buckets=1,048,537
Deep stub buckets=1,028,931
Child buckets=2,507
Max root stub list=1 bucket / 60 items
Max deep stub list=2 buckets / 67 items
Max buckets on path=3
```

解释：

- 早期 P95 很小是状态量不足，不是“每个 shard 永久只归档一两个状态”。
- 6m 开始出现下沉，8m 后路径最大为 3，说明分裂逻辑实际生效。
- root 每个位置最多一个 bucket，root blind merge 约束通过。
- deep internal node 出现最多 2 个 bucket / 67 items，需要结合公共前缀冲突规则判断是否符合预期。
- Max 始终为 60，因为当前实现的 effective cap 是 60。若产品设计要求 100，必须先统一配置和代码语义，再重跑；不能把本轮当作 100-cap 证据。

## 8. 已证实事实与待验证推测

### 8.1 已证实

- 并发 `nodePaths` 崩溃修复后完整跑到 10m。
- Account Update 是 PreCommit 的主要平均成本。
- block 5,904,615 的 6.68 秒 Root 尖峰来自 Account Update。
- NodeCache entry cap 几乎耗尽，而 byte cap 大量空闲。
- NodeCache miss 率超过 93%。
- Archive wait over budget 始终为 0。
- 逻辑去归档改善 68.287%。
- 当前共享物理 DB 比 MPT/Verkle 更大。
- FPR 和 false-negative 指标通过。
- Proof 存在 1.18 MiB 完整包和约 200 ms 验证长尾。

### 8.2 待验证

- 提高 NodeCache entry cap 能否显著降低 Stem Apply 和 Root 时间。
- shard 级写锁是否在热门 shard 上放大 Stem lazy load 延迟。
- 周期统计/Filter 扫描是否污染 node cache 并影响后续窗口。
- Proof 大包是否由重复路径/节点造成。
- active-only 数据导出并压实后，物理磁盘能否达到 50% 改善。
- archive index 是否能在保留赎回能力的前提下压缩。

## 9. 优化建议和实验顺序

### P0：NodeCache entry-cap A/B

保持其他参数不变，只调整：

```text
A: binaryNodeCacheLimit=1048576, bytes=512 MiB  当前基线
B: binaryNodeCacheLimit=4194304, bytes=512 MiB
C: binaryNodeCacheLimit=8388608, bytes=512 MiB
D: binaryNodeCacheLimit=16777216, bytes=512 MiB
```

当前实现没有“只禁用 entry cap、保留 byte cap”的参数；`NodeCacheLimit < 0` 会禁用整个缓存。因此 D 组用足够大的正 entry cap，让 512 MiB byte cap 先成为约束。不要把 `-1` 用作本项 A/B。

每组至少记录：

- hit/miss/eviction。
- entries 和 serialized bytes。
- DB gets、DB get us/get、load bytes/get。
- Stem Apply us/stem。
- Account Update us/account。
- Charged Root Avg/P95/P99/Max。
- RSS、HeapAlloc、HeapSys。

先用可重复的 Stem benchmark 和 2m/4m replay 筛选，只有出现明确改善且内存可控时再跑 10m。

### P0：Stem lazy-load 去重和批量化

关注代码：

```text
trie/archive/stem.go      ApplyBatch, loadStoredStem
trie/archive/trie.go      GetValueRef
trie/archive/shard.go     GetValueRef, loadNode, registerNodePath
trie/archive/node_cache.go
```

候选方向：

- 同一 ApplyBatch 内按 shard/stem 去重 load。
- 对同一 node hash 做 singleflight，避免并发 miss 后重复 DB get。
- 在 worker 启动前预取独立 stem 所需路径。
- 若 KV 后端支持，批量读取 path nodes。
- 避免完整 Stem 仅为一个 suffix 更新被重复 decode/encode。

不能为了性能移除并发修复。若缩小 `Shard.mu` 临界区，需要独立保护 root/child installation 和 `nodePaths` 的所有读写，并保留同 shard 并发 lazy reload 回归测试。

### P0：尖峰定点诊断

优先复现和分析：

```text
block 5,904,615  Account Update 6.660653 s
block 6,448,357  Storage Update 3.797868 s
block 9,187,426  Complete Proof 1,241,901 B
```

每个尖峰需要补充：

- account 数、stem 数、suffix 更新数。
- node-cache hit/miss 和唯一 node hash 数。
- 每个 worker 工作量和最长 worker。
- shard 分布及同 shard 最大并发数。
- DB get 次数、字节、累计时间。
- GC pause 和调度等待。

### P1：并发锁粒度测量

当前并发修复使用 shard 写锁串行化 lazy root、child link 和 path map 安装。先增加以下指标，再决定是否拆锁：

- `Shard.GetValueRef` lock wait。
- lazy node install wait。
- `nodePaths` lookup/register wait。
- 每 block 最热 shard 的等待和调用次数。

没有这些指标前，“粗锁是主瓶颈”只是推测。

### P1：active-only 物理导出

从本轮 DB 只读导出到全新目录：

- root branch。
- hot tree nodes。
- active Stem metadata/value。
- 产品运行所需的 archive index。

执行数据库 flush/compaction 后输出：

- SST/WAL/MANIFEST 分项。
- physical bytes。
- key/value count。
- 随机 active read 和 root correctness 校验。

原始 `state_db` 不允许移动、删除或原地 compact。

### P1：Proof 大包

增加完整 Proof 结构计数和去重统计，对 block 9,187,426 做定点复现。优化目标首先是解释大包来源，再决定压缩、路径共享或请求分片。

### P2：实验和报告工具修复

1. 最终主 CSV 在存储扫描前写出，所以最后一行 `Storage_Breakdown_Valid=false`，而最终 JSON 是 `valid=true`。应在 final scan 后重写 final metrics/audit，或生成独立 final audit。
2. 基线最后区间是 9,953,853，ASCT 是 9,999,999。应生成相同 block 范围的基线，或让 guard 按实际区间对齐。
3. 增加完整 Proof 最大值 SLA 字段。
4. 性能专用 run 应关闭周期 exact scan；结构/FPR 证据在独立同提交 run 中生成，或者证明共享 node cache 不被扫描扰动。
5. `asct_filter_fp_metrics.csv` 本轮约 212 MB。若后续只需汇总，应避免为每个 bucket 重复输出不必要字段。

## 10. 下一轮验收条件

### 正确性硬条件

- 无 panic、race、逻辑读取失败。
- 交易 success/total 与对齐 MPT/Verkle 相同。
- Filter false negatives=0。
- `MaxRootStubListBuckets=1`。
- bucket cap 和设计文档一致，不允许 metadata 写 60、口头按 100 解释。

### 性能条件

- 使用 Charged Root，不把 DB write 和 8 秒内 archive wait 计入。
- Tx Execution 单独报告。
- 至少报告 0-2m、2-4m、4-6m、6-8m、8-10m 分段。
- 优化 A/B 必须在相同区间、相同输入和相同机器条件下比较。
- 短期优化候选至少应相对本轮在 4-10m 降低 20%，且不能靠提高内存到不可接受范围换取。
- 最终产品目标仍是后期接近或超过 MPT；仅“比当前快 10%”不是最终通过。

### 存储条件

- final logical archived reduction >=50%。
- 生成 valid active-only physical export。
- 报告 compact 后物理字节，不能用逻辑字节冒充。
- 同时报告 archive index 是否保留及其大小。

### Filter / Proof 条件

- FPR <=1%，false negative=0。
- Avg Proof <=4 KiB。
- Item Proof <=8 KiB。
- Verify Max <=25 ms，或重新声明并论证新的 SLA。
- 新增完整 Proof 最大值和请求规模口径；1.18 MiB 尾部必须解释。

## 11. 不应采用的“优化”

- 修改、过滤或伪造 replay 输入和失败交易。
- 通过关闭归档功能获得 Root 性能数字。
- 把 archive wait、DB write 或 Tx Execution 混进/移出指标后仍声称与旧数据同口径。
- 用累计归档事件率代替唯一 active/archived 存量。
- 用 active-only 逻辑字节直接声称物理磁盘改善。
- 只看最终窗口 Proof PASS，忽略历史最大值。
- 为降低锁开销重新引入 `nodePaths` 并发写或 lazy child 安装 race。

## 12. 可追溯来源

```text
运行元数据:
F:\codex_asct\results\mainnet\asct\run_local_asct_stembatch_productaudit_baselinealigned_workers16_depth20_20260803_140859_10m\metadata.json

主性能和结构数据:
F:\codex_asct\results\mainnet\asct\run_local_asct_stembatch_productaudit_baselinealigned_workers16_depth20_20260803_140859_10m\asct_mainnet_metrics.csv

最终存储拆分:
F:\codex_asct\results\mainnet\asct\run_local_asct_stembatch_productaudit_baselinealigned_workers16_depth20_20260803_140859_10m\asct_final_storage_breakdown.json

产品审核:
F:\codex_asct\results\mainnet\asct\run_local_asct_stembatch_productaudit_baselinealigned_workers16_depth20_20260803_140859_10m\product_audit.csv

完整日志:
F:\codex_asct\results\mainnet\asct\run_local_asct_stembatch_productaudit_baselinealigned_workers16_depth20_20260803_140859_10m\go_test.out.log

实验方法:
D:\go_workspace\go-ethereum\.agent\asct_replay_experiment_guide.md

架构和桶迁移规则:
D:\go_workspace\go-ethereum\trie\archive\doc\ARCHIVE_CODE_PLUS.md
```
