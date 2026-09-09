# ASCT depth20 replay 临时报告（root path expand fix）

> 快照时间：2026-07-13 16:29:29 +08:00  
> 性质：运行中的阶段报告，不是最终实验结论  
> 用途：交给独立任务继续分析结构、性能、存储和 MPT 对比  
> 重要：当前 replay 仍在运行，不要修改结果文件，也不要为了分析停止进程。

## 1. 当前实验

结果目录：

```text
F:\codex_asct\results\mainnet\asct\run_local_asct_rootpathexpandfix_workers16_depth20_20260713_092805_10m
```

主要文件：

```text
asct_mainnet_metrics.csv
asct_prune_shard_metrics.csv
go_test.out.log
go_test.err.log
guard.log
analysis.log                  # 到 500 万以后才会生成
metadata.json
run_command.ps1
run_status.json
```

实验参数：

| 参数 | 值 |
|---|---:|
| 目标区块数 | 10,000,000 |
| 数据文件 | `E:\ethdata\transactions_1.csv` 至 `transactions_21.csv` |
| ShardDepth | 20（1,048,576 shards） |
| ArchiveBucketSize | 100 |
| CuckooBuckets / Slots | 16 / 4 |
| 有效 bucket cap | 60 |
| CommitWorkers | 16 |
| AsyncPrune | true |
| Node storage | path |
| PhysicalDelete | false |
| Node cache limit | 512 MiB |
| 归档重叠预算 | 8 秒 |
| async prune 告警线 | 30 秒，只告警不中断 |

Guard 规则：

- F 盘可用空间低于 100 GiB 时停止。
- RSS 高于 70 GiB 时停止。
- 100 万区块后 `Bucket_Items_Max > 60` 时停止。
- 100 万区块后 `Max_Root_StubList_Buckets > 1` 时停止。
- 300 万区块后 `Max_Buckets_On_Single_Path > 256` 时停止。
- 500 万开始、之后每 200 万区块输出一次 `analysis.log`。

## 2. 本轮修复目标

上一轮真实 replay 在窗口 `2,600,000-2,699,999` 失败：

```text
Bucket_Items_Max=65
Max_Root_StubList_Buckets=1
Max_Root_StubList_Items=65
effective cap=60
```

精确复现后确认：当 shard root 自身带压缩 `root.Path`，而 root stub 中的归档 key 在该压缩路径中途分叉时，旧代码把这些 key 全部归入 fallback；fallback 超过 cap 后仍被原样重建成一个 65-item root stub。

本轮实现：

- root 重打包发现归档 key 不匹配当前压缩路径时，先把 root 扩展到共同祖先。
- 原热树按剥离出来的首 bit 挂回父节点。
- 归档项从共同祖先重新按 key 分组、下沉。
- root placement 对完整 key 再做一次 membership 去重。
- 单个 root stub 盲合并后若仍超 cap，强制回退到 key-level split/downsink。

相关代码：

```text
trie/archive/archive.go:217   压缩 root 路径分叉检测与 root 扩展
trie/archive/archive.go:244   archiveItemsMatchPath
trie/archive/correctness_test.go:1231
                              TestSingleOverLimitRootStubOutsideCompressedRootPath
trie/archive/doc/ARCHIVE_CODE_PLUS.md:241
                              共同祖先扩展语义说明
```

验证状态：

- 精确复现测试通过。
- root 盲合并、单 stub、promotion、普通写 normalize、重复 key 等定向回归组通过。
- `core/tree_test` 编译通过。
- 完整 `go test ./trie/archive -count=1` 在外部 5 分钟上限内未结束、无失败输出；只能记为“超时未完成”，不能记为通过。
- 本轮 replay 已越过上一轮 270 万故障点，同窗口 `Bucket_Items_Max=60`、root stub `1/60`。

## 3. 结构快照（最新完整窗口）

最新完整窗口：`4,300,000-4,399,999`，实验已开始处理约 450 万区块。

| 指标 | 当前值 |
|---|---:|
| Active leaves | 13,361,883 |
| Current archived items | 15,909,797 |
| Current archived / (active + archived) | 54.3522% |
| Cumulative archived leaves | 16,314,169 |
| CSV cumulative archived vs active | 54.9742% |
| Total buckets | 1,110,576 |
| Bucket Avg / P50 / P95 / P99 / Max | 14.33 / 13 / 32 / 32 / 60 |
| MaxBucketsPath | 3 |
| Max root StubList buckets / items | 1 / 60 |
| Max deep StubList buckets / items | 1 / 56 |

位置分布：

| 位置 | Bucket 数 | Item 数 | 平均 items/bucket |
|---|---:|---:|---:|
| Root stub | 1,048,471 | 13,893,684 | 13.25 |
| Deep stub | 3,975 | 141,867 | 35.69 |
| Child edge | 58,130 | 1,874,246 | 32.24 |
| Root leaf | 0 | 0 | 0 |

结构结论（石锤）：

1. 单 bucket 没有突破 60；`MaxDeepStubListItems` 偶尔出现 62 时，是同一 deep node 上两个 bucket 的 item 总和，不是单 bucket=62。
2. shard root 始终最多一个 stub，root sibling tiny-stub 问题未复发。
3. bucket 装载率随归档推进持续上升：270 万时 Avg/P95 为 5.75/12，440 万时为 14.33/32。
4. root stub 数 1,048,471，已接近全部 1,048,576 个 shard；depth20 下几乎每个 shard 都有自己的 root 聚合桶，这是当前 root 平均装载只有 13.25 的主要结构背景。
5. child/deep bucket 的平均装载约 32-36，说明下沉后的 bucket 明显比 root 聚合桶更密。

暂不能下结论：

- 不能仅凭当前 Avg=14.33 判定 bucket 设计失败；需要跑到更晚阶段观察 root 平均是否继续增长。
- 也不能据此声称接近 cap；depth20 的 shard-local 聚合边界已形成明显装载上限，需要和 depth、shard 活跃分布一起分析。

## 4. 性能快照

最新正常窗口 `4,300,000-4,399,999`：

| 指标 | 平均 | 最大 |
|---|---:|---:|
| State commit | 6.45 ms | 1,999 ms |
| PreCommit | 3.27 ms | 1,960 ms |
| PostCommit | 3.18 ms | 239 ms |
| Charged root compute | 4.18 ms | 76 ms |
| DB write | 2.08 ms | 230 ms |
| Archive compute | 341.45 us | 1.936 s |
| Archive wait over 8s budget | 0 | 0 |

说明：窗口平均值总体较低，但被下列极端块显著破坏，不能只报告平均值。

### 4.1 超过 8 秒预算的事件

| Block | Shard | Commit | Pre | Post | Async prune duration/wait | 超预算 |
|---:|---:|---:|---:|---:|---:|---:|
| 3,191,875 | 0 | 8m54.095s | 7m52.592s | 61.503s | 7m47.918s | 7m39.918s |
| 3,909,961 | 718086 | 51.005s | 50.990s | 15ms | 50.981s | 42.981s |
| 4,177,925 | 986050 | 73.528s | 73.511s | 17ms | 73.501s | 65.501s |
| 4,240,451 | 0 | 7m49.348s | 6m53.817s | 55.532s | 6m49.311s | 6m41.310s |

计时口径已经按需求生效：async wait 的前 8 秒不计 charged root，超过部分计入并输出：

```text
[ASCT_ARCHIVE_WAIT_BUDGET]
[ASCT_ARCHIVE_COMPUTE_LIMIT]
```

### 4.2 两类长尾

#### A. shard 0 周期性重事件（石锤）

`4,240,451 - 3,191,875 = 1,048,576 = 2^20`，两次都是 shard 0 的轮转周期。

两次 commit diagnostics 都显示：

- async `shard.Prune` 持续约 6-8 分钟。
- prune 的遍历、收集、build 计数全为 0。
- 随后的 shard commit 处理约 3,405,000 个节点。
- raw batch 约 3,405,000 ops / 345 MB。
- shard commit 本身约 50-56 秒，DB batch write 约 4.8-5 秒。

这证明极端长尾与 shard 0 的周期轮转和超大节点提交强相关，不是 root bucket split/downsink 产生的大规模归档 item build。

#### B. 非 shard 0 wait-only 事件（石锤 + 待定位）

区块 3,909,961 和 4,177,925：

- PostCommit 仅 15-17 ms。
- shard commit 仅 6-11 ms。
- raw batch 仅约 2.9-3.0 MB。
- prune counters 全为 0。
- `job.pruneNanos` 与 `finishAsyncPrune` 等待时间几乎相等。

代码入口：

```text
trie/archive/trie.go:195-226   finishAsyncPrune 等待 job.done
trie/archive/trie.go:642-688   启动 async shard.Prune
trie/archive/archive.go:12-14  Shard.Prune 一开始获取 s.mu
```

可确认 `job.pruneNanos` 包含 `shard.Prune` 等待 `s.mu` 的时间，因为计时包住整个 `shard.Prune`，而 mutex 在函数入口获取。

当前最强推测：这些 wait-only 事件主要是 async prune goroutine 等待 shard mutex，而不是执行归档遍历。尚缺少 prune lock-wait 独立计时，因此不能把锁等待定为最终结论。

## 5. 误报率

最新窗口：

```text
Cycle_FP_Count=152
Max_FP_In_Single_Block=7
Hit=9,760,560
Miss_NonExistent=3,242,496
Miss_Existent=259,151
```

按全部 lookup 粗略作为分母：

```text
152 / (9,760,560 + 3,242,496 + 259,151) = 0.001146%
```

注意：这不是严格的 Cuckoo filter 条件误报率，因为分母包含并未真正进入 filter 判定的查询。严格误报率需要记录 filter negative、filter positive、entry miss 的分层计数。

当前没有发现 false negative。代码已保证 filter 构建、合并或 append 失败时禁用该 bucket 的 filter，而不是保留不完整 filter。

## 6. 存储口径

快照时实际目录大小：

```text
state_db:   6.228 GiB（3349 files）
archive_db: 70 bytes（基本为空）
```

CSV 同时报告：

```text
Cumulative_Storage_Bytes=7,014,732,185
State_Storage_Bytes=7,014,731,758
Archived_Storage_Bytes=427
```

该拆分目前不能用于回答“归档数据从 flat KV 删除后能节省多少”：当前配置下 archive trie/path nodes 和 flat value 的物理归属没有被 `State_Storage_Bytes` / `Archived_Storage_Bytes` 正确拆开，`archive_db` 基本为空也不代表没有归档数据。

需要新任务进一步确认：

1. flat account/storage value 实际写在哪个 LevelDB namespace。
2. `PhysicalDelete=false` 下归档后 flat value 是否仍全部保留。
3. 按累计唯一归档 key 统计可删除 flat bytes，而不是用归档事件次数乘平均值。
4. 用 live DB key scan 或独立 namespace 统计估算“移除归档 flat value”后的真实尺寸。

## 7. MPT 对比基线

指导文件记录的 MPT replay：

```text
/root/asct_codex/results/mainnet/mpt/run_remote_mpt_20260622_112506_files11
server: 192.168.3.51
```

当前 ASCT 本地使用 21 个数据文件，而该 MPT 基线使用 11 个文件。对比时必须按相同 block window 截取，不能直接比较最终目录或总时间。

建议对比字段：

- transaction execution（不能混入 root/commit）。
- PreCommit / IntermediateRoot。
- PostCommit。
- DB write。
- charged root compute（ASCT async wait 前 8 秒已扣除）。
- P50/P95/P99/max，而不只是平均。
- 同区块长尾，特别是 3,191,875、4,240,451、5,000,001。
- state DB 实际文件尺寸和逻辑 live bytes。

## 8. 需要独立任务优先处理的问题

优先级 P0：定位 async prune 长尾。

1. 给 `Shard.Prune` 的 `s.mu.Lock()` 增加独立 lock-wait 计时。
2. 将 `job.pruneNanos` 拆成 lock wait、load root、walk、bucket build、stale bookkeeping。
3. 分析 shard 0 为什么每轮提交约 340 万节点、345 MB，确认是 shard 数据倾斜、epoch flip 触发全树 dirty、缓存卸载/重载，还是 path-storage 持久化逻辑。
4. 对比 3,191,875 与 4,240,451 的 shard 0 root/node/bucket/raw-key 分布。
5. 不要通过调高告警阈值掩盖问题。

优先级 P1：完成 500 万阶段结构分析。

1. 等当前实验自然到达 500 万。
2. 读取 guard 自动生成的 `analysis.log`。
3. 统计 root/deep/child bucket 的数量、items、平均装载及占比。
4. 检查 `Bucket_Items_Max<=60`、`MaxRootStubList<=1` 是否持续成立。
5. 统计归档总数、当前归档数、活跃数和赎回差值。

优先级 P1：修正存储拆分统计。

1. 明确 flat KV、archive nodes、hot nodes、LevelDB WAL/SST 的统计边界。
2. 给出“当前物理 DB”和“假设删除已归档 flat value”的两套结果。
3. 与 MPT 使用相同 block window 和相同文件集合比较。

优先级 P2：严格误报率。

- 增加 filter query、filter positive、entry miss、sampled bucket size 计数。
- 分别报告条件误报率和每次状态查询触发误报的总体概率。

## 9. 新任务建议开场指令

```text
继续分析 ASCT depth20 replay 的性能长尾和存储效果。先读取：

D:\go_workspace\go-ethereum\.agent\asct_replay_rootpathexpandfix_interim_report_20260713.md
D:\go_workspace\go-ethereum\.agent\asct_replay_experiment_guide.md
D:\go_workspace\go-ethereum\trie\archive\doc\ARCHIVE_CODE_PLUS.md

当前 replay 仍在运行，不要停止、删除或修改实验结果。优先分析 block
3191875、3909961、4177925、4240451，先给 Shard.Prune 增加 lock-wait
细分诊断，再判断长尾是否来自 shard mutex、epoch flip 或 shard 0 全树提交。
所有结论明确区分石锤和推测，不要修改已有 CSV 数据。
```

