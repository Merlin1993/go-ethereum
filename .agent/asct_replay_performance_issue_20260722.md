# ASCT 主网 Replay 当前性能问题单（2026-07-22）

## 0. 2026-07-22 开发处理结果

本报告确认的两个 P0 已完成代码修改，等待主网回放复验：

1. `Trie.Hash()` 已按 dirty shard 并行计算，worker 数受 `binaryCommitWorkers` 限制；分片结果仍按原二叉路径合并，单线程和多线程 root 一致。
2. PreCommit 已算出的节点 hash 会在 PostCommit 持久化时复用。节点内容、存储路径或子 hash 改变都会使旧 hash 失效。
3. 账户销毁改为先按 stem 分组，再一次性取旧值并删除；每个 stem 最多加载、解码和写回一次，不再逐 slot `Get` 后又在 `ApplyBatch` 中重复加载。
4. storage/code index 删除改成批量加锁和批量登记。
5. 已增加 8 秒单账户软告警，以及 index scan、stem delete、index stage、StateDB Keccak/RLP/map build 的分段计时和 slot/stem/code 数量。
6. replay CSV 已增加交易执行、统计扫描、Hash 分段、缓存 hit/miss/eviction 和 wipe 分段指标。Hash 诊断在 PreCommit 结束后立即采样，避免被 PostCommit 创建下一读视图时的空 Hash 覆盖。ASCT 存储明确标为 shared state DB，不再把空 archive 目录当成独立归档体积。
7. 二叉树模式已停用旧的“账户树/每账户 storage 子树”预读取器。ASCT 的账户、storage 和 code 共用一棵树，旧预读取器会产生另一棵停留在区块开始状态的树，提交时可能覆盖已经完成的账户销毁；ASCT 自身的共享节点缓存仍保留。
8. 索引查询会合并磁盘索引与本批次尚未提交的增删，但不会对十万级磁盘索引做二次全量排序；同时补齐了拜占庭升级前多次计算中间根时的“创建后销毁”和“销毁、重建、再次销毁”清理逻辑，并保留区块开始时的 storage 旧值。

本机一次性基准（Ryzen 9 3900X，Windows）：

| 项目 | 结果 |
| --- | ---: |
| Hash workers=1 | 36.59 ms |
| Hash workers=4 | 11.28 ms |
| Hash workers=8 | 6.52 ms |
| Hash workers=16 | 5.91 ms |
| Depth-20、1024 个稀疏 dirty shard：workers=1 / 16 | 47.77–55.04 ms / 14.10–17.67 ms（上层二叉根合并 9.01–12.69 ms） |
| 100,000 slot 的 StemTrie 核心批量删除 | 166.44 ms，391 stems |
| 100,000 slot 的 ArchiveTrie 完整索引扫描、解码和删除 | 328.08–373.02 ms，392 stems |
| 100,000 slot 的 StateDB 旧值编码和 history map 构建 | 158.33–225.45 ms |
| 100,000 slot 账户的完整 StateDB 销毁（PreCommit + PostCommit） | 543.64–656.27 ms；其中 wipe 443.5–496.5 ms，392 stems |

最新复测三次时，depth-20、1024 个稀疏 dirty shard 的 Hash 在 workers=16 下为 14.10–17.67 ms，其中分片并行部分为 4.39–6.19 ms；完整 100,000 slot 索引扫描、解码和删除为 328.08–373.02 ms。完整 StateDB 基准会先建立并持久化十万 slot 账户，再执行销毁、计算根、提交、重载并确认账户和末尾 slot 均已删除；三次耗时 0.544–0.656 秒。它仍不是主网区块 5,370,183 的实测，但已端到端覆盖原 28.5 秒账户销毁代码路径。

验证结果：除专门的超大压力用例外，全部 archive 正确性测试通过；Stem 账户销毁、启用 StateDB 预读取入口后的账户销毁、同块销毁重建、多次中间根下的创建后销毁和重复销毁、root 单/多线程一致、Hash 复用和 path relocation 边界测试通过；相关并发检测通过。当前代码还完成了一块真实主网输入的 depth-20 stem 冒烟回放，174 列 CSV 表头与数据行完全一致，新增交易、扫描、Hash、缓存和 wipe 字段均成功写出。`core/state` 的 `TestNodeIteratorCoverage` 和 `TestStateChanges` 在未修改的 HEAD 上也会原样失败，已确认不是本次修改引入。上述微基准证明代码热点已消除，但不能替代主网数据回放，下一步仍需越过 5,370,183 并观察 470 万到 550 万区间。

## 1. 结论摘要

当前 1000 万区块 replay 已完成 550 万块（55%），进程仍在运行，无读取错误、无 panic、无 guard 触发。旧的“销毁账户时扫描全局 stem”问题已经消失，但当前仍有两个需要优先处理的性能问题：

1. **普通区块的 PreCommit / 根计算吞吐很差。** 最新窗口 Commit 平均 77.68 ms，其中 charged root 平均 71.09 ms，占 Commit 的 91.5%。490 万窗口 charged root 曾达到 127.99 ms。代码上 `PreCommit -> IntermediateRoot -> trie.Hash()` 会串行遍历 dirty shards；`binaryCommitWorkers=16` 主要用于后续 Commit，不能加速这段 Hash。
2. **大账户销毁仍有严重长尾。** 区块 5,370,183 的 `accountStateWipe` 为 28.494 秒，同一区块 PreCommit 为 28.564 秒、Commit 为 28.749 秒，已经超过 8 秒警戒线。该窗口共清理 101,876 个 storage slots，说明 indexed wipe 消除了全局扫描，但复杂度仍与单账户 storage 规模线性相关。

当前归档计算平均只有 0.715 ms，DB 写入平均 6.65 ms，二者都不是普通区块的第一瓶颈。

## 2. 实验身份

- 运行目录：`F:\codex_asct\results\mainnet\asct\run_local_asct_stemindexedwipefix_workers16_depth20_20260720_112853_10m`
- Git HEAD：`aad650f39eb5069207460b2ec02e9e1f81ca8bfb`，运行时工作区为 dirty，快照和 patch 已保存在运行目录
- 启动时间：`2026-07-20 11:32:32 +08:00`
- 最新完整窗口：Epoch 55，处理计数 `5,400,000-5,499,999`
- 最新窗口完成时间：`2026-07-22 09:56:35 +08:00`
- 总耗时：约 46.40 小时
- 全程平均吞吐：约 32.93 blocks/s
- 最近窗口吞吐：10.26 blocks/s
- 配置：stem mode、shard depth 20、commit workers 16、path storage、async prune
- `archiveBucketSize=100`，但 metadata 记录的 `effectiveBucketCap=60`
- 机器：Ryzen 9 3900X，24 logical processors，约 96 GiB 内存

## 3. 吞吐趋势（实测）

| 完成区块 | 每 10 万块耗时 | 吞吐 blocks/s |
|---:|---:|---:|
| 4,000,000 | 66.77 min | 24.96 |
| 4,200,000 | 105.04 min | 15.87 |
| 4,600,000 | 95.40 min | 17.47 |
| 4,700,000 | 161.43 min | 10.32 |
| 4,800,000 | 219.06 min | 7.61 |
| 4,900,000 | 270.46 min | 6.16 |
| 5,000,000 | 189.13 min | 8.81 |
| 5,100,000 | 152.48 min | 10.93 |
| 5,200,000 | 135.23 min | 12.32 |
| 5,300,000 | 123.52 min | 13.49 |
| 5,400,000 | 120.48 min | 13.83 |
| 5,500,000 | 162.52 min | 10.26 |

性能在 490 万附近达到最差点，之后有所恢复，但最新窗口再次变慢。它不是单调退化，但已经长期低于 15 blocks/s。

## 4. 根计算性能（P0）

### 4.1 实测证据

| 窗口结束 | Commit avg | PreCommit avg | PostCommit avg | Charged root avg | DB write avg | Archive compute avg |
|---:|---:|---:|---:|---:|---:|---:|
| 3,000,000 | 5.03 ms | 3.52 ms | 1.50 ms | 4.07 ms | 0.92 ms | 0.410 ms |
| 4,000,000 | 32.46 ms | 26.46 ms | 6.00 ms | 28.66 ms | 3.82 ms | 0.442 ms |
| 4,600,000 | 48.62 ms | 40.21 ms | 8.41 ms | 43.41 ms | 5.25 ms | 0.522 ms |
| 4,900,000 | 138.76 ms | 120.12 ms | 18.63 ms | 127.99 ms | 10.90 ms | 0.543 ms |
| 5,400,000 | 59.06 ms | 50.46 ms | 8.60 ms | 53.82 ms | 5.29 ms | 0.643 ms |
| 5,500,000 | 77.68 ms | 66.74 ms | 10.94 ms | 71.09 ms | 6.65 ms | 0.715 ms |

最新窗口中：

- Charged root 占 Commit 的 **91.5%**。
- PreCommit 占 Commit 的 **85.9%**。
- DB write 占 Commit 的 **8.6%**。
- Archive compute 占 Commit 的 **0.92%**。
- 10 万块 Commit 累计约 129.47 分钟，而该窗口墙钟为 162.52 分钟。

因此，普通区块的主要性能问题是根计算，不是归档计算，也不是 DB 写入。

### 4.2 代码证据

调用链：

```text
StateDB.PreCommit
  -> StateDB.IntermediateRoot
     -> ArchiveTrie.Hash
        -> archive.Trie.Hash
           -> for dirtyShardList
              -> shard.Hash
                 -> shard.commit(..., batch=nil)
```

关键位置：

- `core/state/statedb.go:1554`：`PreCommit` 调用 `IntermediateRoot`。
- `core/state/statedb.go:1108`：`IntermediateRoot` 调用 `s.trie.Hash()`。
- `trie/archive_trie.go:1309`：wrapper `Hash()` 调用底层 archive trie Hash。
- `trie/archive/trie.go:423`：`Trie.Hash()` 在一个 `for` 循环中串行调用每个 dirty shard 的 `s.Hash()`。
- `trie/archive/shard.go:1458`：`Shard.Hash()` 调用递归 `commit(..., batch=nil)`。
- `trie/archive/shard.go:1823`：hash-only commit 会遍历 dirty nodes、序列化并计算哈希。
- `trie/archive/trie.go:495`：并行 worker 出现在 `CommitToBatch`，不是前面的 `Hash()`。

**石锤结论：** 本次配置虽然是 `binaryCommitWorkers=16`，但最耗时的 PreCommit Hash 路径仍然按 dirty shard 串行执行。

运行中 8 秒 CPU 采样为约 1.98 CPU cores，测试进程有 29 个线程，但只使用约 8.2% 的 24 逻辑核总能力。短采样不能代表全程，不过它与串行 Hash 的代码结构一致。

### 4.3 另一个可能的重复工作

`Shard.Hash()` 在 `batch=nil` 时会计算并设置节点 hash，但不会把节点持久化为 clean。随后 PostCommit/CommitToBatch 还需要再次遍历 dirty nodes；InternalNode 在 commit 中仍会重新序列化、重新 hash。

这是代码阅读得到的高可信推测，当前 CSV 没有“Hash 首次计算”和“PostCommit 重复计算”的节点级计数，仍需专门埋点或 CPU profile 验证。

### 4.4 建议开发动作

1. 给 `Trie.Hash()` 增加与 `CommitToBatch` 类似的 dirty-shard worker pool；每个 shard 独立计算，最后按 shard ID 更新 binary root。
2. 增加 Hash 阶段诊断：dirty shard 数、访问节点数、dirty/clean 节点数、load/serialize/hash/root-merge 耗时、最慢 shard。
3. 复用 Hash 阶段已经生成的节点 hash/序列化结果，避免 PostCommit 对同一批 dirty nodes 重复计算。
4. 在 470-550 万对应负载上采集 30-60 秒 CPU profile，确认时间是否主要落在 `Shard.commit`、节点加载、序列化、哈希或锁等待。
5. A/B 测试 `Hash workers=1/4/8/16`，观察 wall time、CPU 使用率和 root 一致性。

## 5. 大账户 wipe 长尾（P0）

### 5.1 两次明确长尾

| 链上区块 | Account wipe | PreCommit | Commit | 同窗口 wiped slots |
|---:|---:|---:|---:|---:|
| 5,219,875 | 2.632 s | 2.748 s | 2.855 s | 3,383 |
| 5,370,183 | **28.494 s** | **28.564 s** | **28.749 s** | **101,876** |

5,370,183 的三个最大值出现在同一个区块，时间差只有约 70-255 ms，因果关系非常明确：该区块的 Commit 长尾来自账户状态 wipe。

注意：101,876 是该统计窗口总量，不是 CSV 直接给出的单区块数量；结合该窗口其他销毁数量和同区块最大时间，可以高度怀疑绝大部分来自该大账户，但仍应增加 per-account 计数确认。

### 5.2 当前实现中的放大点

`trie/archive_trie.go:1197` 的 `WipeAccountState` 当前执行：

1. 按账户前缀迭代 index，收集全部 storage index keys。
2. 对每个 slot 单独调用 `stem.Get()` 读取并解码旧值。
3. 再把全部 key 交给 `StemTrie.ApplyBatch()`；ApplyBatch 按 stem 分组后再次 `loadStem()`。
4. 对每个 index key 单独调用 `stageArchiveIndex()`，每次都会 lock/unlock `indexMu` 并更新 map。
5. 回到 `StateDB.wipeUnifiedDestructedState()` 后，再逐 slot 执行 Keccak、RLP 编码，并写入两张 map。

所以 indexed wipe 已经把复杂度从“全局状态规模”降到了“该账户规模”，但 10 万级 slot 仍会形成大量串行读取、重复 stem load、对象分配和 map/mutex 操作。

### 5.3 建议开发动作

1. 新增分段计时：index scan、slot Get/decode、code index、ApplyBatch、index staging、StateDB Keccak/RLP/map build。
2. 增加 per-account 指标：address、slot 数、stem 数、code chunk 数、耗时；日志中的地址可脱敏哈希。
3. 为 wipe 增加“按 stem 一次加载、返回旧值并原地批量删除”的专用接口，避免先逐 key `Get`、随后 ApplyBatch 再次 load。
4. 将 `stageArchiveIndex` 改为批量加锁/批量写入；评估账户前缀 tombstone 或 range-delete 方案。
5. 研究在检测到 destruction 后提前预取该账户 index/stems，使工作与交易执行或其他准备阶段重叠，但不能只通过修改计时口径隐藏 28 秒墙钟。
6. 增加 8 秒软告警。当前 hard limit 是 `maxHandleDestructionMs=180000`，因此 28.5 秒不会失败；`Archive_Wait_Over_Budget=0` 也不会覆盖同步 account wipe。

## 6. 内存和缓存（P1，需验证）

最新/峰值数据：

- 当前 RSS 在 18.5-20.8 GiB 波动，guard 记录峰值 **21.09 GiB**。
- 最新 HeapAlloc 15,362 MiB，HeapSys 23,430 MiB，RuntimeSys 24,091 MiB。
- NodeCache 约 19 MiB、260,675 entries。
- 默认 entry cap 为 262,144，当前长期贴近 entry cap；但 512 MiB byte cap 远未用满。

**已确认：** entry 数量长期接近上限，内存占用较高且 GC 后波动明显。

**尚未确认：** entry cap 是否造成 cache churn 并放大 Hash 的 DB load。当前主 CSV 没有窗口级 node-cache hit/miss、eviction 和 path DB get 指标，不能直接认定它是根因。

建议：

1. 把 NodeCache hit/miss/eviction/path DB get 写入 replay CSV。
2. 在保持 byte cap 的前提下，对 entry cap 做 A/B 测试，例如 262K、1M、仅 byte cap。
3. 记录 GC pause、GC CPU fraction、每窗口分配字节，确认 15-23 GiB heap 是否影响尾延迟。

## 7. Bucket 结构目前不是第一性能嫌疑

550 万时：

- Total buckets：1,048,594
- Root buckets：1,048,569
- Deep stub buckets：25
- Max path：2
- Bucket Avg/P95/P99/Max：28.13 / 40 / 45 / 60
- Root stub list 最大仍为 1 个 bucket

当前只出现 25 个下沉 bucket，结构没有出现大量深路径。最大值 60 与 metadata 的 `effectiveBucketCap=60` 一致，但与命令行 `archiveBucketSize=100` 的直觉不一致，应由开发确认这是预期的 cuckoo 物理上限还是参数语义问题。

## 8. 测量口径缺口（P1）

### 8.1 交易执行时间没有进入 CSV

测试代码维护并打印 `totalTxTime/maxTxTime`，但 `asct_mainnet_metrics.csv` 表头和数据行没有 Tx Execution 字段。由于 `go test` 输出在长测试结束前被缓冲，运行中无法从日志取得该数据。

最新 10 万块墙钟 162.52 分钟，Commit 累计约 129.47 分钟，尚有约 33.05 分钟不能由 CSV 分解。这里可能包含交易执行、输入读取、统计扫描和其他测试开销。

建议新增：`Avg_Tx_Execution_ms`、`Max_Tx_Execution_ms`、`Max_Tx_Execution_Block`、交易数和 TPS。

### 8.2 统计本身没有计时

`reportStats()` 同步执行目录递归大小统计和 `bt.Stats()`，后者需要汇总约 104.9 万个 shard。CSV 没有 `Stats_ms` 和 `DirSize_ms`，无法判断每 10 万块报告一次的统计开销。

建议单独记录并从 replay 业务吞吐中剥离。

### 8.3 state/archive 存储拆分失真

550 万时 CSV 显示：

- Total archived physical items：29,501,781
- Archived logical values：57,379,356
- `Archived_Storage_Bytes=427`
- `State_Storage_Bytes=21,758,332,220`

archive_db 从启动后没有实际增长，说明归档数据当前落在共享 state DB，或者 archiveDir 仅是空目录。总磁盘量仍可使用，但 `State_Storage_Share`、`Archive_Storage_Share` 和 `Archive_Bytes_Per_*` 不能用于判断冷热存储收益，必须修正或明确标注 shared DB 口径。

## 9. 正确性/健康状态

- 运行状态：running
- `go_test.err.log`：0 bytes
- Active logical read failures：0
- Archived logical read failures：0
- Archive wait over 8-second budget：0
- 最新 Bucket Max：60，MaxPath：2
- 最新累计逻辑值：active 68,536,148；archived 57,379,356
- 按 `archived/(active+archived)` 计算的当前归档占比：45.57%
- CSV 自带 `Cumulative_Archived_vs_Active_Pct`：59.0997%，两者分母不同，不应混用

## 10. 建议处理顺序

1. **先处理 28.5 秒 wipe 长尾**：补阶段计时，做按 stem 的单遍 bulk wipe，增加 8 秒软告警。
2. **并行化并复用 PreCommit Hash**：这是普通区块长期低吞吐的主因，优先验证 dirty-shard 并行和 Hash/Commit 复用。
3. **补齐 Tx、Stats、cache/GC 指标**：避免下一轮仍只能通过窗口墙钟反推。
4. **修正存储拆分口径**：否则无法回答“从 flat KV 去除归档数据后能省多少”。
5. **最后做 cache cap 与 bucket cap A/B**：当前它们是值得验证的次级因素，不应先于前两个石锤问题。

## 11. 非严格 MPT 参考

已有 MPT 随机数据压测的最终 root 平均为 8.36 ms；本次 ASCT replay 最新 charged root 平均为 71.09 ms，最差窗口为 127.99 ms，数值上分别约为 8.5 倍和 15.3 倍。

但两组实验的负载、数据分布、机器和测试入口不同，这个倍数只能说明 ASCT 当前 root latency 值得优先优化，不能作为正式的同负载 MPT/ASCT 性能结论。正式对比仍需同一 replay、同一机器、相同区块范围和相同计时口径。
