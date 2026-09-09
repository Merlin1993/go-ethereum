# ASCT Replay 性能跟进问题单（截至 440 万块）

## 1. 实验状态

- 新实验目录：`F:\codex_asct\results\mainnet\asct\run_local_asct_hashparallel_bulkwipe_workers16_depth20_20260722_114113_10m`
- 新版本：`3f0d1cd9e6801eb3056154d4de98401d011b6757`
- 对照实验：`F:\codex_asct\results\mainnet\asct\run_local_asct_stemindexedwipefix_workers16_depth20_20260720_112853_10m`
- 当前完成：4,400,000 / 10,000,000（44%）
- 状态：running
- stderr：0 bytes
- Active/Archived logical read failures：0 / 0
- 当前 RSS：约 15.2-15.4 GiB
- 当前 CPU 短采样：约 1.87 cores（24 logical processors 的 7.8%）
- 8 秒 root/wipe 验收线：未触发

## 2. 结论

1. 新版本的并行 Hash 已经生效，但截至 440 万块，整体 Commit/charged root 只改善约 1%-2%，不能认为性能问题已经解决。
2. 新埋点证明 Hash 只占 PreCommit 的 5.31%。PreCommit 的剩余 33.9 ms 位于 Hash 之前的状态更新路径，目前没有写入 replay CSV。
3. NodeCache 窗口命中率只有 6.45%，每 10 万块发生约 4648 万次 miss 和 251 万次 eviction。entry cap 已顶满，但 byte cap 只用了约 19 MiB/512 MiB。
4. 每 10 万块同步执行一次全量 `Trie.Stats()`，最新耗时 388.76 秒，单次暂停约 6.48 分钟。这是实验统计开销，不是区块 Commit，但严重拖慢 replay。
5. 新旧实验在 440 万块的 active、archived、bucket、累计归档等结构字段完全一致，暂未发现性能修改造成状态统计偏移。
6. 大账户 bulk wipe 的最终效果尚未经过链上观察点 5,219,875 和 5,370,183 验证。

## 3. 新旧性能对齐

### 3.1 Commit 分段

| 窗口结束 | 新 Commit | 旧 Commit | 新 PreCommit | 旧 PreCommit | 新 charged root | 旧 charged root |
|---:|---:|---:|---:|---:|---:|---:|
| 3,700,000 | 14.74 ms | 13.25 ms | 11.40 ms | 10.16 ms | 12.58 ms | 11.29 ms |
| 3,800,000 | 22.85 ms | 21.51 ms | 18.22 ms | 16.97 ms | 19.83 ms | 18.62 ms |
| 3,900,000 | 33.23 ms | 31.20 ms | 27.01 ms | 25.13 ms | 29.24 ms | 27.38 ms |
| 4,000,000 | 34.46 ms | 32.46 ms | 28.23 ms | 26.46 ms | 30.49 ms | 28.66 ms |
| 4,100,000 | 35.60 ms | 33.20 ms | 29.07 ms | 26.90 ms | 31.44 ms | 29.23 ms |
| 4,200,000 | 54.57 ms | 54.76 ms | 45.62 ms | 45.36 ms | 48.93 ms | 48.92 ms |
| 4,300,000 | 57.48 ms | 58.43 ms | 48.13 ms | 48.45 ms | 51.62 ms | 52.25 ms |
| 4,400,000 | 43.12 ms | 43.84 ms | 35.80 ms | 35.97 ms | 38.49 ms | 38.92 ms |

440 万窗口的改善：

- Commit：1.64%
- PreCommit：0.47%
- charged root：1.10%
- PostCommit：6.99%
- DB write：6.26%

结论：PostCommit/DB 有小幅改善，但主导耗时的 PreCommit 基本没有变化。

### 3.2 墙钟吞吐

| 窗口结束 | 新版每 10 万块 | 旧版每 10 万块 | 新版变化 |
|---:|---:|---:|---:|
| 3,000,000 | 16.43 min | 15.81 min | 慢 4.0% |
| 3,700,000 | 30.53 min | 27.55 min | 慢 10.8% |
| 4,000,000 | 70.96 min | 66.77 min | 慢 6.3% |
| 4,200,000 | 105.59 min | 105.04 min | 慢 0.5% |
| 4,300,000 | 113.42 min | 114.20 min | 快 0.7% |
| 4,400,000 | 85.68 min | 87.19 min | 快 1.7% |

整体上仍是持平，不能宣称显著加速。

## 4. 并行 Hash：实现有效，但优化目标不是主耗时

440 万窗口：

- Avg PreCommit：35.80 ms
- Avg Hash total：1.900 ms
- Avg Hash shard wall：0.258 ms
- Avg Hash aggregate shard work：1.874 ms
- Avg Hash root merge：1.591 ms
- Avg Hash workers：15.270
- Avg dirty shards：96.212
- Avg Hash nodes：463.288
- Hash 占 PreCommit：5.31%
- PreCommit 中非 Hash 时间：33.9 ms

**已确认：** 约 96 个 dirty shards 使用了约 15.3 个 workers；并行 shard wall 已压到 0.258 ms，说明 worker pool 正常工作。

**已确认：** binary root merge 占 Hash total 的约 83.7%，已经成为 Hash 内部主要串行段。

**优先级判断：** 即便进一步把整个 Hash 降为 0，理论上也只能节省约 5.3% 的 PreCommit。binary root merge 可以优化，但不应该再作为第一优先级。

## 5. PreCommit 中未解释的 33.9 ms（P0）

调用路径：

```text
StateDB.PreCommit
  -> StateDB.IntermediateRoot
     -> Finalise
     -> wipeUnifiedDestructedState
     -> storage object updateRoot workers
     -> updateStateObject/deleteStateObject mutation loop
     -> trie.Hash
```

代码中其实已有三个内部计时器：

- `StateDB.StorageUpdates`：`core/state/statedb.go:1109`
- `StateDB.AccountUpdates`：`core/state/statedb.go:1159`
- `StateDB.AccountHashes`：`core/state/statedb.go:1165`

但 replay CSV 当前没有输出这些字段。现有数据只能确认 Hash 很小，无法继续区分 33.9 ms 落在 storage update、account update、节点加载还是锁等待。

建议立即增加：

1. `Avg/Max_Intermediate_Finalise_ms`
2. `Avg/Max_Storage_Updates_ms`
3. `Avg/Max_Account_Updates_ms`
4. `Avg/Max_Account_Hashes_ms`
5. 对应 max block 和每块 mutation/account/storage-slot 数
6. Stem ApplyBatch、Shard Put/Delete、archive promotion check、flat value get/put 的次数和耗时
7. updateRoot worker 数、worker wall、aggregate work、最长 state object

只有补齐这些字段后，才能决定下一步是优化 ApplyBatch、状态对象并发、节点加载还是 archive membership 检查。

## 6. NodeCache 高 miss/高淘汰（P0/P1）

440 万窗口：

- Entries：261,177
- Entry cap：262,144
- Cache bytes：约 19 MiB
- Byte cap：512 MiB
- Hits：3,202,986
- Misses：46,484,564
- Hit rate：6.45%
- Miss rate：93.55%
- Evictions：2,508,565
- Evictions per block：25.09

### 已确认

1. entry cap 长期顶满。
2. byte cap 远未使用完。
3. 绝大多数查询 miss，并且窗口内发生数百万次淘汰。
4. `nodeBlobCache` 的 get/add/remove 都使用一把全局 mutex：`trie/archive/node_cache.go:83-130`。

### 高概率推测

1. 过小的 entry cap 造成大量重复 DB load。
2. 并行 update/Hash 访问同一个全局 LRU mutex，可能导致串行化和锁竞争。
3. 只有 1.87 cores 的运行采样与“主要路径并行度不足或被锁/IO限制”一致，但短采样不能单独证明锁是根因。

### 建议验证

1. 保持 512 MiB byte cap，A/B 测试 entry cap：262K、1M、4M、仅 byte cap。
2. 增加 cache lock-wait、DB get latency、loaded bytes、重复加载 key 数。
3. 将全局 LRU 按 shard/key prefix 分片，避免所有 workers 竞争一把 mutex。
4. 分别统计 transaction update、Hash、Stats 产生的 cache hit/miss，避免把实验统计扫描与真实 Commit 混在一起。

注意：当前 `Stats()` 使用 isolated view 和 nil node cache，因此 388 秒 Stats 不会污染 NodeCache；上述 4648 万 miss 主要来自 replay 运行路径，而不是这次 Stats 扫描本身。

## 7. Trie.Stats 同步全量扫描（实验性能 P0）

最近 10 个窗口的 `Trie_Stats_ms`：

| 窗口结束 | Trie Stats |
|---:|---:|
| 3,500,000 | 300.81 s |
| 3,600,000 | 271.57 s |
| 3,800,000 | 401.77 s |
| 4,000,000 | 425.78 s |
| 4,100,000 | 457.22 s |
| 4,200,000 | 346.49 s |
| 4,300,000 | 456.68 s |
| 4,400,000 | 388.76 s |

440 万窗口每 10 万块墙钟 85.68 分钟，其中 Stats 占 6.48 分钟，即 7.56%。

代码原因：

- `reportStats()` 同步调用 `bt.Stats()`：`core/tree_test/processor_expire_state_test.go:598-604`。
- `Trie.Stats()` 串行遍历所有 loaded/persisted shards：`trie/archive/config.go:124-160`。
- persisted shard 使用 isolated view，逐节点从 DB 加载。
- stem active leaf 还会读取 flat payload 统计逻辑值；archive bucket 也会读取值并累计逻辑数量。

建议：

1. 将每 10 万块的性能 CSV 与重型结构统计拆开。
2. Commit/Tx/Hash/cache 指标每 10 万块输出；完整 Trie Stats 改为每 200 万块或最终阶段执行。
3. 长期方案是维护增量结构计数，不再为 Bucket/active/archive 数量全树扫描。
4. 如果必须全量核验，基于只读快照异步执行，并明确从 replay 业务墙钟中剥离。

## 8. 正确性与结构对齐

440 万块时新旧实验以下字段完全一致：

| 字段 | 新/旧共同值 |
|---|---:|
| Outer leaves | 11,649,261 |
| Archive records | 16,109,924 |
| Active logical values | 32,040,784 |
| Archived logical values | 21,045,867 |
| Total buckets | 1,048,403 |
| Root buckets | 1,048,403 |
| Deep buckets | 0 |
| Max path | 1 |
| Bucket Avg/P95/Max | 15.37 / 26 / 46 |
| Cumulative archived leaves | 16,932,428 |

- 当前 archived/(active+archived)：39.64%
- 总共享 DB：约 9.55 GiB
- 存储布局已正确标记为 `shared_state_db`
- `Archive_Storage_Bytes_Valid=false`

这说明截至当前窗口，性能代码修改没有改变主要结构统计结果。

## 9. 当前长尾与 wipe

440 万窗口：

- Max Commit：833 ms
- Max PreCommit：820 ms
- Max charged root：827 ms
- Max Hash：666.526 ms，block 4,393,400
- Max account wipe：2.785 ms
- Max archive compute：192.175 ms
- Archive wait over 8 seconds：0

当前没有超过 8 秒的事件。但 bulk wipe 是否解决旧版 28.5 秒事件，必须继续跑过：

- 5,219,875
- 5,370,183

## 10. 建议开发处理顺序

1. **先把 StorageUpdates/AccountUpdates/AccountHashes 写入 CSV。** 当前 33.9 ms 未解释，这是业务性能第一问题。
2. **做 NodeCache entry-cap A/B 和锁等待埋点。** 6.45% hit rate、93.55% miss rate和每块 25 次 eviction 已经足够异常。
3. **把全量 Trie Stats 降频或拆出主 replay。** 立即可节省每 10 万块约 4.5-7.6 分钟实验时间。
4. **再优化 binary root merge。** 它占 Hash 内部大头，但只占整体 PreCommit 很小一部分。
5. **继续保留当前 replay，跑过两个 wipe 观察点。** 不要因增加埋点而修改这轮正在运行的数据。
