# ASCT Stem 模式性能劣化问题单

## 1. 结论

当前 Stem 模式存在明确的平均性能劣化。

在相同主网输入、`shardDepth=20`、`binaryCommitWorkers=16`、
`binaryNodeStorage=path` 下，对齐到 470 万区块：

- StateDB PreCommit 加权平均慢约 **8.1 倍**。
- 根计算计费时间加权平均慢约 **7.1 倍**。
- DB 写入时间加权平均慢约 **2.6 倍**。
- 到达 470 万区块的墙钟时间慢约 **2.56 倍**。
- Flat value 写入次数基本相同，但写入字节达到 **6.14 倍**。
- 470 万时数据库总体积增加约 **49%**。

这不是严格的“同一 commit 只切换一个 flag”实验。当前 Stem 轮已经额外包含
Hash 并行、indexed wipe、缓存分片/扩容、关闭详细 prune 统计、全树统计降频等
有利优化，但仍显著慢于 Stem 前基线。因此，“当前最终系统发生性能劣化”的结论
可信度很高；要精确量化 Stem 开关本身的净成本，仍建议补做同 commit A/B。

## 2. 对比实验

### Stem 前基线

目录：

`F:\codex_asct\results\mainnet\asct\run_local_asct_rootpathexpandfix_workers16_depth20_20260713_092805_10m`

身份：

- `binaryStemArchive` 未启用。
- 1000 万区块完整完成，exit code 0。
- 输入文件 1-21。
- Depth 20、bucket size 100、16 commit workers、path storage。
- 每 10 万区块执行结构统计。
- `binaryPruneShardMetrics=true`。

### 当前 Stem 实验

目录：

`F:\codex_asct\results\mainnet\asct\run_local_asct_phasecache_stats2m_workers16_depth20_20260723_094202_10m`

身份：

- `binaryStemArchive=true`。
- 输入文件 1-21。
- Depth 20、bucket size 100、16 commit workers、path storage。
- 节点缓存 1,048,576 entries、512 MiB、64 shards。
- 每 10 万输出轻量指标，每 200 万执行精确全树统计。
- `binaryPruneShardMetrics=false`。
- 截至分析时已运行到约 476 万区块，stderr 为空。

## 3. 核心性能数据

### 0-470 万区块加权平均

| 指标 | Stem 前 | 当前 Stem | 倍率 |
|---|---:|---:|---:|
| PreCommit | 1.545 ms/block | 12.561 ms/block | 8.13x |
| PostCommit | 1.180 ms/block | 2.888 ms/block | 2.45x |
| 根计算计费 | 1.925 ms/block | 13.619 ms/block | 7.08x |
| DB 写入 | 0.699 ms/block | 1.829 ms/block | 2.62x |
| 根计算累计时间 | 2.51 h | 17.78 h | 7.08x |
| 到达 470 万墙钟 | 8.78 h | 22.50 h | 2.56x |

墙钟由 guard 首次看到对应完整 CSV 窗口的时间计算，存在最多约 30 秒采样误差，
不影响倍数结论。

### 460-470 万单窗口

| 指标 | Stem 前 | 当前 Stem | 倍率 |
|---|---:|---:|---:|
| PreCommit | 6.12 ms/block | 72.67 ms/block | 11.87x |
| PostCommit | 5.58 ms/block | 12.22 ms/block | 2.19x |
| 根计算计费 | 7.99 ms/block | 77.08 ms/block | 9.65x |
| DB 写入 | 3.61 ms/block | 7.88 ms/block | 2.18x |

### Stem 第一版与后续修复效果

对齐 0-170 万区块：

| 实验 | PreCommit | PostCommit | 根计算计费 |
|---|---:|---:|---:|
| Stem 前 | 0.136 ms | 0.312 ms | 0.288 ms |
| 第一版 Stem | 1.596 ms | 6.986 ms | 8.375 ms |
| 当前 Stem | 1.859 ms | 0.933 ms | 2.234 ms |

indexed wipe、Hash/Commit 和其他修复已经让第一版 Stem 的根计算成本下降约 73%，
但当前值仍是 Stem 前的约 7.8 倍。

## 4. 当前瓶颈的直接证据

460-470 万窗口：

- `Account_Updated=16,889,633`
- `Avg_Account_Updates_ms=68.566`
- `Avg_State_PreCommit_Time_ms=72.67`
- `Avg_Storage_Updates_ms=0.875`
- `Avg_Account_Hashes_ms=3.231`

账户更新占 PreCommit 的约 94%。

账户更新阶段累计时间约为：

`68.566 ms/block * 100,000 blocks = 6,856.6 seconds`

平均每个账户更新约为：

`6,856.6 s / 16,889,633 = 0.406 ms/update`

该结果与 Stem 单次更新的实现成本相符。

## 5. 对应代码路径

### 5.1 账户更新逐条调用 StemTrie.Put

文件：`trie/archive_trie.go`

符号：`ArchiveTrie.UpdateAccount`、`ArchiveTrie.UpdateAccountRLP`

Stem 模式下，每个账户更新单独执行：

```go
return t.stem.Put(trieutils.BinaryTreeBasicDataKey(address), value)
```

### 5.2 单次 Put 加载并重写整个 Stem

文件：`trie/archive/stem.go`

符号：`StemTrie.Put`

单次更新执行：

1. `loadStem(stemKey)`
2. 修改一个 suffix
3. `encodeStem(stem, hasher)`
4. `backend.Put(stemKey, encodedStem)`

### 5.3 encodeStem 重建完整 256-suffix commitment

`encodeStem` 调用 `Stem.ValuesRoot`。

`ValuesRoot` 会创建 256 个叶位置，并重建八层二叉树。即使只修改一个 suffix，
当前实现仍会计算完整树的约 255 个 branch hash，并重新序列化整个 Stem。

对账户基本数据而言，一个区块内的大量账户通常对应不同 stem，因此仅仅把调用
包装成 batch，未必能消除这一核心成本；关键是避免每个 suffix 更新都重建整个
commitment。

### 5.4 Storage 路径已有部分批处理

`ArchiveTrie.UpdateStorageBatch` 使用 `StemTrie.ApplyBatch`，会按 stem 分组，使每个
受影响 stem 最多加载和编码一次。

账户路径没有同等级别的批量入口，而且即使不同账户各自只有一个 stem，仍需承担
每个 stem 的完整 256-suffix commitment 重建。

## 6. 写放大与空间劣化

### 470 万区块

| 指标 | Stem 前 | 当前 Stem | 差异 |
|---|---:|---:|---:|
| Flat put 次数 | 157,788,009 | 155,177,165 | -1.65% |
| Flat put 字节 | 3.053 GiB | 18.741 GiB | 6.14x |
| 平均每次 put | 20.8 B | 129.7 B | 6.24x |
| 总存储 | 7.322 GiB | 10.923 GiB | +49.2% |

Flat put 次数几乎相同，而写入字节扩大约 6 倍，符合“修改一个 suffix 后重新写入
整个 encoded stem”的行为。

需要注意：当前实验是 `shared_state_db`，`Archived_Storage_Bytes` 不能单独拆分；
这里比较的是同一 CSV 口径的 `Cumulative_Storage_Bytes`。

## 7. 缓存不能解决根因

当前缓存已经达到约 104 万 entries，但仅占约 77-81 MiB，先撞到 entry limit，
没有撞到 512 MiB byte limit。

当前运行到约 476 万时：

- lifetime hits 约 7,294 万
- lifetime misses 约 70,627 万
- lifetime hit rate 约 9.36%
- evictions 约 5,131 万

扩大节点缓存改善了部分淘汰，但相同窗口的 miss 数没有发生数量级下降。外层节点
缓存也不能避免 `StemTrie.Put` 内部的完整 commitment 重算和整 stem 编码。

## 8. 不是所有指标都变差

当前轮的尾延迟和稳定性明显改善：

- Stem 前 470 万范围最大根计算：约 521 秒。
- 当前 Stem 最大根计算：约 4.27 秒。
- 当前没有超过 8 秒的根计算。
- 活跃值和归档值读取失败均为 0。
- indexed wipe 已消除第一版 Stem 的全局扫描销毁问题。

这些改善主要来自异步裁剪、indexed wipe、Hash/Commit 等后续修复，不能抵消
Stem 常规账户更新路径的平均成本。

## 9. 结构结果

400 万精确统计：

- Stem 前外层 child nodes：10,760,825
- 当前 Stem 外层 child nodes：9,981,216
- 当前外层节点约少 7.2%

当前外层树并没有更大，但根计算仍显著更慢，进一步说明主要成本位于 Stem 内部
更新、commitment 和 value blob 读写，而不是外层 ASCT 节点数量。

Stem 与非 Stem 使用不同根和不同记录布局，bucket/leaf 数量不能直接作为完全等价
的正确性结果比较。

## 10. 建议修复优先级

### P0：增量更新 Stem commitment

单 suffix 更新只重算该 suffix 到根的 8 层路径，不要重建完整 256-suffix 树。

可考虑：

- 在已解码 Stem 中缓存八层内部 commitment。
- 持久化或缓存可增量恢复的 branch commitments。
- 同一 Stem 多个 suffix 更新后统一执行一次增量 root 更新。

这是最可能带来数量级收益的修改。

### P0：避免整 Stem value blob 重写

评估以下方案：

- suffix value 与 Stem commitment 元数据分离存储。
- 使用 suffix delta/journal，达到阈值后再压实。
- 只更新变化的 suffix value 和 8 层 commitment path。

目标是消除当前约 6 倍的 flat write byte amplification。

### P1：增加账户批量更新入口

增加类似 `UpdateAccountsBatch`/`StemTrie.ApplyBatch` 的账户路径，统一做：

- key 转换
- 锁获取
- backend PutBatch
- 同 stem 合并

它可以减少调用、锁和 backend 边界成本，但如果大部分账户对应不同 stem，
单独做这一项不足以解决完整 commitment 重算。

### P1：增加 decoded Stem/commitment 缓存

当前 node cache 主要缓存外层序列化节点。建议单独统计和缓存：

- decoded Stem payload
- Stem commitment levels
- account basic-data stem 的命中率

### P1：补齐单账户 Put 分阶段诊断

目前 `StemTrie.ApplyBatch` 有 load/encode/backend 诊断，但 `StemTrie.Put` 没有同等
诊断。建议为账户路径增加：

- account stem load time/bytes
- decode time
- values-root hash time/hash count
- encode time/bytes
- backend write time/bytes
- decoded-stem cache hit/miss

这样可以直接验证 0.406 ms/update 中各阶段占比。

## 11. 建议验收实验

为了严格隔离 Stem 净成本，使用同一个修复 commit、全新数据库运行：

1. `binaryStemArchive=false`
2. `binaryStemArchive=true`

其他参数完全一致：

- 输入文件 1-21
- Depth 20
- bucket size 100
- 16 commit workers
- path storage
- 相同 cache limits
- 相同统计频率和诊断开关

建议至少跑到 500 万，因为 230-250 万和 420-470 万存在明显高负载窗口。

重点验收：

- PreCommit、PostCommit、charged root compute
- Account Update 分阶段时间
- Flat put bytes/update
- DB gets 和 loaded bytes
- 墙钟时间
- RSS/Heap
- 活跃/归档读取失败
- 最大根计算是否仍低于 8 秒

初步性能目标建议：

- Account Update 平均成本相比当前下降至少 70%。
- Stem 根计算加权平均控制在同 commit 非 Stem 的 2 倍以内。
- Flat put bytes/update 相比当前下降至少 70%。
- 不重新引入大于 8 秒的尾延迟。

