# ASCT / MPT / Verkle 1B trace 对比性能分析

日期：2026-08-20  
数据来源：2026-08-18 titanide 正式 1B run，以及 2026-08-20 ASCT 10M CPU profile。  
核心问题：ASCT 为什么比 MPT 和 Verkle 慢？差距主要来自哪里？

## 结论先行

是的，在这次 structural trace workload 中，ASCT 的性能确实明显落后：

```text
MPT    是 ASCT 的 4.144x
Verkle 是 ASCT 的 1.488x
```

但这个结果更适合解读为：当前 ASCT 实现在这条读密集、每批 4000 op、每批 durable commit 的 trace 上，读路径和对象维护成本被显著放大；它还不是“ASCT 设计上限”或“生产客户端吞吐”的结论。

最重要的证据是：

```text
ASCT Operations_ms = 4,604.699 s
MPT   Operations_ms =   931.601 s
Verkle Operations_ms = 2,950.695 s
```

ASCT 的操作路径是 MPT 的 4.94 倍、Verkle 的 1.56 倍。它不是只在 commit 或 DB write 上慢，而是在 8.27 亿次 Get 和 1.73 亿次 Put 的主循环里就慢了。

## 1. 正式结果

| 指标 | ASCT | MPT | Verkle |
|---|---:|---:|---:|
| End-to-end | 1h51m27s | 34m26s | 1h17m47s |
| Comparative throughput | 164,270.08 ops/s | 680,788.60 ops/s | 244,364.21 ops/s |
| vs ASCT | 1.000x | 4.144x | 1.488x |
| Comparative wall | 6,087.536 s | 1,468.885 s | 4,092.252 s |
| Operation path | 4,604.699 s | 931.601 s | 2,950.695 s |
| Commit / root | 561.548 s | 129.291 s | 788.131 s |
| DB write | 921.289 s | 407.993 s | 353.427 s |
| Prune launch, diagnostic-only | 1.158 s | 0 s | 0 s |
| Peak RSS | 9.081 GiB | 1.455 GiB | 2.134 GiB |
| Active-only logical bytes | 1.312 GiB | n/a | n/a |
| Archived logical bytes, excluded | 0.125 GiB | n/a | n/a |
| LevelDB directory, unsplit diagnostic | 1.384 GiB | 1.069 GiB | 1.184 GiB |

Comparative throughput 的分母是 `Operations_ms + Commit_ms/Root_ms + DB_Write_ms`；trace parse、ASCT prune、final stats 和启动/收尾开销都不进入对比。ASCT 的 `Prune_Launch_ms=1.158s` 是独立诊断字段，不计入 `Commit_ms`，也不计入 comparative wall。

磁盘口径也做了拆分：ASCT 的非归档 reachable logical bytes 为 `1,408,837,104`，归档 payload logical bytes 为 `134,715,885`，后验统计读取失败为 0。归档部分不进入性能对比。这个拆分是 logical record bytes，不包含 LevelDB WAL、obsolete version 和 compaction overhead；MPT/Verkle 的 1.069/1.184GiB 是物理目录大小，因此三者不能放在一个百分比里直接比较。ASCT RSS 仍分别达到 MPT 的 6.24 倍、Verkle 的 4.25 倍，内存对象膨胀判断不受磁盘口径影响。

### 1.1 归档收益同样未达标

这次 run 不能被解读为“归档开销小，所以归档效果好”。相反，最终结构显示归档覆盖和压实都很弱：

| 诊断项 | 数值 | 判断 |
|---|---:|---|
| Archived KV records | 479,685 | 偏小 |
| Archived logical suffix values | 820,808 | 仅占全部 logical values 的 10.92% |
| Archived payload logical bytes | 134,715,885 | 仅占 reachable logical bytes 的 8.73% |
| Bucket average fill | 2.62 / 60 = 4.36% | 严重碎片化 |
| Bucket P50 / P95 / max | 2 / 6 / 16 | 远低于有效桶容量 |
| Root StubList buckets | 143,745 / 183,342 | 多数桶停留在 root 侧挂缓冲 |
| Ordinary child buckets | 0 | 没有形成下沉的 archive subtree |
| Prune ring coverage | 250,000 / 1,048,576 = 23.84% | 未完成一个 rolling-epoch 周期 |

因此这份 1B 实验对归档能力的结论是：**当前配置和 workload 下没有展示出有说服力的归档收益**。它不是一个稳态归档实验：depth 20 有 1,048,576 个 shard，但 trace 只有 250,000 个 batch，驱动每 batch 只推进一个 prune 游标，所以连一轮 shard ring 都没有扫完。

同时，bucket 平均只有 2.62 个 item 是实现层面的警示信号。它会放大 archive metadata、查找路径和 proof path，也说明冷数据没有被有效压实。此前 10M 产品 replay 在另一套策略下达到平均 39.01 items/bucket、85.48% archived logical-value share，说明机制并非天然无效，但对 shard depth、prune cadence 和访问局部性非常敏感。

准确量化收益还需要 archive-on / archive-off A/B，而不是用本次 final scan 直接推断：同一 trace、同一配置下分别开启和关闭 prune/archive，比较 active logical bytes、hot-tree bytes、bucket fill、RSS、cold-read latency、proof size，并在 LevelDB compaction 后记录 physical bytes。该 A/B 是能力实验，不应混入 MPT/Verkle 性能排名。

### 1.2 100M 阶段趋势

| Operations | ASCT | MPT | Verkle | MPT/ASCT | Verkle/ASCT |
|---:|---:|---:|---:|---:|---:|
| 100M | 227,212 | 930,849 | 246,543 | 4.097x | 1.085x |
| 300M | 187,156 | 707,290 | 235,978 | 3.779x | 1.261x |
| 500M | 175,176 | 681,440 | 242,612 | 3.890x | 1.385x |
| 700M | 152,751 | 644,247 | 247,641 | 4.218x | 1.621x |
| 900M | 139,816 | 615,458 | 248,116 | 4.402x | 1.775x |
| 1B | 132,991 | 611,280 | 256,364 | 4.596x | 1.928x |

三个引擎都会随状态增长变慢，但幅度不同：

- MPT 从 930,849 降到 611,280 ops/s，降幅 34.3%，仍保持明显领先。
- ASCT 从 227,212 降到 132,991 ops/s，降幅 41.5%。
- Verkle 从 246,543 到 256,364 ops/s，后期反而略升，整体非常稳定。

因此，Verkle 对 ASCT 的优势主要在 300M 之后逐渐扩大；MPT 的优势从第一阶段就稳定存在。

## 2. 时间去哪了

以各引擎自己的 comparative wall 为分母，即 `Operations_ms + Commit_ms/Root_ms + DB_Write_ms`：

| 路径 | ASCT | MPT | Verkle |
|---|---:|---:|---:|
| Operation path | 75.6% | 63.4% | 72.1% |
| Commit / root | 9.2% | 8.8% | 19.3% |
| DB write | 15.1% | 27.8% | 8.6% |

ASCT 的主要问题不是最后 commit：它的 `Commit_ms=561.5s`，甚至比 Verkle 的 `Root_ms=788.1s` 更好。它的主要问题是每次操作的路径太贵。

DB write 也偏慢：ASCT 921.3s，MPT 408.0s，Verkle 353.4s。但即使完全忽略 DB write，ASCT 的 operations + commit 仍为 5166.2s，而 Verkle 全部 comparative wall 为 4092.3s，MPT 为 1468.9s。DB write 解释不了总差距。

## 3. Workload 为什么会放大 ASCT 成本

三引擎 executed 操作完全一致：

```text
executed gets      826,822,027  82.68%
executed puts      173,103,387  17.31%
executed deletes       74,586   0.01%
```

这是强读密集负载。对 MPT 来说，Get 主要沿 trie path 查当前节点；对 ASCT 来说，`StemTrie.Get` 的语义是先取得或重建整个 31-byte stem 对应的 `Stem`，再取一个 suffix value。

ASCT 读路径关键代码：

```text
trie/archive/stem.go
  StemTrie.Get
    splitStemKey
    loadStem
      loadStoredStem
        loadStoredStemWithCache
          cache.get
          loadStoredStemFromFlat
            GetFlatValue(stem metadata)
            GetFlatValue(each present suffix)
            buildStemCommitment
            ValuesRoot verify
```

cache miss 后不是只读目标 32-byte key。它可能：

1. 读 stem metadata。
2. 逐个读取该 stem 的 present suffix value。
3. 重建 sparse stem commitment。
4. 重算并校验 `ValuesRoot`。
5. 把完整 Stem 放入 LRU。

这让一次逻辑 Get 变成多次 flat value lookup 加对象/commitment 重建。82.68% 的 Get 占比会持续放大该成本。

## 4. CPU profile 证据

profile run：

```text
ASCT cache-on normal config
10,000,000 operations / 2,500 batches
elapsed 42.006 s
comparative throughput 277,011.38 ops/s
```

注意：这是冷启动 10M，吞吐高于 1B 后期；它只用于热点定位，不作为性能成绩。

pprof 概况：

```text
wall duration  42.02 s
CPU samples    65.67 s / 156.27%
```

sample 百分比超过 100% 是并行 CPU 采样所致，不能把 sample 秒数当 wall 秒数。

### 4.1 主要热点

| CPU profile 项 | cumulative | 占 CPU samples | 解释 |
|---|---:|---:|---|
| `runtime.gcBgMarkWorker` | 18.21s | 27.73% | GC 标记成本极高 |
| `StemTrie.Put` | 11.82s | 18.00% | 写路径整体成本 |
| `StemTrie.Get` | 10.41s | 15.85% | 读路径整体成本 |
| `loadStoredStemWithCache` | 12.00s | 18.27% | cache hit + miss 的 Stem 加载簇 |
| `Trie.GetFlatValue` | 8.99s | 13.69% | flat value 查找簇 |
| `Shard.getFlatValue` | 8.59s | 13.08% | pending/staged/DB 查找 |
| `Stem.put` | 5.41s | 8.24% | stem 内部更新 |
| `stemCommitment.setLeaf` | 4.54s | 6.91% | 承诺/Keccak 更新 |
| `stemStateCache.get` | 2.95s | 4.49% | cache hit 路径 |
| `cloneStem` | 2.08s | 3.17% | cache hit 深拷贝 map |
| `CommitToBatch` | 4.29s | 6.53% | 最终提交路径 |

### 4.2 flat value miss 很贵

`loadStoredStemWithCache` 的 12.00s 中，`Trie.GetFlatValue` 为 8.99s，占 74.9%。`Shard.getFlatValue` 的 8.59s 里，绝大多数消耗在 goleveldb `Get`/`findGE` 相关路径。

`Shard.getFlatValue` 查找顺序是：

```text
pendingFlatValues
pendingFlatValueDeletes
最近两个 staged maps
external FlatReader
LevelDB db.Get(flatValueDataKey(key))
```

所以 cache miss 后，ASCT 会多次穿过这串 map/LevelDB 查找。该结论是 profile 直接支持的主要瓶颈。

### 4.3 cache hit 也不是零成本

`stemStateCache.get` 为 2.95s，其中 `cloneStem` 2.08s，约 70.5%。`cloneStem` 会复制：

```text
values map
sparse commitment nodes map
present bitmap / counters / references
```

这个 clone 是当前 cache copy-on-write 安全策略的一部分，但对纯 Get 来说没有必要重建可变副本。它还会制造大量小对象，进一步推高 GC。

### 4.4 写路径的 commitment/hash 成本高

`Stem.put` 为 5.41s，其中 `stemCommitment.setLeaf` 4.54s，约 83.9%。`PooledKeccakHasher.HashTriple` 也达到 4.46s。

这说明 ASCT 的写路径不只是写 value 和 metadata，还会执行大量 stem-level 承诺更新和 Keccak/triple-hash。Verkle 也有承诺成本，但本次实现中 ASCT 的 per-op 路径更贵。

## 5. 内存和 GC 是明确的放大器

正式 1B：

```text
ASCT peak RSS    9.081 GiB
MPT peak RSS     1.455 GiB
Verkle peak RSS  2.134 GiB
```

10M profile 中 GC mark worker 占 27.73% CPU samples。这个比例非常高，说明大量 CPU 花在扫描指针和 map 对象上。

ASCT 同时保留多种结构：

```text
live Shard / Node / root branch state
pendingFlatValues maps
pendingFlatValueDeletes maps
最近两个 staged flat maps
Stem values maps
sparse commitment maps
stem cache entry / LRU wrapper
path/node 映射
```

这些结构大多是指针密集、map 密集或小对象密集。它们不但占 RSS，也扩大 GC mark 集合。profile 里的 GC 热点与 9.08 GiB RSS 互相印证。

## 6. Stem cache 诊断，不作为主线

正式 ASCT run 的 Stem cache：

```text
entries       65,536
hits          459,818,962
misses        540,181,038
hit rate      45.982%
evictions     12,008,716
final bytes   94.70 MiB
```

此前 cache-off 诊断显示：

| 诊断项 | cache on | cache off |
|---|---:|---:|
| Operations_ms | 4,604.699s | 45,963.529s |
| Comparative throughput | 164,270.08 ops/s | 21,082.06 ops/s |

这说明 Stem 重建/加载路径确实贵，cache 把 operation path 缩短了约 10 倍。但它也说明“打开 cache”没有解决问题：命中路径仍有 map clone，miss 路径仍有 flat value lookup 和 commitment 重建，整体仍有 GC 与对象开销。

另一个关键现象是：正式 run 后期 hit rate 从早期约 36.98% 提升到末段约 55.07%，但吞吐从 227,212 降到 132,991 ops/s。因此 hit rate 不是吞吐下降的唯一解释；活跃状态、heap、shard/root metadata 和路径长度增长同样重要。

## 7. 对慢因的置信度分层

### 7.1 已有强证据

1. **操作路径是主瓶颈。**  
   `Operations_ms=4604.7s`，占 ASCT 核心 time 75.6%。

2. **flat value / Stem 加载路径很贵。**  
   `loadStoredStemWithCache` 18.27%，`GetFlatValue` 13.69%，其中大部分进入 goleveldb Get。

3. **读密集 workload 放大 Stem 物化成本。**  
   82.68% 操作是 Get，而 ASCT Get 会加载/重建完整 Stem，而不只是取一个 suffix。

4. **写路径 commitment/hash 成本高。**  
   `Stem.put` 5.41s，`setLeaf` 4.54s。

5. **GC 和内存结构显著放大 CPU。**  
   GC mark worker 27.73%，RSS 9.08 GiB。

6. **cache hit 仍有 clone 成本。**  
   `stemStateCache.get` 2.95s，其中 `cloneStem` 2.08s。

### 7.2 合理但还需实验确认

1. **ShardDepth=20 可能是放大器。**  
   depth 20 意味着最多 1,048,576 个 shard。随机分布下，数百万活跃 key 会触及接近百万级 shard；每个 Shard 又带 root、maps、pending state、锁和 NodePool 状态。10M profile 中 `hashRootBranchLocked` 也有 3.63s。  
   但目前没有本次 run 的 per-shard active count 和访问分布统计，不能断言它是主因。应做 depth 16 vs 20 对照。

2. **stem grouping 密度不足。**  
   ASCT final stats 有 `LeafCount=4,095,849`、`ActiveLogicalValues=6,696,539`，平均每个 leaf 约 1.64 个 value。若每个 stem 只承载少量 suffix，则 Stem metadata、map、commitment 和 root 维护的摊销效果较差。  
   还需要统计每 stem 的 active suffix 分布和 per-batch stem 复用距离。

3. **每批 reload/rebuild 的重复工作可能偏高。**  
   trace batch 中同一 stem 可能被多次访问；当前 driver 逐 op 调 Get/Put，每次都会经过 lock/load/cache clone。按 stem 分组或引入 batch-level working set 可能减少重复。需要计数确认。

### 7.3 解释边界

MPT 快有实现成熟度和 workload 口径优势：

- MPT 是 go-ethereum 高度成熟的实现。
- 它接收 raw 32-byte structural key，不承担 canonical secure-MPT account/storage layout 的额外开销。
- 本 workload 不执行 EVM、transaction rollback、witness/proof serving。
- MPT 的内存 dirty trie / PathDB cache 与 ASCT 的 archive、flat value、prune、stem commitment 职责不完全等价。

因此结论应写成：

> 在这条 structural trace、当前实现和固定配置下，MPT 明显快于 ASCT；不能推论生产以太坊 MPT 一定有同样吞吐，也不能推论 ASCT 设计本质上必然只有 MPT 的四分之一。

## 8. 为什么 Verkle 反而比 ASCT 快

Verkle 的 operation path 是 2950.7s，比 ASCT 低 35.9%；commit/root 是 788.1s，比 ASCT 高 40.4%；DB write 是 353.4s，比 ASCT 低 61.6%。

这说明 Verkle 的实现虽然 root/commit 很重，但每 4000 op 的访问循环更稳定，且没有 ASCT 这套 flat-value miss、完整 Stem 物化、cache clone 和 archive-aware metadata 维护成本；ASCT prune 本身已按口径排除。Verkle 后期吞吐稳定在约 240k-256k ops/s，因此总吞吐仍高于 ASCT。

## 9. 下一步定位实验

优先顺序如下。前四项应先做，不要直接大改实现。

### 9.1 加计数器，把路径拆开

建议在 ASCT 增加：

```text
Stem cache hit / miss，按 Get、Put、Delete 分开
metadata-only reload / full suffix reload
每次 miss 平均读取 suffix 数
GetFlatValue 调用次数、字节数、pending/staged/FlatReader/LevelDB 来源
cloneStem 调用次数和复制 node 数
Stem.put setLeaf/hash 次数
每 batch unique stem 数与重复访问数
active shard count、root branch 更新数
```

这些计数能把 profile 热点转化为 per-op 成本，也能验证 depth 和 stem 密度假设。

### 9.2 100M 热稳态 CPU + heap profile

10M 是冷启动。应在 100M 结束点或 1B 的 300M/700M/1B 阶段取样，另加：

```text
heap profile
goroutine profile
mutex profile
GC trace / GOGC 对照
```

目标是确认后期 GC、shard/root、cache miss 结构如何变化。

### 9.3 读路径专用优化原型

优先尝试：

1. 纯 Get 不 clone 可变 Stem，读取 immutable/shared snapshot。
2. Get 只读目标 suffix 和 metadata，不重建完整 stem commitment。
3. 对 metadata/presence 和 suffix value 分层缓存。
4. 对纯读路径跳过 `ValuesRoot` 重建校验，改为加载时一次性校验或后台校验。

这是最直接的读密集优化点。

### 9.4 Batch 内按 Stem 聚合

同一 batch 内先按 stem 分组，再一次性加载/更新，可能减少：

```text
重复 lock/unlock
重复 cache clone
重复 ValuesRoot / setLeaf
重复 flat lookup
```

需要保持原始 trace 顺序语义和错误语义，不能把最终状态等价误当成中间读语义等价。

### 9.5 结构与配置对照

用同一 100M/1B trace 做：

```text
depth 16 vs depth 20
Stem cache 65K vs 256K vs 512K entries
只读场景 vs 混合场景
physical delete off/on
```

cache 对照只是优化实验，不应取代 ASCT/MPT/Verkle 主对比。

## 10. 总体判断

ASCT 这次慢，是三层问题叠加：

1. **读路径设计成本高**：逻辑 Get 需要完整 Stem，miss 时多值加载、commitment 重建、root 校验。
2. **写路径承诺更新贵**：`Stem.put` 中 `setLeaf` 和 Keccak/triple-hash 占比很高。
3. **对象/map/GC 开销大**：9 GiB RSS、27.73% GC CPU samples、cache hit 也要 clone map。

最值得优先验证的改动是纯读路径免 clone / metadata-suffix 直读、batch 内 stem 聚合、以及 depth 16 对照。DB write 和最终 root 并不是第一优先级。
