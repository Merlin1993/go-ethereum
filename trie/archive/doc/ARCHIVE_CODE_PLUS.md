# ASCT Plus 代码说明

本文只说明当前实现。

## 核心模型

ASCT Plus 将证明结构和真实 value 分开：

```text
ASC tree:  root / hot leaf / archive bucket / filter / ECMH commitment
Flat KV:   full key -> real value
```

树内不保存真实 value。热 leaf 和冷 bucket 都只保存 `valueRef`：

```text
valueRef = H("BVR1" || len(key) || key || len(value) || value)
```

真实 value 只从 flat value store 或外部 flat snapshot reader 读取。

## 代码分层

```text
外层适配: go-ethereum state.Trie 接口、triedb 接入、flat snapshot、commit 统计
全局 Trie: shard 路由、dirty shard、异步剪枝、TopTree root
Shard: 单分片读写、promotion、懒加载、提交
归档构建: 过期 leaf -> cold item -> 路径吸收 / 根部聚合 / 满桶下放
Bucket: entry list、Cuckoo filter、ECMH commitment
Value: flat value 写入、删除、staged value 窗口、valueRef
诊断: commit / prune / path / cache / bucket 统计
```

## 读流程

```text
ArchiveTrie.Get
  -> Trie.Get: 按 key 前缀定位 shard
  -> Shard.Get
     1. 懒加载 shard root
     2. 优先沿热 trie child 查找 leaf
     3. 热路径未命中时，检查路径上的 archive leaf bucket / root 小 bucket
     4. 命中 ArchiveBucketNode:
        a. 匹配 bucket path
        b. 用 Cuckoo filter 快速排除
        c. 用 entry suffix 精确匹配
        d. 从 flat value store 读取真实 value
        e. 校验 valueRef == H(key,value)
```

读取冷数据不会把它变热。只有对同一个 key 写入时才会 promotion。

## 写入 / Promotion 流程

```text
Trie.Put
  -> Shard.Put
     1. stage 真实 value 到 pending flat value
     2. 计算 key-bound valueRef
     3. 若 archive 中可能存在该 key:
        a. 定位 bucket entry
        b. 从 bucket entry list 删除旧 entry
        c. 增量更新 Cuckoo filter 和 ECMH commitment
        d. 空 bucket 从父节点移除
     4. 插入新的热 leaf(valueRef)
     5. 标记 shard dirty
```

promotion 不读取旧真实 value，只移动和校验 `valueRef`。

## 剪枝 / 归档流程

```text
Trie.PruneNextShard
  -> 选择 pruneShardIdx
  -> 必要时切换 globalEpochBit
  -> Shard.Prune
     1. 从 shard root 递归扫描
     2. 当前 epoch leaf 保持热状态
     3. 过期 leaf 转成 ArchivedKV{absolutePath,valueRef}
     4. cold items 沿路径向上查找可吸收 bucket
     5. 如果路径上有相邻 bucket 或未满 leaf bucket，且合并后不超 cap，直接合入该 bucket
     6. 否则先放入 shard archive root 的聚合池
     7. shard archive 完成后，统一处理 root 聚合池:
        a. item 数不足 bucket cap，形成一个 root 小 bucket
        b. item 数超过 bucket cap，按 key 排序后下放
        c. 下放过程按公共前缀形成不超 cap 的 archive leaf bucket / subtree
```

这个策略把小桶聚合推迟到 shard archive 结束时统一处理，避免沿路径长期累积大量
侧挂小 bucket，同时让下放的 bucket 尽量满、尽量按 key 聚合。

## 异步剪枝

启用 `AsyncPrune` 后，`PruneNextShard` 可以提前启动当前 shard 的后台归档任务。
但交易执行不能和未完成的归档树并行：进入下一批交易执行前，必须等待 pending
archive tree 完成并并入 ASCT。

```text
PruneNextShard
  -> finish 上一个 async job
  -> 启动当前 shard prune goroutine
  -> 后台构建 shard archive tree
  -> 完成后推进 pruneShardIdx
  -> 预取下一个 shard 的 path-storage 节点

交易执行入口 / Hash / Commit / Get / Put / Delete / CommitShard
  -> 如果存在 pending async prune，先 finishAsyncPrune
```

异步化只允许把归档计算提前到交易执行前完成，不允许在交易执行期间留下未并入的
归档树；它不改变 root 语义。

## 提交流程

```text
Trie.Commit
  1. finishAsyncPrune
  2. CommitToBatch:
     a. dirty shards 并行 commit 到 worker-local memBatcher
     b. 串行回放 worker batch 到外层 batch
     c. TopTree.Compute 汇总 shard roots
     d. 清理 dirty shard 标记
  3. batch.Write
```

`Shard.CommitToBatch(destructive=true)` 会在落盘后卸载 live root，只保留 `rootHash`，降低长期内存占用。

## 当前配置

| 配置 | 用途 |
| --- | --- |
| `ShardDepth` | shard 前缀深度 |
| `ArchiveBucketSize` | 单 bucket 最大 item 数 |
| `CuckooBuckets` / `CuckooSlots` | Cuckoo filter 参数 |
| `NodeStorageScheme` | `hash` 或 `path` 节点落盘方式 |
| `NodeCacheLimit` / `NodeCacheBytesLimit` | 进程级序列化节点缓存 |
| `NodeCacheWarmPathBits` | path-storage 提交时预热缓存的深度 |
| `CommitmentPointCacheLimit` | ECMH commitment point 缓存 |
| `ArchiveStubMaxBucketsPath` | 过渡实现中的侧挂压力兜底；目标逻辑以根部聚合和满桶下放为准 |
| `AsyncPrune` | 是否异步剪枝 |
| `CommitWorkers` | shard commit 并发度 |
| `CommitWatchdogSeconds` | 长 commit goroutine dump |
| `EnablePathDiagnostics` | 是否记录 path/cache 诊断 |
| `PhysicalDelete` | 是否物理删除过期节点 |

当前没有为“路径吸收 / root 聚合 / 满桶下放”新增开关；它们属于归档语义。
实验只需要调 `ArchiveBucketSize`、`AsyncPrune`、缓存和诊断开关。
bucket 放置的 20% / 70% / 80% / 95% 阈值同样属于归档语义，按
`ResolveArchiveBucketSize()` 派生，不作为实验数据可调旋钮。

## 不变量

* 一个 key 只能在一个位置：热 leaf 或冷 bucket。
* bucket entry 只保存 suffix 和 `valueRef`。
* 真实 value 必须来自 flat value store 或 flat snapshot reader。
* bucket item 数不能超过 `ResolveArchiveBucketSize()`。
* Cuckoo filter 只做快速排除，最终必须 suffix 精确匹配。
* ECMH commitment 必须和 bucket entry list 同步。
* path storage 只改变节点落盘 key，不改变 ASC commitment。
* 异步归档完成并并入 ASCT 前，不能执行交易，也不能计算最终 root。

## 必须保留的逻辑

这些规则是后续性能优化不能破坏的边界。

### Flat KV 真相库

* `Put` 必须先写 pending flat value，再把树内 leaf 更新为新的 `valueRef`。
* `Delete` 必须删除热/冷 membership，并写 pending flat delete。
* ASC tree 永远不保存真实 value；热 leaf 和冷 bucket 都只保存 `valueRef`。
* flat value 与 ASC metadata 必须在同一个 commit 边界落盘。

### Write Promotion

* 写入一个可能已归档的 key 时，必须先移除旧冷 entry，再插入新热 leaf。
* 移除旧冷 entry 只依赖 bucket entry 里的 suffix 和 `valueRef`，不能依赖旧真实 value。
* Cuckoo filter 只能作为跳过不可能命中的优化；filter 命中后必须做 suffix 精确匹配。
* 删除冷 entry 时必须保持 `Count`、`Keys`、filter 和 ECMH commitment 同步。

### Cold Read

* 冷读命中条件是：bucket path 匹配、filter 未排除、entry suffix 精确匹配。
* 返回真实 value 前必须从 flat value store / snapshot reader 取值，并校验 `valueRef == H(key,value)`。
* Cuckoo false positive 只能增加一次精确匹配成本，不能变成真实命中。
* Cuckoo filter 不能产生 false negative；如果构建、合并或增量 append 时 filter 插入失败，必须丢弃该 bucket 的 filter，
  让读写路径退化为扫描 `Keys` 做精确匹配，不能保存半成品 filter。
* 同一个完整 key 在 archive bucket 中只能有一个 membership。重建 bucket、合并 stub、吸收新归档 item 时如果发现重复 key，
  优先保留与当前 flat value 的 `valueRef` 匹配的条目；没有可读 flat value 时才保留后到条目作为保守兜底。
* 完整 ECMH 重算属于 proof / diagnostics 层；普通执行读可以只做目标 entry 的 `valueRef` 校验。

### Archive Build

* 剪枝只移动 `valueRef`，不读取旧真实 value。
* 归档 item 使用完整逻辑 key 的绝对 path，进入 bucket 时再转成本地 suffix。
* `ArchiveBucketSize` 是硬上限；构建、合并、追加后都不能持久化超限 bucket。
* shard archive 期间，归档 item 优先沿路径向上合入相邻 bucket 或未满 leaf bucket。
* 找不到可吸收 bucket 的 item，先进入 shard archive root 聚合池。
* root 聚合池不足 `ArchiveBucketSize` 时，先按 root 聚合阈值判断：低于 70% cap 可保留为 root leaf bucket；达到 70% cap 后进入 root/internal 的单一 root stub。
* shard root 上的 `StubList` 不是普通 list 语义；它最多只能保留一个聚合 stub bucket。多个 sibling root stub bucket 说明 root 聚合没有合并，是实现问题。
* root stub 合并应优先走 bucket 级盲合并：只重写 bucket key/filter/ECMH 元数据，不读取真实 value，也不先把所有 bucket 展开成 root pool items。
* root 聚合池或 root stub 总量超过 `ArchiveBucketSize` 时，必须按 key 重新分裂：达到 95% cap 或为了让 root 余量不超 cap 的分支下沉到 child edge，剩余小分支最多合成一个 root stub。
* 只有当 root stub 溢出且无法按 bucket path 直接下沉时，才退回到 key 级分裂；这是纠正跨分支 root bucket 的必要成本，不应成为普通 root 合并路径。
* promotion、redeem/delete、普通写入把 archive bucket 撞开、sparse child 上升等路径把 bucket 返回 root 时，也必须走同一套 root 单 stub 聚合/分裂逻辑；不能只调用廉价的 `attachStubs`，否则历史 tiny root stub 会绕开合并。

### Bucket Placement

Archive bucket 在 ASCT Plus 中按三段式状态机移动：

```text
root leaf bucket
  -- count >= 70% cap --> root single stub

root single stub
  -- count <= cap --> 留在 shard root 作为唯一 root stub
  -- count > cap --> 按 key split；成熟分支或为控制 root 余量必须下沉的分支进入 child-edge

internal non-root StubList bucket
  -- count >= 95% cap 且 bucket.PathBits > nodePathBits --> child-edge archive bucket
  -- count >= 95% cap 但公共前缀与当前 internal node 重合 --> 继续留在 StubList

child-edge archive bucket
  -- promotion/delete 后 count < 80% cap --> 回到父 internal node 的 StubList

internal StubList bucket
  -- promotion/delete 后 count < 20% cap --> 回流到 root 聚合
```

这些阈值用 `ResolveArchiveBucketSize()` 计算。当前实验参数 `CuckooBuckets=16, CuckooSlots=4`
时有效 cap 为 60，因此 20%/70%/80%/95% 分别是 12/42/48/57。

* 路径上已有相邻 bucket 或未满 bucket 时，新归档 item 应优先合入，减少 root 聚合池压力。
* root 聚合池是小冷数据的最终兜底聚合点，但它在 shard root 上只能表现为一个 root leaf bucket 或一个 root single stub，不能表现为多个 root sibling stub。
* root single stub 在 cap 内继续盲合并；超过 cap 后才触发 split/downsink，不应每次 normalize 都拆成 items 重包。
* 非 root `StubList` 是中间聚合层；达到 95% cap 且存在合法 child bit 时，才能下沉为普通 child edge archive bucket。
* child edge 上的 bucket 是稳定形态；赎回或删除导致低于 80% cap 时，回到父节点 `StubList`，等待继续合并。
* internal `StubList` 中的 bucket 继续变小并低于 20% cap 时，回流到 root 聚合，避免在深路径形成个位数 bucket 下挂。
* root 聚合池满桶切分后，必须按绝对 path 切分；第一层产生的低载分支可以回到 root，但只能合成一个 root stub。如果低载分支合计仍超过 cap，就必须继续选择分支下沉，不能形成 root stub list。
* root `StubList` 的整体重打包只处理当前 shard root 上的侧挂 bucket，不遍历整棵 archive tree；它的目标是把历史遗留或局部回流造成的大量 tiny root stub 合成一个 root 余量，并把溢出部分下沉。
* root internal node 自身存在压缩 `Path` 时，root 重打包和 promoted-stub 回流都必须按 `shardPrefix + root.Path` 作为当前 node path 判断下沉；不能只用 shard prefix。
* 如果待重打包的归档 key 在压缩 `root.Path` 中途分叉，必须先把 root 扩展到两者的共同祖先，再从该祖先按 key 分组；不匹配当前压缩 path 的 item 不能作为 fallback 原样保留成超 cap 的 root stub。
* 如果目标 child 同时包含热节点和冷 bucket，必须重建在同一棵 child subtree 内，不能把冷桶挂到热 leaf 上。
* bucket 上升/下沉只改变物理放置，不改变 `valueRef`、filter、ECMH 或 flat value 语义。
* 放置判断只发生在本次 prune/promotion/delete 触达的局部路径上；不能为了判断迁移而全局遍历所有 bucket。对 archive-only child 的计数必须有界，达到阈值即可停止。

#### Shard root 聚合边界

当前实现里的 `root leaf bucket` 和 `root StubList` 都是 shard root 级别，不是全局 ASCT root 级别。`ShardDepth=20`
时全局 key 空间先被切成 1,048,576 个 shard，每个 shard 独立执行 root 聚合、StubList 聚合和 child-edge 下沉。

因此如果某个统计窗口只有少量归档 item，并且这些 item 分散在大量 shard 中，即使 `MaxBucketsPath=1`，也可能出现
`BucketItemsAvg/P95` 接近 1 的结果。这类结果可能是 per-shard 聚合边界造成的结构性下限，也可能是 root `StubList`
缺少整体重打包造成的代码问题，必须结合 root leaf/root stub 的拆分统计判断，不能只看全局平均。

这类情况必须用拆分后的统计判断：

```text
RootBuckets = RootLeafBuckets + RootStubBuckets
```

如果 `RootLeafBuckets/RootLeafItems` 占主导，说明主要问题更可能是 shard 粒度过细或缺少跨 shard/浅层 archive 聚合层。
如果 `MaxRootStubBuckets > 1` 或 `RootStubBuckets` 长期大量 tiny bucket，则这是 root 聚合实现错误的强信号；root 侧应优先排查单 stub 聚合、溢出 split/downsink、赎回回流和普通写入 normalize。
如果 `DeepStubBuckets` 长期大量 tiny bucket，则优先排查非 root StubList 的整体重打包、下沉/上升状态机或赎回回流逻辑。

统计口径上，`MaxBucketsPath` 只表示一条顺序 lookup path 上经过多少个带 archive bucket 的节点；
同一个 `InternalNode.StubList` 里的多个并列 bucket 只算作这个 path 上的一站。`MaxStubListBuckets`、
`MaxRootStubBuckets` 和 `MaxDeepStubBuckets` 单独表示同一个节点上的侧挂 fanout 压力。修正后的目标是：
root 上不应出现 64 个 sibling stub bucket；如果统计出现 `MaxRootStubBuckets > 1`，优先按实现 bug 处理。

要让 `BucketItemsAvg/P95` 在 depth20、全局稀疏归档场景下稳定接近 bucket cap，仅靠 shard 内 placement 不够；需要引入
更浅的 archive routing 层、全局 archive 聚合层，或把 archive 聚合深度和热状态 shard 深度解耦。

### Path Storage

* path storage 只改变节点物理落盘 key，节点 hash/commitment 仍由序列化内容决定。
* 节点真实消失或 Patricia 压缩导致 entry path 改变时，才删除旧路径。
* `PhysicalDelete=false` 只影响磁盘清理，不影响 root、读写或证明语义。

### Async Prune

* 同一时间最多一个后台 prune job。
* 后台 prune job 必须先完成 shard archive tree 构建并并入 ASCT，才能允许下一批交易执行。
* `Hash` / `Commit` 必须先等待 pending prune 完成。
* `Get` / `Put` / `Delete` 作为交易执行读写路径，也必须先等待 pending prune 完成。
* 异步剪枝只隐藏交易执行前的归档计算耗时，不改变 root 语义或剪枝顺序。
* replay 统计里，后台归档等待可以按区块时间预算扣减：8s 以内的 async wait 不计入 charged root compute；
  超过预算的部分必须记录为 `Archive_Wait_Over_Budget` 并打印告警。async archive compute 超过 `MaxPruningMs`
  时只告警不中断 replay；同步 prune 仍可使用 `MaxPruningMs` 作为硬失败阈值。
* replay 统计必须把交易执行、`StateDB.PreCommit`、`StateDB.PostCommit`、DB write 和 charged root compute 分开。
  `PreCommit` 对应 `IntermediateRoot`，属于交易后的状态合入 / trie root 更新阶段，不属于交易执行；
  因此最大时间异常时优先看 `Max_State_PreCommit_Time_ms`、`Max_Root_DB_Write_Time_ms`
  和 `Max_Archive_Wait_Over_Budget_ms` 的归因，不能只看总 `State Commit`。

### Diagnostics

* `Stats`、filter FP sampling、path/cache diagnostics 不能改变 root、bucket placement、promotion 或 prune 结果。
* 精确统计如果需要遍历持久化冷节点，应使用隔离读取视图，避免污染执行热路径缓存。
* prune 诊断必须区分：路径吸收 item 数、root pool item 数、root pool 估算 bucket 数。

## 实验目标

实验比较必须使用同一 block range、同一机器、同一数据目录策略，并同时跑 MPT 基线。

建议先按阶段目标判断：

| 维度 | 阶段目标 |
| --- | --- |
| 正确性 | 与 MPT 读取结果一致；无 `valueRef` 校验失败；无同 key 热/冷或冷/冷重复 membership |
| 性能 | root/commit 平均和 P95 不明显差于 MPT；长尾 block 必须能解释到 shard、raw batch 或 prune 工作量 |
| 路径 | `MaxBucketsPath` 受控，不能出现线性增长或数千级路径 |
| 聚合 | tiny bucket 不能长期占主导；`BucketItemsAvg/P50/P95` 必须随剪枝轮次稳定上升或保持可解释 |
| 误报 | false positive 只能是次要成本；FP sampling 不能成为主要执行时间 |
| 内存 | RSS/heap 不随 block 单调增长；node cache、commitment cache、raw batch 峰值必须可解释 |
| 分片 | per-shard leaf/archive/bucket/raw bytes 分布必须可解释；重 shard 要能归因到地址或 storage 模式 |

第三波实验优先回答：

```text
1. 长尾 block 的 raw batch 主要由哪些类型写入组成？
2. 最重 shard 是否由少数 storage address 主导？
3. 当前 prefix routing 和 hash routing 的 per-shard max 差多少？
4. prune 是否需要 budgeted / resumable，而不是一次处理完整重 shard？
```

## 诊断入口

优先看这些指标：

* 长尾 commit：`LastCommitDiagnostics().ShardCommitNanos`、`BatchWriteNanos`、`TopTreeNanos`
* 分片不均：per-shard commit/prune metrics、dirty shard 分布
* 路径增长：`Stats().MaxBucketsPath`；同节点侧挂压力看 `MaxStubListBuckets/MaxRootStubBuckets/MaxDeepStubBuckets`
* bucket 聚合：`BucketItemsAvg/P95/P99/Max`
* root 聚合：`PrunePathAbsorbedItems`、`PruneRootPoolItems`、`PruneRootPoolBuckets`
* per-prune CSV：`Path_Absorbed_Items`、`Root_Pool_Items`、`Root_Pool_Buckets`
* 误报：`FalsePositiveCount` 和 filter FP sampling
* 内存：node cache、commitment point cache、bucket `cachedFilter`、`stagedFlatValues`
