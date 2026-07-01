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
     5. 如果路径上有相邻 bucket 或未满 leaf bucket，直接合入该 bucket
     6. 否则先放入 shard archive root 的聚合池
     7. shard archive 完成后，统一处理 root 聚合池:
        a. item 数不足 bucket cap，形成一个 root 小 bucket
        b. item 数超过 bucket cap，按 key 排序切成多个尽量满的 bucket
        c. 满 bucket 按各自公共前缀下放，成为 archive leaf bucket / subtree
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
* 完整 ECMH 重算属于 proof / diagnostics 层；普通执行读可以只做目标 entry 的 `valueRef` 校验。

### Archive Build

* 剪枝只移动 `valueRef`，不读取旧真实 value。
* 归档 item 使用完整逻辑 key 的绝对 path，进入 bucket 时再转成本地 suffix。
* `ArchiveBucketSize` 是硬上限；构建、合并、追加后都不能持久化超限 bucket。
* shard archive 期间，归档 item 优先沿路径向上合入相邻 bucket 或未满 leaf bucket。
* 找不到可吸收 bucket 的 item，先进入 shard archive root 聚合池。
* root 聚合池不足 `ArchiveBucketSize` 时，保留为一个 root 小 bucket。
* root 聚合池超过 `ArchiveBucketSize` 时，必须按 key 排序，切成多个尽量满且前缀最聚合的 bucket，再下放为 archive leaf bucket / subtree。

### Bucket Placement

* bucket 优先作为 archive leaf bucket 存在；不要让大量小 bucket 长期侧挂在路径上。
* 路径上已有相邻 bucket 或未满 leaf bucket 时，新归档 item 应直接合入，减少 root 聚合池压力。
* root 聚合池只作为本 shard 本轮 archive 的临时聚合位置。
* 已下放的 archive bucket / subtree，不因 item 数偏小回流到 root 聚合池或路径侧挂池。
* root 聚合池满桶切分后，必须按绝对 path 下放到普通 child edge。
* 如果目标 child 同时包含热节点和冷 bucket，必须重建在同一棵 child subtree 内，不能把冷桶挂到热 leaf 上。
* bucket 下沉只改变物理放置，不改变 `valueRef`、filter、ECMH 或 flat value 语义。

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

### Diagnostics

* `Stats`、filter FP sampling、path/cache diagnostics 不能改变 root、bucket placement、promotion 或 prune 结果。
* 精确统计如果需要遍历持久化冷节点，应使用隔离读取视图，避免污染执行热路径缓存。

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
* 路径增长：`Stats().MaxBucketsPath`
* bucket 聚合：`BucketItemsAvg/P95/P99/Max`
* 误报：`FalsePositiveCount` 和 filter FP sampling
* 内存：node cache、commitment point cache、bucket `cachedFilter`、`stagedFlatValues`
