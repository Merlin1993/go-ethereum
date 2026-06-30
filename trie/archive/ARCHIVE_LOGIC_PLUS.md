# ASC Archive Logical Plus

本文档是当前 ASC archive 逻辑的第一准则。`ARCHIVE_LOGIC.md` 仅作为旧版历史参考；实现、测试和实验结论若与本文档冲突，以本文档为准。

本文档描述 ASC 归档逻辑的 plus 版本。核心目标是把执行读取与证明骨架解耦：

* Flat KV / snapshot 是执行真相库，EVM 读取不关心冷热状态。
* ASC 树只维护 root、proof 和 cold metadata。
* 冷桶不保存真实 value，也不保存完整 `ArchivedKV[]` payload。

## 1. 两层架构

### Flat KV 层

Flat KV 是所有执行读取的唯一入口。热写入、冷覆盖、删除都必须同步维护这层数据。

在 geth 里应优先复用已有 snapshot 结构：

* `SnapshotAccountPrefix + accountHash -> account trie value`
* `SnapshotStoragePrefix + accountHash + storageHash -> storage value`
* 对应读写接口在 `core/rawdb/accessors_snapshot.go`
* `core/state/snapshot.Snapshot` 已提供 `AccountRLP` 和 `Storage`

当前 `trie/archive` 实验实现里也有一层 `BFV1 || key -> value` flat value store，用来模拟同样语义。plus 版本的长期目标是对齐 geth snapshot，而不是额外维护第二套执行真相库。

### ASC 树层

ASC 树只负责证明结构：

* 热叶子保存完整 key 路径和 `valueRef`。
* 冷桶保存证明 metadata。
* 真实 value 一律从 Flat KV / snapshot 读取。

`valueRef` 是 key-bound value commitment：

```text
valueRef = H("BVR1" || len(K) || K || len(V) || V)
```

热叶和冷桶 ECMH 都使用同一份 `valueRef`。这样归档和 promotion 都不需要读取真实旧 value。

## 2. Minimalist Bucket

`ArchiveBucketNode` 的逻辑载荷是：

1. ECMH commitment
2. Cuckoo filter
3. Entry list

### ECMH

冷桶承诺为：

```text
Commitment = sum(hashToPoint(K_i, valueRef_i))
```

这里的 `K_i` 是完整逻辑 key，`valueRef_i` 是 `H(K_i,V_i)`。证明时 verifier 先检查：

```text
valueRef_i == H(K_i, V_i)
```

再用 `(K_i, valueRef_i)` 重算 ECMH。

### Cuckoo Filter

filter 只用于内存态快速拦截不可能命中的 key，输入必须是可消歧编码：

```text
uvarint(suffixBits) || suffixBytes
```

### Entry List

冷桶 entry 持久化 key 后缀和 `valueRef`：

```go
type ArchivedKey struct {
    Suffix     []byte
    SuffixBits int
    ValueRef   []byte
}
```

完整 key 由：

```text
ArchiveBucketNode.Path/PathBits + ArchivedKey.Suffix/SuffixBits
```

重构得到。

`ArchiveBucketSize` 仍是硬上限。默认 cuckoo 参数 `CuckooBuckets=32, CuckooSlots=4` 时，有效上限仍建议为 100。任何构建、追加、合并都不能持久化超过上限的大桶。

## 3. Archive Flow

触发条件：热状态变冷。

流程：

1. 从热 leaf 中取出完整 key 和 `valueRef`。
2. 从热树移除该 leaf。
3. 将 key 后缀和 `valueRef` 加入目标冷桶 entry list。
4. 对 bucket 执行：

   ```text
   ECMH_new = ECMH_old + hashToPoint(K, valueRef)
   ```

5. 更新 cuckoo filter、bucket metadata、父节点 hash 和 top root。
6. Flat KV 不做任何事，因为热写入时真实 value 已经在 Flat KV 里。

重要约束：

* prune / archive 不应逐项读取 Flat KV。
* prune / archive 不应写 `BucketHash + 0x01 -> ArchivedKV[]` 这种完整 payload。
* `FlushArchives` 不应承载 plus 默认路径的 durable 语义。

## 4. Write-Promotion Flow

触发条件：写请求命中冷状态 `K`。

流程：

1. 在 ASC 树中定位包含 `K` 的 cold bucket。
2. 从 bucket entry list 中取出旧 `valueRef_old`。
3. 对 bucket 执行冷桶销账：

   ```text
   ECMH_new = ECMH_old - hashToPoint(K, valueRef_old)
   ```

4. 从 entry list 和 cuckoo filter 中移除 `K`。
5. 用新值计算：

   ```text
   valueRef_new = H(K, V_new)
   ```

6. 将 `(K, valueRef_new)` 封装成新的热 leaf。
7. 在 Flat KV 中用 `V_new` 覆写 `K`。
8. ASC metadata、top root、Flat KV 覆写必须进入同一个 LevelDB batch。

这意味着 write-promotion 不再强制读取旧 `V_old`。旧 value 只在 proof 生成或执行读取时按需从 Flat KV 获取。

## 5. Lookup / Execution Read

执行读取路径：

1. 读取 Flat KV / snapshot。
2. 命中即返回完整 value。
3. 未命中按不存在处理。

ASC 树不参与普通 EVM 读取。ASC 树只用于：

* proof 生成和验证
* archive flow
* write-promotion / delete-promotion
* 统计、调试和重建

## 6. Proof Generation

冷状态 proof 生成流程：

1. 根据 ASC 路径找到候选 `ArchiveBucketNode`。
2. 用 cuckoo filter 快速排除不命中 key。
3. 在 entry list 中确认目标 key 后缀存在。
4. 对 bucket 内最多 `ArchiveBucketSize` 个 entry，从 Flat KV 读取真实 value。
5. 验证每项：

   ```text
   valueRef_i == H(K_i, V_i)
   ```

6. 用 `(K_i, valueRef_i)` 重算 ECMH 并对比 bucket commitment。
7. 输出目标 key/value、entry list、bucket metadata 和必要 sibling path。

因为 bucket size 有硬上限，无状态验证的暴力重构成本被固定在可控范围内。

## 7. Persistence Boundary

plus 默认路径：

* `ArchiveBucketNode` metadata 直接进入 ASC 树。
* entry list 直接跟随 bucket node 持久化。
* 真实 value 只在 Flat KV / snapshot。
* ASC 变动与 Flat KV 变动同 batch 提交。

legacy 路径可以暂时保留作兼容或实验，但不能成为 plus 默认语义：

* `BucketHash + 0x01 -> ArchivedKV[]`
* 独立 `ArchiveDB` 保存执行真实 value
* `FlushArchives` 后置写完整 bucket payload

## 8. 当前实现改造要点

已经对齐的方向：

* `ArchivedKey` 持久化 `Suffix/SuffixBits/ValueRef`。
* `recomputeBucket` / `blindAppendToBucket` / `blindDeleteFromBucket` 直接使用 `valueRef`，不再二次读 Flat KV。
* write-promotion 使用 bucket 中的旧 `valueRef` 做 ECMH 减法，不再强制读取旧 value。
* `FlushArchives` 只遍历 archive-dirty shard，普通 dirty shard 不再触发空 flush。
* `ForEach` 类遍历在返回 value 时单独回到 Flat KV / value store 解析。

仍需继续压实的 legacy 面：

* `serializeArchivedKV` / `deserializeArchivedKV` 仍保留 legacy 兼容。
* `pendingArchives` / `pendingArchiveItems` / `pendingAppends` / `pendingDeletes` 仍在类型中存在。
* `ArchiveDB` 仍可配置，实验路径应逐步收敛到同一个底层 DB / batch。
* `ArchiveStorageSize` 指标需要从完整 archive payload 口径改成 metadata / entry list 口径。

## 9. Bucket Placement

ASC 树保持冷热同树。冷桶仍是 ASC 树中的证明节点，不拆成独立 archive root。

成长型冷桶策略：

* `InternalNode.StubList` 是短期小桶合并缓冲，不是 archive subtree 的默认物理形态。
* prune / archive 已经构成 `InternalNode` archive subtree 时，必须保留 subtree
  形态并按绝对 path 合入普通 child 边；不得为了“小桶合并”再 flatten 到父
  `StubList`。
* 已经下沉到普通 child 边的 archive-only bucket/subtree，不得仅因为 item
  count 低于成熟阈值而回流成 side-mounted bucket。
* 单个小 bucket 仍可短暂侧挂在中间 `InternalNode.StubList`，以便后续同一区域
  冷数据继续合并；但一旦能够作为 child archive subtree 稳定挂载，应优先保留
  child 形态，避免整条 proof path 累积大量 side-mounted bucket。
* 侧挂桶的成熟阈值固定为 `70% * Config.ResolveArchiveBucketSize()`，向上取整。
* 当侧挂桶 `Count >= threshold` 时，只迁移该目标桶：
  * 从当前 `StubList` 移除。
  * 沿该桶的绝对 `Path/PathBits` 放回 ASC 同树 child 边。
  * 成为普通 child 路径下的 `ArchiveBucketNode` leaf，或与该 child 下已有 archive-only subtree 合并/重建。
* 下沉不得把 bucket 侧挂到其他 leaf 节点上，也不得拆出第二棵 archive tree。
* 若目标 child 包含热节点，不能把成熟桶侧挂到该热 leaf 上；实现应沿绝对 path 将成熟桶和热节点重建成同一 child 子树，使成熟桶继续作为普通 child 路径下的 archive bucket leaf / archive subtree 存在。
* 侧挂桶还受路径证明压力上限约束。`Config.ArchiveStubMaxBucketsPath`
  限制单个 `InternalNode.StubList` 可保留的 side-mounted bucket 数；默认
  64，负数可关闭。
* 当 `StubList` 超过该上限时，超出的可下沉 bucket 即使未达到 70%
  容量，也必须按自身绝对 path 的下一位批量下沉到普通 child archive
  subtree。该规则只改变桶放置，不改变 bucket commitment、Flat KV truth、
  冷热同树或 `ArchiveBucketSize` 硬上限。
* 路径压力下沉按目标 child 分组合并/重建，保留少量 side-mounted bucket
  的继续合并空间，同时防止 `MaxBucketsPath` 因稀疏小桶在同一节点堆积而
  线性放大 proof。该上限只是兜底；不能依赖“每层限 64”来允许整条路径累计
  数百个 side-mounted bucket。
* `ArchiveBucketSize` 仍是硬上限。合并和重建必须保证任何持久化 bucket 都不超过 `ResolveArchiveBucketSize()`。

归档构桶时机：

* prune 遇到完全变冷的子树时，不应在叶子父节点立即构造 1-item/小 item bucket。
* 该子树产生的 `ArchivedKV` 构建项应继续向父节点返回，直到遇到仍有热 child、已有 archive stub、或 shard root 的边界。
* 在边界处一次性构造 archive subtree/bucket。若结果是 archive subtree，
  按 child 边合入同一 ASC 树；若结果只是单个且确有合并价值的小 bucket，才进入
  `StubList` 缓冲，并受 70% 下沉和路径压力规则约束。
* 这样只改变工程上的构桶时机，不改变 bucket metadata、ECMH、proof 路径、冷热同树或 Flat KV 真相库语义。
* 实现不得留下“无 child、无 stub、仅携带待归档 items”的空 `InternalNode`；这种节点应返回 `nil + items` 继续向上聚合。

这个策略的目标是同时避免两类退化：

* bucket 太小：大量 1~5 项小桶造成证明和元数据膨胀。
* side-mounted bucket 太多：单路径 `MaxBucketsPath` 持续增长，拖慢 proof、promotion 和统计。

## 10. Statistics Boundary

统计不能改变 ASC 树运行态：

* `Stats()` 不得把 destructive commit 后卸载的 shard root 写回 `Shard.root`。
* `Stats()` 不得把临时加载的 child 挂回 parent。
* path storage 下，`Stats()` 使用 `shardID + entryPath` 临时读取节点，并校验 `H(nodeBytes) == expectedHash`，但不扩张长期 `nodePaths`。
* `Stats()` 不得复用运行态 clean-node cache；exact stats 应使用隔离读取视图，避免把诊断遍历读到的冷节点塞进热路径 LRU。
* 压测中的高成本结构统计应按诊断频率运行；普通 metrics window 可以复用最近一次精确结构统计。
* Stress experiments may set exact structural stats frequency to `0` to disable
  exact `Stats()` entirely. CSV columns should remain stable, but disabled
  structural fields must not feed archive growth protection or performance
  conclusions.

`Stats()` 可以作为诊断工具读取持久化节点，但它不应影响 root 计算、prune 行为、promotion 行为或实验速度结论。

## 11. FlushArchives Boundary

plus 默认路径中，`ArchiveBucketNode` metadata 和 entry list 已随 ASC 节点持久化。`FlushArchives` 不再承载 durable 语义：

* 不应写 `BucketHash + 0x01 -> ArchivedKV[]` 作为默认 archive payload。
* 不应保存真实 value；真实 value 只在 Flat KV / snapshot。
* legacy `pendingArchives` / `pendingArchiveItems` / `pendingAppends` / `pendingDeletes` 可暂留作兼容，但不能成为 plus 的默认正确性依赖。

## 12. Path Storage

ASC 的密码学寻址与物理存储寻址必须解耦：

* 节点 hash 继续参与父节点编码、Top Root 计算和 proof 校验。
* 磁盘 key 使用稳定的 `shardID + entryPath`，节点内容变化时原位覆盖。
* 普通更新不再执行 `Put(newHash) + Delete(oldHash)`。
* 只有节点真实消失或因 Patricia 压缩移动到其他 entry path 时，才删除旧路径。

当前路径键格式：

```text
Shard node: BPN1 || shardID(uint32) || pathBits(uint16) || pathBytes
Top node:   BPT1 || level(uint8) || prefix(uint32)
```

加载节点时仍使用父节点记录的 child hash 做完整性校验：

```text
H(load(pathKey)) == expectedChildHash
```

因此 Path Storage 不改变 ASC commitment，只改变节点在 LevelDB/RocksDB 中的物理位置。

当前实现保留 `hash` / `path` 两种模式用于迁移和实验。Path 模式只维护最新磁盘版本；深度回滚需要后续增加类似 MPT PathDB 的 diff layer / history。下一阶段应增加多轮 write buffer，使同一路径在落盘前的多次修改合并成最终版本。

### 12.1 PathDB-style Clean Node Cache

Path mode may keep an in-process, bounded clean-node blob cache:

* Cache key is the physical storage key, not the commitment key.
  * Shard node: `BPN1 || shardID || pathBits || pathBytes`.
  * Hash mode fallback: node hash.
* Cache value is serialized node bytes only. It must not cache mutable `Node`
  pointers, so destructive commit can still unload the live tree.
* Cache capacity must be bounded by both entry count and approximate serialized
  bytes. Entry count alone is not a safe memory guard for depth20 replay because
  archive metadata node sizes are uneven. The default byte guard is 512 MiB and
  may be raised only as an explicit experiment knob.
* `loadNode` / `loadNodeAtPath` may read from cache before LevelDB/RocksDB.
* `persistNode` should not eagerly warm nodes by default; long destructive path
  runs show that even root eager warming adds commit/LRU churn. Experiments may
  explicitly enable bounded eager warming, while the default path warms lazily
  after `loadNode`.
* Destructive path commits should not maintain transient child-hash
  `nodePaths` entries for every persisted internal node. Those entries are
  discarded when the shard root is unloaded, so the destructive path should
  persist bytes and stale deletes, then retain only the final root path mapping.
  Non-destructive commits may keep full hash-to-path mappings for in-memory
  follow-up reads.
* stale physical path deletion must evict the same cache key.
* Cache hits must still obey the same integrity rule:

```text
H(cachedNodeBytes) == expectedChildHash
```

This cache is an engineering optimization. It does not change ASC commitment,
top root calculation, proof semantics, bucket placement, or Flat KV truth.

Path mode may also keep an optional bounded decoded ECMH commitment-point cache:

* Cache key is the 33-byte bucket commitment.
* Cache value is the decoded curve point used only for in-memory merge math.
* The cache is not serialized and must not change bucket metadata, ECMH bytes,
  root calculation, or proof verification.
* It exists to avoid repeatedly decoding the same bucket commitment after
  destructive path reloads.
* It is disabled by default unless an experiment demonstrates real reuse; the
  current 2000 insert + 2000 update destructive path workload showed near-zero
  hit rate because merge input buckets are usually consumed immediately.

### 12.2 Promotion Gate and Diagnostics

Write-promotion may first check Flat KV existence:

* If Flat KV has no `K`, the write is a new execution key and can skip archive
  removal probing.
* If Flat KV has `K`, the write must run the existing archive removal path so
  side-mounted and sunk archive buckets are both truncated correctly.
* Explicit activation APIs still run full archive removal regardless of the
  Flat KV gate.

Diagnostics such as node-cache hit/miss, path-node DB gets, promotion checks,
promotion hits, bucket recomputes, prune child hit/skip, prune internal visits,
prune build item/bucket counts, and commitment-point cache hit/miss are
optional. They must be disabled by default or sampled at diagnostic frequency so
statistics do not become part of the measured runtime.

Replay experiments may additionally enable commit diagnostics without changing
ASC semantics:

* `binaryPathDiagnostics` records path cache hits/misses and path-node DB gets.
* `binaryCommitWorkers` caps parallel shard commit workers for LevelDB/RocksDB
  stall A/B tests; `0` keeps the binary default cap. Path-mode default should
  stay conservative rather than following `GOMAXPROCS` blindly, because high
  dirty-shard fanout can amplify LevelDB/GC stalls.
* `binaryNodeCacheBytesLimitMB` caps serialized clean-node cache memory. This
  is an engineering safety guard and must not affect commitment semantics.
* `binaryPhysicalDelete=false` must also disable stale path-node tracking in
  memory. Obsolete path nodes are unreachable once the parent/root points to the
  new hash and path key, so they may be left for offline garbage collection.
  They must not be accumulated in `Shard.staleSet` and replayed as millions of
  LevelDB deletes during root computation.
* `binaryCommitWatchdogSec` dumps goroutine stacks if one wrapper commit exceeds
  the configured wall time, allowing long global stalls to be distinguished from
  pruning, proof generation, or bucket placement.
* Per-shard prune pressure may be monitored as an experiment-only diagnostic.
  `PruneNextShard()` records the last pruned shard and the heaviest shard in the
  current reporting window, including prune wall time and, when path diagnostics
  are enabled, internal visits, child hit/skip decisions, collected leaves/stubs,
  and archive build item/bucket counts. This is used to detect hot shards or
  skewed prune work; it must not change pruning order, epoch bits, bucket
  placement, commitment calculation, or Flat KV writes.
* `binaryPruneShardMetrics` writes a per-prune CSV row for replay experiments so
  shard pressure distribution can be inspected without inferring it from root
  long-tail logs. Keep it off for pure performance runs unless the run is
  explicitly measuring prune locality.
* Slow wrapper commits should also report resource and locality counters:
  top-tree max child compute/apply time, raw path batch ops/bytes, max raw
  shard contribution, clean-node cache entries/estimated bytes, and Go runtime
  heap/GC counters. These are diagnostic observations only; they must not feed
  root calculation or bucket placement decisions.
* A shard root-commit long tail with tiny `nodeCount` must be split into
  serialize/hash/persist/batch-put/cache/bookkeeping timers before changing
  archive semantics. If the split time is mostly GC/runtime pause, the remedy is
  an engineering memory/concurrency fix, not a protocol change.

Direct archive-filter false-positive sampling may be used as an experiment-only
diagnostic: sample non-member suffixes inside each archive bucket's suffix
domain, query the in-bucket Cuckoo filter, and report
`false_positives / sampled_non_members`. This measures filter behavior directly;
it must run outside root/commit timing and must not affect bucket metadata,
commitment, proof semantics, or Flat KV state.
