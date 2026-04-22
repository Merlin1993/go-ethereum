# Binary Trie Archival 实现逻辑总结

本文档总结了 `trie/binary` 包中归档（Archival）的核心实现逻辑，旨在为测试用例排查和系统调试提供参考。

## 0. 近期性能优化说明（2026-04-22）

*   **ECMH 映射快路径**：`hashToPoint` 保持原始 try-and-increment 语义，但通过 Decred secp256k1 压缩公钥解析（`0x02 || x`）验证候选点；旧 big.Int 映射保留为测试参考。
*   **ECMH 批量累加**：大批量 `Add/Delete` 使用 worker-local Jacobian partial sum 后再合并，避免重复 affine 转换；空 commitment 下 1~3 个 hash 的新归档桶走直接 Jacobian 编码路径。
*   **Value 写入批处理**：`Shard.Put/Activate` 先把 value blob 暂存在内存，`Shard.CommitToBatch` 再与节点元数据写入同一个 batch，使压力测试里的异步 batch write 可以被下一轮 prune/commit 覆盖。读路径会先查 pending/staged value，再查 LevelDB，保证异步窗口内的 read-your-write。
*   **归档桶 hash 懒恢复**：反序列化后的 `ArchiveBucketNode` 不持久化内存态 `hash` 字段，读取归档数据前会根据桶元数据懒重建 hash，确保 reload 后仍可使用 `BucketHash + 0x01` 定位 archive data。
*   **Prune 优化边界**：归档后立即断开热子树指针不是安全的局部优化，必须配合 StubList 可达性和路径压缩规则整体重构；否则部分侧挂桶可能在 shrink 后不可达。本轮保持正确性优先，不启用该优化。

## 1. 核心架构与术语

### 分片 (Sharding)
*   Trie 按 `ShardDepth` 划分为多个独立的分片（子树）。
*   归档操作在 **Shard 级别** 独立运行。
*   分片 ID 由 Key 的前 `ShardDepth` 位决定。

### 纪元位 (Epoch Bit)
*   每个节点（`LeafNode`, `InternalNode`）都有一个 `epoch` 字段。
*   `globalEpochBit` (0 或 1) 用于区分冷热数据。
*   `InternalNode.epoch` 的预留位缓存子树 epoch mask，用于快速判断子树是全热、全冷还是冷热混合；旧节点缺少 valid 标记时会回退到递归检查。
*   **状态对齐关系**:
    *   **裁剪前**: 在一个裁剪周期内，尚未遍历到的分片包含旧纪元的数据（`epoch != globalEpochBit`）。此时插入的新数据会使用当前全局位，导致分片内数据暂时“纪元不一致”。
    *   **裁剪中**: `Shard.Prune(global)` 被调用，所有冷数据（旧纪元）被移入归档桶，分片内的热数据状态完成对齐。
    *   **裁剪后**: 分片已“清空”旧数据，完全使用当前全局纪元位记录新插入的数据，处于平稳运行环境。

---

## 2. 归档流程 (Pruning/Archival)

### 触发机制
通过 `trie.PruneNextShard()` 顺序轮询分片。每轮完成所有分片后，`globalEpochBit` 翻转。

### 递归处理 (`pruneAndArchive`)
1.  **节点遍历**: 递归遍历分片树。
2.  **判定逻辑**:
    *   如果节点纪元不匹配（冷数据），则标记为归档。
    *   **LeafNode**: 转换为 `ArchivedKV` 记录（包含后缀路径和位长度）。
    *   **InternalNode**: 
        *   如果整个子树变冷，递归收集所有叶子位 `ArchivedKV`。
        *   如果部分变冷，热的分支保留，冷的分支转换为 **ArchiveBucketNode** 挂载到该节点的 `StubList`。
3.  **路径更新**: 在归档项上抛过程中，使用 `prependBit` 和 `prependPath` 动态维护其相对于分片根部的完整路径。

### 路径位数上限 (`MaxPathBits`)
*   **统一边界**: 路径相关操作以 `MaxPathBits` 为硬上限，当前配置为 416 位。
*   **覆盖范围**: 账户路径通常为 160 位；Storage composite key 使用 `address(20 bytes) + slot(32 bytes)`，总计 52 bytes / 416 位。
*   **溢出保护**: `prependBit`、`appendBit`、`prependPath`、`concatPath` 均必须在超过 `MaxPathBits` 时立即失败，避免归档路径在深层存储键下溢出或截断。

### 节点收缩 (`tryShrink`) 与 StubList 上浮
*   **路径压缩**: 当一个内部节点只有一个热分支时，触发路径压缩（Collapse）。
*   **StubList 上浮**: 如果被压缩或删除的节点携带有归档桶（`StubList`），这些桶的内容不会被丢弃，而是被重新打散并“上浮”返回给递归上层。最终，这些内容会根据补全后的路径重新挂载到更高层的归档桶中，确保树结构的极致紧凑。

---

## 3. 存储结构

### StubList (侧挂桶列表)
`InternalNode` 维护一个 `StubList []*ArchiveBucketNode`。
*   **哈希依赖**: 在 `InternalNode.Serialize()` 中，所有侧挂桶的哈希按顺序参与序列化。这意味着归档数据的变动会递归反映到 Shard 的 Root Hash 上。
*   **查找顺序**: 按“后进先出”（栈）顺序遍历。

### ArchiveBucketNode (归档桶)
*   **增量更新**: 系统支持增量修改（`pendingAppends/pendingDeletes`），避免每次微小变动都触发全量桶重建。
*   **Filter**: 包含 Cuckoo Filter，Key 采用 `[uvarint(后缀位数)] + [后缀内容]` 编码以防碰撞，并支持 `MaxPathBits` 范围内的路径长度。
    *   **Fingerprint Hash Cache**: `alternateIndex` 仍使用原 Keccak(fp) 算法以兼容既有过滤器编码，但 16-bit fingerprint 的 65536 个哈希前缀会被全局缓存，避免归档桶重算时重复对 2 字节输入执行 Keccak。
*   **盲删除**: 在 `Activate` 过程中，通过元数据更新而非全量加载来实现桶内项的移除。

---

## 4. 存储位置与物理布局

### 逻辑存储接口
系统通过 `Config.ArchiveDB` 接口管理归档数据。如果未显式配置独立的归档库，数据默认存储在状态库中。

### Key 编码规则 (规避冲突)
为了防止归档桶的哈希与 Trie 原始节点的哈希冲突，系统在持久化时使用了特殊的 Key 编码：
*   **存储 Key**: `BucketHash + 0x01` (增加 1 字节后缀)。
*   这确保了 Archive 数据与热状态元数据即使哈希相同，在物理层面上也是隔离的。

### 物理路径 (典型配置)
在测试和实际运行中，归档数据通常存储在独立目录中：
*   **主网回放场景**: 由 `-binaryArchiveDir2` 标志指定（如 `F:\expire_data\expire_state_db_achive`）。
*   **压力测试场景**: 位于 `baseDir/asct_archive_db`（如 `F:\trie_stress_data_final_v3\asct_archive_db`）。

### 内存缓冲
分片维护一个 `pendingArchives` 映射。在 `Commit` 过程触发 `FlushArchives` 之前，新生成的归档桶暂存在内存中。

---

## 5. 查找与恢复 (Lookup & Activation)

### 查找序列
1.  **热路径检索**: 优先走标准的 Trie 查找逻辑。
2.  **冷路径回溯**: 如果热路径查找失败（例如遇到 `nil` 或路径不匹配），则检查当前所在 `InternalNode` 的 `StubList`。
3.  **桶内搜索**: 遍历 `StubList`，先过过滤器，再反序列化匹配具体后缀。

### 自动激活 (`Activate`)
*   **命中即激活**: `Shard.Get` 如果从归档桶中找到数据，会自动调用 `Activate`。
*   **移除与重插**: `Activate` 执行“盲删除”从桶中移除项，并将其作为热 `LeafNode` 插入。

---

## 5. 底层设计细节

### 全状态完整性保证 (ECMH)
每个 Shard 维护一个基于椭圆曲线的多集哈希（ECMH）。它对“热路径节点数据 + 冷路径归档数据”统一生成累加承诺，确保存储引擎在不遍历树的情况下也能验证冷热混合状态的正确性。
*   **并行映射**: 批量 `Add/Delete/Verify` 时，`hashToPoint` 映射可并行计算，最终仍按确定顺序累加点，保持 ECMH 的顺序无关语义不变。
*   **内存取舍**: 当前不启用全局 point cache，避免数百万唯一归档项把内存再次顶高。
*   **低分配输入**: 归档项的 filter key、ECMH hash 输入和 `ArchivedKV` 序列化采用可复用缓冲/精确预分配，减少桶重算和 pending archive 写入时的短命切片。

### 并发模型
*   **分片并行**: `Trie.shardsMu` 仅控制分片容器，各分片拥有独立读写锁 `Shard.mu`，支持多线程并行 Commit 或 Prune。

### 内存管理 (Node Pool)
*   通过 `NodePool` 复用 `InternalNode` 和 `LeafNode` 对象，在高频剪枝和数据注入期间显著降低 Golang GC 压力。

### 空分片状态 (Empty Shard)
*   若分片内所有热数据均被裁剪，分片根节点 `s.root` 将动态转换为单个 `ArchiveBucketNode` 或一颗纯归档子树。

---

## 7. 验证与实验逻辑

为了确保归档逻辑的正确性与高性能，系统中设计了两套核心实验方案。

### 一致性实验 (`TestBinaryTrieConsistency`)
*   **核心逻辑**: 采用“双路并行”对比。
    *   **对比对象**: 标准 MPT (无归档) vs. Binary Trie (有归档)。
    *   **追踪技术**: 使用 `consistencyTracer` 挂载在 EVM 钩子上，为每区块的状态变更生成一个反映全量改动的 `WriteHash`。
    *   **验证点**: 每一区块结束时，强制校验两个引擎的 **WriteCount** 和 **WriteHash**。由于标准 MPT 与 Binary Trie 的节点结构和哈希域不同，二者的结构性 State Root 不作为相等性判据。
*   **排查用途**: 若出现不一致，通过打印 `consistencyTracer` 的日志（如 `debug_b50107.txt`）可以精确定位是哪一笔交易的哪个状态（余额、Nonce 等）导致了分歧。

### 压测与性能实验

#### 裁剪触发时机 (Pruning Trigger)
裁剪的真实发生依赖于 **Shard 轮询周期** 和 **Epoch 翻转**：
*   **计算公式**: 一个完整周期 = $2^{ShardDepth}$ 次 `PruneNextShard` 调用。
*   **主网回放场景**: 默认 `ShardDepth=20`，若每区块调用一次裁剪，则一个周期约为 **105 万个区块**。
*   **压力测试场景**: 默认 `ShardDepth=16`，若每 1000 个 Item 调用一次，则一个周期约为 **6553.6 万次注入**。
*   **真实裁剪点**: 只有在 **第二个周期** 开始时（此时 `globalEpochBit` 已翻转），第一个周期注入的“旧”节点才会被判定为冷数据并移入归档桶。因此，在实验初期（第一个周期内），归档库大小通常为 0。

#### 数据变化趋势
随着实验进入第二个周期及以后，统计数据会出现以下显著变化：
1.  **数据库容量**: State DB 增长曲线变缓；Archive DB 从 0 开始快速增长。
2.  **内存占用**: RSS 和 Heap 因为节点被清理而趋于平稳（或随状态总量缓慢爬升）。
3.  **读指标**: "Miss (Existent/Archived)" 从 0 变为正值，伴随 Cuckoo Filter 的查询记录。
4.  **通信指标**: 出现“证明生成耗时”和“证明大小”统计，反映了由于自动赎回（Activation）带来的额外开销。

#### A. 主网回放压测 (`TestExpireStateProcessor`)
*   **逻辑**: 使用真实主网数据文件（Transaction Streamer）进行长时间回放。
*   **核心指标**: 统计区块证明（Block Proof）的大小分布、过滤器假阳性率、各阶段处理延迟。

#### B. 滑动窗口 Trie 压测 (`TestTrieStressBinary`)
*   **监控**: 重点观察 RSS/Heap 波动以及归档库 (Archive DB) 的膨胀速度。

---

## 8. 基础验证（快速自测）

在进行 10B 级超大规模测试前，必须通过以下基础验证。这两项测试旨在确保核心逻辑在“中等规模”下的鲁棒性：

### 8.1 一致性基础验证
**目标**：确保在真实主网数据流下，数万个区块的连续处理不出现任何状态分歧。
*   **测试项**：`TestBinaryTrieConsistency`
*   **通过标准**：
    1.  **规模**：运行至少 **1~5 万个区块**。
    2.  **结果**：全程 **0 报错**，且每一块的 `WriteCount` 与 `WriteHash` 均与 MPT 路线完全一致。
*   **运行建议**：使用 `-blocks 50000` 等参数限制运行范围。

### 8.2 压测基础验证
**目标**：验证裁剪逻辑是否能真实触发出归档行为，并确保统计数据链路畅通。
*   **测试项**：`TestTrieStressBinary`
*   **通过标准**：
    1.  **规模**：在 `ShardDepth=8` 的配置下，处理至少 50~100 万个 Item 注入。
    2.  **增长判定**：
        *   **进入归档期**：在一个全量周期（$2^8 \times 1000 = 25.6$ 万项）之后，`Archive_MB` 必须开始稳定增长。
        *   **预期比例**：归档量应与注入量成正比。例如在 50% 更新率下，每注入 100 万项，理论上应有约 **40~50 万项** 进入归档。如果 `Archive_MB` 仅增长几 KB 或项数极少（如仅几百项），通常意味着裁剪判定逻辑存在漏洞（漏判）或 Epoch 翻转未生效。
    3.  **现象**：`ArchivedDataSize` 统计指标应与预期的冷数据量大致匹配。
*   **核心观测点**：确认进入第二个周期后，归档库文件物理生成并被成功索引。

---

## 9. 性能与一致性标准 (Pass Criteria)

在代码提交或逻辑重大变更后，建议通过以下自测流程进行归档系统验证：

### 8.1 一致性实验 (`TestBinaryTrieConsistency`)
*   **通过标准 (Pass Criteria)**:
    1.  **WriteCount 零偏差**: 处理主网任意区块区间后，Binary Trie 记录的状态写入次数必须与标准 MPT 完全一致。
    2.  **WriteHash 校验**: `consistencyTracer` 记录的所有账户、存储变更的哈希指纹必须与 MPT 线路 100% 匹配。State Root 仅作为各自引擎的持久化锚点记录，不要求跨结构相等。
    3.  **零异常报错**: 运行过程中不允许出现任何 "Node Not Found" 或持久化层面的加载/序列化错误。
*   **观测细节**: 异常发生时，通过 `consistencyTracer` 输出的交易索引（Tx Index）快速定位状态分歧点。

### 8.2 压测实验 (`TestTrieStressBinary` / `TestExpireStateProcessor`)
*   **通过标准 (Pass Criteria)**:
    1.  **系统稳定性**: 持续注入 1B+ KV 或回放 1M+ 主网区块期间，系统内存（RSS/Heap）必须趋于平稳，严禁出现内存泄露（OOM）或 Panic。
    2.  **数据可访问性**: 即使在数据被大规模归档（Cold items > 90%）的情况下，所有 `Get` 请求及后续的 `Activate` 动作必须 100% 成功。
    3.  **归档生效判定**: 进入第二个裁剪周期后，`Archive_MB` 必须显示线性增长速度。
*   **核心性能指标**:
    *   **Cuckoo FP 控制**: 监控 `Stats.FalsePositiveCount`，假阳性导致的 IO 开销占比需维持在极低比例（通常 < 1%）。
    *   **证明延迟表现**: 区块证明（Block Proof）的生成耗时在归档过程中应保持平稳分布，不应随 Archive DB 的膨胀而出现显著的线性恶化。

---

> [!TIP]
> **本地快速自测**: 如果修改了 StubList 处理逻辑，建议将 `ShardDepth` 暂时调低至 4 或 8（如前所述），通过几十个区块的短程注入即可触发 Epoch 翻转与收缩（Shrink）上浮逻辑。

## 10. Hot Tree Stabilization Fix (2026-04-22)

This section records the invariants added after the stress run where `State`,
RSS/Heap, and latency continued to grow through millions of injected items.

* **Archived child subtrees must be detached from the hot tree**: after a cold
  left/right subtree is converted into an `ArchiveBucketNode` and side-mounted
  in `StubList`, the corresponding child pointer, child hash, and child epoch
  must be cleared. Keeping the child alive makes `LeafCount`, state nodes,
  prune traversal cost, and heap usage grow linearly with total injected items.
* **Archive bucket paths are absolute and extend forward**: child traversal from
  an accumulated absolute prefix must use `appendBit`, not `prependBit`.
  `prependBit` reverses the child direction relative to the absolute key and
  can make archived buckets unreachable once the hot child is detached.
* **Nodes with side-mounted buckets are not shrinkable**: an `InternalNode` with
  a non-empty `StubList` is an addressable anchor for archived sibling data.
  Collapsing it into the only remaining hot child can bypass those buckets
  during lookup.
* **Stale node deletes are required for async/non-destructive commits too**:
  stress mode can commit through a batch while `destructive=false`; stale node
  hashes still have to be deleted from LevelDB, otherwise the physical state DB
  keeps old trie nodes indefinitely. LevelDB may reclaim disk space only after
  compaction, so the expected signal is a much slower growth rate, not an
  immediate file-size shrink.
* **ArchiveDB=nil must still support bucket lookup**: when no external archive
  DB is configured, `FlushArchives` stores `archiveDataKey(hash)` in the state
  DB. Reads must use that same fallback path.

### 10.1 Expected Stress Signal

With `ShardDepth=8`, `stressEpochItems=100000`, `stressBatchSize=1000`, and
async IO enabled, a 1M item local run should show:

* `LeafCount` stabilizing after the archive cycle starts instead of following
  total injected items linearly.
* `ArchiveItems` increasing roughly linearly as cold data is moved out of the
  hot tree.
* `Avg` staying around the 20-30ms band on the local benchmark setup, with
  occasional IO/GC outliers still possible.

Observed verification after the fix:

* 300k items: `LeafCount=130657`, `ArchiveItems=171578`, `Avg=24.55ms`.
* 1M items: `LeafCount=131830`, `ArchiveItems=877807`, `Avg=25.62ms`.
