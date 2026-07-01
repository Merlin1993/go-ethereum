# Archive Trie Archival 实现逻辑总结

> 旧设计归档：本文记录早期 archive payload / `ArchiveDB` / `FlushArchives` 路径的设计与排查思路，不代表当前 ASCT Plus 实现。当前代码结构和流程以 `ARCHIVE_CODE_PLUS.md` 为准。

本文档总结了 `trie/archive` 包中归档（Archival）的核心实现逻辑，旨在为测试用例排查和系统调试提供参考。

## 0. 近期性能优化说明（2026-04-22）

*   **ECMH 映射快路径**：`hashToPoint` 保持原始 try-and-increment 语义，但通过 Decred secp256k1 压缩公钥解析（`0x02 || x`）验证候选点；旧 big.Int 映射保留为测试参考。
*   **ECMH 批量累加**：大批量 `Add/Delete` 使用 worker-local Jacobian partial sum 后再合并，避免重复 affine 转换；空 commitment 下 1~3 个 hash 的新归档桶走直接 Jacobian 编码路径。
*   **Value 写入批处理**：`Shard.Put/Activate` 先把 value blob 暂存在内存，`Shard.CommitToBatch` 再与节点元数据写入同一个 batch，使压力测试里的异步 batch write 可以被下一轮 prune/commit 覆盖。读路径会先查 pending/staged value，再查 LevelDB，保证异步窗口内的 read-your-write。
*   **归档桶 hash 懒恢复**：反序列化后的 `ArchiveBucketNode` 不持久化内存态 `hash` 字段，读取归档数据前会根据桶元数据懒重建 hash，确保 reload 后仍可使用 `BucketHash + 0x01` 定位 archive data。
*   **Prune/Shrink 可达性边界**：归档后断开热子树指针必须保留冷路径可达性。当前归档剪枝优先把冷分支替换为 archive subtree，`StubList` 仅作为同路径或 shrink 上浮的兜底承载。

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
        *   如果部分变冷，热的分支保留，冷的分支转换为以 **ArchiveBucketNode** 为叶子的 archive subtree，并挂回原来的 child 边；只有入口路径已经等于当前节点位置、无法再通过 child bit 区分时，才挂入 `StubList`。
3.  **路径更新**: 当前实现统一使用**绝对路径**表示归档桶入口路径。递归下探时从分片前缀累积绝对路径，子方向使用 `appendBit` 向前扩展；桶内 `ArchivedKV.Suffix` 仅保存相对于桶入口路径的后缀。
4.  **迁移边界**: 归档桶允许在 Shrink 过程中向上提升到递归父节点，但不会向下迁移到唯一热子树；上浮时 `ArchiveBucketNode.Path` 保持绝对路径不变，不做重写。

### 路径位数上限 (`MaxPathBits`)
*   **统一边界**: 路径相关操作以 `MaxPathBits` 为硬上限，当前配置为 416 位。
*   **覆盖范围**: 账户路径通常为 160 位；Storage composite key 使用 `address(20 bytes) + slot(32 bytes)`，总计 52 bytes / 416 位。
*   **溢出保护**: `prependBit`、`appendBit`、`prependPath`、`concatPath` 均必须在超过 `MaxPathBits` 时立即失败，避免归档路径在深层存储键下溢出或截断。

### 节点收缩 (`tryShrink`) 与 StubList 上浮
*   **路径压缩**: 当一个内部节点只有一个热分支时，触发路径压缩（Collapse）。
*   **StubList 上浮**: 如果被压缩或删除的节点携带有归档桶（`StubList`），这些桶的内容不会被丢弃，而是上浮返回给递归上层。新生成的冷分支不应为了压缩而主动 flatten 到 `StubList`；它应优先作为 archive subtree 留在 child 边上。

---

## 3. 存储结构

### StubList (侧挂桶列表)
`InternalNode` 维护一个 `StubList []*ArchiveBucketNode`。
*   **哈希依赖**: 在 `InternalNode.Serialize()` 中，所有侧挂桶的哈希按顺序参与序列化。这意味着归档数据的变动会递归反映到 Shard 的 Root Hash 上。
*   **查找顺序**: 按“后进先出”（栈）顺序遍历。
*   **使用边界**: `StubList` 不是冷分支的默认承载结构；默认承载结构是普通 child 指针下的 archive subtree。`StubList` 只用于同入口路径、root 容器或 shrink promotion 这类无法用 child bit 表达的情况。

### ArchiveBucketNode (归档桶)
*   **绝对入口路径**: `ArchiveBucketNode.Path` 表示桶在分片中的绝对入口路径，而不是相对于当前挂载节点的相对路径。
*   **增量更新**: 系统支持受限的增量修改（`pendingAppends/pendingDeletes`）。增量追加不得突破 `Config.ResolveArchiveBucketSize()`；一旦追加会超限，必须回退为加载原 bucket 并重建 archive subtree。
*   **Filter**: 包含 Cuckoo Filter，Key 采用 `[uvarint(后缀位数)] + [后缀内容]` 编码以防碰撞，并支持 `MaxPathBits` 范围内的路径长度。
    *   **Fingerprint Hash Cache**: `alternateIndex` 仍使用原 Keccak(fp) 算法以兼容既有过滤器编码，但 16-bit fingerprint 的 65536 个哈希前缀会被全局缓存，避免归档桶重算时重复对 2 字节输入执行 Keccak。
*   **盲删除**: 当前“盲删除”指不重建整桶并通过 pending delete / 元数据更新完成落库；匹配具体删除项时仍可能读取桶数据或使用缓存项来定位目标。
*   **同入口前缀合并**: StubList 挂载新桶或提升旧桶时，会按 `Path + PathBits` 分组；同一绝对入口前缀下的多个小桶若合计不超过 `Config.ResolveArchiveBucketSize()`，会重建为一个桶。该合并不跨前缀，旧 archive data key 通过 pending 删除在 `FlushArchives` 阶段清理。
*   **桶大小配置**: 归档桶分裂阈值由 `Config.ResolveArchiveBucketSize()` 生效，实际限制会结合 Cuckoo 参数收敛，而不是仅取原始 `ArchiveBucketSize` 字段。

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
2.  **Archive child 检索**: 如果当前 child 是 `ArchiveBucketNode` 或 archive subtree，按普通 child 边继续下探；bucket 自身使用绝对入口路径确认命中。
3.  **冷路径回溯**: 如果热路径查找失败（例如遇到 `nil` 或路径不匹配），再检查当前所在 `InternalNode` 的 `StubList`。
4.  **桶内搜索**: 对候选 bucket 先过过滤器，再反序列化匹配具体后缀。

### 自动激活 (`Activate`)
*   **命中即激活**: `Shard.Get` 如果从归档桶中找到数据，会自动调用 `Activate`。
*   **移除与重插**: `Activate` 执行“盲删除”从桶中移除项，并将其作为热 `LeafNode` 插入。
*   **并发边界**: 当前实现不承诺并发读安全。`Get` 命中归档后会触发写路径，因此该 Trie 不能按“多 reader + 自动激活”模型使用；这里的锁主要用于保护分片内部状态，而不是提供通用并发读语义。

---

## 6. 底层设计细节

### 全状态完整性保证 (ECMH)
每个归档桶维护一个基于椭圆曲线的多集哈希（ECMH）承诺，用于覆盖该桶内归档项集合，并在桶元数据变化时参与父节点哈希计算。
*   **并行映射**: 批量 `Add/Delete/Verify` 时，`hashToPoint` 映射可并行计算，最终仍按确定顺序累加点，保持 ECMH 的顺序无关语义不变。
*   **内存取舍**: 当前不启用全局 point cache，避免数百万唯一归档项把内存再次顶高。
*   **低分配输入**: 归档项的 filter key、ECMH hash 输入和 `ArchivedKV` 序列化采用可复用缓冲/精确预分配，减少桶重算和 pending archive 写入时的短命切片。
*   **当前边界**: 已落地的是 archive-bucket 级承诺；本文不再把它表述为统一覆盖热路径节点与冷路径数据的 shard-level 全状态承诺。

### 并发模型
*   **分片并行**: `Trie.shardsMu` 仅控制分片容器，各分片拥有独立锁 `Shard.mu`。当前实现支持多线程并行 Commit，不存在“每轮同时裁剪多个分片”的顶层调度。
*   **裁剪粒度**: `PruneNextShard()` 每次只处理一个 shard；是否在单个 shard 内继续并行化，属于实现优化空间，不是当前外部语义保证。
*   **读取语义**: 由于 `Get` 可能触发 `Activate`，读路径并不保证无副作用，也不应按通用并发读接口理解。

### Commit / Flush 边界
*   **承诺计算边界**: 影响 root / commitment 的逻辑必须留在 `Commit` / `CommitToBatch` 主路径中完成，`FlushArchives` 只负责把已决定的 archive 数据落库。
*   **合并边界**: 同入口前缀小桶合并会改变 bucket metadata 与父节点 hash，因此必须在内存树挂载/剪枝路径中完成；合并后的新 archive data 写入、旧 archive data 删除仍通过 pending 队列交给 `FlushArchives`。
*   **非破坏持久化路径**: `CommitToBatch -> Write -> FlushArchives -> Reload` 的设计目标，是避免 archive flush 时间影响主 commit 的承诺计算速度，同时保证 archive 数据最终可恢复。
*   **实现备注**: `Trie.Commit()` 是 destructive 的便捷路径，其调度顺序可与外部 batch 的 non-destructive 流程不同；理解 durability 时应优先区分这两条路径。

### 统计口径
*   **`ArchivedDataSize` / `ArchiveItems`**: 表示逻辑归档项数量，可在生成、删除归档项时增减。
*   **`ArchiveStorageSize` / `Archive_MB`**: 表示 archive store 中已落库的物理数据量，应以 `FlushArchives` 实际 `Put/Delete` 为准，而不是仅依据 pending delta 推导。
*   **`FalsePositiveCount`**: 统一按 `TrieStats.FalsePositiveCount` 口径统计 Cuckoo Filter 假阳性；历史压测里若同时打印全局计数器，应视为兼容性输出而非新的统计定义。

### 配置口径
*   当前有效配置包括 `ShardDepth`、`ArchiveBucketSize`、`ArchiveItemCacheLimit`、`ArchiveDB`、`CuckooBuckets`、`CuckooSlots`。
*   `ShardCacheLimit` 不属于当前实现语义，若旧分支/旧文档出现该字段，应视为过期配置。

### 内存管理 (Node Pool)
*   通过 `NodePool` 复用 `InternalNode` 和 `LeafNode` 对象，在高频剪枝和数据注入期间显著降低 Golang GC 压力。

### 空分片状态 (Empty Shard)
*   若分片内所有热数据均被裁剪，分片根节点 `s.root` 将动态转换为单个 `ArchiveBucketNode` 或一颗纯归档子树。

---

### 内存管理 (Node Pool)
*   通过 `NodePool` 复用 `InternalNode` 和 `LeafNode` 对象，在高频剪枝和数据注入期间显著降低 Golang GC 压力。

### 空分片状态 (Empty Shard)
*   若分片内所有热数据均被裁剪，分片根节点 `s.root` 将动态转换为单个 `ArchiveBucketNode` 或一颗纯归档子树。

---

## 7. 验证与实验逻辑

为了确保归档逻辑的正确性与高性能，系统中设计了两套核心实验方案。

### 一致性实验 (`TestBinaryTrieConsistency`)
*   **核心逻辑**: 采用“双路并行”对比。
    *   **对比对象**: 标准 MPT (无归档) vs. Archive Trie (有归档)。
    *   **追踪技术**: 使用 `consistencyTracer` 挂载在 EVM 钩子上，为每区块的状态变更生成一个反映全量改动的 `WriteHash`。
    *   **验证点**: 每一区块结束时，强制校验两个引擎的 **WriteCount** 和 **WriteHash**。由于标准 MPT 与 Archive Trie 的节点结构和哈希域不同，二者的结构性 State Root 不作为相等性判据。
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
1.  **数据库容量**: State DB 增长曲线变缓；Archive DB 从 0 开始快速增长。这里的 `Archive_MB` 应按已落库的物理 archive 数据量理解。
2.  **内存占用**: RSS 和 Heap 因为节点被清理而趋于平稳（或随状态总量缓慢爬升）。
3.  **读指标**: "Miss (Existent/Archived)" 从 0 变为正值，伴随 Cuckoo Filter 的查询记录；假阳性统一看 `TrieStats.FalsePositiveCount`。
4.  **通信指标**: 出现“证明生成耗时”和“证明大小”统计，反映了由于自动赎回（Activation）带来的额外开销。

#### Flush 与 Commit 的性能边界
*   `FlushArchives` 的职责是 archive 数据落库，不应承担影响 root / commitment 的逻辑。
*   如果某项工作会改变承诺结果，就必须在 `Commit` / `CommitToBatch` 中完成，而不是推迟到 flush 阶段。
*   这样做的目标是把 archive IO 延迟与主 commit 的承诺计算解耦，避免 flush 时间直接拖慢 commit。

#### 裁剪并行性边界
*   顶层调度每次只裁剪一个 shard；不存在一次同时裁剪多个分片的外部语义。
*   “裁剪可并行”如果成立，也只应理解为单个 shard 内部实现可继续优化，而不是多 shard 并行 prune。

#### 历史压力数字的解释
*   文中的固定 MB / 延迟数字是历史观测样本，用于识别趋势，不应被理解为当前实现必须精确复现的常量。
*   当前更重要的判断标准是：热树规模是否稳定、逻辑归档项是否按预期增长、以及 archive store 的物理大小是否在 flush 后持续增长。

#### A. 主网回放压测 (`TestExpireStateProcessor`)
*   **逻辑**: 使用真实主网数据文件（Transaction Streamer）进行长时间回放。
*   **核心指标**: 统计区块证明（Block Proof）的大小分布、过滤器假阳性率、各阶段处理延迟。

#### B. 滑动窗口 Trie 压测 (`TestArchiveTrieStress`)
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
*   **测试项**：`TestArchiveTrieStress`
*   **通过标准**：
    1.  **规模**：在 `ShardDepth=8` 的配置下，处理至少 50~100 万个 Item 注入。
    2.  **增长判定**：
        *   **进入归档期**：在一个全量周期（$2^8 \times 1000 = 25.6$ 万项）之后，`Archive_MB` 必须开始稳定增长。
        *   **预期比例**：归档量应与注入量成正比。例如在 50% 更新率下，每注入 100 万项，理论上应有约 **40~50 万项** 进入归档。如果 `Archive_MB` 仅增长几 KB 或项数极少（如仅几百项），通常意味着裁剪判定逻辑存在漏洞（漏判）或 Epoch 翻转未生效。
    3.  **现象**：`ArchivedDataSize` 统计指标应与预期的冷数据量大致匹配。
*   **核心观测点**：确认进入第二个周期后，归档库文件物理生成并被成功索引。
*   **分阶段 ShardDepth 验证**：
    1.  先使用较小的 `ShardDepth`（例如 8）快速压测，用较短周期触发 epoch 翻转与归档，判断裁剪、归档落库、`ArchivedDataSize` / `Archive_MB` 增长链路是否成功。
    2.  再使用较大的 `ShardDepth`（例如 `log2(65536*16)=20`）压测分片数量变多后的行为，重点观察 `Commit` 是否随 shard 数量或 dirty shard 分布明显退化、bucket 是否过度分散、`BucketItemsAvg/P50/P95/P99/Max` 是否异常偏小，以及 `MaxBucketsPath` 是否过大导致单次查询需要轮询过多桶。
    3.  大 `ShardDepth` 压测不能只跑满一个 shard 轮询周期：第一轮主要完成分片覆盖，第二轮才开始形成可裁剪旧数据，至少跑到第三轮后再判断归档量、bucket 分布和单路径桶数，否则容易把“尚未过期”误判为“bucket 过度分散”或“归档不足”。

---

## 9. 性能与一致性标准 (Pass Criteria)

在代码提交或逻辑重大变更后，建议通过以下自测流程进行归档系统验证：

### 9.1 一致性实验 (`TestBinaryTrieConsistency`)
*   **通过标准 (Pass Criteria)**:
    1.  **WriteCount 零偏差**: 处理主网任意区块区间后，Archive Trie 记录的状态写入次数必须与标准 MPT 完全一致。
    2.  **WriteHash 校验**: `consistencyTracer` 记录的所有账户、存储变更的哈希指纹必须与 MPT 线路 100% 匹配。State Root 仅作为各自引擎的持久化锚点记录，不要求跨结构相等。
    3.  **零异常报错**: 运行过程中不允许出现任何 "Node Not Found" 或持久化层面的加载/序列化错误。
*   **观测细节**: 异常发生时，通过 `consistencyTracer` 输出的交易索引（Tx Index）快速定位状态分歧点。

### 9.2 压测实验 (`TestArchiveTrieStress` / `TestExpireStateProcessor`)
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

* **Archived child subtrees must replace stale hot nodes**: after a cold
  left/right subtree is converted into an archive subtree, the old hot child
  pointer/hash must be removed and replaced by the archive subtree. Keeping the
  original hot child alive makes `LeafCount`, state nodes, prune traversal cost,
  and heap usage grow linearly with total injected items.
* **Archive bucket paths are absolute and extend forward**: child traversal from
  an accumulated absolute prefix must use `appendBit`, not `prependBit`.
  `prependBit` reverses the child direction relative to the absolute key and
  can make archived buckets unreachable once the hot child is detached.
* **Nodes with side-mounted buckets promote those buckets during archive
  shrink**: an `InternalNode` with a non-empty `StubList` may still collapse,
  but the buckets must be returned to the recursive parent and attached only if
  no child edge can preserve their entry path.
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

## 11. State DB Write Amplification Fix (2026-04-22)

The long stress run later showed a different bottleneck: `LeafCount` and heap
were stable, but `State` kept growing and `WriteWait` reached second-level
latency. The root cause was in the write pipeline rather than the prune logic.

* **Worker batch deletes must be replayed**: `Trie.CommitToBatch` commits shards
  in parallel through a worker-local `memBatcher`. The old `memBatcher.Delete`
  was a no-op, so stale trie node deletes produced by shards were silently
  dropped before reaching the real LevelDB batch.
* **Dirty shard tracking is not archive flush tracking**: non-destructive
  commits now clear `dirtyShards` after the top tree is computed, but archive
  flush uses a separate `archiveDirtyShards` set so `CommitToBatch -> Write ->
  FlushArchives -> Reload` remains durable.
* **Side-mounted archive buckets are embedded metadata**: buckets in
  `InternalNode.StubList` are already serialized into their parent node. They
  should not be written again as standalone state nodes on every commit.
* **Changed bucket metadata invalidates the old bucket hash**:
  `blindAppendToBucket` and `blindDeleteFromBucket` mark the previous bucket
  metadata hash stale and mark the bucket dirty before recomputing its new hash.
* **Pending archive deltas are flushed durably**: `pendingAppends` and
  `pendingDeletes` are materialized to the archive store and old archive data
  keys are deleted, instead of only keeping the delta chain in memory.

### 11.1 Updated Stress Signal

With real stale deletes enabled, the benchmark includes LevelDB deletion and
compaction cost that was previously being skipped. The expected result is lower
State DB growth and stable memory, but not the earlier artificially low 20-30ms
average when deletes were not actually replayed.

Observed verification after the write-pipeline fix:

* 1M items: `State=838.37 MB`, `Archive=72.63 MB`, `LeafCount=131960`,
  `ArchiveItems=877432`, `Avg=57.33ms`, `WriteWait=31.85ms`.
* 2M items: `State=1.61 GB`, `Archive=163.75 MB`, `LeafCount=132192`,
  `ArchiveItems=1888341`, `Avg=84.28ms` with one 2.8s LevelDB outlier;
  most windows stayed around `Avg=54-59ms`.

## 12. StubList Promotion During Shrink (2026-04-22)

Archive-prune shrink uses a parent-return channel for side-mounted buckets:

* `shrinkPromote` collapses an `InternalNode` with zero or one hot child.
* If the collapsed node owns `StubList` buckets, those buckets are removed from
  the node and returned to the recursive caller.
* The caller attaches promoted buckets to its own `StubList`, so the bucket
  moves upward and remains searchable before descending into the hot branch.
* The promoted bucket keeps its absolute `ArchiveBucketNode.Path`; no suffix
  rewrite is needed during the move.
* The shard root is the only no-parent edge case. If root-level buckets are
  promoted, a minimal root container is created only to hold the promoted
  buckets and the remaining hot child.

This replaces the earlier conservative rule that blocked shrink whenever a
node had side-mounted buckets. That conservative rule preserved reachability but
kept unnecessary middle nodes and increased hot-tree depth.

## 13. Put Rewrite Must Evict Old Archive Entry (2026-04-24)

Rewriting an already-archived key through `Trie.Put` must not leave the old
archive version behind.

* **`Put` and `Activate` share the same archive-eviction rule**: before the hot
  insert runs, the shard removes any matching archived item from the reachable
  archive child, `StubList`, or root bucket path.
* **Archive deletion can empty the shard root**: if that removal clears the last
  root-level bucket, the shard drops the empty root container instead of
  keeping an empty `ArchiveBucketNode` or `InternalNode`.
* **Root-node eviction still needs stale delete propagation**: when the old root
  container disappears, its prior node hash is pushed into the stale-delete set
  so the State DB does not keep the orphaned serialized node forever.
* **Archive flush tracking must follow `Put` as well**: `Trie.Put` now marks the
  shard in `archiveDirtyShards` whenever the rewrite produced pending archive
  appends/deletes. This keeps the non-destructive durability sequence correct:
  `CommitToBatch -> Write -> FlushArchives -> Reload`.

Expected correctness signal after this fix:

* Rewriting an archived key through `Trie.Put` keeps `ArchivedDataSize` flat or
  decreases it; it must not create `ArchiveItems + Leaves > Injected` drift for
  that key.
* Reloading after a non-destructive commit must show only the new hot value, and
  the old archive bucket entry must be gone.

## 14. Archive Bucket Size Bound and Placement (2026-05-28)

Long stress runs showed that `Bucket_Items_Max` could grow far beyond
`Config.ResolveArchiveBucketSize()` while P99 stayed near the expected bound.
That means the bug is not a general distribution shift; it is a small number of
paths where bucket construction or append logic bypassed the hard cap.

The updated invariant is:

* **Bucket size is a hard cap**: no normal archive bucket may exceed
  `Config.ResolveArchiveBucketSize()`. With `cuckooBuckets=32` and
  `cuckooSlots=4`, the effective cap is 100.
* **Append is not allowed to grow a bucket past the cap**:
  `blindAppendToBucket` must refuse an append that would exceed the resolved
  limit. A caller that needs to add more items must load the source items and
  rebuild the affected archive subtree so the split is reflected in trie
  metadata before commit.
* **Flush must not silently persist an oversized append**: pending append
  materialization is only an IO step. If it would serialize more than the
  resolved limit into one bucket, it is a logic error and must fail instead of
  writing the oversized bucket.
* **Degenerate splits continue walking the path**: when all items fall on the
  same side at a split bit, `buildArchiveSubtree` must advance one bit and try
  again instead of returning a large fallback bucket. Only `MaxPathBits` is a
  true unsplittable boundary.
* **Archive buckets are leaf nodes by default**: a cold child branch should be
  replaced by an archive subtree whose leaves are `ArchiveBucketNode`s. It
  should not be flattened into the hot parent node's `StubList` merely because
  the sibling branch is hot.
* **StubList is the same-path escape hatch**: side-mounted buckets remain valid
  only when the archive bucket's entry path is exactly the current trie
  position and there is no child edge that can distinguish it from the hot
  branch.

This placement rule keeps `MaxBucketsPath` from growing just because many cold
branches share a hot ancestor, and it keeps proof generation bounded by the
configured bucket cap instead of by the full history on one path.

## 15. Sparse Archive Bucket Compaction (2026-06-11)

The next long run exposed the opposite failure mode after enforcing the hard
bucket cap: with `ShardDepth=20` (about 1M shards), early pruning often archives
only one or two items per shard. If each tiny cold branch immediately becomes a
deep archive leaf, later adjacent items cannot find a shallow merge window and
the system degenerates into one bucket per few items.

Observed bad signal:

* `ArchiveItems=22,076,815`
* `BucketCount=14,219,598`
* `BucketItemsAvg=1.55`, `P50=1`, `P95=3`, `P99=5`
* `BucketItemsMax=100`

This means the hard cap works, but bucket utilization collapses.

The updated rule is a hybrid placement policy:

* **Small cold branches are side-mounted first**: if a newly pruned cold branch
  has no more than `Config.ResolveArchiveBucketSize()` items, it is converted to
  a sparse bucket and attached to the current ancestor's `StubList` instead of
  being pushed down as a standalone archive child.
* **StubList compaction is enabled by default**: `CompactArchiveStubs` defaults
  to true. Adjacent sparse buckets are sorted by absolute path and greedily
  rebuilt into groups up to the resolved bucket cap.
* **Large cold branches still become archive subtrees**: branches larger than
  the cap keep using archive subtree construction, so proof size stays bounded
  by `BucketItemsMax <= ResolveArchiveBucketSize()`.
* **Existing tiny archive children are folded upward opportunistically**: when
  prune revisits an archive-only child subtree whose total item count fits in
  one bucket, the subtree is replaced by one ancestor-side sparse bucket. This
  gradually repairs small buckets created before the compaction rule was added.
* **Hard cap remains non-negotiable**: neither sparse compaction nor pending
  append materialization may write a bucket over the resolved cap.

Expected stress signal after this change:

* `BucketItemsMax` remains exactly at or below the cap (100 for
  `cuckooBuckets=32,cuckooSlots=4`).
* `BucketItemsAvg/P50` should move toward tens of items per bucket rather than
  staying near 1.
* `MaxBucketsPath` may rise because sparse buckets are intentionally held at
  ancestors, so long runs must monitor it together with proof latency.

Local 1M-item verification with `ShardDepth=8`, `BatchSize=1000`:

* `ArchiveItems=868,215`
* `BucketCount=12,872`
* `BucketItemsAvg=67.45`, `P50=67`, `P95=97`, `P99=99`
* `BucketItemsMax=100`, `MaxBucketsPath=54`
