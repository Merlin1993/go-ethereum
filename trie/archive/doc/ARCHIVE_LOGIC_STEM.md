# ASCT Archive Logic Stem

本文档描述 ASCT 当前的 Stem 归档版本。它以 `ARCHIVE_LOGIC_PLUS.md` 为基础，但数据单位已经从“单个键值”改成“31 字节 Stem + 256 个 suffix 槽位”，根的组织方式也改成了一棵完整的二叉树。

旧的 Logic Plus 文档保留用于说明按单键归档的历史方案；启用 Stem 模式时，以本文档为准。

## 1. 核心结论

Stem 模式可以概括为四点：

1. 32 字节状态键被拆成前 31 字节的 Stem 和最后 1 字节的 suffix。
2. 外层 ASCT 只保存 Stem；一个 Stem 内最多保存 256 个实际值。
3. Stem 是统一的冷热和归档单位。一个 suffix 更新，会让整个 Stem 回到活跃树中。
4. 分片只是同一棵二叉树在固定深度下切出的子树，不再有独立的 Top Tree。

整体结构如下：

```text
全局二叉根
└── 按 key 前若干位选择分片
    └── 外层 ASCT 子树
        └── Stem 叶子（31 字节 key）
            ├── suffix 0
            ├── suffix 1
            ├── ...
            └── suffix 255
```

这里需要区分：

- **Stem 是逻辑归档单位**：同一 Stem 的值一起计时、一起归档、一起恢复。
- **ArchiveBucket 是物理容器**：一个桶可以装多个 Stem 记录。恢复一个 Stem 时，只从桶里移除这一条记录，不会恢复同桶的其他 Stem。

因此，当前设计不是“一 Stem 一个物理桶”，而是“一个 Stem 对应桶里的一条归档记录”。

## 2. 状态键和 Stem

### 2.1 键的拆分

Stem 模式接收 32 字节二叉状态键：

```text
[ 31-byte stem ][ 1-byte suffix ]
```

- 前 31 字节参与外层 ASCT 的二叉寻址。
- 最后 1 字节选择 Stem 内的 256 个槽位之一。
- 外层树永远不会看到完整的 32 字节键，只会看到 31 字节 Stem key。

### 2.2 以太坊状态映射

当前实现复用仓库内二叉状态树的 SHA-256 键生成规则：

| 数据 | 在 Stem 中的位置 |
| --- | --- |
| 账户 RLP | 账户头 Stem 的 suffix 0 |
| storage slot 0..63 | 同一账户头 Stem 的 suffix 64..127 |
| code chunk 0..127 | 同一账户头 Stem 的 suffix 128..255 |
| 其余 storage 和 code chunk | 按同一键生成规则进入后续 Stem |

每个 code chunk 是 32 字节：1 字节元数据加 31 字节代码。

storage 的二叉键不可反向还原原始地址和 slot。因此，当前实现会在 storage value 外包一层很小的记录，带上原始地址和 slot。普通读取时会去掉这层记录；调试遍历时依赖它找回原始信息。账户销毁不再通过全树遍历找 slot，而是使用第 5.4 节的账户索引。

删除账户只删除账户 RLP 对应的 suffix 0。storage 和 code 是否删除仍由原有状态清理流程决定，不能因为它们和账户数据位于同一个 Stem 就顺带删除。

## 3. Stem 内部结构

### 3.1 稀疏存储

内存中的一个 Stem 有 256 个槽位，但落盘时只写实际存在的值。编码内容为：

```text
版本标记
+ 32-byte ValuesRoot
+ 32-byte presence bitmap
+ 按 suffix 顺序排列的非空槽位长度和值
```

32 字节 bitmap 表示哪些 suffix 存在。空字节串也可以是一个“存在的值”；真正删除必须清掉对应的存在位。

### 3.2 Stem 内部根

256 个 suffix 在 Stem 内组成一棵固定 8 层的二叉树：

```text
256 个槽位 -> 128 -> 64 -> 32 -> 16 -> 8 -> 4 -> 2 -> ValuesRoot
```

空槽位、实际值和内部二叉节点使用不同的哈希前缀，避免不同类型的数据产生相同含义的哈希。

对单个 suffix 的内部证明固定包含 8 个相邻哈希。这个证明只说明某个 suffix 是否属于某个 `ValuesRoot`，并不单独证明该 Stem 已经进入全局 ASCT 根。

### 3.3 外层叶子实际承诺的内容

当前外层 ASCT 叶子不是只保存 `ValuesRoot`，而是对以下内容整体生成引用：

```text
31-byte stem key + 完整的 Stem 编码数据
```

所以，只提供一个 suffix、`ValuesRoot` 和 8 个内部证明，目前还不足以通过外层 ASCT 的值校验。完整的两层无状态证明还没有接入现有读写接口，这一点在第 9 节单独说明。

## 4. 一棵二叉树和分片

### 4.1 分片不是另一棵树

`ShardDepth` 表示用 Stem key 开头的多少个 bit 选择分片：

```text
shardID = stemKey 的前 ShardDepth 个 bit
```

这些 bit 本来就是全局二叉路径的一部分。分片只是把该深度以下的子树交给独立对象管理，便于并行读取、归档和提交；它不引入第二套逻辑根。

分片以上的节点只是普通二叉分叉：每个节点只有左、右两个子哈希。提交时，先更新发生变化的分片根，再沿实际二叉路径逐层向上计算，最终得到全局根。这个“汇总”就是计算二叉树根的正常过程，不再作为单独的 Top Tree 设计。

### 4.2 读取和提交

- 读取一个 Stem 时，根据它的前缀直接找到对应分片，再在分片子树中查找。
- 没有访问到的分片和上层路径不需要全部加载。
- 提交时只处理发生变化的分片，并重算这些分片通向全局根的路径。
- PreCommit 计算 dirty shard 根时可以并行；随后持久化会复用已经算出的节点 hash。节点内容、路径或子 hash 变化时必须先让旧 hash 失效。
- `ShardDepth = 0` 时，全局根就是唯一分片的根，不增加额外包装层。

### 4.3 落盘路径

路径存储模式下：

- 分片内部节点使用 `BPN1 || shardID || pathBits || path`。
- 分片以上的普通二叉节点使用 `BPR1 || depth || prefix`。

`BPR1` 只是分片边界以上二叉节点的物理存储前缀，不代表一棵独立的树。

## 5. 更新流程

### 5.1 单个 suffix 更新

一次写入按下面的顺序执行：

1. 把 32 字节状态键拆成 Stem key 和 suffix。
2. 用 Stem key 读取当前完整 Stem 数据。
3. 修改或删除目标 suffix。
4. 重新计算该 Stem 的 `ValuesRoot`。
5. 重新编码整个 Stem。
6. 用 31 字节 Stem key 把新编码写回外层 ASCT。

如果 Stem 原来已经归档，第 2 步会读取当前完整 Stem，第 6 步会移除它的冷记录，并把更新后的 Stem 作为一个活跃叶子重新放回树中。

这意味着：

- 未修改的 suffix 会和新值一起进入新的 `ValuesRoot`。
- 实时状态只保留新的根，不在活跃树里继续保存旧的 `ValuesRoot`。
- 同一 Stem 的所有 suffix 共用同一份冷热状态和更新时间。

### 5.2 批量更新

同一批写入会先按 Stem 分组。每个受影响的 Stem 最多读取一次、编码一次，再把所有 suffix 更新一起写回。

这是必要的性能优化。否则，同一区块内连续修改同一个 Stem 的多个 suffix，会反复重算 256 槽位的内部根并反复写入完整 Stem。

### 5.3 删除

- 删除一个 suffix 后，只要 Stem 内还有其他值，就重写该 Stem。
- 删除最后一个 suffix 后，才删除外层 Stem 叶子和对应的 flat value。

### 5.4 账户销毁

账户地址和原始 slot 经过二叉键映射后无法从树路径反推。若账户销毁时再遍历所有 Stem 并按地址过滤，耗时会随着全局状态增长，而且每销毁一个账户都可能重新扫一遍全树。

当前实现为此维护两组轻量索引：

```text
ASIS\x01 || 20-byte address || 32-byte raw slot
ASIC\x01 || 20-byte address || 8-byte code chunk number
```

- storage 写入、删除时同步增加或移除 slot 索引。
- code 更新时同步更新 chunk 索引；代码缩短后，多余的旧 chunk 会被明确删除。
- 账户销毁时只扫描该地址的索引，批量删除它的 storage 和 code suffix，工作量与该账户自身的数据量成正比，不再与全局 Stem 数量成正比。
- 销毁批次会先按 Stem 分组；每个受影响 Stem 最多加载、解码和写回一次，同时批量登记索引删除，避免逐 slot 重复读取同一 Stem。
- 旧数据删除先执行，同一块内销毁后重建产生的新数据随后写入，因此不会误删新账户的数据，也不会让旧 slot 重新出现。
- 树、flat value 和索引在同一次提交中落盘，避免根和索引处于不同版本。

## 6. 归档和恢复流程

### 6.1 更新时间统计

冷热标记位于外层 Stem 叶子上，而不是每个 suffix 上。因此：

- 任一 suffix 被更新，整个 Stem 都被视为刚更新。
- 到期扫描按 Stem 判断，不再逐个 suffix 判断。
- 一个 Stem 不可能出现“一部分活跃、一部分已归档”的状态。

这是当前设计为了保持逻辑简单而明确接受的粒度。

### 6.2 归档

Stem 到期后：

1. 从活跃子树中移除 Stem 叶子。
2. 把 Stem key 的剩余路径和它的值引用放入归档桶。
3. 归档桶更新快速筛选信息和整体承诺。
4. Stem 的完整编码数据仍保留在 flat value store 中。

归档桶容量限制的是 **Stem 记录数**，不是 suffix 数量。一个装有 100 条记录的桶，代表 100 个 Stem；这些 Stem 内实际存在的 suffix 总数可能远大于 100。

桶仍可能出现在分片根、内部节点旁或普通子边上。具体放置位置只影响查找和空间效率，不改变“每条记录对应一个 Stem”的语义。归档记录不会跨越分片，因为一个 Stem key 只属于一个分片。

### 6.3 冷读取

读取已归档 Stem 中的一个 suffix 时：

1. 沿 Stem key 路径找到候选归档桶。
2. 先做快速筛选，再精确匹配 Stem key。
3. 从 flat value store 读取完整 Stem 编码。
4. 校验该编码与桶内保存的值引用一致。
5. 解码 Stem 并返回目标 suffix。

普通读取不会把 Stem 自动变回活跃状态。只有显式恢复或写入才会激活它。

### 6.4 恢复和写后恢复

当前恢复单位是整个 Stem：

1. 读取并校验完整 Stem 编码。
2. 从归档桶中只删除这一条 Stem 记录。
3. 把整个 Stem 作为活跃叶子放回外层树。
4. 更新沿途二叉根。

同一归档桶中的其他 Stem 不受影响。

如果恢复后某个 suffix 又发生更新，系统直接在完整 Stem 上生成新的编码和新根。因为当前节点本地仍保存完整 Stem 数据，所以以后恢复或读取其他 suffix 不需要额外记住旧根；其他 suffix 已经被带入新根。

## 7. 数据持久化

### 7.1 flat value

真实 Stem 数据使用下面的键保存：

```text
BFV1 || 31-byte stem key -> encoded stem payload
```

活跃叶子和归档桶都不重复保存完整数据，只保存由 Stem key 和完整编码共同计算出的 32 字节值引用。

一次提交中，树节点、归档元数据和 flat value 的改动进入同一个提交边界，避免根已经更新但真实 Stem 数据尚未可见。

### 7.2 当前“归档”的实际含义

当前版本归档的是树结构中的活跃叶子，**没有从本地删除已归档 Stem 的完整 payload**。所以它已经能测量树结构压缩、归档桶开销和 Stem 粒度带来的影响，但还不是“全节点完全不保存归档数据”的最终形态。

如果以后全节点删除冷 Stem payload，恢复时必须从外部获得以下二者之一：

- 当前完整 Stem 数据；或
- 能证明目标 suffix 的数据，加上从证明对应根推进到当前根所需的全部公开更新。

后者需要额外的数据可用性和更新见证协议，当前代码尚未实现。

## 8. 根变化如何处理

Stem 内任一 suffix 更新都会改变 `ValuesRoot`，外层 Stem 值引用也会随之改变，最终全局根继续变化。这是正常状态更新，不需要在活跃树中并存新旧两个根。

当前方案能够这样处理，是因为：

1. 恢复或更新时已经取得完整 Stem。
2. 未更新的 suffix 也在这份完整 Stem 中。
3. 重新计算后，所有 suffix 共同受新的 `ValuesRoot` 约束。
4. 以后读取其他 suffix 时，以当前完整 Stem 和当前根为准。

只有在“节点不保存完整归档 Stem、每次只从外部取回一个 suffix”的方案中，才会遇到旧证明如何跟随每次根更新的问题。当前实现没有选择这条路径，因此也没有维护旧根链或逐 suffix 的增量见证。

## 9. 证明模型和当前边界

完整的 Stem 证明逻辑上分成两层：

1. **外层证明**：证明某个 Stem 记录属于当前 ASCT 全局根，或者属于某个受全局根约束的归档桶。
2. **内部证明**：用固定 8 个相邻哈希证明某个 suffix 属于该 Stem 的 `ValuesRoot`。

当前代码已经具备 Stem 内部证明，也保留了外层归档桶的查找和校验能力，但还没有提供一套组合后的“只带一个 suffix 就能无状态恢复”的接口。

主要原因是当前外层值引用约束的是 **完整 Stem 编码**，而不只是 `ValuesRoot`。因此，内部 suffix 证明不能替代完整 payload 的外层校验。

如果下一阶段要支持不保存冷 payload 的全节点，需要在以下方向中做选择：

- 外部提供完整 Stem，继续沿用当前外层值引用；
- 调整外层承诺，使它可以直接约束 `ValuesRoot` 和必要元数据，再设计两层组合证明；
- 引入可验证的更新见证，让旧 suffix 证明能推进到当前根。

这些属于后续协议设计，不应被写成当前已经完成的能力。

## 10. 统计口径

Stem 模式同时保留两组口径，避免把“Stem 数”和“真实值数”混在一起：

| 指标 | Stem 模式下的含义 |
| --- | --- |
| `LeafCount` | 活跃 Stem 数 |
| `ArchivedDataSize` | 已归档 Stem 记录数 |
| `ActiveLogicalValues` | 活跃 Stem 内实际存在的 suffix 总数 |
| `ArchivedLogicalValues` | 已归档 Stem 内实际存在的 suffix 总数 |
| `ActiveLogicalValueReadFailures` | 统计活跃 Stem 时无法读取或解码 payload 的数量 |
| `ArchivedLogicalValueReadFailures` | 统计归档 Stem 时无法读取或解码 payload 的数量 |
| `BucketItems*` | 每个归档桶包含的 Stem 记录数 |

实验中比较真实数据密度时，应优先使用 logical value 口径，例如：

- `Archive_Bytes_Per_Logical_Value`
- `State_Bytes_Per_Active_Logical_Value`

仅看 `ArchivedDataSize` 会低估一个 Stem 内聚合的实际值数量。

统计 suffix 数时只读取 Stem 编码头部的 bitmap，不会为每个 Stem 重算完整的 256 槽位根。两个失败计数必须同时为零；否则 logical value 总数是不完整的，不能用于计算归档率或单位存储成本。

## 11. 配置和兼容性

Stem 模式由 `Config.StemMode` 控制，回放实验使用：

```text
-binaryStemArchive=true
```

默认仍为关闭，便于和原来的按单键模式对照。

以下变化都影响持久化布局或根编码：

- 按 Stem 保存 flat value；
- 外层树 key 从完整逻辑键变成 31 字节 Stem key；
- 分片以上改成普通二叉根路径；
- path storage 使用新的 `BPR1` 根路径记录。

因此实验必须使用全新的数据库目录。不能用旧根直接打开，也不能在同一个数据库上来回切换 Stem 模式和按单键模式。账户索引带有版本标记；如果数据库里已有 Stem payload 却没有该标记，打开时会直接报错，防止缺失索引时静默漏删账户数据。

## 12. 必须保持的规则

实现和后续修改需要保持以下规则：

1. 同一 Stem 的所有 suffix 只能存在于一份编码数据中。
2. 一个 Stem 只能处于活跃或已归档中的一种状态，不能拆成两部分。
3. 更新任一 suffix 都要刷新整个 Stem 的更新时间和外层值引用。
4. Stem 编码中的 `ValuesRoot` 必须能由实际 suffix 值重新计算得到。
5. 外层 Stem key 必须固定为 31 字节；suffix 不参与外层寻址。
6. 一个归档桶可以包含多个 Stem，但同一 Stem 不能同时留下重复的有效记录。
7. 桶容量按 Stem 记录数计算，逻辑值统计按实际 suffix 数计算。
8. 分片是全局二叉树的固定深度子树，不存在第二套独立的聚合树语义。
9. 当前恢复依赖完整 Stem payload 可用；没有该数据时不能声称恢复已经完成。

## 13. 与 Logic Plus 的主要差异

| Logic Plus | Logic Stem |
| --- | --- |
| 单个键值独立计时和归档 | 整个 Stem 统一计时和归档 |
| 桶里一条记录对应一个键值 | 桶里一条记录对应一个 Stem |
| 更新只恢复一个键值 | 更新会恢复并重写整个 Stem |
| 叶子数可近似看作实际值数 | 必须分开统计 Stem 数和 suffix 数 |
| 根上方有独立汇总结构的表述 | 分片前缀就是同一棵二叉树的正常路径 |
| 单层键值证明 | 外层 Stem 证明加内部 suffix 证明 |

## 14. 代码对应关系

- `trie/archive/stem.go`：Stem 编码、内部根、内部证明和 Stem 级读写。
- `trie/utils/binary_tree.go`：账户、storage 和 code 的 32 字节二叉键映射。
- `trie/archive_trie.go`：状态层接口和 Stem 模式适配。
- `trie/archive/root.go`：分片以上的普通二叉根路径。
- `trie/archive/value_store.go`：完整 Stem payload 的 flat value 存取。
- `trie/archive/config.go`：Stem 开关和 Stem/suffix 两套统计口径。

这份文档描述的是当前已经落到代码中的 Stem 版本。后续如果改为删除冷 payload 或支持单 suffix 无状态恢复，应先更新第 7～9 节，再调整实现和实验口径。
