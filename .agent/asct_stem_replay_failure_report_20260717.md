# ASCT Stem重放失败问题单：handleDestruction全局扫描疑似导致超线性退化

## 1. 摘要

本机ASCT Stem模式1000万区块重放在区块`1,796,872`失败，只完成17.97%。
失败不是panic、磁盘不足、OOM、archive wait或guard主动停止，而是一次
`handleDestruction`耗时超过配置的3分钟硬上限：

```text
handleDestruction exceeded configured limit at block 1796872:
got 3m6.7333981s > 3m0s
```

失败块总commit为`3m6.7353991s`，其中`handleDestruction=3m6.7333981s`。
同一诊断行中DB write、commit workers和prune分项接近0。

源码中存在一个与该退化高度吻合的风险路径：Stem模式的单账户storage iterator
调用`StemTrie.ForEach`遍历整个StemTrie，再逐条解码并按地址筛选；per-key模式则
使用账户前缀定向遍历。若`StateDB.deleteStorage`的snapshot快路径失败并进入slow
fallback，每销毁一个有storage的账户都可能扫描全局全部stem/逻辑值。

当前日志没有记录fast/slow deleteStorage分支，因此“本轮一定进入了slow fallback”
仍需加诊断确认；但全局扫描实现本身是确定存在的扩展性问题。

## 2. 实验身份

```text
Run:
F:\codex_asct\results\mainnet\asct\run_local_asct_stem_workers16_depth20_20260717_113212_10m

Git HEAD:
aad650f39eb5069207460b2ec02e9e1f81ca8bfb

Variant:
stem_workers16_depth20

Started:
2026-07-17T11:33:21.9442443+08:00

Finished:
2026-07-17T16:52:29.8428545+08:00

Status / exit:
failed / 1
```

核心参数：

```text
-useBinaryTrie2=true
-binaryStemArchive=true
-useVerkle2=false
-useKV2=false
-blocks 10000000
-statsInterval2 100000
-shardDepth 20
-archiveBucketSize 100
-cuckooBuckets 16
-cuckooSlots 4
-binaryPhysicalDelete=false
-binaryNodeStorage path
-binaryCommitWorkers=16
-binaryNodeCacheBytesLimitMB=512
-binaryAsyncPrune=true
-binaryPruneShardMetrics=true
-archiveOverlapBudgetMs 8000
-maxRootPipelineMs 900000
-maxHandleDestructionMs 180000
-maxPruningMs 30000
```

完整命令在运行目录的`run_command.ps1`。

## 3. 已证实的失败事实

### 3.1 完成位置

- `run_status.json`: `failed`。
- `go_test.exit.txt`: `1`。
- `asct_mainnet_metrics.csv`: 1行表头+17行完整阶段数据，最后阶段为
  `1,600,000..1,699,999`。
- `asct_prune_shard_metrics.csv`最后完整记录到`1,796,871`。
- 下一块`1,796,872`触发fatal，因此没有第18个完整阶段点。
- stderr为0 bytes；stdout包含FAIL，没有panic，没有最终状态根和PASS。

### 3.2 慢Commit数量

从stdout的`[ASCT_COMMIT_DIAG]`解析得到：

| 项目 | 数量 |
|---|---:|
| Commit诊断记录 | 854 |
| Commit > 8s | 432 |
| Commit > 20s | 284 |
| Commit > 30s | 157 |
| Commit > 60s | 32 |
| Commit > 180s | 1 |
| Archive wait over 8s | 0 |

最大值：

```text
block=1796872
commit=186735.399 ms
handleDestruction=186733.398 ms
```

因此不能把这批长尾归因于async archive的8秒重叠预算。

### 3.3 最后完整阶段性能

区块窗口`1,600,000..1,699,999`：

| 指标 | Stem | 同窗口per-key基线 |
|---|---:|---:|
| Commit Avg | 18.67 ms | 0.80 ms |
| Commit Max | 59,048 ms | 232 ms |
| PreCommit Avg | 2.93 ms | 0.28 ms |
| PostCommit Avg | 15.74 ms | 0.52 ms |
| Charged root Avg | 18.37 ms | 0.50 ms |
| DB write Avg | 0.30 ms | 0.29 ms |
| Archive compute Avg | 11.88 us | 11.96 us |
| Archive wait over budget Max | 0 | 0 |

Stem的Commit平均约为基线23.3倍，阶段最大值约为基线254.5倍；DB write和
archive compute与基线同量级，差异集中在PostCommit/handleDestruction。

## 4. 源码调用链

### 4.1 失败检查

```text
core/tree_test/processor_expire_state_test.go:1401
checkDurationLimit(t, "handleDestruction", ...)
```

配置`-maxHandleDestructionMs 180000`最终通过`t.Fatalf`终止实验。提高或关闭该阈值
只能让实验继续，不会解决实际的3分钟stall。

### 4.2 StateDB storage wiping

```text
core/state/statedb.go:1164  StateDB.handleDestruction
core/state/statedb.go:1197  s.deleteStorage(addr, addrHash, prev.Root)
core/state/statedb.go:1123  StateDB.deleteStorage
core/state/statedb.go:1087  StateDB.slowDeleteStorage
core/state/statedb.go:1267  CommitHandleDestruction计时结束
```

`handleDestruction`遍历本块销毁账户。对于原账户存在且storage root非空的账户，
调用`deleteStorage`清除全部storage。

`deleteStorage`先尝试snapshot快路径；snapshot不可用或返回错误时，无条件回退到
`slowDeleteStorage`。当前实现没有记录：

- 本块销毁账户数。
- 每个账户的storage slot数。
- snapshot快路径是否成功。
- snapshot错误内容。
- slow fallback是否执行。
- 每个账户各分段耗时。

### 4.3 Per-key与Stem iterator差异

Per-key模式：

```text
trie/archive_trie.go:1415  newArchiveStorageIterator
trie/archive_trie.go:1438  bt.ForEachPrefix(accountPrefix, ...)
```

它使用20-byte账户域前缀，只遍历目标账户对应的shard/path。

Stem模式：

```text
trie/archive_trie.go:1443  newArchiveStemStorageIterator
trie/archive_trie.go:1448  stem.ForEach(...)
trie/archive/stem.go:524   StemTrie.ForEach
trie/archive/stem.go:529   backend.ForEachAll(...)
```

单账户模式同样调用全局`stem.ForEach`，然后：

```go
recordAddr, slot, value, ok := decodeArchiveStemStorageRecord(encoded)
if ok && recordAddr == address {
    // retain the target account only
}
```

`StemTrie.ForEach`会遍历backend全部active/archive stem，解码每个stem，并枚举其
256个suffix位置。因此单账户slow storage wipe的复杂度不是只和该账户storage规模
相关，而可能是：

```text
O(本块需清理的账户数 * 全局stem/逻辑值数量)
```

iterator还会先把所有匹配leaf复制进内存，再由`Next`读取，并非流式返回。

## 5. 根因判断分级

### 已证实

1. 实验由`handleDestruction > 3m`主动判FAIL。
2. 最慢块几乎全部时间位于`handleDestruction`。
3. archive wait、DB write和日志中的prune分项不是主要耗时。
4. Stem单账户iterator包含全局`StemTrie.ForEach`扫描。
5. Per-key iterator有账户前缀定向遍历，Stem iterator没有对应索引。

### 高置信推断

1. 本轮至少部分销毁块进入了slow deleteStorage路径，触发全局stem扫描。
2. 随全局stem数量增长，单次销毁耗时不断增长，因此在约180万区块达到3分钟。
3. 失败块可能包含多个需wipe storage的销毁账户，或一个slot很多的账户；目前日志
   无法区分。

### 尚待验证

1. 区块`1,796,872`具体销毁了哪些账户、各有多少storage slot。
2. snapshot快路径为什么失败或是否本身也很慢。
3. 3分6秒中全局扫描、stem decode、slot收集各自占比。
4. 是否存在重复扫描同一全局stem集合的多个销毁账户。

## 6. Bucket稀疏不是当前失败根因

最后完整阶段结构：

```text
Outer leaves                 356,997
Archive records               66,456
Archived logical values      253,924
Buckets                       63,307
Root buckets                  63,307
Deep buckets                       0
Child buckets                      0
Bucket Avg/P50/P95/P99/Max   1.05/1/1/2/4
Max path                           1
```

这组“全部在root、每桶约1项”在Stem+depth20下基本符合数学预期，不足以证明下沉bug：

1. `BinaryTreeKey`在`trie/utils/binary_tree.go:35-46`通过SHA-256生成stem前缀。
2. `shardDepth=20`意味着`2^20=1,048,576`个shard。
3. 66,456个近似均匀stem record投放到这些shard，平均负载仅`0.0634`。
4. 理论占用shard约64,394个，理论每个已占用shard约1.032项。
5. 实测63,307个bucket、每桶1.050项，与理论非常接近。

因此根上稀疏主要是参数/设计效果：当前outer stem数量远小于depth20 shard数，没有
达到继续下沉的阈值。若目标是更高bucket填充率，应重新评估Stem模式的shard depth，
而不是强制对1到4项的root bucket继续下沉。

另外，bucket item统计的是outer stem record，不是逻辑suffix value。最后阶段每个
archive record平均包含约3.82个逻辑值，后续图表应同时展示record和logical value口径。

## 7. 另外两个统计问题

### 7.1 ActiveLogicalValues恒为0

最后阶段：

```text
Trie_Child_Node_Count=356997
Active_Logical_Values=0
Archived_Logical_Values=253924
```

在Stem模式下，356,997个active outer leaves不可能对应0个逻辑suffix，因此
`Active_Logical_Values`统计没有成功读取/解码active stem payload。当前不能计算可靠的
逻辑值整体归档率，也不能把`Cumulative_Archived_vs_Active_Pct`当成逻辑值归档率。

建议让Stats返回payload读取/解码失败数，而不是静默跳过后留下0。

### 7.2 Prune详细CSV体积大但诊断不足

`asct_prune_shard_metrics.csv`达到109,181,311 bytes，写到区块`1,796,871`，但
`Detailed_Counters_Enabled=false`，失败前记录的分项均为0。此次失败确实不在prune，
但该文件没有帮助定位storage wipe，却产生了约109 MB数据。

建议将storage wipe诊断单独输出，不要借用prune字段；详细计数关闭时可降低全零逐块
CSV的记录频率。

## 8. 建议修复方向

### P0：消除单账户storage wipe的全局扫描

可选方向：

1. 为Stem模式维护`account -> stem/suffix`辅助索引，storage wipe按账户索引读取。
2. 让snapshot快路径直接产出该账户的slot，并批量构造`StemUpdate Delete`，避免
   `ArchiveStorageTrie.NodeIterator`全局扫描。
3. 如果snapshot失败，slow fallback必须有可定位账户的持久索引；不能退化成扫描全部
   global stems。
4. 多个销毁账户应避免重复加载和解码同一全局数据集。
5. iterator应尽量流式或批量处理，避免先复制全部匹配值。

由于BinaryTree stem key是SHA-256派生，不能直接照搬per-key的20-byte账户前缀扫描；
需要明确的反向索引或使用snapshot中的账户storage索引。

### P1：补齐诊断

在`handleDestruction/deleteStorage`增加：

```text
block
destroyed account count
account address
origin storage root
fast path time / result
snapshot error
slow fallback time
global stems visited
logical suffixes visited
matched slots
generated delete nodes
```

诊断必须在超过阈值fatal前输出。

### P1：修复逻辑值统计

- Active stem payload读取失败必须计数并暴露错误。
- 输出`ActiveLogicalValues`、`ArchivedLogicalValues`和总逻辑归档率。
- Bucket分布同时输出record count和logical-value count，避免把1个stem误读成1个状态值。

### P2：重新评估Stem的shardDepth

depth20适合更大数量的独立key，但在当前stem record规模下天然产生大量单项root bucket。
先基于100万到1000万区块的outer stem增长曲线评估depth，再选择小规模参数矩阵；不要
把稀疏bucket直接修成强制下沉。

## 9. 建议测试

1. **复杂度回归测试**：构造大量无关账户stem，只wipe一个小账户；耗时和访问计数不应
   随无关stem总数线性增长。
2. **多账户销毁测试**：同块销毁多个有storage账户，确认不会重复全局扫描。
3. **snapshot fallback测试**：分别强制fast成功和fast失败，验证slow路径仍有界。
4. **大storage账户测试**：记录slot数与耗时，确保成本主要与目标账户slot数相关。
5. **逻辑统计测试**：active/archive stem混合时，逻辑值总数与实际suffix数严格一致。
6. **阶段重放**：修复后先跑到区块`1,800,000`并越过`1,796,872`，确认长尾消失，
   再启动1000万区块正式实验。

## 10. 修复验收标准

- 越过区块`1,796,872`，无handleDestruction fatal。
- 单账户wipe访问量不随无关全局stem数量线性增长。
- 不通过单纯提高`maxHandleDestructionMs`获得PASS。
- Archive wait、DB write、storage wipe和root compute继续分开统计。
- `ActiveLogicalValues > 0`且可与实际stem suffix数核对。
- Bucket稀疏按depth20分布解释；若调整depth，单独标记实验变体。
- 小阶段通过后再跑1000万区块，保留独立的新state/archive DB目录。
