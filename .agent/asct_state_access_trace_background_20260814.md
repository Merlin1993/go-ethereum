# ASCT 主网状态读写序列数据获取背景

## 1. 新线程目标

不要拿 `transactions_*.csv` 反复执行 EVM，因为交易重放太慢。

本线程的目标是获取或生成一份可直接驱动 ASCT 压测的主网状态访问序列：

- 每个 block 内，所有状态读写的发生顺序；
- 状态对象分为 account / storage / code；
- 至少保留 `(block, tx, seq, object_type, address, slot_or_chunk, operation)`；
- 可选保留 value hash 或 value length，用于确定写入负载和做根一致性校验。

后续 ASCT 压测直接按这个序列执行 `ArchiveTrie` / `StemTrie` 的读写，不再重新跑 EVM。

## 2. 为什么改用状态访问序列

随机数据的问题：

- 随机 32-byte key 会导致几乎一个 stem 一个 suffix；
- StemTrie 的聚合能力失效；
- Stem cache 生命周期命中率只有约 `2.375%`。

交易重放的问题：

- 每轮都要执行 EVM；
- 大范围主网重放耗时太长；
- 压测真正关心的不是 EVM 执行，而是 ASCT 对“状态读写序列”的响应。

因此正确做法是：

1. 只做一次带 hook 的主网执行，导出状态读写序列；
2. 之后多轮 ASCT/MPT/Verkle 压测都重放同一个序列；
3. 这样既保留真实访问局部性，又去掉 EVM 执行成本。

## 3. 需要的 trace 数据格式

建议采用 CSV 或 Parquet。CSV 参考格式：

```text
block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len
```

字段说明：

| 字段 | 说明 |
|---|---|
| `block_number` | 区块号 |
| `tx_index` | 区块内交易序号；系统调用可标 `-1` |
| `seq` | 当前交易内状态访问序号，保持发生顺序 |
| `object_type` | `account`、`storage`、`code` |
| `address` | 20-byte 账户地址 hex |
| `slot_or_chunk` | storage slot 或 code chunk；account 类型为空 |
| `operation` | `read`、`write`、`create`、`delete`、`touch` |
| `value_hash` | 可选，写入值或旧值的 hash，用于校验 |
| `value_len` | 可选，写入值长度，用于模拟写入压力 |

如果无法提供实际 value，也必须提供 `value_len`，让压测器能生成相同长度的 dummy value。

对 ASCT 最关键的是：

- account header 与 storage slot 的访问必须保留真实顺序和局部性；
- 小 slot `0..63`、code chunk `0..127` 要与账户 header 映射到同一个 stem；
- 不能只输出“去重后的访问集合”，因为去重会破坏读后写、连续写等顺序特征。

## 4. 可能的生成方式

新线程可以先做 inventory，选择最可行的一条：

### 方案 A：基于本仓库 hook 生成

仓库中已有可用的 hook 基础：

- `core/state/hookedStateDB` 封装了 `StateDB`，可捕获 balance/nonce/code/storage change；
- `core/tracing.Hooks` 提供 `OnTxStart`、`OnTxEnd`、`OnEnter`、`OnExit`、`OnOpcode`、`OnStorageChange` 等 hook；
- `core/tree_test/processor_utils.go` 已有 `CompareCountingStateDB` 和 `CompareStateAccessCounter`，但目前主要做 per-block 去重统计，不是顺序 trace。

需要补一个 recorder，在 StateDB 的读写路径上按发生顺序写出一行 trace。重点路径包括：

- account：balance、nonce、code hash、code、account create/delete；
- storage：`GetState`、`SetState`、`GetCommittedState`；
- code：`SetCode` 和 code chunk 写入。

推荐在 `core/tree_test` 增加一个测试/工具入口，例如：

```text
TestExportStateAccessTrace
```

从主网数据执行一次，输出 `state_access_trace_*.csv`。

### 方案 B：复用现成客户端的 access trace

如果已有 geth/erigon/reth 导出的状态访问 trace，可以清洗成上面的 schema。新线程需要确认：

- 数据源；
- 是否保留 block/tx/opcode 顺序；
- 是否区分 account/storage/code；
- 是否包含读操作，还是只有写操作；
- 文件覆盖范围和 SHA-256。

只包含 writes 的数据可以用于写放大测试，但不能完整评估 Stem cache 的读命中率。所以优先找包含 reads 的完整 trace。

### 方案 C：从 EVM tracing 生成

使用 `core/tracing.Hooks` 的 `OnOpcode` 无法直接知道每个 opcode 访问的具体 slot，但可以配合 `StateDB` 的访问 hook。若现成 trace 不可得，就用本仓库方案 A，避免重新发明完整 EVM tracer。

## 5. 数据文件组织和验收

建议输出目录：

```text
F:\codex_asct\mainnet_state_access_trace\<date>\
```

按 block 范围切分，例如：

```text
state_access_trace_00000000_00099999.csv
state_access_trace_00100000_00199999.csv
...
```

每个文件附 manifest，至少包含：

- 来源和生成命令
- 起始/结束 block
- 文件大小、行数、SHA-256
- 是否包含 reads
- 是否包含实际 value
- 是否存在缺块或失败交易

验收标准：

1. block 号连续，无重复、无缺口。
2. 同一 block/tx 内 `seq` 严格按发生顺序递增。
3. account 和 storage 访问分开记录。
4. 小 slot 和 code chunk 与账户 header 的 stem 关系可被重建。
5. 至少抽样检查一个 ERC-20 转账 block、一个合约创建 block、一个 storage 密集 block。
6. 如果目标是性能对比，不要使用去重后的访问集合。

## 6. 已有资源

随机压测归档：

```text
F:\codex_asct\plot_data\20260813_asct_stem_sparse_218m_stopped
```

主网重放参考：

```text
F:\codex_asct\plot_data\20260812_asct_mainnet_replay_10m_stemcache
```

其中重放数据可用于对比 trace 是否覆盖了真实访问模式，但不要再作为每轮压测输入重复跑 EVM。

关键代码位置：

- `core/tree_test/processor_expire_state_test.go`
- `core/tree_test/processor_utils.go`
- `core/state/hookedStateDB`：`core/state/statedb_hooked.go`
- `core/tracing/hooks.go`
- `trie/utils/binary_tree.go`
- `trie/archive/stem.go`

## 7. 新线程启动提示

```text
读取：
D:\go_workspace\go-ethereum\.agent\asct_state_access_trace_background_20260814.md

目标：
获取或生成主网状态读写顺序 trace，而不是 transactions 文件。
优先盘点是否已有可用的 access trace；
如果没有，则基于 core/tree_test 和 core/tracing hooks 增加一次导出工具，
执行一次主网范围，输出 state_access_trace_*.csv 和 manifest。
不要覆盖已有数据；新数据写入独立目录。

完成后输出：
- trace 来源和生成方式
- 覆盖 block 范围
- schema 和示例记录
- 文件清单、行数、SHA-256
- 是否包含 reads、实际 value、缺块说明
- 后续如何直接用于 ASCT StemTrie 压测
```

## 8. 推荐规模与服务器

### 造多少

不要一上来就生成 10M block 的全量 trace。先按 block 分批，做一次小规模 pilot 估算行数和压缩后体积。

推荐顺序：

1. `100,000 blocks` pilot
   - 用于确定平均每交易/每 block 的状态访问行数；
   - 产出 `state_access_trace_00000000_00099999` 或类似分片；
   - 生成后立即统计 rows、未压缩大小、Parquet/CSV.gz 压缩后大小。

2. 若 pilot 可控，扩到 `1,000,000 blocks`
   - 形成趋势曲线和 storage/cache 命中特征；
   - 与现有 ASCT replay 的 1M 阶段对齐。

3. 正式实验优先 `5,000,000 blocks`
   - 兼顾规模、时间和磁盘；
   - 如果磁盘与生成时间允许，再考虑 `10,000,000 blocks` 与已有主网重放完全对齐。

原则：

- 不先承诺 10M，先按 pilot 数据反推。
- trace 尽量使用 Parquet/Arrow 或 CSV.gz，不要长期保存未压缩 CSV。
- 如果 1M block 的压缩后体积已经接近数百 GiB，正式规模就控制在 5M 以内。

### 在哪台服务器

默认推荐：

`192.168.3.116`，hostname `titanide`

理由：

- 已经跑过 10M ASCT 主网重放；
- 已有 `/root/asct_codex/ethdata` 输入和 go-ethereum 源码；
- 磁盘为 2TB NVMe，重放结束后仍有约 `642 GiB` 可用；
- 内存约 `58.63 GiB`，CPU 20 logical cores；
- 适合先做 100k/1m block trace 生成。

建议目录：

```text
/root/asct_codex/mainnet_state_access_trace/
```

启动前先执行：

```bash
df -h /root/asct_codex
```

如果可用空间不足，再考虑 `192.168.3.51`，但必须先 `df -h` 和确认其 workdir 与 ethdata 状态。不要把 trace 写入 192.168.0.144 的只读 ethdata 目录。

最终 trace 目录应单独归档，不覆盖现有 10M replay 数据。

## 9. 最终规模口径：不要少于重放规模

之前第 8 节建议“正式先 5M”，这个口径作废。现有成功 ASCT 重放已经覆盖
`10,000,000 blocks`、约 `697M` 笔交易，因此新的状态访问 trace 至少要与它
对齐，目标应当更高。

最终推荐：

- pilot 仍只用于估算行数和压缩体积：`100,000 blocks` 或 `1,000,000 blocks`。
- 正式数据规模：至少 `10,000,000 blocks`。
- 如果主网数据源和磁盘允许，优先做 `20,000,000 blocks`；
- 有条件的话，向 `50,000,000 blocks` 扩展，用于观察 ASCT 在更长历史下的
  归档比例、Stem cache 命中和存储增长。

服务器仍默认 `192.168.3.116`：

- 已有 10M 重放源码和输入；
- 2TB NVMe，当前约 642 GiB 可用。

但 20M/50M 的未压缩 trace 可能超过这块盘。新线程在生成 pilot 后必须
立即按以下公式估算：

```text
正式大小 ≈ pilot 压缩后大小 × (目标 blocks / pilot blocks)
```

并在启动正式生成前检查 `df -h`。如果预计超过 500 GiB，就换到
`192.168.3.51`，或先把 trace 分流到独立大容量盘；不要强行写入
`192.168.0.144` 的只读 ethdata 目录。

## 10. 小样本放大生成方案

可以用一小段真实状态访问 trace 提取“配比”，再生成大量同分布数据。

### 推荐做法

1. 先获取一段小规模真实 trace，建议 `100,000` 或 `1,000,000 blocks`。
2. 从这段 trace 提取 profile，而不是直接保存全量主网 trace。
3. 用确定性生成器放大到 `10M / 20M / 50M blocks`。
4. 生成器输出仍然是 `state_access_trace_*.csv`，但数据来源标记为
   `synthetic_from_profile`，不能冒充真实主网 trace。

### profile 至少包含

- 每 block：交易数、账户读写数、storage 读写数、code 读写数；
- 新账户 / 销毁账户比例；
- 热门账户的 Zipf 或 empirical rank；
- 每个账户的 storage slot 数量分布；
- 每个 stem 的 suffix 数量分布；
- 读写顺序：读后写、连续写、先读后改；
- value length 分布；
- 合约账户与 EOA 比例；
- 代码 chunk 数量分布。

### 生成方式

- 账户地址和 slot 通过 `trie/utils/binary_tree.go` 的
  `BinaryTreeBasicDataKey`、`BinaryTreeStorageSlotKey`、`BinaryTreeCodeChunkKey`
  生成，保证 stem 局部性。
- 访问频率按 profile 抽样，使用固定 seed。
- value 可以用同长度的 dummy value，除非要做 root 一致性校验。
- 每 block 内保留真实顺序特征：account read/write、storage read/write、
  create/delete 交错。

### 验证生成质量

生成器完成后，跑一个短 ASCT 对比：

- 随机 sparse 基线；
- 原始小样本 trace；
- 放大后的 synthetic trace。

重点看这些是否接近：

- Stem cache hit rate；
- unique stems；
- suffixes per stem；
- account/storage update ratio；
- tree puts / flat puts；
- archive items / active leaves；
- commit 和 prune 的增长曲线。

如果 synthetic trace 的 Stem cache 命中率、stem 局部性和写放大与原始小样本
接近，就可以用它做大规模趋势测试。

### 哪些结论不能用 synthetic trace

- 不能声称“真实主网根一致”；
- 不能拿它做 proof / 状态根正确性验证；
- 只能用于吞吐、存储、缓存、归档和扩展性趋势对比。

真实 trace 保留一份小规模基准，用于校验生成器和做最终根/结构验证。
