# ASCT 主网数据获取背景与交接

## 1. 新线程目标

获取或整理可用于 ASCT `StemTrie` 主网重放压测的 Ethereum 主网交易数据，
用于替代或补充当前的“随机 32-byte sparse key”压测。

完成后应能交付一份可审计的数据清单，包含：

- 数据来源、下载方式或已有数据位置
- 覆盖区块范围和文件清单
- 文件数量、大小、SHA-256
- 数据格式校验结果
- 能否直接喂给 `TestExpireStateProcessor` 重放

## 2. 为什么要主网重放，而不是继续用随机数据

随机压测入口：

`trie/archive/trie_test.go` 的 `TestArchiveTrieStress` 使用
`rand.Read` 生成 32-byte key。前 31 bytes 是 stem，最后一个 byte 是 suffix。
在均匀随机下，几乎每个 key 都对应一个独立 stem，`StemTrie` 的“一个 stem
聚合同一账户/合约的 256 个 suffix”的优势完全失效。

表现：

- 随机 218M 压测中，Stem cache 生命周期命中率只有约 `2.375%`。
- 每次 update 基本都要重新加载 metadata、value-ref 和 suffix，并重建 commitment。
- 每批 4,000 次逻辑写，约产生 `68,662 puts + 3,958 deletes`，写放大严重。

主网重放则相反：

- 账户 header 使用 suffix 0。
- 小 storage slot `0..63` 和 code chunk `0..127` 与账户 header 共用 stem。
- 真实合约账户会反复更新同一批 storage slot / code chunk。
- 10M block 重放中，Stem cache 生命周期命中率约为 `60.93%`。

因此，判断 StemTrie 对“账户、合约账户归档”的产品价值，应优先使用主网重放
数据；随机 sparse 数据只适合作为最坏情况对照，不应作为主结论。

## 3. 关键代码和数据格式

重放入口：

`core/tree_test/processor_expire_state_test.go` 的 `TestExpireStateProcessor`

相关参数：

- `-dataDir2`：输入目录，必须包含 `transactions_*.csv`
- `-startFileIdx2`：起始文件索引
- `-endFileIdx2`：结束文件索引
- `-blocks`：最大处理 block 数，0 表示全部
- `-statsInterval2`：统计窗口
- `-fullTrieStatsInterval`：精确结构扫描间隔

`compareFindTransactionFiles` 使用以下匹配：

```text
<dataDir>\transactions_*.csv
```

`TransactionStreamer` 会先读 header 行，再按第 4 列 block number 建立索引。
当前解析逻辑 `ParseCSVRecordToMessage` 使用的字段索引如下：

| CSV 索引 | 含义 |
|---:|---|
| 0 | hash（header 行以 `hash` 开头，会被跳过） |
| 1 | nonce |
| 2 | block_hash |
| 3 | block_number |
| 4 | transaction_index |
| 5 | from_address |
| 6 | to_address（可为空或 `null`） |
| 7 | value |
| 8 | gas / gas_limit |
| 9 | gas_price |
| 10 | input |
| 后续列 | block_timestamp、max_fee_per_gas、max_priority_fee_per_gas、transaction_type 等 |

推荐 CSV header 至少包含：

```text
hash,nonce,block_hash,block_number,transaction_index,from_address,to_address,value,gas,gas_price,input,block_timestamp,max_fee_per_gas,max_priority_fee_per_gas,transaction_type
```

## 4. 已有数据与实验参考

本地 Windows 输入目录：

```text
E:\ethdata
```

历史上本地有 21 个 `transactions_*.csv`，命名从 `transactions_1.csv` 到
`transactions_21.csv`。远程实验曾使用过 11 个文件。新线程必须先重新统计：

```powershell
Get-ChildItem E:\ethdata -Filter 'transactions_*.csv' | Sort-Object Name |
  Select-Object Name, Length, LastWriteTime
```

成功重放归档参考：

```text
F:\codex_asct\plot_data\20260812_asct_mainnet_replay_10m_stemcache
```

其中可参考：

- `FINAL_SUMMARY.md`
- `TEST_METHOD.md`
- `data/asct_mainnet_metrics.csv`
- `metadata/input_files_manifest.csv`
- `scripts/run_command.txt`

该重放覆盖到 block `9,953,853`，共 `697,373,173` 笔交易，最终状态根为：

```text
0x4f33c4813f981a0e82040bc4fa4d2aff6ee3cf51e2239ab23c14ddbb771fa93e
```

用于验证新数据的基线根和交易数可以从此归档读取。

服务器和 SSH 凭据不在本文件公开，统一放在本地：

```text
D:\go_workspace\go-ethereum\.agent\asct_remote_servers.local.json
```

新线程需要访问远程数据时，先读取该文件；不要将密码写入任何公开报告、
`plot_data`、plot 图片或 git commit。

## 5. 数据获取要求

新线程应优先完成以下事情：

1. 先盘点本地 `E:\ethdata`，确认是否已有可用的 `transactions_*.csv`。
2. 检查文件是否按 block number 连续，且文件之间无重叠、无缺口。
3. 如果数据不足，从主网数据源获取新的 `transactions_*.csv`，覆盖目标区块
   范围。优先获取或重建与已有成功重放相同或更长的区间。
4. 每批数据保存为独立 `transactions_*.csv`，按 block 顺序切分和命名。
5. 生成数据清单，至少包含：

   - 来源和获取时间
   - 起始 block、结束 block
   - 每个文件名、大小、行数、SHA-256
   - 抽样验证前几行、最后几行和 header
   - 缺失 block 或异常交易说明

6. 校验示例：

```powershell
$files = Get-ChildItem E:\ethdata -Filter 'transactions_*.csv' | Sort-Object Name
foreach ($f in $files) {
  $first = Get-Content -LiteralPath $f.FullName -TotalCount 2
  $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $f.FullName).Hash
  [PSCustomObject]@{
    Name = $f.Name
    Size = $f.Length
    SHA256 = $hash
    HeaderAndFirst = ($first -join ' | ')
  }
}
```

## 6. 新线程启动提示

可以直接复制以下内容作为新线程指令：

```text
读取：
D:\go_workspace\go-ethereum\.agent\asct_mainnet_data_fetch_background_20260814.md
D:\go_workspace\go-ethereum\.agent\asct_replay_experiment_guide.md

目标：
整理或获取可用于 ASCT StemTrie 主网重放的 transactions_*.csv 数据。
先盘点本地 E:\ethdata，再确认是否需要从主网数据源补充数据。
不要覆盖已有数据；新数据写入独立目录。
完成后输出：
- 数据源
- 覆盖 block 范围
- 文件清单和 SHA-256
- 数据格式校验结果
- 是否可以直接交给 TestExpireStateProcessor 重放
```
