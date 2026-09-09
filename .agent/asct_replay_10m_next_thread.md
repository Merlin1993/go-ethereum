# ASCT 1000万区块重放方法（新线程执行版）

本文用于启动一轮新的本机 ASCT 主网交易重放。目标是独立复现并核验
`rootpathexpandfix + depth20 + async-prune + workers16` 在1000万区块上的
结构正确性、归档率、性能、误报和资源占用。

这是一份执行手册，不代表授权修改历史实验数据。新实验必须写入新的时间戳目录。

## 1. 基线与目标

成功基线：

```text
原始运行目录:
F:\codex_asct\results\mainnet\asct\run_local_asct_rootpathexpandfix_workers16_depth20_20260713_092805_10m

轻量归档:
F:\codex_asct\plot_data\20260717_ASCT原始实验

基线 Git HEAD:
aad650f39eb5069207460b2ec02e9e1f81ca8bfb

基线最终状态根:
0x24114a75262955713df813716ecf1768284f410ddeb6efa6415ab8dd78550cc5
```

新实验目标：

- 重放区块 `0..9,999,999`，总计1000万区块。
- 每10万区块写一个主指标点，共应得到100行数据。
- 500万区块后，每增加200万区块输出一次阶段分析，即500万、700万、
  900万和最终完成时各分析一次。
- 不复用任何旧运行的 `state_db`、`archive_db` 或指标文件。
- 不修改、补写或清理旧运行目录。

## 2. 新线程首先读取

```text
D:\go_workspace\go-ethereum\trie\archive\doc\ARCHIVE_CODE_PLUS.md
D:\go_workspace\go-ethereum\.agent\asct_replay_experiment_guide.md
D:\go_workspace\go-ethereum\.agent\asct_replay_10m_next_thread.md
F:\codex_asct\plot_data\20260717_ASCT原始实验\TEST_METHOD.md
F:\codex_asct\plot_data\20260717_ASCT原始实验\FINAL_SUMMARY.md
F:\codex_asct\plot_data\20260717_ASCT原始实验\metadata\run_command.ps1
F:\codex_asct\plot_data\20260717_ASCT原始实验\metadata\guard.ps1
```

读取后先同步当前源码、Git状态、已有进程、输入文件和磁盘空间。不要直接启动。

## 3. 不可破坏规则

1. 所有新输出只能放在新的时间戳目录：

   ```text
   F:\codex_asct\results\mainnet\asct\run_local_asct_rootpathexpandfix_workers16_depth20_<YYYYMMDD_HHMMSS>_10m
   ```

2. 禁止使用旧运行目录作为 `dbDir2`、`binaryArchiveDir2` 或 `metricsDir2`。
3. 禁止移动、删除、清理或覆盖任何旧实验数据库和日志。
4. 不得为了通过验收而修改CSV、日志、exit code、状态根或最终摘要。
5. 当前工作树可能包含用户改动。不得执行 `git reset --hard`、
   `git checkout --` 或其他回退用户改动的操作。
6. 启动后不要做影响实验的源码编译、磁盘压测或另一个重放。
7. 只有守护阈值、真实失败或用户明确要求时才停止实验。

## 4. 启动前核验

### 4.1 代码身份

```powershell
Set-Location D:\go_workspace\go-ethereum
git rev-parse HEAD
git status --short
git diff --check
git diff -- trie/archive core/tree_test
```

将HEAD、`git status --short`和相关源码diff保存到新运行目录。不要因为HEAD相同
就假设代码相同，基线实验运行在dirty worktree上。可用以下文件核对基线变化：

```text
F:\codex_asct\plot_data\20260717_ASCT原始实验\method\source_changes.patch
F:\codex_asct\plot_data\20260717_ASCT原始实验\method\git_status.txt
```

若当前ASCT相关源码与成功基线不同，必须在 `metadata.json` 中使用新的variant说明，
并在启动前向用户说明差异。不得仍声称是严格复现实验。

### 4.2 排除重复进程

```powershell
Get-CimInstance Win32_Process |
  Where-Object {
    $_.CommandLine -and
    ($_.CommandLine -like '*TestExpireStateProcessor*' -or
     $_.CommandLine -like '*tree_test.test*')
  } |
  Select-Object ProcessId,ParentProcessId,Name,CommandLine
```

发现已有重放时不要启动第二轮，先识别它属于哪个运行目录。

### 4.3 输入、磁盘和工具链

```powershell
Get-ChildItem E:\ethdata -Filter 'transactions_*.csv' |
  Sort-Object Name |
  Select-Object Name,Length,LastWriteTime

(Get-ChildItem E:\ethdata -Filter 'transactions_*.csv').Count
Get-PSDrive F
go version
```

本基线使用 `transactions_1.csv` 到 `transactions_21.csv`。如果数量、名称、大小或
时间戳与归档中的 `metadata/input_files.json` 不同，要记录输入变化并停止声称严格复现。

启动前F盘建议至少保留100 GiB；低于该值不得启动。

### 4.4 参数存在性和轻量测试

先从当前测试代码确认参数仍然存在。不要使用已删除的
`-archiveItemCacheLimit`。

```powershell
go test ./core/tree_test -run TestExpireStateProcessor -count=1 -timeout 0 -v -args -h
```

如果这条命令的行为会实际进入重放，应改为直接检查flag定义或先编译测试二进制后查看
帮助，不能误启动正式实验。正式长跑前至少完成相关包编译/短测试；短测试必须使用独立
临时目录，不能污染正式运行目录。

## 5. 正式参数

正式运行必须包含以下参数：

```text
-useBinaryTrie2=true
-useVerkle2=false
-useKV2=false
-dataDir2 E:\ethdata
-startFileIdx2 1
-endFileIdx2 21
-statsInterval2 100000
-blocks 10000000
-shardDepth 20
-archiveBucketSize 100
-cuckooBuckets 16
-cuckooSlots 4
-binaryPhysicalDelete=false
-binaryNodeStorage path
-binaryCommitWorkers=16
-binaryNodeCacheBytesLimitMB=512
-binaryPathDiagnostics=false
-binaryCommitWatchdogSec=30
-binaryAsyncPrune=true
-binaryPruneShardMetrics=true
-archiveOverlapBudgetMs 8000
-maxRootPipelineMs 900000
-maxHandleDestructionMs 180000
-maxPruningMs 30000
```

说明：`archiveBucketSize=100`是聚合/分裂策略参数，不等于物理单bucket硬cap。
当前 `cuckooBuckets=16`、`cuckooSlots=4` 配置下有效单bucket cap为60。

## 6. 创建运行目录和记录

用启动时刻生成唯一目录：

```powershell
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$run = "F:\codex_asct\results\mainnet\asct\run_local_asct_rootpathexpandfix_workers16_depth20_${stamp}_10m"
New-Item -ItemType Directory -Path $run | Out-Null
New-Item -ItemType Directory -Force -Path 'F:\codex_asct\tmp','F:\codex_asct\go-build-cache' | Out-Null
```

启动前创建并核对：

```text
metadata.json
run_status.json            # 初始状态 starting
run_command.ps1
runner.ps1
guard.ps1
git_head.txt
git_status.txt
source_changes.patch
input_files.json
```

`metadata.json`至少记录：运行目录、创建时间、repo、输入目录、Git HEAD、variant、
全部核心参数、机器信息和基线归档路径。

`run_command.ps1`可依据成功基线脚本生成，只允许替换成新的 `$run` 路径；正式命令为：

```powershell
$env:GOTMPDIR = 'F:\codex_asct\tmp'
$env:GOCACHE = 'F:\codex_asct\go-build-cache'
$env:GOFLAGS = ''

& go test ./core/tree_test -run TestExpireStateProcessor -count=1 -timeout 0 -v -args `
  -useBinaryTrie2=true -useVerkle2=false -useKV2=false `
  -dataDir2 E:\ethdata `
  -dbDir2 (Join-Path $run 'state_db') `
  -binaryArchiveDir2 (Join-Path $run 'archive_db') `
  -metricsDir2 $run `
  -startFileIdx2 1 -endFileIdx2 21 -statsInterval2 100000 -blocks 10000000 `
  -shardDepth 20 -archiveBucketSize 100 `
  -cuckooBuckets 16 -cuckooSlots 4 `
  -binaryPhysicalDelete=false -binaryNodeStorage path `
  -binaryCommitWorkers=16 `
  -binaryNodeCacheBytesLimitMB=512 `
  -binaryPathDiagnostics=false `
  -binaryCommitWatchdogSec=30 `
  -binaryAsyncPrune=true `
  -binaryPruneShardMetrics=true `
  -archiveOverlapBudgetMs 8000 `
  -maxRootPipelineMs 900000 `
  -maxHandleDestructionMs 180000 `
  -maxPruningMs 30000 `
  1>> (Join-Path $run 'go_test.out.log') `
  2>> (Join-Path $run 'go_test.err.log')
```

脚本必须把 `$LASTEXITCODE` 写入 `go_test.exit.txt`，并把 `run_status.json` 更新为
`completed` 或 `failed`，同时写入 `started_at`、`finished_at` 和 `exit_code`。

## 7. 守护和后台启动

以成功基线 `metadata/guard.ps1` 为模板，只替换新运行目录。守护进程每30秒记录：

- 运行状态和worker数量。
- Replay进程RSS。
- F盘剩余空间。
- 最新 `Block_End`。
- `Bucket_Items_Max`。
- `Max_Buckets_On_Single_Path`。
- `Max_Root_StubList_Buckets`。

硬停止条件：

- F盘剩余低于100 GiB。
- Replay RSS超过70 GiB。
- 100万区块后 `Bucket_Items_Max > 60`。
- 100万区块后 `Max_Root_StubList_Buckets > 1`。
- 300万区块后 `Max_Buckets_On_Single_Path > 256`。
- 进程panic、exit code非0或出现确定的数据损坏。

正常的单次慢归档不是立即停止条件。必须先区分archive compute、archive wait、
charged root和DB write，重复严重卡顿或用户要求时再停止。

后台启动使用隐藏窗口，并记录两个 `Start-Process` 返回的PID：

```powershell
$runner = Start-Process powershell.exe -WindowStyle Hidden -PassThru `
  -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File',(Join-Path $run 'runner.ps1')

$guard = Start-Process powershell.exe -WindowStyle Hidden -PassThru `
  -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File',(Join-Path $run 'guard.ps1')

$runner.Id
$guard.Id
```

启动后等待至少60秒，确认状态从 `starting` 进入 `running`、进程仍在、日志增长且
F盘路径正确。确认无误后即可让实验无人值守运行；只有用户要求查看时再检查，无需持续轮询。

## 8. 用户要求“看下情况”时

只读检查，不改变运行状态：

```powershell
$run = '<本轮运行目录>'
$runName = Split-Path $run -Leaf

Get-Content "$run\run_status.json" -Raw
Get-Content "$run\go_test.exit.txt" -ErrorAction SilentlyContinue
Get-Content "$run\guard.log" -Tail 20
Get-Content "$run\go_test.err.log" -Tail 50
Get-Content "$run\go_test.out.log" -Tail 120
Import-Csv "$run\asct_mainnet_metrics.csv" | Select-Object -Last 1

Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -and $_.CommandLine -like "*$runName*" } |
  Select-Object ProcessId,ParentProcessId,Name,
    @{n='RSS_GiB';e={[math]::Round($_.WorkingSetSize/1GB,2)}},CommandLine
```

每次汇报至少包含：

- 当前状态、进程是否存在、最新区块和完成百分比。
- F盘剩余、RSS、HeapAlloc/HeapSys/RuntimeSys。
- 当前归档items、活跃leaves、当前归档率、累计归档数。
- Bucket Avg/P50/P95/P99/Max、root/deep stub、最大路径。
- Avg/Max commit、precommit、postcommit、charged root、DB write。
- archive wait超过8秒的累计次数和本阶段新增次数。
- Cycle FP、累计FP和粗略FP/all-lookups。
- stderr、panic、guard停止原因或其他异常。

## 9. 时间统计口径

必须分开报告：

1. 交易执行时间。
2. `PreCommit`。
3. `PostCommit`。
4. root pipeline wall time。
5. charged root compute。
6. DB write。
7. archive compute和archive wait。

12秒区块间隔下，async archive wait前8秒按重叠预算处理，不计入charged root；只有
超过8秒的部分才计入charged root并记录告警。DB write可以统计，但不要混入纯root计算
结论。不能用窗口Avg/Max反推时间P95/P99；未采集就标记 `not_recorded`。

## 10. 500万后阶段分析

在 `Block_End` 首次达到以下阈值时写入 `analysis.log`：

```text
5,000,000
7,000,000
9,000,000
final
```

每次阶段分析比较“当前值”和“从开始到当前的累计值”，至少包括：

- 当前归档率：`archived / (archived + active)`。
- 累计归档事件：`Cumulative_Archived_Leaves`。
- Bucket分布和root/deep结构。
- 存储、RSS和Heap增长。
- 等长窗口性能均值、窗口最大值和对应区块。
- 超8秒事件数量、最长事件和对应区块。
- FP累计值和分母口径。

不能把某个窗口的当前归档率当成整体累计归档率，也不能把427-byte的
`archive_db`解释成全部冷数据存储。`binaryPhysicalDelete=false` 时，本轮不能直接得出
“从flat KV删除归档数据后”的实际物理节省量。

## 11. 最终验收

完成必须同时满足：

- `run_status.status=completed`。
- `go_test.exit.txt=0`。
- stdout含 `PASS`，没有panic。
- 主CSV为101行：1行表头加100行数据。
- 阶段窗口连续覆盖 `0..9,999,999`。
- 最终状态根存在；严格相同代码和输入时应与基线根一致。
- `Bucket_Items_Max <= 60`。
- `Max_Root_StubList_Buckets <= 1`。
- 所有数据库和日志仍保留在本轮独立目录中。

最终报告必须给出：

- 总交易、成功交易、最终状态根。
- 当前归档数、活跃数、当前归档率、累计归档数。
- 总bucket、Avg/P50/P95/P99/Max、root/deep/path最大值。
- 存储、Peak RSS、最终HeapAlloc/HeapSys/RuntimeSys。
- Commit/PreCommit/PostCommit/charged root/DB write/archive compute的Avg和Max。
- 超8秒归档等待次数和最长时间。
- 总FP、总lookup、粗略FP/all-lookups和证明大小/验证时间。
- 与成功基线的结构差异和性能差异；结构数据在代码和输入相同时应优先检查确定性。
- 未记录字段明确写 `not_recorded`，尤其是 `Total_Injected` 和时间P95/P99。

## 12. 新线程启动提示词

新线程直接使用下面这段：

```text
执行一轮新的ASCT本机1000万区块重放。先完整读取：

D:\go_workspace\go-ethereum\.agent\asct_replay_10m_next_thread.md
D:\go_workspace\go-ethereum\trie\archive\doc\ARCHIVE_CODE_PLUS.md
D:\go_workspace\go-ethereum\.agent\asct_replay_experiment_guide.md

成功基线及核验数据在：
F:\codex_asct\plot_data\20260717_ASCT原始实验

严格按照新线程执行版方法做preflight、创建全新的时间戳运行目录、生成metadata、
run_command、runner和guard，再启动1000万区块重放。不要复用或修改任何旧实验目录，
不要清理state_db/archive_db，不要修改实验数据。当前工作树可能有用户改动，禁止reset。

启动后先确认运行稳定并汇报运行目录和PID，然后无需一直盯着；我后续让你“看下情况”时
再只读检查。500万区块以后，每增加200万区块输出一次阶段分析，最终按方法中的验收清单
生成完整报告，并与20260713成功基线对比。
```
