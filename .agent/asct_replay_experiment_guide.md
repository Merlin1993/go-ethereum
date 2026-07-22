# Ethereum Replay Experiment Guide

This document is the handoff guide for replay experiments that compare MPT,
Verkle, and ASCT on Ethereum transaction data. Use it when starting a fresh
Codex thread.

Do not put passwords in this file. Server credentials live in the local-only
file `.agent/asct_remote_servers.local.json`.

## 1. Directory Rules

Local Windows:

- Repo: `D:\go_workspace\go-ethereum`
- Local ethdata: `E:\ethdata`
- Local output root: `F:\codex_asct`
- Local results: `F:\codex_asct\results`
- Go temp: `F:\codex_asct\tmp`
- Go build cache: `F:\codex_asct\go-build-cache`

Never write replay state DBs under `D:\go_workspace\go-ethereum\results`.
D: has filled up before.

Remote:

- MPT / ASCT server: `192.168.3.51`, workdir `/root/asct_codex`
- Verkle server: `192.168.0.144`, workdir `/home/zkjg/asct_codex`
- Read `.agent/asct_remote_servers.local.json` for usernames/passwords.
- Only operate under the configured workdir on each server.

Run directory naming:

```text
<output-root>/results/mainnet/<tree>/run_<scope>_<tree>_<variant>_depth20_<YYYYMMDD_HHMMSS>_allfiles
```

Examples:

```text
F:\codex_asct\results\mainnet\asct\run_local_asct_asyncprune_fix2b_workers16_depth20_20260701_134517_allfiles
/root/asct_codex/results/mainnet/mpt/run_remote_mpt_20260622_112506_files11
/home/zkjg/asct_codex/results/mainnet/verkle/run_remote_verkle_20260622_115736_files11_genroot_commit10
```

Each run directory should contain:

- `run_command.ps1` or `run_command.sh`
- `guard.ps1` or `disk_guard.sh`
- `metadata.json`
- `run_status.json`
- `go_test.out.log`
- `go_test.err.log`
- `go_test.exit.txt` or `go_test.exit`
- `asct_mainnet_metrics.csv`
- ASCT only: `asct_prune_shard_metrics.csv` when `-binaryPruneShardMetrics=true`

## 2. Preflight Checklist

Before every replay:

1. Check for existing replay processes.

```powershell
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -and ($_.CommandLine -like '*TestExpireStateProcessor*' -or $_.CommandLine -like '*tree_test.test*') } |
  Select-Object ProcessId,ParentProcessId,Name,CommandLine
```

2. Count transaction files and set `-endFileIdx2` correctly.

```powershell
Get-ChildItem E:\ethdata -Filter 'transactions_*.csv' | Measure-Object
```

Current local Windows `E:\ethdata` has 21 transaction files, so use
`-endFileIdx2 21` locally. Remote datasets used earlier had 11 transaction
files, so use `-endFileIdx2 11` there unless recounting says otherwise.

3. Check free disk.

```powershell
Get-PSDrive F
```

4. After code changes, check current supported flags.

```powershell
go test ./core/tree_test -run TestExpireStateProcessor -count=1 -timeout 0 -v -args -h
```

Important current flag note: `-archiveItemCacheLimit` is no longer defined in
the current test binary. Do not use it unless the flag is reintroduced.

## 3. Local ASCT Replay

Use this for local Windows ASCT depth-20 replay:

```powershell
$run = "F:\codex_asct\results\mainnet\asct\<run-name>"
New-Item -ItemType Directory -Force -Path $run | Out-Null
New-Item -ItemType Directory -Force -Path "F:\codex_asct\tmp","F:\codex_asct\go-build-cache" | Out-Null

$env:GOTMPDIR = "F:\codex_asct\tmp"
$env:GOCACHE = "F:\codex_asct\go-build-cache"
$env:GOFLAGS = ""

go test ./core/tree_test -run TestExpireStateProcessor -count=1 -timeout 0 -v -args `
  -useBinaryTrie2=true -useVerkle2=false -useKV2=false `
  -dataDir2 E:\ethdata `
  -dbDir2 "$run\state_db" `
  -binaryArchiveDir2 "$run\archive_db" `
  -metricsDir2 "$run" `
  -startFileIdx2 1 -endFileIdx2 21 -statsInterval2 100000 -blocks 0 `
  -shardDepth 20 -archiveBucketSize 100 `
  -cuckooBuckets 16 -cuckooSlots 4 `
  -binaryStemArchive=true `
  -binaryPhysicalDelete=false -binaryNodeStorage path `
  -binaryCommitWorkers=16 `
  -binaryNodeCacheBytesLimitMB=512 `
  -binaryPathDiagnostics=false `
  -binaryCommitWatchdogSec=30 `
  -binaryAsyncPrune=true `
  -binaryPruneShardMetrics=false `
  -maxRootPipelineMs 8000 `
  -maxHandleDestructionMs 8000 `
  -maxPruningMs 30000
```

The 8-second root and destruction limits are acceptance guards, not timeout
workarounds. Keep per-shard prune CSV disabled for the first performance replay;
enable it only when archive/prune itself is the suspected bottleneck.

Recommended local guard thresholds:

- Stop if F: free space drops below `100 GiB`.
- Stop if replay RSS exceeds `70 GiB`.
- Stop if `Max_Buckets_On_Single_Path > 256` after epoch 30.

Known ASCT watch blocks:

- `3191875`
- `4240451`
- `5000001`
- `5219875` (medium account wipe)
- `5370183` (previous 28.5-second, roughly 100k-slot account wipe)

When ASCT is using async prune, always split analysis into two paths:

Archive / prune path:

- `Avg_Archive_Compute_Time_us`
- `Max_Archive_Compute_Time_us`
- `Avg_Archive_Wait_Time_us`
- `Max_Archive_Wait_Time_us`
- `[ASCT_COMMIT_DIAG] pruneTotal/pruneWait/pruneShard`
- `asct_prune_shard_metrics.csv`: `Total_us`, `Wait_us`, `Shard_us`,
  `Internal_Visits`, `Collected_Leaves`, `Collected_Stubs`, `Build_Items`,
  `Build_Buckets`

Transaction update-tree / commit path:

- `pre=statedb.PreCommit(false)`
- `post/commitInternal=PostCommit`
- `commitToBatch`
- `shardCommit`
- binary root merge (`Avg_Hash_Root_Merge_ms`)
- `batchWrite`
- `rawBatchOps`
- `rawBatchBytes`
- `dirtyShards`
- `shardMax*`
- `root_pipeline`

Interpretation:

- If `pruneWait/pruneShard` or archive wait/compute dominates, say
  archive/prune is slow.
- If `pre`, `commitToBatch`, binary root merge, `batchWrite`, or raw batch metrics
  dominate while prune fields are low, say transaction update-tree / commit
  path is slow.

Recent ASCT failure references:

- `run_local_asct_asyncprune_retry_workers16_depth20_20260630_095912_allfiles`
  failed at block `3191875`. `pruneWait/pruneShard` was about `67s`, but
  `Collected_Leaves`, `Collected_Stubs`, `Build_Items`, and `Build_Buckets`
  were all `0`.
- `run_local_asct_asyncprune_fix2b_workers16_depth20_20260701_134517_allfiles`
  failed very early with `panic: prependPath: path overflow 667 bits
  (prefix=414, base=253)` in `trie/archive.(*Shard).prependPath`, called from
  `absorbArchiveItemsIntoExistingBuckets` and `pruneAndArchive`.

## 4. Remote MPT Replay

Run MPT on `192.168.3.51` under `/root/asct_codex`.

```bash
go test ./core/tree_test -run TestExpireStateProcessor -count=1 -timeout 0 -v -args \
  -useBinaryTrie2=false -useVerkle2=false -useKV2=false \
  -dataDir2 /root/asct_codex/ethdata \
  -dbDir2 /root/asct_codex/results/mainnet/mpt/<run-name>/state_db \
  -binaryArchiveDir2 /root/asct_codex/results/mainnet/mpt/<run-name>/archive_db \
  -metricsDir2 /root/asct_codex/results/mainnet/mpt/<run-name> \
  -startFileIdx2 1 -endFileIdx2 11 -statsInterval2 100000 -blocks 0
```

Expected behavior:

- MPT keeps only current state.
- `Archived_Storage_Bytes` should remain `0`.
- Watch RSS, disk, exit code, and root pipeline max.

Known good result:

- `/root/asct_codex/results/mainnet/mpt/run_remote_mpt_20260622_112506_files11`
  exited successfully with code `0` at epoch `100`, block end `9953853`;
  `Archived_Storage_Bytes=0`, stderr empty.

## 5. Remote Verkle Replay

Run Verkle on `192.168.0.144` under `/home/zkjg/asct_codex`.

```bash
go test ./core/tree_test -run TestExpireStateProcessor -count=1 -timeout 0 -v -args \
  -useBinaryTrie2=false -useVerkle2=true -useKV2=false \
  -dataDir2 /home/zkjg/asct_codex/ethdata \
  -dbDir2 /home/zkjg/asct_codex/results/mainnet/verkle/<run-name>/state_db \
  -binaryArchiveDir2 /home/zkjg/asct_codex/results/mainnet/verkle/<run-name>/archive_db \
  -metricsDir2 /home/zkjg/asct_codex/results/mainnet/verkle/<run-name> \
  -startFileIdx2 1 -endFileIdx2 11 -statsInterval2 100000 -blocks 0 \
  -trieCommitInterval2 10
```

Verkle-specific checks:

- It must pass block `46147`.
- If it fails near block `46147` with a missing pathdb parent layer, verify the
  genesis root fix: the test must use `genesis.Root()` after `MustCommit`, not
  `types.EmptyRootHash`.
- Watch root pipeline, pruning/destruction timings, RSS, disk, and exit code.

Known remote Verkle run:

- `/home/zkjg/asct_codex/results/mainnet/verkle/run_remote_verkle_20260622_115736_files11_genroot_commit10`

## 6. How To Check A Run

Local Windows process check:

```powershell
$run = "F:\codex_asct\results\mainnet\asct\<run-name>"
$runName = Split-Path $run -Leaf
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -and ($_.CommandLine -like "*$runName*" -or $_.CommandLine -like "*$run*") } |
  Select-Object ProcessId,ParentProcessId,Name,@{n="RSS_GiB";e={[math]::Round($_.WorkingSetSize/1GB,2)}},CommandLine
```

Local CSV/log check:

```powershell
$run = "F:\codex_asct\results\mainnet\asct\<run-name>"
Import-Csv "$run\asct_mainnet_metrics.csv" | Select-Object -Last 1
Get-Content "$run\guard.log" -Tail 20
Get-Content "$run\go_test.err.log" -Tail 50
Get-Content "$run\go_test.out.log" -Tail 120
Get-Content "$run\go_test.exit.txt" -ErrorAction SilentlyContinue
```

ASCT diagnostic search:

```powershell
Select-String -Path "$run\go_test.out.log" -Pattern "\[ASCT_COMMIT_WATCHDOG\]|\[ASCT_WRAPPER_SHARD_DIAG\]|\[ASCT_COMMIT_DIAG\]|3191875|4240451|5000001|5219875|5370183" |
  Select-Object -Last 100
```

Remote Linux check:

```bash
ps -eo pid,ppid,stat,etime,rss,cmd | grep -E 'tree_test|go test|disk_guard|run_command|metricsDir2|dbDir2' | grep -v grep
df -h /
du -sh <run_dir>
tail -n 50 <run_dir>/go_test.err.log
tail -n 100 <run_dir>/go_test.out.log
tail -n 20 <run_dir>/disk_guard.log
tail -n 2 <run_dir>/asct_mainnet_metrics.csv
```

## 7. Key Metrics

Common metrics:

- `Epoch_ID`
- `Block_Start`, `Block_End`
- `Avg_Root_Pipeline_Wall_Time_ms`
- `Max_Root_Pipeline_Wall_Time_ms`
- `Max_Root_Pipeline_Block`
- `Avg_Pruning_Time_us`
- `Max_Pruning_Time_us`
- `Max_Pruning_Block`
- `Avg_Handle_Destruction_Time_ms`
- `Max_Handle_Destruction_Time_ms`
- `Avg_Account_State_Wipe_Time_ms`
- `Max_Account_State_Wipe_Time_ms`
- `Indexed_Wiped_Storage_Slots`
- `Indexed_Wiped_Stem_Records`
- `Wipe_Index_Scan_ms`, `Wipe_Stem_Delete_ms`, `Wipe_Index_Stage_ms`, `Wipe_Origin_Build_ms`
- `Avg_Hash_Total_ms`, `Avg_Hash_Shard_Wall_ms`, `Avg_Hash_Shard_Work_ms`
- `Avg_Hash_Workers`, `Avg_Hash_Dirty_Shards`, `Max_Hash_Total_ms`
- `NodeCache_Window_Hits`, `NodeCache_Window_Misses`, `NodeCache_Window_Evictions`
- `Metrics_Collection_ms`, `State_Dir_Scan_ms`, `Trie_Stats_ms`
- `State_Storage_Bytes`
- `Archived_Storage_Bytes`
- RSS from process or guard log
- Free disk
- Exit status and stderr

ASCT-only metrics:

- `Total_Bucket_Count`
- `Total_Archived_Items`
- `Bucket_Items_Avg`
- `Bucket_Items_P50`
- `Bucket_Items_P95`
- `Bucket_Items_P99`
- `Bucket_Items_Max`
- `Max_Buckets_On_Single_Path`
- `Avg_Proof_Size_Byte`
- `Max_Proof_Size_Byte`
- `Cycle_FP_Count`
- `Max_FP_In_Single_Block`
- `asct_prune_shard_metrics.csv` shard totals

## 8. Stop Rules

Stop a run when:

- stderr is non-empty and indicates a real failure.
- exit code is nonzero.
- a configured 8-second root or account-destruction acceptance guard fires.
- F: or remote disk is approaching the configured free-space limit.
- RSS exceeds the configured threshold.
- ASCT `Max_Buckets_On_Single_Path` grows past `256` after epoch 30.
- ASCT hits repeated archive/prune stalls and the run is only collecting
  redundant failure evidence.

Local stop pattern:

```powershell
$run = "F:\codex_asct\results\mainnet\asct\<run-name>"
$runName = Split-Path $run -Leaf
$self = $PID
Get-CimInstance Win32_Process |
  Where-Object { $_.ProcessId -ne $self -and $_.CommandLine -and ($_.CommandLine -like "*$runName*" -or $_.CommandLine -like "*$run*") } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
```

## 9. New Thread Prompt

Use this prompt when opening a fresh Codex thread:

```text
继续以太坊 MPT / Verkle / ASCT 重放实验。先读取：

D:\go_workspace\go-ethereum\.agent\asct_replay_experiment_guide.md
D:\go_workspace\go-ethereum\.agent\asct_experiment_runbook.md
D:\go_workspace\go-ethereum\.agent\asct_remote_servers.local.json

不要把密码写进日志或回复。所有本机实验输出放到 F:\codex_asct，不要写到 D:\go_workspace\go-ethereum\results。

当前目标是重新验证 ASCT depth20 async-prune workers16 重放。上一轮有效失败：

F:\codex_asct\results\mainnet\asct\run_local_asct_asyncprune_fix2b_workers16_depth20_20260701_134517_allfiles

失败很早，panic 为：
prependPath: path overflow 667 bits (prefix=414, base=253)

调用栈在 trie/archive.(*Shard).prependPath -> absorbArchiveItemsIntoExistingBuckets -> pruneAndArchive。

重新实验时先确认当前代码支持的 flags，不要再使用已删除的 -archiveItemCacheLimit。ASCT 长跑参数应包含：
-binaryCommitWorkers=16
-binaryStemArchive=true
-binaryNodeCacheBytesLimitMB=512
-binaryPathDiagnostics=false
-binaryCommitWatchdogSec=30
-binaryAsyncPrune=true
-binaryPruneShardMetrics=false
-maxRootPipelineMs 8000
-maxHandleDestructionMs 8000
-maxPruningMs 30000

检查时必须把 async archive/prune 和 transaction update-tree/commit-path 分开分析。重点看 block 3191875、4240451、5000001、5219875、5370183。
```
