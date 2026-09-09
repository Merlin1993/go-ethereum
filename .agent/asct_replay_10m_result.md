# ASCT Replay 10M+ Verification Result

## Latest completed timing/accounting run

Run directory:

`F:\codex_asct\results\mainnet\asct\run_local_asct_timingfix_statsfix_cumulative_workers16_depth20_20260703_132833_10m`

Status: completed, exit code 0. The run processed 100 complete metrics epochs through block `9,999,999`.

Key latest-run observations:

- At 7M (`Block_End=6,999,999`), current archived items were only `3,602`, spread across `3,146` buckets: avg `1.14`, P50 `1`, P95 `2`, P99 `3`, max `7`, `Max_Buckets_On_Single_Path=1`.
- At 10M (`Block_End=9,999,999`), current archived items were `277,923`, spread across `141,976` buckets: avg `1.96`, P50 `2`, P95 `4`, P99 `6`, max `59`, `Max_Buckets_On_Single_Path=1`.
- Cumulative archived leaves reached `6,483,413` at 7M and `9,426,762` at 10M. This is the whole-run archive event count; it is intentionally distinct from the current reachable archived item count.
- Active leaves were `760,358` at 7M and `5,359,633` at 10M, so cumulative archived-vs-active was `89.5033%` at 7M and `63.7529%` at 10M.
- Root timing accounting behaved as intended in this run: max charged root compute was `278 ms` at 7M and `438 ms` at 10M; archive wait over the 8s overlap budget stayed `0`.

Sparse-bucket diagnosis:

- The sparse buckets are not explained by "all data staying at the root". `Max_Buckets_On_Single_Path=1` only proves no stacked buckets on one path; it does not prove placement at the root.
- Code inspection showed `collapseSmallArchiveChildToStub` had been deliberately disabled, so once over-cap archive data split onto ordinary child edges, later archive-only child subtrees below the side-mount maturity threshold could remain as tiny child-edge buckets.
- The fix re-enables bounded collapse for archive-only child subtrees below the side-mount maturity threshold, while keeping mature buckets and mixed hot/archive children in their current shape. New stats split total buckets into `Stub_Bucket_Count/Items` and `Child_Bucket_Count/Items` for the next replay.

## Previous 10M+ manual-stop run

Run directory:

`F:\codex_asct\results\mainnet\asct\run_local_asct_asyncprune_pathnorm1_workers16_depth20_20260702_011859_allfiles`

Status: manually stopped after the user accepted 10M+ blocks as sufficient validation. This is not a completed `endFileIdx2=21` run.

Stop evidence:

- All run processes were stopped: wrapper PowerShell PID 1380, `go.exe` PID 49024, guard PID 47712, `tree_test.test.exe` PID 45824.
- `go_test.err.log` length was 0 at stop time.
- Last complete main metrics epoch: epoch 106, block range 10,500,000-10,599,999.
- Last live shard metrics line before stop reached block 10,693,242.
- Latest input file reached: `E:\ethdata\transactions_13.csv`.

Command configuration:

- `-useBinaryTrie2=true -useVerkle2=false -useKV2=false`
- `-dataDir2 E:\ethdata`
- `-startFileIdx2 1 -endFileIdx2 21 -statsInterval2 100000 -blocks 0`
- `-shardDepth 20 -archiveBucketSize 100`
- `-cuckooBuckets 16 -cuckooSlots 4`
- `-binaryPhysicalDelete=false -binaryNodeStorage path`
- `-binaryCommitWorkers=16`
- `-binaryNodeCacheBytesLimitMB=512`
- `-binaryPathDiagnostics=false`
- `-binaryCommitWatchdogSec=30`
- `-binaryAsyncPrune=true`
- `-binaryPruneShardMetrics=true`
- `-maxRootPipelineMs 900000`
- `-maxHandleDestructionMs 180000`
- `-maxPruningMs 30000`

Key checks:

- Guide watch blocks passed: 3,191,875; 4,240,451; 5,000,001.
- Previous failure point passed: block 5,383,143 completed normally after the path-normalization fix.
- `Max_Buckets_On_Single_Path`: max 1 across all 106 complete epochs.
- Bucket item max: 60, under `archiveBucketSize=100`.
- Bucket item P95 max: 18.
- Max root pipeline: 25,563 ms at epoch 24 / block 2,423,445, under the 900,000 ms guard.
- Max archive wait: 16,977.998 ms at epoch 50, under the effective root/prune guard envelope.
- Max pruning time: 95.045 ms at epoch 48 / block 4,769,995, under the 30,000 ms pruning guard.
- Max handle destruction: 24 ms at epoch 47 / block 4,687,016, under the 180,000 ms guard.
- Guard sample max RSS: 34 GiB; last RSS sample: 24.51 GiB; RSS stayed below 70 GiB.
- Guard sample minimum free space: 849.75 GiB; disk free stayed above 100 GiB.
- Summed Cuckoo/lookup counters across complete epochs: Hit=977,799; MissNonExistent=94,767; MissExistent=1,872; CycleFP=0.

Representative later epochs:

| Epoch | Block end | MaxBucketsPath | Bucket avg | Bucket P95 | Bucket max | Root max ms | Archive wait us | Prune max us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 102 | 10,199,999 | 1 | 1.58 | 3 | 60 | 195 | 10,008 | 1,204 |
| 103 | 10,299,999 | 1 | 1.29 | 2 | 44 | 276 | 58,052 | 1,427 |
| 104 | 10,399,999 | 1 | 1.40 | 3 | 30 | 508 | 10,008 | 1,026 |
| 105 | 10,499,999 | 1 | 1.03 | 1 | 2 | 342 | 110,100 | 1,360 |
| 106 | 10,599,999 | 1 | 1.63 | 3 | 35 | 1,715 | 40,036 | 1,576 |

Timing accounting update:

- The original `Root_Pipeline` columns are wall-clock measurements from `Finalise -> PreCommit -> PostCommit`; they include async archive wait and ASCT batch DB write.
- The experiment code now emits `Root_Compute_Charged`, `Root_DB_Write`, and `Archive_Wait_Over_Budget` separately. The default overlap budget is `-archiveOverlapBudgetMs 8000`.
- Charged root compute is calculated as `root wall - DB write - min(async archive wait, 8s)`, clamped at zero. Archive wait above 8s is reported separately as over-budget.
- Recomputing the old slow `ASCT_COMMIT_DIAG` lines with this rule: block 4,964,047 had `root_wall=18.484799s`, `archive_wait=16.977999s`, `batch_write=155.986ms`, so charged root is about `10.328813s` and archive over-budget is `8.977999s`.
- The largest old root wall at block 2,423,445 remains a true charged root long tail: `root_wall=25.563762s`, `batch_write=33.014ms`, `archive_wait=0`, charged root about `25.530748s`. This was not caused by archive wait or DB write.
- The largest old DB-write slow block in the slow log was block 7,546,147: `root_wall=5.138752s`, `batch_write=5.122689s`, charged root about `16.063ms`.

Conclusion:

The 10M+ replay validates the path-storage/async-prune fixes against the documented watch blocks, the prior path-overflow crash point, and sustained mainnet replay through 10.6M complete blocks without stderr, guard breach, bucket overflow, or `Max_Buckets_On_Single_Path` growth. Because the run was manually stopped by request, it should be cited as a 10M+ verification run rather than a full 21-file completion run.
