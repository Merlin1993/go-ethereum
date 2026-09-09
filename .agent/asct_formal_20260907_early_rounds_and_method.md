# Formal 20.242B ASCT/MPT/Verkle Replay

Date: 2026-09-07  
Status: experiment running; this frozen early snapshot contains the first 16 ASCT metric windows. MPT and Verkle have not started because the launcher runs all engines serially.

## 1. Early Results

### Run Identity

```text
host:       192.168.2.230
launcher:   /root/asct_codex/run_full_replay_20260907_092532.sh
run:        /root/asct_codex/results/trace_stress/formal_20242112513_depth16_20260907_092532
operations: 20,242,112,513
snapshot:   160,000,000 operations, 16 metric windows
coverage:   0.790% of the requested replay
```

Each metric window is 10,000,000 operations, or 2,500 batches of 4,000 operations. At depth 16 there are 65,536 ASCT shards. Pruning advances one shard per batch, so one complete shard/archive sweep is 262,144,000 operations. Therefore the first 160M operations are only 0.610 of one shard sweep. They are useful as an early health check, not as a final multi-archive-round conclusion.

### Metric Windows

Performance uses `Comparative_Wall_ms`: operation time + commit time + batch write time. Parse time, access sampling, prune time, and cold-data promotion/maintenance are excluded. Throughput is therefore directly comparable with the declared comparison scope. The storage column below is only raw `state_db` directory size during replay; the main ASCT storage comparison will use the final non-archive logical breakdown after the run.

| Ops | Trace Block Range | Ops/s | Hot Read Hit | Raw State MB | Commit ms | Batch Write ms | Filter Lookups | Filter Positives | Runtime FP |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10M | 526753-526754 | 184,744 | 100.000000% | 276.2 | 3,714 | 7,945 | 411 | 0 | 0 |
| 20M | 826047-826051 | 168,464 | 100.000000% | 230.2 | 3,770 | 7,959 | 739 | 0 | 0 |
| 30M | 1034449-1034490 | 174,185 | 99.981979% | 223.9 | 4,220 | 8,780 | 4,299 | 25 | 0 |
| 40M | 1182059-1182148 | 171,079 | 99.961766% | 315.0 | 4,776 | 10,296 | 10,449 | 202 | 0 |
| 50M | 1292080-1292141 | 154,904 | 99.552610% | 299.6 | 4,424 | 9,045 | 8,533 | 209 | 0 |
| 60M | 1406952-1406993 | 158,278 | 99.491049% | 403.8 | 4,820 | 9,790 | 16,409 | 335 | 0 |
| 70M | 1477882-1477930 | 104,541 | 99.786112% | 386.1 | 4,163 | 8,183 | 13,991 | 530 | 0 |
| 80M | 1521599-1521613 | 86,789 | 99.987447% | 291.3 | 3,446 | 6,614 | 12,316 | 576 | 0 |
| 90M | 1573857-1573884 | 98,091 | 99.878411% | 258.2 | 3,724 | 7,436 | 22,330 | 746 | 0 |
| 100M | 1618903-1618946 | 89,549 | 99.946865% | 196.8 | 3,802 | 7,427 | 34,559 | 2,716 | 0 |
| 110M | 1685689-1685706 | 101,336 | 99.859432% | 209.5 | 4,249 | 8,330 | 32,972 | 1,102 | 0 |
| 120M | 1732913-1732933 | 97,012 | 99.329890% | 243.0 | 4,279 | 8,199 | 99,995 | 1,743 | 0 |
| 130M | 1792092-1792139 | 100,993 | 98.149466% | 307.9 | 4,339 | 8,609 | 81,321 | 1,769 | 0 |
| 140M | 1867008-1867078 | 110,313 | 99.742853% | 382.4 | 4,629 | 9,077 | 79,456 | 1,252 | 0 |
| 150M | 1933048-1933063 | 101,319 | 99.884360% | 439.3 | 4,416 | 8,646 | 84,715 | 4,059 | 0 |
| 160M | 1982999-1983017 | 103,741 | 99.929178% | 527.9 | 4,386 | 8,686 | 66,912 | 3,876 | 0 |

### Observations

1. **Early throughput has fallen, but the cause is not prune.** The first six windows ran between about 154.9k and 184.7k ops/s. Windows 7-16 ran between about 86.8k and 110.3k ops/s, without yet returning to the initial level. Commit stayed around 3.4-4.8 seconds per 10M-op window and batch write around 6.6-10.3 seconds, so the decline is mainly in the operation path, not the excluded prune path. The cumulative comparative throughput over the first 160M operations is about 116,950 ops/s.
2. **Hot hit rate is currently strong.** Every observed window is at least 98.15%, above the roughly 90% target. This is sampled logical hot/tree membership, not the decoded-stem cache hit counter. With read activation enabled, an archived stem found by a read is restored to the hot tree, which is the intended A/B configuration.
3. **Runtime filter false positives are still zero.** Across the first 160M operations, the workload produced 569,407 archive-filter lookups, 550,267 negatives, 19,140 positives, and 0 false positives. A read only reaches the cold bucket after a filter positive, then exact bucket membership must also match.
4. **Raw disk size is not the storage conclusion.** `State_Bytes` fluctuates and later grows to about 527.9 MB because it is the physical `state_db` directory while archive, compaction, WAL, and obsolete physical versions are still in play. The formal ASCT storage number will be `ActiveOnlyLogicalBytes`, which explicitly excludes archived payload. Baseline storage remains its `state_db` directory size; the report must state this logical-versus-physical caveat.
5. **This cannot yet answer the long-term hypothesis.** The current point is below one shard sweep and has no MPT/Verkle counterpart yet. The expected ASCT benefit should be judged over the full 20.242B operations, which corresponds to about 77.2 depth-16 shard sweeps.

The correct early conclusion is narrow: correctness is currently healthy, hot hit and runtime FP targets look good, but early ASCT throughput is falling. Whether it later improves after archive growth, and whether it beats MPT/Verkle under the agreed timing scope, cannot be decided from these windows.

## 2. Experiment Goal And Expected Behavior

The experiment tests four primary indicators under one ordered Ethereum state-access trace:

1. **Performance**: compare ASCT, MPT, and Verkle throughput. Prune is excluded for ASCT. Early ASCT may be weaker; after enough archive rounds the remaining active state should become smaller and ASCT should improve.
2. **Storage**: compare ASCT non-archive storage with MPT/Verkle current-state storage. ASCT archived payload is reported separately as diagnostic data and is not included in the primary ASCT storage value.
3. **Hot read hit rate**: sampled existing reads should mostly resolve in the hot/tree path. The target is approximately 90% or higher.
4. **False-positive rate**: after hot/tree misses, the cuckoo filter should reject most cold-data misses. Runtime and synthetic false-positive rates should be low.

The archive policy is operation-based, not calendar-based and not block-count-based. With the current settings, ASCT advances one shard every 4,000 operations. The trace has enough operations for many full shard sweeps, so the run is intended to expose the long-term effect of repeated archiving rather than a short warm-up effect.

## 3. Server And Directory Contract

The formal experiment is pinned to the following host. Older credentials or guides that mention another host must not be used for this run.

```text
SSH host:         192.168.2.230
credentials:      .agent/asct_remote_servers.local.json -> asct_mpt
remote source:    /root/asct_codex/go-ethereum-trace
remote work root: /root/asct_codex
ASCT results:     /root/asct_codex/results/trace_stress
MPT results:      /root/asct_codex/results/trie_compare
Verkle results:   /root/asct_codex/results/trie_compare
minimum free:     8,000,000,000 bytes
```

Do not put SSH passwords in reports, logs, or commands printed to the user. The local-only credentials file is the source of truth.

Current serial run directories:

```text
ASCT:   .../trace_stress/formal_20242112513_depth16_20260907_092532
MPT:    .../trie_compare/formal_20242112513_mpt_20260907_092532
Verkle: .../trie_compare/formal_20242112513_verkle_20260907_092532
```

## 4. Input Trace

```text
manifest:       /root/asct_codex/mainnet_state_access_trace/range_10m/manifest.json
shards:         state_access_trace_*.csv.gz, 10 files
block range:    46,147 through 10,000,000
blocks:         9,953,854
transactions:   697,373,173
total rows/ops: 20,242,112,513
```

Operation counts from the manifest:

| Trace operation | Rows |
|---|---:|
| read | 15,262,192,418 |
| write | 4,018,660,452 |
| touch | 840,688,828 |
| create | 76,502,187 |
| delete | 44,068,628 |

The loader selects shards in sorted order, starts at shard 0, and treats rows as one global ordered operation stream. The requested boundary is `traceStressOps=20,242,112,513`; it does not stop at a block boundary.

## 5. Replay Semantics

The trace header is:

```text
block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len
```

Key mapping:

- `account`: deterministic binary-tree basic-data key from the 20-byte address.
- `storage`: binary-tree storage-slot key from address plus 32-byte slot.
- `code`: binary-tree code-chunk key from address plus chunk number.

Operation mapping, shared by ASCT and the MPT/Verkle driver:

| Trace operation | Replay action |
|---|---|
| `read`, `touch` | get |
| `delete` | delete |
| `write`, `create` | put, except a storage write whose value hash is zero, which becomes delete |

If a write supplies `value_hash`, the 32-byte decoded hash is the value. If the field is empty, the driver uses `Keccak256(key || operation)` as a deterministic filler value. Every engine receives the same parsed keys, values, deletes, ordering, batch boundaries, and operation count.

## 6. Experiment Scripts

The launcher is generated locally and started remotely by:

```powershell
python .agent/run_remote_full_three_20260904.py
```

It performs, in order:

1. Verify that the current run directories and launcher do not already exist.
2. Check at least 8 GiB free disk and reject the launch if another replay process is running.
3. Upload the current `trie/archive/*.go`, `core/tree_test/trace_compare_test.go`, and disk guard.
4. Run the focused archive regression tests remotely.
5. Write and nohup-start `/root/asct_codex/run_full_replay_20260907_092532.sh`.
6. Run ASCT, then ASCT final storage stats, then synthetic filter-FP stats, then MPT, then Verkle.

The disk guard watches the active test process and stops it with exit code 2 if available space falls below 8,000,000,000 bytes.

Status commands:

```powershell
python .agent/remote_full_replay_status.py
python .agent/print_current_formal_early_rows.py
```

The first command prints launcher status, exits, disk usage, processes, run sizes, latest CSV row, and log tails. The second prints every current early-window ASCT CSV row in JSON form.

## 7. Engine Parameters

### ASCT

```text
go test ./trie/archive -run ^TestArchiveStemTraceStress$ -count=1 -timeout 0 -v
  -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m
  -traceStressBaseDir=<asct-run>
  -traceStressOps=20242112513
  -traceStressStartFile=0
  -traceStressBatchSize=4000
  -traceStressMetricsBatches=2500
  -traceStressShardDepth=16
  -traceStressPruneEveryBatches=1
  -traceStressAccessSampleEvery=1000
  -traceStressActivateArchivedStemOnRead=true
  -traceStressFinalStats=true
```

Important effective settings are:

- 4,000 operations per commit batch.
- One metric row every 2,500 batches, i.e. every 10M operations.
- One shard prune per batch; its elapsed time is recorded separately and excluded from comparative throughput.
- Read promotion enabled: a successful read that finds an archived stem activates it in the hot tree.
- Stem/node cache and commit-worker defaults from the test flags: 16 commit workers.
- Cuckoo filter shape is 16 buckets by 4 slots.
- Final structural/storage scan runs after the timed replay and is not part of replay throughput.

### MPT And Verkle

Both use `core/tree_test.TestTrieTraceCompare` with identical input and boundaries:

```text
-traceCompareOps=20242112513
-traceCompareStartFile=0
-traceCompareBatchSize=4000
-traceCompareMetricsBatches=2500
-traceCompareTimingSampleEvery=1000
```

MPT executes `Trie.Get`, `Trie.Update`, and `Trie.Delete`. Verkle executes `VerkleTrie.GetRaw`, `UpdateRaw`, and `DeleteRaw`. Each batch performs operations, `Commit`, path database update/commit, saves the root, and reloads the in-memory trie. There is no ASCT-style archive or prune stage.

## 8. Access And Timing Logic

### ASCT Read Path

The logical order is:

1. Try the decoded active-stem cache.
2. Resolve the stem in the hot tree.
3. If that misses, consult the archive bucket; inside the bucket, match the path prefix, then the cuckoo filter.
4. A filter negative immediately returns a miss.
5. A filter positive requires exact suffix membership and a value-reference check.
6. The real value is loaded from the flat value store.
7. With read activation enabled, a successful archived read promotes the loaded stem back to the hot tree.

The experiment samples every 1,000 operations to classify an access as hot, archived, or missing. `Hot_Read_Hit_Rate` is sampled hot reads divided by sampled existing reads (`hot + archived`). It is not the decoded-stem cache hit ratio.

### ASCT Write Path

A write updates the in-memory/staged tree and flat value path, removing or replacing old cold membership as required. Cold-data promotion/maintenance caused by reads or updates is measured separately and subtracted from the operation interval. The comparison metric still includes normal hot-tree work, `CommitToBatch`, and `batch.Write`.

### Timing Scope

ASCT comparative time is:

```text
operation time
+ CommitToBatch time
+ batch.Write time
```

It excludes:

- CSV parse time.
- prune time.
- sampled classification time.
- cold-data archive promotion/maintenance time.
- final structural/storage scan.
- final synthetic filter-FP scan.

MPT/Verkle comparative time is:

```text
operation time
+ trie Commit/root calculation
+ path database update/commit and root-record write
```

It excludes CSV parse time. In the current driver, the post-write in-memory trie reload is included in full batch-wall latency quantiles but not in the `Operations_Per_Sec` numerator. This is a known reporting asymmetry; final conclusions should prefer the consistently defined component fields and call out this caveat.

## 9. Storage And FP Measurement

### Storage

During replay:

- ASCT and baselines write `State_Bytes` as physical `state_db` directory size.
- ASCT's replay value is operational/diagnostic only.

After a successful ASCT run, `TestArchiveStemTraceStorageStats` reads the final committed root and writes `storage_breakdown.json`. The primary ASCT storage value is:

```text
ActiveOnlyLogicalBytes =
  root branch
+ hot tree node records
+ active stem metadata
+ active suffix values
+ active legacy stem blobs
+ archive index
```

The separately reported, non-comparison value is:

```text
ArchivedPayloadLogicalBytes =
  archive bucket records
+ archived stem metadata
+ archived suffix values
+ archived legacy stem blobs
```

This breakdown measures reachable logical database record bytes. It excludes WAL, obsolete versions, LSM table overhead, and compaction overhead. MPT/Verkle `State_Bytes` is physical directory size, so the final report must state that this is the currently defined comparison and not a perfectly like-for-like logical/physical measurement.

### Runtime Filter FP

Runtime counters measure actual archive lookups:

```text
false-positive rate =
  filter positive but exact bucket membership misses
  / all filter positives
```

In the first 16 windows this is 0/19,140.

### Synthetic Filter FP

After ASCT completes, `TestArchiveStemTraceFilterFPStats` samples 20 known non-members per archive bucket with seed 7. It writes `filter_fp_20_per_bucket.json`. This final scan provides a denser direct FP measurement than the incidental runtime workload and is not included in replay performance.

## 10. Artifacts

Each engine run has `metadata.json`, `run_status.json`, `state_db/`, and `results/`. The main replay CSV names are:

```text
ASCT:   results/asct_trace_stress.csv
MPT:    results/mpt_trace_stress.csv
Verkle: results/verkle_trace_stress.csv
```

The launcher writes `<run>.log`, `<run>.exit`, and launcher-level `.status.log`, `.overall.exit`, and `.pid` files. A successful ASCT additionally gets:

```text
results/summary.json
results/storage_breakdown.json
results/filter_fp_20_per_bucket.json
<run>.storage.log
<run>.storage.exit
<run>.filterfp.log
<run>.filterfp.exit
```

## 11. Final Analysis Rules

1. Align ASCT, MPT, and Verkle rows by `Total_Operations`, not by trace block.
2. Compare ASCT `Comparative_Wall_ms`/`Operations_Per_Sec` with MPT/Verkle measured wall/throughput.
3. Exclude ASCT prune from every performance sentence.
4. Use final `ActiveOnlyLogicalBytes` as primary ASCT storage and archive payload as diagnostic-only.
5. Report hot hit rate from sampled existing reads, separately from stem cache hits.
6. Report runtime FP and synthetic FP separately.
7. Segment curves by cumulative operation windows and by completed shard sweeps, especially around 262.144M-operation boundaries.
8. Do not extrapolate one shard sweep to a multi-round conclusion. The full run should provide about 77.2 shard sweeps.
9. Treat nonzero exit, root mismatch, storage-breakdown read failure, disk-guard stop, or trace execution error as failure even if earlier curves look good.

## 12. Current Limitations

- MPT and Verkle have not started.
- This frozen snapshot covers only 160M operations.
- The first 160M operations do not complete one shard sweep.
- Raw ASCT disk fluctuation and later growth are not evidence for or against the storage goal.
- Early throughput decline may be a transient workload/cache/archive effect; long-term comparison requires the full serial run.
- The logical-versus-physical storage caveat must remain visible in the final report.
