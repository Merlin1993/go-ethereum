# ASCT/MPT/Verkle Experiment README

This document is the canonical guide for running the state-tree experiments and
for converting the resulting CSV/JSON files into paper figures. Keep experiment
code, output schema, and plotting scripts aligned with this file.

## 1. Goals

The evaluation compares three authenticated state structures:

- `MPT`: Ethereum Merkle Patricia Trie baseline.
- `Verkle`: 256-way Verkle trie baseline.
- `ASCT`: Archive Trie with sharded pruning and archive buckets.

The experiments answer four questions:

1. Can ASCT keep state-root generation stable during long mainnet replay?
2. Can ASCT reduce hot-state footprint while keeping total physical storage
   competitive with MPT/Verkle after archived bytes are counted?
3. Are ASCT false positives, pruning cost, and resurrection proofs small enough?
4. Does ASCT remain structurally stable under zero-locality random writes/reads?

## 2. Output Directory Rules

Use one output directory per run. Do not mix MPT, Verkle, and ASCT data from
different parameter sets in one directory.

Recommended layout:

```text
results/
  mainnet/
    mpt/
    verkle/
    asct/
  stress/
    asct/
    mpt/
    verkle/
  ablation/
    expiry_window/
    bucket_size/
  figures/
```

All CSV files must use UTF-8, comma delimiters, one header row, and numeric
values without units. Use bytes for storage fields unless the field name
explicitly ends with `_MB`. Use milliseconds for root/commit/proof timing unless
the field name explicitly ends with `_us`.

## 3. Mainnet Replay Experiment

### Purpose

Replay Ethereum mainnet transaction data and compare long-term state growth and
root-generation cost across MPT, Verkle, and ASCT.

### Code Entry

Main entry:

The examples use `\` for line continuation. On Windows PowerShell, either run
the command on one line or replace `\` with PowerShell backticks.

```bash
go test ./core/tree_test -run TestExpireStateProcessor -timeout 0 -args \
  -dataDir2 <tx-data-dir> \
  -dbDir2 <state-db-dir> \
  -binaryArchiveDir2 <archive-db-dir> \
  -metricsDir2 <metrics-output-dir> \
  -startFileIdx2 1 \
  -endFileIdx2 <end-file> \
  -statsInterval2 100000 \
  -blocks 0
```

ASCT:

```bash
go test ./core/tree_test -run TestExpireStateProcessor -timeout 0 -args \
  -useBinaryTrie2=true -useVerkle2=false \
  -shardDepth 16 \
  -archiveBucketSize 100 \
  -cuckooBuckets 32 \
  -cuckooSlots 4 \
  -binaryNodeCacheLimit 262144 \
  -binaryPhysicalDelete=false \
  -dataDir2 <tx-data-dir> \
  -dbDir2 <asct-state-db> \
  -binaryArchiveDir2 <asct-archive-db> \
  -metricsDir2 <metrics-output-dir> \
  -startFileIdx2 1 -endFileIdx2 <end-file> \
  -statsInterval2 100000
```

MPT:

```bash
go test ./core/tree_test -run TestExpireStateProcessor -timeout 0 -args \
  -useBinaryTrie2=false -useVerkle2=false \
  -dataDir2 <tx-data-dir> \
  -dbDir2 <mpt-state-db> \
  -metricsDir2 <metrics-output-dir> \
  -startFileIdx2 1 -endFileIdx2 <end-file> \
  -statsInterval2 100000
```

Verkle:

```bash
go test ./core/tree_test -run TestExpireStateProcessor -timeout 0 -args \
  -useBinaryTrie2=false -useVerkle2=true \
  -dataDir2 <tx-data-dir> \
  -dbDir2 <verkle-state-db> \
  -metricsDir2 <metrics-output-dir> \
  -startFileIdx2 1 -endFileIdx2 <end-file> \
  -statsInterval2 100000
```

### Output

The main CSV is written under `-metricsDir2`:

```text
<metrics-output-dir>/asct_mainnet_metrics.csv
```

For MPT and Verkle, ASCT-only columns must be zero.

Required columns include:

- `Epoch_ID`
- `Tree_Type`
- `Cumulative_Storage_Bytes`
- `State_Storage_Bytes`
- `Archived_Storage_Bytes`
- `State_Storage_Share_Pct`
- `Archive_Storage_Share_Pct`
- `Archive_Bytes_Per_Item`
- `State_Bytes_Per_Active_Leaf`
- `Trie_Child_Node_Count`
- `Total_Archived_Items`
- `Total_Bucket_Count`
- `Max_Buckets_On_Single_Path`
- `Avg_Finalise_Time_ms`
- `Max_Finalise_Time_ms`
- `Avg_State_Commit_Time_ms`
- `Max_State_Commit_Time_ms`
- `Avg_Root_Pipeline_Time_ms`
- `Max_Root_Pipeline_Time_ms`
- `Avg_Pruning_Time_us`
- `Max_Pruning_Time_us`
- `Hit_Count`
- `Miss_NonExistent_Count`
- `Miss_Existent_Count`
- `Cycle_FP_Count`
- `Max_FP_In_Single_Block`
- `Avg_Proof_Gen_Time_ms`
- `Max_Proof_Gen_Time_ms`
- `Avg_Proof_Verify_Time_ms`
- `Max_Proof_Verify_Time_ms`
- `Item_Proof_Min`
- `Item_Proof_P25`
- `Item_Proof_Med`
- `Item_Proof_P75`
- `Item_Proof_Max`

Storage interpretation:

- Use `Cumulative_Storage_Bytes` for total physical-storage comparisons. For
  ASCT this includes both the active state DB and the archive DB, so it does not
  hide bytes by moving data into the archive.
- Use `State_Storage_Bytes` only for hot-state footprint. This intentionally
  excludes archived data and must not be presented as total compression.
- Use `Archive_Storage_Share_Pct` and `Archive_Bytes_Per_Item` to isolate how
  much of ASCT's total footprint comes from archived data and how compact the
  archive representation is. These columns separate archive compression from
  current-state structural effects.
- Use `State_Bytes_Per_Active_Leaf` as an ASCT-only hot-state density metric;
  it is not directly comparable with MPT/Verkle unless those runs also expose a
  compatible active-item count.

ASCT false-positive bucket sizes are accumulated globally in:

```text
<metrics-output-dir>/global_fp_distribution.json
```

The JSON maps bucket-size labels to frequencies. Bucket sizes from 100 upward
are grouped under `100+`.

Diagnostic guardrails:

- `-maxRootPipelineMs <ms>` aborts a replay if any block's full root pipeline
  exceeds the configured threshold.
- `-maxHandleDestructionMs <ms>` aborts if StateDB storage destruction exceeds
  the threshold.
- `-maxPruningMs <ms>` aborts if one ASCT pruning step exceeds the threshold.

Use these flags for exploratory ASCT reruns so path-locality or pruning
regressions stop early instead of producing hours of known-bad data.

### MPT/ASCT Consistency Check

Run the consistency test with pruning enabled so it covers archived reads,
write-promotion, and bucket commitment updates:

```bash
go test ./core/tree_test -run TestBinaryTrieConsistency -timeout 0 -args \
  -blocks 1000 \
  -pruneInterval 1 \
  -shardDepth 8 \
  -archiveBucketSize 100 \
  -archiveItemCacheLimit 0 \
  -cuckooBuckets 16 \
  -cuckooSlots 4
```

The test compares the per-block MPT and ASCT write count and write hash. The
binary configuration is taken from these command-line flags; it is not allowed
to silently disable pruning.

## 4. Stress Tests: ASCT, MPT, and Verkle

### Purpose

Run zero-locality synthetic workloads to compare ASCT, MPT, and Verkle under
continuous random state injection. The shared comparison focuses on root
generation smoothness, physical storage growth, and memory footprint. ASCT
additionally reports archive buckets, path stability, false positives, and proof
metrics because those mechanisms do not exist in MPT or Verkle.

Use the same epoch size for all three trees:

```text
1 stress epoch = 1,000,000 newly injected KV pairs
```

Run a 500M-item test first. Use 1B only after CSV output, disk growth, and memory
growth look stable.

### ASCT Code Entry

```bash
go test ./trie/archive -run TestArchiveTrieStress -timeout 0 -args \
  -stressItems 500000000 \
  -stressEpochItems 1000000 \
  -stressBatchSize 1000 \
  -stressGetsPerBatch 1000 \
  -stressGetMissPercent 50 \
  -stressBaseDir <asct-stress-dir> \
  -stressShardDepth 16 \
  -stressMaxPool 10000000 \
  -stressArchiveItemCacheLimit 0 \
  -stressDeleteOldValues=true \
  -stressDestructiveCommit=true
```

ASCT output:

```text
<asct-stress-dir>/results/asct_stress_test.csv
```

### MPT Code Entry

```bash
go test ./core/tree_test -run TestTrieStressMPT -timeout 0 -args \
  -mptStressItems 500000000 \
  -mptStressEpochItems 1000000 \
  -mptStressBatchSize 1000 \
  -mptStressBaseDir <mpt-stress-dir> \
  -mptStressScheme path
```

MPT output:

```text
<mpt-stress-dir>/results/mpt_stress.csv
```

### Verkle Code Entry

```bash
go test ./core/tree_test -run TestTrieStressVerkle -timeout 0 -args \
  -verkleStressItems 500000000 \
  -verkleStressEpochItems 1000000 \
  -verkleStressBatchSize 1000 \
  -verkleStressBaseDir <verkle-stress-dir>
```

Verkle output:

```text
<verkle-stress-dir>/results/verkle_stress.csv
```

### Shared Stress Plot Columns

For ASCT:

- `Total_Injected`
- `Avg_Root_ms`
- `Max_Root_ms`
- `State_MB`
- `Archive_MB`
- `RSS_MB`
- `Heap_MB`

For MPT and Verkle:

- `Total_Injected`
- `Avg_Root_ms`
- `Max_Root_ms`
- `P95_ms`
- `P99_ms`
- `Disk_MB`
- `RSS_MB`
- `Heap_MB`

Normalize the columns before drawing cross-tree figures:

- X-axis: `Injected_Items_Millions = Total_Injected / 1e6`.
- Root time: use `Avg_Root_ms` and `Max_Root_ms` for all three trees.
- ASCT storage bytes: `(State_MB + Archive_MB) * 1024 * 1024`.
- MPT/Verkle storage bytes: `Disk_MB * 1024 * 1024`.
- Memory: use `RSS_MB` as the primary physical memory metric and `Heap_MB` as
  the GC pressure metric.

### ASCT-Only Stress Columns

These columns should be plotted only for ASCT structural analysis:

- `Leaf_Count`
- `Archive_Items`
- `Bucket_Count`
- `Max_Buckets_Path`
- `Bucket_Items_Avg`
- `Bucket_Items_P50`
- `Bucket_Items_P95`
- `Bucket_Items_P99`
- `Bucket_Items_Max`

Current implementation note: `Avg_Root_ms` / `Max_Root_ms` in this stress CSV
represent the foreground root pipeline used by the stress harness
(`Prune_ms + Commit_ms + Flush_ms + Write_ms` waits), not only a pure hash
calculation. Use the component columns when plotting pipeline breakdowns.

Current baseline note: MPT and Verkle stress harnesses report write/update and
commit behavior under random injection. They do not produce ASCT-only archive,
filter, bucket, or proof columns. Leave those fields empty when building merged
plotting tables.

## 5. Ablation Experiments

### Pruning Schedule Granularity

The retention-window choice is made by preflight/pre-experiment results. The
main ablation does not search for a shorter or longer retention target. It fixes
the target to roughly one year of hot-state retention and compares pruning
schedule granularity under that target.

Use the following interpretation:

```text
sweep_blocks = 2^ShardDepth * PruneInterval
maximum hot retention ~= 2 * sweep_blocks
```

With 12-second Ethereum slots, the main one-year-retention runs use a sweep of
approximately `1,310,720` blocks. Include the `PruneInterval=1` run as the
fairness baseline because it advances pruning every block.

Run ASCT mainnet replay with these pruning schedule configurations:

| Run Label | ShardDepth | PruneInterval | Sweep Blocks | Max Hot Retention |
| --- | ---: | ---: | ---: | ---: |
| `fair-baseline` | 20 | 1 | 1,048,576 | 2,097,152 |
| `fine` | 18 | 5 | 1,310,720 | 2,621,440 |
| `main-a` | 17 | 10 | 1,310,720 | 2,621,440 |
| `main-b` | 16 | 20 | 1,310,720 | 2,621,440 |
| `coarse` | 15 | 40 | 1,310,720 | 2,621,440 |

Keep the archive bucket capacity fixed while running this ablation. The purpose
is to isolate how the same retention target behaves when pruning is fine-grained
or concentrated into fewer, heavier shard visits.

Output:

```text
ablation_pruning_schedule.csv
```

Current implementation note: the replay test writes the full per-epoch
`asct_mainnet_metrics.csv`. Build `ablation_pruning_schedule.csv` as a summary
from the final row of each schedule run unless a later helper script automates
this.

Required final columns:

- `Run_Label`
- `ShardDepth`
- `PruneInterval`
- `Sweep_Blocks`
- `Max_Hot_Retention_Blocks`
- `Cumulative_Storage_Bytes`
- `State_Storage_Bytes`
- `Archived_Storage_Bytes`
- `Avg_Root_Pipeline_Time_ms`
- `Max_Root_Pipeline_Time_ms`
- `Avg_Pruning_Time_us`
- `Max_Pruning_Time_us`
- `Total_Archived_Items`
- `Total_Bucket_Count`
- `Max_Buckets_On_Single_Path`
- `Bucket_Items_Avg`
- `Bucket_Items_P50`
- `Bucket_Items_P95`
- `Bucket_Items_P99`
- `Bucket_Items_Max`
- `Cycle_FP_Count`
- `Miss_NonExistent_Count`
- `Item_Proof_Min`
- `Item_Proof_P25`
- `Item_Proof_Med`
- `Item_Proof_P75`
- `Item_Proof_Max`

### Bucket Size

Run ASCT with bucket capacity:

- `32`
- `64`
- `100`
- `256`

Use `100` as the default-capacity run. Do not include `128` in the main matrix
because it is too close to `100`; it may be used only as a one-off supplement if
needed.

Important: Cuckoo filter size is not an independent ablation parameter. Derive
it from the bucket capacity with a fixed rule, for example:

```text
cuckooSlots = 8
cuckooBuckets = nextPow2(ceil(2 * ArchiveBucketSize / cuckooSlots))
```

This gives the following concrete settings while avoiding the hard-coded
`ResolveArchiveBucketSize()` mappings for 4-slot filters:

| ArchiveBucketSize | CuckooBuckets | CuckooSlots |
| ---: | ---: | ---: |
| 32 | 8 | 8 |
| 64 | 16 | 8 |
| 100 | 32 | 8 |
| 256 | 64 | 8 |

Keep the pruning schedule fixed while running this ablation. Prefer `main-a`
(`ShardDepth=17`, `PruneInterval=10`) as the default schedule unless a preflight
run shows that another one-year-retention schedule is more stable.

Output:

```text
ablation_bucket_size.csv
```

Current implementation note: build this summary from the final row of each
bucket-size run. Use `Total_Bucket_Count`, `Cycle_FP_Count`, and the average of
the proof-size observations (`Avg_Proof_Size_Byte`) as the source columns.

Required final columns:

- `Bucket_Capacity`
- `CuckooBuckets`
- `CuckooSlots`
- `Total_Bucket_Count`
- `Max_Buckets_On_Single_Path`
- `Bucket_Items_Avg`
- `Bucket_Items_P50`
- `Bucket_Items_P95`
- `Bucket_Items_P99`
- `Bucket_Items_Max`
- `Cycle_FP_Count`
- `Miss_NonExistent_Count`
- `Item_Avg_Proof_Size_Bytes`

## 6. Plotting Standards

Use Python and matplotlib. Plot scripts should live under `plot/` or a dedicated
experiment plotting folder.

Rules:

1. Every plot script must read CSV/JSON paths from command-line arguments.
2. Do not hard-code machine-specific paths.
3. Save figures as both `.pdf` and `.png`.
4. Use consistent colors:
   - MPT: blue
   - Verkle: orange
   - ASCT: green
5. X-axis for mainnet replay is `Epoch_ID` or block height in units of 100k
   blocks.
6. X-axis for stress tests is injected item count in millions.
7. Storage plots should use GB unless the data is tiny.
8. Time plots should use ms, except pruning plots, which may use us.
9. Box plots must be generated from the five-number proof-size columns or from
   raw sampled proof sizes if a later run stores them.
10. Every figure must include a short caption string in the plotting script
    comments describing which CSV columns it uses.

Recommended figures:

- Mainnet root time: `Avg_Root_Pipeline_Time_ms` and `Max_Root_Pipeline_Time_ms`.
- Mainnet total physical storage: `Cumulative_Storage_Bytes`, plus ASCT
  `Archive_Storage_Share_Pct`. This is the storage plot used to rule out a
  benefit caused only by excluding or compacting archived data.
- Mainnet hot-state footprint: `State_Storage_Bytes` and, for ASCT only,
  `State_Bytes_Per_Active_Leaf`. This plot must be labeled as hot-state only.
- ASCT archive efficiency: `Archive_Bytes_Per_Item` and
  `Archive_Storage_Share_Pct`.
- ASCT pruning stability: `Avg_Pruning_Time_us`, `Max_Pruning_Time_us`.
- ASCT false positives: `Cycle_FP_Count / Miss_NonExistent_Count`.
- FP bucket distribution: `global_fp_distribution.json`.
- Proof size: `Item_Proof_Min/P25/Med/P75/Max`.
- Stress root time: `Avg_Root_ms`, `Max_Root_ms`.
- Stress storage: ASCT `State_MB + Archive_MB`; MPT/Verkle `Disk_MB`.
- Stress structure: `Bucket_Count`, `Max_Buckets_Path`.
- Stress memory: `RSS_MB`, `Heap_MB`.

## 7. Preflight Runs

Before a full run, always execute:

1. Small mainnet ASCT run: `-blocks 100000`.
2. Small mainnet MPT run: `-blocks 100000`.
3. Small Verkle run: `-blocks 100000`.
4. ASCT stress smoke test: `-stressItems 10000000`.
5. MPT stress smoke test: `-mptStressItems 10000000`.
6. Verkle stress smoke test: `-verkleStressItems 10000000`.

Only start the long run after the CSV headers are correct, counters are nonzero
where expected, and memory growth is stable.

## 8. Open Follow-Ups

### ASCT Pruning Hotspot Accounting

Status: TODO.

Observed in `run_pruneopaque_depth20_dedup_20260611_152606_allfiles`:

- `Max_Pruning_Time_us` reached `28,041,844` at block `3,191,875`.
- With `ShardDepth=20`, the hotspot shard is `3,191,875 % 1,048,576 = 46,147`,
  close to the earlier problematic shard area around `46,145`.
- Additional repeated shard hotspots, such as shard `764,233`, suggest this is
  tied to shard-local archive/stub structure rather than only random I/O noise.

Before treating this as ASCT full-node pruning cost, split the pruning timing
into at least:

- metadata-only pruning and blind archive append/update work;
- archive bucket body reads and deserialization;
- synchronous `CompactArchiveStubs` work;
- bucket recomputation and archive write/delete enqueueing.

The design target is that full-node timing should include blind append and
metadata maintenance, but archive bucket data body maintenance should be
accounted separately as archive-node work. Do not change `ARCHIVE_LOGIC.md`
semantics until this breakdown confirms where the hotspot time is spent.
