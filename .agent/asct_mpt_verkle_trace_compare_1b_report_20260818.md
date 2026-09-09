# ASCT / MPT / Verkle trace-driven 1B comparison

Date: 2026-08-18

## Result

The three formal runs completed successfully on titanide (`192.168.2.230`). Each run consumed the same ordered mainnet state-access trace, executed 1,000,000,000 operations in 250,000 batches of 4,000 operations, produced 100 metrics windows, and exited with `PASS` / exit code 0.

| Metric | ASCT | MPT | Verkle |
|---|---:|---:|---:|
| End-to-end elapsed | 1h51m27s | 34m26s | 1h17m47s |
| Comparative throughput | 164,270.08 ops/s | 680,788.60 ops/s | 244,364.21 ops/s |
| Throughput vs ASCT | 1.000x | 4.144x | 1.488x |
| Comparative wall | 6,087.536 s | 1,468.885 s | 4,092.252 s |
| Operation-path time | 4,604.699 s | 931.601 s | 2,950.695 s |
| Commit / root time | 561.548 s | 129.291 s | 788.131 s |
| DB write time | 921.289 s | 407.993 s | 353.427 s |
| Prune launch time, diagnostic-only | 1.158 s | 0 s | 0 s |
| Median 10M-window comparative throughput | 168,723.27 ops/s | 661,947.21 ops/s | 244,612.69 ops/s |
| Median batch P50 | 21.939 ms | 5.791 ms | 16.077 ms |
| Median batch P95 | 35.790 ms | 9.140 ms | 24.328 ms |
| Median batch P99 | 63.386 ms | 11.611 ms | 29.447 ms |
| Peak RSS | 9.081 GiB | 1.455 GiB | 2.134 GiB |
| ASCT active-only logical bytes | 1.312 GiB | n/a | n/a |
| ASCT archived logical bytes, diagnostic-only | 0.125 GiB | n/a | n/a |
| LevelDB state directory, unsplit diagnostic | 1.384 GiB | 1.069 GiB | 1.184 GiB |

For this structural trace workload, MPT is the clear throughput winner. Verkle is second at 1.488x ASCT overall, and its advantage grows later in the run. ASCT has the largest operation-path and DB-write cost, and its highest RSS by a wide margin.

The ASCT number is the configured normal runtime: Stem cache enabled at 65,536 entries / 128 MiB and node cache at 512 MiB. The cache A/B is not part of this comparison.

Comparative throughput uses `Operations_ms + Commit_ms/Root_ms + DB_Write_ms`. It excludes trace parse, ASCT `Prune_Launch_ms`, final statistics, and other end-to-end startup/shutdown overhead. ASCT prune is diagnostic-only and is not included in commit/root time or the comparative wall.

The ASCT storage split is reachable logical record bytes, not LevelDB physical-file bytes. `ActiveOnlyLogicalBytes` is the non-archive comparison scope; `ArchivedPayloadLogicalBytes` is excluded from that comparison. The MPT/Verkle state sizes are LevelDB physical-directory sizes, so they are shown for context and must not be combined into one exact percentage comparison.

This split must not be read as evidence of a successful archive benefit. The final structure contains 479,685 archived KV records and 820,808 archived logical suffix values, only 10.92% of all 7,517,347 reachable logical values. Archived payload is 8.73% of reachable logical bytes. Bucket fragmentation is also poor: average fill is 2.62 items against the configured effective cap of 60 (4.36%), with P50/P95/max fill of 2/6/16; 143,745 of 183,342 buckets are root StubList buckets and no bucket is placed on an ordinary child edge.

The archive configuration was also not exercised to steady state. At shard depth 20 there are 1,048,576 shards, while the trace has 250,000 batches and the driver advances the prune cursor once per batch. The run therefore covers only 23.84% of one shard ring and never completes a full rolling-epoch cycle. This experiment is a valid cross-engine timing comparison, but it does not validate ASCT archive value; a separate full-cycle archive-on/archive-off experiment is required.

## Aligned 100M Stages

| Operations | ASCT ops/s | MPT ops/s | Verkle ops/s | MPT / ASCT | Verkle / ASCT |
|---:|---:|---:|---:|---:|---:|
| 100M | 227,212 | 930,849 | 246,543 | 4.097x | 1.085x |
| 200M | 203,341 | 792,350 | 241,600 | 3.897x | 1.188x |
| 300M | 187,156 | 707,290 | 235,978 | 3.779x | 1.261x |
| 400M | 165,725 | 678,715 | 240,522 | 4.095x | 1.451x |
| 500M | 175,176 | 681,440 | 242,612 | 3.890x | 1.385x |
| 600M | 168,925 | 654,209 | 240,789 | 3.873x | 1.425x |
| 700M | 152,751 | 644,247 | 247,641 | 4.218x | 1.621x |
| 800M | 136,377 | 603,768 | 244,617 | 4.427x | 1.794x |
| 900M | 139,816 | 615,458 | 248,116 | 4.402x | 1.775x |
| 1B | 132,991 | 611,280 | 256,364 | 4.596x | 1.928x |

MPT slows from 930,849 ops/s in the first 100M stage to 611,280 ops/s at 1B, but remains far ahead throughout. Verkle is unusually stable, staying between 235,978 and 256,364 ops/s across all stages. ASCT declines from 227,212 to 132,991 ops/s, so Verkle's relative advantage grows from 1.085x to 1.928x.

In the final 100M stage, median batch P95 latency is 45.865 ms for ASCT, 9.558 ms for MPT, and 23.024 ms for Verkle. The formal ASCT CSV predates the added comparative batch-latency columns, so its legacy batch quantiles include the very small prune-launch cost; prune is excluded from all comparative throughput calculations above.

## Workload And Correctness

All engines consumed:

```text
state_access_trace_009046147_010000000.csv.gz
blocks 9,046,147..9,372,346
1,000,000,000 ordered operations
250,000 batches x 4,000 operations
```

The trace classification and executed operation counts are identical across the three runs:

```text
reads              793,382,002
touches             33,440,025
writes             170,668,785
creates              2,506,216
trace deletes            2,972

executed gets      826,822,027
executed puts      173,103,387
executed deletes       74,586
```

The larger executed-delete count comes from the agreed mapping rule: a storage write whose value hash is the zero hash is executed as a delete.

Final roots:

```text
ASCT    0x5b64116749d52c3a84e3af29bd2ed474119e91871e5c411bc2d7cb38ae1d9acf
MPT     0xc2ff315fdc575fb2c887f75d185791a3653e74a766896433fbe4083378ad45d5
Verkle  0x08c669f205ff5e68c4583a892e4b1f44984ab46198f8c3cba22227e8be825536
```

These roots are not expected to match one another because the commitment constructions and node encodings differ. Correctness alignment here means identical ordered keys, values, operations, batches, final block, counts, and successful completion.

## Experiment Configuration

- Host: 13th Gen Intel Core i5-13600K, 20 logical CPUs, 58 GiB RAM, NVMe, Linux 6.8.
- Remote Go: `go1.26.0 linux/amd64`.
- Key rule: the same 32-byte BinaryTree/Verkle structural key is used for all engines:
  - account: `BinaryTreeBasicDataKey(address)`
  - storage: `BinaryTreeStorageSlotKey(address, slot)`
  - code: `BinaryTreeCodeChunkKey(address, chunk)`
- Value rule: 32-byte trace `value_hash` when present; otherwise a deterministic key/op hash.
- Storage zero-hash write: delete.
- Metrics window: 2,500 batches / 10,000,000 operations.
- Comparative timing: `Operations_ms + Commit_ms/Root_ms + DB_Write_ms`; ASCT `Prune_Launch_ms` is excluded.
- Parse time is excluded from measured throughput and reported separately.
- ASCT storage comparison: reachable `ActiveOnlyLogicalBytes`; archived payload bytes are excluded from comparison and reported separately as diagnostics.

ASCT:

- Shard depth 20.
- Stem cache 65,536 entries / 128 MiB.
- Node cache 4,194,304 entries / 512 MiB.
- Commit workers 16.
- Async prune every batch.
- Destructive commit enabled.

MPT and Verkle:

- LevelDB cache 512 MiB, 256 open files.
- Path scheme.
- PathDB clean cache 256 MiB and write buffer 256 MiB.
- Snapshot limit disabled.
- PathDB retained diff layers forced to 0.
- Dirty batches committed durably every batch.
- Verkle point cache 1,024 entries.

## Driver Fixes Required For The Formal Run

Three issues were corrected before the valid 1B pair:

1. PathDB initially retained one diff layer because this branch sets `common.VerkleLayerCount = 1`. The comparison driver temporarily sets it to 0, forcing dirty diff layers through `diffToDisk` every batch and matching ASCT's durable per-batch cadence.
2. Verkle crashed in `BatchSerialize` after deleting the last value in one suffix half because `LeafNode.Delete` set `c1` or `c2` to nil. The fix assigns an identity point instead, with a regression test in `TestDeleteLastValueInSuffixHalf`.
3. An MPT batch with no dirty node set or an unchanged root panicked in PathDB update. Such batches now skip diff insertion while still saving and reloading the current root.

Verification completed locally:

```text
go test ./core/tree_test -run '^TestTrieTraceCompare$' -count=1
go test ./trie -run '^$' -count=1
go test . -run '^TestDeleteLastValueInSuffixHalf$' -count=1   # inside go-verkle
python -m py_compile .agent/analyze_trie_compare.py
```

The full `go-verkle` test suite was not used as acceptance criteria because this checkout already has unrelated proof, compatibility-vector, and timing failures.

## Scope

This is a structural-layer trace workload comparison, not full EVM replay. In particular:

- MPT receives the same raw 32-byte structural keys as ASCT and Verkle; it does not use canonical secure-MPT account/storage key layout.
- Values are trace value hashes or deterministic substitutes, not full RLP account/storage encodings.
- Code chunks are represented as structural keys for all engines.
- The run does not execute EVM, transaction rollback, block-final state transitions, witness generation, or proof serving.
- Results are one formal run per engine on one machine.

Therefore the numbers should be read as an aligned tree-access benchmark. They should not be quoted as production Ethereum client throughput or as a production MPT replay result.

## Artifacts

Local archive:

```text
F:\codex_asct\plot_data\20260818_trie_trace_compare_1b
```

Contents:

- `asct_1b/`: ASCT metadata, status, summary, 100-window CSV, log, exit file.
- `asct_1b/results/storage_breakdown_20260820.json`: active-only and archived logical-byte split for the formal ASCT root.
- `mpt_1b_forcepath/`: MPT formal 1B small artifacts.
- `verkle_1b_forcepath/`: Verkle formal 1B small artifacts.
- `validation_pilots/`: fixed 100M MPT and Verkle pilots.
- `analysis/engine_summary.csv`: machine-readable headline comparison.
- `analysis/stage_metrics_100m.csv`: long-form 100M stage metrics.
- `analysis/stage_metrics_100m_wide.csv`: aligned stage throughput and ratios.
- `profile/asct_10m_20260820/`: supplementary ASCT 10M CPU profile and pprof summaries. This profile diagnoses hot paths and is not a performance score.
- `docs/asct_mpt_verkle_trace_compare_experiment_runbook_20260820.md`: reproducible experiment procedure and acceptance checks.
- `docs/asct_mpt_verkle_trace_compare_performance_analysis_20260820.md`: performance breakdown and root-cause hypotheses.
- `source/`: source and launcher snapshots used for the comparison.
- `README.md`: copy of this report.
- `SHA256SUMS`: checksums for archived small artifacts.

The remote LevelDB state directories remain under `/root/asct_codex/results/trie_compare/`; they were not copied locally because only roots, summaries, logs, and metrics are needed to reproduce the reported calculations.
