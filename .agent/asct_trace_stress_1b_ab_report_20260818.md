# ASCT trace-driven 1B A/B stress result

Date: 2026-08-18

## Result

The formal 1B-operation A/B run completed successfully on titanide (`192.168.2.230`). Both runs consumed the same ordered mainnet state-access trace, executed 250,000 batches of 4,000 operations, and exited with `PASS` / exit code 0.

| Metric | Cache off | Cache on | Result |
|---|---:|---:|---:|
| Operations | 1,000,000,000 | 1,000,000,000 | identical |
| Batches | 250,000 | 250,000 | identical |
| End-to-end elapsed | 13h20m54s | 1h51m27s | 7.186x faster |
| Measured throughput | 21,081.53 ops/s | 164,238.84 ops/s | 7.791x |
| Operation-path time | 45,963.53 s | 4,604.70 s | 9.982x faster |
| Median 10M-window throughput | 25,962.37 ops/s | 168,690.92 ops/s | 6.497x |
| Median batch P50 | 95.43 ms | 21.94 ms | 4.352x lower |
| Median batch P95 | 464.39 ms | 35.79 ms | 12.977x lower |
| Median batch P99 | 715.46 ms | 63.39 ms | 11.288x lower |
| Peak RSS | 8.73 GiB | 9.08 GiB | +0.35 GiB |
| Final state DB | 1,488,225,733 B | 1,485,715,308 B | 2.394 MiB difference |

The cache-on run used a 65,536-entry / 128 MiB decoded Stem cache with 64 shards. It finished with 459,818,962 hits and 540,181,038 misses, a 45.982% lifetime hit rate, and 12,008,716 evictions.

## Aligned 100M stages

| Operations | Off ops/s | On ops/s | On/off speedup | Off batch P95 | On batch P95 | On hit rate |
|---:|---:|---:|---:|---:|---:|---:|
| 100M | 132,330 | 227,143 | 1.716x | 58.15 ms | 24.85 ms | 36.98% |
| 200M | 78,331 | 203,291 | 2.595x | 127.96 ms | 27.37 ms | 39.67% |
| 300M | 52,136 | 187,115 | 3.589x | 199.52 ms | 29.98 ms | 41.46% |
| 400M | 26,953 | 165,694 | 6.148x | 439.99 ms | 36.04 ms | 45.07% |
| 500M | 31,735 | 175,142 | 5.519x | 406.16 ms | 34.24 ms | 44.05% |
| 600M | 25,441 | 168,893 | 6.639x | 484.69 ms | 34.68 ms | 44.85% |
| 700M | 17,906 | 152,725 | 8.529x | 618.43 ms | 38.67 ms | 48.04% |
| 800M | 11,773 | 136,356 | 11.582x | 878.98 ms | 45.55 ms | 52.17% |
| 900M | 12,280 | 139,795 | 11.384x | 850.11 ms | 44.11 ms | 52.45% |
| 1B | 9,550 | 132,971 | 13.923x | 1,044.95 ms | 45.87 ms | 55.07% |

The separation grows with the working set. Cache-off degraded from 132,330 ops/s in the first 100M stage to 9,550 ops/s in the final stage. Cache-on remained between 132,971 and 227,143 ops/s across all stages, despite the same growing state and the same LevelDB write path.

## Correctness checks

Both runs produced the same final root:

```text
0x5b64116749d52c3a84e3af29bd2ed474119e91871e5c411bc2d7cb38ae1d9acf
```

The final structural statistics also match:

```text
BucketCount                 183,342
LeafCount                   4,095,849
ArchivedDataSize              479,685
ActiveLogicalValues         6,696,539
ArchivedLogicalValues         820,808
ActiveLogicalValueReadFailures       0
ArchivedLogicalValueReadFailures     0
RootLeafBucketCount            39,597
StubBucketCount               143,745
MaxStubListItems                   14
```

The small final physical-state difference is LevelDB compaction/timing variance, not a semantic difference: root and all final structure counters are identical, and both runs report zero logical-value read failures.

## Experiment configuration

- Input: `state_access_trace_009046147_010000000.csv.gz`
- Covered blocks: `9,046,147..9,372,346`
- Ordered operations: `1,000,000,000`
- Batch size: `4,000`
- Batches: `250,000`
- Metrics window: `2,500` batches / `10,000,000` operations
- Shard depth: `20`
- Node storage: `path`
- Node cache: `4,194,304` entries / `512 MiB`
- Commit workers: `16`
- Async prune: enabled, once per batch
- Destructive commit: enabled
- Storage zero hash as delete: enabled
- Cache-off: Stem cache disabled
- Cache-on: `65,536` entries / `128 MiB` / 64 shards
- Value rule: 32-byte `value_hash`; deterministic key/op hash when absent

Driver entry point: [trace_stress_test.go](../trie/archive/trace_stress_test.go:389).

## Artifacts

Local archive:

```text
F:\codex_asct\plot_data\20260818_asct_trace_stress_1b_ab
```

Contents:

- `cache_off/` and `cache_on/`: metadata, run status, summary, full 100-window CSV, full `go_test.log`, exit file
- `stage_metrics_100m.csv`: aligned 100M-stage comparison
- `comparison_summary.csv`: machine-readable headline comparison
- `README.md`: copy of this report
- `SHA256SUMS`: checksums for all archived small artifacts

The remote LevelDB state directories remain under:

```text
/root/asct_codex/results/trace_stress/formal_1b_cache_off_depth20_batch4000_20260817_1655
/root/asct_codex/results/trace_stress/formal_1b_cache_on_depth20_batch4000_20260818_0832
```

They were not copied locally because each run is approximately 1.4 GiB and the root and structural statistics are already captured by the summaries.

## Interpretation

This experiment shows that, for the recorded late-mainnet access pattern, a bounded decoded-Stem cache removes most of the performance degradation caused by an expanding working set. The benefit is not limited to the early warm-up region: it becomes larger as the cache-off run loses locality and performs more persistent stem reads.

The result applies to the low-level ASCT/StemTrie access workload. It does not by itself reproduce full EVM replay semantics, rolled-back writes, block-final state, real RLP values, or production account/code construction. Those remain separate experiments.
