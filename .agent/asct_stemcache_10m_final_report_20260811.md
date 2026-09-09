# ASCT Stem Cache 10M Replay Final Report

## Experiment identity

- Status: completed, PASS, exit code 0
- Remote host: `192.168.3.116`
- Run directory: `/root/asct_codex/results/mainnet/asct/run_remote_asct_stemcache_formal10m_a2211377d_20260810_113252`
- Source commit: `a2211377da97b2e485d2494991074f4c879af56b` (`perf(trie): cache decoded active stems`)
- Baseline ASCT commit: `30b06fcec9c62cbc6c561bfaba0dd57f5fb90edb`
- Input: `/root/asct_codex/ethdata`, files 1 through 11
- Coverage: 100 epochs, `Block_End=9,953,853`
- Started: `2026-08-10T11:36:04+08:00`
- Finished: `2026-08-11T16:13:32+08:00`
- Go test duration: `103047.699s`; sum of 100 replay windows: `100801.846s` (28.00h)
- Transactions: 697,373,173 total, 697,373,169 successful
- Final root: `0x4f33c4813f981a0e82040bc4fa4d2aff6ee3cf51e2239ab23c14ddbb771fa93e`

## Experiment environment

- Hostname/address: `titanide`, `192.168.3.116`
- CPU: 13th Gen Intel Core i5-13600K, 14 cores / 20 logical CPUs
- Memory/swap: 58.63 GiB RAM, 2.00 GiB swap
- Storage: HS-SSD-C2000Pro 2048G NVMe, ext4 root filesystem
- OS/kernel: Ubuntu 22.04.5 LTS, Linux 6.8.0-124-generic x86_64
- Go: `go1.26.0 linux/amd64`
- Runtime paths: module cache `/root/asct_codex/go/pkg/mod`; build cache and
  temporary files inside the new run directory
- Limits: 1,048,576 open files and 1,048,576 processes

The state DB and archive DB shared the root NVMe/ext4 filesystem. No other
replay process was present at launch. The ASCT baseline and stem-cache run were
performed on this same host and form the controlled A/B comparison.

The archived historical datasets identify MPT host `192.168.3.51` and Verkle
host `192.168.0.144`, but do not include complete machine fingerprints. Their
aligned performance data is useful for engineering comparison, but absolute
wall-time ratios are not a strict same-machine benchmark.

The final root, transaction counts, archive structure, logical storage inventory,
filter probes, and proof distribution match the ASCT baseline. The optimization
did not change replay semantics or archive contents.

## Stem-cache effect

| Metric | Baseline ASCT | Stem-cache ASCT | Change |
| --- | ---: | ---: | ---: |
| Sum of replay windows | 30.11h | 28.00h | -7.0% |
| Mean state commit | 8.237ms | 7.633ms | -7.3% |
| Mean charged root | 6.078ms | 5.496ms | -9.6% |
| Mean pre-commit | 4.708ms | 4.175ms | -11.3% |
| Mean Stem Apply | 36.411us/stem | 29.400us/stem | -19.3% |
| Mean account update | 33.847us/account | 28.083us/account | -17.0% |
| Mean transaction execution | 1.417ms | 1.337ms | -5.6% |
| Final-window state commit | 15.64ms | 14.43ms | -7.7% |
| Final-window charged root | 12.15ms | 10.97ms | -9.7% |

The stem cache ended at 65,536 entries and about 83.8 MiB. Lifetime counters
were 521,632,883 hits, 334,514,325 misses, and 333,556,516 evictions. It was
effective, but almost every late miss evicted an entry, so the configured entry
limit was below the long-replay working set.

Peak sampled RSS was 38.2 GiB. Final HeapAlloc was 27.8 GiB and RuntimeSys was
40.9 GiB. Memory growth is the main cost of this optimization.

## Trend against MPT and Verkle

Ratios below compare aligned one-million-block phases. ASCT charged root excludes
DB write and the first 8 seconds of async archive wait. Old MPT and Verkle CSVs
only provide root-pipeline wall time, so root ratios are informative but not
perfectly symmetric.

| Phase | Charged root / MPT | Commit / MPT | Charged root / Verkle |
| --- | ---: | ---: | ---: |
| 0-1M | 1.53x | 2.36x | 0.26x |
| 1-2M | 1.34x | 2.21x | 0.25x |
| 2-3M | 0.98x | 1.44x | 0.50x |
| 4-5M | 1.00x | 1.47x | 0.56x |
| 5-6M | 1.03x | 1.44x | 0.58x |
| 6-7M | 1.15x | 1.57x | 0.56x |
| 7-8M | 1.17x | 1.59x | 0.53x |
| 8-9M | 1.19x | 1.60x | 0.57x |
| 9-10M | 1.13x | 1.52x | 0.53x |

The expected convergence is visible from 0 to 6M: charged root moves from 1.53x
MPT to approximately parity. It plateaus and regresses mildly between 6M and
9M, then improves from 1.19x to 1.13x MPT in the final phase. This supports the
claim that ASCT becomes relatively more competitive as state grows, but it does
not prove monotonic convergence or an eventual crossover with MPT.

Across all 100 windows, mean commit was 7.633ms for ASCT, 4.945ms for MPT, and
10.196ms for Verkle. Mean charged root was 5.496ms versus MPT root wall 4.962ms
and Verkle root wall 10.224ms. ASCT is clearly faster than Verkle and remains
slower than MPT.

## Storage and archive result

- Physical shared DB: 42,375,028,738 bytes (39.46 GiB)
- Active-only logical bytes: 11,328,568,637 (10.55 GiB)
- Archived-payload logical bytes: 24,473,412,702 (22.79 GiB)
- Reachable logical bytes: 35,801,981,339 (33.34 GiB)
- Current archived items: 81,018,476
- Current active logical values: 33,921,724
- Current item archive share: 70.49%
- Archived logical values: 199,680,074, or 85.48% of active plus archived logical values
- Cumulative archived leaves: 93,151,104

The logical active-only size is about 53% below the final MPT physical size of
22.51 GiB. This supports the product storage target in the logical inventory.
It is not a measured physical saving: `binaryPhysicalDelete=false`, archived
flat values remain in the shared DB, and the actual ASCT DB is larger than both
MPT and Verkle. Physical deletion/compaction needs a separate experiment.

## Structure, filter, and proofs

- Buckets: 2,076,930
- Bucket average/P50/P95/P99/max: 39.01 / 39 / 51 / 56 / 60
- Max buckets on one path: 3
- Max root stub-list buckets: 1
- Synthetic false positives: 159 / 2,075,669 negatives = 0.0076602%
- Final representative average proof: 2,588.15 bytes
- Final representative verification average/max: 0.2672ms / 3.2508ms
- Full-run max verification: 19.3233ms
- Full-run max proof generation: 0.1021ms

The filter result is good and the typical proof is small. There is one material
proof/latency outlier: block 9,187,426 recorded a 1,241,901-byte proof, a 2.744s
state commit, and a 2.427s charged root. The final-window 16,905-byte maximum
must not be reported as the full-run maximum. This outlier requires separate
diagnosis and a declared proof-size SLA.

Archive wait above the 8-second overlap budget was zero in every epoch. Archive
work did not contaminate charged-root accounting in this run.

## Product-goal assessment

1. Performance: partially met. ASCT is faster than Verkle and approaches MPT
   during the long replay, but it does not overtake MPT and convergence is not
   monotonic after 6M.
2. Storage: logically met, physically not met. Active-only logical storage is
   more than 50% below MPT, but physical deletion was disabled.
3. False positives and typical proofs: met for FPR and typical proof cost. The
   1.18 MiB full-run proof outlier remains an open product issue.

No source data, CSV rows, logs, roots, or exit status were modified. Remote
state and archive databases remain in the original run directory.
