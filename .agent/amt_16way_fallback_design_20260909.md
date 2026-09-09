# AMT 16-Way Fallback Design

Status: implementation started on `codex/amt-mpt-hot-layer-20260909`.

The first implementation is intentionally isolated in
`trie/archive/mpt`: it is benchmarkable without changing the default AMT
wrapper or creating an import cycle with the parent `trie` package. The
remaining integration work is to adapt the state wrapper and the production
trace driver after the standalone module's measurements are reviewed.

## Decision Gate

Use the B1 structural probe to measure the share of `Operations_ms` in the
comparative wall time before changing the hot tree. The result `r` is compared
with the fixed thresholds below:

| `r` | Decision |
| ---: | --- |
| `>= 0.90` | The tree shape is not the bottleneck. Keep the binary AMT and retire this fallback. |
| `0.70 - 0.90` | The tree has measurable cost, but keep AMT and leave this design inactive. |
| `< 0.70` | The binary hot tree is the bottleneck. Start the fallback implementation. |

Only the `Operations_ms / comparative` share can trigger this decision. If the
share is below 70%, commit or database writes are the dominant cost and changing
the fanout will not address the measured problem.

## Proposed Shape

The hot layer should use the existing go-ethereum `trie.Trie`, which is a
16-way hexary trie. The cold layer keeps the current archive implementation:

- archive whole shards on the existing round-robin schedule;
- retain bucket storage, Cuckoo membership filters, flat values, and read
  activation;
- attach one archived shard record to the hot-trie shard key rather than
  storing per-key epoch bits in a second binary tree.

The hot trie is therefore replaceable and independently benchmarkable, while
the archive layer remains the same key/value service.

## Root and Migration

The fallback cannot preserve the byte-for-byte root of the current binary AMT.
Each shard gets a hex-trie root and a small aggregation layer combines those
roots into the archive-trie root. The implementation must define that
aggregation record before any benchmark claims root compatibility.

Migration is limited to two paths:

1. materialize a shard's active leaves into a go-ethereum hex trie and persist
   its shard root;
2. restore an archived shard by loading its flat values, rebuilding that shard
   trie, and removing the archive membership.

No Verkle, MPT comparison-driver, Cuckoo calibration, or checkpoint work is
part of this fallback design.

## First implementation slice

`trie/archive/mpt` now provides a shard-routed native hexary MPT hot layer,
flat value records, persisted archive records with Cuckoo membership filters,
round-robin whole-shard pruning, aggregate shard-root MPTs, reload support,
and optional read activation. The aggregate root is intentionally distinct
from the binary AMT root; no byte-for-byte root compatibility is claimed.
