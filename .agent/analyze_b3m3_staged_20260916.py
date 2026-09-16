"""Break down B3m3 staged-bytes columns + per-op cost vs B0b."""
import csv
from pathlib import Path

OUT = Path(r"D:/go_workspace/go-ethereum/.agent/cmp_20260916")

with open(OUT / "b3m3_t3a.csv", newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

hdr = ["win", "ops/s", "stagedMB", "hotNode%", "agg%", "arch%", "flat%", "idx%", "cacheHit%", "commit_us", "ops_us"]
print(("{:>7} " * len(hdr)).format(*hdr))
total_ops = 0
wins = []
for r in rows[1:]:
    win = int(r["Total_Batches"])
    ops = int(r["Window_Operations"])
    total_ops += ops
    hot = int(r["MPT_Staged_HotNode_Bytes"])
    agg = int(r["MPT_Staged_Aggregate_Bytes"])
    arch = int(r["MPT_Staged_Archive_Bytes"])
    flat = int(r["MPT_Staged_Flat_Bytes"])
    idx = int(r["MPT_Staged_Index_Bytes"])
    gets = int(r["MPT_NodeCache_Gets"])
    hits = int(r["MPT_NodeCache_Hits"])
    staged = hot + agg + arch + flat + idx
    commit_ms = float(r["Commit_ms"]) + float(r["DB_Write_ms"])
    ops_ms = float(r["Operations_ms"])
    tot = staged or 1
    wins.append((win, ops, staged, commit_ms, ops_ms,
                 (hot / tot, agg / tot, arch / tot, flat / tot, idx / tot),
                 hits / gets if gets else 0))

for win, ops, staged, commit_ms, ops_ms, pct, hit in wins:
    print("{:>7} {:>7.0f} {:>8.1f} {:>8.1f} {:>5.1f} {:>5.1f} {:>5.1f} {:>4.1f} {:>8.1f} {:>9.2f} {:>7.2f}".format(
        win, ops / (commit_ms + ops_ms) * 1000 if (commit_ms + ops_ms) else 0,
        staged / 1e6, pct[0] * 100, pct[1] * 100, pct[2] * 100, pct[3] * 100, pct[4] * 100,
        hit * 100, commit_ms * 1e3 / ops, ops_ms * 1e3 / ops))

sum_staged = sum(w[2] for w in wins)
sum_ops = sum(w[1] for w in wins)
print("\nTOTALS: ops={:,} staged={:.2f} GB  ({:.1f} B/op)".format(sum_ops, sum_staged / 1e9, sum_staged / sum_ops))
for i, name in enumerate(["hotNode", "aggregate", "archive", "flat", "index"]):
    share = sum(w[5][i] * w[2] for w in wins) / max(sum_staged, 1)
    print("  {:>9}: {:>5.1f}%  ({:.1f} B/op)".format(name, share * 100, share * sum_staged / sum_ops))
rawbytes = sum(int(r["Raw_Batch_Bytes"]) for r in rows[1:])
print("  raw batch bytes: {:.2f} GB  ({:.1f} B/op)".format(rawbytes / 1e9, rawbytes / sum_ops))
db = rows[-1].get("State_Bytes")
if db:
    print("  final state dir: {:.2f} GB".format(int(db) / 1e9))
