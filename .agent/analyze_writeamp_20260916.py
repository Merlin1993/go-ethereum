"""Compare staged bytes/op across B0b (native), B3m2, B3m3."""
import csv
from pathlib import Path

OUT = Path(r"D:/go_workspace/go-ethereum/.agent/cmp_20260916")

def load(name):
    with open(OUT / name, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))[1:]

def per_op(rows, cols):
    ops = sum(int(r["Window_Operations"]) for r in rows)
    total = sum(sum(int(r[c]) for c in cols) for r in rows)
    return ops, total

for label, fname, cols in [
    ("B0b native ", "b0b_mpt_trace_stress.csv", ["Raw_Batch_Bytes"]),
    ("B3m2 (T1T2)", "b3m2_t1t2.csv", ["Raw_Batch_Bytes"]),
    ("B3m3 (T3a) ", "b3m3_t3a.csv", ["MPT_Staged_HotNode_Bytes", "MPT_Staged_Aggregate_Bytes",
                                     "MPT_Staged_Archive_Bytes", "MPT_Staged_Flat_Bytes", "MPT_Staged_Index_Bytes"]),
]:
    rows = load(fname)
    if not rows:
        print(label, "no rows"); continue
    usable = [c for c in cols if c in rows[0]]
    if usable:
        ops, total = per_op(rows, usable)
        src = "+".join(usable).replace("MPT_Staged_", "").replace("Bytes", "").replace("_", "/")
        line = f"{label}: staged {total/ops:6.1f} B/op ({src}), ops={ops:,}"
    else:
        line = f"{label}: staged n/a,"
    state = int(rows[-1].get("State_Bytes", 0) or 0)
    print(line + f" final_db={state/1e9:.2f} GB")

# cache hit detail for B3m3
rows = load("b3m3_t3a.csv")
gets = sum(int(r["MPT_NodeCache_Gets"]) for r in rows)
hits = sum(int(r["MPT_NodeCache_Hits"]) for r in rows)
print(f"\nB3m3 node cache: gets={gets:,} hits={hits:,} hit_rate={hits/max(gets,1)*100:.1f}%")
# aggregate bytes per batch
agg = sum(int(r["MPT_Staged_Aggregate_Bytes"]) for r in rows)
batches = sum(int(r["Total_Batches"]) for r in rows[1:]) if len(rows) > 1 else int(rows[0]["Total_Batches"])
print(f"B3m3 aggregate nodes: {agg/1e6:.1f} MB total = {agg/(196608000/4000):.0f} B/batch (~{agg/(196608000/4000)/500:.0f} fullNode blobs)")
