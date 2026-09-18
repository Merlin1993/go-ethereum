"""Fetch + analyze the S1' op-trace run.

Pulls the run artifacts via sftp into .agent/cmp_20260918_s1p/ and prints:
1. run status + summary.json highlights (r-relevant fields),
2. op_trace per-window split: hot / probe / load / remove as share of the
   window's Operations_ms (plus the uninstrumented remainder),
3. r vs the S1 AMT CSV (window-aligned, same budget) and vs the S1 MPT ref.

Usage: python .agent/analyze_s1p_20260918.py <stamp>
"""
import csv
import json
import sys
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
REMOTE_WORK = "/root/asct_codex"
LOCAL = Path(".agent/cmp_20260918_s1p")
S1_AMT = Path(".agent/cmp_20260918/results/trace_stress/S1_amt_pathdb_nib4_393216000_20260918_085636/results/asct_trace_stress.csv")
S1_MPT_GLOB = ".agent/cmp_20260918/results/trie_compare/S1_mpt_ref_393216000_20260918_085636"


def fetch(stamp: str) -> Path:
    run_dir = f"{REMOTE_WORK}/results/trace_stress/S1p_amt_optrace_nib4_393216000_{stamp}"
    credentials = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=credentials["ssh_user"], password=credentials["ssh_password"],
                   look_for_keys=False, allow_agent=False)
    dest = LOCAL / f"S1p_amt_optrace_nib4_393216000_{stamp}"
    (dest / "results").mkdir(parents=True, exist_ok=True)
    try:
        sftp = client.open_sftp()
        try:
            for rel in ["metadata.json", "run_status.json",
                        "results/asct_trace_stress.csv", "results/op_trace.csv",
                        "results/summary.json"]:
                try:
                    sftp.get(f"{run_dir}/{rel}", str(dest / rel))
                except FileNotFoundError:
                    print(f"MISSING {rel}")
        finally:
            sftp.close()
    finally:
        client.close()
    return dest


def rows(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def num(row: dict, col: str) -> float:
    v = row.get(col)
    return float(v) if v not in (None, "") else 0.0


def main() -> None:
    stamp = sys.argv[1]
    dest = fetch(stamp)

    status = json.loads((dest / "run_status.json").read_text())
    summary = json.loads((dest / "results/summary.json").read_text())
    print(f"status={status.get('status')} ops={summary.get('operations_total')} "
          f"windows={summary.get('windows')} ops_per_sec={summary.get('operations_per_sec')}")
    for key in ["archive_round_domains", "archive_period_ops", "archive_resident_entries",
                "path_hot_written_bytes", "path_hot_buffered_bytes", "path_hot_diff_bytes",
                "active_layer_bytes_total", "op_trace_total"]:
        if key in summary:
            print(f"  {key} = {summary[key]}")

    main_rows = rows(dest / "results/asct_trace_stress.csv")
    op_rows = rows(dest / "results/op_trace.csv")
    print(f"\nmain rows={len(main_rows)} op_trace rows={len(op_rows)}")

    # Per-window op-trace split vs Operations_ms.
    print("\nwindow  ops        hot%   probe%  lock%  remove%  other%   hot_ns/op  probe_ns/op  probes  probe_us  hot_misses  miss%")
    tot = dict(ops=0.0, hot=0.0, probe=0.0, load=0.0, rem=0.0, lock=0.0, wall=0.0)
    for m, o in zip(main_rows, op_rows):
        ops = num(o, "Ops")
        wall_ms = num(m, "Operations_ms")
        hot, probe, load, rem = num(o, "Hot_ns"), num(o, "Probe_ns"), num(o, "Load_ns"), num(o, "Remove_ns")
        lock = num(o, "Lock_ns")
        probes, misses = num(o, "Probes"), num(o, "Hot_Misses")
        wall_ns = wall_ms * 1e6
        if ops <= 0 or wall_ns <= 0:
            continue
        tot["ops"] += ops; tot["hot"] += hot; tot["probe"] += probe
        tot["load"] += load; tot["rem"] += rem; tot["lock"] += lock; tot["wall"] += wall_ns
        other = wall_ns - hot - probe - load - rem - lock
        print(f"{o['Window_End_Batch']:>6}  {ops:>9.0f}  "
              f"{hot/wall_ns*100:5.1f}  {probe/wall_ns*100:5.1f}  {lock/wall_ns*100:5.1f}  "
              f"{rem/wall_ns*100:5.1f}  {other/wall_ns*100:6.1f}   "
              f"{hot/ops:9.0f}  {probe/ops:10.0f}  {probes:7.0f}  "
              f"{(probe/probes/1000 if probes else 0):8.1f}  {misses:10.0f}  {misses/ops*100:5.2f}")
    if tot["wall"] > 0:
        w = tot["wall"]
        print(f"\nTOTAL shares of Operations_ms: hot={tot['hot']/w*100:.1f}% probe={tot['probe']/w*100:.1f}% "
              f"lock={tot['lock']/w*100:.1f}% load={tot['load']/w*100:.1f}% remove={tot['rem']/w*100:.1f}% "
              f"other={(w-tot['hot']-tot['probe']-tot['load']-tot['rem']-tot['lock'])/w*100:.1f}%")

    # Window-aligned r vs S1 AMT (same budget/protocol) and vs S1 MPT ref.
    if S1_AMT.exists():
        s1 = rows(S1_AMT)
        n = min(len(s1), len(main_rows))
        ratios = [num(main_rows[i], "Operations_Per_Sec") / num(s1[i], "Operations_Per_Sec")
                  for i in range(n) if num(s1[i], "Operations_Per_Sec") > 0]
        if ratios:
            tail = ratios[-10:] if len(ratios) >= 10 else ratios
            print(f"\nr(S1'/S1 AMT same-config): mean={sum(ratios)/len(ratios):.3f} "
                  f"last10={sum(tail)/len(tail):.3f}  (instrumentation overhead check)")
    mpt_csv = next(Path(S1_MPT_GLOB).glob("results/*.csv"), None)
    if mpt_csv:
        ref = rows(mpt_csv)
        n = min(len(ref), len(main_rows))
        pairs = [(num(main_rows[i], "Operations_Per_Sec"), num(ref[i], "Operations_Per_Sec"))
                 for i in range(n)]
        pairs = [(a, m) for a, m in pairs if m > 0]
        if pairs:
            ratios = [a / m for a, m in pairs]
            tail = ratios[-10:] if len(ratios) >= 10 else ratios
            print(f"r(S1'/MPT ref): mean={sum(ratios)/len(ratios):.3f} last10={sum(tail)/len(tail):.3f} "
                  f"(compare S1's r=0.594)")


if __name__ == "__main__":
    main()
