"""D2 harvest: pull run artifacts for a D2 calibration run and print key gates.

Usage: python .agent/fetch_d2_20260918.py [run-name-or-latest]
Fetches into .agent/d2_20260918/<run>/: asct_trace_stress.csv, metadata.json,
summary.json (if present), plus the .exit/.status.log bookends. Then prints the
D-stage selection numbers: windows, ops/s, hot hit (G3), FPR raw counts + rate
(G4), State_Bytes + path_hot three-sum (G2), archive_round_domains.
"""
import json
import sys
from pathlib import Path

import paramiko

HOST = "192.168.2.230"
REMOTE_WORK = "/root/asct_codex"
RESULTS = f"{REMOTE_WORK}/results/trace_stress"
OUT = Path(".agent/d2_20260918")


def run_remote(client, command, timeout=60):
    _, stdout, _ = client.exec_command(command, timeout=timeout)
    return stdout.read().decode(errors="replace")


def main() -> None:
    credentials = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=credentials["ssh_user"], password=credentials["ssh_password"],
                   look_for_keys=False, allow_agent=False)
    try:
        arg = sys.argv[1] if len(sys.argv) > 1 else ""
        if not arg or arg == "latest":
            listing = run_remote(client, f"ls -dt {RESULTS}/D2_amt_* 2>/dev/null | head -1")
            run_dir = listing.strip().splitlines()[0] if listing.strip() else ""
            if not run_dir:
                raise SystemExit("no D2 run found remotely")
        else:
            run_dir = arg if arg.startswith("/") else f"{RESULTS}/{arg}"
        name = run_dir.rstrip("/").rsplit("/", 1)[-1]
        print(f"RUN={name}")
        print(run_remote(client, f"cat {run_dir}.exit 2>/dev/null | sed 's/^/EXIT=/' || echo EXIT=missing"))
        print(run_remote(client, f"find {run_dir} -maxdepth 2 -type f | head -30"))

        dest = OUT / name
        dest.mkdir(parents=True, exist_ok=True)
        sftp = client.open_sftp()
        fetched = []
        try:
            for cand in [f"{run_dir}/results/asct_trace_stress.csv",
                         f"{run_dir}/asct_trace_stress.csv",
                         f"{run_dir}/metadata.json",
                         f"{run_dir}/results/metadata.json",
                         f"{run_dir}/summary.json",
                         f"{run_dir}/results/summary.json"]:
                try:
                    sftp.stat(cand)
                except FileNotFoundError:
                    continue
                target = dest / cand.rsplit("/", 1)[-1]
                sftp.get(cand, str(target))
                fetched.append(target.name)
        finally:
            sftp.close()
        print("FETCHED=" + ",".join(fetched))

        csv_path = dest / "asct_trace_stress.csv"
        if csv_path.exists():
            import csv as csvmod
            rows = list(csvmod.DictReader(open(csv_path, newline="", encoding="utf-8")))
            print(f"WINDOWS={len(rows)}")
            if rows:
                def col(r, k):
                    return float(r[k]) if k in r and r[k] not in ("", None) else None
                last10 = rows[-10:]
                ops = [col(r, "Operations_Per_Sec") for r in rows]
                ops = [v for v in ops if v]
                hot = [col(r, "Hot_Read_Hit_Rate") for r in last10]
                hot = [v for v in hot if v is not None]
                fpr_fp = sum(col(r, "Archive_Filter_False_Positives") or 0 for r in rows)
                fpr_neg = sum(col(r, "Archive_Filter_Negatives") or 0 for r in rows)
                state = col(rows[-1], "State_Bytes")
                print(f"OPS_PER_SEC mean={sum(ops)/len(ops):.0f} last10={sum(ops[-10:])/min(10,len(ops)):.0f}")
                print(f"HOT_HIT_LAST10={sum(hot)/len(hot)*100:.2f}%" if hot else "HOT_HIT_LAST10=NA")
                print(f"FPR_RAW fp={fpr_fp:.0f} neg={fpr_neg:.0f} rate={(fpr_fp/fpr_neg*100 if fpr_neg else 0):.4f}%")
                print(f"STATE_BYTES={state}")
                print(f"LAST_ROW=" + json.dumps({k: rows[-1][k] for k in list(rows[-1])[:6]}))
        meta = dest / "metadata.json"
        if meta.exists():
            m = json.loads(meta.read_text())
            keys = ["hot_layer", "trie_backend", "domain_nibbles", "domain_depth_nibbles",
                    "domain_count", "epoch_bitmap", "hot_value_layout", "archive_bucket_capacity",
                    "archive_round_domains", "flush_every_batches", "archive_resident_entries",
                    "cuckoo_buckets", "cuckoo_slots", "activate_archived_key_on_read", "operations"]
            print("METADATA=" + json.dumps({k: m.get(k) for k in keys}, ensure_ascii=False))
        summ = dest / "summary.json"
        if summ.exists():
            s = json.loads(summ.read_text())
            keys = ["last_root", "state_bytes", "path_hot_written_bytes", "path_hot_buffered_bytes",
                    "path_hot_diff_bytes", "active_layer_bytes_total", "archive_round_domains",
                    "hot_read_hit_rate", "archive_filter_runtime"]
            print("SUMMARY=" + json.dumps({k: s.get(k) for k in keys if k in s}, ensure_ascii=False)[:1200])
    finally:
        client.close()


if __name__ == "__main__":
    main()
