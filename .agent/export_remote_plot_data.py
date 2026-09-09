import csv
import json
import os
import posixpath
import sys
from datetime import datetime, timezone

import paramiko


RUNS = [
    {
        "key": "merkle",
        "label": "Merkle/MPT baseline",
        "server": "asct_mpt",
        "run_dir": "/root/asct_codex/results/mainnet/mpt/run_remote_mpt_20260622_112506_files11",
        "control_dir": "/root/asct_codex/control_mpt_mainnet_20260622_112506",
    },
    {
        "key": "verkle",
        "label": "Verkle replay",
        "server": "verkle",
        "run_dir": "/home/zkjg/asct_codex/results/mainnet/verkle/run_remote_verkle_20260622_115736_files11_genroot_commit10",
        "control_dir": "/home/zkjg/asct_codex/control_verkle_mainnet_20260622_115736",
    },
]

REMOTE_FILES = {
    "asct_mainnet_metrics.csv": ["run"],
    "go_test.log": ["control", "run"],
    "go_test.out.log": ["control", "run"],
    "go_test.err.log": ["control", "run"],
    "go_test.exit": ["control", "run"],
    "go_test.exit.txt": ["control", "run"],
    "disk_guard.log": ["control", "run"],
}


def remote_exists(sftp, path):
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        return False


def latest_csv_row(local_path):
    if not os.path.exists(local_path):
        return None, 0
    latest = None
    rows = 0
    with open(local_path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            latest = row
            rows += 1
    return latest, rows


def write_log_tail(src_path, dst_path, line_count=200):
    if not os.path.exists(src_path):
        return
    with open(src_path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - 1024 * 1024))
        lines = f.read().decode("utf-8", "replace").splitlines()
    with open(dst_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines[-line_count:]))
        f.write("\n")


def export_run(config, run, root):
    server = config[run["server"]]
    out_dir = os.path.join(root, run["key"])
    os.makedirs(out_dir, exist_ok=True)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        server["host"],
        username=server["ssh_user"],
        password=server["ssh_password"],
        timeout=12,
        banner_timeout=12,
        auth_timeout=12,
    )
    sftp = client.open_sftp()
    downloaded = []
    missing = []
    try:
        for filename, locations in REMOTE_FILES.items():
            chosen = None
            for location in locations:
                base = run["run_dir"] if location == "run" else run["control_dir"]
                candidate = posixpath.join(base, filename)
                if remote_exists(sftp, candidate):
                    chosen = candidate
                    break
            if not chosen:
                missing.append(filename)
                continue
            local_path = os.path.join(out_dir, filename)
            sftp.get(chosen, local_path)
            downloaded.append({"remote": chosen, "local": local_path, "bytes": os.path.getsize(local_path)})
            if filename.endswith(".log"):
                write_log_tail(local_path, os.path.join(out_dir, filename + ".tail.txt"))
    finally:
        sftp.close()
        client.close()

    latest, rows = latest_csv_row(os.path.join(out_dir, "asct_mainnet_metrics.csv"))
    source = {
        "key": run["key"],
        "label": run["label"],
        "host": server["host"],
        "run_dir": run["run_dir"],
        "control_dir": run["control_dir"],
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "csv_rows": rows,
        "latest_csv_row": latest,
        "downloaded": downloaded,
        "missing": missing,
    }
    with open(os.path.join(out_dir, "source.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(source, f, ensure_ascii=False, indent=2)
    return source


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: export_remote_plot_data.py <output-root>")
    output_root = sys.argv[1]
    config_path = ".agent/asct_remote_servers.local.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    os.makedirs(output_root, exist_ok=True)
    sources = [export_run(config, run, output_root) for run in RUNS]
    manifest = {
        "title": "MPT/Merkle and Verkle replay plot data",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "output_root": output_root,
        "notes": [
            "Only lightweight experiment outputs are copied here.",
            "State DB/archive DB directories are intentionally excluded.",
            "Use asct_mainnet_metrics.csv in each subdirectory for plotting.",
        ],
        "runs": sources,
    }
    with open(os.path.join(output_root, "manifest.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
