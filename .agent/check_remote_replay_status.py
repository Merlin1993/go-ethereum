import csv
import json
import os
import subprocess
import sys
import traceback

import paramiko


RUNS = [
    {
        "name": "MPT",
        "server": "asct_mpt",
        "run_dir": "/root/asct_codex/results/mainnet/mpt/run_remote_mpt_20260622_112506_files11",
        "control_dir": "/root/asct_codex/control_mpt_mainnet_20260622_112506",
    },
    {
        "name": "Verkle",
        "server": "verkle",
        "run_dir": "/home/zkjg/asct_codex/results/mainnet/verkle/run_remote_verkle_20260622_115736_files11_genroot_commit10",
        "control_dir": "/home/zkjg/asct_codex/control_verkle_mainnet_20260622_115736",
    },
]


REMOTE_TEMPLATE = r'''
import csv, json, os, subprocess

RUN_DIR = __RUN_DIR__
CONTROL_DIR = __CONTROL_DIR__


def tail(path, n=12):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 65536))
            data = f.read().decode("utf-8", "replace").splitlines()
            return data[-n:]
    except FileNotFoundError:
        return []
    except Exception as exc:
        return ["<tail_error: %s>" % exc]


def file_size(path):
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        return None


def read_text(path, limit=4096):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read(limit).strip()
    except FileNotFoundError:
        return None
    except Exception as exc:
        return "<read_error: %s>" % exc


def latest_csv(path):
    result = {"exists": os.path.exists(path), "rows": 0, "latest": None, "header": []}
    if not result["exists"]:
        return result
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            result["header"] = reader.fieldnames or []
            for row in reader:
                if row:
                    result["rows"] += 1
                    result["latest"] = row
    except Exception as exc:
        result["error"] = str(exc)
    return result


def proc_list():
    procs = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            raw = open("/proc/%s/cmdline" % name, "rb").read()
            cmd = raw.replace(b"\x00", b" ").decode("utf-8", "replace").strip()
            if not cmd:
                continue
            if RUN_DIR not in cmd and CONTROL_DIR not in cmd:
                continue
            if "python3 - <<'PY'" in cmd and "def tail(path" in cmd:
                continue
            rss_kb = None
            state = None
            with open("/proc/%s/status" % name, "r", encoding="utf-8", errors="replace") as sf:
                for line in sf:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            rss_kb = int(parts[1])
                    elif line.startswith("State:"):
                        state = line.split(":", 1)[1].strip()
            procs.append({
                "pid": int(name),
                "rss_mb": round((rss_kb or 0) / 1024, 1),
                "state": state,
                "cmd": cmd[:500],
            })
        except Exception:
            pass
    return procs


def du_bytes(path):
    try:
        out = subprocess.check_output(["du", "-sb", path], stderr=subprocess.DEVNULL).decode().split()[0]
        return int(out)
    except Exception:
        return None


def disk(path):
    target = path if os.path.exists(path) else os.path.dirname(path)
    while target and not os.path.exists(target):
        nxt = os.path.dirname(target)
        if nxt == target:
            break
        target = nxt
    try:
        st = os.statvfs(target)
        return {
            "path": target,
            "free_bytes": st.f_bavail * st.f_frsize,
            "total_bytes": st.f_blocks * st.f_frsize,
        }
    except Exception as exc:
        return {"error": str(exc)}


def first_existing(*paths):
    for path in paths:
        if os.path.exists(path):
            return path
    return paths[0]


csv_path = os.path.join(RUN_DIR, "asct_mainnet_metrics.csv")
out_path = first_existing(os.path.join(RUN_DIR, "go_test.out.log"), os.path.join(CONTROL_DIR, "go_test.out.log"))
err_path = first_existing(os.path.join(RUN_DIR, "go_test.err.log"), os.path.join(CONTROL_DIR, "go_test.err.log"))
log_path = first_existing(os.path.join(RUN_DIR, "go_test.log"), os.path.join(CONTROL_DIR, "go_test.log"))
exit_path = first_existing(os.path.join(RUN_DIR, "go_test.exit"), os.path.join(CONTROL_DIR, "go_test.exit"))
exit_txt_path = first_existing(os.path.join(RUN_DIR, "go_test.exit.txt"), os.path.join(CONTROL_DIR, "go_test.exit.txt"))
guard_path = os.path.join(CONTROL_DIR, "disk_guard.log")
if not os.path.exists(guard_path):
    guard_path = os.path.join(RUN_DIR, "disk_guard.log")

result = {
    "run_dir": RUN_DIR,
    "control_dir": CONTROL_DIR,
    "processes": proc_list(),
    "csv": latest_csv(csv_path),
    "err_bytes": file_size(err_path),
    "err_tail": tail(err_path, 12),
    "out_tail": tail(out_path, 12),
    "go_test_log_tail": tail(log_path, 12),
    "exit": read_text(exit_path),
    "exit_txt": read_text(exit_txt_path),
    "guard_tail": tail(guard_path, 12),
    "run_bytes": du_bytes(RUN_DIR),
    "disk": disk(RUN_DIR),
}
print(json.dumps(result, ensure_ascii=False))
'''


def remote_code(run_dir, control_dir):
    return (
        REMOTE_TEMPLATE
        .replace("__RUN_DIR__", json.dumps(run_dir))
        .replace("__CONTROL_DIR__", json.dumps(control_dir))
    )


def check_run(config, run):
    server = config[run["server"]]
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
    command = "python3 - <<'PY'\n" + remote_code(run["run_dir"], run["control_dir"]) + "\nPY"
    stdin, stdout, stderr = client.exec_command(command, timeout=60)
    out = stdout.read().decode("utf-8", "replace")
    err = stderr.read().decode("utf-8", "replace")
    rc = stdout.channel.recv_exit_status()
    client.close()
    data = json.loads(out.strip().splitlines()[-1]) if out.strip() else None
    return {
        "name": run["name"],
        "host": server["host"],
        "rc": rc,
        "ssh_stderr": err.strip(),
        "data": data,
    }


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else ".agent/asct_remote_servers.local.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    results = []
    for run in RUNS:
        try:
            results.append(check_run(config, run))
        except Exception as exc:
            results.append({
                "name": run["name"],
                "error": repr(exc),
                "trace": traceback.format_exc(limit=2),
            })
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
