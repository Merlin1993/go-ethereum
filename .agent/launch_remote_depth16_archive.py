import hashlib
import json
import sys
from pathlib import Path

import paramiko


HOST_OVERRIDE = "192.168.2.230"
RUN_STAMP = "20260820_depth16"
REMOTE_RUN = f"/root/asct_codex/results/trace_stress/archive_1b_depth16_{RUN_STAMP}"
REMOTE_LAUNCHER = f"/root/asct_codex/run_asct_depth16_{RUN_STAMP}.sh"
LOCAL_DRIVER = Path(r"D:\go_workspace\go-ethereum\trie\archive\trace_stress_test.go")
REMOTE_DRIVER = "/root/asct_codex/go-ethereum-trace/trie/archive/trace_stress_test.go"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    servers = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())
    server = servers["asct_mpt"].copy()
    server["host"] = HOST_OVERRIDE

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        server["host"],
        username=server["ssh_user"],
        password=server["ssh_password"],
        look_for_keys=False,
        allow_agent=False,
    )
    try:
        preflight = (
            "set -e; "
            "test ! -e '{run}' && test ! -e '{run}.log' && test ! -e '{run}.exit'; "
            "test -f '{trace}'; "
            "echo FREE_BYTES=$(df -B1 --output=avail /root/asct_codex | tail -n 1); "
            "pgrep -af 'archive.test|TestArchiveStemTraceStress' || true"
        ).format(run=REMOTE_RUN, trace="/root/asct_codex/mainnet_state_access_trace/range_10m/state_access_trace_009046147_010000000.csv.gz")
        _, stdout, stderr = client.exec_command(preflight, timeout=30)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(f"preflight failed: {err or out}")
        free_line = next(line for line in out.splitlines() if line.startswith("FREE_BYTES="))
        free_bytes = int(free_line.split("=", 1)[1])
        if free_bytes < 8_000_000_000:
            raise RuntimeError(f"only {free_bytes} bytes free; require at least 8 GiB")

        sftp = client.open_sftp()
        local_hash = sha256(LOCAL_DRIVER)
        try:
            remote_hash = sftp.stat(REMOTE_DRIVER)
            del remote_hash
            sftp.put(str(LOCAL_DRIVER), REMOTE_DRIVER)
        finally:
            sftp.close()

        launcher = f"""#!/usr/bin/env bash
set -u
cd /root/asct_codex/go-ethereum-trace
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp
mkdir -p "$GOTMPDIR" /root/asct_codex/results/trace_stress
run='{REMOTE_RUN}'
test ! -e "$run"
code=0
/usr/local/go/bin/go test ./trie/archive \\
  -run '^TestArchiveStemTraceStress$' \\
  -count=1 -timeout 0 -v \\
  -args \\
    -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
    -traceStressBaseDir="$run" \\
    -traceStressOps=1000000000 \\
    -traceStressBatchSize=4000 \\
    -traceStressMetricsBatches=2500 \\
    -traceStressStartFile=9 \\
    -traceStressShardDepth=16 \\
    -traceStressFinalStats=true \\
  >"${{run}}.log" 2>&1 || code=$?
echo "$code" > "${{run}}.exit"
exit "$code"
"""
        with client.open_sftp().file(REMOTE_LAUNCHER, "w") as remote_file:
            remote_file.write(launcher)
        _, stdout, stderr = client.exec_command(f"chmod +x '{REMOTE_LAUNCHER}' && nohup '{REMOTE_LAUNCHER}' >/dev/null 2>&1 & echo $!", timeout=10)
        pid_out = stdout.read().decode().strip()
        pid_err = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0 or not pid_out.isdigit():
            raise RuntimeError(f"launch failed: {pid_err or pid_out}")

        print(json.dumps({
            "status": "launched",
            "host": server["host"],
            "run": REMOTE_RUN,
            "launcher_pid": int(pid_out),
            "driver_sha256": local_hash,
            "free_bytes_before": free_bytes,
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise
