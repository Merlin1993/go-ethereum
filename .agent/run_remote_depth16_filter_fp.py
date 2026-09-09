import hashlib
import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
RUN = "/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16"
ROOT = "0xf5d80f2742e168723da2c7884c6f4d821859f8f919c582ef55be22f2128de436"
OUTPUT = f"{RUN}/results/filter_fp_20_per_bucket.json"
LOG = f"{RUN}.filterfp.log"
EXIT = f"{RUN}.filterfp.exit"
LOCAL_TEST = Path(r"D:\go_workspace\go-ethereum\trie\archive\trace_cold_hit_stats_test.go")
REMOTE_TEST = "/root/asct_codex/go-ethereum-trace/trie/archive/trace_cold_hit_stats_test.go"
REMOTE_LAUNCHER = "/root/asct_codex/run_depth16_filter_fp.sh"


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        _, stdout, stderr = client.exec_command(f"set -e; test ! -e '{OUTPUT}'; test ! -e '{EXIT}'", timeout=20)
        stdout.read()
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(error or "filter-FP output or exit marker already exists")

        sftp = client.open_sftp()
        try:
            sftp.put(str(LOCAL_TEST), REMOTE_TEST)
        finally:
            sftp.close()

        launcher = f"""#!/usr/bin/env bash
set -u
cd /root/asct_codex/go-ethereum-trace
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp
code=0
/usr/local/go/bin/go test ./trie/archive \\
  -run '^TestArchiveStemTraceFilterFPStats$' \\
  -count=1 -timeout 0 -v \\
  -args \\
    -traceStatsBaseDir='{RUN}' \\
    -traceStatsRoot='{ROOT}' \\
    -traceFilterOutput='{OUTPUT}' \\
    -traceFilterSamples=20 \\
    -traceFilterSeed=7 \\
    -traceStressShardDepth=16 \\
  >'{LOG}' 2>&1 || code=$?
echo "$code" >'{EXIT}'
exit "$code"
"""
        with client.open_sftp().file(REMOTE_LAUNCHER, "w") as remote_file:
            remote_file.write(launcher)

        _, stdout, stderr = client.exec_command(f"chmod +x '{REMOTE_LAUNCHER}' && nohup '{REMOTE_LAUNCHER}' >/dev/null 2>&1 & echo $!", timeout=10)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0 or not out.isdigit():
            raise RuntimeError(f"launch failed: {error or out}")
        print(json.dumps({
            "status": "launched",
            "run": RUN,
            "output": OUTPUT,
            "log": LOG,
            "exit": EXIT,
            "launcher_pid": int(out),
            "test_sha256": hashlib.sha256(LOCAL_TEST.read_bytes()).hexdigest(),
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
