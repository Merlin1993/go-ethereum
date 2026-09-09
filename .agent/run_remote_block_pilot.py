import hashlib
import json
from datetime import datetime
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN = f"/root/asct_codex/results/trace_stress/block_pilot_depth16_{STAMP}"
REMOTE_SOURCE = "/root/asct_codex/go-ethereum-trace"
REMOTE_LAUNCHER = f"/root/asct_codex/run_block_pilot_{STAMP}.sh"

FILES = [
    "trie/archive/trace_stress_test.go",
    "trie/archive/trace_cold_hit_stats_test.go",
    "trie/archive/diagnostics.go",
    "trie/archive/shard.go",
    "trie/archive/stem.go",
    "trie/archive/trie.go",
    "trie/archive/value_store.go",
    "trie/archive/stem_test.go",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        command = f"set -e; test ! -e '{RUN}'; test ! -e '{RUN}.log'; test ! -e '{RUN}.exit'; df -B1 --output=avail /root/asct_codex | tail -n 1"
        _, stdout, stderr = client.exec_command(command, timeout=30)
        free_bytes = int(stdout.read().decode().strip().splitlines()[-1])
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(error or "pilot preflight failed")
        if free_bytes < 8_000_000_000:
            raise RuntimeError(f"only {free_bytes} bytes free; require 8 GiB")

        hashes = {}
        sftp = client.open_sftp()
        try:
            for name in FILES:
                local = Path(name)
                remote = f"{REMOTE_SOURCE}/{name}"
                sftp.put(str(local), remote)
                hashes[name] = sha256(local)
        finally:
            sftp.close()

        launcher = f"""#!/usr/bin/env bash
set -u
cd '{REMOTE_SOURCE}'
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp
mkdir -p "$GOTMPDIR" /root/asct_codex/results/trace_stress
code=0
/usr/local/go/bin/go test ./trie/archive \\
  -run '^(TestTraceStressAccessClassifier|TestArchiveStemTraceStress)$' \\
  -count=1 -timeout 0 -v \\
  -args \\
    -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
    -traceStressBaseDir='{RUN}' \\
    -traceStressOps=0 \\
    -traceStressBlocks=1000 \\
    -traceStressStartBlock=9046147 \\
    -traceStressBatchSize=4000 \\
    -traceStressMetricsBatches=100 \\
    -traceStressStartFile=9 \\
    -traceStressShardDepth=16 \\
    -traceStressPruneEveryBatches=0 \\
    -traceStressPruneEveryBlocks=10 \\
    -traceStressAccessSampleEvery=1000 \\
    -traceStressFinalStats=false \\
  >'{RUN}.log' 2>&1 || code=$?
echo "$code" >'{RUN}.exit'
exit "$code"
"""
        with client.open_sftp().file(REMOTE_LAUNCHER, "w") as remote_file:
            remote_file.write(launcher)
        _, stdout, stderr = client.exec_command(f"chmod +x '{REMOTE_LAUNCHER}' && (nohup '{REMOTE_LAUNCHER}' </dev/null >/dev/null 2>&1 &) && echo launched", timeout=10)
        pid = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0 or pid != "launched":
            raise RuntimeError(f"launch failed: {error or pid}")

        print(json.dumps({
            "status": "launched",
            "run": RUN,
            "log": f"{RUN}.log",
            "exit": f"{RUN}.exit",
            "launcher": REMOTE_LAUNCHER,
            "free_bytes": free_bytes,
            "files": hashes,
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
