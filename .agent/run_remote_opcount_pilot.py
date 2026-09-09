import hashlib
import json
from datetime import datetime
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
ROOT = "/root/asct_codex/results"
RUNS = {
    "asct": f"{ROOT}/trace_stress/opcount_pilot_depth16_3m_{STAMP}",
    "mpt": f"{ROOT}/trie_compare/opcount_pilot_mpt_3m_{STAMP}",
    "verkle": f"{ROOT}/trie_compare/opcount_pilot_verkle_3m_{STAMP}",
}
REMOTE_SOURCE = "/root/asct_codex/go-ethereum-trace"
REMOTE_LAUNCHER = f"/root/asct_codex/run_opcount_pilot_{STAMP}.sh"

FILES = [
    "trie/archive/trace_stress_test.go",
    "trie/archive/trace_cold_hit_stats_test.go",
    "trie/archive/diagnostics.go",
    "trie/archive/shard.go",
    "trie/archive/stem.go",
    "trie/archive/trie.go",
    "trie/archive/value_store.go",
    "trie/archive/stem_test.go",
    "core/tree_test/trace_compare_test.go",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        paths = list(RUNS.values())
        command = "set -e; " + " ".join(f"test ! -e '{path}' && test ! -e '{path}.log' && test ! -e '{path}.exit' &&" for path in paths)
        command += " df -B1 --output=avail /root/asct_codex | tail -n 1"
        _, stdout, stderr = client.exec_command(command, timeout=30)
        free_bytes = int(stdout.read().decode().strip().splitlines()[-1])
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(error or "pilot preflight failed")
        if free_bytes < 5_000_000_000:
            raise RuntimeError(f"only {free_bytes} bytes free; require 5 GiB")

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

        common = """#!/usr/bin/env bash
set -u
cd '/root/asct_codex/go-ethereum-trace'
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp
mkdir -p "$GOTMPDIR" /root/asct_codex/results/trace_stress /root/asct_codex/results/trie_compare
overall=0

run_asct() {
  local dir="$1"; code=0
  /usr/local/go/bin/go test ./trie/archive \\
    -run '^TestArchiveStemTraceStress$' -count=1 -timeout 0 -v \\
    -args \\
      -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
      -traceStressBaseDir="$dir" \\
      -traceStressOps=3000000 \\
      -traceStressStartFile=9 \\
      -traceStressBatchSize=4000 \\
      -traceStressMetricsBatches=250 \\
      -traceStressShardDepth=16 \\
      -traceStressPruneEveryBatches=1 \\
      -traceStressAccessSampleEvery=1000 \\
      -traceStressFinalStats=false \\
    >"$dir.log" 2>&1 || code=$?
  echo "$code" >"$dir.exit"
  if [ "$code" -ne 0 ]; then overall=$code; fi
}

run_compare() {
  local engine="$1"; local dir="$2"; code=0
  /usr/local/go/bin/go test ./core/tree_test \\
    -run '^TestTrieTraceCompare$' -count=1 -timeout 0 -v \\
    -args \\
      -traceCompareInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
      -traceCompareEngine="$engine" \\
      -traceCompareBaseDir="$dir" \\
      -traceCompareOps=3000000 \\
      -traceCompareStartFile=9 \\
      -traceCompareBatchSize=4000 \\
      -traceCompareMetricsBatches=250 \\
      -traceCompareTimingSampleEvery=1000 \\
    >"$dir.log" 2>&1 || code=$?
  echo "$code" >"$dir.exit"
  if [ "$code" -ne 0 ]; then overall=$code; fi
}
"""
        common += f"run_asct '{RUNS['asct']}'\n"
        common += f"run_compare mpt '{RUNS['mpt']}'\n"
        common += f"run_compare verkle '{RUNS['verkle']}'\n"
        common += "exit \"$overall\"\n"
        sftp = client.open_sftp()
        try:
            with sftp.file(REMOTE_LAUNCHER, "w") as remote_file:
                remote_file.write(common)
        finally:
            sftp.close()
        _, stdout, stderr = client.exec_command(f"chmod +x '{REMOTE_LAUNCHER}' && (nohup '{REMOTE_LAUNCHER}' </dev/null >/dev/null 2>&1 &) && echo launched", timeout=10)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0 or result != "launched":
            raise RuntimeError(f"launch failed: {error or result}")
        print(json.dumps({
            "status": "launched",
            "runs": RUNS,
            "launcher": REMOTE_LAUNCHER,
            "free_bytes": free_bytes,
            "files": hashes,
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
