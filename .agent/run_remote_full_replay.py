import hashlib
import json
from datetime import datetime
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OPERATIONS = 20_242_112_513
ROOT = "/root/asct_codex/results"
RUNS = {
    "asct": f"{ROOT}/trace_stress/formal_20242112513_depth16_{STAMP}",
    "mpt": f"{ROOT}/trie_compare/formal_20242112513_mpt_{STAMP}",
    "verkle": f"{ROOT}/trie_compare/formal_20242112513_verkle_{STAMP}",
}
REMOTE_SOURCE = "/root/asct_codex/go-ethereum-trace"
REMOTE_LAUNCHER = f"/root/asct_codex/run_full_replay_{STAMP}.sh"
REMOTE_GUARD = "/root/asct_codex/full_replay_disk_guard.sh"
MIN_FREE_BYTES = 8_000_000_000

FILES = [
    ".agent/asct_trace_stress_disk_guard.sh",
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
        command += f" test ! -e '{REMOTE_LAUNCHER}'; df -B1 --output=avail /root/asct_codex | tail -n 1"
        _, stdout, stderr = client.exec_command(command, timeout=30)
        free_bytes = int(stdout.read().decode().strip().splitlines()[-1])
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(error or "full replay preflight failed")
        if free_bytes < 10_000_000_000:
            raise RuntimeError(f"only {free_bytes} bytes free; require 10 GiB before launch")
        _, process_stdout, _ = client.exec_command("pgrep -af '[g]o test|[a]rchive.test|[t]ree.test|[T]estArchiveStemTraceStress|[T]estTrieTraceCompare' || true", timeout=30)
        running_processes = process_stdout.read().decode().strip()
        if running_processes:
            raise RuntimeError(f"an existing replay process is still running:\n{running_processes}")

        hashes = {}
        sftp = client.open_sftp()
        try:
            for name in FILES:
                local = Path(name)
                if name == ".agent/asct_trace_stress_disk_guard.sh":
                    remote = REMOTE_GUARD
                else:
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
mkdir -p "$GOTMPDIR" /root/asct_codex/results/trace_stress /root/asct_codex/results/trie_compare
overall=0

run_guarded() {{
  local log_file="$1"; shift
  "$@" >"$log_file" 2>&1 &
  local run_pid=$!
  '{REMOTE_GUARD}' "$run_pid" {MIN_FREE_BYTES} "$log_file" &
  local guard_pid=$!
  set +e
  wait "$run_pid"
  local code=$?
  set -e
  kill "$guard_pid" 2>/dev/null || true
  wait "$guard_pid" 2>/dev/null || true
  return "$code"
}}

write_exit() {{ local path="$1"; local code="$2"; printf '%s\\n' "$code" >"$path"; }}

set -e
printf '%s asct start\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
if run_guarded '{RUNS['asct']}.log' \\
  /usr/local/go/bin/go test ./trie/archive \\
    -run '^TestArchiveStemTraceStress$' -count=1 -timeout 0 -v \\
    -args \\
      -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
      -traceStressBaseDir='{RUNS['asct']}' \\
      -traceStressOps={OPERATIONS} \\
      -traceStressStartFile=0 \\
      -traceStressBatchSize=4000 \\
      -traceStressMetricsBatches=2500 \\
      -traceStressShardDepth=16 \\
      -traceStressPruneEveryBatches=1 \\
      -traceStressAccessSampleEvery=1000 \\
      -traceStressFinalStats=true; then
  write_exit '{RUNS['asct']}.exit' 0
else
  code=$?
  write_exit '{RUNS['asct']}.exit' "$code"
  printf '%s asct failed exit=%s\\n' "$(date --iso-8601=seconds)" "$code" >>'{REMOTE_LAUNCHER}.status.log'
  exit "$code"
fi

root=$(python3 -c "import json; print(json.load(open('{RUNS['asct']}/results/summary.json'))['last_root'])")
printf '%s asct storage stats start root=%s\\n' "$(date --iso-8601=seconds)" "$root" >>'{REMOTE_LAUNCHER}.status.log'
if run_guarded '{RUNS['asct']}.storage.log' \\
  /usr/local/go/bin/go test ./trie/archive \\
    -run '^TestArchiveStemTraceStorageStats$' -count=1 -timeout 0 -v \\
    -args \\
      -traceStatsBaseDir='{RUNS['asct']}' \\
      -traceStatsRoot="$root" \\
      -traceStatsOutput='{RUNS['asct']}/results/storage_breakdown.json' \\
      -traceStressShardDepth=16; then
  write_exit '{RUNS['asct']}.storage.exit' 0
else
  code=$?
  write_exit '{RUNS['asct']}.storage.exit' "$code"
  printf '%s asct storage failed exit=%s\\n' "$(date --iso-8601=seconds)" "$code" >>'{REMOTE_LAUNCHER}.status.log'
  exit "$code"
fi

printf '%s asct synthetic filter FP start\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
if run_guarded '{RUNS['asct']}.filterfp.log' \\
  /usr/local/go/bin/go test ./trie/archive \\
    -run '^TestArchiveStemTraceFilterFPStats$' -count=1 -timeout 0 -v \\
    -args \\
      -traceStatsBaseDir='{RUNS['asct']}' \\
      -traceStatsRoot="$root" \\
      -traceFilterOutput='{RUNS['asct']}/results/filter_fp_20_per_bucket.json' \\
      -traceFilterSamples=20 \\
      -traceStressShardDepth=16; then
  write_exit '{RUNS['asct']}.filterfp.exit' 0
else
  code=$?
  write_exit '{RUNS['asct']}.filterfp.exit' "$code"
  printf '%s asct filter FP failed exit=%s\\n' "$(date --iso-8601=seconds)" "$code" >>'{REMOTE_LAUNCHER}.status.log'
  exit "$code"
fi

printf '%s mpt start\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
if run_guarded '{RUNS['mpt']}.log' \\
  /usr/local/go/bin/go test ./core/tree_test \\
    -run '^TestTrieTraceCompare$' -count=1 -timeout 0 -v \\
    -args \\
      -traceCompareInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
      -traceCompareEngine=mpt \\
      -traceCompareBaseDir='{RUNS['mpt']}' \\
      -traceCompareOps={OPERATIONS} \\
      -traceCompareStartFile=0 \\
      -traceCompareBatchSize=4000 \\
      -traceCompareMetricsBatches=2500 \\
      -traceCompareTimingSampleEvery=1000; then
  write_exit '{RUNS['mpt']}.exit' 0
else
  code=$?
  write_exit '{RUNS['mpt']}.exit' "$code"
  printf '%s mpt failed exit=%s\\n' "$(date --iso-8601=seconds)" "$code" >>'{REMOTE_LAUNCHER}.status.log'
  exit "$code"
fi

printf '%s verkle start\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
if run_guarded '{RUNS['verkle']}.log' \\
  /usr/local/go/bin/go test ./core/tree_test \\
    -run '^TestTrieTraceCompare$' -count=1 -timeout 0 -v \\
    -args \\
      -traceCompareInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
      -traceCompareEngine=verkle \\
      -traceCompareBaseDir='{RUNS['verkle']}' \\
      -traceCompareOps={OPERATIONS} \\
      -traceCompareStartFile=0 \\
      -traceCompareBatchSize=4000 \\
      -traceCompareMetricsBatches=2500 \\
      -traceCompareTimingSampleEvery=1000; then
  write_exit '{RUNS['verkle']}.exit' 0
else
  code=$?
  write_exit '{RUNS['verkle']}.exit' "$code"
  printf '%s verkle failed exit=%s\\n' "$(date --iso-8601=seconds)" "$code" >>'{REMOTE_LAUNCHER}.status.log'
  exit "$code"
fi

printf '%s complete\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
exit 0
"""
        sftp = client.open_sftp()
        try:
            with sftp.file(REMOTE_LAUNCHER, "w") as remote_file:
                remote_file.write(launcher)
        finally:
            sftp.close()
        _, stdout, stderr = client.exec_command(f"chmod +x '{REMOTE_LAUNCHER}' '{REMOTE_GUARD}' && (nohup '{REMOTE_LAUNCHER}' </dev/null >/dev/null 2>&1 &) && echo launched", timeout=10)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0 or result != "launched":
            raise RuntimeError(f"launch failed: {error or result}")
        print(json.dumps({
            "status": "launched",
            "operations": OPERATIONS,
            "runs": RUNS,
            "launcher": REMOTE_LAUNCHER,
            "status_log": f"{REMOTE_LAUNCHER}.status.log",
            "minimum_free_bytes": MIN_FREE_BYTES,
            "free_bytes": free_bytes,
            "files": hashes,
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
