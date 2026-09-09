import hashlib
import json
from datetime import datetime
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
ROOT = "/root/asct_codex"
SOURCE = f"{ROOT}/go-ethereum-trace"
OPS = 1_000_000_000
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASELINE = f"{ROOT}/results/trace_stress/formal_read_promotion_off_1b_depth16_{STAMP}"
PROMOTED = f"{ROOT}/results/trace_stress/formal_read_promotion_on_1b_depth16_{STAMP}"
LAUNCHER = f"{ROOT}/run_read_promotion_ab_{STAMP}.sh"
GUARD = f"{ROOT}/full_replay_disk_guard.sh"
MIN_FREE = 8_000_000_000

FILES = [
    ".agent/asct_trace_stress_disk_guard.sh",
    "trie/archive/config.go",
    "trie/archive/diagnostics.go",
    "trie/archive/shard.go",
    "trie/archive/stem.go",
    "trie/archive/stem_test.go",
    "trie/archive/trace_stress_test.go",
    "trie/archive/trie.go",
    "trie/archive/value_store.go",
    "trie/archive/cuckoo/cuckoo.go",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    cfg = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=cfg["ssh_user"], password=cfg["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        check = "set -e; " + " ".join(
            f"test ! -e '{p}' && test ! -e '{p}.log' && test ! -e '{p}.exit' &&" for p in (BASELINE, PROMOTED)
        )
        check += f" test ! -e '{LAUNCHER}'; df -B1 --output=avail {ROOT} | tail -n 1"
        _, stdout, stderr = client.exec_command(check, timeout=30)
        text = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(error or "A/B preflight failed")
        free = int(text.splitlines()[-1])
        if free < MIN_FREE:
            raise RuntimeError(f"only {free} bytes free; require {MIN_FREE}")

        _, stdout, _ = client.exec_command("pgrep -af '[g]o test|[a]rchive.test|[t]ree.test|[T]estArchiveStemTraceStress' || true", timeout=30)
        running = stdout.read().decode().strip()
        if running:
            raise RuntimeError(f"an existing replay process is still running:\n{running}")

        hashes = {}
        sftp = client.open_sftp()
        try:
            for name in FILES:
                local = Path(name)
                if not local.exists():
                    raise RuntimeError(f"missing sync file: {name}")
                remote = f"{ROOT}/asct_trace_stress_disk_guard.sh" if name.startswith(".agent/") else f"{SOURCE}/{name}"
                sftp.put(str(local), remote)
                hashes[name] = sha256(local)
        finally:
            sftp.close()

        launcher = f"""#!/usr/bin/env bash
set -u
cd '{SOURCE}'
export GOCACHE={ROOT}/go-build-cache
export GOTMPDIR={ROOT}/trace-tmp
mkdir -p "$GOTMPDIR" '{ROOT}/results/trace_stress'

run_one() {{
  local run="$1"; local promote="$2"; local log="$run.log"
  mkdir -p "$(dirname "$run")"
  /usr/local/go/bin/go test ./trie/archive -run '^TestArchiveStemTraceStress$' -count=1 -timeout 0 -v -args \\
    -traceStressInputDir={ROOT}/mainnet_state_access_trace/range_10m \\
    -traceStressBaseDir="$run" -traceStressOps={OPS} -traceStressStartFile=0 \\
    -traceStressBatchSize=4000 -traceStressMetricsBatches=2500 -traceStressShardDepth=16 \\
    -traceStressPruneEveryBatches=1 -traceStressAccessSampleEvery=1000 \\
    -traceStressActivateArchivedStemOnRead="$promote" -traceStressFinalStats=true >"$log" 2>&1 &
  local pid=$!
  '{GUARD}' "$pid" {MIN_FREE} "$log" & local guard=$!
  set +e; wait "$pid"; local code=$?; set -e
  kill "$guard" 2>/dev/null || true; wait "$guard" 2>/dev/null || true
  printf '%s\\n' "$code" >"$run.exit"
  return "$code"
}}

printf '%s baseline start\\n' "$(date --iso-8601=seconds)" >'{LAUNCHER}.status.log'
run_one '{BASELINE}' false || exit 1
printf '%s baseline complete\\n' "$(date --iso-8601=seconds)" >>'{LAUNCHER}.status.log'
run_one '{PROMOTED}' true || exit 1
printf '%s promoted complete\\n' "$(date --iso-8601=seconds)" >>'{LAUNCHER}.status.log'
"""
        sftp = client.open_sftp()
        try:
            with sftp.file(LAUNCHER, "w") as f:
                f.write(launcher)
        finally:
            sftp.close()
        _, stdout, stderr = client.exec_command(f"chmod +x '{LAUNCHER}' '{GUARD}' && nohup '{LAUNCHER}' </dev/null >/dev/null 2>&1 & echo $!", timeout=10)
        pid = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        if not pid.isdigit():
            raise RuntimeError(error or f"launch failed: {pid}")
        print(json.dumps({
            "status": "launched",
            "operations": OPS,
            "depth16_rings": OPS / (65536 * 4000),
            "baseline": BASELINE,
            "promoted": PROMOTED,
            "launcher": LAUNCHER,
            "status_log": f"{LAUNCHER}.status.log",
            "pid": int(pid),
            "free_bytes": free,
            "minimum_free_bytes": MIN_FREE,
            "files": hashes,
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
