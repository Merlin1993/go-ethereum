"""D-stage (B2 recalibration): AMT pathdb hot-mpt at one shard depth, file 9.

Why: single-tree 16-ary form invalidated the old D15 conclusion; P4/P5 changed
the prune/bucket physics again. Pick the depth by running 1.5 archive rounds
each and comparing hot-hit (G3) -> FPR (G4) -> State_Bytes (G2, PathStats
three-sum) -> ops/s (<=15% degradation tolerance).

2026-09-23: shard granularity moved from nibbles (16^N, 16x steps) to bits
(2^D, 2x steps, commit bca5bef10) so the archive round lands in the 3-6 month
key-lifetime window: epoch two-round eviction means key lifetime is [1, 2]
rounds, so D=19 (524,288 shards, ~86 days/round) gives ~3-5.7 months.

Protocol: file-9 segment, ops = 1.5 * 2^D * 4000 (one shard pruned per batch),
archive ON, read-activation ON, cuckoo 32x4, NodeCache 512MB, pathdb with the
C-loop conclusion FlushEveryBatches=25. MPT_OP_TRACE and MPT_ECMH_VERIFY stay
OFF (formal-run posture per C5).

Knob: D2_DEPTH_BITS (default 19). Historical equivalents: nibbles 3/4/5 = bits
12/16/20.

IMPORTANT: FILES must glob mpt/hx and mpt/ecmh too (P4/P5 added subpackages) —
the old s1p launcher's `mpt/*.go` glob silently misses them.
"""
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

import paramiko


# IP drifts (NIC/DHCP): always take it from the creds JSON, never hardcode.
HOST = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]["host"]
REMOTE_WORK = "/root/asct_codex"
REMOTE_SOURCE = f"{REMOTE_WORK}/go-ethereum-trace"
TRACE_INPUT = "/root/asct_codex/mainnet_state_access_trace/range_10m"

DEPTH_BITS = int(os.environ.get("D2_DEPTH_BITS", "19"))
if DEPTH_BITS < 12 or DEPTH_BITS > 32:
    raise SystemExit(f"D2_DEPTH_BITS must be 12..32, got {DEPTH_BITS}")
# D2_START_FILE: shard index to start from. Default 9 (the calibration segment).
# Use 0 for the long-horizon variant: shards 0..9 hold ~34B ops, enough for a
# D=20 round (4.19B ops) to complete — from file 9 the trace dies at ~3.4B.
START_FILE = int(os.environ.get("D2_START_FILE", "9"))
DOMAIN_COUNT = 1 << DEPTH_BITS
BATCH_SIZE = 4000
ROUNDS = 1.5
OPERATIONS = int(DOMAIN_COUNT * BATCH_SIZE * ROUNDS)
FLUSH_BATCHES = 25  # C-loop conclusion: flush per 100K ops, r=0.891 @D=16
# Window sizing: keep ~24-40 windows per run so the CSV stays readable.
METRICS_BATCHES = max(2500, (DOMAIN_COUNT * 3 // 2) // 40)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS = f"{REMOTE_WORK}/results"
RUN_AMT = f"{RESULTS}/trace_stress/D2_amt_bits{DEPTH_BITS}_{OPERATIONS}_fl{FLUSH_BATCHES}_sf{START_FILE}_{STAMP}"
REMOTE_LAUNCHER = f"{REMOTE_WORK}/run_d2_bits{DEPTH_BITS}_{STAMP}.sh"
REMOTE_GUARD = f"{REMOTE_WORK}/full_replay_disk_guard.sh"
MIN_FREE_BYTES = 8_000_000_000

FILES = [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive").glob("*.go"))
] + [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive/mpt").glob("*.go"))
] + [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive/mpt/hx").glob("*.go"))
] + [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive/mpt/ecmh").glob("*.go"))
] + [
    "core/tree_test/trace_compare_test.go",
    ".agent/asct_trace_stress_disk_guard.sh",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_remote(client: paramiko.SSHClient, command: str, timeout: int) -> tuple[int, str, str]:
    _, stdout, stderr = client.exec_command(command, timeout=timeout)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    code = stdout.channel.recv_exit_status()
    return code, out, err


def main() -> None:
    credentials = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST,
        username=credentials["ssh_user"],
        password=credentials["ssh_password"],
        look_for_keys=False,
        allow_agent=False,
    )

    try:
        preflight = f"""
set -e
test ! -e '{RUN_AMT}' && test ! -e '{RUN_AMT}.exit'
test ! -e '{REMOTE_LAUNCHER}'
test ! -e '{REMOTE_LAUNCHER}.status.log'
free=$(df -B1 --output=avail /root | tail -n 1 | tr -d ' ')
printf 'FREE_BYTES=%s\\n' "$free"
ps -eo pid=,comm=,args= | awk '$2 == "go" || $2 == "archive.test" || $2 == "tree.test" {{print}} /run_full_replay_[0-9]+_[0-9]+\\.sh$/ {{print}} /run_s1p_optrace_[0-9]+_[0-9]+\\.sh$/ {{print}} /run_d2_(nib|bits)[0-9]+_[0-9]+_[0-9]+\\.sh$/ {{print}}' || true
"""
        code, output, error = run_remote(client, preflight, 30)
        if code != 0:
            raise RuntimeError(f"preflight failed: {error or output}")

        free_bytes = 0
        running_lines = []
        for line in output.splitlines():
            if line.startswith("FREE_BYTES="):
                free_bytes = int(line.split("=", 1)[1])
            elif line.strip():
                running_lines.append(line)
        if running_lines:
            raise RuntimeError("remote processes still running: " + "; ".join(running_lines))
        if free_bytes < MIN_FREE_BYTES * 4:
            raise RuntimeError(f"insufficient disk: {free_bytes}")

        local_hashes = {}
        for name in FILES:
            local = Path(name)
            if not local.exists():
                raise RuntimeError(f"local file missing: {name}")
            local_hashes[name] = sha256(local)
        remote_paths = " ".join(f"'{REMOTE_SOURCE}/{name}'" for name in FILES)
        code, output, error = run_remote(client, f"sha256sum {remote_paths} 2>/dev/null || true", 60)
        remote_hashes = {}
        for line in output.splitlines():
            parts = line.split("  ", 1)
            if len(parts) == 2:
                remote_hashes[parts[1].replace(f"{REMOTE_SOURCE}/", "")] = parts[0]
        upload_names = [name for name in FILES if remote_hashes.get(name) != local_hashes[name]]

        sftp = client.open_sftp()
        try:
            for name in upload_names:
                remote = f"{REMOTE_SOURCE}/{name}"
                remote_dir = remote.rsplit("/", 1)[0]
                try:
                    sftp.stat(remote_dir)
                except FileNotFoundError:
                    sftp.mkdir(remote_dir)
                sftp.put(str(Path(name)), remote)
        finally:
            sftp.close()

        launcher = f"""#!/usr/bin/env bash
set -u
cd '{REMOTE_SOURCE}'
export GOCACHE='{REMOTE_WORK}/go-build-cache' GOTMPDIR='{REMOTE_WORK}/trace-tmp'
mkdir -p "$GOTMPDIR" '{RESULTS}/trace_stress'
printf '%s\\n' "$$" >'{REMOTE_LAUNCHER}.pid'
overall=0
disk_stop=0
run_guarded() {{
  local log_file="$1"; shift
  "$@" >"$log_file" 2>&1 &
  local run_pid=$!
  '{REMOTE_GUARD}' "$run_pid" {MIN_FREE_BYTES} "$log_file" &
  local guard_pid=$!
  set +e
  wait "$run_pid"; local code=$?
  set -e
  kill "$guard_pid" 2>/dev/null || true
  wait "$guard_pid" 2>/dev/null || true
  return "$code"
}}
run_stage() {{
  local name="$1" exit_file="$2" log_file="$3"; shift 3
  printf '%s %s start\\n' "$(date --iso-8601=seconds)" "$name" >>'{REMOTE_LAUNCHER}.status.log'
  set +e
  run_guarded "$log_file" "$@"
  local code=$?
  set -e
  printf '%s\\n' "$code" >"$exit_file"
  if [ "$code" -ne 0 ]; then
    printf '%s %s failed exit=%s\\n' "$(date --iso-8601=seconds)" "$name" "$code" >>'{REMOTE_LAUNCHER}.status.log'
    overall=1
    [ "$code" -eq 2 ] && disk_stop=1
    return 1
  fi
  printf '%s %s completed\\n' "$(date --iso-8601=seconds)" "$name" >>'{REMOTE_LAUNCHER}.status.log'
  return 0
}}

printf '%s D2: AMT pathdb hot-mpt depth calibration, {ROUNDS} rounds = {OPERATIONS} ops, depth_bits={DEPTH_BITS}, fl{FLUSH_BATCHES}\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'

# Compile gate first: a build failure must not consume hours of trace time.
set +e
run_stage build '{REMOTE_LAUNCHER}.build.exit' '{REMOTE_LAUNCHER}.build.log' \\
  bash -c "set -e; /usr/local/go/bin/go test -c -o /dev/null ./trie/archive"
build_ok=$?
set -e
if [ "$build_ok" -ne 0 ]; then
  printf '%s aborted: build failed, stage skipped\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
  printf '%s\\n' "1" >'{REMOTE_LAUNCHER}.overall.exit'
  exit 1
fi

# Formal-run posture: MPT_OP_TRACE off, MPT_ECMH_VERIFY off (C5).
set +e
run_stage D2_amt '{RUN_AMT}.exit' '{RUN_AMT}.log' \\
  /usr/local/go/bin/go test ./trie/archive \\
    -run '^TestArchiveStemTraceStress$' -count=1 -timeout 0 -v \\
    -args \\
      -traceStressInputDir={TRACE_INPUT} \\
      -traceStressBaseDir='{RUN_AMT}' \\
      -traceStressOps={OPERATIONS} \\
      -traceStressStartFile={START_FILE} \\
      -traceStressBatchSize={BATCH_SIZE} \\
      -traceStressMetricsBatches={METRICS_BATCHES} \\
      -traceStressStemMode=false \\
      -traceStressDisableArchive=false \\
      -traceStressHotLayer=mpt \\
      -traceStressTrieBackend=pathdb \\
      -traceStressShardDepthBits={DEPTH_BITS} \\
      -traceStressPathCleanCacheMB=256 \\
      -traceStressPathWriteBufferMB=256 \\
      -traceStressPathFlushEveryBatches={FLUSH_BATCHES} \\
      -traceStressCuckooBuckets=32 \\
      -traceStressCuckooSlots=4 \\
      -traceStressNodeCacheMB=512 \\
      -traceStressCommitWorkers=16 \\
      -traceStressAsyncPrune=true \\
      -traceStressPruneEveryBatches=1 \\
      -traceStressDestructiveCommit=true \\
      -traceStressAccessSampleEvery=1000 \\
      -traceStressActivateArchivedKeyOnRead=true \\
      -traceStressFinalStats=false
set -e

if [ "$overall" -ne 0 ]; then
  printf '%s complete_with_errors\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
else
  printf '%s complete\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
fi
printf '%s\\n' "$overall" >'{REMOTE_LAUNCHER}.overall.exit'
exit "$overall"
"""

        sftp = client.open_sftp()
        try:
            with sftp.file(REMOTE_LAUNCHER, "w") as remote_file:
                remote_file.write(launcher)
        finally:
            sftp.close()

        launch_command = (
            f"chmod +x '{REMOTE_LAUNCHER}' '{REMOTE_GUARD}' && "
            f"(setsid nohup '{REMOTE_LAUNCHER}' </dev/null >/dev/null 2>&1 &) && echo launched"
        )
        code, output, error = run_remote(client, launch_command, 20)
        if code != 0 or "launched" not in output:
            raise RuntimeError(f"launch failed: {error or output}")

        print(json.dumps({
            "status": "launched",
            "stamp": STAMP,
            "operations": OPERATIONS,
            "rounds": ROUNDS,
            "shard_depth_bits": DEPTH_BITS,
            "metrics_batches": METRICS_BATCHES,
            "start_file": START_FILE,
            "flush_every_batches": FLUSH_BATCHES,
            "windows": OPERATIONS // (METRICS_BATCHES * BATCH_SIZE),
            "run_amt": RUN_AMT,
            "status_log": f"{REMOTE_LAUNCHER}.status.log",
            "uploaded_files": upload_names,
            "free_bytes": free_bytes,
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
