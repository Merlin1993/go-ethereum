"""S1' run: AMT pathdb hot-mpt re-run of the S1 heavy segment with op-trace on.

Why: S1 (20260918_085636) landed r=0.594 < 0.7 and 66.8% of window wall sat in
the per-operation bucket. T4 instrumentation (MPT_OP_TRACE=1 -> op_trace.csv)
splits that bucket into hot-trie / filter-probe / bucket-load / archived-copy
-removal sections so the optimization loop knows where the time actually goes.

Protocol identical to S1 (file-9 segment, 393,216,000 ops = 1.5 archive rounds
at nibbles=4, batch 4000) so windows align 1:1 with the S1 AMT CSV; the MPT
comparator is S1's own fresh-start reference (its code path is untouched, no
rerun needed). Only the AMT stage runs here.
"""
import hashlib
import json
from datetime import datetime
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
REMOTE_WORK = "/root/asct_codex"
REMOTE_SOURCE = f"{REMOTE_WORK}/go-ethereum-trace"
TRACE_INPUT = "/root/asct_codex/mainnet_state_access_trace/range_10m"

DOMAIN_NIBBLES = 4
DOMAIN_COUNT = 1 << (4 * DOMAIN_NIBBLES)
BATCH_SIZE = 4000
ROUNDS = 1.5
OPERATIONS = int(DOMAIN_COUNT * BATCH_SIZE * ROUNDS)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS = f"{REMOTE_WORK}/results"
RUN_AMT = f"{RESULTS}/trace_stress/S1p_amt_optrace_nib{DOMAIN_NIBBLES}_{OPERATIONS}_{STAMP}"
REMOTE_LAUNCHER = f"{REMOTE_WORK}/run_s1p_optrace_{STAMP}.sh"
REMOTE_GUARD = f"{REMOTE_WORK}/full_replay_disk_guard.sh"
MIN_FREE_BYTES = 8_000_000_000

FILES = [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive").glob("*.go"))
] + [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive/mpt").glob("*.go"))
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
ps -eo pid=,comm=,args= | awk '$2 == "go" || $2 == "archive.test" || $2 == "tree.test" {{print}} /run_full_replay_[0-9]+_[0-9]+\\.sh$/ {{print}} /run_s1_pair_[0-9]+_[0-9]+\\.sh$/ {{print}} /run_s1p_optrace_[0-9]+_[0-9]+\\.sh$/ {{print}}' || true
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

printf '%s S1p: AMT pathdb hot-mpt with MPT_OP_TRACE=1, {ROUNDS} rounds = {OPERATIONS} ops, nibbles={DOMAIN_NIBBLES}\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'

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

export MPT_OP_TRACE=1
set +e
run_stage S1p_amt_optrace '{RUN_AMT}.exit' '{RUN_AMT}.log' \\
  /usr/local/go/bin/go test ./trie/archive \\
    -run '^TestArchiveStemTraceStress$' -count=1 -timeout 0 -v \\
    -args \\
      -traceStressInputDir={TRACE_INPUT} \\
      -traceStressBaseDir='{RUN_AMT}' \\
      -traceStressOps={OPERATIONS} \\
      -traceStressStartFile=9 \\
      -traceStressBatchSize={BATCH_SIZE} \\
      -traceStressMetricsBatches=2500 \\
      -traceStressStemMode=false \\
      -traceStressDisableArchive=false \\
      -traceStressHotLayer=mpt \\
      -traceStressTrieBackend=pathdb \\
      -traceStressDomainNibbles={DOMAIN_NIBBLES} \\
      -traceStressPathCleanCacheMB=256 \\
      -traceStressPathWriteBufferMB=256 \\
      -traceStressPathFlushEveryBatches=0 \\
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
            "domain_nibbles": DOMAIN_NIBBLES,
            "start_file": 9,
            "windows": OPERATIONS // 10_000_000,
            "run_amt": RUN_AMT,
            "status_log": f"{REMOTE_LAUNCHER}.status.log",
            "uploaded_files": upload_names,
            "free_bytes": free_bytes,
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
