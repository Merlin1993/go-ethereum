"""B3m calibration: A6 hexary-MPT hot layer wired into trace stress.

Goal: T3 recalibration of B3m after T1 commit-merge + T2 incremental aggregate.
committing to a full B3 re-run. Budget 500M ops (= 50 x 10M windows),
which covers the region where the binary hot layer measured r=0.15-0.19
(win 1-25 r=0.31, win 26-50 r=0.17, win 51+ r~0.15).

Protocol clone of run_remote_b3_amt_selfheal_20260915.py minus the storage
scan (G2 is not the subject of this gate). Adds trie/archive/mpt/*.go to the
upload set (the A6 fallback package was never synced before).
"""
import hashlib
import json
import paramiko
import time
from datetime import datetime
from pathlib import Path


HOST = "192.168.2.230"
REMOTE_WORK = "/root/asct_codex"
REMOTE_SOURCE = f"{REMOTE_WORK}/go-ethereum-trace"
RESULTS = f"{REMOTE_WORK}/results/trace_stress"
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OPERATIONS = 196_608_000
RUN_NAME = f"B3m2_t1t2_{OPERATIONS}_D15_{STAMP}"
RUN = f"{RESULTS}/{RUN_NAME}"
RUN_LOG = f"{RUN}.log"
RUN_EXIT = f"{RUN}.exit"
REMOTE_LAUNCHER = f"{REMOTE_WORK}/run_full_replay_{STAMP}.sh"
REMOTE_GUARD = f"{REMOTE_WORK}/full_replay_disk_guard.sh"
MIN_FREE_BYTES = 8_000_000_000

FILES = [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive").glob("*.go"))
] + [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive/mpt").glob("*.go"))
] + [
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
test ! -e '{RUN}' && test ! -e '{RUN_LOG}' && test ! -e '{RUN_EXIT}'
test ! -e '{REMOTE_LAUNCHER}'
test ! -e '{REMOTE_LAUNCHER}.status.log'
free=$(df -B1 --output=avail /root | tail -n 1 | tr -d ' ')
printf 'FREE_BYTES=%s\\n' "$free"
ps -eo pid=,comm=,args= | awk '$2 == "go" || $2 == "archive.test" || $2 == "tree.test" {{print}} /run_full_replay_[0-9]+_[0-9]+\\.sh$/ {{print}}' || true
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
        code, output, error = run_remote(client, f"sha256sum {remote_paths} 2>/dev/null || true", 30)
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
mkdir -p "$GOTMPDIR" '{RESULTS}'
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

printf '%s B3m hot-layer=MPT calibration D15 start ops={OPERATIONS} (A6 wiring test)\\n' "$(date --iso-8601=seconds)" >>'{REMOTE_LAUNCHER}.status.log'
set +e
run_stage B3m_D15 '{RUN_EXIT}' '{RUN_LOG}' \\
  /usr/local/go/bin/go test ./trie/archive \\
    -run '^TestArchiveStemTraceStress$' -count=1 -timeout 0 -v \\
    -args \\
      -traceStressInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
      -traceStressBaseDir='{RUN}' \\
      -traceStressOps={OPERATIONS} \\
      -traceStressStartFile=9 \\
      -traceStressBatchSize=4000 \\
      -traceStressMetricsBatches=2500 \\
      -traceStressStemMode=false \\
      -traceStressDisableArchive=false \\
      -traceStressHotLayer=mpt \\
      -traceStressShardDepth=15 \\
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
            f"(nohup '{REMOTE_LAUNCHER}' </dev/null >/dev/null 2>&1 &) && echo launched"
        )
        code, output, error = run_remote(client, launch_command, 20)
        if code != 0 or "launched" not in output:
            raise RuntimeError(f"launch failed: {error or output}")

        exit_state = "running"
        pid = None
        launcher_pid_file = f"{REMOTE_LAUNCHER}.pid"
        for _ in range(12):
            time.sleep(5)
            code, output, error = run_remote(client, f"test -f '{launcher_pid_file}' && cat '{launcher_pid_file}' || true", 15)
            if code == 0 and output.strip().isdigit():
                pid = output.strip()
                break
        for _ in range(90):
            code, output, error = run_remote(
                client,
                f"test -e '{RUN_EXIT}' && cat '{RUN_EXIT}' || true; test -e '{REMOTE_LAUNCHER}.overall.exit' && cat '{REMOTE_LAUNCHER}.overall.exit' || true",
                15,
            )
            exit_codes = [line.strip() for line in output.splitlines() if line.strip().isdigit()]
            if exit_codes and any(code != "0" for code in exit_codes):
                exit_state = "failed"
                break
            if len(exit_codes) >= 2 and all(code == "0" for code in exit_codes):
                exit_state = "success"
                break
            time.sleep(10)
        summary = {
            "status": "completed" if exit_state == "success" else exit_state,
            "stage": "B3m_D15",
            "hot_layer": "mpt",
            "shard_depth": 15,
            "operations": OPERATIONS,
            "remote_host": HOST,
            "launcher_pid": pid,
            "run": RUN,
            "log": RUN_LOG,
            "uploaded_files": len(upload_names),
            "verified_hashes": local_hashes,
        }
        print(json.dumps(summary, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
