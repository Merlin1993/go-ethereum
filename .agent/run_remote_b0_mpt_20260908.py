import hashlib
import json
from datetime import datetime
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
OPERATIONS = 20_242_112_513
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
REMOTE_SOURCE = "/root/asct_codex/go-ethereum-trace"
REMOTE_WORK = "/root/asct_codex"
ROOT = f"{REMOTE_WORK}/results"
RUN = f"{ROOT}/trie_compare/B0_mpt_20242112513_{STAMP}"
REMOTE_LAUNCHER = f"{REMOTE_WORK}/run_full_replay_{STAMP}.sh"
REMOTE_GUARD = f"{REMOTE_WORK}/full_replay_disk_guard.sh"
MIN_FREE_BYTES = 8_000_000_000

# Upload the full archive package and the compare driver so the remote tree
# cannot diverge from the local code reviewed for this experiment.
FILES = [
    str(path).replace("\\", "/")
    for path in sorted(Path("trie/archive").glob("*.go"))
] + [
    ".agent/asct_trace_stress_disk_guard.sh",
    "core/tree_test/trace_compare_test.go",
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
test ! -e '{RUN}' && test ! -e '{RUN}.log' && test ! -e '{RUN}.exit'
test ! -e '{REMOTE_LAUNCHER}'
test ! -e '{REMOTE_LAUNCHER}.status.log'
free=$(df -B1 --output=avail {REMOTE_WORK} | tail -n 1 | tr -d ' ')
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
        if free_bytes < MIN_FREE_BYTES:
            raise RuntimeError(f"only {free_bytes} bytes free; require {MIN_FREE_BYTES}")
        if running_lines:
            raise RuntimeError("an existing replay process is still running:\n" + "\n".join(running_lines))

        hashes = {}
        sftp = client.open_sftp()
        try:
            for name in FILES:
                local = Path(name)
                if name == ".agent/asct_trace_stress_disk_guard.sh":
                    remote = REMOTE_GUARD
                else:
                    remote = f"{REMOTE_SOURCE}/{name}"
                remote_dir = remote.rsplit("/", 1)[0]
                try:
                    sftp.stat(remote_dir)
                except FileNotFoundError:
                    sftp.mkdir(remote_dir)
                sftp.put(str(local), remote)
                hashes[name] = sha256(local)
        finally:
            sftp.close()

        launcher = f"""#!/usr/bin/env bash
set -u
cd '{REMOTE_SOURCE}'
export GOCACHE='{REMOTE_WORK}/go-build-cache'
export GOTMPDIR='{REMOTE_WORK}/trace-tmp'
mkdir -p "$GOTMPDIR" '{ROOT}/trie_compare'
overall=0
disk_stop=0
printf '%s\\n' "$$" >'{REMOTE_LAUNCHER}.pid'

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

printf '%s B0 MPT full replay start ops=%s\\n' "$(date --iso-8601=seconds)" '{OPERATIONS}' >>'{REMOTE_LAUNCHER}.status.log'
set +e
run_stage B0 '{RUN}.exit' '{RUN}.log' \\
  /usr/local/go/bin/go test ./core/tree_test \\
    -run '^TestTrieTraceCompare$' -count=1 -timeout 0 -v \\
    -args \\
      -traceCompareInputDir=/root/asct_codex/mainnet_state_access_trace/range_10m \\
      -traceCompareEngine=mpt \\
      -traceCompareBaseDir='{RUN}' \\
      -traceCompareOps={OPERATIONS} \\
      -traceCompareStartFile=0 \\
      -traceCompareBatchSize=4000 \\
      -traceCompareMetricsBatches=2500 \\
      -traceCompareTimingSampleEvery=1000
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
        code, output, error = run_remote(client, launch_command, 10)
        if code != 0 or output.strip() != "launched":
            raise RuntimeError(f"launch failed: {error or output}")

        print(json.dumps({
            "status": "launched",
            "stage": "B0",
            "engine": "mpt",
            "operations": OPERATIONS,
            "remote_host": HOST,
            "launcher_pid_file": f"{REMOTE_LAUNCHER}.pid",
            "run": RUN,
            "launcher": REMOTE_LAUNCHER,
            "status_log": f"{REMOTE_LAUNCHER}.status.log",
            "minimum_free_bytes": MIN_FREE_BYTES,
            "free_bytes": free_bytes,
            "uploaded_files": hashes,
        }, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
