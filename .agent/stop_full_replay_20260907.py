import json
import shlex
from pathlib import Path

import paramiko


HOST = "192.168.2.230"

PATTERNS = [
    r"[r]un_full_replay_",
    r"[f]ull_replay_disk_guard",
    r"[a]rchive\.test",
    r"[t]ree\.test",
    r"[g]o test ./trie/archive",
    r"[g]o test ./core/tree_test",
]


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST,
        username=server["ssh_user"],
        password=server["ssh_password"],
        look_for_keys=False,
        allow_agent=False,
    )
    runs = " ".join(shlex.quote(p) for p in PATTERNS)
    command = f"""
set -u
list_hits() {{
  for p in "$@"; do
    pgrep -f "$p" || true
  done | sort -un
}}
printf 'BEFORE\\n'
pgrep -af '[r]un_full_replay_|[f]ull_replay_disk_guard|[a]rchive\\.[t]est|[t]ree\\.[t]est|[g]o test \\./' || true
TARGETS=$(list_hits {runs})
printf 'TARGETS %s\\n' "$TARGETS"
for pid in $TARGETS; do kill -TERM "$pid" 2>/dev/null || true; done
sleep 5
TARGETS=$(list_hits {runs})
for pid in $TARGETS; do kill -KILL "$pid" 2>/dev/null || true; done
sleep 2
printf 'AFTER\\n'
pgrep -af '[r]un_full_replay_|[f]ull_replay_disk_guard|[a]rchive\\.[t]est|[t]ree\\.[t]est|[g]o test \\./' || true
printf 'MARKER\\n'
echo "$(date -Is) operator stopped run at 2.0B ops per decision 2026-09-07 (stem-mode driver; new plan targets AMT non-stem)" \\
  >> /root/asct_codex/results/trace_stress/formal_20242112513_depth16_20260907_092532.stopped.log
printf 'DISK\\n'
df -h /root/asct_codex
printf 'CSVROWS\\n'
wc -l < /root/asct_codex/results/trace_stress/formal_20242112513_depth16_20260907_092532/results/asct_trace_stress.csv
"""
    _, stdout, stderr = client.exec_command(command, get_pty=True)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    print(out)
    if err.strip():
        print("STDERR:", err)
    client.close()


if __name__ == "__main__":
    main()
