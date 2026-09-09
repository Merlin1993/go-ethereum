import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
ROOT = "/root/asct_codex"

COMMAND = r"""
set -e
printf '=== DISK ===\n'
df -h /root/asct_codex

printf '\n=== ACTIVE PROCESSES ===\n'
ps -eo pid=,ppid=,stat=,etime=,comm=,args= | awk '$5 == "go" || $5 == "archive.test" || $5 == "tree.test" || $6 ~ /run_full_replay_[0-9]+_[0-9]+[.]sh$/ || $6 ~ /full_replay_disk_guard[.]sh$/ {print}'

printf '\n=== TOP LEVEL BY SIZE ===\n'
du -xh --max-depth=1 /root/asct_codex 2>/dev/null | sort -hr

printf '\n=== TOP LEVEL FILES ===\n'
find /root/asct_codex -maxdepth 1 -type f -printf '%10s  %TY-%Tm-%Td %TH:%TM  %f\n' | sort -nr

printf '\n=== RESULT TREES ===\n'
printf '\n-- all direct children under results --\n'
du -xh --max-depth=1 /root/asct_codex/results 2>/dev/null | sort -hr
for tree in trace_stress trie_compare; do
  printf '\n-- %s --\n' "$tree"
  du -sh /root/asct_codex/results/"$tree"/* 2>/dev/null | sort -hr || true
done

printf '\n=== REBUILDABLE CACHES/TMP ===\n'
for path in /root/asct_codex/go-build-cache /root/asct_codex/trace-tmp /root/.cache/go-build /root/go/pkg/mod; do
  if [ -e "$path" ]; then du -shx "$path"; fi
done

printf '\n=== SOURCE AND TRACE DATA ===\n'
for path in /root/asct_codex/go-ethereum-trace /root/asct_codex/mainnet_state_access_trace; do
  if [ -e "$path" ]; then du -shx "$path"; fi
done
printf 'trace shards: '
find /root/asct_codex/mainnet_state_access_trace/range_10m -maxdepth 1 -type f 2>/dev/null | wc -l

printf '\n=== LATEST RUN STATUS ===\n'
launcher=$(find /root/asct_codex -maxdepth 1 -type f -name 'run_full_replay_*.sh' -printf '%T@ %p\n' | sort -nr | head -n 1 | cut -d' ' -f2-)
printf 'launcher=%s\n' "$launcher"
cat "$launcher.status.log" 2>/dev/null || true
if [ -e "$launcher.pid" ]; then printf 'launcher_pid='; cat "$launcher.pid"; fi
printf '\nlatest asct csv/log tails:\n'
asct=/root/asct_codex/results/trace_stress/formal_20242112513_depth16_${launcher##*run_full_replay_}
asct=${asct%.sh}
tail -n 3 "$asct.log" 2>/dev/null || true
tail -n 2 "$asct/results/asct_trace_stress.csv" 2>/dev/null || true

printf '\n=== HUGE HISTORICAL TOP LEVEL RUNS ===\n'
for base in \
  /root/asct_codex/asct_path_destructive_depth20_1p2b_batch1000_20260620 \
  /root/asct_codex/asct_path_destructive_depth20_1p2b_batch1000_stats100m_20260621_1530; do
  printf '\n-- %s --\n' "$base"
  du -xh --max-depth=2 "$base" 2>/dev/null | sort -hr | head -n 20
  find "$base" -maxdepth 3 -type f \( -name 'summary.json' -o -name 'run_status.json' -o -name '*.csv' -o -name '*.log' \) -printf '%p\n' | head -n 30
  for leaf in run_status.json results/summary.json; do
    if [ -e "$base/$leaf" ]; then printf '%s: ' "$leaf"; cat "$base/$leaf"; printf '\n'; fi
  done
done
"""


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
        _, stdout, stderr = client.exec_command(COMMAND, timeout=120)
        output = stdout.read().decode(errors="replace")
        error = stderr.read().decode(errors="replace")
        print(output)
        if error:
            print("STDERR")
            print(error)
        if stdout.channel.recv_exit_status() != 0:
            raise SystemExit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
