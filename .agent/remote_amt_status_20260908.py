import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
    command = r"""
set -e
launcher=$(find /root/asct_codex -maxdepth 1 -type f -name 'run_full_replay_*.sh' -printf '%T@ %p\n' | sort -nr | head -n 1 | cut -d' ' -f2-)
stamp=${launcher##*run_full_replay_}; stamp=${stamp%.sh}
printf 'LAUNCHER\n%s\n' "$launcher"
printf 'STAMP\n%s\n' "$stamp"
printf 'STATUS_LOG\n'; cat "$launcher.status.log" 2>/dev/null || true
printf 'DISK\n'; df -h /root/asct_codex
printf 'PROCESSES\n'; pgrep -af '[f]ull_replay|[f]ull_replay_disk_guard|[g]o test|[a]rchive.test|[t]ree.test|[T]estArchiveStemTraceStress|[T]estTrieTraceCompare' || true
printf 'RUN_DIRS\n'
find /root/asct_codex/results -maxdepth 3 -type d -name "*_$stamp" -print 2>/dev/null | sort
printf 'EXITS\n'
find /root/asct_codex/results -maxdepth 4 -type f -name "*_$stamp.exit" -print -exec cat {} \; 2>/dev/null || true
printf 'LATEST_CSV\n'
python3 - <<'PY'
import csv, glob, json, os
patterns = [
    '/root/asct_codex/results/trie_compare/B*_mpt_*/results/mpt_trace_stress.csv',
    '/root/asct_codex/results/trie_compare/B*_verkle_*/results/verkle_trace_stress.csv',
    '/root/asct_codex/results/trace_stress/B*_amt_*/results/asct_trace_stress.csv',
]
for pattern in patterns:
    matches = sorted((p for p in glob.glob(pattern) if os.path.exists(p)), key=os.path.getmtime)
    if not matches:
        continue
    path = matches[-1]
    rows = list(csv.DictReader(open(path)))
    print(path, 'rows=', len(rows))
    if rows:
        wanted = ['Total_Batches','Total_Operations','First_Block','Last_Block','Window_Operations','Operations_ms','Commit_ms','Root_ms','DB_Write_ms','Operations_Per_Sec','State_Bytes','Hot_Read_Hit_Rate','Cold_Maintenance_ms','Archive_Filter_Lookups','Archive_Filter_Positives','Archive_Filter_False_Positives','Runtime_Filter_FPR']
        print(json.dumps({key: rows[-1].get(key) for key in wanted if key in rows[-1]}, indent=2))
PY
printf 'LOG_TAILS\n'
for log in $(find /root/asct_codex/results -maxdepth 4 -type f -name "*_$stamp.log" 2>/dev/null | sort); do
  printf '=== %s ===\n' "$log"
  tail -n 10 "$log" 2>/dev/null || true
done
"""
    try:
        _, stdout, stderr = client.exec_command(command, timeout=60)
        output = stdout.read().decode(errors="replace")
        error = stderr.read().decode(errors="replace")
        print(output)
        if error:
            print("STDERR")
            print(error)
    finally:
        client.close()


if __name__ == "__main__":
    main()
