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
asct=/root/asct_codex/results/trace_stress/formal_20242112513_depth16_$stamp
mpt=/root/asct_codex/results/trie_compare/formal_20242112513_mpt_$stamp
verkle=/root/asct_codex/results/trie_compare/formal_20242112513_verkle_$stamp
printf 'LAUNCHER\n%s\n' "$launcher"
printf 'STATUS_LOG\n'; cat "$launcher.status.log" 2>/dev/null || true
printf 'DISK\n'; df -h /root/asct_codex
printf 'EXITS\n'; for suffix in '' '.storage' '.filterfp'; do if [ -e "$asct$suffix.exit" ]; then printf '%s ' "$asct$suffix.exit"; cat "$asct$suffix.exit"; fi; done; for d in "$mpt" "$verkle"; do if [ -e "$d.exit" ]; then printf '%s ' "$d.exit"; cat "$d.exit"; fi; done
printf 'PROCESSES\n'; pgrep -af '[f]ull_replay|[f]ull_replay_disk_guard|[g]o test|[a]rchive.test|[t]ree.test|[T]estArchiveStemTraceStress|[T]estTrieTraceCompare' || true
printf 'RUN_SIZES\n'; du -sh "$asct" "$mpt" "$verkle" 2>/dev/null || true
printf 'LATEST_CSV\n'
python3 - <<'PY'
import csv, glob, json, os
paths = [
    '/root/asct_codex/results/trace_stress/formal_20242112513_depth16_*/results/asct_trace_stress.csv',
    '/root/asct_codex/results/trie_compare/formal_20242112513_mpt_*/results/mpt_trace_stress.csv',
    '/root/asct_codex/results/trie_compare/formal_20242112513_verkle_*/results/verkle_trace_stress.csv',
]
for pattern in paths:
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
printf 'ASCT_TAIL\n'; tail -n 15 "$asct.log" 2>/dev/null || true
printf 'MPT_TAIL\n'; tail -n 5 "$mpt.log" 2>/dev/null || true
printf 'VERKLE_TAIL\n'; tail -n 5 "$verkle.log" 2>/dev/null || true
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
