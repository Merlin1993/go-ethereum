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
asct=$(find /root/asct_codex/results/trace_stress -maxdepth 1 -type d -name 'opcount_pilot_depth16_3m_*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2-)
mpt=$(find /root/asct_codex/results/trie_compare -maxdepth 1 -type d -name 'opcount_pilot_mpt_3m_*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2-)
verkle=$(find /root/asct_codex/results/trie_compare -maxdepth 1 -type d -name 'opcount_pilot_verkle_3m_*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2-)
printf 'RUNS\n%s\n%s\n%s\n' "$asct" "$mpt" "$verkle"
printf 'EXITS\n'; for d in "$asct" "$mpt" "$verkle"; do [ -z "$d" ] && continue; printf '%s ' "$d"; cat "$d.exit" 2>/dev/null || echo RUNNING; done
printf 'PROCESSES\n'; pgrep -af 'run_opcount_pilot|archive.test|tree.test|TestArchiveStemTraceStress|TestTrieTraceCompare' || true
printf 'SUMMARIES\n'
python3 - <<'PY'
import csv, glob, json, os
runs = [
    *sorted((path for path in glob.glob('/root/asct_codex/results/trace_stress/opcount_pilot_depth16_3m_*') if os.path.isdir(path)), key=os.path.getmtime),
    *sorted((path for path in glob.glob('/root/asct_codex/results/trie_compare/opcount_pilot_*_3m_*') if os.path.isdir(path)), key=os.path.getmtime),
]
for run in runs[-6:]:
    path = os.path.join(run, 'results', 'summary.json')
    print(run)
    try:
        data = json.load(open(path))
        keys = ['status','operations','batches','counts','operations_ms','root_ms','commit_ms','db_write_ms','measured_ops_per_s','access_sample','access_average_ns','hot_read_hit_rate','archive_filter_runtime','cold_maintenance_ms','cold_maintenance_estimate_ms','timing_sample','timing_average_ns']
        print(json.dumps({key: data.get(key) for key in keys if key in data}, ensure_ascii=False, indent=2))
    except Exception as exc:
        print(exc)
    csv_path = os.path.join(run, 'results', 'asct_trace_stress.csv')
    try:
        rows = list(csv.DictReader(open(csv_path)))
        keys = ['Total_Operations','Total_Batches','Access_Samples','Sampled_Read_Hot','Sampled_Read_Archived','Sampled_Read_Missing','Sampled_Write_Hot','Sampled_Write_Archived','Sampled_Write_Missing','Hot_Read_Hit_Rate','Cold_Maintenance_ms','Archive_Filter_Lookups','Archive_Filter_Negatives','Archive_Filter_Positives','Archive_Filter_False_Positives','Runtime_Filter_FPR']
        if rows:
            print(json.dumps({key: rows[-1].get(key) for key in keys}, indent=2))
    except Exception:
        pass
PY
printf 'TAILS\n'; for d in "$asct" "$mpt" "$verkle"; do [ -z "$d" ] && continue; echo "--- $d.log"; tail -n 8 "$d.log" 2>/dev/null || true; done
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
