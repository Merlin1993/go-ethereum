import json
from pathlib import Path

import paramiko


RUN = "/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16"
COMMAND = r"""
set +e
printf 'EXIT='; cat /root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16.exit 2>/dev/null; echo
printf 'PROCESSES\n'; pgrep -af 'run_asct_depth16|archive.test|TestArchiveStemTraceStress' || true
printf 'FILES\n'; ls -ld /root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16* 2>/dev/null || true
printf 'LOG_TAIL\n'; tail -n 30 /root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16.log 2>/dev/null || true
printf 'CSV_LATEST\n'
python3 - <<'PY'
import csv
path='/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16/results/asct_trace_stress.csv'
try:
    with open(path, newline='') as f:
        rows=list(csv.DictReader(f))
    print('rows', len(rows))
    print(rows[-1] if rows else {})
except Exception as exc:
    print(exc)
PY
printf 'SUMMARY\n'
python3 - <<'PY'
import json
path='/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16/results/summary.json'
try:
    with open(path) as f:
        data=json.load(f)
    keys=['operations','batches','elapsed_ms','parse_ms','operations_ms','prune_launch_ms','commit_ms','db_write_ms','measured_ops_per_s','last_root','state_bytes','final_stats_ms','final_stats']
    print(json.dumps({k:data.get(k) for k in keys}, indent=2, ensure_ascii=False))
except Exception as exc:
    print(exc)
PY
printf 'STAGES\n'
python3 - <<'PY'
import csv
path='/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16/results/asct_trace_stress.csv'
with open(path, newline='') as f:
    rows=list(csv.DictReader(f))
for i in range(0, len(rows), 10):
    group=rows[i:i+10]
    wall=sum(float(r['Comparative_Wall_ms']) for r in group)
    ops=sum(int(r['Window_Operations']) for r in group)
    rss=max(int(r['RSS_Bytes']) for r in group)
    db=int(group[-1]['State_Bytes'])
    print(f"{ops} {ops/(wall/1000):.2f} {wall/1000:.3f} {rss} {db}")
PY
printf 'RING_ROWS\n'
python3 - <<'PY'
import csv
path='/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16/results/asct_trace_stress.csv'
with open(path, newline='') as f:
    rows=list(csv.DictReader(f))
for i in range(25, 28):
    if i < len(rows):
        r=rows[i]
        print({k:r[k] for k in ('Window_Start_Batch','Window_End_Batch','Total_Operations','First_Block','Last_Block')})
PY
printf 'STORAGE\n'
python3 - <<'PY'
import json
path='/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16/results/storage_breakdown_depth16.json'
try:
    with open(path) as f:
        data=json.load(f)
    print(json.dumps(data, indent=2, ensure_ascii=False))
except Exception as exc:
    print(exc)
PY
printf 'RSS_KB\n'; grep VmRSS /proc/$(pgrep -n archive.test)/status 2>/dev/null || true
"""


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    server["host"] = "192.168.2.230"
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server["host"], username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        _, stdout, stderr = client.exec_command(COMMAND, timeout=30)
        print(stdout.read().decode("utf-8", "replace"))
        error = stderr.read().decode("utf-8", "replace").strip()
        if error:
            print(error)
    finally:
        client.close()


if __name__ == "__main__":
    main()
