import argparse
import json
from pathlib import Path

import paramiko


parser = argparse.ArgumentParser()
parser.add_argument("run")
args = parser.parse_args()

command = r"""
set +e
printf 'EXIT='; cat '__RUN__.exit' 2>/dev/null; echo
printf 'PROCESSES\n'; pgrep -af 'archive.test|TestArchiveStemTraceStress' || true
printf 'LOG\n'; tail -n 40 '__RUN__.log' 2>/dev/null || true
printf 'SUMMARY\n'
python3 - <<'PY'
import json
path = r'__RUN__/results/summary.json'
try:
    data = json.load(open(path))
    keys = [
        'operations', 'batches', 'first_block', 'last_block', 'block_count',
        'operations_ms', 'commit_ms', 'db_write_ms', 'measured_ops_per_s',
        'access_sample_every', 'access_sample_ms', 'access_sample',
        'cold_maintenance_ms',
        'hot_read_hit_rate', 'read_archive_share'
    ]
    print(json.dumps({key: data.get(key) for key in keys}, indent=2))
except Exception as exc:
    print(exc)
PY
printf 'CSV\n'
python3 - <<'PY'
import json
import csv
path = r'__RUN__/results/asct_trace_stress.csv'
try:
    rows = list(csv.DictReader(open(path)))
    keys = [
        'Total_Operations', 'First_Block', 'Last_Block', 'Access_Samples',
        'Sampled_Read_Hot', 'Sampled_Read_Archived', 'Sampled_Read_Missing',
        'Hot_Read_Hit_Rate', 'Access_Sample_ms', 'Cold_Maintenance_ms', 'Operations_ms',
        'Commit_ms', 'DB_Write_ms', 'Comparative_Wall_ms'
    ]
    print(json.dumps([{key: row.get(key) for key in keys} for row in rows[-3:]], indent=2))
except Exception as exc:
    print(exc)
PY
""".replace("__RUN__", args.run)

server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("192.168.2.230", username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
try:
    _, stdout, stderr = client.exec_command(command, timeout=30)
    print(stdout.read().decode("utf-8", "replace"))
    error = stderr.read().decode("utf-8", "replace").strip()
    if error:
        print(error)
finally:
    client.close()
