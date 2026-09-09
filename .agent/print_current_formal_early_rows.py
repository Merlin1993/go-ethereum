import csv
import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
RUN = "/root/asct_codex/results/trace_stress/formal_20242112513_depth16_20260907_092532"

COMMAND = f"""
set -e
python3 - <<'PY'
import csv, json
path = "{RUN}/results/asct_trace_stress.csv"
rows = list(csv.DictReader(open(path)))
keys = [
    "Total_Operations", "Total_Batches", "First_Block", "Last_Block",
    "Operations_ms", "Commit_ms", "DB_Write_ms", "Comparative_Wall_ms",
    "Operations_Per_Sec", "State_Bytes", "RSS_Bytes", "Heap_Bytes",
    "StemCache_Entries", "Hot_Read_Hit_Rate", "Cold_Maintenance_ms",
    "Archive_Filter_Lookups", "Archive_Filter_Negatives",
    "Archive_Filter_Positives", "Archive_Filter_False_Positives",
    "Runtime_Filter_FPR", "Comparative_Batch_P50_ms",
    "Comparative_Batch_P95_ms", "Comparative_Batch_P99_ms",
]
print(json.dumps([{{key: row.get(key) for key in keys}} for row in rows], indent=2))
PY
"""


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        _, stdout, stderr = client.exec_command(COMMAND, timeout=30)
        print(stdout.read().decode("utf-8", "replace"))
        error = stderr.read().decode("utf-8", "replace").strip()
        if error:
            raise RuntimeError(error)
    finally:
        client.close()


if __name__ == "__main__":
    main()
