"""Wait for B3m3 run completion: poll RUN_EXIT on titanide, print PASS/FAIL on exit."""
import json
import sys
import time
from pathlib import Path

import paramiko

GLOB = "/root/asct_codex/results/trace_stress/B3m3_t3a_*/"
RUN_SH = f"$(ls -dt {GLOB} 2>/dev/null | head -1)"
creds = json.loads(Path(r"D:/go_workspace/go-ethereum/.agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("192.168.2.230", username=creds["ssh_user"], password=creds["ssh_password"],
               look_for_keys=False, allow_agent=False, timeout=20)

deadline = time.time() + 3 * 3600
while time.time() < deadline:
    _, o, _ = client.exec_command(
        f'RUN={RUN_SH}; echo "EXIT=$(cat $RUN.exit 2>/dev/null)"; echo "ROWS=$(wc -l < $RUN/results/asct_trace_stress.csv 2>/dev/null || echo NA)"',
        timeout=20)
    fields = {}
    for line in o.read().decode().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key.strip()] = value.strip()
    exit_code = fields.get("EXIT") or None
    rows = fields.get("ROWS", "?")
    if exit_code is not None:
        print(f"B3m3 finished exit={exit_code} csv_rows={rows}")
        _, o, _ = client.exec_command(f'RUN={RUN_SH}; tail -n 15 "$RUN.log"', timeout=20)
        print(o.read().decode())
        sys.exit(0 if exit_code == "0" else 1)
    print(f"[{time.strftime('%H:%M:%S')}] running... csv_rows={rows}", flush=True)
    time.sleep(120)
print("TIMEOUT 3h — checking state manually")
_, o, _ = client.exec_command(f'RUN={RUN_SH}; tail -n 20 "$RUN.log"; ps -eo pid=,args= | grep archive.test | grep -v grep | head -2', timeout=20)
print(o.read().decode())
client.close()
