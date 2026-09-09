import json
from pathlib import Path

import paramiko


COMMAND = r"""
find /root/asct_codex/results/trace_stress -maxdepth 1 -type d -name 'block_pilot_depth16_*' -printf '%f\n' | sort
echo PROCS
pgrep -af 'archive.test|TestArchiveStemTraceStress|run_block_pilot' || true
"""


server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("192.168.2.230", username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
try:
    _, stdout, stderr = client.exec_command(COMMAND, timeout=30)
    print(stdout.read().decode("utf-8", "replace"))
    error = stderr.read().decode("utf-8", "replace").strip()
    if error:
        print(error)
finally:
    client.close()
