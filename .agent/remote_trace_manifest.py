import json
from pathlib import Path

import paramiko


COMMAND = r"""
python3 - <<'PY'
import json
path = '/root/asct_codex/mainnet_state_access_trace/range_10m/manifest.json'
print(json.dumps(json.load(open(path)), indent=2, ensure_ascii=False))
PY
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
