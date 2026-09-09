import argparse
import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops", type=int, required=True)
    args = parser.parse_args()

    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST,
        username=server["ssh_user"],
        password=server["ssh_password"],
        look_for_keys=False,
        allow_agent=False,
    )
    command = f"""
python3 - <<'PY'
import glob
import json
import os

matches = sorted(
    glob.glob('/root/asct_codex/results/archive_period/period_{args.ops}_*.json'),
    key=os.path.getmtime,
)
if not matches:
    raise SystemExit('no matching result')
path = matches[-1]
data = json.load(open(path))
result = {{
    'path': path,
    'operations': data['operations'],
    'elapsed_ms': data['elapsed_ms'],
    'existing_reads': data['existing_reads'],
    'missing_reads': data['missing_reads'],
    'unique_stems_ever_seen': data['unique_stems_ever_seen'],
    'variants': data['variants'],
    'windows': data['windows'],
    'checkpoints': data['checkpoints'],
}}
print(json.dumps(result, indent=2))
PY
"""
    try:
        _, stdout, stderr = client.exec_command(command, timeout=60)
        output = stdout.read().decode("utf-8", "replace")
        error = stderr.read().decode("utf-8", "replace")
        status = stdout.channel.recv_exit_status()
        print(output)
        if error:
            print(error)
        if status != 0:
            raise SystemExit(status)
    finally:
        client.close()


if __name__ == "__main__":
    main()
