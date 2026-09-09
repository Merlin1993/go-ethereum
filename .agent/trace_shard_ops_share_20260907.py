import json
from pathlib import Path

import paramiko

HOST = "192.168.2.230"

PY = r"""
import gzip, json, os
d = '/root/asct_codex/mainnet_state_access_trace/range_10m'
man = json.load(open(os.path.join(d, 'manifest.json')))
print('MANIFEST_KEYS', sorted(man.keys()))
files = sorted(x for x in os.listdir(d) if x.startswith('state_access_trace_'))
rows = []
for f in files:
    size = os.path.getsize(os.path.join(d, f))
    rows.append((f, size))
tot = sum(r[1] for r in rows)
for f, size in rows:
    print('%-52s %8.2f GB %6.2f%%' % (f, size / 1e9, 100.0 * size / tot))
print('TOTAL_GB %.1f' % (tot / 1e9))
for key in ('totalOperations', 'total_operations', 'operations', 'rowCounts', 'row_counts'):
    if key in man:
        print('MANIFEST', key, json.dumps(man[key])[:4000])
"""


def main() -> None:
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
    _, stdout, stderr = client.exec_command("python3 - <<'PYEOF'\n" + PY + "\nPYEOF\n")
    print(stdout.read().decode(errors="replace"))
    err = stderr.read().decode(errors="replace")
    if err.strip():
        print("STDERR:", err[:2000])
    client.close()


if __name__ == "__main__":
    main()
