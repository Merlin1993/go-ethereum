import gzip
import hashlib
import json
from collections import Counter
from pathlib import Path

import paramiko

HOST = "192.168.2.230"

REMOTE = r"""
import gzip, hashlib, json, os
from collections import Counter
d = '/root/asct_codex/mainnet_state_access_trace/range_10m'
f = sorted(x for x in os.listdir(d) if x.startswith('state_access_trace_'))[0]
path = os.path.join(d, f)

def binkey(addr, tail):
    # mirror trie/utils/binary_tree.go binaryTreeKey with offset[0:31]=0, offset[31]=tail
    a = bytes.fromhex(addr[2:] if addr.startswith('0x') else addr)
    buf = bytearray(64)
    buf[12:32] = a
    buf[33:63] = b'\x00' * 30
    # offset[31] only lands in hash[31]; it is not part of offset[0:31]
    h = bytearray(hashlib.sha256(bytes(buf)).digest())
    h[31] = tail
    return bytes(h)

def top16(key):
    return (key[0] << 8) | key[1]

n = 0
shards = Counter()
prefix248 = Counter()
account_keys = 0
seen = set()
limit = 400000
with gzip.open(path, 'rt') as fh:
    header = fh.readline().strip().split(',')
    ia, iso, io = header.index('address'), header.index('slot_or_chunk'), header.index('object_type')
    for line in fh:
        p = line.rstrip('\n').split(',')
        if len(p) <= max(ia, iso, io):
            continue
        try:
            slot = int(p[iso], 16) if p[iso].startswith('0x') else int(p[iso] or 0)
        except ValueError:
            continue
        if p[io] == 'storage' and slot >= 64:
            continue  # those use a different offset prefix
        tail = 0
        if p[io] == 'account':
            tail = 0
        elif p[io] == 'storage':
            tail = 64 + slot
        elif p[io] == 'code':
            tail = 128 + slot
        else:
            continue
        k = binkey(p[ia], tail & 0xFF)
        if k in seen:
            continue
        seen.add(k)
        shards[top16(k)] += 1
        prefix248[k[:31].hex()] += 1
        n += 1
        if n >= limit:
            break
print(json.dumps({
  'file': f,
  'distinct_keys': n,
  'distinct_shards_top16': len(shards),
  'expected_shards_bernoulli': round(65536 * (1 - (1 - 1/65536) ** n)),
  'max_keys_per_shard': shards.most_common(1)[0][1],
  'shard_top10': shards.most_common(10),
  'distinct_248_prefixes': len(prefix248),
  'max_keys_per_prefix': prefix248.most_common(1)[0][1],
}, indent=2))
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
    _, stdout, stderr = client.exec_command("python3 - <<'PYEOF'\n" + REMOTE + "\nPYEOF\n")
    print(stdout.read().decode(errors="replace"))
    err = stderr.read().decode(errors="replace")
    if err.strip():
        print("STDERR:", err[:2000])
    client.close()


if __name__ == "__main__":
    main()
