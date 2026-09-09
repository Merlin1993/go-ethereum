import json
from pathlib import Path

import paramiko


RUN = "/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16"
OUTPUT = f"{RUN}/results/storage_breakdown_depth16.json"
ROOT = "0xf5d80f2742e168723da2c7884c6f4d821859f8f919c582ef55be22f2128de436"


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    server["host"] = "192.168.2.230"
    command = f"""cd /root/asct_codex/go-ethereum-trace && \
export GOCACHE=/root/asct_codex/go-build-cache && \
export GOTMPDIR=/root/asct_codex/trace-tmp && \
test ! -e '{OUTPUT}' && \
/usr/local/go/bin/go test ./trie/archive \
  -run '^TestArchiveStemTraceStorageStats$' \
  -count=1 -timeout 0 -v \
  -args \
    -traceStatsBaseDir='{RUN}' \
    -traceStatsRoot='{ROOT}' \
    -traceStatsOutput='{OUTPUT}' \
    -traceStressShardDepth=16"""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server["host"], username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        _, stdout, stderr = client.exec_command(command, timeout=900)
        output = stdout.read().decode("utf-8", "replace")
        error = stderr.read().decode("utf-8", "replace")
        status = stdout.channel.recv_exit_status()
        print(output[-12000:])
        if error:
            print(error, file=sys.stderr)
        if status != 0:
            raise RuntimeError(f"storage scan exited {status}")
    finally:
        client.close()


if __name__ == "__main__":
    import sys

    main()
