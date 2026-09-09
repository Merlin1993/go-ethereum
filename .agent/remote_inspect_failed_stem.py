import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
SOURCE = "/root/asct_codex/go-ethereum-trace"
RUN = "/root/asct_codex/results/trace_stress/formal_20242112513_depth16_20260904_161556"
STEM = "06713dd340d0eb6b2ddc3b0a621090e9bd0ca0c327a49974d10ccdbc7dfa50"


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
    try:
        sftp = client.open_sftp()
        try:
            sftp.put("trie/archive/trace_stress_test.go", f"{SOURCE}/trie/archive/trace_stress_test.go")
        finally:
            sftp.close()
        command = f"""
set -e
cd '{SOURCE}'
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp
/usr/local/go/bin/go test ./trie/archive \\
  -run '^TestArchiveStemTraceInspectStem$' -count=1 -timeout 10m -v \\
  -args \\
    -traceInspectBaseDir='{RUN}' \\
    -traceInspectStem='{STEM}' \\
    -traceStressShardDepth=16
"""
        _, stdout, stderr = client.exec_command(command, timeout=900)
        output = stdout.read().decode("utf-8", "replace")
        error = stderr.read().decode("utf-8", "replace")
        code = stdout.channel.recv_exit_status()
        print(output)
        if error:
            print(error)
        if code != 0:
            raise SystemExit(code)
    finally:
        client.close()


if __name__ == "__main__":
    main()
