import argparse
import hashlib
import json
import shlex
from datetime import datetime
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
REMOTE_SOURCE = "/root/asct_codex/go-ethereum-trace"
REMOTE_TRACE = "/root/asct_codex/mainnet_state_access_trace/range_10m"
REMOTE_RESULTS = "/root/asct_codex/results/archive_period"
FILES = [
    "trie/archive/archive.go",
    "trie/archive/correctness_test.go",
    "trie/archive/trace_archive_period_test.go",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops", type=int, required=True)
    args = parser.parse_args()
    if args.ops <= 0:
        raise SystemExit("--ops must be positive")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = f"{REMOTE_RESULTS}/period_{args.ops}_{stamp}.json"
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
        hashes = {}
        sftp = client.open_sftp()
        try:
            for name in FILES:
                local = Path(name)
                sftp.put(str(local), f"{REMOTE_SOURCE}/{name}")
                hashes[name] = sha256(local)
        finally:
            sftp.close()

        command = " ".join(
            [
                "set -e;",
                f"cd {shlex.quote(REMOTE_SOURCE)};",
                f"mkdir -p {shlex.quote(REMOTE_RESULTS)};",
                "export GOCACHE=/root/asct_codex/go-build-cache;",
                "export GOTMPDIR=/root/asct_codex/trace-tmp;",
                "mkdir -p \"$GOTMPDIR\";",
                "/usr/bin/time -v /usr/local/go/bin/go test ./trie/archive",
                "-run '^TestTraceArchivePeriodSimulation$' -count=1 -timeout 0 -v",
                "-args",
                f"-traceArchivePeriodInputDir={shlex.quote(REMOTE_TRACE)}",
                f"-traceArchivePeriodOutput={shlex.quote(output)}",
                f"-traceArchivePeriodOps={args.ops}",
                "-traceArchivePeriodBatchSize=4000",
                "-traceArchivePeriodShardDepth=16",
                "-traceArchivePeriodPruneBatches=1,2,4,8",
                f"-traceArchivePeriodWindowOps={min(args.ops, 100_000_000)}",
                f"-traceArchivePeriodCheckpointOps={min(args.ops, 100_000_000)}",
            ]
        )
        _, stdout, stderr = client.exec_command(command, timeout=7200)
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        status = stdout.channel.recv_exit_status()
        print(out)
        if err:
            print(err)
        print(json.dumps({"status": status, "output": output, "files": hashes}, indent=2))
        if status != 0:
            raise SystemExit(status)
    finally:
        client.close()


if __name__ == "__main__":
    main()
