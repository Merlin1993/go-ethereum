import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"


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
    command = r"""
set -e
printf 'HOST\n'; hostname
printf 'MACHINE\n'; printf 'cores=%s\n' "$(nproc)"; awk '/MemTotal/ {printf "mem_gib=%.0f", $2/1048576}' /proc/meminfo; echo
printf 'GO\n'; /usr/local/go/bin/go version
printf 'DISK\n'; df -hT; lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINTS
printf 'TRACE\n'; find /root/asct_codex/mainnet_state_access_trace/range_10m -maxdepth 1 -name 'state_access_trace_*.csv.gz' -printf '%f\n' | sort
printf 'TRACE_BYTES\n'; du -sb /root/asct_codex/mainnet_state_access_trace/range_10m
printf 'RESULT_TOP\n'; du -h --max-depth=2 /root/asct_codex/results 2>/dev/null | sort -h | tail -n 30
printf 'SOURCE\n'; ls -ld /root/asct_codex/go-ethereum-trace; stat -c '%y %s %n' /root/asct_codex/go-ethereum-trace/trie/archive/trace_stress_test.go /root/asct_codex/go-ethereum-trace/core/tree_test/trace_compare_test.go
printf 'PROCESSES\n'; pgrep -af 'go test|archive.test|tree.test|TestArchiveStemTraceStress|TestTrieTraceCompare' || true
"""
    try:
        _, stdout, stderr = client.exec_command(command, timeout=60)
        output = stdout.read().decode(errors="replace")
        error = stderr.read().decode(errors="replace")
        status = stdout.channel.recv_exit_status()
        if status != 0:
            raise RuntimeError(error or f"preflight exited {status}")
        print(output)
        if error:
            print("STDERR")
            print(error)
    finally:
        client.close()


if __name__ == "__main__":
    main()
