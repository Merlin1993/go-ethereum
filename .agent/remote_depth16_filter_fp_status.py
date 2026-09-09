import json
from pathlib import Path

import paramiko


RUN = "/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16"
COMMAND = rf"""
set +e
printf 'EXIT='; cat '{RUN}.filterfp.exit' 2>/dev/null; echo
printf 'PROCESSES\n'; pgrep -af 'TestArchiveStemTraceFilterFPStats|archive.test' || true
printf 'FILES\n'; ls -l '{RUN}.filterfp.log' '{RUN}/results/filter_fp_20_per_bucket.json' 2>/dev/null || true
printf 'LOG\n'; tail -n 40 '{RUN}.filterfp.log' 2>/dev/null || true
printf 'RESULT\n'; cat '{RUN}/results/filter_fp_20_per_bucket.json' 2>/dev/null || true
printf 'RSS_KB\n'; grep VmRSS /proc/$(pgrep -n archive.test)/status 2>/dev/null || true
"""


def main() -> None:
    server = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    server["host"] = "192.168.2.230"
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server["host"], username=server["ssh_user"], password=server["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        _, stdout, stderr = client.exec_command(COMMAND, timeout=30)
        print(stdout.read().decode("utf-8", "replace"))
        error = stderr.read().decode("utf-8", "replace").strip()
        if error:
            print(error)
    finally:
        client.close()


if __name__ == "__main__":
    main()
