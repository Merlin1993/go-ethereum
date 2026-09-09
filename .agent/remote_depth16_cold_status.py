import json
from pathlib import Path

import paramiko


RUN = "/root/asct_codex/results/trace_stress/archive_1b_depth16_20260820_depth16"
COMMAND = rf"""
set +e
printf 'COLD_EXIT='; cat '{RUN}.cold1b.exit' 2>/dev/null; echo
printf 'PROCESSES\n'; pgrep -af 'TestArchiveStemTraceColdHitStats|archive.test' || true
printf 'FILES\n'; ls -l '{RUN}.cold1b.log' '{RUN}/results/cold_hit_sample_30k_1b.json' 2>/dev/null || true
printf 'LOG\n'; cat '{RUN}.cold1b.log' 2>/dev/null || true
printf 'RESULT\n'; cat '{RUN}/results/cold_hit_sample_30k_1b.json' 2>/dev/null || true
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
