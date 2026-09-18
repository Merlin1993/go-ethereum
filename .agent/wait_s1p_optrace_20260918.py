"""Block until the S1' op-trace launcher finishes, then print final status.

Polls <launcher>.overall.exit (written as the launcher's last act), tracking
CSV row growth of both the main metrics and the new op_trace.csv. Prints one
line per poll so the wait is observable.
"""
import json
import sys
import time
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
REMOTE_WORK = "/root/asct_codex"


def main() -> None:
    stamp = sys.argv[1]
    max_minutes = float(sys.argv[2]) if len(sys.argv) > 2 else 120.0
    launcher = f"{REMOTE_WORK}/run_s1p_optrace_{stamp}.sh"
    run_dir = f"{REMOTE_WORK}/results/trace_stress/S1p_amt_optrace_nib4_393216000_{stamp}"
    credentials = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]

    deadline = time.time() + max_minutes * 60
    while time.time() < deadline:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                HOST,
                username=credentials["ssh_user"],
                password=credentials["ssh_password"],
                look_for_keys=False,
                allow_agent=False,
            )
            command = f"""
printf 'STATUS '; tail -n 1 '{launcher}.status.log' 2>/dev/null || echo no-status-yet
printf 'OVERALL '; cat '{launcher}.overall.exit' 2>/dev/null || echo running
printf 'ROWS main=%s optrace=%s\\n' \\
  "$(( $(wc -l < '{run_dir}/results/asct_trace_stress.csv' 2>/dev/null || echo 1) - 1 ))" \\
  "$(( $(wc -l < '{run_dir}/results/op_trace.csv' 2>/dev/null || echo 1) - 1 ))"
"""
            _, stdout, _ = client.exec_command(command, timeout=60)
            out = stdout.read().decode(errors="replace").strip()
        finally:
            client.close()

        print(out, flush=True)
        if "running" not in out:
            print("FINISHED", flush=True)
            return
        time.sleep(180)

    print("TIMEOUT: launcher still running after %.0f minutes" % max_minutes, flush=True)
    sys.exit(1)


if __name__ == "__main__":
    main()
