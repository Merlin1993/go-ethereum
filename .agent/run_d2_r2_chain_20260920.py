"""D2 R2 batch: run nib3 -> nib4 -> nib5 sequentially (preflight enforces serial).

One repeat sample per tier at the same scale as R1. Prints a per-run summary
line; the final line is R2_CHAIN_DONE with per-tier exit codes.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import paramiko

REPO = Path(r"D:\go_workspace\go-ethereum")
LAUNCHER = REPO / ".agent" / "run_remote_d2_nibbles_20260918.py"
CREDS = json.loads((REPO / ".agent" / "asct_remote_servers.local.json").read_text())["asct_mpt"]
TIMEOUTS = {3: 15 * 60, 4: 40 * 60, 5: 3 * 3600}


def ssh():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(CREDS["host"], username=CREDS["ssh_user"], password=CREDS["ssh_password"],
              look_for_keys=False, allow_agent=False, timeout=20)
    return c


def wait_exit(run_dir: str, timeout: int) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        c = ssh()
        _, o, _ = c.exec_command(f"cat {run_dir}.exit 2>/dev/null || echo RUNNING", timeout=30)
        out = o.read().decode().strip()
        c.close()
        if out != "RUNNING":
            return int(out)
        time.sleep(45)
    return -1


def main() -> None:
    results = {}
    for nib in (3, 4, 5):
        env = dict(os.environ, D2_NIBBLES=str(nib))
        p = subprocess.run([sys.executable, str(LAUNCHER)], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=120)
        if p.returncode != 0:
            print(f"nib{nib} LAUNCH FAILED: {p.stdout[-400:]} {p.stderr[-400:]}", flush=True)
            results[nib] = ("launch_fail", None)
            break
        info = json.loads(p.stdout)
        run_dir = info["run_amt"]
        print(f"nib{nib} launched: {run_dir} ops={info['operations']}", flush=True)
        code = wait_exit(run_dir, TIMEOUTS[nib])
        results[nib] = (run_dir, code)
        print(f"nib{nib} exit={code}", flush=True)
        if code != 0:
            break
        time.sleep(10)  # let the server settle before the next preflight
    print("R2_CHAIN_DONE " + json.dumps({str(k): v for k, v in results.items()}), flush=True)


if __name__ == "__main__":
    main()
