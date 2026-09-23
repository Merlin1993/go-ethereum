"""Wait for the R2 chain's nib5 run to finish, then launch the FAIR nib5:
same depth-5 config but D2_START_FILE=0 (all 10 shards, ~34B ops available)
so a full archive round (4.19B ops) can actually complete. Serial discipline:
the R2 nib5 run must exit before the fair run launches (launcher preflight
also enforces this). Fetch + gate summary at the end.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

import paramiko

REPO = Path(r"D:\go_workspace\go-ethereum")
CREDS = json.loads((REPO / ".agent" / "asct_remote_servers.local.json").read_text())["asct_mpt"]
R2_NIB5 = "/root/asct_codex/results/trace_stress/D2_amt_nib5_6291456000_fl25_20260923_090528"


def ssh():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(CREDS["host"], username=CREDS["ssh_user"], password=CREDS["ssh_password"],
              look_for_keys=False, allow_agent=False, timeout=20)
    return c


def wait_exit(run_dir: str, timeout: int) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = ssh()
            _, o, _ = c.exec_command(f"cat {run_dir}.exit 2>/dev/null || echo RUNNING", timeout=30)
            out = o.read().decode().strip()
            c.close()
            if out != "RUNNING":
                return int(out)
        except Exception as ex:
            print("probe error:", ex, flush=True)
        time.sleep(60)
    return -1


def main() -> None:
    print("waiting for R2 nib5 to finish...", flush=True)
    code = wait_exit(R2_NIB5, 4 * 3600)
    print(f"R2 nib5 exit={code}; settling 60s before fair launch", flush=True)
    time.sleep(60)

    import os
    env = dict(os.environ, D2_NIBBLES="5", D2_START_FILE="0")
    p = subprocess.run([sys.executable, str(REPO / ".agent" / "run_remote_d2_nibbles_20260918.py")],
                       cwd=REPO, env=env, capture_output=True, text=True, timeout=180)
    if p.returncode != 0:
        print("FAIR LAUNCH FAILED:", p.stdout[-400:], p.stderr[-400:], flush=True)
        sys.exit(1)
    info = json.loads(p.stdout)
    run_dir = info["run_amt"]
    print(f"fair nib5 launched: {run_dir}", flush=True)

    code = wait_exit(run_dir, 7 * 3600)
    print(f"fair nib5 exit={code}", flush=True)
    if code == 0:
        name = run_dir.rsplit("/", 1)[-1]
        f = subprocess.run([sys.executable, str(REPO / ".agent" / "fetch_d2_20260918.py"), name],
                           cwd=REPO, capture_output=True, text=True, timeout=300)
        print(f.stdout[-1500:], flush=True)
    print("FAIR_NIB5_DONE", flush=True)


if __name__ == "__main__":
    main()
