"""Poll a D2 remote run until its .exit file appears, then print exit + log tail."""
import json, sys, time, paramiko

RUN = sys.argv[1]
TIMEOUT_MIN = int(sys.argv[2]) if len(sys.argv) > 2 else 30

creds = json.load(open(r"D:\go_workspace\go-ethereum\.agent\asct_remote_servers.local.json"))["asct_mpt"]
base = f"/root/asct_codex/results/trace_stress/{RUN}"
deadline = time.time() + TIMEOUT_MIN * 60
while time.time() < deadline:
    try:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        c.connect(creds["host"], username=creds["ssh_user"], password=creds["ssh_password"], timeout=20)
        _, o, _ = c.exec_command(f"cat {base}.exit 2>/dev/null || echo RUNNING", timeout=30)
        out = o.read().decode().strip()
        if out != "RUNNING":
            _, o2, _ = c.exec_command(f"tail -4 {base}.log | cut -c1-160", timeout=30)
            print(f"EXIT={out}")
            print(o2.read().decode().strip())
            c.close()
            sys.exit(0 if out == "0" else 1)
        # progress heartbeat: last metrics window
        _, o3, _ = c.exec_command(f"tail -1 {base}.log 2>/dev/null | cut -c1-120", timeout=30)
        print("progress:", o3.read().decode().strip(), flush=True)
        c.close()
    except Exception as ex:
        print("probe error:", ex, flush=True)
    time.sleep(60)
print("TIMEOUT: still running after", TIMEOUT_MIN, "min")
sys.exit(2)
