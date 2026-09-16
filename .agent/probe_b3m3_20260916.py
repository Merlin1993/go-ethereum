"""One-shot probe: has B3m3 landed on titanide, is it running, where is it?"""
import json
from pathlib import Path

import paramiko

creds = json.loads(Path(r"D:/go_workspace/go-ethereum/.agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("192.168.2.230", username=creds["ssh_user"], password=creds["ssh_password"],
               look_for_keys=False, allow_agent=False, timeout=20)
cmd = (
    "ls -dt /root/asct_codex/results/trace_stress/B3m3_t3a_*/ 2>/dev/null | head -1; "
    "ps -eo pid=,etime=,args= | grep archive.test | grep -v grep | head -2; "
    "df -B1 --output=avail /root | tail -1; free -m | awk '/Mem/{print $7}'; "
    "for d in $(ls -dt /root/asct_codex/results/trace_stress/B3m3_t3a_*/ 2>/dev/null | head -1); do "
    "  cat $d/run_status.json 2>/dev/null; tail -3 $d/*.log 2>/dev/null | tail -3; "
    "  wc -l < $d/results/asct_trace_stress.csv 2>/dev/null; done"
)
_, o, e = client.exec_command(cmd, timeout=30)
print(o.read().decode())
err = e.read().decode()
if err:
    print("STDERR:", err[:400])
client.close()
