"""Fetch B3m2 (T1+T2 recalibration) artifacts and pair-compare vs B0b / B2r.

r = AMT/MPT window-by-window (same budget 196,608,000 ops, file 9, fresh
start). Post-T1 comparability rule: old B3m hid its sync flush inside
Commit_ms; new runs must be judged on Commit_ms + DB_Write_ms together.
"""
import csv
import json
import paramiko
from pathlib import Path

OUT = Path(r"D:/go_workspace/go-ethereum/.agent/cmp_20260916")
LOCAL_CSV = OUT / "b3m2_t1t2.csv"

creds = json.loads(Path(r"D:/go_workspace/go-ethereum/.agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("192.168.2.230", username=creds["ssh_user"], password=creds["ssh_password"],
               look_for_keys=False, allow_agent=False, timeout=20)

_, o, _ = client.exec_command("ls -d /root/asct_codex/results/trace_stress/B3m2_t1t2_196608000_D15_20260916_* | head -1", timeout=30)
base = o.read().decode().strip()
if not base:
    raise SystemExit("B3m2 run dir not found yet")

sftp = client.open_sftp()
LOCAL_CSV.parent.mkdir(parents=True, exist_ok=True)
sftp.get(base + "/results/asct_trace_stress.csv", str(LOCAL_CSV))
sftp.get(base + "/metadata.json", str(OUT / "b3m2_metadata.json"))
sftp.close()
_, o, _ = client.exec_command(f"ls {base}; tail -n 5 {base}.log; cat {base}.exit 2>/dev/null || echo NO_EXIT_FILE", timeout=30)
print(o.read().decode())
client.close()


def rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


amt = rows(LOCAL_CSV)
mpt = rows(OUT / "b0b_mpt_trace_stress.csv")
bin2 = rows(OUT / "b2r.csv")
print(f"windows: b3m2={len(amt)} b0b={len(mpt)} b2r={len(bin2)}")

ratios, shares = [], []
for a, m in zip(amt, mpt):
    ra = float(a["Operations_Per_Sec"]) / float(m["Operations_Per_Sec"])
    ratios.append(ra)
    wall = float(a["Comparative_Wall_ms"])
    commit_share = (float(a["Commit_ms"]) + float(a["DB_Write_ms"])) / wall * 100 if wall else 0
    op_share = float(a["Operations_ms"]) / wall * 100 if wall else 0
    shares.append((commit_share, op_share))
    print(f"win {a['Total_Batches']:>6}  B3m2 {float(a['Operations_Per_Sec']):>12,.0f}  B0b {float(m['Operations_Per_Sec']):>12,.0f}  r={ra:.3f}   Commit+Write={commit_share:5.1f}%  Ops={op_share:5.1f}%")

old_b3m = rows(OUT / "b3m_hotmpt.csv")
print("\nold B3m (StartFile=0, pre-T1T2) first 8 windows for shape reference:")
for o_ in old_b3m[:8]:
    wall = float(o_["Comparative_Wall_ms"])
    print(f"  win {o_['Total_Batches']:>6}  ops/s {float(o_['Operations_Per_Sec']):>10,.0f}  Commit={float(o_['Commit_ms'])/wall*100:5.1f}%  DBW={float(o_['DB_Write_ms'])/wall*100:4.1f}%  Ops={float(o_['Operations_ms'])/wall*100:5.1f}%")

if ratios:
    print(f"\nB3m2 vs B0b: window-mean r = {sum(ratios)/len(ratios):.3f}, last10 = {sum(ratios[-10:])/len(ratios[-10:]):.3f}")
    print(f"B2r reference (binary, same pairing): mean r ~= 0.26")
    cs = [s[0] for s in shares]
    os_ = [s[1] for s in shares]
    print(f"Commit+Write share: mean {sum(cs)/len(cs):.1f}%  |  Operations share: mean {sum(os_)/len(os_):.1f}%")
