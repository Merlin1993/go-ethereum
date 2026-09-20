"""Watchdog: poll titanide SSH reachability until it comes back, then exit.

Silent while down; prints one line per hour at most; exits 0 when reachable.
"""
import json, socket, sys, time

creds = json.load(open(r"D:\go_workspace\go-ethereum\.agent\asct_remote_servers.local.json"))["asct_mpt"]
host = creds["host"]

deadline = time.time() + 6 * 3600  # give up after 6h
last_report = 0
attempts = 0
while time.time() < deadline:
    attempts += 1
    s = socket.socket()
    s.settimeout(10)
    try:
        s.connect((host, 22))
        print(f"titanide {host}:22 REACHABLE after {attempts} attempts / "
              f"{int(time.time() - (deadline - 6*3600))}s down", flush=True)
        sys.exit(0)
    except OSError:
        pass
    finally:
        s.close()
    if time.time() - last_report > 3600:
        last_report = time.time()
        print(f"still unreachable (attempt {attempts})", flush=True)
    time.sleep(60)
print("gave up after 6h", flush=True)
sys.exit(1)
