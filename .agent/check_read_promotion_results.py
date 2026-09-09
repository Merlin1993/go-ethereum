import json
import posixpath

import paramiko


RUNS = {
    "off": "/root/asct_codex/results/trace_stress/formal_read_promotion_off_1b_depth16_20260902_094611",
    "on": "/root/asct_codex/results/trace_stress/formal_read_promotion_on_1b_depth16_20260902_094611",
}
STATUS = "/root/asct_codex/run_read_promotion_poststats_20260904b.sh.status.log"


def main() -> None:
    cfg = json.load(open(".agent/asct_remote_servers.local.json"))["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        "192.168.2.230",
        username=cfg["ssh_user"],
        password=cfg["ssh_password"],
        look_for_keys=False,
        allow_agent=False,
        timeout=10,
        banner_timeout=10,
        auth_timeout=10,
    )
    try:
        checks = []
        checks.append("echo '=== PROCESSES ==='; pgrep -af '[g]o test|[a]rchive.test|[r]ead_promotion' || true")
        checks.append(f"echo '=== STATUS ==='; tail -n 40 '{STATUS}' 2>/dev/null || true")
        for name, run in RUNS.items():
            checks.append(f"echo '=== {name.upper()} FILES ==='; find '{run}' -maxdepth 2 -type f -printf '%P %s\\n' | sort | tail -n 30")
        command = "\n".join(checks)
        _, stdout, stderr = client.exec_command(command, timeout=30)
        print(stdout.read().decode())
        print(stderr.read().decode(), end="")
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError("remote inspection failed")

        sftp = client.open_sftp()
        try:
            for name, run in RUNS.items():
                print(f"=== {name.upper()} FILES ===")
                for base in (run, run + "/results"):
                    for item in sorted(sftp.listdir_attr(base), key=lambda x: x.filename):
                        if item.st_size > 0:
                            print(f"{item.filename} {item.st_size}")
                for leaf in ("summary.json", "results/summary.json"):
                    path = posixpath.join(run, leaf)
                    try:
                        size = sftp.stat(path).st_size
                    except FileNotFoundError:
                        continue
                    with sftp.open(path, "r") as f:
                        f.seek(max(0, size - 3000))
                        print(f"=== {name.upper()} {leaf} TAIL {size} ===")
                        print(f.read().decode(errors="replace"))

            path = RUNS["off"] + "/results/filter_fp_20_per_bucket.json"
            size = sftp.stat(path).st_size
            with sftp.open(path, "r") as f:
                f.seek(0)
                print(f"=== FILTER FP HEAD {size} ===")
                print(f.read(2000).decode(errors="replace"))
                f.seek(max(0, size - 2000))
                print("=== FILTER FP TAIL ===")
                print(f.read().decode(errors="replace"))
        finally:
            sftp.close()
    finally:
        client.close()


if __name__ == "__main__":
    main()
