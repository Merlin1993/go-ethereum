import json
import posixpath

import paramiko


RUNS = {
    "ASCT": "/root/asct_codex/results/trace_stress/formal_20242112513_depth16_20260825_140122",
    "MPT": "/root/asct_codex/results/trie_compare/formal_20242112513_mpt_20260825_140122",
    "Verkle": "/root/asct_codex/results/trie_compare/formal_20242112513_verkle_20260825_140122",
}
EXTRA = {
    "ON 1B": "/root/asct_codex/results/trace_stress/formal_read_promotion_on_1b_depth16_20260902_094611",
}


def read_tail(f, size, count=5000):
    f.seek(max(0, size - count))
    return f.read().decode(errors="replace")


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
    )
    try:
        _, stdout, stderr = client.exec_command("pgrep -af '[g]o test|[a]rchive.test' || true", timeout=10)
        print("PROCESSES")
        print(stdout.read().decode())
        print(stderr.read().decode(), end="")

        sftp = client.open_sftp()
        try:
            for name, run in EXTRA.items():
                print(f"=== {name} ===")
                path = run + "/results/summary.json"
                with sftp.open(path, "r") as f:
                    print(f.read().decode())
            for name, run in RUNS.items():
                print(f"=== {name} ===")
                try:
                    for item in sorted(sftp.listdir_attr(run), key=lambda x: x.filename):
                        if not item.filename.startswith("state_db") and item.st_size > 0:
                            print(f"{item.filename} {item.st_size}")
                except FileNotFoundError:
                    print("MISSING RUN DIR")
                    continue
                for leaf in (".exit", "run_status.json", "results/summary.json"):
                    path = posixpath.join(run, leaf)
                    try:
                        size = sftp.stat(path).st_size
                    except FileNotFoundError:
                        continue
                    with sftp.open(path, "r") as f:
                        text = read_tail(f, size, 8000)
                    print(f"--- {leaf} ---")
                    print(text)
                log = posixpath.join(run, run.rsplit("/", 1)[-1] + ".log")
                try:
                    size = sftp.stat(log).st_size
                except FileNotFoundError:
                    pass
                else:
                    with sftp.open(log, "r") as f:
                        print(f"--- log tail {size} ---")
                        print(read_tail(f, size, 2000))
        finally:
            sftp.close()
    finally:
        client.close()


if __name__ == "__main__":
    main()
