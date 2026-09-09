import json
import re

import paramiko


RUNS = {
    "OFF": "/root/asct_codex/results/trace_stress/formal_read_promotion_off_1b_depth16_20260902_094611",
    "ON": "/root/asct_codex/results/trace_stress/formal_read_promotion_on_1b_depth16_20260902_094611",
}


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
        sftp = client.open_sftp()
        try:
            for name, run in RUNS.items():
                path = run + "/results/filter_fp_20_per_bucket.json"
                with sftp.open(path, "r") as f:
                    head = f.read(32768).decode()
                start = head.index('"stats":')
                end = head.index('"Groups"', start)
                payload = head[start + len('"stats":'):end].strip().rstrip(",")
                if not payload.endswith("}"):
                    payload += "}"
                stats = json.loads(payload)
                selected = {
                    key: stats[key]
                    for key in (
                        "BucketCount",
                        "SampledBuckets",
                        "Samples",
                        "NegativeQueries",
                        "FalsePositives",
                        "Rate",
                    )
                }
                print(name, json.dumps(selected, sort_keys=True))
        finally:
            sftp.close()
    finally:
        client.close()


if __name__ == "__main__":
    main()
