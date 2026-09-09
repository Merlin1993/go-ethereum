import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
ROOT = "/root/asct_codex"
SOURCE = f"{ROOT}/go-ethereum-trace"
RUNS = {
    "off": f"{ROOT}/results/trace_stress/formal_read_promotion_off_1b_depth16_20260902_094611",
    "on": f"{ROOT}/results/trace_stress/formal_read_promotion_on_1b_depth16_20260902_094611",
}
ROOTS = {
    "off": "0x02083e6771f23876bbb21113069671371c54f8d0b065032e4e42ffea6e817ad8",
    "on": "0x9d75f22476cfa46c5d18f2cc4021279966d75ce8ad4ecbe2b86985072471b5d8",
}
STAMP = "20260904b"
LAUNCHER = f"{ROOT}/run_read_promotion_poststats_{STAMP}.sh"
STATUS = f"{LAUNCHER}.status.log"


def main() -> None:
    cfg = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=cfg["ssh_user"], password=cfg["ssh_password"], look_for_keys=False, allow_agent=False)
    try:
        outputs = []
        for name, run in RUNS.items():
            outputs.extend([
                f"{run}/results/storage_breakdown.json",
                f"{run}/results/filter_fp_20_per_bucket.json",
            ])
        check = "set -e; test ! -e '" + STATUS + "'; pgrep -af '[g]o test|[a]rchive.test' || true"
        _, stdout, stderr = client.exec_command(check, timeout=20)
        text = stdout.read().decode().strip()
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(stderr.read().decode().strip() or "post-stats preflight failed")
        if text:
            raise RuntimeError(f"existing test process is running:\n{text}")

        sftp = client.open_sftp()
        try:
            for name in ("trie/archive/trace_stress_test.go", "trie/archive/trace_cold_hit_stats_test.go"):
                sftp.put(name, f"{SOURCE}/{name}")
            script = "#!/usr/bin/env bash\nset -u\ncd '" + SOURCE + "'\n"
            script += "export GOCACHE='" + ROOT + "/go-build-cache'\nexport GOTMPDIR='" + ROOT + "/trace-tmp'\n"
            script += "printf '%s post-stats start\\n' \"$(date --iso-8601=seconds)\" >'" + STATUS + "'\n"
            for name, run in RUNS.items():
                root = ROOTS[name]
                storage = run + "/results/storage_breakdown.json"
                fp = run + "/results/filter_fp_20_per_bucket.json"
                script += "test -e '" + storage + "' || /usr/local/go/bin/go test ./trie/archive -run '^TestArchiveStemTraceStorageStats$' -count=1 -timeout 0 -v -args -traceStatsBaseDir='" + run + "' -traceStatsRoot='" + root + "' -traceStatsOutput='" + storage + "' -traceStressShardDepth=16 >>'" + STATUS + "' 2>&1 || true\n"
                script += "test -e '" + fp + "' || /usr/local/go/bin/go test ./trie/archive -run '^TestArchiveStemTraceFilterFPStats$' -count=1 -timeout 0 -v -args -traceStatsBaseDir='" + run + "' -traceStatsRoot='" + root + "' -traceFilterOutput='" + fp + "' -traceFilterSamples=20 -traceFilterSeed=7 -traceStressShardDepth=16 >>'" + STATUS + "' 2>&1 || true\n"
            script += "printf '%s post-stats complete\\n' \"$(date --iso-8601=seconds)\" >>'" + STATUS + "'\n"
            with sftp.file(LAUNCHER, "w") as f:
                f.write(script)
        finally:
            sftp.close()

        _, stdout, stderr = client.exec_command(
            f"chmod +x '{LAUNCHER}' && nohup '{LAUNCHER}' </dev/null >'{LAUNCHER}.nohup.log' 2>&1 & echo $!", timeout=10
        )
        pid = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        if not pid.isdigit():
            raise RuntimeError(error or f"launch failed: {pid}")
        print(json.dumps({"status": "launched", "pid": int(pid), "launcher": LAUNCHER, "status_log": STATUS}, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
