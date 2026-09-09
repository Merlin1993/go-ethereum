import json
from pathlib import Path

import paramiko


HOST = "192.168.2.230"
ARCHIVE_ROOT = "/root/asct_codex/archive_metadata_20260904"

REMOTE_SCRIPT = r"""
set -euo pipefail

ROOT=/root/asct_codex
CURRENT_RUN=$ROOT/results/trace_stress/formal_20242112513_depth16_20260904_161556
FAILED_RUN=$ROOT/results/trace_stress/formal_20242112513_depth16_20260825_140122
TRACE=$ROOT/mainnet_state_access_trace
ETHDATA=$ROOT/ethdata
ARCHIVE_ROOT=/root/asct_codex/archive_metadata_20260904

bases=(
  "$ROOT/asct_path_destructive_depth20_1p2b_batch1000_20260620"
  "$ROOT/asct_path_destructive_depth20_1p2b_batch1000_stats100m_20260621_1530"
  "$ROOT/results/mainnet/asct/run_remote_asct_formal10m_30b06fcec_20260807_152514"
  "$ROOT/results/mainnet/asct/run_remote_asct_stemcache_formal10m_a2211377d_20260810_113252"
  "$ROOT/results/mainnet/mpt/run_remote_mpt_20260622_112506_files11"
  "$ROOT/results/random_stress/asct_stem/run_remote_asct_stem_sparse_500m_batch2000_depth16_a2211377d_20260812_110109"
)

dbs=(
  "$ROOT/asct_path_destructive_depth20_1p2b_batch1000_20260620/asct_state_db"
  "$ROOT/asct_path_destructive_depth20_1p2b_batch1000_20260620/asct_archive_db"
  "$ROOT/asct_path_destructive_depth20_1p2b_batch1000_stats100m_20260621_1530/asct_state_db"
  "$ROOT/asct_path_destructive_depth20_1p2b_batch1000_stats100m_20260621_1530/asct_archive_db"
  "$ROOT/results/mainnet/asct/run_remote_asct_formal10m_30b06fcec_20260807_152514/state_db"
  "$ROOT/results/mainnet/asct/run_remote_asct_formal10m_30b06fcec_20260807_152514/archive_db"
  "$ROOT/results/mainnet/asct/run_remote_asct_stemcache_formal10m_a2211377d_20260810_113252/state_db"
  "$ROOT/results/mainnet/asct/run_remote_asct_stemcache_formal10m_a2211377d_20260810_113252/archive_db"
  "$ROOT/results/mainnet/mpt/run_remote_mpt_20260622_112506_files11/state_db"
  "$ROOT/results/mainnet/mpt/run_remote_mpt_20260622_112506_files11/archive_db"
  "$ROOT/results/random_stress/asct_stem/run_remote_asct_stem_sparse_500m_batch2000_depth16_a2211377d_20260812_110109/data/asct_state_db"
  "$ROOT/results/random_stress/asct_stem/run_remote_asct_stem_sparse_500m_batch2000_depth16_a2211377d_20260812_110109/preflight_smoke/asct_state_db"
)

abort() {
  printf 'ABORT: %s\n' "$*" >&2
  exit 3
}

free_bytes() {
  df -B1 --output=avail "$ROOT" | tail -n 1 | tr -d ' '
}

pre_free=$(free_bytes)

launcher=$(find "$ROOT" -maxdepth 1 -type f -name 'run_full_replay_*.sh' -printf '%T@ %p\n' | sort -nr | head -n 1 | cut -d' ' -f2-)
[ -n "$launcher" ] || abort "current replay launcher not found"
[ "$launcher" = "$ROOT/run_full_replay_20260904_161556.sh" ] || abort "unexpected launcher: $launcher"
launcher_pid=$(ps -eo pid=,args= | awk -v target="$launcher" '$2 == "bash" && $3 == target {print $1; exit}')
[ -n "$launcher_pid" ] && kill -0 "$launcher_pid" 2>/dev/null || abort "current replay launcher is not running"
[ -d "$CURRENT_RUN" ] || abort "current run missing"

for db in "${dbs[@]}"; do
  case "$db" in
    "$CURRENT_RUN"/*|"$FAILED_RUN"/*|"$TRACE"/*|"$ETHDATA"/*)
      abort "protected path entered deletion list: $db"
      ;;
  esac
  case "$db" in
    "$ROOT"/*) ;;
    *) abort "path outside experiment root: $db" ;;
  esac
done

for base in "${bases[@]}"; do
  [ -d "$base" ] || abort "expected old run missing: $base"
done

mkdir -p "$ARCHIVE_ROOT"

printf 'PRE_FREE_BYTES=%s\n' "$pre_free"
printf 'CURRENT_LAUNCHER=%s\n' "$launcher"
printf 'CURRENT_LAUNCHER_PID=%s\n' "$launcher_pid"
printf 'ARCHIVE_ROOT=%s\n' "$ARCHIVE_ROOT"
printf '\nDB_INVENTORY_BEFORE\n'
for db in "${dbs[@]}"; do
  if [ -e "$db" ]; then
    printf '%s\t%s\n' "$db" "$(du -shx "$db" | cut -f1)"
  else
    printf '%s\tmissing\n' "$db"
  fi
done

for base in "${bases[@]}"; do
  slug=${base#"$ROOT"/}
  slug=${slug//\//__}
  slug=${slug// /_}
  archive=$ARCHIVE_ROOT/${slug}.tar.gz
  list=$ARCHIVE_ROOT/${slug}.filelist
  excludes=()
  for db in "${dbs[@]}"; do
    case "$db" in
      "$base"/*) excludes+=( -path "$db" ) ;;
    esac
  done

  : > "$list"
  find "$base" \
    \( "${excludes[@]}" -type d -prune \) -o \
    \( -type d \( -path "$base/src" -o -path "$base/source" -o -path "$base/go-build-cache" \) -prune \) -o \
    \( -type f \( -name '*.csv' -o -name '*.json' -o -name '*.log' -o -name '*.txt' -o -name '*.sh' -o -name '*.md' -o -name '*.pid' -o -name '*.exit' \) -print0 \) \
    > "$list"

  file_count=$(tr -cd '\0' < "$list" | wc -c)
  if [ "$file_count" -eq 0 ]; then
    abort "no metadata found for $base"
  fi

  tar --null -T "$list" -czf "$archive"
  gzip -t "$archive"
  tar -tzf "$archive" > /dev/null
  archive_count=$(tar -tzf "$archive" | wc -l)
  [ "$archive_count" -eq "$file_count" ] || abort "entry mismatch for $archive: list=$file_count archive=$archive_count"
  [ -s "$archive" ] || abort "empty archive: $archive"
  sha256sum "$archive" > "$archive.sha256"
  printf 'ARCHIVED\t%s\tfiles=%s\tbytes=%s\n' "$archive" "$file_count" "$(stat -c %s "$archive")"
done

archive_count=$(find "$ARCHIVE_ROOT" -maxdepth 1 -type f -name '*.tar.gz' | wc -l)
[ "$archive_count" -eq "${#bases[@]}" ] || abort "archive count mismatch: $archive_count"

printf '\nDELETING_DB_DIRECTORIES\n'
removed=0
for db in "${dbs[@]}"; do
  if [ -e "$db" ] || [ -L "$db" ]; then
    rm -rf -- "$db"
    removed=$((removed + 1))
  fi
done

post_free=$(free_bytes)
freed=$((post_free - pre_free))
printf 'REMOVED_DB_DIRECTORIES=%s\n' "$removed"
printf 'POST_FREE_BYTES=%s\n' "$post_free"
printf 'FREED_BYTES=%s\n' "$freed"
printf 'ARCHIVE_TOTAL_BYTES=%s\n' "$(du -sb "$ARCHIVE_ROOT" | cut -f1)"
printf 'CURRENT_REPLAY_STILL_RUNNING='
kill -0 "$launcher_pid" 2>/dev/null && echo yes || echo no
printf '\nARCHIVE_SHA256\n'
cat "$ARCHIVE_ROOT"/*.sha256
"""


def main() -> None:
    credentials = json.loads(Path(".agent/asct_remote_servers.local.json").read_text())["asct_mpt"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST,
        username=credentials["ssh_user"],
        password=credentials["ssh_password"],
        look_for_keys=False,
        allow_agent=False,
    )
    try:
        _, stdout, stderr = client.exec_command(REMOTE_SCRIPT, timeout=600)
        output = stdout.read().decode(errors="replace")
        error = stderr.read().decode(errors="replace")
        print(output)
        if error:
            print("STDERR")
            print(error)
        if stdout.channel.recv_exit_status() != 0:
            raise SystemExit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
