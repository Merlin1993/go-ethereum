#!/usr/bin/env bash
set -u

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <pid> <minimum-free-bytes> <log-file>" >&2
  exit 64
fi

target_pid=$1
minimum_free=$2
log_file=$3

while kill -0 "$target_pid" 2>/dev/null; do
  free_bytes=$(df -B1 --output=avail /root/asct_codex | tail -n 1 | tr -d ' ')
  if [ "$free_bytes" -lt "$minimum_free" ]; then
    printf '%s free_bytes=%s threshold=%s stopping_pid=%s\n' \
      "$(date --iso-8601=seconds)" "$free_bytes" "$minimum_free" "$target_pid" >> "$log_file"
    pkill -TERM -P "$target_pid" 2>/dev/null || true
    kill -TERM "$target_pid" 2>/dev/null || true
    exit 2
  fi
  sleep 60
done
