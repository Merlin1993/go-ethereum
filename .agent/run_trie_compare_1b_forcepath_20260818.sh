#!/usr/bin/env bash
set -u

stamp="${1:?usage: run_trie_compare_1b_forcepath_20260818.sh YYYYMMDD_HHMM}"

cd /root/asct_codex/go-ethereum-trace
export GOCACHE=/root/asct_codex/go-build-cache
export GOTMPDIR=/root/asct_codex/trace-tmp
mkdir -p "$GOTMPDIR" /root/asct_codex/results/trie_compare

for engine in mpt verkle; do
  run="/root/asct_codex/results/trie_compare/formal_1b_forcepath_${engine}_batch4000_${stamp}"
  if [[ -e "$run" || -e "${run}.exit" ]]; then
    echo "refusing to overwrite $run" >&2
    exit 91
  fi

  code=0
  /usr/local/go/bin/go test ./core/tree_test \
    -run TestTrieTraceCompare \
    -count=1 \
    -timeout 0 \
    -v \
    -args \
      -traceCompareInputDir /root/asct_codex/mainnet_state_access_trace/range_10m \
      -traceCompareEngine "$engine" \
      -traceCompareBaseDir "$run" \
      -traceCompareOps 1000000000 \
      -traceCompareBatchSize 4000 \
      -traceCompareMetricsBatches 2500 \
      -traceCompareStartFile 9 >"${run}.log" 2>&1 || code=$?
  echo "$code" >"${run}.exit"
  if [[ "$code" -ne 0 ]]; then
    exit "$code"
  fi
done
