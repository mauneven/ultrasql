#!/usr/bin/env bash
# Late-materialization smoke/full runner.
#
# Measures UltraSQL's wide fact-table payload projection behind a selective
# indexed filter. The raw artifact is only valid when EXPLAIN ANALYZE reports
# Late Materialization counters.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

profile="${1:-smoke}"
OUT_DIR="${LATE_MAT_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"

case "$profile" in
    smoke)
        rows="${LATE_MAT_ROWS:-10000}"
        warmup="${LATE_MAT_WARMUP:-0}"
        iters="${LATE_MAT_ITERS:-1}"
        ;;
    full)
        rows="${LATE_MAT_ROWS:-1000000}"
        warmup="${LATE_MAT_WARMUP:-2}"
        iters="${LATE_MAT_ITERS:-5}"
        ;;
    *)
        echo "late_materialization.sh: profile must be smoke or full, got '$profile'" >&2
        exit 2
        ;;
esac

mkdir -p "$RAW_DIR"
cargo build --release -p ultrasql-bench --features sql-bench --bin cross_compare_sql

if (( rows >= 1000000 && rows % 1000000 == 0 )); then
    row_label="$((rows / 1000000))m"
elif (( rows >= 1000 && rows % 1000 == 0 )); then
    row_label="$((rows / 1000))k"
else
    row_label="$rows"
fi

target/release/cross_compare_sql \
    --workload late-materialization \
    --rows "$rows" \
    --warmup "$warmup" \
    --iters "$iters" \
    --output "$RAW_DIR/late_materialization_${row_label}-ultrasql.json"
