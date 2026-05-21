#!/usr/bin/env bash
# Run benchmark certification suites with explicit smoke/full profiles.
#
# Usage:
#   benchmarks/certify.sh smoke
#   benchmarks/certify.sh full
#   benchmarks/certify.sh full tpch,clickbench,vector-ann,ai-gauntlet,csv-gauntlet,object-parquet-range,firebolt-aggregate,firebolt-sparse-pruning,firebolt-vector
#
# Smoke is PR-safe: tiny datasets, crash/correctness checks, no external
# benchmark assets. Full is nightly/manual: it attempts the full certification
# runners and records setup-missing suites as unavailable, not as benchmark
# claims.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

profile="${1:-smoke}"
suite_filter="${2:-}"
OUT_DIR="${BENCH_CERT_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
MANIFEST="$OUT_DIR/benchmark_certification_manifest.json"

mkdir -p "$RAW_DIR"

case "$profile" in
    smoke)
        suites=(regression-gate vector-ann sysbench)
        ;;
    full)
        suites=(tpch clickbench tpcb tpcc sysbench vector-topk vector-ann ai-gauntlet csv-gauntlet object-parquet-range firebolt-aggregate firebolt-sparse-pruning firebolt-vector)
        ;;
    *)
        echo "certify.sh: profile must be smoke or full, got '$profile'" >&2
        exit 2
        ;;
esac

if [[ -n "$suite_filter" ]]; then
    IFS=',' read -r -a suites <<<"$suite_filter"
fi

status_file="$(mktemp)"
trap 'rm -f "$status_file"' EXIT
failed=0

record_suite() {
    local suite="$1"
    local status="$2"
    local code="$3"
    printf '%s\t%s\t%s\n' "$suite" "$status" "$code" >>"$status_file"
}

run_suite() {
    local suite="$1"
    shift

    echo "=== certification suite: $suite profile=$profile ==="
    set +e
    "$@"
    local code=$?
    set -e

    case "$code" in
        0)
            record_suite "$suite" "passed" "$code"
            ;;
        2)
            record_suite "$suite" "unavailable" "$code"
            ;;
        *)
            record_suite "$suite" "failed" "$code"
            failed=1
            ;;
    esac
}

build_smoke_bins() {
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --features sql-bench \
            --bin regression-gate \
            --bin ultrasql-bench \
            --bin cross_compare_sql >/dev/null
}

run_regression_smoke() {
    build_smoke_bins
    target/release/regression-gate --stage current --smoke
}

run_vector_ann_smoke() {
    VECTOR_ANN_ROWS=512 \
        VECTOR_ANN_DIMS=8 \
        VECTOR_ANN_K=10 \
        VECTOR_ANN_QUERIES=4 \
        VECTOR_ANN_WARMUP=0 \
        benchmarks/vector_ann_hnsw.sh
}

run_sysbench_smoke() {
    SYSBENCH_ROWS=1000 \
        SYSBENCH_ITERS=1 \
        SYSBENCH_WARMUP=0 \
        benchmarks/sysbench_certify.sh
}

run_vector_ann_full() {
    benchmarks/vector_ann_hnsw.sh
}

run_vector_topk_full() {
    benchmarks/vector_topk_exact.sh
}

run_ai_gauntlet_smoke() {
    AI_GAUNTLET_PROFILE=smoke benchmarks/ai_benchmark_gauntlet.sh smoke
}

run_ai_gauntlet_full() {
    AI_GAUNTLET_PROFILE=full benchmarks/ai_benchmark_gauntlet.sh full
}

run_csv_gauntlet_smoke() {
    CSV_GAUNTLET_PROFILE=smoke benchmarks/csv_benchmark_gauntlet.sh smoke
}

run_csv_gauntlet_full() {
    CSV_GAUNTLET_PROFILE=full benchmarks/csv_benchmark_gauntlet.sh full
}

run_object_parquet_range_smoke() {
    OBJECT_PARQUET_RANGE_PROFILE=smoke benchmarks/object_parquet_range.sh smoke
}

run_object_parquet_range_full() {
    OBJECT_PARQUET_RANGE_PROFILE=full benchmarks/object_parquet_range.sh full
}

run_firebolt_aggregate_smoke() {
    FIREBOLT_AGG_PROFILE=smoke benchmarks/firebolt_aggregate_index.sh smoke
}

run_firebolt_aggregate_full() {
    FIREBOLT_AGG_PROFILE=full benchmarks/firebolt_aggregate_index.sh full
}

run_firebolt_sparse_pruning_smoke() {
    FIREBOLT_SPARSE_PROFILE=smoke benchmarks/firebolt_sparse_pruning.sh smoke
}

run_firebolt_sparse_pruning_full() {
    FIREBOLT_SPARSE_PROFILE=full benchmarks/firebolt_sparse_pruning.sh full
}

run_firebolt_vector_smoke() {
    FIREBOLT_VECTOR_PROFILE=smoke benchmarks/firebolt_vector_search.sh smoke
}

run_firebolt_vector_full() {
    FIREBOLT_VECTOR_PROFILE=full benchmarks/firebolt_vector_search.sh full
}

for suite in "${suites[@]}"; do
    case "$suite" in
        regression-gate)
            run_suite "$suite" run_regression_smoke
            ;;
        vector-ann)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_vector_ann_smoke
            else
                run_suite "$suite" run_vector_ann_full
            fi
            ;;
        vector-topk)
            run_suite "$suite" run_vector_topk_full
            ;;
        ai-gauntlet)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_ai_gauntlet_smoke
            else
                run_suite "$suite" run_ai_gauntlet_full
            fi
            ;;
        csv-gauntlet)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_csv_gauntlet_smoke
            else
                run_suite "$suite" run_csv_gauntlet_full
            fi
            ;;
        object-parquet-range)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_object_parquet_range_smoke
            else
                run_suite "$suite" run_object_parquet_range_full
            fi
            ;;
        firebolt-aggregate)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_firebolt_aggregate_smoke
            else
                run_suite "$suite" run_firebolt_aggregate_full
            fi
            ;;
        firebolt-sparse-pruning)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_firebolt_sparse_pruning_smoke
            else
                run_suite "$suite" run_firebolt_sparse_pruning_full
            fi
            ;;
        firebolt-vector)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_firebolt_vector_smoke
            else
                run_suite "$suite" run_firebolt_vector_full
            fi
            ;;
        sysbench)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_sysbench_smoke
            else
                run_suite "$suite" benchmarks/sysbench_certify.sh
            fi
            ;;
        tpch)
            run_suite "$suite" benchmarks/tpch_sf10_certify.sh
            ;;
        clickbench)
            run_suite "$suite" benchmarks/clickbench_certify.sh
            ;;
        tpcb)
            run_suite "$suite" benchmarks/tpcb_certify.sh
            ;;
        tpcc)
            run_suite "$suite" benchmarks/tpcc_certify.sh
            ;;
        *)
            echo "certify.sh: unknown suite '$suite'" >&2
            record_suite "$suite" "unavailable" 2
            ;;
    esac
done

python3 - "$profile" "$MANIFEST" "$status_file" <<'PY'
import json
import pathlib
import sys
import time

profile, manifest_path, status_path = sys.argv[1:]
entries = []
for line in pathlib.Path(status_path).read_text().splitlines():
    suite, status, code = line.split("\t")
    entries.append({"suite": suite, "status": status, "exit_code": int(code)})

doc = {
    "profile": profile,
    "generated_at_unix": int(time.time()),
    "passed": all(entry["status"] != "failed" for entry in entries),
    "suites": entries,
    "policy": (
        "smoke is PR-safe and short; full attempts certification runners. "
        "Exit code 2 means prerequisites were unavailable and the suite did "
        "not produce a benchmark claim."
    ),
}
pathlib.Path(manifest_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY

exit "$failed"
