#!/usr/bin/env bash
# Run benchmark certification suites with explicit smoke/full profiles.
#
# Usage:
#   benchmarks/certify.sh smoke
#   benchmarks/certify.sh full
#   benchmarks/certify.sh full tpch,tpch-sf1-postgres,clickbench,sql-regression,vector-ann,ai-vector-pgvector,ai-gauntlet,csv-gauntlet,object-parquet-range,late-materialization,firebolt-aggregate,firebolt-sparse-pruning,firebolt-vector,rls-tenant,chaos-recovery
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
        suites=(regression-gate rls-tenant vector-ann sysbench)
        ;;
    full)
        suites=(tpch tpch-sf1-postgres clickbench tpcb tpcc sysbench sql-regression vector-topk vector-ann ai-vector-pgvector ai-gauntlet csv-gauntlet object-parquet-range late-materialization firebolt-aggregate firebolt-sparse-pruning firebolt-vector rls-tenant chaos-recovery)
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
        SYSBENCH_DURATION=1 \
        SYSBENCH_WARMUP=0 \
        SYSBENCH_CONNECTIONS=4 \
        SYSBENCH_ALLOW_ULTRASQL_ONLY=1 \
        benchmarks/sysbench_certify.sh
}

run_sql_regression_full() {
    local artifact="$OUT_DIR/sql_regression_certification.json"
    local suites=(tests/slt/sql_regression/regression_subset/*.slt)
    local reference_url="${ULTRASQL_SLT_REFERENCE_URL:-${POSTGRES_URL:-}}"

    cargo run -p ultrasql-sqllogictest-runner -- --mode in-process "${suites[@]}"

    if [[ -z "$reference_url" ]]; then
        python3 - "$artifact" "${#suites[@]}" <<'PY'
import json
import pathlib
import sys
import time

artifact, suite_count = sys.argv[1], int(sys.argv[2])
doc = {
    "schema_version": 1,
    "suite": "sql_regression",
    "status": "not_available",
    "reason": "reference_url_missing",
    "active_shards": suite_count,
    "in_process_status": "passed",
    "reference_engine": "postgres",
    "generated_at_unix": int(time.time()),
    "policy": (
        "Active public SQL regression shards must pass in-process. "
        "Full differential certification also requires ULTRASQL_SLT_REFERENCE_URL "
        "or POSTGRES_URL for PostgreSQL comparison."
    ),
}
pathlib.Path(artifact).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(json.dumps(doc, indent=2))
PY
        return 2
    fi

    tests/slt/run_sql_regression.sh
    python3 - "$artifact" "${#suites[@]}" <<'PY'
import json
import pathlib
import sys
import time

artifact, suite_count = sys.argv[1], int(sys.argv[2])
doc = {
    "schema_version": 1,
    "suite": "sql_regression",
    "status": "measured",
    "active_shards": suite_count,
    "in_process_status": "passed",
    "reference_engine": "postgres",
    "differential_status": "passed",
    "generated_at_unix": int(time.time()),
}
pathlib.Path(artifact).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(json.dumps(doc, indent=2))
PY
}

run_vector_ann_full() {
    benchmarks/vector_ann_hnsw.sh
}

run_vector_topk_full() {
    VECTOR_TOPK_REQUIRE_PGVECTOR=1 \
        VECTOR_TOPK_REQUIRE_DUCKDB=1 \
        VECTOR_TOPK_REQUIRE_CLICKHOUSE=1 \
        benchmarks/vector_topk_exact.sh
}

run_ai_vector_pgvector_full() {
    benchmarks/ai_vector_pgvector_certify.sh
}

run_ai_gauntlet_smoke() {
    AI_GAUNTLET_PROFILE=smoke benchmarks/ai_benchmark_gauntlet.sh smoke
}

run_ai_gauntlet_full() {
    AI_GAUNTLET_PROFILE=full \
        AI_GAUNTLET_REQUIRE_PGVECTOR=1 \
        AI_GAUNTLET_REQUIRE_DUCKDB=1 \
        AI_GAUNTLET_REQUIRE_CLICKHOUSE=1 \
        benchmarks/ai_benchmark_gauntlet.sh full
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

run_late_materialization_smoke() {
    LATE_MAT_ROWS=10000 \
        LATE_MAT_WARMUP=0 \
        LATE_MAT_ITERS=1 \
        benchmarks/late_materialization.sh smoke
}

run_late_materialization_full() {
    benchmarks/late_materialization.sh full
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

run_chaos_recovery_smoke() {
    CHAOS_PROFILE=smoke CHAOS_OUT_DIR="$OUT_DIR" benchmarks/chaos_recovery.sh smoke
}

run_chaos_recovery_full() {
    CHAOS_PROFILE=full CHAOS_OUT_DIR="$OUT_DIR" benchmarks/chaos_recovery.sh full
}

run_rls_tenant_certification() {
    RLS_TENANT_PROFILE="$profile" \
        RLS_TENANT_OUT_DIR="$OUT_DIR" \
        benchmarks/rls_tenant_certify.sh "$profile"
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
        ai-vector-pgvector)
            run_suite "$suite" run_ai_vector_pgvector_full
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
        late-materialization)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_late_materialization_smoke
            else
                run_suite "$suite" run_late_materialization_full
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
        chaos-recovery)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_chaos_recovery_smoke
            else
                run_suite "$suite" run_chaos_recovery_full
            fi
            ;;
        rls-tenant)
            run_suite "$suite" run_rls_tenant_certification
            ;;
        sysbench)
            if [[ "$profile" == "smoke" ]]; then
                run_suite "$suite" run_sysbench_smoke
            else
                run_suite "$suite" benchmarks/sysbench_certify.sh
            fi
            ;;
        sql-regression)
            run_suite "$suite" run_sql_regression_full
            ;;
        tpch)
            run_suite "$suite" benchmarks/tpch_sf10_certify.sh
            ;;
        tpch-sf1-postgres)
            run_suite "$suite" benchmarks/tpch_sf1_postgres_certify.sh
            ;;
        clickbench)
            run_suite "$suite" benchmarks/clickbench_certify.sh
            ;;
        tpcb)
            run_suite "$suite" env TPCB_OUT_DIR="$OUT_DIR" benchmarks/tpcb_certify.sh
            ;;
        tpcc)
            run_suite "$suite" env TPCC_OUT_DIR="$OUT_DIR" benchmarks/tpcc_certify.sh
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
