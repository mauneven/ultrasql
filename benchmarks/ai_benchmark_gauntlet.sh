#!/usr/bin/env bash
# Reproducible AI database benchmark gauntlet.
#
# This runner is the single entrypoint for vector/RAG/AI workload artifacts.
# Required UltraSQL suites must emit measured artifacts. Competitor gaps inside
# cross-engine child scripts remain allowed in smoke profiles and are recorded
# by those child artifacts without failing this manifest.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${AI_GAUNTLET_PROFILE:-${1:-smoke}}"
OUT_DIR="${AI_GAUNTLET_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
MANIFEST="$OUT_DIR/ai_benchmark_gauntlet_manifest.json"
REQUIRE_PGVECTOR="${AI_GAUNTLET_REQUIRE_PGVECTOR:-0}"
REQUIRE_DUCKDB="${AI_GAUNTLET_REQUIRE_DUCKDB:-0}"
REQUIRE_CLICKHOUSE="${AI_GAUNTLET_REQUIRE_CLICKHOUSE:-0}"

mkdir -p "$RAW_DIR"

case "$PROFILE" in
    smoke | full)
        ;;
    *)
        echo "ai_benchmark_gauntlet.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

status_file="$(mktemp)"
trap 'rm -f "$status_file"' EXIT
failed=0

record_suite() {
    local suite="$1"
    local status="$2"
    local code="$3"
    local artifact="$4"
    printf '%s\t%s\t%s\t%s\n' "$suite" "$status" "$code" "$artifact" >>"$status_file"
}

run_suite() {
    local suite="$1"
    local artifact="$2"
    shift 2

    echo "=== AI gauntlet suite: $suite profile=$PROFILE ==="
    set +e
    "$@"
    local code=$?
    set -e

    case "$code" in
        0)
            record_suite "$suite" "passed" "$code" "$artifact"
            ;;
        *)
            failed=1
            record_suite "$suite" "failed" "$code" "$artifact"
            ;;
    esac
}

run_exact_vector_scan() {
    local rows dims top_k iters warmup
    if [[ "$PROFILE" == "smoke" ]]; then
        rows="${AI_GAUNTLET_VECTOR_ROWS:-${VECTOR_TOPK_ROWS:-512}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-${VECTOR_TOPK_DIMS:-8}}"
        top_k="${AI_GAUNTLET_VECTOR_K:-${VECTOR_TOPK_K:-5}}"
        iters="${AI_GAUNTLET_ITERS:-${N_ITERS:-1}}"
        warmup="${AI_GAUNTLET_WARMUP:-${WARMUP:-0}}"
    else
        rows="${AI_GAUNTLET_VECTOR_ROWS:-${VECTOR_TOPK_ROWS:-10000}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-${VECTOR_TOPK_DIMS:-8}}"
        top_k="${AI_GAUNTLET_VECTOR_K:-${VECTOR_TOPK_K:-10}}"
        iters="${AI_GAUNTLET_ITERS:-${N_ITERS:-8}}"
        warmup="${AI_GAUNTLET_WARMUP:-${WARMUP:-2}}"
    fi

    RAW_DIR="$RAW_DIR" \
        VECTOR_TOPK_OUT_DIR="$OUT_DIR" \
        VECTOR_TOPK_ROWS="$rows" \
        VECTOR_TOPK_DIMS="$dims" \
        VECTOR_TOPK_K="$top_k" \
        N_ITERS="$iters" \
        WARMUP="$warmup" \
        VECTOR_TOPK_REQUIRE_PGVECTOR="$REQUIRE_PGVECTOR" \
        VECTOR_TOPK_REQUIRE_DUCKDB="$REQUIRE_DUCKDB" \
        VECTOR_TOPK_REQUIRE_CLICKHOUSE="$REQUIRE_CLICKHOUSE" \
        VECTOR_TOPK_RENDER_RESULTS=0 \
        benchmarks/vector_topk_exact.sh
}

run_ann_recall_latency() {
    local rows dims top_k queries warmup
    if [[ "$PROFILE" == "smoke" ]]; then
        rows="${AI_GAUNTLET_VECTOR_ROWS:-${VECTOR_ANN_ROWS:-512}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-${VECTOR_ANN_DIMS:-8}}"
        top_k="${AI_GAUNTLET_VECTOR_K:-${VECTOR_ANN_K:-10}}"
        queries="${AI_GAUNTLET_QUERIES:-${VECTOR_ANN_QUERIES:-4}}"
        warmup="${AI_GAUNTLET_WARMUP:-${VECTOR_ANN_WARMUP:-0}}"
    else
        rows="${AI_GAUNTLET_VECTOR_ROWS:-${VECTOR_ANN_ROWS:-10000}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-${VECTOR_ANN_DIMS:-8}}"
        top_k="${AI_GAUNTLET_VECTOR_K:-${VECTOR_ANN_K:-10}}"
        queries="${AI_GAUNTLET_QUERIES:-${VECTOR_ANN_QUERIES:-50}}"
        warmup="${AI_GAUNTLET_WARMUP:-${VECTOR_ANN_WARMUP:-5}}"
    fi

    RAW_DIR="$RAW_DIR" \
        VECTOR_ANN_OUT_DIR="$OUT_DIR" \
        VECTOR_ANN_ROWS="$rows" \
        VECTOR_ANN_DIMS="$dims" \
        VECTOR_ANN_K="$top_k" \
        VECTOR_ANN_QUERIES="$queries" \
        VECTOR_ANN_WARMUP="$warmup" \
        benchmarks/vector_ann_hnsw.sh
}

run_filtered_vector_search() {
    local rows dims top_k queries warmup tenant_count category_count artifact
    if [[ "$PROFILE" == "smoke" ]]; then
        rows="${AI_GAUNTLET_FILTERED_ROWS:-${AI_GAUNTLET_VECTOR_ROWS:-512}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-8}"
        top_k="${AI_GAUNTLET_FILTERED_K:-${AI_GAUNTLET_VECTOR_K:-5}}"
        queries="${AI_GAUNTLET_FILTERED_QUERIES:-${AI_GAUNTLET_QUERIES:-4}}"
        warmup="${AI_GAUNTLET_WARMUP:-0}"
    else
        rows="${AI_GAUNTLET_FILTERED_ROWS:-${AI_GAUNTLET_VECTOR_ROWS:-10000}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-8}"
        top_k="${AI_GAUNTLET_FILTERED_K:-${AI_GAUNTLET_VECTOR_K:-10}}"
        queries="${AI_GAUNTLET_FILTERED_QUERIES:-${AI_GAUNTLET_QUERIES:-50}}"
        warmup="${AI_GAUNTLET_WARMUP:-5}"
    fi
    tenant_count="${AI_GAUNTLET_FILTERED_TENANTS:-8}"
    category_count="${AI_GAUNTLET_FILTERED_CATEGORIES:-4}"
    artifact="$RAW_DIR/ai_gauntlet_filtered_vector_search_${PROFILE}-ultrasql.json"
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --bin ultrasql-bench >/dev/null
    target/release/ultrasql-bench filtered-vector \
        --profile "$PROFILE" \
        --workload-id "ai_gauntlet_filtered_vector_search_${PROFILE}" \
        --rows "$rows" \
        --dims "$dims" \
        --top-k "$top_k" \
        --queries "$queries" \
        --warmup "$warmup" \
        --tenant-count "$tenant_count" \
        --category-count "$category_count" \
        --output "$artifact"
}

run_hybrid_search_latency() {
    local rows top_k iters warmup artifact
    if [[ "$PROFILE" == "smoke" ]]; then
        rows="${AI_GAUNTLET_HYBRID_ROWS:-512}"
        top_k="${AI_GAUNTLET_HYBRID_K:-2}"
        iters="${AI_GAUNTLET_ITERS:-${N_ITERS:-1}}"
        warmup="${AI_GAUNTLET_WARMUP:-${WARMUP:-0}}"
    else
        rows="${AI_GAUNTLET_HYBRID_ROWS:-10000}"
        top_k="${AI_GAUNTLET_HYBRID_K:-2}"
        iters="${AI_GAUNTLET_ITERS:-${N_ITERS:-8}}"
        warmup="${AI_GAUNTLET_WARMUP:-${WARMUP:-2}}"
    fi
    artifact="$RAW_DIR/ai_gauntlet_hybrid_search_latency_${PROFILE}-ultrasql.json"
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --features sql-bench \
            --bin cross_compare_sql >/dev/null
    target/release/cross_compare_sql \
        --workload hybrid-search-latency \
        --rows "$rows" \
        --top-k "$top_k" \
        --warmup "$warmup" \
        --iters "$iters" \
        --workload-id "ai_gauntlet_hybrid_search_latency_${PROFILE}" \
        --output "$artifact"
}

run_rag_retrieval_quality() {
    local top_k iters warmup artifact
    if [[ "$PROFILE" == "smoke" ]]; then
        top_k="${AI_GAUNTLET_RAG_K:-2}"
        iters="${AI_GAUNTLET_ITERS:-${N_ITERS:-1}}"
        warmup="${AI_GAUNTLET_WARMUP:-${WARMUP:-0}}"
    else
        top_k="${AI_GAUNTLET_RAG_K:-2}"
        iters="${AI_GAUNTLET_ITERS:-${N_ITERS:-8}}"
        warmup="${AI_GAUNTLET_WARMUP:-${WARMUP:-2}}"
    fi
    artifact="$RAW_DIR/ai_gauntlet_rag_retrieval_quality_${PROFILE}-ultrasql.json"
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --features sql-bench \
            --bin cross_compare_sql >/dev/null
    target/release/cross_compare_sql \
        --workload rag-retrieval-quality \
        --top-k "$top_k" \
        --warmup "$warmup" \
        --iters "$iters" \
        --workload-id "ai_gauntlet_rag_retrieval_quality_${PROFILE}" \
        --output "$artifact"
}

run_ingestion_throughput() {
    local rows dims artifact
    if [[ "$PROFILE" == "smoke" ]]; then
        rows="${AI_GAUNTLET_INGEST_ROWS:-${AI_GAUNTLET_VECTOR_ROWS:-512}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-8}"
    else
        rows="${AI_GAUNTLET_INGEST_ROWS:-${AI_GAUNTLET_VECTOR_ROWS:-10000}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-8}"
    fi
    artifact="$RAW_DIR/ai_gauntlet_ingestion_throughput_${PROFILE}-ultrasql.json"
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --features sql-bench \
            --bin cross_compare_sql >/dev/null
    target/release/cross_compare_sql \
        --workload ingestion-throughput \
        --rows "$rows" \
        --vector-dims "$dims" \
        --workload-id "ai_gauntlet_ingestion_throughput_${PROFILE}" \
        --output "$artifact"
}

run_memory_per_million_vectors() {
    local rows dims lists probes artifact
    if [[ "$PROFILE" == "smoke" ]]; then
        rows="${AI_GAUNTLET_MEMORY_ROWS:-512}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-8}"
        lists="${AI_GAUNTLET_IVFFLAT_LISTS:-16}"
        probes="${AI_GAUNTLET_IVFFLAT_PROBES:-4}"
    else
        rows="${AI_GAUNTLET_MEMORY_ROWS:-10000}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-8}"
        lists="${AI_GAUNTLET_IVFFLAT_LISTS:-64}"
        probes="${AI_GAUNTLET_IVFFLAT_PROBES:-8}"
    fi
    artifact="$RAW_DIR/ai_gauntlet_memory_per_million_vectors_${PROFILE}-ultrasql.json"
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --bin ultrasql-bench >/dev/null
    target/release/ultrasql-bench vector-memory \
        --profile "$PROFILE" \
        --workload-id "ai_gauntlet_memory_per_million_vectors_${PROFILE}" \
        --rows "$rows" \
        --dims "$dims" \
        --lists "$lists" \
        --probes "$probes" \
        --output "$artifact"
}

run_cold_start_index_load() {
    local rows dims top_k artifact
    if [[ "$PROFILE" == "smoke" ]]; then
        rows="${AI_GAUNTLET_COLD_START_ROWS:-${AI_GAUNTLET_VECTOR_ROWS:-512}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-8}"
        top_k="${AI_GAUNTLET_COLD_START_K:-${AI_GAUNTLET_VECTOR_K:-5}}"
    else
        rows="${AI_GAUNTLET_COLD_START_ROWS:-${AI_GAUNTLET_VECTOR_ROWS:-10000}}"
        dims="${AI_GAUNTLET_VECTOR_DIMS:-8}"
        top_k="${AI_GAUNTLET_COLD_START_K:-${AI_GAUNTLET_VECTOR_K:-10}}"
    fi
    artifact="$RAW_DIR/ai_gauntlet_cold_start_index_load_${PROFILE}-ultrasql.json"
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --features sql-bench \
            --bin cross_compare_sql >/dev/null
    target/release/cross_compare_sql \
        --workload cold-start-index-load \
        --rows "$rows" \
        --vector-dims "$dims" \
        --top-k "$top_k" \
        --workload-id "ai_gauntlet_cold_start_index_load_${PROFILE}" \
        --output "$artifact"
}

run_suite \
    "exact_vector_scan" \
    "$RAW_DIR/vector_topk_exact_*-{ultrasql,postgres17_pgvector,duckdb_list,clickhouse_vector}.json" \
    run_exact_vector_scan
run_suite \
    "ann_recall_latency" \
    "$RAW_DIR/vector_ann_hnsw_*-ultrasql_hnsw.json" \
    run_ann_recall_latency
run_suite \
    "filtered_vector_search" \
    "$RAW_DIR/ai_gauntlet_filtered_vector_search_${PROFILE}-ultrasql.json" \
    run_filtered_vector_search
run_suite \
    "hybrid_search_latency" \
    "$RAW_DIR/ai_gauntlet_hybrid_search_latency_${PROFILE}-ultrasql.json" \
    run_hybrid_search_latency
run_suite \
    "rag_retrieval_quality" \
    "$RAW_DIR/ai_gauntlet_rag_retrieval_quality_${PROFILE}-ultrasql.json" \
    run_rag_retrieval_quality
run_suite \
    "ingestion_throughput" \
    "$RAW_DIR/ai_gauntlet_ingestion_throughput_${PROFILE}-ultrasql.json" \
    run_ingestion_throughput
run_suite \
    "memory_per_million_vectors" \
    "$RAW_DIR/ai_gauntlet_memory_per_million_vectors_${PROFILE}-ultrasql.json" \
    run_memory_per_million_vectors
run_suite \
    "cold_start_index_load" \
    "$RAW_DIR/ai_gauntlet_cold_start_index_load_${PROFILE}-ultrasql.json" \
    run_cold_start_index_load

python3 - "$PROFILE" "$MANIFEST" "$status_file" <<'PY'
import json
import pathlib
import sys
import time

profile, manifest_path, status_path = sys.argv[1:]
entries = []
for line in pathlib.Path(status_path).read_text(encoding="utf-8").splitlines():
    suite, status, code, artifact = line.split("\t")
    entries.append(
        {
            "suite": suite,
            "status": status,
            "exit_code": int(code),
            "artifact": artifact,
        }
    )

has_failed = any(entry["status"] == "failed" for entry in entries)
doc = {
    "schema_version": 1,
    "profile": profile,
    "generated_at_unix": int(time.time()),
    "status": "failed" if has_failed else "passed",
    "passed": not has_failed,
    "suites": entries,
    "policy": (
        "AI benchmark gauntlet requires every UltraSQL suite to emit measured "
        "artifacts. Smoke profiles may record missing competitors inside child "
        "artifacts, but UltraSQL runner gaps fail this manifest. Set "
        "AI_GAUNTLET_REQUIRE_PGVECTOR=1, AI_GAUNTLET_REQUIRE_DUCKDB=1, "
        "and AI_GAUNTLET_REQUIRE_CLICKHOUSE=1 to require measured same-host "
        "PostgreSQL+pgvector, DuckDB, and ClickHouse exact-vector artifacts."
    ),
}
pathlib.Path(manifest_path).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(json.dumps(doc, indent=2))
PY

if (( failed != 0 )); then
    exit 1
fi
exit 0
