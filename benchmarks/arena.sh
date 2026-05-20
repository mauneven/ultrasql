#!/usr/bin/env bash
# Public benchmark arena.
#
# One command:
#   benchmarks/arena.sh --engines ultrasql,duckdb,clickhouse,postgres
#
# The arena publishes artifacts only. It does not rank engines, render winner
# tables, or invent claims. Missing prerequisites and unimplemented
# suite/engine pairs write not_available JSON artifacts.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${BENCH_ARENA_PROFILE:-smoke}"
ENGINES="${BENCH_ARENA_ENGINES:-ultrasql,duckdb,clickhouse,postgres}"
SUITES="${BENCH_ARENA_SUITES:-csv,parquet,tpch,clickbench,sqllogictest,vector,jsonb}"
OUT_DIR="${BENCH_ARENA_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
MANIFEST="$OUT_DIR/benchmark_arena_manifest.json"
ARTIFACTS_MD="$OUT_DIR/benchmark_arena_artifacts.md"

usage() {
    cat <<'EOF'
Usage:
  benchmarks/arena.sh --engines ultrasql,duckdb,clickhouse,postgres

Options:
  --profile smoke|full       Dataset/runtime profile. Default: smoke.
  --engines a,b,c            Engines: ultrasql,duckdb,clickhouse,postgres.
  --suites a,b,c             Suites: csv,parquet,tpch,clickbench,sqllogictest,vector,jsonb.
  --out-dir PATH             Artifact directory. Default: benchmarks/results/latest.
  --help                     Show this help.

Policy:
  Publish artifacts only. Missing engines, datasets, extensions, or runners
  become not_available artifacts. They are visible gaps, not benchmark claims.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            PROFILE="${2:?missing --profile value}"
            shift 2
            ;;
        --profile=*)
            PROFILE="${1#--profile=}"
            shift
            ;;
        --engines)
            ENGINES="${2:?missing --engines value}"
            shift 2
            ;;
        --engines=*)
            ENGINES="${1#--engines=}"
            shift
            ;;
        --suites)
            SUITES="${2:?missing --suites value}"
            shift 2
            ;;
        --suites=*)
            SUITES="${1#--suites=}"
            shift
            ;;
        --out-dir)
            OUT_DIR="${2:?missing --out-dir value}"
            RAW_DIR="$OUT_DIR/raw"
            MANIFEST="$OUT_DIR/benchmark_arena_manifest.json"
            ARTIFACTS_MD="$OUT_DIR/benchmark_arena_artifacts.md"
            shift 2
            ;;
        --out-dir=*)
            OUT_DIR="${1#--out-dir=}"
            RAW_DIR="$OUT_DIR/raw"
            MANIFEST="$OUT_DIR/benchmark_arena_manifest.json"
            ARTIFACTS_MD="$OUT_DIR/benchmark_arena_artifacts.md"
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "arena.sh: unknown argument '$1'" >&2
            usage >&2
            exit 2
            ;;
    esac
done

case "$PROFILE" in
    smoke|full)
        ;;
    *)
        echo "arena.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

IFS=',' read -r -a REQUESTED_ENGINES <<<"$ENGINES"
IFS=',' read -r -a REQUESTED_SUITES <<<"$SUITES"

for engine in "${REQUESTED_ENGINES[@]}"; do
    case "$engine" in
        ultrasql|duckdb|clickhouse|postgres)
            ;;
        *)
            echo "arena.sh: unknown engine '$engine'" >&2
            exit 2
            ;;
    esac
done

for suite in "${REQUESTED_SUITES[@]}"; do
    case "$suite" in
        csv|parquet|tpch|clickbench|sqllogictest|vector|jsonb)
            ;;
        *)
            echo "arena.sh: unknown suite '$suite'" >&2
            exit 2
            ;;
    esac
done

mkdir -p "$RAW_DIR"

STATUS_FILE="$(mktemp)"
trap 'rm -f "$STATUS_FILE"' EXIT
failed=0
unavailable=0

contains_csv() {
    local csv="$1"
    local needle="$2"
    case ",$csv," in
        *",$needle,"*) return 0 ;;
        *) return 1 ;;
    esac
}

join_csv() {
    local IFS=','
    echo "$*"
}

join_space() {
    local IFS=' '
    echo "$*"
}

requested_supported_csv() {
    local supported="$1"
    local selected=()
    local engine
    for engine in "${REQUESTED_ENGINES[@]}"; do
        if contains_csv "$supported" "$engine"; then
            selected+=("$engine")
        fi
    done
    if (( ${#selected[@]} > 0 )); then
        join_csv "${selected[@]}"
    fi
}

record_suite() {
    local suite="$1"
    local status="$2"
    local code="$3"
    local artifact="$4"
    printf '%s\t%s\t%s\t%s\n' "$suite" "$status" "$code" "$artifact" >>"$STATUS_FILE"
}

emit_not_available() {
    local suite="$1"
    local engine="$2"
    local reason="$3"
    local metrics="$4"
    local out="$RAW_DIR/arena_${suite}_${PROFILE}-${engine}.json"

    python3 - "$out" "$suite" "$engine" "$PROFILE" "$reason" "$metrics" <<'PY'
import json
import pathlib
import sys
import time

out, suite, engine, profile, reason, metrics = sys.argv[1:]
doc = {
    "schema_version": 1,
    "suite": suite,
    "engine": engine,
    "workload": f"arena_{suite}_{profile}",
    "profile": profile,
    "status": "not_available",
    "reason": reason,
    "generated_at_unix": int(time.time()),
    "required_metrics": [metric for metric in metrics.split(",") if metric],
    "policy": (
        "No benchmark claim exists for this suite/engine until a committed "
        "runner emits measured samples from reproducible inputs."
    ),
}
pathlib.Path(out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out)
PY
}

emit_engine_gap() {
    local suite="$1"
    local engine="$2"
    local reason="$3"
    local metrics="$4"
    local artifact
    artifact="$(emit_not_available "$suite" "$engine" "$reason" "$metrics")"
    record_suite "$suite:$engine" "unavailable" 2 "$artifact"
    unavailable=1
}

emit_suite_gap() {
    local suite="$1"
    local reason="$2"
    local metrics="$3"
    local engine
    for engine in "${REQUESTED_ENGINES[@]}"; do
        emit_engine_gap "$suite" "$engine" "$reason" "$metrics"
    done
}

emit_unsupported_engines() {
    local suite="$1"
    local supported="$2"
    local metrics="$3"
    local engine
    for engine in "${REQUESTED_ENGINES[@]}"; do
        if ! contains_csv "$supported" "$engine"; then
            emit_engine_gap "$suite" "$engine" "runner_not_implemented_for_engine" "$metrics"
        fi
    done
}

run_suite() {
    local suite="$1"
    local artifact="$2"
    shift 2

    echo "=== arena suite: $suite profile=$PROFILE engines=$ENGINES ==="
    set +e
    "$@"
    local code=$?
    set -e

    case "$code" in
        0)
            record_suite "$suite" "passed" "$code" "$artifact"
            ;;
        2)
            record_suite "$suite" "unavailable" "$code" "$artifact"
            unavailable=1
            ;;
        *)
            record_suite "$suite" "failed" "$code" "$artifact"
            failed=1
            ;;
    esac
}

run_csv_suite() {
    local supported selected
    supported="ultrasql,duckdb,clickhouse"
    selected="$(requested_supported_csv "$supported")"
    if [[ -n "$selected" ]]; then
        run_suite \
            "csv" \
            "$OUT_DIR/csv_benchmark_gauntlet_manifest.json" \
            env \
                CSV_GAUNTLET_PROFILE="$PROFILE" \
                CSV_GAUNTLET_ENGINES="$selected" \
                CSV_GAUNTLET_OUT_DIR="$OUT_DIR" \
                RAW_DIR="$RAW_DIR" \
                benchmarks/csv_benchmark_gauntlet.sh "$PROFILE"
    fi
    emit_unsupported_engines \
        "csv" \
        "$supported" \
        "cold_read_us,warm_read_us,copy_import_us,group_by_us,filter_us,join_us,bad_row_behavior"
}

run_tpch_suite() {
    local supported selected
    supported="ultrasql,duckdb"
    selected="$(requested_supported_csv "$supported")"
    if [[ -n "$selected" ]]; then
        run_suite \
            "tpch" \
            "$OUT_DIR/tpch_sf10_certification.json" \
            env \
                BENCH_CERT_OUT_DIR="$OUT_DIR" \
                benchmarks/tpch_sf10_certify.sh
    fi
    emit_unsupported_engines \
        "tpch" \
        "$supported" \
        "query_median_ms,geomean_ms,load_time_ms"
}

run_clickbench_suite() {
    local supported selected
    supported="ultrasql,postgres"
    selected="$(requested_supported_csv "$supported")"
    if [[ -n "$selected" ]]; then
        run_suite \
            "clickbench" \
            "$OUT_DIR/clickbench_certification.json" \
            env \
                BENCH_CERT_OUT_DIR="$OUT_DIR" \
                benchmarks/clickbench_certify.sh
    fi
    emit_unsupported_engines \
        "clickbench" \
        "$supported" \
        "query_median_ms,geomean_ms,load_time_ms"
}

run_sqllogictest_suite() {
    local supported slt_refs refs=()
    supported="ultrasql,duckdb,postgres"
    if contains_csv "$ENGINES" "duckdb"; then
        refs+=("duckdb")
    fi
    if contains_csv "$ENGINES" "postgres"; then
        refs+=("postgres")
    fi
    # `slt_speed_compare.sh` defaults to sqlite+duckdb when the env var
    # is unset or empty. Pass the no-op `ultrasql` sentinel when the arena
    # request asks for UltraSQL-only replay.
    slt_refs="ultrasql"
    if (( ${#refs[@]} > 0 )); then
        slt_refs="$(join_space "${refs[@]}")"
    fi
    run_suite \
        "sqllogictest" \
        "$OUT_DIR/slt_speed_comparison.json" \
        env \
            SLT_BENCH_PROFILE="${SLT_BENCH_PROFILE:-release}" \
            SLT_BENCH_RUNS="${SLT_BENCH_RUNS:-5}" \
            SLT_BENCH_CASE_LIMIT="${SLT_BENCH_CASE_LIMIT:-50}" \
            SLT_BENCH_ENGINES="$slt_refs" \
            SLT_BENCH_OUT="$OUT_DIR/slt_speed_comparison.json" \
            benchmarks/slt_speed_compare.sh
    emit_unsupported_engines \
        "sqllogictest" \
        "$supported" \
        "case_count,passed,failed,median_us"
}

run_vector_suite() {
    local supported
    supported="ultrasql,duckdb,clickhouse,postgres"
    run_suite \
        "vector" \
        "$OUT_DIR/ai_benchmark_gauntlet_manifest.json" \
        env \
            AI_GAUNTLET_PROFILE="$PROFILE" \
            AI_GAUNTLET_OUT_DIR="$OUT_DIR" \
            RAW_DIR="$RAW_DIR" \
            benchmarks/ai_benchmark_gauntlet.sh "$PROFILE"
    emit_unsupported_engines \
        "vector" \
        "$supported" \
        "recall_at_k,p50_latency_us,p95_latency_us,p99_latency_us,build_time_us,memory_bytes"
}

suite_requested() {
    local needle="$1"
    local suite
    for suite in "${REQUESTED_SUITES[@]}"; do
        if [[ "$suite" == "$needle" ]]; then
            return 0
        fi
    done
    return 1
}

if suite_requested csv; then
    run_csv_suite
fi
if suite_requested parquet; then
    emit_suite_gap \
        "parquet" \
        "runner_not_implemented" \
        "scan_us,projection_pushdown_us,predicate_pushdown_us,row_group_pruning_us"
fi
if suite_requested tpch; then
    run_tpch_suite
fi
if suite_requested clickbench; then
    run_clickbench_suite
fi
if suite_requested sqllogictest; then
    run_sqllogictest_suite
fi
if suite_requested vector; then
    run_vector_suite
fi
if suite_requested jsonb; then
    emit_suite_gap \
        "jsonb" \
        "runner_not_implemented" \
        "ingest_rows_per_second,parse_us,query_median_us,shape_cache_hit_rate"
fi

python3 - "$PROFILE" "$ENGINES" "$SUITES" "$MANIFEST" "$ARTIFACTS_MD" "$STATUS_FILE" <<'PY'
import json
import pathlib
import sys
import time

profile, engines, suites, manifest_path, md_path, status_path = sys.argv[1:]
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
has_unavailable = any(entry["status"] == "unavailable" for entry in entries)
doc = {
    "schema_version": 1,
    "profile": profile,
    "requested_engines": [engine for engine in engines.split(",") if engine],
    "requested_suites": [suite for suite in suites.split(",") if suite],
    "generated_at_unix": int(time.time()),
    "status": "failed" if has_failed else "partial" if has_unavailable else "passed",
    "passed": not has_failed and not has_unavailable,
    "suites": entries,
    "policy": (
        "Public benchmark arena publishes artifacts only. It does not rank "
        "engines or make benchmark claims; each claim must come from the "
        "referenced raw artifact and its reproducible runner."
    ),
}

manifest = pathlib.Path(manifest_path)
manifest.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")

lines = [
    "# Benchmark Arena Artifacts",
    "",
    f"- profile: `{profile}`",
    f"- engines: `{engines}`",
    f"- suites: `{suites}`",
    "- policy: artifacts only; no rankings or winner claims",
    "",
    "| suite | status | exit | artifact |",
    "| --- | --- | ---: | --- |",
]
for entry in entries:
    lines.append(
        f"| `{entry['suite']}` | `{entry['status']}` | "
        f"{entry['exit_code']} | `{entry['artifact']}` |"
    )
pathlib.Path(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")

print(json.dumps(doc, indent=2))
PY

if (( failed != 0 )); then
    exit 1
fi
exit 0
