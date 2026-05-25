#!/usr/bin/env bash
# Sysbench-style OLTP read/write certification runner.
#
# Full certification requires same-host UltraSQL and PostgreSQL 17 raw
# artifacts. A pass requires both engines to preserve row-count correctness and
# UltraSQL throughput to be at least 2x PostgreSQL throughput.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${SYSBENCH_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
ROWS="${SYSBENCH_ROWS:-10000}"
DURATION="${SYSBENCH_DURATION:-60}"
WARMUP="${SYSBENCH_WARMUP:-30}"
CONNECTIONS="${SYSBENCH_CONNECTIONS:-32}"
POSTGRES_DSN="${POSTGRES_DSN:-}"
ALLOW_ULTRASQL_ONLY="${SYSBENCH_ALLOW_ULTRASQL_ONLY:-0}"
TIMEOUT_SECONDS="${SYSBENCH_TIMEOUT_SECONDS:-600}"
SUMMARY_OUT="$OUT_DIR/sysbench_certification.json"
ULTRASQL_RESULT="$RAW_DIR/sysbench_oltp_read_write-ultrasql.json"
POSTGRES_RESULT="$RAW_DIR/sysbench_oltp_read_write-postgres17.json"
TMP_ULTRASQL_RESULT="$RAW_DIR/.sysbench_oltp_read_write-ultrasql.$$.tmp.json"
TMP_POSTGRES_RESULT="$RAW_DIR/.sysbench_oltp_read_write-postgres17.$$.tmp.json"

mkdir -p "$RAW_DIR"

cleanup() {
    rm -f "$TMP_ULTRASQL_RESULT" "$TMP_POSTGRES_RESULT"
}
trap cleanup EXIT

write_setup_summary() {
    local reason="$1"
    local detail="$2"
    python3 - "$SUMMARY_OUT" "$reason" "$detail" "$ROWS" "$DURATION" "$WARMUP" \
        "$CONNECTIONS" "$POSTGRES_DSN" "$ULTRASQL_RESULT" "$POSTGRES_RESULT" <<'PY'
import json
import pathlib
import sys

(
    summary_path,
    reason,
    detail,
    rows,
    duration,
    warmup,
    connections,
    postgres_dsn,
    ultrasql_result,
    postgres_result,
) = sys.argv[1:]
doc = {
    "schema_version": 1,
    "workload": "sysbench_oltp_read_write",
    "target": "UltraSQL throughput >= 2x PostgreSQL 17 throughput on the same sysbench-style OLTP read/write shape",
    "passed": False,
    "status": "not_available",
    "reason": reason,
    "detail": detail,
    "rows": int(rows),
    "duration_secs": int(duration),
    "warmup_secs": int(warmup),
    "connections": int(connections),
    "postgres_dsn_present": bool(postgres_dsn),
    "ultrasql_result": ultrasql_result,
    "postgres_result": postgres_result,
    "competitor_claim": None,
    "policy": "Full sysbench certification requires both raw artifacts; UltraSQL-only smoke is non-certifying.",
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
}

run_with_watchdog() {
    local label="$1"
    shift
    local timeout="$TIMEOUT_SECONDS"

    if [[ "$timeout" == "0" ]]; then
        "$@"
        return $?
    fi

    "$@" &
    local pid=$!
    local started
    started="$(date +%s)"

    while kill -0 "$pid" 2>/dev/null; do
        local now
        now="$(date +%s)"
        if (( now - started >= timeout )); then
            echo "$label exceeded SYSBENCH_TIMEOUT_SECONDS=$timeout; killing run." >&2
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            return 124
        fi
        sleep 2
    done

    wait "$pid"
}

if (( ROWS <= 0 || DURATION <= 0 || WARMUP < 0 || CONNECTIONS <= 0 )); then
    write_setup_summary "invalid_config" "SYSBENCH_ROWS, SYSBENCH_DURATION, SYSBENCH_WARMUP, or SYSBENCH_CONNECTIONS is invalid."
    exit 2
fi

if [[ -z "$POSTGRES_DSN" && "$ALLOW_ULTRASQL_ONLY" != "1" ]]; then
    write_setup_summary \
        "missing_cross_engine_results" \
        "POSTGRES_DSN is required for PostgreSQL 17 sysbench certification."
    exit 2
fi

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --features sql-bench \
        --bin ultrasql-bench >/dev/null

rm -f "$ULTRASQL_RESULT" "$POSTGRES_RESULT" "$TMP_ULTRASQL_RESULT" "$TMP_POSTGRES_RESULT"

if [[ -n "$POSTGRES_DSN" ]]; then
    echo "Running PostgreSQL 17 sysbench OLTP read/write"
    if ! run_with_watchdog \
        "PostgreSQL sysbench OLTP read/write" \
        target/release/ultrasql-bench sysbench \
            --engine postgres17 \
            --dsn "$POSTGRES_DSN" \
            --rows "$ROWS" \
            --duration "$DURATION" \
            --warmup "$WARMUP" \
            --connections "$CONNECTIONS" \
            --output "$TMP_POSTGRES_RESULT"; then
        write_setup_summary "postgres_run_failed" "PostgreSQL sysbench OLTP read/write did not complete."
        exit 1
    fi
fi

echo "Running UltraSQL sysbench OLTP read/write"
if ! run_with_watchdog \
    "UltraSQL sysbench OLTP read/write" \
    target/release/ultrasql-bench sysbench \
        --engine ultrasql \
        --rows "$ROWS" \
        --duration "$DURATION" \
        --warmup "$WARMUP" \
        --connections "$CONNECTIONS" \
        --output "$TMP_ULTRASQL_RESULT"; then
    write_setup_summary "ultrasql_run_failed" "UltraSQL sysbench OLTP read/write did not complete."
    exit 1
fi

if [[ -z "$POSTGRES_DSN" ]]; then
    mv "$TMP_ULTRASQL_RESULT" "$ULTRASQL_RESULT"
    python3 - "$SUMMARY_OUT" "$ULTRASQL_RESULT" "$ROWS" "$DURATION" "$WARMUP" "$CONNECTIONS" <<'PY'
import json
import math
import pathlib
import sys

summary_path, ultrasql_path, rows, duration, warmup, connections = sys.argv[1:]
ultra = json.loads(pathlib.Path(ultrasql_path).read_text())
correct = bool(ultra.get("correctness", {}).get("passed"))
throughput = float(ultra.get("throughput_per_sec", 0.0) or 0.0)
passed = correct and math.isfinite(throughput) and throughput > 0.0
doc = {
    "schema_version": 1,
    "workload": "sysbench_oltp_read_write",
    "target": "UltraSQL throughput >= 2x PostgreSQL 17 throughput on the same sysbench-style OLTP read/write shape",
    "passed": passed,
    "status": "smoke_passed" if passed else "failed",
    "reason": None if passed else "ultrasql_smoke_failed",
    "rows": int(rows),
    "duration_secs": int(duration),
    "warmup_secs": int(warmup),
    "connections": int(connections),
    "ultrasql_result": ultrasql_path,
    "postgres_result": None,
    "ultrasql_throughput_per_sec": throughput,
    "postgres_throughput_per_sec": None,
    "throughput_ratio_ultrasql_vs_postgres": None,
    "competitor_claim": None,
    "policy": "UltraSQL-only sysbench smoke is non-certifying; set POSTGRES_DSN for the 2x PostgreSQL gate.",
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(0 if passed else 1)
PY
    exit $?
fi

set +e
python3 - "$TMP_POSTGRES_RESULT" "$TMP_ULTRASQL_RESULT" "$POSTGRES_RESULT" "$ULTRASQL_RESULT" "$SUMMARY_OUT" "$ROWS" "$DURATION" "$WARMUP" "$CONNECTIONS" <<'PY'
import json
import math
import pathlib
import sys

(
    postgres_read_path,
    ultrasql_read_path,
    postgres_final_path,
    ultrasql_final_path,
    summary_path,
    rows,
    duration,
    warmup,
    connections,
) = sys.argv[1:]
postgres = json.loads(pathlib.Path(postgres_read_path).read_text())
ultrasql = json.loads(pathlib.Path(ultrasql_read_path).read_text())

pg_tps = float(postgres.get("throughput_per_sec", 0.0) or 0.0)
ultra_tps = float(ultrasql.get("throughput_per_sec", 0.0) or 0.0)
pg_correct = bool(postgres.get("correctness", {}).get("passed"))
ultra_correct = bool(ultrasql.get("correctness", {}).get("passed"))
pg_ops = int(postgres.get("operations", 0) or 0)
ultra_ops = int(ultrasql.get("operations", 0) or 0)
ratio = ultra_tps / pg_tps if pg_tps > 0.0 else None
valid = (
    pg_correct
    and ultra_correct
    and pg_ops > 0
    and ultra_ops > 0
    and math.isfinite(pg_tps)
    and math.isfinite(ultra_tps)
    and pg_tps > 0.0
    and ultra_tps > 0.0
)
passed = valid and ultra_tps >= pg_tps * 2.0
doc = {
    "schema_version": 1,
    "workload": "sysbench_oltp_read_write",
    "target": "UltraSQL throughput >= 2x PostgreSQL 17 throughput on the same sysbench-style OLTP read/write shape",
    "passed": passed,
    "status": "passed" if passed else "failed",
    "reason": None if passed else ("target_not_met" if valid else "missing_cross_engine_results"),
    "rows": int(rows),
    "duration_secs": int(duration),
    "warmup_secs": int(warmup),
    "connections": int(connections),
    "ultrasql_result": str(ultrasql_final_path),
    "postgres_result": str(postgres_final_path),
    "ultrasql_correct": ultra_correct,
    "postgres_correct": pg_correct,
    "ultrasql_operations": ultra_ops,
    "postgres_operations": pg_ops,
    "ultrasql_throughput_per_sec": ultra_tps,
    "postgres_throughput_per_sec": pg_tps,
    "throughput_ratio_ultrasql_vs_postgres": ratio,
    "competitor_claim": "UltraSQL >= 2x PostgreSQL 17" if passed else None,
    "policy": "Full sysbench certification publishes complete same-driver PostgreSQL and UltraSQL raw artifacts only.",
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(0 if passed else 1)
PY
summary_status=$?
set -e

mv "$TMP_POSTGRES_RESULT" "$POSTGRES_RESULT"
mv "$TMP_ULTRASQL_RESULT" "$ULTRASQL_RESULT"
exit "$summary_status"
