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
ENGINES="${LATE_MAT_ENGINES:-ultrasql-late,ultrasql-eager,duckdb,clickhouse}"

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

emit_not_available() {
    local engine="$1"
    local reason="$2"
    local out="$RAW_DIR/late_materialization_${row_label}-${engine}.json"
    python3 - "$out" "$engine" "$rows" "$row_label" "$warmup" "$iters" "$reason" <<'PY'
import json
import pathlib
import sys
import time

out, engine, rows, row_label, warmup, iters, reason = sys.argv[1:]
doc = {
    "schema_version": 1,
    "suite": "late_materialization",
    "engine": engine,
    "workload": f"late_materialization_{row_label}",
    "wide_columns": 100,
    "projected_columns": ["amount", "pad003", "pad096"],
    "dataset_rows": int(rows),
    "warmup": int(warmup),
    "iters": int(iters),
    "status": "not_available",
    "reason": reason,
    "median_us": None,
    "samples": 0,
    "generated_at_unix": int(time.time()),
    "policy": "No late-materialization competitor claim exists without measured raw samples.",
}
pathlib.Path(out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out)
PY
}

run_ultrasql() {
    target/release/cross_compare_sql \
        --workload late-materialization \
        --rows "$rows" \
        --warmup "$warmup" \
        --iters "$iters" \
        --output "$RAW_DIR/late_materialization_${row_label}-ultrasql.json"
}

run_duckdb() {
    local out="$RAW_DIR/late_materialization_${row_label}-duckdb.json"
    python3 - "$out" "$rows" "$row_label" "$warmup" "$iters" <<'PY'
import json
import math
import pathlib
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

out, rows, row_label, warmup, iters = sys.argv[1:]
rows = int(rows)
warmup = int(warmup)
iters = int(iters)
duckdb = shutil.which("duckdb")
if duckdb is None:
    raise SystemExit("duckdb missing")

def percentile_nearest_rank(values, percentile):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * percentile) - 1))
    return ordered[index]

def run(db, sql, csv=False):
    args = [duckdb, db]
    if csv:
        args.append("-csv")
    args.extend(["-c", sql])
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout

pad_exprs = []
for idx in range(1, 97):
    pad_exprs.append(f"'p{idx}_' || (row_id % {idx + 17})::VARCHAR AS pad{idx:03}")
setup_sql = (
    "CREATE TABLE late_mat AS SELECT "
    "row_id::INT AS id, "
    "(row_id % 32)::INT AS tenant_id, "
    "((row_id // 32) % 128)::INT AS bucket, "
    "((row_id * 19) % 2000 - 1000)::BIGINT AS amount, "
    + ", ".join(pad_exprs)
    + f" FROM range({rows}) AS r(row_id);"
)
query = "SELECT amount, pad003, pad096 FROM late_mat WHERE tenant_id = 7"

with tempfile.TemporaryDirectory(prefix="ultrasql-late-mat-duckdb-") as tmp:
    db = str(pathlib.Path(tmp) / "bench.duckdb")
    started = time.perf_counter()
    run(db, setup_sql)
    load_time_us = (time.perf_counter() - started) * 1_000_000.0
    samples = []
    result_row_count = 0
    for iteration in range(warmup + iters):
        started = time.perf_counter()
        output = run(db, query, csv=True)
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        if iteration >= warmup:
            samples.append(elapsed_us)
            lines = [line for line in output.splitlines() if line.strip()]
            result_row_count = max(0, len(lines) - 1)

version = subprocess.run([duckdb, "--version"], check=True, text=True, capture_output=True).stdout.strip()
doc = {
    "schema_version": 1,
    "suite": "late_materialization",
    "engine": "duckdb",
    "workload": f"late_materialization_{row_label}",
    "wide_columns": 100,
    "projected_columns": ["amount", "pad003", "pad096"],
    "dataset_rows": rows,
    "warmup": warmup,
    "iters": iters,
    "samples": len(samples),
    "median_us": statistics.median(samples),
    "p95_us": percentile_nearest_rank(samples, 0.95),
    "iterations_us": samples,
    "load_time_us": load_time_us,
    "result_row_count": result_row_count,
    "duckdb_version": version,
    "status": "measured",
    "policy": "DuckDB late-materialization competitor artifact is a same-shape local CLI measurement; no claim without raw artifact.",
}
pathlib.Path(out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out)
PY
}

ultrasql_ran=0
IFS=',' read -r -a REQUESTED_ENGINES <<<"$ENGINES"
for engine in "${REQUESTED_ENGINES[@]}"; do
    case "$engine" in
        ultrasql-late|ultrasql-eager|ultrasql)
            if (( ultrasql_ran == 0 )); then
                run_ultrasql
                ultrasql_ran=1
            fi
            ;;
        duckdb)
            if command -v duckdb >/dev/null 2>&1; then
                run_duckdb
            else
                emit_not_available duckdb "duckdb_unavailable"
            fi
            ;;
        clickhouse)
            if command -v clickhouse >/dev/null 2>&1 || command -v clickhouse-local >/dev/null 2>&1; then
                emit_not_available clickhouse "clickhouse_runner_pending"
            else
                emit_not_available clickhouse "clickhouse_unavailable"
            fi
            ;;
        *)
            emit_not_available "$engine" "unknown_engine"
            ;;
    esac
done
