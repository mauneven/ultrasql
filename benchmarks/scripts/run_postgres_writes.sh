#!/usr/bin/env bash
# run_postgres_writes.sh — measure scale-sweep workloads against PostgreSQL.
#
# Measurement runs through benchmarks/scripts/run_postgres_writes.py, which
# holds ONE persistent psycopg (v3) connection with server-side prepared
# statements across warmup + measured samples. No timed region spawns a `psql`
# process. mixed_correctness is delegated to run_mixed_correctness.py.
#
# Connection target comes from the standard libpq environment
# (PGHOST/PGPORT/PGUSER/PGDATABASE/PGPASSWORD). Point those at a tuned
# PostgreSQL 17 cluster — see benchmarks/scripts/pg17_bench_server.sh — for
# release-grade fairness. The default PATH `psql`/`postgres` may be an older
# version; the recorded engine_version always reflects the live server.
#
# Output: one JSON file per workload in $RAW_DIR: <workload>-postgres.json
#
# Environment (with defaults):
#   PGUSER     (default: current user)
#   PGDATABASE (default: ultrasql_bench)
#   RAW_DIR    (default: benchmarks/results/latest/raw)
#   N_ITERS    (default: 8)
#   WARMUP     (default: 2)
#   N_ROWS     (default: 10000)
#   ANALYTICAL_ROWS  row count for SUM/AVG/filter/window workloads

set -euo pipefail

ENGINE="postgres"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
RAW_DIR="${RAW_DIR:-benchmarks/results/latest/raw}"
N_ITERS="${N_ITERS:-8}"
WARMUP="${WARMUP:-2}"
N_ROWS="${N_ROWS:-10000}"
PGDATABASE="${PGDATABASE:-ultrasql_bench}"
PGUSER="${PGUSER:-$(id -un)}"
export PGDATABASE PGUSER
ANALYTICAL_ROWS="${ANALYTICAL_ROWS:-}"
BENCH_STORAGE_MODE="${BENCH_STORAGE_MODE:-memory}"
case "$BENCH_STORAGE_MODE" in
    memory|data-dir) ;;
    *) echo "run_postgres_writes.sh: unknown BENCH_STORAGE_MODE '$BENCH_STORAGE_MODE' (memory|data-dir)" >&2; exit 2 ;;
esac
BENCH_DATA_ROOT="${BENCH_DATA_ROOT:-$(dirname "$RAW_DIR")/data-dirs/competitors}"
if [[ "$BENCH_STORAGE_MODE" == "data-dir" ]]; then
    PG_DURABILITY_MODE="durable"
else
    PG_DURABILITY_MODE="volatile"
fi
PYTHON="${PYTHON:-python3}"
WORKLOAD="${1:-all}"

row_suffix() {
    local rows="$1"
    if [[ "$rows" -eq 65536 ]]; then
        echo "65k"
    elif [[ "$rows" -ge 1000000 && $((rows % 1000000)) -eq 0 ]]; then
        echo "$((rows / 1000000))m"
    elif [[ "$rows" -ge 1000 && $((rows % 1000)) -eq 0 ]]; then
        echo "$((rows / 1000))k"
    else
        echo "$rows"
    fi
}

analytical_rows_for() {
    local wl="$1"
    case "$wl" in
        select_sum_*_i64|window_row_number_*_i64) echo "${ANALYTICAL_ROWS:-65536}" ;;
        select_avg_*_i64|filter_sum_*_i64)        echo "${ANALYTICAL_ROWS:-1000000}" ;;
        *) echo "$N_ROWS" ;;
    esac
}

target_workloads() {
    case "$WORKLOAD" in
        all)
            echo "insert_throughput_$(row_suffix "$N_ROWS")"
            echo "update_throughput_$(row_suffix "$N_ROWS")"
            echo "delete_throughput_$(row_suffix "$N_ROWS")"
            echo "mixed_oltp_pgbench_like"
            echo "mixed_correctness_$(row_suffix "$N_ROWS")"
            echo "select_scan_$(row_suffix "$N_ROWS")"
            echo "select_sum_$(row_suffix "${ANALYTICAL_ROWS:-65536}")_i64"
            echo "select_avg_$(row_suffix "${ANALYTICAL_ROWS:-1000000}")_i64"
            echo "filter_sum_$(row_suffix "${ANALYTICAL_ROWS:-1000000}")_i64"
            echo "window_row_number_$(row_suffix "${ANALYTICAL_ROWS:-65536}")_i64"
            ;;
        insert_throughput_*)     echo "insert_throughput_$(row_suffix "$N_ROWS")" ;;
        update_throughput_*)     echo "update_throughput_$(row_suffix "$N_ROWS")" ;;
        delete_throughput_*)     echo "delete_throughput_$(row_suffix "$N_ROWS")" ;;
        mixed_oltp_pgbench_like) echo "mixed_oltp_pgbench_like" ;;
        mixed_correctness_*)     echo "mixed_correctness_$(row_suffix "$N_ROWS")" ;;
        select_scan_*)           echo "select_scan_$(row_suffix "$N_ROWS")" ;;
        select_sum_*_i64)        echo "select_sum_$(row_suffix "${ANALYTICAL_ROWS:-65536}")_i64" ;;
        select_avg_*_i64)        echo "select_avg_$(row_suffix "${ANALYTICAL_ROWS:-1000000}")_i64" ;;
        filter_sum_*_i64)        echo "filter_sum_$(row_suffix "${ANALYTICAL_ROWS:-1000000}")_i64" ;;
        window_row_number_*_i64) echo "window_row_number_$(row_suffix "${ANALYTICAL_ROWS:-65536}")_i64" ;;
        *) echo "run_postgres_writes.sh: unknown workload '$WORKLOAD'" >&2; exit 2 ;;
    esac
}

emit_unavailable() {
    local wl="$1"
    local rows="$2"
    local reason="$3"
    "$PYTHON" - "$RAW_DIR/${wl}-${ENGINE}.json" "$wl" "$rows" "$reason" \
        "$BENCH_STORAGE_MODE" "$PG_DURABILITY_MODE" <<'PY'
import json
import sys
from pathlib import Path

out, workload, rows, reason, storage_mode, durability_mode = sys.argv[1:]
doc = {
    "schema_version": 1,
    "engine": "postgres",
    "status": "not_available",
    "workload": workload,
    "n_rows": int(rows),
    "storage_mode": storage_mode,
    "durability_mode": durability_mode,
    "reason": reason,
    "policy": "No PostgreSQL benchmark claim exists until this artifact records measured samples from the same scale-sweep run.",
}
Path(out).parent.mkdir(parents=True, exist_ok=True)
Path(out).write_text(json.dumps(doc, sort_keys=True) + "\n")
PY
}

emit_unavailable_all() {
    local reason="$1"
    echo "run_postgres_writes.sh: WARNING: ${reason} — emitting not_available stubs" >&2
    mkdir -p "$RAW_DIR"
    while IFS= read -r wl; do
        local rows
        case "$wl" in
            select_sum_*_i64|window_row_number_*_i64) rows="${ANALYTICAL_ROWS:-65536}" ;;
            select_avg_*_i64|filter_sum_*_i64)        rows="${ANALYTICAL_ROWS:-1000000}" ;;
            *) rows="$N_ROWS" ;;
        esac
        emit_unavailable "$wl" "$rows" "$reason"
    done < <(target_workloads)
}

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------
if ! "$PYTHON" -c "import psycopg" >/dev/null 2>&1; then
    emit_unavailable_all "python psycopg module not installed"
    exit 0
fi

mkdir -p "$RAW_DIR"

# Ensure the benchmark database exists. createdb/psql honor PG* env, so a
# PATH client of any version reaches the configured server. Failure to reach
# the server is reported as not_available per workload by the Python driver.
if command -v createdb >/dev/null 2>&1; then
    createdb "$PGDATABASE" >/dev/null 2>&1 || true
fi

run_one() {
    local wl="$1"
    local rows="$2"
    echo "  workload: ${wl} (n_rows=${rows})"
    if [[ "$wl" == mixed_correctness_* ]]; then
        "$PYTHON" "$REPO_ROOT/benchmarks/scripts/run_mixed_correctness.py" \
            --engine "$ENGINE" \
            --workload "$wl" \
            --rows "$N_ROWS" \
            --iters "$N_ITERS" \
            --storage-mode "$BENCH_STORAGE_MODE" \
            --data-root "$BENCH_DATA_ROOT" \
            --pg-user "$PGUSER" \
            --pg-database "$PGDATABASE" \
            --out "$RAW_DIR/${wl}-${ENGINE}.json"
        "$PYTHON" - "$RAW_DIR/${wl}-${ENGINE}.json" "$BENCH_STORAGE_MODE" "$PG_DURABILITY_MODE" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
doc = json.loads(path.read_text())
doc["storage_mode"] = sys.argv[2]
doc["durability_mode"] = sys.argv[3]
path.write_text(json.dumps(doc, sort_keys=True) + "\n")
PY
        return
    fi
    "$PYTHON" "$REPO_ROOT/benchmarks/scripts/run_postgres_writes.py" \
        --workload "$wl" \
        --rows "$rows" \
        --iters "$N_ITERS" \
        --warmup "$WARMUP" \
        --storage-mode "$BENCH_STORAGE_MODE" \
        --out "$RAW_DIR/${wl}-${ENGINE}.json"
}

while IFS= read -r wl; do
    rows="$(analytical_rows_for "$wl")"
    run_one "$wl" "$rows"
done < <(target_workloads)

echo "run_postgres_writes.sh: done — results in ${RAW_DIR}/"
