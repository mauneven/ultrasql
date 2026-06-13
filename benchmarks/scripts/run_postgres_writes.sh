#!/usr/bin/env bash
# run_postgres_writes.sh — measure write-side workloads against PostgreSQL.
#
# Workloads:
#   insert_throughput_10k  — BEGIN; INSERT 10 000 rows; COMMIT
#   update_throughput_10k  — BEGIN; UPDATE 10 000 rows; COMMIT
#   delete_throughput_10k  — BEGIN; DELETE 10 000 rows; COMMIT
#   mixed_oltp_pgbench_like — 1-second window: 50% point reads, 30% updates,
#                             20% inserts; reports ops/s converted to median_us
#   mixed_correctness_100k  — UPDATE + INSERT + aggregate
#                             with answer_sha256 emitted for cross-engine check
#   select_scan_10k        — preload 10 000 rows; time `SELECT id, val FROM t`
#                             draining the full result set
#   select_sum_65k_i64     — preload 65 536 rows; time `SELECT SUM(x) FROM
#                             bench_analytical`
#   select_avg_1m_i64      — preload 1 000 000 rows; time `SELECT AVG(x) FROM
#                             bench_analytical`
#   filter_sum_1m_i64      — preload 1 000 000 rows; time `SELECT SUM(x) FROM
#                             bench_analytical WHERE x > 5000000`
#
# Output: one JSON file per workload in $RAW_DIR:
#   <workload>-postgres.json
#
# An optional positional argument selects a single workload (e.g.
# `select_scan_10k`); with no argument all workloads run.
#
# Environment (with defaults):
#   PGHOST    (default: none — uses Unix socket)
#   PGUSER    (default: current user)
#   PGDATABASE (default: ultrasql_bench)
#   RAW_DIR   (default: benchmarks/results/latest/raw)
#   N_ITERS   (default: 8)
#   N_ROWS    (default: 10000)

set -euo pipefail

ENGINE="postgres"
RAW_DIR="${RAW_DIR:-benchmarks/results/latest/raw}"
N_ITERS="${N_ITERS:-8}"
N_ROWS="${N_ROWS:-10000}"
PGDATABASE="${PGDATABASE:-ultrasql_bench}"
PGUSER="${PGUSER:-$(id -un)}"
ANALYTICAL_ROWS="${ANALYTICAL_ROWS:-}"
INSERT_CHUNK_ROWS="${INSERT_CHUNK_ROWS:-10000}"

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

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! command -v psql >/dev/null 2>&1; then
    echo "run_postgres_writes.sh: WARNING: psql not found — skipping postgres benchmarks" >&2
    for wl in insert_throughput_10k update_throughput_10k delete_throughput_10k mixed_oltp_pgbench_like "mixed_correctness_$(row_suffix "$N_ROWS")" select_scan_10k select_sum_65k_i64 select_avg_1m_i64 filter_sum_1m_i64 window_row_number_65k_i64; do
        echo "{\"engine\":\"${ENGINE}\",\"status\":\"not_available\",\"workload\":\"${wl}\"}" \
            > "${RAW_DIR}/${wl}-${ENGINE}.json"
    done
    exit 0
fi

if ! pg_isready -q 2>/dev/null; then
    echo "run_postgres_writes.sh: WARNING: PostgreSQL not accepting connections — skipping" >&2
    for wl in insert_throughput_10k update_throughput_10k delete_throughput_10k mixed_oltp_pgbench_like "mixed_correctness_$(row_suffix "$N_ROWS")" select_scan_10k select_sum_65k_i64 select_avg_1m_i64 filter_sum_1m_i64 window_row_number_65k_i64; do
        echo "{\"engine\":\"${ENGINE}\",\"status\":\"not_available\",\"workload\":\"${wl}\"}" \
            > "${RAW_DIR}/${wl}-${ENGINE}.json"
    done
    exit 0
fi

# Validate connection.
if ! psql -U "$PGUSER" -d postgres -c "SELECT 1" -q --no-align -t >/dev/null 2>&1; then
    echo "run_postgres_writes.sh: WARNING: cannot connect to PostgreSQL as $PGUSER — skipping" >&2
    for wl in insert_throughput_10k update_throughput_10k delete_throughput_10k mixed_oltp_pgbench_like "mixed_correctness_$(row_suffix "$N_ROWS")" select_scan_10k select_sum_65k_i64 select_avg_1m_i64 filter_sum_1m_i64 window_row_number_65k_i64; do
        echo "{\"engine\":\"${ENGINE}\",\"status\":\"not_available\",\"workload\":\"${wl}\"}" \
            > "${RAW_DIR}/${wl}-${ENGINE}.json"
    done
    exit 0
fi

mkdir -p "$RAW_DIR"

ensure_database() {
    if createdb -U "$PGUSER" "$PGDATABASE" >/dev/null 2>&1; then
        return 0
    fi

    local exists
    local status
    set +e
    exists="$(psql -U "$PGUSER" -d postgres -q --no-align -t \
        --set=db="$PGDATABASE" 2>/dev/null <<'SQL'
SELECT 1 FROM pg_database WHERE datname = :'db';
SQL
)"
    status=$?
    set -e
    if [[ "$status" -eq 0 && "$exists" == "1" ]]; then
        return 0
    fi

    echo "run_postgres_writes.sh: ERROR: failed to create PostgreSQL database '$PGDATABASE'" >&2
    return 1
}

ensure_database

PSQL="psql -U $PGUSER -d $PGDATABASE -q --no-align -t"
PG_SERVER_VERSION="$($PSQL -c "SHOW server_version;" | xargs)"
PG_ENGINE_VERSION="PostgreSQL ${PG_SERVER_VERSION:-unknown}"

echo "run_postgres_writes.sh: ${PG_ENGINE_VERSION} — N_ROWS=${N_ROWS} N_ITERS=${N_ITERS}"

# ---------------------------------------------------------------------------
# Helper: compute median of space-separated microsecond values
# ---------------------------------------------------------------------------
compute_median() {
    python3 - "$@" <<'PYEOF'
import sys, statistics
vals = [float(x) for x in sys.argv[1:] if x]
if not vals:
    print("0")
else:
    print(statistics.median(vals))
PYEOF
}

# ---------------------------------------------------------------------------
# Helper: emit JSON record
# ---------------------------------------------------------------------------
emit_json() {
    local workload="$1"
    local n_rows="$2"
    local median_us="$3"
    shift 3
    # Remaining args: individual sample values
    local samples_json
    samples_json=$(python3 - "$@" <<'PYEOF'
import sys, json
vals = [float(x) for x in sys.argv[1:] if x]
print(json.dumps(vals))
PYEOF
)
    local n_samples
    n_samples="$#"
    local min_us
    min_us="$(python3 -c "import sys; vals=[float(x) for x in sys.argv[1:]]; print(min(vals) if vals else 0)" "$@")"
    python3 - "$ENGINE" "$PG_ENGINE_VERSION" "$workload" "$n_rows" "$n_samples" "$median_us" "$min_us" "$samples_json" <<'PYEOF'
import json
import sys

engine, version, workload, n_rows, samples, median_us, min_us, samples_json = sys.argv[1:]
doc = {
    "engine": engine,
    "engine_version": version,
    "workload": workload,
    "n_rows": int(n_rows),
    "samples": int(samples),
    "median_us": float(median_us),
    "min_us": float(min_us),
    "iterations_us": json.loads(samples_json),
}
print(json.dumps(doc, sort_keys=True))
PYEOF
}

annotate_json() {
    local path="$1"
    python3 - "$path" "$PG_ENGINE_VERSION" <<'PYEOF'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
doc = json.loads(path.read_text())
doc["engine_version"] = sys.argv[2]
path.write_text(json.dumps(doc, sort_keys=True) + "\n")
PYEOF
}

# ---------------------------------------------------------------------------
# Workload: insert_throughput_10k
# ---------------------------------------------------------------------------
run_insert() {
    local wl="insert_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    # Setup: ensure empty table.
    $PSQL <<SQL
DROP TABLE IF EXISTS bench_write;
CREATE UNLOGGED TABLE bench_write (id BIGINT PRIMARY KEY, val BIGINT);
SQL

    # Pre-generate values as a Python CSV to avoid shell loops.
    local values_file
    values_file="$(mktemp /tmp/pg_bench_insert_XXXXXXXX.sql)"
    python3 - "$N_ROWS" "$INSERT_CHUNK_ROWS" "$values_file" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
chunk_rows = int(sys.argv[2])
out = sys.argv[3]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("BEGIN;\n")
    # Build multi-row INSERT chunks for efficiency (matches the single-transaction benchmark).
    chunks = [ids[i:i+chunk_rows] for i in range(0, n, chunk_rows)]
    vchunks = [vals[i:i+chunk_rows] for i in range(0, n, chunk_rows)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
    f.write("COMMIT;\n")
PYEOF

    local samples=()
    for (( i=0; i<N_ITERS; i++ )); do
        # Truncate before each iteration so all iterations are equivalent.
        $PSQL -c "TRUNCATE bench_write;" >/dev/null

        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        $PSQL -f "$values_file" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples+=("$dt")
    done

    rm -f "$values_file"

    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Workload: update_throughput_10k
# ---------------------------------------------------------------------------
run_update() {
    local wl="update_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    # Preload N_ROWS rows once.
    $PSQL <<SQL
DROP TABLE IF EXISTS bench_write;
CREATE UNLOGGED TABLE bench_write (id BIGINT PRIMARY KEY, val BIGINT);
SQL
    python3 - "$N_ROWS" <<'PYEOF' | $PSQL >/dev/null
import sys
n = int(sys.argv[1])
import random
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
print("BEGIN;")
chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
for ch, vc in zip(chunks, vchunks):
    rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
    print(f"INSERT INTO bench_write(id,val) VALUES {rows};")
print("COMMIT;")
PYEOF

    local samples=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        $PSQL -c "BEGIN; UPDATE bench_write SET val = val + 1 WHERE id BETWEEN 0 AND $((N_ROWS - 1)); COMMIT;" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples+=("$dt")
    done

    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Workload: delete_throughput_10k
# ---------------------------------------------------------------------------
run_delete() {
    local wl="delete_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    # Generate insert SQL once.
    local insert_file
    insert_file="$(mktemp /tmp/pg_bench_delete_XXXXXXXX.sql)"
    python3 - "$N_ROWS" "$insert_file" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("BEGIN;\n")
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
    f.write("COMMIT;\n")
PYEOF

    local samples=()
    for (( i=0; i<N_ITERS; i++ )); do
        # Reload the table before each delete iteration.
        $PSQL -c "DROP TABLE IF EXISTS bench_write; CREATE UNLOGGED TABLE bench_write (id BIGINT PRIMARY KEY, val BIGINT);" >/dev/null
        $PSQL -f "$insert_file" >/dev/null

        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        $PSQL -c "BEGIN; DELETE FROM bench_write WHERE id BETWEEN 0 AND $((N_ROWS - 1)); COMMIT;" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples+=("$dt")
    done

    rm -f "$insert_file"

    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Workload: mixed_oltp_pgbench_like
# ---------------------------------------------------------------------------
run_mixed() {
    local wl="mixed_oltp_pgbench_like"
    echo "  workload: ${wl}"

    # Pre-populate a table with N_ROWS rows for reads/updates.
    $PSQL <<SQL
DROP TABLE IF EXISTS bench_write;
CREATE UNLOGGED TABLE bench_write (id BIGINT PRIMARY KEY, val BIGINT);
SQL
    python3 - "$N_ROWS" <<'PYEOF' | $PSQL >/dev/null
import sys
n = int(sys.argv[1])
import random
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
print("BEGIN;")
chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
for ch, vc in zip(chunks, vchunks):
    rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
    print(f"INSERT INTO bench_write(id,val) VALUES {rows};")
print("COMMIT;")
PYEOF

    # Run a 1-second window per iteration and count ops.
    local samples=()
    local window_secs=1
    for (( i=0; i<N_ITERS; i++ )); do
        local ops
        ops="$(python3 - "$N_ROWS" "$window_secs" <<PYEOF
import subprocess, time, random, sys

n = int(sys.argv[1])
window = float(sys.argv[2])
rng = random.Random(0xDEAD + $i)

cmd = ["psql", "-U", "$PGUSER", "-d", "$PGDATABASE", "-q", "--no-align", "-t"]

deadline = time.perf_counter() + window
count = 0
next_id = n  # next INSERT id starts past existing rows

while time.perf_counter() < deadline:
    r = rng.random()
    if r < 0.50:
        # Point read (50%)
        row_id = rng.randint(0, n - 1)
        sql = f"SELECT val FROM bench_write WHERE id = {row_id};"
    elif r < 0.80:
        # Update (30%)
        row_id = rng.randint(0, n - 1)
        sql = f"UPDATE bench_write SET val = val + 1 WHERE id = {row_id};"
    else:
        # Insert (20%) — use incrementing id to avoid PK conflicts
        new_val = rng.randint(-2**31, 2**31 - 1)
        sql = f"INSERT INTO bench_write(id, val) VALUES ({next_id}, {new_val}) ON CONFLICT DO NOTHING;"
        next_id += 1
    subprocess.run(cmd + ["-c", sql], capture_output=True)
    count += 1

print(count)
PYEOF
)"
        # Convert ops/window to median_us: us per op (lower = better).
        local us_per_op
        us_per_op="$(python3 -c "print($window_secs * 1e6 / max($ops, 1))")"
        samples+=("$us_per_op")
    done

    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs/op"
}

# ---------------------------------------------------------------------------
# Workload: mixed_correctness
# ---------------------------------------------------------------------------
run_mixed_correctness() {
    local wl="mixed_correctness_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"
    python3 benchmarks/scripts/run_mixed_correctness.py \
        --engine "$ENGINE" \
        --workload "$wl" \
        --rows "$N_ROWS" \
        --iters "$N_ITERS" \
        --pg-user "$PGUSER" \
        --pg-database "$PGDATABASE" \
        --out "${RAW_DIR}/${wl}-${ENGINE}.json"
    annotate_json "${RAW_DIR}/${wl}-${ENGINE}.json"
}

# ---------------------------------------------------------------------------
# Workload: select_scan_10k
# ---------------------------------------------------------------------------
run_select_scan() {
    local wl="select_scan_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    # Preload N_ROWS rows once, outside the timed region. Schema matches the
    # UltraSQL select-scan blueprint in cross_compare_sql.rs: (id INT, val INT).
    $PSQL <<SQL
DROP TABLE IF EXISTS bench_select_scan;
CREATE UNLOGGED TABLE bench_select_scan (id INT NOT NULL, val INT);
SQL
    python3 - "$N_ROWS" <<'PYEOF' | $PSQL >/dev/null
import sys
n = int(sys.argv[1])
print("BEGIN;")
chunks = [list(range(i, min(i + 1000, n))) for i in range(0, n, 1000)]
for ch in chunks:
    rows = ",".join(f"({j},{j * 10})" for j in ch)
    print(f"INSERT INTO bench_select_scan(id,val) VALUES {rows};")
print("COMMIT;")
PYEOF

    local samples=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt rowcount
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        rowcount="$($PSQL -c "SELECT id, val FROM bench_select_scan;" | wc -l | tr -d '[:space:]')"
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        if [[ "$rowcount" -ne "$N_ROWS" ]]; then
            echo "    WARNING: row count mismatch on iter $i: expected $N_ROWS, observed $rowcount" >&2
        fi
        samples+=("$dt")
    done

    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Helper: run a SELECT against bench_analytical with the given query.
# Args: workload_id, n_rows, query_sql
# Preloads bench_analytical(id INT, x INT) with rows (j, j*10) for j in 0..n,
# then times the query across N_ITERS iterations.
# ---------------------------------------------------------------------------
run_analytical() {
    local wl="$1"
    local n_rows="$2"
    local query="$3"
    echo "  workload: ${wl} (n_rows=${n_rows})"

    # Preload bench_analytical once. Schema matches the UltraSQL analytical
    # blueprint in cross_compare_sql.rs: (id INT, x INT) with x = j * 10.
    $PSQL <<SQL
DROP TABLE IF EXISTS bench_analytical;
CREATE UNLOGGED TABLE bench_analytical (id INT NOT NULL, x INT);
SQL
    python3 - "$n_rows" <<'PYEOF' | $PSQL >/dev/null
import sys
n = int(sys.argv[1])
print("BEGIN;")
chunks = [list(range(i, min(i + 1000, n))) for i in range(0, n, 1000)]
for ch in chunks:
    rows = ",".join(f"({j},{j * 10})" for j in ch)
    print(f"INSERT INTO bench_analytical(id,x) VALUES {rows};")
print("COMMIT;")
PYEOF

    local samples=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        $PSQL -c "$query" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples+=("$dt")
    done

    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$n_rows" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Workload: select_sum_65k_i64
# ---------------------------------------------------------------------------
run_sum_scalar() {
    local rows="${ANALYTICAL_ROWS:-65536}"
    run_analytical "select_sum_$(row_suffix "$rows")_i64" "$rows" \
        "SELECT SUM(x) FROM bench_analytical;"
}

# ---------------------------------------------------------------------------
# Workload: select_avg_1m_i64
# ---------------------------------------------------------------------------
run_avg_scalar() {
    local rows="${ANALYTICAL_ROWS:-1000000}"
    run_analytical "select_avg_$(row_suffix "$rows")_i64" "$rows" \
        "SELECT AVG(x) FROM bench_analytical;"
}

# ---------------------------------------------------------------------------
# Workload: filter_sum_1m_i64
# ---------------------------------------------------------------------------
run_filter_sum() {
    local rows="${ANALYTICAL_ROWS:-1000000}"
    local threshold=$((rows * 5))
    run_analytical "filter_sum_$(row_suffix "$rows")_i64" "$rows" \
        "SELECT SUM(x) FROM bench_analytical WHERE x > ${threshold};"
}

# ---------------------------------------------------------------------------
# Workload: window_row_number_65k_i64 — covers the v0.5 WindowAgg wire
# (`SELECT id, row_number() OVER (ORDER BY x) FROM t`). Same preload
# fixture as the SUM/AVG/FilterSum benches.
# ---------------------------------------------------------------------------
run_window_row_number() {
    local rows="${ANALYTICAL_ROWS:-65536}"
    run_analytical "window_row_number_$(row_suffix "$rows")_i64" "$rows" \
        "SELECT id, row_number() OVER (ORDER BY x) FROM bench_analytical;"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
WORKLOAD="${1:-all}"
case "$WORKLOAD" in
    insert_throughput_*)     run_insert ;;
    update_throughput_*)     run_update ;;
    delete_throughput_*)     run_delete ;;
    mixed_oltp_pgbench_like) run_mixed ;;
    mixed_correctness_*)     run_mixed_correctness ;;
    select_scan_*)           run_select_scan ;;
    select_sum_*_i64)        run_sum_scalar ;;
    select_avg_*_i64)        run_avg_scalar ;;
    filter_sum_*_i64)        run_filter_sum ;;
    window_row_number_65k_i64) run_window_row_number ;;
    all)
        run_insert
        run_update
        run_delete
        run_mixed
        run_mixed_correctness
        run_select_scan
        run_sum_scalar
        run_avg_scalar
        run_filter_sum
        run_window_row_number
        ;;
    *)
        echo "run_postgres_writes.sh: unknown workload '$WORKLOAD'" >&2
        exit 2
        ;;
esac

echo "run_postgres_writes.sh: done — results in ${RAW_DIR}/"
