#!/usr/bin/env bash
# run_duckdb_writes.sh — measure write-side workloads against DuckDB.
#
# Workloads:
#   insert_throughput_10k  — BEGIN TRANSACTION; INSERT 10 000 rows; COMMIT
#   update_throughput_10k  — BEGIN TRANSACTION; UPDATE 10 000 rows; COMMIT
#   delete_throughput_10k  — BEGIN TRANSACTION; DELETE 10 000 rows; COMMIT
#   mixed_oltp_pgbench_like — 1-second window: 50% point reads, 30% updates,
#                             20% inserts; reports µs per op
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
#   <workload>-duckdb.json
#
# An optional positional argument selects a single workload (e.g.
# `select_scan_10k`); with no argument all workloads run.
#
# Environment (with defaults):
#   RAW_DIR  (default: benchmarks/results/latest/raw)
#   N_ITERS  (default: 8)
#   N_ROWS   (default: 10000)

set -euo pipefail

ENGINE="duckdb"
RAW_DIR="${RAW_DIR:-benchmarks/results/latest/raw}"
N_ITERS="${N_ITERS:-8}"
N_ROWS="${N_ROWS:-10000}"

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! command -v duckdb >/dev/null 2>&1; then
    echo "run_duckdb_writes.sh: WARNING: duckdb not found — skipping duckdb benchmarks" >&2
    for wl in insert_throughput_10k update_throughput_10k delete_throughput_10k mixed_oltp_pgbench_like select_scan_10k select_sum_65k_i64 select_avg_1m_i64 filter_sum_1m_i64; do
        echo "{\"engine\":\"${ENGINE}\",\"status\":\"not_available\",\"workload\":\"${wl}\"}" \
            > "${RAW_DIR}/${wl}-${ENGINE}.json"
    done
    exit 0
fi

mkdir -p "$RAW_DIR"

echo "run_duckdb_writes.sh: DuckDB $(duckdb --version 2>&1 | head -1) — N_ROWS=${N_ROWS} N_ITERS=${N_ITERS}"

# ---------------------------------------------------------------------------
# Helper: compute median
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
    local samples_json
    samples_json=$(python3 - "$@" <<'PYEOF'
import sys, json
vals = [float(x) for x in sys.argv[1:] if x]
print(json.dumps(vals))
PYEOF
)
    local n_samples="$#"
    printf '{"engine":"%s","workload":"%s","n_rows":%d,"samples":%d,"median_us":%.3f,"min_us":%.3f,"iterations_us":%s}\n' \
        "$ENGINE" "$workload" "$n_rows" "$n_samples" "$median_us" \
        "$(python3 -c "import sys; vals=[float(x) for x in sys.argv[1:]]; print(min(vals) if vals else 0)" "$@")" \
        "$samples_json"
}

# ---------------------------------------------------------------------------
# Workload: insert_throughput_10k
# ---------------------------------------------------------------------------
run_insert() {
    local wl="insert_throughput_10k"
    echo "  workload: ${wl}"

    # Generate values CSV.
    local values_sql
    values_sql="$(mktemp /tmp/duckdb_insert_XXXX.sql)"
    python3 - "$N_ROWS" "$values_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE OR REPLACE TABLE bench_write(id BIGINT PRIMARY KEY, val BIGINT);\n")
    f.write("BEGIN TRANSACTION;\n")
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
    f.write("COMMIT;\n")
PYEOF

    local samples=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$values_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples+=("$dt")
    done

    rm -f "$values_sql"

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
    local wl="update_throughput_10k"
    echo "  workload: ${wl}"

    # Generate a pre-load + update script.
    local update_sql
    update_sql="$(mktemp /tmp/duckdb_update_XXXX.sql)"
    python3 - "$N_ROWS" "$update_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    # Preload
    f.write("CREATE OR REPLACE TABLE bench_write(id BIGINT PRIMARY KEY, val BIGINT);\n")
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
    # Timed UPDATE (DuckDB :memory: times the whole script; we measure
    # externally to separate preload from measured work).
    f.write("BEGIN TRANSACTION;\n")
    f.write(f"UPDATE bench_write SET val = val + 1 WHERE id BETWEEN 0 AND {n-1};\n")
    f.write("COMMIT;\n")
PYEOF

    # We need to separate preload cost from update cost.
    # Strategy: build the DB with preload in a tempfile, then feed a tiny
    # update-only script against it via stdin. DuckDB :memory: resets on
    # each invocation, so we measure the full CREATE + INSERT + UPDATE
    # and subtract the preload baseline measured separately.

    # Measure full script (preload + update).
    local samples_full=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$update_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_full+=("$dt")
    done

    # Measure preload-only baseline.
    local preload_sql
    preload_sql="$(mktemp /tmp/duckdb_preload_XXXX.sql)"
    python3 - "$N_ROWS" "$preload_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE OR REPLACE TABLE bench_write(id BIGINT PRIMARY KEY, val BIGINT);\n")
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
PYEOF

    local samples_preload=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$preload_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_preload+=("$dt")
    done

    rm -f "$update_sql" "$preload_sql"

    # Net samples: full - preload median subtracted per-sample.
    local preload_median
    preload_median="$(compute_median "${samples_preload[@]}")"
    local samples=()
    for dt_full in "${samples_full[@]}"; do
        local net
        net="$(python3 -c "v = $dt_full - $preload_median; print(max(v, 1.0))")"
        samples+=("$net")
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
    local wl="delete_throughput_10k"
    echo "  workload: ${wl}"

    # Generate a preload + delete script.
    local delete_sql
    delete_sql="$(mktemp /tmp/duckdb_delete_XXXX.sql)"
    python3 - "$N_ROWS" "$delete_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE OR REPLACE TABLE bench_write(id BIGINT PRIMARY KEY, val BIGINT);\n")
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
    f.write("BEGIN TRANSACTION;\n")
    f.write(f"DELETE FROM bench_write WHERE id BETWEEN 0 AND {n-1};\n")
    f.write("COMMIT;\n")
PYEOF

    # Measure preload baseline separately (same approach as update).
    local preload_sql
    preload_sql="$(mktemp /tmp/duckdb_preload2_XXXX.sql)"
    python3 - "$N_ROWS" "$preload_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE OR REPLACE TABLE bench_write(id BIGINT PRIMARY KEY, val BIGINT);\n")
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
PYEOF

    local samples_full=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$delete_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_full+=("$dt")
    done

    local samples_preload=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$preload_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_preload+=("$dt")
    done

    rm -f "$delete_sql" "$preload_sql"

    local preload_median
    preload_median="$(compute_median "${samples_preload[@]}")"
    local samples=()
    for dt_full in "${samples_full[@]}"; do
        local net
        net="$(python3 -c "v = $dt_full - $preload_median; print(max(v, 1.0))")"
        samples+=("$net")
    done

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

    local samples=()
    local window_secs=1
    for (( i=0; i<N_ITERS; i++ )); do
        local us_per_op
        us_per_op="$(python3 - "$N_ROWS" "$window_secs" "$i" <<'PYEOF'
import subprocess, time, random, sys, tempfile, os

n = int(sys.argv[1])
window = float(sys.argv[2])
seed = int(sys.argv[3])
rng = random.Random(0xBEEF + seed)

# Build initial DB in a tempfile-backed :memory: approach via stdin piping.
def run_duckdb(sql: str) -> str:
    r = subprocess.run(
        ["duckdb", ":memory:"],
        input=sql,
        capture_output=True,
        text=True,
    )
    return r.stdout

# Pre-populate table via a temp file that we can reuse as a preload.
ids = list(range(n))
rng2 = random.Random(0xC0FFEE)
rng2.shuffle(ids)
vals = [rng2.randint(-2**31, 2**31-1) for _ in range(n)]
preload_parts = ["CREATE OR REPLACE TABLE bench_write(id BIGINT PRIMARY KEY, val BIGINT);"]
chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
for ch, vc in zip(chunks, vchunks):
    rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
    preload_parts.append(f"INSERT INTO bench_write(id,val) VALUES {rows};")
preload_sql = "\n".join(preload_parts)

# DuckDB :memory: is stateless per-invocation, so we batch ops per call
# to amortize startup cost. Run 50-op batches for the window.
deadline = time.perf_counter() + window
count = 0
next_id = n

while time.perf_counter() < deadline:
    batch_ops = []
    for _ in range(50):
        r = rng.random()
        if r < 0.50:
            row_id = rng.randint(0, n - 1)
            batch_ops.append(f"SELECT val FROM bench_write WHERE id = {row_id};")
        elif r < 0.80:
            row_id = rng.randint(0, n - 1)
            batch_ops.append(f"UPDATE bench_write SET val = val + 1 WHERE id = {row_id};")
        else:
            new_val = rng.randint(-2**31, 2**31 - 1)
            batch_ops.append(f"INSERT INTO bench_write(id, val) VALUES ({next_id}, {new_val}) ON CONFLICT DO NOTHING;")
            next_id += 1
    sql = preload_sql + "\n" + "\n".join(batch_ops)
    run_duckdb(sql)
    count += 50

elapsed = time.perf_counter() - (deadline - window)
us_per_op = elapsed * 1e6 / max(count, 1)
print(us_per_op)
PYEOF
)"
        samples+=("$us_per_op")
    done

    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs/op"
}

# ---------------------------------------------------------------------------
# Workload: select_scan_10k
# ---------------------------------------------------------------------------
run_select_scan() {
    local wl="select_scan_10k"
    echo "  workload: ${wl}"

    # DuckDB :memory: is stateless per invocation; preload + scan in the same
    # script and subtract the preload-only baseline, matching the strategy
    # used by run_update / run_delete above.

    local scan_sql
    scan_sql="$(mktemp /tmp/duckdb_select_scan_XXXX.sql)"
    python3 - "$N_ROWS" "$scan_sql" <<'PYEOF'
import sys
n = int(sys.argv[1])
out = sys.argv[2]
with open(out, "w") as f:
    f.write("CREATE OR REPLACE TABLE bench_select_scan(id INTEGER NOT NULL, val INTEGER);\n")
    chunks = [list(range(i, min(i + 1000, n))) for i in range(0, n, 1000)]
    for ch in chunks:
        rows = ",".join(f"({j},{j * 10})" for j in ch)
        f.write(f"INSERT INTO bench_select_scan(id,val) VALUES {rows};\n")
    # Timed SELECT — emit all rows so the CLI drains the full result set.
    f.write("SELECT id, val FROM bench_select_scan;\n")
PYEOF

    local preload_sql
    preload_sql="$(mktemp /tmp/duckdb_select_preload_XXXX.sql)"
    python3 - "$N_ROWS" "$preload_sql" <<'PYEOF'
import sys
n = int(sys.argv[1])
out = sys.argv[2]
with open(out, "w") as f:
    f.write("CREATE OR REPLACE TABLE bench_select_scan(id INTEGER NOT NULL, val INTEGER);\n")
    chunks = [list(range(i, min(i + 1000, n))) for i in range(0, n, 1000)]
    for ch in chunks:
        rows = ",".join(f"({j},{j * 10})" for j in ch)
        f.write(f"INSERT INTO bench_select_scan(id,val) VALUES {rows};\n")
PYEOF

    local samples_full=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$scan_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_full+=("$dt")
    done

    local samples_preload=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$preload_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_preload+=("$dt")
    done

    rm -f "$scan_sql" "$preload_sql"

    local preload_median
    preload_median="$(compute_median "${samples_preload[@]}")"
    local samples=()
    for dt_full in "${samples_full[@]}"; do
        local net
        net="$(python3 -c "v = $dt_full - $preload_median; print(max(v, 1.0))")"
        samples+=("$net")
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
# DuckDB :memory: is stateless per invocation, so preload + query is timed
# together and a preload-only baseline is subtracted (same methodology as
# run_update / run_delete / run_select_scan above).
# ---------------------------------------------------------------------------
run_analytical() {
    local wl="$1"
    local n_rows="$2"
    local query="$3"
    echo "  workload: ${wl} (n_rows=${n_rows})"

    local scan_sql
    scan_sql="$(mktemp /tmp/duckdb_analytical_XXXX.sql)"
    python3 - "$n_rows" "$scan_sql" "$query" <<'PYEOF'
import sys
n = int(sys.argv[1])
out = sys.argv[2]
query = sys.argv[3]
with open(out, "w") as f:
    f.write("CREATE OR REPLACE TABLE bench_analytical(id INTEGER NOT NULL, x INTEGER);\n")
    chunks = [list(range(i, min(i + 1000, n))) for i in range(0, n, 1000)]
    for ch in chunks:
        rows = ",".join(f"({j},{j * 10})" for j in ch)
        f.write(f"INSERT INTO bench_analytical(id,x) VALUES {rows};\n")
    f.write(query + "\n")
PYEOF

    local preload_sql
    preload_sql="$(mktemp /tmp/duckdb_analytical_preload_XXXX.sql)"
    python3 - "$n_rows" "$preload_sql" <<'PYEOF'
import sys
n = int(sys.argv[1])
out = sys.argv[2]
with open(out, "w") as f:
    f.write("CREATE OR REPLACE TABLE bench_analytical(id INTEGER NOT NULL, x INTEGER);\n")
    chunks = [list(range(i, min(i + 1000, n))) for i in range(0, n, 1000)]
    for ch in chunks:
        rows = ",".join(f"({j},{j * 10})" for j in ch)
        f.write(f"INSERT INTO bench_analytical(id,x) VALUES {rows};\n")
PYEOF

    local samples_full=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$scan_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_full+=("$dt")
    done

    local samples_preload=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        duckdb :memory: < "$preload_sql" >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_preload+=("$dt")
    done

    rm -f "$scan_sql" "$preload_sql"

    local preload_median
    preload_median="$(compute_median "${samples_preload[@]}")"
    local samples=()
    for dt_full in "${samples_full[@]}"; do
        local net
        net="$(python3 -c "v = $dt_full - $preload_median; print(max(v, 1.0))")"
        samples+=("$net")
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
    run_analytical "select_sum_65k_i64" 65536 \
        "SELECT SUM(x) FROM bench_analytical;"
}

# ---------------------------------------------------------------------------
# Workload: select_avg_1m_i64
# ---------------------------------------------------------------------------
run_avg_scalar() {
    run_analytical "select_avg_1m_i64" 1000000 \
        "SELECT AVG(x) FROM bench_analytical;"
}

# ---------------------------------------------------------------------------
# Workload: filter_sum_1m_i64
# ---------------------------------------------------------------------------
run_filter_sum() {
    run_analytical "filter_sum_1m_i64" 1000000 \
        "SELECT SUM(x) FROM bench_analytical WHERE x > 5000000;"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
WORKLOAD="${1:-all}"
case "$WORKLOAD" in
    insert_throughput_10k)   run_insert ;;
    update_throughput_10k)   run_update ;;
    delete_throughput_10k)   run_delete ;;
    mixed_oltp_pgbench_like) run_mixed ;;
    select_scan_10k)         run_select_scan ;;
    select_sum_65k_i64)      run_sum_scalar ;;
    select_avg_1m_i64)       run_avg_scalar ;;
    filter_sum_1m_i64)       run_filter_sum ;;
    all)
        run_insert
        run_update
        run_delete
        run_mixed
        run_select_scan
        run_sum_scalar
        run_avg_scalar
        run_filter_sum
        ;;
    *)
        echo "run_duckdb_writes.sh: unknown workload '$WORKLOAD'" >&2
        exit 2
        ;;
esac

echo "run_duckdb_writes.sh: done — results in ${RAW_DIR}/"
