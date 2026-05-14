#!/usr/bin/env bash
# run_postgres_writes.sh — measure write-side workloads against PostgreSQL.
#
# Workloads:
#   insert_throughput_10k  — BEGIN; INSERT 10 000 rows; COMMIT
#   update_throughput_10k  — BEGIN; UPDATE 10 000 rows; COMMIT
#   delete_throughput_10k  — BEGIN; DELETE 10 000 rows; COMMIT
#   mixed_oltp_pgbench_like — 1-second window: 50% point reads, 30% updates,
#                             20% inserts; reports ops/s converted to median_us
#
# Output: one JSON file per workload in $RAW_DIR:
#   <workload>-postgres17.json
#
# Environment (with defaults):
#   PGHOST    (default: none — uses Unix socket)
#   PGUSER    (default: current user)
#   PGDATABASE (default: ultrasql_bench)
#   RAW_DIR   (default: benchmarks/results/latest/raw)
#   N_ITERS   (default: 8)
#   N_ROWS    (default: 10000)

set -euo pipefail

ENGINE="postgres17"
RAW_DIR="${RAW_DIR:-benchmarks/results/latest/raw}"
N_ITERS="${N_ITERS:-8}"
N_ROWS="${N_ROWS:-10000}"
PGDATABASE="${PGDATABASE:-ultrasql_bench}"
PGUSER="${PGUSER:-$(id -un)}"

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! command -v psql >/dev/null 2>&1; then
    echo "run_postgres_writes.sh: WARNING: psql not found — skipping postgres17 benchmarks" >&2
    for wl in insert_throughput_10k update_throughput_10k delete_throughput_10k mixed_oltp_pgbench_like; do
        echo "{\"engine\":\"${ENGINE}\",\"status\":\"not_available\",\"workload\":\"${wl}\"}" \
            > "${RAW_DIR}/${wl}-${ENGINE}.json"
    done
    exit 0
fi

if ! pg_isready -q 2>/dev/null; then
    echo "run_postgres_writes.sh: WARNING: PostgreSQL not accepting connections — skipping" >&2
    for wl in insert_throughput_10k update_throughput_10k delete_throughput_10k mixed_oltp_pgbench_like; do
        echo "{\"engine\":\"${ENGINE}\",\"status\":\"not_available\",\"workload\":\"${wl}\"}" \
            > "${RAW_DIR}/${wl}-${ENGINE}.json"
    done
    exit 0
fi

# Validate connection.
if ! psql -U "$PGUSER" -d postgres -c "SELECT 1" -q --no-align -t >/dev/null 2>&1; then
    echo "run_postgres_writes.sh: WARNING: cannot connect to PostgreSQL as $PGUSER — skipping" >&2
    for wl in insert_throughput_10k update_throughput_10k delete_throughput_10k mixed_oltp_pgbench_like; do
        echo "{\"engine\":\"${ENGINE}\",\"status\":\"not_available\",\"workload\":\"${wl}\"}" \
            > "${RAW_DIR}/${wl}-${ENGINE}.json"
    done
    exit 0
fi

mkdir -p "$RAW_DIR"

# Create/ensure bench database.
createdb -U "$PGUSER" "$PGDATABASE" 2>/dev/null || true

PSQL="psql -U $PGUSER -d $PGDATABASE -q --no-align -t"

echo "run_postgres_writes.sh: PostgreSQL ${ENGINE} — N_ROWS=${N_ROWS} N_ITERS=${N_ITERS}"

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

    # Setup: ensure empty table.
    $PSQL <<SQL
DROP TABLE IF EXISTS bench_write;
CREATE UNLOGGED TABLE bench_write (id BIGINT PRIMARY KEY, val BIGINT);
SQL

    # Pre-generate values as a Python CSV to avoid shell loops.
    local values_file
    values_file="$(mktemp /tmp/pg_bench_insert_XXXX.sql)"
    python3 - "$N_ROWS" "$values_file" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("BEGIN;\n")
    # Build one multi-row INSERT for efficiency (matches the single-transaction benchmark).
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
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
    local wl="update_throughput_10k"
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
    local wl="delete_throughput_10k"
    echo "  workload: ${wl}"

    # Generate insert SQL once.
    local insert_file
    insert_file="$(mktemp /tmp/pg_bench_delete_XXXX.sql)"
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
# Main
# ---------------------------------------------------------------------------
run_insert
run_update
run_delete
run_mixed

echo "run_postgres_writes.sh: done — results in ${RAW_DIR}/"
