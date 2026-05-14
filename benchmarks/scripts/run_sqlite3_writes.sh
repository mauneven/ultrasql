#!/usr/bin/env bash
# run_sqlite3_writes.sh — measure write-side workloads against SQLite.
#
# Workloads:
#   insert_throughput_10k  — BEGIN; INSERT 10 000 rows; COMMIT
#   update_throughput_10k  — BEGIN; UPDATE 10 000 rows; COMMIT
#   delete_throughput_10k  — BEGIN; DELETE 10 000 rows; COMMIT
#   mixed_oltp_pgbench_like — 1-second window: 50% point reads, 30% updates,
#                             20% inserts; reports µs per op
#
# Uses PRAGMA journal_mode=MEMORY and PRAGMA synchronous=OFF for in-memory
# hot mode to match the DuckDB :memory: and UltraSQL in-memory profiles.
#
# Output: one JSON file per workload in $RAW_DIR:
#   <workload>-sqlite3.json
#
# Environment (with defaults):
#   RAW_DIR  (default: benchmarks/results/latest/raw)
#   N_ITERS  (default: 8)
#   N_ROWS   (default: 10000)

set -euo pipefail

ENGINE="sqlite3"
RAW_DIR="${RAW_DIR:-benchmarks/results/latest/raw}"
N_ITERS="${N_ITERS:-8}"
N_ROWS="${N_ROWS:-10000}"

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "run_sqlite3_writes.sh: WARNING: sqlite3 not found — skipping sqlite3 benchmarks" >&2
    for wl in insert_throughput_10k update_throughput_10k delete_throughput_10k mixed_oltp_pgbench_like; do
        echo "{\"engine\":\"${ENGINE}\",\"status\":\"not_available\",\"workload\":\"${wl}\"}" \
            > "${RAW_DIR}/${wl}-${ENGINE}.json"
    done
    exit 0
fi

mkdir -p "$RAW_DIR"

echo "run_sqlite3_writes.sh: SQLite $(sqlite3 --version 2>&1 | head -1) — N_ROWS=${N_ROWS} N_ITERS=${N_ITERS}"

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
# Common SQLite preamble (in-memory hot mode)
# ---------------------------------------------------------------------------
SQLITE_PREAMBLE="PRAGMA journal_mode=MEMORY;
PRAGMA synchronous=OFF;
PRAGMA temp_store=MEMORY;"

# ---------------------------------------------------------------------------
# Workload: insert_throughput_10k
# ---------------------------------------------------------------------------
run_insert() {
    local wl="insert_throughput_10k"
    echo "  workload: ${wl}"

    local values_sql
    values_sql="$(mktemp /tmp/sqlite_insert_XXXX.sql)"
    python3 - "$N_ROWS" "$values_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE TABLE bench_write(id INTEGER PRIMARY KEY, val INTEGER);\n")
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
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        printf '%s\n' "$SQLITE_PREAMBLE" | cat - "$values_sql" | sqlite3 :memory: >/dev/null
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

    # Full script (preload + update).
    local update_sql
    update_sql="$(mktemp /tmp/sqlite_update_XXXX.sql)"
    python3 - "$N_ROWS" "$update_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE TABLE bench_write(id INTEGER PRIMARY KEY, val INTEGER);\n")
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
    f.write("BEGIN;\n")
    f.write(f"UPDATE bench_write SET val = val + 1 WHERE id BETWEEN 0 AND {n-1};\n")
    f.write("COMMIT;\n")
PYEOF

    # Preload-only script.
    local preload_sql
    preload_sql="$(mktemp /tmp/sqlite_preload_XXXX.sql)"
    python3 - "$N_ROWS" "$preload_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE TABLE bench_write(id INTEGER PRIMARY KEY, val INTEGER);\n")
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
        printf '%s\n' "$SQLITE_PREAMBLE" | cat - "$update_sql" | sqlite3 :memory: >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_full+=("$dt")
    done

    local samples_preload=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        printf '%s\n' "$SQLITE_PREAMBLE" | cat - "$preload_sql" | sqlite3 :memory: >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_preload+=("$dt")
    done

    rm -f "$update_sql" "$preload_sql"

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

    local delete_sql
    delete_sql="$(mktemp /tmp/sqlite_delete_XXXX.sql)"
    python3 - "$N_ROWS" "$delete_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE TABLE bench_write(id INTEGER PRIMARY KEY, val INTEGER);\n")
    chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
    vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
    f.write("BEGIN;\n")
    f.write(f"DELETE FROM bench_write WHERE id BETWEEN 0 AND {n-1};\n")
    f.write("COMMIT;\n")
PYEOF

    local preload_sql
    preload_sql="$(mktemp /tmp/sqlite_preload2_XXXX.sql)"
    python3 - "$N_ROWS" "$preload_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
out = sys.argv[2]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE TABLE bench_write(id INTEGER PRIMARY KEY, val INTEGER);\n")
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
        printf '%s\n' "$SQLITE_PREAMBLE" | cat - "$delete_sql" | sqlite3 :memory: >/dev/null
        t1="$(python3 -c 'import time; print(time.perf_counter())')"
        dt="$(python3 -c "print(($t1 - $t0) * 1e6)")"
        samples_full+=("$dt")
    done

    local samples_preload=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        printf '%s\n' "$SQLITE_PREAMBLE" | cat - "$preload_sql" | sqlite3 :memory: >/dev/null
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
import subprocess, time, random, sys

n = int(sys.argv[1])
window = float(sys.argv[2])
seed = int(sys.argv[3])
rng = random.Random(0xBEEF + seed)

preamble = "PRAGMA journal_mode=MEMORY;\nPRAGMA synchronous=OFF;\nPRAGMA temp_store=MEMORY;\n"

# Build initial preload SQL.
ids = list(range(n))
rng2 = random.Random(0xC0FFEE)
rng2.shuffle(ids)
vals = [rng2.randint(-2**31, 2**31-1) for _ in range(n)]
preload_parts = ["CREATE TABLE bench_write(id INTEGER PRIMARY KEY, val INTEGER);"]
chunks = [ids[i:i+1000] for i in range(0, n, 1000)]
vchunks = [vals[i:i+1000] for i in range(0, n, 1000)]
for ch, vc in zip(chunks, vchunks):
    rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
    preload_parts.append(f"INSERT INTO bench_write(id,val) VALUES {rows};")
preload_sql = "\n".join(preload_parts)

def run_sqlite(extra_sql: str) -> None:
    sql = preamble + preload_sql + "\n" + extra_sql
    subprocess.run(["sqlite3", ":memory:"], input=sql, capture_output=True, text=True)

deadline = time.perf_counter() + window
count = 0
next_id = n

while time.perf_counter() < deadline:
    ops_batch = []
    for _ in range(20):
        r = rng.random()
        if r < 0.50:
            row_id = rng.randint(0, n - 1)
            ops_batch.append(f"SELECT val FROM bench_write WHERE id = {row_id};")
        elif r < 0.80:
            row_id = rng.randint(0, n - 1)
            ops_batch.append(f"UPDATE bench_write SET val = val + 1 WHERE id = {row_id};")
        else:
            new_val = rng.randint(-2**31, 2**31 - 1)
            ops_batch.append(f"INSERT INTO bench_write(id, val) VALUES ({next_id}, {new_val});")
            next_id += 1
    run_sqlite("\n".join(ops_batch))
    count += 20

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
# Main
# ---------------------------------------------------------------------------
run_insert
run_update
run_delete
run_mixed

echo "run_sqlite3_writes.sh: done — results in ${RAW_DIR}/"
