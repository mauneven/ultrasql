#!/usr/bin/env bash
# run_sqlite3_writes.sh — measure write-side workloads against SQLite.
#
# Workloads:
#   insert_throughput_10k  — BEGIN; INSERT 10 000 rows; COMMIT
#   update_throughput_10k  — BEGIN; UPDATE 10 000 rows; COMMIT
#   delete_throughput_10k  — BEGIN; DELETE 10 000 rows; COMMIT
#   mixed_oltp_pgbench_like — 1-second window: 50% point reads, 30% updates,
#                             20% inserts; reports µs per op
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
# Uses PRAGMA journal_mode=MEMORY and PRAGMA synchronous=OFF for in-memory
# hot mode to match the DuckDB :memory: and UltraSQL in-memory profiles.
#
# Output: one JSON file per workload in $RAW_DIR:
#   <workload>-sqlite3.json
#
# An optional positional argument selects a single workload (e.g.
# `select_scan_10k`); with no argument all workloads run.
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
ANALYTICAL_ROWS="${ANALYTICAL_ROWS:-}"
INSERT_CHUNK_ROWS="${INSERT_CHUNK_ROWS:-10000}"
BENCH_STORAGE_MODE="${BENCH_STORAGE_MODE:-memory}"
case "$BENCH_STORAGE_MODE" in
    memory|data-dir) ;;
    *) echo "run_sqlite3_writes.sh: unknown BENCH_STORAGE_MODE '$BENCH_STORAGE_MODE' (memory|data-dir)" >&2; exit 2 ;;
esac
BENCH_DATA_ROOT="${BENCH_DATA_ROOT:-$(dirname "$RAW_DIR")/data-dirs/competitors}"
if [[ "$BENCH_STORAGE_MODE" == "data-dir" ]]; then
    SQLITE_DURABILITY_MODE="durable"
else
    SQLITE_DURABILITY_MODE="volatile"
fi

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

target_stub_workloads() {
    echo "insert_throughput_$(row_suffix "$N_ROWS")"
    echo "update_throughput_$(row_suffix "$N_ROWS")"
    echo "delete_throughput_$(row_suffix "$N_ROWS")"
    echo "mixed_oltp_pgbench_like"
    echo "mixed_correctness_$(row_suffix "$N_ROWS")"
    echo "select_scan_$(row_suffix "$N_ROWS")"
    echo "select_sum_65k_i64"
    echo "select_avg_1m_i64"
    echo "filter_sum_1m_i64"
    echo "window_row_number_65k_i64"
}

workload_rows() {
    local wl="$1"
    case "$wl" in
        select_sum_*_i64|window_row_number_*_i64) echo 65536 ;;
        select_avg_*_i64|filter_sum_*_i64) echo 1000000 ;;
        *) echo "$N_ROWS" ;;
    esac
}

emit_unavailable_all() {
    local reason="$1"
    echo "run_sqlite3_writes.sh: WARNING: ${reason} — skipping sqlite3 benchmarks" >&2
    mkdir -p "$RAW_DIR"
    target_stub_workloads | while IFS= read -r wl; do
        local rows
        rows="$(workload_rows "$wl")"
        python3 - "$RAW_DIR/${wl}-${ENGINE}.json" "$ENGINE" "$wl" "$rows" "$reason" \
            "$BENCH_STORAGE_MODE" "$SQLITE_DURABILITY_MODE" <<'PY'
import json
import sys
from pathlib import Path

out, engine, workload, rows, reason, storage_mode, durability_mode = sys.argv[1:]
doc = {
    "schema_version": 1,
    "engine": engine,
    "status": "not_available",
    "workload": workload,
    "n_rows": int(rows),
    "storage_mode": storage_mode,
    "durability_mode": durability_mode,
    "reason": reason,
    "policy": "No SQLite benchmark claim exists until this artifact records measured samples from the same scale-sweep run.",
}
Path(out).write_text(json.dumps(doc, sort_keys=True) + "\n")
PY
    done
}

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! command -v sqlite3 >/dev/null 2>&1; then
    emit_unavailable_all "sqlite3 not found"
    exit 0
fi

mkdir -p "$RAW_DIR"

SQLITE_ENGINE_VERSION="$(sqlite3 --version 2>&1 | head -1)"
echo "run_sqlite3_writes.sh: SQLite ${SQLITE_ENGINE_VERSION} — N_ROWS=${N_ROWS} N_ITERS=${N_ITERS}"

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
    local min_us
    min_us="$(python3 -c "import sys; vals=[float(x) for x in sys.argv[1:]]; print(min(vals) if vals else 0)" "$@")"
    python3 - "$ENGINE" "$SQLITE_ENGINE_VERSION" "$workload" "$n_rows" "$n_samples" \
        "$median_us" "$min_us" "$samples_json" "$BENCH_STORAGE_MODE" "$SQLITE_DURABILITY_MODE" <<'PYEOF'
import json
import sys

engine, version, workload, n_rows, samples, median_us, min_us, samples_json, storage_mode, durability_mode = sys.argv[1:]
doc = {
    "schema_version": 1,
    "engine": engine,
    "engine_version": version,
    "workload": workload,
    "status": "measured",
    "n_rows": int(n_rows),
    "storage_mode": storage_mode,
    "durability_mode": durability_mode,
    "samples": int(samples),
    "median_us": float(median_us),
    "min_us": float(min_us),
    "iterations_us": json.loads(samples_json),
    "policy": "Raw measured samples only; no ranking or winner claim.",
}
print(json.dumps(doc, sort_keys=True))
PYEOF
}

annotate_profile_json() {
    local path="$1"
    python3 - "$path" "$BENCH_STORAGE_MODE" "$SQLITE_DURABILITY_MODE" <<'PYEOF'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
doc = json.loads(path.read_text())
doc["storage_mode"] = sys.argv[2]
doc["durability_mode"] = sys.argv[3]
path.write_text(json.dumps(doc, sort_keys=True) + "\n")
PYEOF
}

# ---------------------------------------------------------------------------
# Common SQLite preamble.
# ---------------------------------------------------------------------------
if [[ "$BENCH_STORAGE_MODE" == "data-dir" ]]; then
    SQLITE_PREAMBLE="PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA temp_store=DEFAULT;"
else
    SQLITE_PREAMBLE="PRAGMA journal_mode=MEMORY;
PRAGMA synchronous=OFF;
PRAGMA temp_store=MEMORY;"
fi

sqlite_db_path() {
    local workload="$1"
    local sample="$2"
    if [[ "$BENCH_STORAGE_MODE" == "memory" ]]; then
        echo ":memory:"
        return
    fi
    local dir="$BENCH_DATA_ROOT/sqlite3"
    mkdir -p "$dir"
    echo "$dir/${workload}-${sample}.sqlite3"
}

reset_sqlite_db() {
    local db_path="$1"
    if [[ "$db_path" != ":memory:" ]]; then
        rm -f "$db_path" "$db_path-wal" "$db_path-shm"
    fi
}

# ---------------------------------------------------------------------------
# Workload: insert_throughput_10k
# ---------------------------------------------------------------------------
run_insert() {
    local wl="insert_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    local values_sql
    values_sql="$(mktemp /tmp/sqlite_insert_XXXX.sql)"
    python3 - "$N_ROWS" "$INSERT_CHUNK_ROWS" "$values_sql" <<'PYEOF'
import sys, random
n = int(sys.argv[1])
chunk_rows = int(sys.argv[2])
out = sys.argv[3]
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31-1) for _ in range(n)]
with open(out, "w") as f:
    f.write("CREATE TABLE bench_write(id INTEGER NOT NULL, val INTEGER);\n")
    f.write("BEGIN;\n")
    chunks = [ids[i:i+chunk_rows] for i in range(0, n, chunk_rows)]
    vchunks = [vals[i:i+chunk_rows] for i in range(0, n, chunk_rows)]
    for ch, vc in zip(chunks, vchunks):
        rows = ",".join(f"({i},{v})" for i, v in zip(ch, vc))
        f.write(f"INSERT INTO bench_write(id,val) VALUES {rows};\n")
    f.write("COMMIT;\n")
PYEOF

    local samples=()
    for (( i=0; i<N_ITERS; i++ )); do
        local t0 t1 dt db_path
        db_path="$(sqlite_db_path "$wl" "$i")"
        reset_sqlite_db "$db_path"
        t0="$(python3 -c 'import time; print(time.perf_counter())')"
        printf '%s\n' "$SQLITE_PREAMBLE" | cat - "$values_sql" | sqlite3 "$db_path" >/dev/null
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
    local wl="update_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    # Persistent in-process connection via the Python sqlite3 driver:
    # preload N_ROWS once outside the timed region, then per iteration
    # time `BEGIN; UPDATE all rows; ROLLBACK;` so each sample measures
    # the same UPDATE against the same row image. Avoids the
    # subtract-two-process methodology that clamped against
    # `max(v, 1.0)` and blew variance on sub-millisecond queries.
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time, random
import sqlite3
from pathlib import Path

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
storage_mode = sys.argv[3]
data_root = Path(sys.argv[4])
workload = sys.argv[5]

rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-(2**31), 2**31 - 1) for _ in range(n)]

if storage_mode == "data-dir":
    db_path = data_root / "sqlite3" / f"{workload}-{os.getpid()}.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = sqlite3.connect(str(db_path), isolation_level=None)
if storage_mode == "data-dir":
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=FULL;")
    con.execute("PRAGMA temp_store=DEFAULT;")
else:
    con.execute("PRAGMA journal_mode=MEMORY;")
    con.execute("PRAGMA synchronous=OFF;")
    con.execute("PRAGMA temp_store=MEMORY;")
con.execute("CREATE TABLE bench_write(id INTEGER NOT NULL, val INTEGER);")
con.execute("BEGIN;")
con.executemany(
    "INSERT INTO bench_write(id,val) VALUES (?, ?);",
    list(zip(ids, vals)),
)
con.execute("COMMIT;")

for _ in range(2):
    con.execute("BEGIN;")
    con.execute(f"UPDATE bench_write SET val = val + 1 WHERE id BETWEEN 0 AND {n-1};")
    con.execute("ROLLBACK;")

for _ in range(n_iters):
    con.execute("BEGIN;")
    t0 = time.perf_counter()
    con.execute(f"UPDATE bench_write SET val = val + 1 WHERE id BETWEEN 0 AND {n-1};")
    t1 = time.perf_counter()
    con.execute("ROLLBACK;")
    print((t1 - t0) * 1e6)
PYEOF
)"

    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"

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

    # Persistent in-process connection via the Python sqlite3 driver:
    # preload N_ROWS once outside the timed region, then per iteration
    # time `BEGIN; DELETE all rows; ROLLBACK;` so each sample measures
    # the same DELETE against the same row image. Avoids the
    # subtract-two-process methodology and its `max(v, 1.0)` clamp.
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time, random
import sqlite3
from pathlib import Path

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
storage_mode = sys.argv[3]
data_root = Path(sys.argv[4])
workload = sys.argv[5]

rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-(2**31), 2**31 - 1) for _ in range(n)]

if storage_mode == "data-dir":
    db_path = data_root / "sqlite3" / f"{workload}-{os.getpid()}.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = sqlite3.connect(str(db_path), isolation_level=None)
if storage_mode == "data-dir":
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=FULL;")
    con.execute("PRAGMA temp_store=DEFAULT;")
else:
    con.execute("PRAGMA journal_mode=MEMORY;")
    con.execute("PRAGMA synchronous=OFF;")
    con.execute("PRAGMA temp_store=MEMORY;")
con.execute("CREATE TABLE bench_write(id INTEGER NOT NULL, val INTEGER);")
con.execute("BEGIN;")
con.executemany(
    "INSERT INTO bench_write(id,val) VALUES (?, ?);",
    list(zip(ids, vals)),
)
con.execute("COMMIT;")

for _ in range(2):
    con.execute("BEGIN;")
    con.execute(f"DELETE FROM bench_write WHERE id BETWEEN 0 AND {n-1};")
    con.execute("ROLLBACK;")

for _ in range(n_iters):
    con.execute("BEGIN;")
    t0 = time.perf_counter()
    con.execute(f"DELETE FROM bench_write WHERE id BETWEEN 0 AND {n-1};")
    t1 = time.perf_counter()
    con.execute("ROLLBACK;")
    print((t1 - t0) * 1e6)
PYEOF
)"

    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"

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
        us_per_op="$(python3 - "$N_ROWS" "$window_secs" "$i" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import subprocess, time, random, sys
from pathlib import Path

n = int(sys.argv[1])
window = float(sys.argv[2])
seed = int(sys.argv[3])
storage_mode = sys.argv[4]
data_root = Path(sys.argv[5])
workload = sys.argv[6]
rng = random.Random(0xBEEF + seed)

if storage_mode == "data-dir":
    preamble = "PRAGMA journal_mode=WAL;\nPRAGMA synchronous=FULL;\nPRAGMA temp_store=DEFAULT;\n"
else:
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

def sqlite_target(batch: int) -> str:
    if storage_mode != "data-dir":
        return ":memory:"
    db_path = data_root / "sqlite3" / f"{workload}-{seed}-{batch}.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
    return str(db_path)

def run_sqlite(extra_sql: str, batch: int) -> None:
    sql = preamble + preload_sql + "\n" + extra_sql
    subprocess.run(["sqlite3", sqlite_target(batch)], input=sql, capture_output=True, text=True)

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
    run_sqlite("\n".join(ops_batch), count)
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
        --storage-mode "$BENCH_STORAGE_MODE" \
        --data-root "$BENCH_DATA_ROOT" \
        --out "${RAW_DIR}/${wl}-${ENGINE}.json"
    annotate_profile_json "${RAW_DIR}/${wl}-${ENGINE}.json"
}

# ---------------------------------------------------------------------------
# Workload: select_scan_10k
# ---------------------------------------------------------------------------
run_select_scan() {
    local wl="select_scan_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    # Persistent in-process connection via the Python sqlite3 driver:
    # preload N_ROWS once outside the timed region, then time a full
    # `SELECT id, val FROM bench_select_scan` that drains every row via
    # `fetchall()`. Avoids the subtract-two-process methodology that
    # clamped fast scans against `max(v, 1.0)`.
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time
import sqlite3
from pathlib import Path

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
storage_mode = sys.argv[3]
data_root = Path(sys.argv[4])
workload = sys.argv[5]

if storage_mode == "data-dir":
    db_path = data_root / "sqlite3" / f"{workload}-{os.getpid()}.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = sqlite3.connect(str(db_path), isolation_level=None)
if storage_mode == "data-dir":
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=FULL;")
    con.execute("PRAGMA temp_store=DEFAULT;")
else:
    con.execute("PRAGMA journal_mode=MEMORY;")
    con.execute("PRAGMA synchronous=OFF;")
    con.execute("PRAGMA temp_store=MEMORY;")
con.execute("CREATE TABLE bench_select_scan(id INTEGER NOT NULL, val INTEGER);")
con.execute("BEGIN;")
con.executemany(
    "INSERT INTO bench_select_scan(id,val) VALUES (?, ?);",
    [(j, j * 10) for j in range(n)],
)
con.execute("COMMIT;")

for _ in range(2):
    con.execute("SELECT id, val FROM bench_select_scan;").fetchall()

for _ in range(n_iters):
    t0 = time.perf_counter()
    rows = con.execute("SELECT id, val FROM bench_select_scan;").fetchall()
    t1 = time.perf_counter()
    if len(rows) != n:
        sys.stderr.write(f"run_select_scan: row mismatch (got {len(rows)}, expected {n})\n")
    print((t1 - t0) * 1e6)
PYEOF
)"

    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"

    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Helper: run a SELECT against bench_analytical with the given query.
# Args: workload_id, n_rows, query_sql
# Uses the Python sqlite3 driver: opens an in-memory connection once, runs
# the preload outside the timed region, then times the query across
# N_ITERS iterations with `.fetchall()` to force materialization.
# Avoids the subtract-two-process methodology that clamped sub-ms queries
# against `max(v, 1.0)`.
# ---------------------------------------------------------------------------
run_analytical() {
    local wl="$1"
    local n_rows="$2"
    local query="$3"
    echo "  workload: ${wl} (n_rows=${n_rows})"

    local samples_raw
    samples_raw="$(python3 - "$n_rows" "$query" "$N_ITERS" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time
import sqlite3
from pathlib import Path

n = int(sys.argv[1])
query = sys.argv[2]
n_iters = int(sys.argv[3])
storage_mode = sys.argv[4]
data_root = Path(sys.argv[5])
workload = sys.argv[6]

if storage_mode == "data-dir":
    db_path = data_root / "sqlite3" / f"{workload}-{os.getpid()}.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = sqlite3.connect(str(db_path), isolation_level=None)
if storage_mode == "data-dir":
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=FULL;")
    con.execute("PRAGMA temp_store=DEFAULT;")
else:
    con.execute("PRAGMA journal_mode=MEMORY;")
    con.execute("PRAGMA synchronous=OFF;")
    con.execute("PRAGMA temp_store=MEMORY;")
con.execute("CREATE TABLE bench_analytical(id INTEGER NOT NULL, x INTEGER);")
con.execute("BEGIN;")
con.executemany(
    "INSERT INTO bench_analytical(id,x) VALUES (?, ?);",
    [(j, j * 10) for j in range(n)],
)
con.execute("COMMIT;")

for _ in range(2):
    con.execute(query).fetchall()

for _ in range(n_iters):
    t0 = time.perf_counter()
    rows = con.execute(query).fetchall()
    t1 = time.perf_counter()
    if not rows:
        sys.stderr.write("run_analytical: empty result set\n")
    print((t1 - t0) * 1e6)
PYEOF
)"

    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"

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
# Workload: window_row_number_65k_i64
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
    insert_throughput_10k)   run_insert ;;
    update_throughput_10k)   run_update ;;
    delete_throughput_10k)   run_delete ;;
    mixed_oltp_pgbench_like) run_mixed ;;
    mixed_correctness_*)     run_mixed_correctness ;;
    select_scan_10k)         run_select_scan ;;
    select_sum_65k_i64)      run_sum_scalar ;;
    select_avg_1m_i64)       run_avg_scalar ;;
    filter_sum_1m_i64)       run_filter_sum ;;
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
        echo "run_sqlite3_writes.sh: unknown workload '$WORKLOAD'" >&2
        exit 2
        ;;
esac

echo "run_sqlite3_writes.sh: done — results in ${RAW_DIR}/"
