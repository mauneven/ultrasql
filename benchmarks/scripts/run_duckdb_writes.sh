#!/usr/bin/env bash
# run_duckdb_writes.sh — measure write-side workloads against DuckDB.
#
# Workloads:
#   insert_throughput_10k  — BEGIN TRANSACTION; INSERT 10 000 rows; COMMIT
#   update_throughput_10k  — BEGIN TRANSACTION; UPDATE 10 000 rows; COMMIT
#   delete_throughput_10k  — BEGIN TRANSACTION; DELETE 10 000 rows; COMMIT
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
ANALYTICAL_ROWS="${ANALYTICAL_ROWS:-}"
INSERT_CHUNK_ROWS="${INSERT_CHUNK_ROWS:-10000}"
BENCH_STORAGE_MODE="${BENCH_STORAGE_MODE:-memory}"
case "$BENCH_STORAGE_MODE" in
    memory|data-dir) ;;
    *) echo "run_duckdb_writes.sh: unknown BENCH_STORAGE_MODE '$BENCH_STORAGE_MODE' (memory|data-dir)" >&2; exit 2 ;;
esac
BENCH_DATA_ROOT="${BENCH_DATA_ROOT:-$(dirname "$RAW_DIR")/data-dirs/competitors}"
if [[ "$BENCH_STORAGE_MODE" == "data-dir" ]]; then
    DUCKDB_DURABILITY_MODE="durable"
else
    DUCKDB_DURABILITY_MODE="volatile"
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
    echo "run_duckdb_writes.sh: WARNING: ${reason} — skipping duckdb benchmarks" >&2
    mkdir -p "$RAW_DIR"
    target_stub_workloads | while IFS= read -r wl; do
        local rows
        rows="$(workload_rows "$wl")"
        python3 - "$RAW_DIR/${wl}-${ENGINE}.json" "$ENGINE" "$wl" "$rows" "$reason" \
            "$BENCH_STORAGE_MODE" "$DUCKDB_DURABILITY_MODE" <<'PY'
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
    "policy": "No DuckDB benchmark claim exists until this artifact records measured samples from the same scale-sweep run.",
}
Path(out).write_text(json.dumps(doc, sort_keys=True) + "\n")
PY
    done
}

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! command -v duckdb >/dev/null 2>&1; then
    emit_unavailable_all "duckdb not found"
    exit 0
fi

mkdir -p "$RAW_DIR"

DUCKDB_ENGINE_VERSION="$(duckdb --version 2>&1 | head -1)"
echo "run_duckdb_writes.sh: DuckDB ${DUCKDB_ENGINE_VERSION} — N_ROWS=${N_ROWS} N_ITERS=${N_ITERS}"

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
    python3 - "$ENGINE" "$DUCKDB_ENGINE_VERSION" "$workload" "$n_rows" "$n_samples" \
        "$median_us" "$min_us" "$samples_json" "$BENCH_STORAGE_MODE" "$DUCKDB_DURABILITY_MODE" <<'PYEOF'
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
    python3 - "$path" "$BENCH_STORAGE_MODE" "$DUCKDB_DURABILITY_MODE" <<'PYEOF'
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
# Workload: insert_throughput_10k
# ---------------------------------------------------------------------------
run_insert() {
    local wl="insert_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    # Persistent in-process connection via the duckdb Python driver — the
    # same methodology as every other row (see run_update). Each sample
    # recreates an empty table outside the timed region, then times
    # BEGIN TRANSACTION + chunked multi-row INSERTs + COMMIT. The previous
    # path timed a `duckdb` CLI process per sample (process + extension
    # startup, CREATE TABLE), which violated the no-process-spawn
    # methodology contract and inflated DuckDB's reported latencies.
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$INSERT_CHUNK_ROWS" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time, random
import duckdb
from pathlib import Path

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
chunk_rows = int(sys.argv[3])
storage_mode = sys.argv[4]
data_root = Path(sys.argv[5])
workload = sys.argv[6]
warmup = int(os.environ.get("BENCH_WARMUP", "2"))

rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-(2**31), 2**31 - 1) for _ in range(n)]

stmts = []
for i in range(0, n, chunk_rows):
    rows = ",".join(
        f"({j},{v})" for j, v in zip(ids[i:i + chunk_rows], vals[i:i + chunk_rows])
    )
    stmts.append(f"INSERT INTO bench_write(id,val) VALUES {rows};")

if storage_mode == "data-dir":
    db_path = data_root / "duckdb" / f"{workload}-{os.getpid()}.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", ".wal"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = duckdb.connect(str(db_path))


def one_sample():
    con.execute("DROP TABLE IF EXISTS bench_write;")
    con.execute("CREATE TABLE bench_write(id BIGINT NOT NULL, val BIGINT);")
    t0 = time.perf_counter()
    con.execute("BEGIN TRANSACTION;")
    for stmt in stmts:
        con.execute(stmt)
    con.execute("COMMIT;")
    t1 = time.perf_counter()
    return (t1 - t0) * 1e6


for _ in range(warmup):
    one_sample()
for _ in range(n_iters):
    print(one_sample())
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
# Workload: update_throughput_10k
# ---------------------------------------------------------------------------
run_update() {
    local wl="update_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"

    # Persistent in-process connection via the duckdb Python driver:
    # preload N_ROWS once outside the timed region, then per iteration
    # time `BEGIN; UPDATE all rows; ROLLBACK;` — the rollback restores
    # the row image after each timed sample so every iteration measures
    # the same amount of work against the same starting state.
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time, random
import duckdb
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
    db_path = data_root / "duckdb" / f"{workload}-{os.getpid()}.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", ".wal"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = duckdb.connect(str(db_path))
con.execute("CREATE TABLE bench_write(id BIGINT NOT NULL, val BIGINT);")
for chunk_start in range(0, n, 1000):
    rows = ",".join(
        f"({ids[i]},{vals[i]})"
        for i in range(chunk_start, min(chunk_start + 1000, n))
    )
    con.execute(f"INSERT INTO bench_write(id,val) VALUES {rows};")

for _ in range(int(__import__("os").environ.get("BENCH_WARMUP", "2"))):
    con.execute("BEGIN TRANSACTION;")
    con.execute(f"UPDATE bench_write SET val = val + 1 WHERE id BETWEEN 0 AND {n-1};")
    con.execute("ROLLBACK;")

for _ in range(n_iters):
    con.execute("BEGIN TRANSACTION;")
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

    # Persistent in-process connection via the duckdb Python driver:
    # preload N_ROWS once outside the timed region, then per iteration
    # time `BEGIN; DELETE all rows; ROLLBACK;` — the rollback restores
    # the table so each iteration measures the same DELETE work against
    # the same starting state. Avoids the subtract-two-process timing.
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time, random
import duckdb
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
    db_path = data_root / "duckdb" / f"{workload}-{os.getpid()}.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", ".wal"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = duckdb.connect(str(db_path))
con.execute("CREATE TABLE bench_write(id BIGINT NOT NULL, val BIGINT);")
for chunk_start in range(0, n, 1000):
    rows = ",".join(
        f"({ids[i]},{vals[i]})"
        for i in range(chunk_start, min(chunk_start + 1000, n))
    )
    con.execute(f"INSERT INTO bench_write(id,val) VALUES {rows};")

for _ in range(int(__import__("os").environ.get("BENCH_WARMUP", "2"))):
    con.execute("BEGIN TRANSACTION;")
    con.execute(f"DELETE FROM bench_write WHERE id BETWEEN 0 AND {n-1};")
    con.execute("ROLLBACK;")

for _ in range(n_iters):
    con.execute("BEGIN TRANSACTION;")
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
# Persistent in-process connection via the duckdb Python driver: preload
# N_ROWS once outside the timed window, then run single point ops (50% read,
# 30% update, 20% insert) on that warm connection until the 1-second deadline
# and report µs/op. The older path spawned a fresh `duckdb` process per 50-op
# batch AND re-ran the full preload every batch, so it timed process startup,
# parse, temp-DB init, and a 10k-row reload instead of DuckDB's per-op cost.
run_mixed() {
    local wl="mixed_oltp_pgbench_like"
    echo "  workload: ${wl}"

    local samples=()
    local window_secs=1
    for (( i=0; i<N_ITERS; i++ )); do
        local us_per_op
        us_per_op="$(python3 - "$N_ROWS" "$window_secs" "$i" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time, random
import duckdb
from pathlib import Path

n = int(sys.argv[1])
window = float(sys.argv[2])
seed = int(sys.argv[3])
storage_mode = sys.argv[4]
data_root = Path(sys.argv[5])
workload = sys.argv[6]
rng = random.Random(0xBEEF + seed)

if storage_mode == "data-dir":
    db_path = data_root / "duckdb" / f"{workload}-{seed}-{os.getpid()}.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", ".wal"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

# Preload once, outside the timed window.
ids = list(range(n))
rng2 = random.Random(0xC0FFEE)
rng2.shuffle(ids)
vals = [rng2.randint(-2**31, 2**31-1) for _ in range(n)]
con = duckdb.connect(str(db_path))
con.execute("CREATE OR REPLACE TABLE bench_write(id BIGINT PRIMARY KEY, val BIGINT);")
for chunk_start in range(0, n, 1000):
    rows = ",".join(
        f"({ids[i]},{vals[i]})"
        for i in range(chunk_start, min(chunk_start + 1000, n))
    )
    con.execute(f"INSERT INTO bench_write(id,val) VALUES {rows};")

deadline = time.perf_counter() + window
count = 0
next_id = n

while time.perf_counter() < deadline:
    r = rng.random()
    if r < 0.50:
        row_id = rng.randint(0, n - 1)
        con.execute(f"SELECT val FROM bench_write WHERE id = {row_id};").fetchall()
    elif r < 0.80:
        row_id = rng.randint(0, n - 1)
        con.execute(f"UPDATE bench_write SET val = val + 1 WHERE id = {row_id};")
    else:
        new_val = rng.randint(-2**31, 2**31 - 1)
        con.execute(f"INSERT INTO bench_write(id, val) VALUES ({next_id}, {new_val}) ON CONFLICT DO NOTHING;")
        next_id += 1
    count += 1

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

    # Persistent in-process connection via the duckdb Python driver:
    # preload N_ROWS once outside the timed region, then time a full
    # `SELECT id, val FROM bench_select_scan` that drains every row via
    # `fetchall()`. Avoids the subtract-two-process methodology that
    # clamped fast scans against `max(v, 1.0)`.
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$BENCH_STORAGE_MODE" "$BENCH_DATA_ROOT" "$wl" <<'PYEOF'
import os
import sys, time
import duckdb
from pathlib import Path

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
storage_mode = sys.argv[3]
data_root = Path(sys.argv[4])
workload = sys.argv[5]

if storage_mode == "data-dir":
    db_path = data_root / "duckdb" / f"{workload}-{os.getpid()}.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", ".wal"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = duckdb.connect(str(db_path))
con.execute("CREATE TABLE bench_select_scan(id INTEGER NOT NULL, val INTEGER);")
chunks = [list(range(i, min(i + 1000, n))) for i in range(0, n, 1000)]
for ch in chunks:
    rows = ",".join(f"({j},{j * 10})" for j in ch)
    con.execute(f"INSERT INTO bench_select_scan(id,val) VALUES {rows};")

for _ in range(int(__import__("os").environ.get("BENCH_WARMUP", "2"))):
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
# Uses the duckdb Python driver: opens an in-memory connection once, runs
# the preload outside the timed region, then times the query across
# N_ITERS iterations with `.fetchall()` to force materialization (DuckDB
# query objects are lazy until consumed).
#
# Older methodology launched `duckdb :memory:` per iteration, timed
# preload+query, then subtracted a preload-only baseline. Sub-millisecond
# queries had `full - preload_median` swing negative and got clamped to
# `max(v, 1.0)`, reporting a 1.0 µs floor that was harness noise, not
# DuckDB performance.
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
import duckdb
from pathlib import Path

n = int(sys.argv[1])
query = sys.argv[2]
n_iters = int(sys.argv[3])
storage_mode = sys.argv[4]
data_root = Path(sys.argv[5])
workload = sys.argv[6]

if storage_mode == "data-dir":
    db_path = data_root / "duckdb" / f"{workload}-{os.getpid()}.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", ".wal"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
else:
    db_path = Path(":memory:")

con = duckdb.connect(str(db_path))
con.execute("CREATE TABLE bench_analytical(id INTEGER NOT NULL, x INTEGER);")
# Preload outside the timed region.
chunks = [list(range(i, min(i + 1000, n))) for i in range(0, n, 1000)]
for ch in chunks:
    rows = ",".join(f"({j},{j * 10})" for j in ch)
    con.execute(f"INSERT INTO bench_analytical(id,x) VALUES {rows};")

# Warmup: prime caches, parser, type checks.
for _ in range(int(__import__("os").environ.get("BENCH_WARMUP", "2"))):
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
        echo "run_duckdb_writes.sh: unknown workload '$WORKLOAD'" >&2
        exit 2
        ;;
esac

echo "run_duckdb_writes.sh: done — results in ${RAW_DIR}/"
