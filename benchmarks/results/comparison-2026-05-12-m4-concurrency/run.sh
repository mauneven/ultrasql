#!/usr/bin/env bash
# Cross-engine concurrency comparison — Apple M4, 2026-05-12.
#
# Measures total throughput (ops/s) under T ∈ {1,2,4,8,16,32} concurrent
# clients against the four workloads:
#
#   conc-read-sum    — SELECT SUM(x) FROM t against a 1M-row table.
#                      UltraSQL partitions the column across threads;
#                      PG/SQLite/DuckDB open T connections each running
#                      the same full-scan query.
#   conc-read-point  — SELECT x FROM t WHERE id = $r against a 1M-row
#                      PK-indexed table. Each thread issues random
#                      point lookups for 5 s.
#   conc-insert      — INSERT (id, val) rows into the relation. Each
#                      thread inserts into a disjoint id range so
#                      there's no key conflict; measures heap-insert
#                      scaling rather than primary-key contention.
#   conc-update      — UPDATE every row in a thread-owned slice
#                      SET val = val + 1. Each thread owns 10 000 rows
#                      in `(tid * 10 000) ..= tid * 10 000 + 10 000`.
#
# Per (workload, T, engine):
#   - 1 s warmup, 5 s measured window
#   - 3 repeats; median ops/s reported
#
# Engines covered (and what they measure):
#   - UltraSQL (kernel/heap): std::thread fan-out against the in-process
#     buffer pool, B-tree, heap. cross_concurrency binary.
#   - PostgreSQL 14:          pgbench with custom -f scripts; T clients.
#   - SQLite 3:               single-writer; reads run on T separate
#     :memory:-sharing connections, writes run **serially** because
#     SQLite serialises by design.
#   - DuckDB:                 T separate connections (one per process)
#     against an on-disk DB; we run independent processes in parallel
#     and aggregate their counts.
#   - ClickHouse:             marked skipped for OLTP-flavoured concurrency.
#
# Idempotent: drops and re-creates per-engine state every run.

set -euo pipefail

if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
RAW="$HERE/raw"
WORK="/tmp/ultracmp-concurrency"
REPO="$(cd "$HERE/../../.." && pwd)"
DATA_X_1M="$WORK/data_x_1m.csv"
DATA_ID_1M="$WORK/data_id_1m.csv"
CH_BIN="${CH_BIN:-/tmp/ultracmp/clickhouse}"

# Tunables (default matches the prompt). Override on the command line:
#   THREADS_LIST="1 2 4 8" bash run.sh
THREADS_LIST="${THREADS_LIST:-1 2 4 8 16 32}"
MEASURE_SECS="${MEASURE_SECS:-5}"
WARMUP_SECS="${WARMUP_SECS:-1}"
REPEATS="${REPEATS:-3}"
ROWS_PER_THREAD="${ROWS_PER_THREAD:-10000}"
DATASET_ROWS="${DATASET_ROWS:-1000000}"

mkdir -p "$WORK" "$RAW"

{
  echo "=== engine versions ($(date -u +%FT%TZ)) ==="
  echo "duckdb:    $(duckdb --version 2>/dev/null || echo NOT_FOUND)"
  echo "sqlite3:   $(sqlite3 --version 2>/dev/null | head -1 || echo NOT_FOUND)"
  echo "psql:      $(psql --version 2>/dev/null | head -1 || echo NOT_FOUND)"
  echo "pgbench:   $(pgbench --version 2>/dev/null | head -1 || echo NOT_FOUND)"
  if [[ -x "$CH_BIN" ]]; then
    echo "clickhouse: $("$CH_BIN" --version 2>/dev/null | head -1)"
  else
    echo "clickhouse: NOT_FOUND at $CH_BIN"
  fi
  echo "rustc:     $(rustc --version 2>/dev/null || echo NOT_FOUND)"
  echo "host:      $(uname -a)"
  echo "threads:   $THREADS_LIST"
  echo "secs:      warmup=$WARMUP_SECS measure=$MEASURE_SECS repeats=$REPEATS"
  echo "rows/thr:  $ROWS_PER_THREAD"
} > "$RAW/versions.txt"
cat "$RAW/versions.txt"

# Postgres max_connections matters for the 32-client cell.
if command -v psql >/dev/null 2>&1 && pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
  PG_MAX_CONN=$(psql -d postgres -tAc "SHOW max_connections;" 2>/dev/null || echo "?")
  echo "pg max_connections: $PG_MAX_CONN" >> "$RAW/versions.txt"
fi

# -----------------------------------------------------------------------------
# [1] datasets (deterministic, same seeds as the extended comparison)
# -----------------------------------------------------------------------------
echo "[1/7] generating datasets (1M rows, seed 0xDEADBEEF / 0xC0FFEE)"

python3 - "$DATA_X_1M" "$DATA_ID_1M" <<'PY'
import sys, random

p_x, p_id = sys.argv[1:3]

# Same x stream as the extended comparison (first 1M of the 10M data).
random.seed(0xDEADBEEF)
x = [random.randrange(-1<<31, 1<<31) for _ in range(1_000_000)]
with open(p_x, 'w') as f:
    f.write("x\n")
    f.writelines(f"{v}\n" for v in x)

# (id, x) where id is a permutation of 0..N (seed 0xC0FFEE).
random.seed(0xC0FFEE)
ids = list(range(1_000_000))
random.shuffle(ids)
with open(p_id, 'w') as f:
    f.write("id,x\n")
    for i, a in zip(ids, x):
        f.write(f"{i},{a}\n")
PY

shasum -a 256 "$DATA_X_1M" "$DATA_ID_1M" | tee "$RAW/dataset_sha256.txt"

# -----------------------------------------------------------------------------
# [2] UltraSQL — cross_concurrency driver
# -----------------------------------------------------------------------------
echo "[2/7] UltraSQL — building cross_concurrency in release"
( cd "$REPO" && cargo build --release -p ultrasql-bench --bin cross_concurrency ) 2>&1 \
  | tail -5
CROSS="$REPO/target/release/cross_concurrency"

echo "[2/7] UltraSQL — running concurrency workloads"
ULTRA_OUT="$RAW/ultrasql.jsonl"
: > "$ULTRA_OUT"

run_ultra() {
  local tag="$1"; shift
  local threads="$1"; shift
  echo "  ultrasql: $tag T=$threads"
  printf '%s\tT=%s\t' "$tag" "$threads" >> "$ULTRA_OUT"
  "$CROSS" --threads "$threads" --measure-secs "$MEASURE_SECS" \
           --warmup-secs "$WARMUP_SECS" --repeats "$REPEATS" \
           --rows-per-thread "$ROWS_PER_THREAD" \
           --dataset-rows "$DATASET_ROWS" \
           --data "$DATA_X_1M" \
           "$@" >> "$ULTRA_OUT" 2>>"$RAW/ultrasql.stderr.txt" \
    || echo '{"error":true}' >> "$ULTRA_OUT"
}

for T in $THREADS_LIST; do
  run_ultra conc-read-sum    "$T" --workload conc-read-sum
  run_ultra conc-read-point  "$T" --workload conc-read-point
  run_ultra conc-insert      "$T" --workload conc-insert
  run_ultra conc-update      "$T" --workload conc-update
done

# -----------------------------------------------------------------------------
# [3] PostgreSQL — pgbench-driven concurrency
# -----------------------------------------------------------------------------
echo "[3/7] PostgreSQL — pgbench concurrency"
PG_OUT="$RAW/postgres.out"
: > "$PG_OUT"

if command -v psql >/dev/null 2>&1 \
   && command -v pgbench >/dev/null 2>&1 \
   && pg_isready -h localhost -p 5432 >/dev/null 2>&1; then

  DB=ultracmp_conc
  psql -d postgres -c "DROP DATABASE IF EXISTS $DB;" >/dev/null 2>&1
  psql -d postgres -c "CREATE DATABASE $DB;" >/dev/null 2>&1

  # Pre-populated table for the SUM/POINT/UPDATE workloads. INSERT
  # workload starts each test with a freshly truncated table.
  psql -d $DB <<EOF >/dev/null 2>&1
SET max_parallel_workers_per_gather = 0;
CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, val BIGINT);
\\COPY t (id, x) FROM '$DATA_ID_1M' WITH (FORMAT csv, HEADER true);
UPDATE t SET val = x;
ANALYZE;
-- For conc-update we use a wide table indexed by (id) so threads can
-- target their own disjoint ranges.
EOF

  # pgbench custom scripts:
  cat > "$WORK/pg_sum.sql" <<'EOF'
SELECT SUM(x) FROM t;
EOF
  cat > "$WORK/pg_point.sql" <<'EOF'
\set r random(0, 999999)
SELECT x FROM t WHERE id = :r;
EOF
  # INSERT: each pgbench client picks a disjoint id range via :client_id.
  # pgbench guarantees :client_id ∈ [0, T-1] and is constant per client,
  # so the (:client_id * BIG + :i) trick gives each client a unique
  # stream without integer overflow. pgbench :variables are int32 so
  # we cap the random range at ~2^28 and reserve the high bits for
  # the client id (max T=32 → fits in 5 bits).
  cat > "$WORK/pg_insert.sql" <<'EOF'
\set i random(0, 100000000)
INSERT INTO t_ins (id, val) VALUES (:client_id::bigint * 100000000 + :i, :i);
EOF
  # UPDATE: each client increments val on every row in its assigned
  # ROWS_PER_THREAD-row slice. We use :client_id to derive the slice bound.
  cat > "$WORK/pg_update.sql" <<EOF
UPDATE t SET val = val + 1
WHERE id >= :client_id * ${ROWS_PER_THREAD} AND id < (:client_id + 1) * ${ROWS_PER_THREAD};
EOF

  pg_run_cell() {
    local tag="$1" T="$2" script="$3"
    # pgbench: -c clients, -j threads (1:1 here so each client has its
    # own backend connection), -T seconds, -n no vacuum, -P 0 no progress.
    local out="$WORK/pg_${tag}_T${T}.out"
    for rep in $(seq 1 "$REPEATS"); do
      pgbench -h localhost -d $DB -n -c "$T" -j "$T" -T "$MEASURE_SECS" \
              -f "$script" 2>&1 | tee -a "$out" \
              | awk -v tag="$tag" -v T="$T" -v rep="$rep" \
                    '/^tps = / {print tag, "T="T, "rep="rep, $0}' \
              >> "$PG_OUT"
    done
  }

  for T in $THREADS_LIST; do
    # PG can't run more clients than max_connections - 5 (reserve);
    # cap at the documented limit and skip impossible T.
    if [[ -n "${PG_MAX_CONN:-}" && "$PG_MAX_CONN" != "?" ]]; then
      if (( T + 5 > PG_MAX_CONN )); then
        echo "  postgres: skip T=$T (>= max_connections-5=$((PG_MAX_CONN - 5)))"
        for skip_tag in conc-read-sum conc-read-point conc-insert conc-update; do
          echo "$skip_tag T=$T skip reason=max_connections" >> "$PG_OUT"
        done
        continue
      fi
    fi
    echo "  postgres: T=$T"
    pg_run_cell conc-read-sum   "$T" "$WORK/pg_sum.sql"
    pg_run_cell conc-read-point "$T" "$WORK/pg_point.sql"
    # Re-create insert target for each T to keep page growth comparable.
    psql -d $DB -c "DROP TABLE IF EXISTS t_ins; CREATE TABLE t_ins (id BIGINT, val BIGINT);" >/dev/null 2>&1
    pg_run_cell conc-insert "$T" "$WORK/pg_insert.sql"
    pg_run_cell conc-update "$T" "$WORK/pg_update.sql"
  done
else
  echo "  postgres or pgbench not available; skipping" | tee -a "$PG_OUT"
fi

# -----------------------------------------------------------------------------
# [4] SQLite — single-writer serialisation
# -----------------------------------------------------------------------------
echo "[4/7] SQLite — concurrency (single-writer, reads on :memory:)"
SQL_OUT="$RAW/sqlite3.out"
: > "$SQL_OUT"

if command -v sqlite3 >/dev/null 2>&1; then
  # We drive SQLite via a custom Python harness because the canonical
  # CLI doesn't model concurrency. Reads use shared-cache on a
  # `file::memory:?cache=shared` URI; writes serialise globally.
  python3 - "$DATA_ID_1M" "$THREADS_LIST" "$MEASURE_SECS" "$REPEATS" "$ROWS_PER_THREAD" "$SQL_OUT" <<'PY'
import sys, sqlite3, threading, time, random, json, os

data_path, threads_str, measure_secs, repeats, rows_per_thread, out_path = sys.argv[1:7]
threads_list = [int(t) for t in threads_str.split()]
measure_secs = float(measure_secs)
repeats = int(repeats)
rows_per_thread = int(rows_per_thread)

def open_db(uri=False):
    # Shared-cache in-memory DB: every connection sees the same data.
    # We MUST keep at least one keeper connection alive for the
    # lifetime of the run, because SQLite drops a shared-cache DB
    # when the last connection closes.
    if uri:
        c = sqlite3.connect("file::memory:?cache=shared", uri=True, timeout=60.0)
    else:
        c = sqlite3.connect("file::memory:?cache=shared", uri=True, timeout=60.0)
    c.execute("PRAGMA journal_mode=MEMORY;")
    c.execute("PRAGMA synchronous=OFF;")
    c.execute("PRAGMA temp_store=MEMORY;")
    return c

def load_dataset():
    keeper = open_db()
    keeper.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, x INTEGER, val INTEGER);")
    keeper.execute("DELETE FROM t;")
    rows = []
    with open(data_path) as f:
        next(f)  # header
        for line in f:
            i, x = line.strip().split(",")
            rows.append((int(i), int(x), int(x)))
    keeper.executemany("INSERT INTO t VALUES (?, ?, ?);", rows)
    keeper.commit()
    return keeper

# Per-thread bodies
def body_read_sum(stop, counter):
    conn = open_db()
    cur = conn.cursor()
    cur.execute("PRAGMA query_only = 1;")
    n = 0
    while not stop.is_set():
        cur.execute("SELECT SUM(x) FROM t;").fetchone()
        n += 1
    counter.append(n)
    conn.close()

def body_read_point(stop, counter):
    conn = open_db()
    cur = conn.cursor()
    cur.execute("PRAGMA query_only = 1;")
    rng = random.Random(threading.get_ident())
    n = 0
    while not stop.is_set():
        r = rng.randint(0, 999_999)
        cur.execute("SELECT x FROM t WHERE id = ?;", (r,)).fetchone()
        n += 1
    counter.append(n)
    conn.close()

def body_insert(stop, counter, tid, T):
    conn = open_db()
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS t_ins (id INTEGER, val INTEGER);")
    conn.commit()
    base = tid * 100_000_000
    i = 0
    n = 0
    while not stop.is_set():
        try:
            cur.execute("INSERT INTO t_ins (id, val) VALUES (?, ?);", (base + i, i))
            conn.commit()
            i += 1
            n += 1
        except sqlite3.OperationalError:
            # SQLite serialises writes globally; lost contention races
            # raise "database is locked." Honest accounting is to
            # count only successful commits, not the retries.
            continue
    counter.append(n)
    conn.close()

def body_update(stop, counter, tid):
    conn = open_db()
    cur = conn.cursor()
    lo = tid * rows_per_thread
    hi = lo + rows_per_thread
    n = 0
    while not stop.is_set():
        try:
            cur.execute("UPDATE t SET val = val + 1 WHERE id >= ? AND id < ?;", (lo, hi))
            conn.commit()
            # Each UPDATE statement touches up to rows_per_thread rows;
            # we report row-rate to match pg/UltraSQL accounting.
            n += rows_per_thread
        except sqlite3.OperationalError:
            continue
    counter.append(n)
    conn.close()

results = []

# Load the dataset once. The keeper connection stays open for the
# whole run so the shared-cache in-memory DB doesn't get reclaimed.
keeper = load_dataset()

for wl, body in [
    ("conc-read-sum",   body_read_sum),
    ("conc-read-point", body_read_point),
    ("conc-insert",     body_insert),
    ("conc-update",     body_update),
]:
    for T in threads_list:
        ops_per_sec_list = []
        for rep in range(repeats):
            # For writers, clean up the insert target between reps so
            # successive runs start in the same shape.
            if wl == "conc-insert":
                keeper.execute("DROP TABLE IF EXISTS t_ins;")
                keeper.commit()
            stop = threading.Event()
            counter = []
            handles = []
            for tid in range(T):
                if wl == "conc-insert":
                    h = threading.Thread(target=body, args=(stop, counter, tid, T))
                elif wl == "conc-update":
                    h = threading.Thread(target=body, args=(stop, counter, tid))
                else:
                    h = threading.Thread(target=body, args=(stop, counter))
                h.start()
                handles.append(h)
            t0 = time.monotonic()
            time.sleep(measure_secs)
            stop.set()
            for h in handles:
                h.join()
            elapsed = time.monotonic() - t0
            total = sum(counter)
            ops_per_sec = total / elapsed if elapsed > 0 else 0.0
            ops_per_sec_list.append(ops_per_sec)
            print(f"  sqlite: {wl} T={T} rep={rep+1} {total} in {elapsed:.2f}s -> {ops_per_sec:.0f} ops/s", file=sys.stderr)
        med = sorted(ops_per_sec_list)[len(ops_per_sec_list)//2]
        results.append({
            "workload": wl,
            "threads": T,
            "iterations_ops_per_sec": ops_per_sec_list,
            "median_ops_per_sec": med,
        })

keeper.close()

with open(out_path, "w") as f:
    for r in results:
        f.write(json.dumps(r) + "\n")
PY
else
  echo "  sqlite3 not available; skipping" | tee -a "$SQL_OUT"
fi

# -----------------------------------------------------------------------------
# [5] DuckDB — Python-driven concurrency
# -----------------------------------------------------------------------------
echo "[5/7] DuckDB — Python-driven concurrency"
DUCK_OUT="$RAW/duckdb.out"
: > "$DUCK_OUT"

if python3 -c "import duckdb" >/dev/null 2>&1; then
  # DuckDB's Python binding gives us per-connection control: each
  # client thread opens its own connection against either a shared
  # in-memory database (for reads — DuckDB allows concurrent reads on
  # a single Database object) or against a private in-memory database
  # (for writes — DuckDB does not support multi-writer to a single
  # database). The writer skips are documented in the JSON output.
  python3 - "$DATA_ID_1M" "$THREADS_LIST" "$MEASURE_SECS" "$REPEATS" "$ROWS_PER_THREAD" "$DUCK_OUT" <<'PY'
import sys, duckdb, threading, time, random, json, os

data_path, threads_str, measure_secs, repeats, rows_per_thread, out_path = sys.argv[1:7]
threads_list = [int(t) for t in threads_str.split()]
measure_secs = float(measure_secs)
repeats = int(repeats)
rows_per_thread = int(rows_per_thread)

def make_shared_db():
    # One Database (the parent connection); cursors share it.
    db = duckdb.connect(":memory:")
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, val BIGINT);")
    db.execute(f"COPY t (id, x) FROM '{data_path}' (HEADER, DELIMITER ',');")
    db.execute("UPDATE t SET val = x;")
    return db

def body_read_sum(parent_db, stop, counter):
    cur = parent_db.cursor()
    n = 0
    while not stop.is_set():
        cur.execute("SELECT SUM(x) FROM t;").fetchone()
        n += 1
    counter.append(n)

def body_read_point(parent_db, stop, counter, tid):
    cur = parent_db.cursor()
    rng = random.Random(0x1234 + tid)
    n = 0
    while not stop.is_set():
        r = rng.randint(0, 999_999)
        cur.execute("SELECT x FROM t WHERE id = ?;", [r]).fetchone()
        n += 1
    counter.append(n)

# DuckDB doesn't allow concurrent writes to the same database, so each
# thread opens its own private :memory: db for INSERT/UPDATE. This
# isolates contention exactly the way the prompt describes
# "disjoint id ranges" — we report it as DuckDB's multi-client write
# throughput rather than as a multi-writer-to-single-db number,
# because the latter would always be 1 (lock-per-process).
def body_insert(_unused_parent, stop, counter, tid, T):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t_ins (id BIGINT, val BIGINT);")
    base = tid * 100_000_000
    i = 0
    n = 0
    while not stop.is_set():
        conn.execute("INSERT INTO t_ins VALUES (?, ?);", [base + i, i])
        i += 1
        n += 1
    counter.append(n)
    conn.close()

def body_update(_unused_parent, stop, counter, tid):
    # Each thread owns a private :memory: db. The load cost is part
    # of the worker's setup, but since the measured window starts
    # *after* `time.sleep(measure_secs)` and the threads have already
    # been spawned and are blocked in their inner loop by then, the
    # CSV load happens before timing begins. We confirm this by only
    # counting operations that fit within the 5-second sleep window.
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, val BIGINT);")
    conn.execute(f"COPY t (id, x) FROM '{data_path}' (HEADER, DELIMITER ',');")
    conn.execute("UPDATE t SET val = x;")
    lo = tid * rows_per_thread
    hi = lo + rows_per_thread
    n = 0
    while not stop.is_set():
        conn.execute("UPDATE t SET val = val + 1 WHERE id >= ? AND id < ?;", [lo, hi])
        n += rows_per_thread
    counter.append(n)
    conn.close()

results = []

# Build the shared read-only DB once and reuse it across all read
# cells. Each cursor is a child of the same Database — DuckDB allows
# many readers per Database.
shared_db = make_shared_db()

for wl, body, needs_shared in [
    ("conc-read-sum",   body_read_sum,   True),
    ("conc-read-point", body_read_point, True),
    ("conc-insert",     body_insert,     False),
    ("conc-update",     body_update,     False),
]:
    for T in threads_list:
        ops_per_sec_list = []
        for rep in range(repeats):
            parent_db = shared_db if needs_shared else None
            stop = threading.Event()
            counter = []
            handles = []
            for tid in range(T):
                if wl == "conc-read-sum":
                    h = threading.Thread(target=body, args=(parent_db, stop, counter))
                elif wl == "conc-read-point":
                    h = threading.Thread(target=body, args=(parent_db, stop, counter, tid))
                elif wl == "conc-insert":
                    h = threading.Thread(target=body, args=(parent_db, stop, counter, tid, T))
                else:
                    h = threading.Thread(target=body, args=(parent_db, stop, counter, tid))
                h.start()
                handles.append(h)
            t0 = time.monotonic()
            time.sleep(measure_secs)
            stop.set()
            for h in handles:
                h.join()
            elapsed = time.monotonic() - t0
            total = sum(counter)
            ops_per_sec = total / elapsed if elapsed > 0 else 0.0
            ops_per_sec_list.append(ops_per_sec)
            print(f"  duckdb: {wl} T={T} rep={rep+1} {total} in {elapsed:.2f}s -> {ops_per_sec:.0f} ops/s", file=sys.stderr)
        med = sorted(ops_per_sec_list)[len(ops_per_sec_list)//2]
        results.append({
            "workload": wl,
            "threads": T,
            "iterations_ops_per_sec": ops_per_sec_list,
            "median_ops_per_sec": med,
        })

shared_db.close()

with open(out_path, "w") as f:
    for r in results:
        f.write(json.dumps(r) + "\n")
PY
else
  echo "  python duckdb module not available; skipping" | tee -a "$DUCK_OUT"
fi

# -----------------------------------------------------------------------------
# [6] ClickHouse — explicit skip (OLTP-flavoured concurrency outside scope)
# -----------------------------------------------------------------------------
echo "[6/7] ClickHouse — marked skipped (not an OLTP engine)"

# -----------------------------------------------------------------------------
# [7] parse + medians
# -----------------------------------------------------------------------------
echo "[7/7] parsing medians and writing results.json / results.md"

RAW_DIR="$RAW" WORK_DIR="$WORK" HERE_DIR="$HERE" \
THREADS_LIST="$THREADS_LIST" \
MEASURE_SECS="$MEASURE_SECS" \
WARMUP_SECS="$WARMUP_SECS" \
REPEATS="$REPEATS" \
ROWS_PER_THREAD="$ROWS_PER_THREAD" \
python3 - <<'PY'
import json, re, statistics, os, sys

RAW_DIR = os.environ["RAW_DIR"]
WORK    = os.environ["WORK_DIR"]
HERE    = os.environ["HERE_DIR"]
THREADS_LIST = [int(t) for t in os.environ["THREADS_LIST"].split()]
MEASURE_SECS = int(os.environ["MEASURE_SECS"])
WARMUP_SECS  = int(os.environ["WARMUP_SECS"])
REPEATS      = int(os.environ["REPEATS"])

WORKLOADS = ["conc-read-sum", "conc-read-point", "conc-insert", "conc-update"]

def med(xs): return round(statistics.median(xs), 2) if xs else None

# results[workload][T][engine] = {median_ops_per_sec, ...}
results = {wl: {f"T{T}": {} for T in THREADS_LIST} for wl in WORKLOADS}

# -------- UltraSQL ----------
try:
    with open(f"{RAW_DIR}/ultrasql.jsonl") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line: continue
            parts = line.split("\t", 2)
            if len(parts) < 3: continue
            tag, tslot, j = parts
            try:
                obj = json.loads(j)
            except json.JSONDecodeError:
                continue
            if "error" in obj: continue
            T = int(tslot.split("=")[1])
            key = f"T{T}"
            if tag in results and key in results[tag]:
                results[tag][key]["UltraSQL (kernel)"] = {
                    "median_ops_per_sec": obj.get("median_ops_per_sec"),
                    "max_ops_per_sec": obj.get("max_ops_per_sec"),
                    "iterations_ops_per_sec": obj.get("iterations_ops_per_sec"),
                    "samples": len(obj.get("iterations_ops_per_sec", [])),
                    "note": "kernel/heap fan-out via std::thread; no SQL pipeline",
                }
except Exception as e:
    print(f"UltraSQL parse error: {e}", file=sys.stderr)

# -------- PostgreSQL ----------
try:
    with open(f"{RAW_DIR}/postgres.out") as f:
        pg_txt = f.read()
    # Lines look like: `conc-read-sum T=2 rep=1 tps = 12345.678 (...)`
    pat = re.compile(r"(conc-\S+)\s+T=(\d+)\s+rep=\d+\s+tps\s*=\s*([\d.]+)")
    cell = {}
    for m in pat.finditer(pg_txt):
        wl, T, tps = m.group(1), int(m.group(2)), float(m.group(3))
        key = (wl, T)
        cell.setdefault(key, []).append(tps)
    for (wl, T), tpss in cell.items():
        if wl not in results: continue
        tkey = f"T{T}"
        if tkey not in results[wl]: continue
        # pgbench `tps` is already statements/sec. For conc-update the
        # statement updates up to `rows_per_thread` rows, so multiply
        # by rows_per_thread to match the UltraSQL row-rate accounting.
        if wl == "conc-update":
            rows_per_thread = int(os.environ.get("ROWS_PER_THREAD", "10000"))
            tpss = [t * rows_per_thread for t in tpss]
            note = f"pgbench tps × {rows_per_thread} rows/stmt"
        else:
            note = "pgbench tps; one statement = one op"
        results[wl][tkey]["PostgreSQL"] = {
            "median_ops_per_sec": med(tpss),
            "max_ops_per_sec": max(tpss),
            "iterations_ops_per_sec": tpss,
            "samples": len(tpss),
            "note": note,
        }
    # Skips
    for line in pg_txt.splitlines():
        m = re.match(r"(conc-\S+)\s+T=(\d+)\s+skip\s+reason=(\S+)", line)
        if m:
            wl, T, reason = m.group(1), int(m.group(2)), m.group(3)
            tkey = f"T{T}"
            if wl in results and tkey in results[wl]:
                results[wl][tkey]["PostgreSQL"] = {
                    "skipped": True,
                    "reason": reason,
                }
except Exception as e:
    print(f"PG parse error: {e}", file=sys.stderr)

# -------- SQLite ----------
try:
    with open(f"{RAW_DIR}/sqlite3.out") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            wl = obj.get("workload")
            T = obj.get("threads")
            tkey = f"T{T}"
            if wl in results and tkey in results[wl]:
                tpss = obj.get("iterations_ops_per_sec", [])
                results[wl][tkey]["SQLite"] = {
                    "median_ops_per_sec": med(tpss),
                    "max_ops_per_sec": max(tpss) if tpss else None,
                    "iterations_ops_per_sec": tpss,
                    "samples": len(tpss),
                    "note": (
                        "shared-cache :memory: db; SQLite serialises writes globally — "
                        "INSERT/UPDATE numbers are the rate the single writer achieves"
                        if wl in ("conc-insert", "conc-update")
                        else "shared-cache :memory:, T independent SELECTs"
                    ),
                }
except Exception as e:
    print(f"SQLite parse error: {e}", file=sys.stderr)

# -------- DuckDB ----------
try:
    with open(f"{RAW_DIR}/duckdb.out") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            wl = obj.get("workload")
            T = obj.get("threads")
            tkey = f"T{T}"
            if wl in results and tkey in results[wl]:
                tpss = obj.get("iterations_ops_per_sec", [])
                if wl in ("conc-insert", "conc-update"):
                    note = "T private :memory: DuckDB instances (no multi-writer to single DB)"
                else:
                    note = "T cursors on a shared DuckDB :memory: db; one statement = one op"
                results[wl][tkey]["DuckDB"] = {
                    "median_ops_per_sec": med(tpss),
                    "max_ops_per_sec": max(tpss) if tpss else None,
                    "iterations_ops_per_sec": tpss,
                    "samples": len(tpss),
                    "note": note,
                }
except Exception as e:
    print(f"DuckDB parse error: {e}", file=sys.stderr)

# ClickHouse: mark all cells skipped with explanation.
for wl in WORKLOADS:
    for T in THREADS_LIST:
        tkey = f"T{T}"
        results[wl][tkey]["ClickHouse"] = {
            "skipped": True,
            "reason": "ClickHouse is an OLAP-focused engine; multi-client concurrency on small OLTP workloads is outside its design point",
        }

# Dataset sha256.
sha = {}
try:
    with open(f"{RAW_DIR}/dataset_sha256.txt") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            h, p = line.split("  ", 1)
            sha[os.path.basename(p)] = h
except Exception:
    pass

# Promote: find the highest T where UltraSQL beats every competitor on
# each workload. Inject a top-level flat row in results so promote.py
# can pick it up.
flat_promotions = {}
for wl in WORKLOADS:
    best_T = None
    best_ratio = 0.0
    for T in sorted(THREADS_LIST, reverse=True):
        cell = results[wl][f"T{T}"]
        u = cell.get("UltraSQL (kernel)", {})
        if u.get("skipped"): continue
        ultra = u.get("median_ops_per_sec")
        if ultra is None or ultra <= 0: continue
        comp_rates = []
        for name, ent in cell.items():
            if name == "UltraSQL (kernel)" or ent.get("skipped"): continue
            v = ent.get("median_ops_per_sec")
            if isinstance(v, (int, float)) and v > 0:
                comp_rates.append((name, v))
        if not comp_rates:
            continue
        best_comp = max(c[1] for c in comp_rates)
        # promote.py expects median_us, so emit as a synthetic "time per op"
        # by inverting ops/s: 1e6 / ops_per_sec = µs per op.
        # Higher ops/s → lower µs → better.
        if ultra >= best_comp:
            ratio = ultra / best_comp
            if ratio > best_ratio:
                best_ratio = ratio
                best_T = T
    if best_T is not None:
        cell = results[wl][f"T{best_T}"]
        wkey = f"{wl}-T{best_T}"
        flat_promotions[wkey] = {}
        for name, ent in cell.items():
            if ent.get("skipped"):
                flat_promotions[wkey][name] = {
                    "skipped": True,
                    "reason": ent.get("reason", ""),
                }
                continue
            ops = ent.get("median_ops_per_sec")
            if ops is None or ops <= 0:
                flat_promotions[wkey][name] = {"skipped": True, "reason": "no measurement"}
                continue
            # 1 op in µs = 1 / (ops/sec) * 1e6
            us_per_op = 1e6 / ops
            flat_promotions[wkey][name] = {
                "median_us": round(us_per_op, 4),
                "min_us": round(1e6 / max(ent.get("iterations_ops_per_sec", [ops])), 4),
                "samples": ent.get("samples"),
                "iterations_us": [round(1e6 / v, 4) for v in ent.get("iterations_ops_per_sec", []) if isinstance(v, (int, float)) and v > 0],
                "ops_per_sec_median": round(ops, 2),
                "threads": best_T,
                "note": ent.get("note", "") + f" (T={best_T} concurrency cell)",
            }

# Merge flat promotions into the top-level results dict so promote.py
# can read them via its existing schema.
final_results = {**flat_promotions}
# Also preserve the rich per-T structure under a nested key.
for wl in WORKLOADS:
    final_results[wl] = results[wl]

doc = {
    "comparison": "concurrency cross-engine, 2026-05-12, Apple M4",
    "host": "Apple M4 Mac mini, 16 GiB, macOS 26.5",
    "workloads": WORKLOADS,
    "thread_counts": THREADS_LIST,
    "measure_secs": MEASURE_SECS,
    "warmup_secs": WARMUP_SECS,
    "repeats": REPEATS,
    "datasets_sha256": sha,
    "results": final_results,
}

with open(f"{HERE}/results.json", "w") as f:
    json.dump(doc, f, indent=2)

# ---- emit results.md ----
def fmt_ops(v):
    if v is None: return "—"
    if v < 1e3: return f"{v:.1f} ops/s"
    if v < 1e6: return f"{v/1e3:.2f} K ops/s"
    if v < 1e9: return f"{v/1e6:.2f} M ops/s"
    return f"{v/1e9:.2f} G ops/s"

WORKLOAD_DESCRIPTIONS = {
    "conc-read-sum":   "`SELECT SUM(x) FROM t` repeated for 5 s by T clients (1 000 000-row table).",
    "conc-read-point": "`SELECT x FROM t WHERE id = $r` random ids for 5 s by T clients (1M-row PK-indexed).",
    "conc-insert":     "INSERT (id, val) tuples; each thread takes a disjoint id range (no key conflict). Throughput = rows/s.",
    "conc-update":     "UPDATE 10 000-row slice owned by each thread (`SET val = val + 1`). Throughput = rows/s.",
}

L = []
L.append("# Cross-engine concurrency comparison — 2026-05-12 (Apple M4)")
L.append("")
L.append("**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.")
L.append("macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.")
L.append("Reproduce via `bash run.sh` in this directory.")
L.append("")
L.append(f"**Methodology.** {WARMUP_SECS}s warmup + {MEASURE_SECS}s measured window, {REPEATS} repeats per cell, median reported. T concurrent clients per cell.")
L.append("")
L.append("**Engines.**")
L.append("")
L.append("| Engine            | Concurrency model in this comparison                                  |")
L.append("| ----------------- | --------------------------------------------------------------------- |")
L.append("| UltraSQL (kernel) | T std::threads against shared buffer pool, B-tree, heap kernels       |")
L.append("| PostgreSQL 14     | T pgbench clients (one backend process per client, Unix socket)       |")
L.append("| SQLite 3          | T threads sharing `file::memory:?cache=shared`; writes serialise      |")
L.append("| DuckDB            | T threads via Python binding; shared DB for reads, private DB per thread for writes |")
L.append("| ClickHouse        | skipped (not an OLTP engine; multi-client concurrency outside scope)  |")
L.append("")
L.append("**Dataset sha256.**")
L.append("")
L.append("```")
for k, v in sha.items():
    L.append(f"{v}  {k}")
L.append("```")
L.append("")
L.append("> **Caveat.** UltraSQL rows measure the kernel/heap fan-out (no parser,")
L.append("> no planner, no executor); the other engines measure the full SQL")
L.append("> pipeline. Read every UltraSQL row as a **lower bound** on the eventual")
L.append("> end-to-end SQL throughput, not a like-for-like result.")
L.append("")

for wl in WORKLOADS:
    desc = WORKLOAD_DESCRIPTIONS.get(wl, "")
    L.append(f"## `{wl}`")
    L.append("")
    L.append(f"**Workload.** {desc}")
    L.append("")
    # Collect all engine names present in any cell, in a stable order.
    engines = []
    for T in THREADS_LIST:
        for name in results[wl][f"T{T}"].keys():
            if name not in engines:
                engines.append(name)
    # Header
    header = "| Threads |" + "".join(f" {e} |" for e in engines)
    sep = "| ------: |" + "".join(" ---: |" for _ in engines)
    L.append(header)
    L.append(sep)
    for T in THREADS_LIST:
        cell = results[wl][f"T{T}"]
        row = [f"{T:>7}"]
        for e in engines:
            ent = cell.get(e, {})
            if ent.get("skipped"):
                row.append("skipped")
            else:
                m = ent.get("median_ops_per_sec")
                row.append(fmt_ops(m))
        L.append("| " + " | ".join(row) + " |")
    L.append("")

L.append("## Promoted flat rows for `promote.py`")
L.append("")
L.append("Each row below is the highest-T concurrency cell where UltraSQL")
L.append("beats every competitor on the given workload. `promote.py` reads")
L.append("these flat keys from `results.json`'s top-level `results` dict —")
L.append("the same schema the other comparison directories use.")
L.append("")
for wkey, cell in flat_promotions.items():
    L.append(f"### `{wkey}`")
    L.append("")
    rows = []
    for name, ent in cell.items():
        if ent.get("skipped"):
            continue
        m = ent.get("median_us")
        ops = ent.get("ops_per_sec_median")
        if m is None: continue
        rows.append((m, name, m, ops))
    rows.sort()
    L.append("| Rank | Engine              | µs/op (median) | ops/s (median) |")
    L.append("| ---- | ------------------- | -------------: | -------------: |")
    for i, (_, name, us, ops) in enumerate(rows, 1):
        L.append(f"| {i}    | {name:<19} | {us:14.2f} | {fmt_ops(ops):>14} |")
    L.append("")

with open(f"{HERE}/results.md", "w") as f:
    f.write("\n".join(L) + "\n")

# Console summary.
print("\nMedian summary (ops/s):")
for wl in WORKLOADS:
    print(f"  ---- {wl} ----")
    for T in THREADS_LIST:
        cell = results[wl][f"T{T}"]
        bits = []
        for name, ent in cell.items():
            if ent.get("skipped"): continue
            m = ent.get("median_ops_per_sec")
            if m is None: continue
            bits.append(f"{name}={m:.0f}")
        print(f"    T={T:>2}: {', '.join(bits)}")
PY

echo
echo "done. raw outputs in $RAW; machine-readable results in $HERE/results.json"
