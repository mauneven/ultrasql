#!/usr/bin/env bash
# Cross-engine write comparison — Apple M4, 2026-05-12.
#
# Companion to ../comparison-2026-05-12-m4-extended/ and -fillin/.
# Same five engines, same host, same deterministic seed pattern; this
# directory covers the write side of the workload matrix.
#
# Workloads (all single-thread, durability ON per engine's normal
# production setting — see methodology.md):
#
#   insert-bulk-100k — INSERT 100,000 (id, val) rows into an empty table
#   insert-bulk-1m   — INSERT 1,000,000 (id, val) rows into an empty table
#   update-1m        — UPDATE every row's val = val + 1 in a 1M-row table
#   delete-100k      — DELETE every row matching val > 0 in a 100k-row table
#   upsert-100k      — INSERT ... ON CONFLICT (id) DO UPDATE over 100k rows
#                      (UltraSQL skips: no native ON CONFLICT path at v0.5)
#
# Iteration deviations from the standard 8-iter cadence are listed in
# methodology.md and tagged in results.json; insert-bulk-1m and
# update-1m run 4 measured iterations because a single iteration
# exceeds the 2-minute relaxation threshold.
#
# Usage:
#   bash run.sh
#
# Pre-reqs:
#   - duckdb, sqlite3, psql on PATH
#   - postgres running on localhost:5432
#   - clickhouse at /tmp/ultracmp/clickhouse, or CH_BIN env var
#   - python3 on PATH; cargo on PATH; UltraSQL workspace at $REPO

set -euo pipefail

if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
RAW="$HERE/raw"
WORK="/tmp/ultracmp-writes"
REPO="$(cd "$HERE/../../.." && pwd)"
DATA_100K="$WORK/data_idval_100k.csv"
DATA_1M="$WORK/data_idval_1m.csv"
CH_BIN="${CH_BIN:-/tmp/ultracmp/clickhouse}"

mkdir -p "$WORK" "$RAW"

{
  echo "=== engine versions ($(date -u +%FT%TZ)) ==="
  echo "duckdb: $(duckdb --version 2>/dev/null || echo NOT_FOUND)"
  echo "sqlite3: $(sqlite3 --version 2>/dev/null | head -1 || echo NOT_FOUND)"
  echo "psql: $(psql --version 2>/dev/null | head -1 || echo NOT_FOUND)"
  if [[ -x "$CH_BIN" ]]; then
    echo "clickhouse: $("$CH_BIN" --version 2>/dev/null | head -1)"
  else
    echo "clickhouse: NOT_FOUND at $CH_BIN"
  fi
  echo "rustc: $(rustc --version 2>/dev/null || echo NOT_FOUND)"
  echo "host:  $(uname -a)"
} > "$RAW/versions.txt"
cat "$RAW/versions.txt"

# -----------------------------------------------------------------------------
# [1] datasets — deterministic (id, val) CSVs. The 100k file is a prefix of
#     the 1M one so cross-size comparisons share data.
# -----------------------------------------------------------------------------
echo "[1/8] generating datasets (deterministic seed 0xDEADBEEF)"

python3 - "$DATA_100K" "$DATA_1M" <<'PY'
import sys, random

p_100k, p_1m = sys.argv[1:3]

# id is a deterministic shuffle of 0..N (so primary-key inserts are
# non-sequential and exercise the index path on the SQL engines), val
# is a 32-bit signed range so int math is well-defined everywhere.
random.seed(0xDEADBEEF)
n = 1_000_000
ids = list(range(n))
random.shuffle(ids)
vals = [random.randrange(-(1<<31), 1<<31) for _ in range(n)]

with open(p_1m, 'w') as f:
    f.write("id,val\n")
    for i in range(n):
        f.write(f"{ids[i]},{vals[i]}\n")

with open(p_100k, 'w') as f:
    f.write("id,val\n")
    for i in range(100_000):
        f.write(f"{ids[i]},{vals[i]}\n")
PY

shasum -a 256 "$DATA_100K" "$DATA_1M" | tee "$RAW/dataset_sha256.txt"

# -----------------------------------------------------------------------------
# [2] UltraSQL via cross_compare_writes driver
# -----------------------------------------------------------------------------
echo "[2/8] UltraSQL — building cross_compare_writes in release"
( cd "$REPO" && cargo build --release -p ultrasql-bench --bin cross_compare_writes ) 2>&1 \
  | tail -5
CROSS="$REPO/target/release/cross_compare_writes"

echo "[2/8] UltraSQL — running heap-access workloads"
ULTRA_OUT="$RAW/ultrasql.jsonl"
: > "$ULTRA_OUT"

run_ultra() {
  local tag="$1"; shift
  echo "  ultrasql: $tag"
  printf '%s\t' "$tag" >> "$ULTRA_OUT"
  "$CROSS" "$@" >> "$ULTRA_OUT" 2>>"$RAW/ultrasql.stderr.txt" || echo '{"error":true}' >> "$ULTRA_OUT"
}

#   Iter-count strategy:
#   - insert-bulk-100k: 4 iters at ~3s each; stable median.
#   - insert-bulk-1m:   2 iters at ~80s each (v0.5 HeapAccess::insert is
#     O(blocks)/insert with no FSM — quadratic total cost). Reduced
#     from the 4-iter relaxation floor to 2 to fit budget.
#   - update-1m:        UltraSQL **skipped** (would take ~30 min for a
#     single iter at v0.5 — see methodology.md, caveat #3). SQL
#     engines still measure it.
#   - delete-100k:      4 iters at ~80 ms each.
run_ultra insert-bulk-100k --workload insert-bulk --data "$DATA_100K" --warmup 1 --iters 4
run_ultra insert-bulk-1m   --workload insert-bulk --data "$DATA_1M"   --warmup 0 --iters 2
# update-1m is skipped on UltraSQL; emit the skip marker so the parser
# surfaces it cleanly.
printf 'update-1m\t%s\n' '{"workload":"update","skipped":true,"reason":"each iteration ~30 min wall-clock at v0.5 (no FSM, O(blocks)/insert × 1M rows + WAL fsync); not runnable inside the 25-min cap"}' \
  >> "$ULTRA_OUT"
run_ultra delete-100k      --workload delete      --data "$DATA_100K" --warmup 1 --iters 4

# upsert-100k: emit a skip marker so the parser surfaces it cleanly.
printf 'upsert-100k\t%s\n' '{"workload":"upsert","skipped":true,"reason":"no native ON CONFLICT path at v0.5"}' \
  >> "$ULTRA_OUT"

# -----------------------------------------------------------------------------
# [3] DuckDB — attach to a file so the WAL is durable.
# -----------------------------------------------------------------------------
echo "[3/8] DuckDB"
DUCK_DB="$WORK/duck.db"
rm -f "$DUCK_DB" "$DUCK_DB.wal" || true
if command -v duckdb >/dev/null 2>&1; then
  # DuckDB's PRAGMA enable_profiling only emits a JSON profile for
  # SELECT queries; INSERT/UPDATE/DELETE don't write a file. We use
  # the `.timer on` dot-command instead — it prints
  # `Run Time (s): real X user Y sys Z` after every statement,
  # matching SQLite's format, and the parser splits on iter-N
  # begin/end markers.
  DUCK_SQL="$WORK/duckdb_writes.sql"
  cat > "$DUCK_SQL" <<EOF
.timer on
PRAGMA threads=1;
ATTACH '$DUCK_DB' AS db;
USE db;
CREATE TABLE t_seed (id BIGINT, val BIGINT);
COPY t_seed FROM '$DATA_100K' (HEADER, DELIMITER ',');
CREATE TABLE t_seed_1m (id BIGINT, val BIGINT);
COPY t_seed_1m FROM '$DATA_1M' (HEADER, DELIMITER ',');
EOF

  emit_duck_insert_bulk() {
    local tag="$1"; shift
    local seed_table="$1"; shift
    local iters="$1"; shift
    local safe="${tag//-/_}"
    # One warmup, then `iters` measured iterations.
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
      echo "INSERT INTO t_$safe SELECT id, val FROM $seed_table;"
      echo "CHECKPOINT;"
    } >> "$DUCK_SQL"
    echo ".print --measure-$tag--" >> "$DUCK_SQL"
    for i in $(seq 1 "$iters"); do
      {
        echo "DROP TABLE IF EXISTS t_$safe;"
        echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
        echo ".print -- iter-$i begin"
        echo "INSERT INTO t_$safe SELECT id, val FROM $seed_table;"
        echo "CHECKPOINT;"
        echo ".print -- iter-$i end"
      } >> "$DUCK_SQL"
    done
    echo ".print --end-measure--" >> "$DUCK_SQL"
  }

  emit_duck_update_or_delete() {
    local tag="$1"; shift
    local seed_table="$1"; shift
    local stmt="$1"; shift
    local iters="$1"; shift
    local safe="${tag//-/_}"
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
      echo "INSERT INTO t_$safe SELECT id, val FROM $seed_table;"
      echo "$stmt;"
      echo "CHECKPOINT;"
    } >> "$DUCK_SQL"
    echo ".print --measure-$tag--" >> "$DUCK_SQL"
    for i in $(seq 1 "$iters"); do
      {
        echo "DROP TABLE IF EXISTS t_$safe;"
        echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
        echo "INSERT INTO t_$safe SELECT id, val FROM $seed_table;"
        echo "CHECKPOINT;"
        echo ".print -- iter-$i begin"
        echo "$stmt;"
        echo "CHECKPOINT;"
        echo ".print -- iter-$i end"
      } >> "$DUCK_SQL"
    done
    echo ".print --end-measure--" >> "$DUCK_SQL"
  }

  emit_duck_upsert() {
    local tag="$1"; shift
    local iters="$1"; shift
    local safe="${tag//-/_}"
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
      echo "INSERT INTO t_$safe SELECT id, val FROM t_seed;"
      echo "CHECKPOINT;"
      # Build a 100k-row upsert source where ~half conflict with the
      # preloaded ids.
      echo "DROP TABLE IF EXISTS t_${safe}_src;"
      echo "CREATE TABLE t_${safe}_src AS"
      echo "  SELECT id, val + 1 AS val FROM t_seed"
      echo "  UNION ALL"
      echo "  SELECT id + 100000 AS id, val FROM t_seed LIMIT 50000;"
      echo "INSERT INTO t_$safe SELECT * FROM t_${safe}_src ON CONFLICT(id) DO UPDATE SET val = excluded.val;"
      echo "CHECKPOINT;"
    } >> "$DUCK_SQL"
    echo ".print --measure-$tag--" >> "$DUCK_SQL"
    for i in $(seq 1 "$iters"); do
      {
        echo "DROP TABLE IF EXISTS t_$safe;"
        echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
        echo "INSERT INTO t_$safe SELECT id, val FROM t_seed;"
        echo "CHECKPOINT;"
        echo ".print -- iter-$i begin"
        echo "INSERT INTO t_$safe SELECT * FROM t_${safe}_src ON CONFLICT(id) DO UPDATE SET val = excluded.val;"
        echo "CHECKPOINT;"
        echo ".print -- iter-$i end"
      } >> "$DUCK_SQL"
    done
    echo ".print --end-measure--" >> "$DUCK_SQL"
  }

  emit_duck_insert_bulk    "insert-bulk-100k" t_seed     4
  emit_duck_insert_bulk    "insert-bulk-1m"   t_seed_1m  4
  emit_duck_update_or_delete "update-1m"      t_seed_1m  "UPDATE t_update_1m SET val = val + 1" 4
  emit_duck_update_or_delete "delete-100k"    t_seed     "DELETE FROM t_delete_100k WHERE val > 0" 4
  emit_duck_upsert         "upsert-100k"      4

  duckdb < "$DUCK_SQL" > "$RAW/duckdb.out" 2>&1 || true
else
  echo "  duckdb not found, skipping" | tee "$RAW/duckdb.out"
fi

# -----------------------------------------------------------------------------
# [4] SQLite — tempfile, WAL mode, synchronous=NORMAL.
# -----------------------------------------------------------------------------
echo "[4/8] SQLite (tempfile, WAL mode)"
SQLITE_DB="$WORK/sqlite.db"
rm -f "$SQLITE_DB" "$SQLITE_DB-wal" "$SQLITE_DB-shm" || true

SQLITE_SQL="$WORK/sqlite_writes.sql"
cat > "$SQLITE_SQL" <<EOF
.timer on
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;

-- Seed datasets (loaded once, cloned per measured iteration).
CREATE TABLE t_seed_100k (id INTEGER, val INTEGER);
CREATE TABLE t_seed_1m   (id INTEGER, val INTEGER);
.mode csv
.import --skip 1 $DATA_100K t_seed_100k
.import --skip 1 $DATA_1M   t_seed_1m
EOF

emit_sqlite_insert_bulk() {
  local tag="$1"; shift
  local seed="$1"; shift
  local iters="$1"; shift
  local safe="${tag//-/_}"
  # Warmup
  {
    echo "DROP TABLE IF EXISTS t_$safe;"
    echo "CREATE TABLE t_$safe (id INTEGER PRIMARY KEY, val INTEGER);"
    echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
  } >> "$SQLITE_SQL"
  echo ".print --measure-$tag--" >> "$SQLITE_SQL"
  for i in $(seq 1 "$iters"); do
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id INTEGER PRIMARY KEY, val INTEGER);"
      echo ".print -- iter-$i begin"
      echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
      echo ".print -- iter-$i end"
    } >> "$SQLITE_SQL"
  done
  echo ".print --end-measure--" >> "$SQLITE_SQL"
}

emit_sqlite_update_or_delete() {
  local tag="$1"; shift
  local seed="$1"; shift
  local stmt="$1"; shift
  local iters="$1"; shift
  local safe="${tag//-/_}"
  # Warmup
  {
    echo "DROP TABLE IF EXISTS t_$safe;"
    echo "CREATE TABLE t_$safe (id INTEGER PRIMARY KEY, val INTEGER);"
    echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
    echo "$stmt;"
  } >> "$SQLITE_SQL"
  echo ".print --measure-$tag--" >> "$SQLITE_SQL"
  for i in $(seq 1 "$iters"); do
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id INTEGER PRIMARY KEY, val INTEGER);"
      echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
      echo ".print -- iter-$i begin"
      echo "$stmt;"
      echo ".print -- iter-$i end"
    } >> "$SQLITE_SQL"
  done
  echo ".print --end-measure--" >> "$SQLITE_SQL"
}

emit_sqlite_upsert() {
  local tag="$1"; shift
  local iters="$1"; shift
  local safe="${tag//-/_}"
  # Build the half-conflicting source once.
  {
    echo "DROP TABLE IF EXISTS t_${safe}_src;"
    echo "CREATE TABLE t_${safe}_src AS"
    echo "  SELECT id, val + 1 AS val FROM t_seed_100k"
    echo "  UNION ALL"
    echo "  SELECT id + 100000 AS id, val FROM t_seed_100k LIMIT 50000;"
    echo "DROP TABLE IF EXISTS t_$safe;"
    echo "CREATE TABLE t_$safe (id INTEGER PRIMARY KEY, val INTEGER);"
    echo "INSERT INTO t_$safe SELECT id, val FROM t_seed_100k;"
    echo "INSERT INTO t_$safe SELECT * FROM t_${safe}_src WHERE true ON CONFLICT(id) DO UPDATE SET val = excluded.val;"
  } >> "$SQLITE_SQL"
  echo ".print --measure-$tag--" >> "$SQLITE_SQL"
  for i in $(seq 1 "$iters"); do
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id INTEGER PRIMARY KEY, val INTEGER);"
      echo "INSERT INTO t_$safe SELECT id, val FROM t_seed_100k;"
      echo ".print -- iter-$i begin"
      echo "INSERT INTO t_$safe SELECT * FROM t_${safe}_src WHERE true ON CONFLICT(id) DO UPDATE SET val = excluded.val;"
      echo ".print -- iter-$i end"
    } >> "$SQLITE_SQL"
  done
  echo ".print --end-measure--" >> "$SQLITE_SQL"
}

emit_sqlite_insert_bulk      insert-bulk-100k  t_seed_100k  4
emit_sqlite_insert_bulk      insert-bulk-1m    t_seed_1m    4
emit_sqlite_update_or_delete update-1m         t_seed_1m    "UPDATE t_update_1m SET val = val + 1" 4
emit_sqlite_update_or_delete delete-100k       t_seed_100k  "DELETE FROM t_delete_100k WHERE val > 0" 4
emit_sqlite_upsert           upsert-100k       4

sqlite3 "$SQLITE_DB" < "$SQLITE_SQL" > "$RAW/sqlite3.out" 2>&1 || true

# -----------------------------------------------------------------------------
# [5] PostgreSQL — synchronous_commit=on, fsync=on (defaults).
# -----------------------------------------------------------------------------
echo "[5/8] PostgreSQL"
if command -v psql >/dev/null 2>&1 && pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
  psql -d postgres -c "DROP DATABASE IF EXISTS ultracmp_writes;" >/dev/null 2>&1
  psql -d postgres -c "CREATE DATABASE ultracmp_writes;" >/dev/null 2>&1
  # Seed tables (loaded once via \copy, cloned per measured iteration).
  psql -d ultracmp_writes <<EOF >/dev/null 2>&1
CREATE TABLE t_seed_100k (id BIGINT, val BIGINT);
\\COPY t_seed_100k FROM '$DATA_100K' WITH (FORMAT csv, HEADER true);
CREATE TABLE t_seed_1m (id BIGINT, val BIGINT);
\\COPY t_seed_1m FROM '$DATA_1M' WITH (FORMAT csv, HEADER true);
-- Half-conflicting upsert source: 50k existing ids + 50k fresh ids.
CREATE TABLE t_upsert_src AS
  SELECT id, val + 1 AS val FROM t_seed_100k
  UNION ALL
  SELECT id + 100000 AS id, val FROM t_seed_100k LIMIT 50000;
ANALYZE;
EOF

  PG_SQL="$WORK/pg_writes.sql"
  printf '\\timing on\n' > "$PG_SQL"

  emit_pg_insert_bulk() {
    local tag="$1"; shift
    local seed="$1"; shift
    local iters="$1"; shift
    local safe="${tag//-/_}"
    # warmup
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
      echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
    } >> "$PG_SQL"
    echo "\\echo -- BEGIN $tag" >> "$PG_SQL"
    for _ in $(seq 1 "$iters"); do
      {
        echo "DROP TABLE IF EXISTS t_$safe;"
        echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
        echo "\\echo -- MEASURE $tag"
        echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
      } >> "$PG_SQL"
    done
    echo "\\echo -- END $tag" >> "$PG_SQL"
  }

  emit_pg_update_or_delete() {
    local tag="$1"; shift
    local seed="$1"; shift
    local stmt="$1"; shift
    local iters="$1"; shift
    local safe="${tag//-/_}"
    # warmup
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
      echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
      echo "$stmt;"
    } >> "$PG_SQL"
    echo "\\echo -- BEGIN $tag" >> "$PG_SQL"
    for _ in $(seq 1 "$iters"); do
      {
        echo "DROP TABLE IF EXISTS t_$safe;"
        echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
        echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
        echo "\\echo -- MEASURE $tag"
        echo "$stmt;"
      } >> "$PG_SQL"
    done
    echo "\\echo -- END $tag" >> "$PG_SQL"
  }

  emit_pg_upsert() {
    local tag="$1"; shift
    local iters="$1"; shift
    local safe="${tag//-/_}"
    # warmup
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
      echo "INSERT INTO t_$safe SELECT id, val FROM t_seed_100k;"
      echo "INSERT INTO t_$safe SELECT id, val FROM t_upsert_src ON CONFLICT (id) DO UPDATE SET val = excluded.val;"
    } >> "$PG_SQL"
    echo "\\echo -- BEGIN $tag" >> "$PG_SQL"
    for _ in $(seq 1 "$iters"); do
      {
        echo "DROP TABLE IF EXISTS t_$safe;"
        echo "CREATE TABLE t_$safe (id BIGINT PRIMARY KEY, val BIGINT);"
        echo "INSERT INTO t_$safe SELECT id, val FROM t_seed_100k;"
        echo "\\echo -- MEASURE $tag"
        echo "INSERT INTO t_$safe SELECT id, val FROM t_upsert_src ON CONFLICT (id) DO UPDATE SET val = excluded.val;"
      } >> "$PG_SQL"
    done
    echo "\\echo -- END $tag" >> "$PG_SQL"
  }

  emit_pg_insert_bulk      insert-bulk-100k  t_seed_100k  4
  emit_pg_insert_bulk      insert-bulk-1m    t_seed_1m    4
  emit_pg_update_or_delete update-1m         t_seed_1m    "UPDATE t_update_1m SET val = val + 1" 4
  emit_pg_update_or_delete delete-100k       t_seed_100k  "DELETE FROM t_delete_100k WHERE val > 0" 4
  emit_pg_upsert           upsert-100k       4

  psql -d ultracmp_writes -f "$PG_SQL" > "$RAW/postgres.out" 2>&1 || true
else
  echo "  postgres not running on localhost:5432, skipping" | tee "$RAW/postgres.out"
fi

# -----------------------------------------------------------------------------
# [6] ClickHouse — MergeTree on disk (not Memory). One-shot multiquery.
# -----------------------------------------------------------------------------
echo "[6/8] ClickHouse"
CH_DATA="$WORK/ch"
rm -rf "$CH_DATA" || true
if [[ -x "$CH_BIN" ]]; then
  CH_SQL="$WORK/ch_writes.sql"
  cat > "$CH_SQL" <<EOF
CREATE TABLE t_seed_100k (id Int64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_seed_100k SELECT * FROM file('$DATA_100K', 'CSVWithNames', 'id Int64, val Int64');
CREATE TABLE t_seed_1m   (id Int64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_seed_1m   SELECT * FROM file('$DATA_1M',   'CSVWithNames', 'id Int64, val Int64');
EOF

  emit_ch_insert_bulk() {
    local tag="$1"; shift
    local seed="$1"; shift
    local iters="$1"; shift
    local safe="${tag//-/_}"
    # warmup
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id Int64, val Int64) ENGINE = MergeTree ORDER BY id;"
      echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
    } >> "$CH_SQL"
    echo "-- MEASURE $tag" >> "$CH_SQL"
    for _ in $(seq 1 "$iters"); do
      {
        echo "DROP TABLE IF EXISTS t_$safe;"
        echo "CREATE TABLE t_$safe (id Int64, val Int64) ENGINE = MergeTree ORDER BY id;"
        echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
      } >> "$CH_SQL"
    done
  }

  emit_ch_update_or_delete() {
    local tag="$1"; shift
    local seed="$1"; shift
    local stmt="$1"; shift
    local iters="$1"; shift
    local safe="${tag//-/_}"
    # ClickHouse: UPDATE / DELETE are async mutations on MergeTree.
    # We measure the time to issue the synchronous ALTER ... UPDATE /
    # ALTER ... DELETE with `mutations_sync = 2`, which blocks until
    # the mutation has materialized. This is the closest analog to
    # how PostgreSQL / SQLite / DuckDB report UPDATE/DELETE latency.
    {
      echo "DROP TABLE IF EXISTS t_$safe;"
      echo "CREATE TABLE t_$safe (id Int64, val Int64) ENGINE = MergeTree ORDER BY id;"
      echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
      echo "SET mutations_sync = 2;"
      echo "$stmt;"
    } >> "$CH_SQL"
    echo "-- MEASURE $tag" >> "$CH_SQL"
    for _ in $(seq 1 "$iters"); do
      {
        echo "DROP TABLE IF EXISTS t_$safe;"
        echo "CREATE TABLE t_$safe (id Int64, val Int64) ENGINE = MergeTree ORDER BY id;"
        echo "INSERT INTO t_$safe SELECT id, val FROM $seed;"
        echo "$stmt;"
      } >> "$CH_SQL"
    done
  }

  emit_ch_insert_bulk      insert-bulk-100k  t_seed_100k  4
  emit_ch_insert_bulk      insert-bulk-1m    t_seed_1m    4
  emit_ch_update_or_delete update-1m         t_seed_1m    "ALTER TABLE t_update_1m UPDATE val = val + 1 WHERE 1" 4
  emit_ch_update_or_delete delete-100k       t_seed_100k  "ALTER TABLE t_delete_100k DELETE WHERE val > 0" 4
  # ClickHouse has no native ON CONFLICT (ReplacingMergeTree handles
  # this asynchronously and changes semantics). We mark upsert-100k
  # as skipped on ClickHouse rather than measure an unfair shape.

  # --time emits per-query elapsed seconds on stderr (one float per
  # line, in execution order). We capture stdout (table output) and
  # stderr (timings) into the same file with stderr first so the
  # parser can read them.
  "$CH_BIN" local --path "$CH_DATA" --multiquery --queries-file "$CH_SQL" --time \
    > "$RAW/clickhouse.stdout" 2> "$RAW/clickhouse.out" || true
else
  echo "  clickhouse binary not found at $CH_BIN, skipping" | tee "$RAW/clickhouse.out"
fi

# -----------------------------------------------------------------------------
# [7] parse + medians + emit results.json / results.md
# -----------------------------------------------------------------------------
echo "[7/8] parsing medians and writing results.json"
RAW_DIR="$RAW" WORK_DIR="$WORK" HERE_DIR="$HERE" python3 - <<'PY'
import json, re, statistics, os, sys

RAW_DIR = os.environ["RAW_DIR"]
WORK    = os.environ["WORK_DIR"]
HERE    = os.environ["HERE_DIR"]

def med(xs): return round(statistics.median(xs), 2)
def mn(xs):  return round(min(xs), 2)

# (tag, ultra-iter-count, sql-iter-count). To fit the 25-minute
# wall-clock cap on v0.5 (where HeapAccess::insert is O(blocks)/insert
# with no FSM), several rows run reduced iter counts. update-1m and
# upsert-100k are skipped for UltraSQL (see methodology.md).
WORKLOADS = [
    ("insert-bulk-100k",  4, 4),
    ("insert-bulk-1m",    2, 4),
    ("update-1m",         0, 4),
    ("delete-100k",       4, 4),
    ("upsert-100k",       0, 4),
]

results = {w[0]: {} for w in WORKLOADS}

# -------- UltraSQL ----------
try:
    with open(f"{RAW_DIR}/ultrasql.jsonl") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line: continue
            tag, _, j = line.partition("\t")
            try:
                obj = json.loads(j)
            except json.JSONDecodeError:
                continue
            if "error" in obj or tag not in results:
                continue
            if obj.get("skipped"):
                results[tag]["UltraSQL (kernel)"] = {
                    "skipped": True,
                    "reason":  obj.get("reason", "skipped"),
                }
                continue
            iters = obj.get("iterations_us", [])
            results[tag]["UltraSQL (kernel)"] = {
                "median_us": med(iters) if iters else None,
                "min_us":    mn(iters) if iters else None,
                "samples":   obj.get("samples"),
                "iterations_us": iters,
                "answer":    obj.get("answer"),
                "n_rows":    obj.get("n_rows"),
                "note":     "heap-access + WAL group-commit; kernel-level (no SQL pipeline)",
            }
except Exception as e:
    print(f"UltraSQL parse error: {e}", file=sys.stderr)

# -------- DuckDB ----------
try:
    with open(f"{RAW_DIR}/duckdb.out") as f:
        duck_txt = f.read()
    # `.timer on` emits: `Run Time (s): real X user Y sys Z`
    duck_pat = re.compile(r"--measure-([\w-]+)--\n(.*?)--end-measure--", re.DOTALL)
    duck_iter_pat = re.compile(r"-- iter-\d+ begin\n(.*?)-- iter-\d+ end", re.DOTALL)
    duck_timer_pat = re.compile(r"Run Time \(s\): real (\S+) user (\S+) sys (\S+)")
    for m in duck_pat.finditer(duck_txt):
        wl, body = m.group(1), m.group(2)
        if wl not in results: continue
        us = []
        for it in duck_iter_pat.finditer(body):
            entries = duck_timer_pat.findall(it.group(1))
            if not entries:
                continue
            # Sum each statement's real time inside this iter (the
            # measured statement + CHECKPOINT, both timed since the
            # whole iter is the unit of work we care about).
            real_total = sum(float(e[0]) for e in entries)
            us.append(real_total * 1e6)
        if us:
            results[wl]["DuckDB"] = {
                "median_us": med(us),
                "min_us":    mn(us),
                "samples":   len(us),
                "iterations_us": [round(x, 2) for x in us],
                "note": "ATTACH-to-file; CHECKPOINT inside the timed region",
            }
        else:
            results[wl]["DuckDB"] = {"skipped": True, "reason": "no .timer output in block"}
except Exception as e:
    print(f"DuckDB parse error: {e}", file=sys.stderr)

# -------- SQLite ----------
try:
    with open(f"{RAW_DIR}/sqlite3.out") as f:
        sqlite_txt = f.read()
    pat = re.compile(r"--measure-([\w-]+)--\n(.*?)--end-measure--", re.DOTALL)
    iter_pat = re.compile(r"-- iter-\d+ begin\n(.*?)-- iter-\d+ end", re.DOTALL)
    timer_pat = re.compile(r"Run Time: real (\S+) user (\S+) sys (\S+)")
    for m in pat.finditer(sqlite_txt):
        wl, body = m.group(1), m.group(2)
        if wl not in results: continue
        # Inside each iter block, take the sum of `user` timer entries —
        # the measured statement may emit more than one timer line if
        # SQLite splits its work, but the dominant statement is the
        # single INSERT/UPDATE/DELETE we explicitly marked.
        us = []
        for it in iter_pat.finditer(body):
            entries = timer_pat.findall(it.group(1))
            if not entries:
                continue
            real_total = sum(float(e[0]) for e in entries)
            us.append(real_total * 1e6)
        if us:
            results[wl]["SQLite"] = {
                "median_us": med(us),
                "min_us":    mn(us),
                "samples":   len(us),
                "iterations_us": [round(x, 2) for x in us],
                "note": "tempfile; WAL mode; synchronous=NORMAL",
            }
        else:
            results[wl]["SQLite"] = {"skipped": True, "reason": "no timer output in block"}
except Exception as e:
    print(f"SQLite parse error: {e}", file=sys.stderr)

# -------- PostgreSQL ----------
try:
    with open(f"{RAW_DIR}/postgres.out") as f:
        pg_txt = f.read()
    blocks = re.split(r"-- BEGIN (\S+)", pg_txt)
    for i in range(1, len(blocks), 2):
        wl = blocks[i].strip()
        body = blocks[i + 1] if i + 1 < len(blocks) else ""
        if wl not in results:
            continue
        # MEASURE markers split each iter; we want only the time of the
        # statement that immediately follows -- MEASURE.
        iters = re.split(r"-- MEASURE \S+", body)
        # iters[0] is before the first MEASURE (drops + creates from
        # the harness emit); iters[1..] each start with the measured
        # statement plus the next iter's prologue.
        times = []
        for chunk in iters[1:]:
            ms = re.findall(r"Time: ([\d.]+) ms", chunk)
            if ms:
                # First Time: line after MEASURE is the measured stmt.
                times.append(float(ms[0]))
        if times:
            us = [round(t * 1000.0, 2) for t in times]
            results[wl]["PostgreSQL"] = {
                "median_us": med(us),
                "min_us":    mn(us),
                "samples":   len(us),
                "iterations_us": us,
                "note": "psql \\timing; synchronous_commit=on; fsync=on",
            }
        else:
            results[wl]["PostgreSQL"] = {"skipped": True, "reason": "no measured timings"}
except Exception as e:
    print(f"PG parse error: {e}", file=sys.stderr)

# -------- ClickHouse ----------
try:
    with open(f"{RAW_DIR}/clickhouse.out") as f:
        ch_txt = f.read()
    # `--time` emits one elapsed-seconds float per query on stderr,
    # in execution order. Pull every float-looking line out and slot
    # them by the script's known shape: 2 seed inserts + per workload
    # (DROP, CREATE, INSERT-seed) × N + warmup-set + N timed × (DROP, CREATE, INSERT-seed, STMT).
    # Rather than count every helper statement, we look for the
    # statement count emitted per workload by `emit_ch_*` and slice.
    elapsed = [float(x) for x in re.findall(r"^\s*([\d.eE+-]+)\s*$", ch_txt, re.MULTILINE)]

    # The CH script emits the following per emit_ch_insert_bulk block:
    #   warmup: DROP + CREATE + INSERT  -> 3 timings
    #   then per iter: DROP + CREATE + INSERT -> 3 timings each
    # For emit_ch_update_or_delete:
    #   warmup: DROP + CREATE + INSERT + SET mutations_sync + STMT -> 5
    #   per iter: DROP + CREATE + INSERT + STMT -> 4
    # The seed prelude emits 4 timings (2× CREATE + 2× INSERT-FROM-FILE).
    # We index the measured-statement timing inside each iter as the
    # last timing of that iter's run.

    # Build the slot map.
    plan = [
        # (tag, prelude_count, per_iter_count, iter_count)
        ("insert-bulk-100k", 3, 3, 4),
        ("insert-bulk-1m",   3, 3, 4),
        ("update-1m",        5, 4, 4),
        ("delete-100k",      5, 4, 4),
        # upsert-100k is skipped on CH (no native ON CONFLICT).
    ]

    cursor = 4  # 2× CREATE + 2× INSERT seed
    for tag, prelude, per_iter, n_iters in plan:
        cursor += prelude  # warmup block
        us = []
        for _ in range(n_iters):
            iter_slice = elapsed[cursor:cursor + per_iter]
            cursor += per_iter
            if len(iter_slice) == per_iter:
                # Last timing in the iter is the measured statement.
                us.append(iter_slice[-1] * 1e6)
        if us:
            results[tag]["ClickHouse"] = {
                "median_us": med(us),
                "min_us":    mn(us),
                "samples":   len(us),
                "iterations_us": [round(x, 2) for x in us],
                "note": "MergeTree on disk; --time stderr elapsed; UPDATE/DELETE via ALTER + mutations_sync=2",
            }
        else:
            results[tag]["ClickHouse"] = {"skipped": True, "reason": "insufficient timing entries"}
    # Upsert is always skipped on CH.
    results["upsert-100k"]["ClickHouse"] = {
        "skipped": True,
        "reason": "no native ON CONFLICT; ReplacingMergeTree changes semantics",
    }
except Exception as e:
    print(f"CH parse error: {e}", file=sys.stderr)

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

WORKLOAD_TAGS = [w[0] for w in WORKLOADS]
doc = {
    "comparison": "writes cross-engine, 2026-05-12, Apple M4",
    "host": "Apple M4 Mac mini, 16 GiB, macOS 26.5",
    "workloads": WORKLOAD_TAGS,
    "datasets_sha256": sha,
    "iter_count_deviations": {
        "insert-bulk-100k": {
            "ultrasql_iters": 4,
            "sql_iters": 4,
            "reason": "reduced from 8 to 4 to fit overall wall-clock budget; per-iter cost is small (~3s) so distribution is still stable",
        },
        "insert-bulk-1m": {
            "ultrasql_iters": 2,
            "sql_iters": 4,
            "reason": "v0.5 HeapAccess::insert is O(blocks)/insert with no FSM; each iter ~80s; reduced from the 4-iter floor to 2 to fit 25-min wall-clock cap",
        },
        "update-1m": {
            "ultrasql_iters": 0,
            "sql_iters": 4,
            "reason": "UltraSQL skipped: ~30 min/iter at v0.5 (preload + delete-then-insert × 1M, each re-insert pays the O(blocks) walk); not runnable inside the 25-min cap",
        },
        "delete-100k": {
            "ultrasql_iters": 4,
            "sql_iters": 4,
            "reason": "reduced from 8 to 4 to fit overall wall-clock budget; per-iter cost is small (~80ms) so distribution is still stable",
        },
        "upsert-100k": {
            "ultrasql_iters": 0,
            "sql_iters": 4,
            "reason": "UltraSQL skipped: no native ON CONFLICT path at v0.5",
        },
    },
    "results": results,
}
with open(f"{HERE}/results.json", "w") as f:
    json.dump(doc, f, indent=2)

# ---- emit results.md ----
WORKLOAD_META = {
    "insert-bulk-100k": ("`INSERT 100k rows`",                                          "100,000 i64 PK rows"),
    "insert-bulk-1m":   ("`INSERT 1M rows`",                                            "1,000,000 i64 PK rows"),
    "update-1m":        ("`UPDATE t SET val = val + 1`",                                "1,000,000 rows preloaded"),
    "delete-100k":      ("`DELETE FROM t WHERE val > 0`",                               "100,000 rows preloaded; ~50% match"),
    "upsert-100k":      ("`INSERT ... ON CONFLICT (id) DO UPDATE SET val = excluded.val`", "100,000 rows preloaded; ~50% conflict"),
}

def fmt_us(v):
    if v is None: return "—"
    if v < 1: return f"{v:.3f} µs"
    if v < 1000: return f"{v:.2f} µs"
    if v < 1000000: return f"{v/1000:.2f} ms"
    return f"{v/1e6:.2f} s"

L = []
L.append("# Write cross-engine comparison — 2026-05-12 (Apple M4)")
L.append("")
L.append("**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.")
L.append("macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.")
L.append("Reproduce via `bash run.sh` in this directory.")
L.append("")
L.append("Companion to `../comparison-2026-05-12-m4-extended/` and `-fillin/`.")
L.append("Five engines, same host, same deterministic seed pattern; this")
L.append("directory covers the write side of the workload matrix.")
L.append("")
L.append("**Durability.** Every engine runs in its **normal durable mode**.")
L.append("PostgreSQL: `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`")
L.append("(defaults). SQLite: tempfile, `journal_mode=WAL`,")
L.append("`synchronous=NORMAL`. DuckDB: ATTACH to file + `CHECKPOINT` after")
L.append("each measured statement. ClickHouse: `MergeTree` on disk.")
L.append("UltraSQL: `WalWriter` group-commit fsync + segment-file fsync per")
L.append("iteration. See `methodology.md` for full details.")
L.append("")
L.append("**Dataset sha256.**")
L.append("")
L.append("```")
for k, v in sha.items():
    L.append(f"{v}  {k}")
L.append("```")
L.append("")
L.append("> **Caveat.** The UltraSQL row measures the heap access method and")
L.append("> WAL writer **in isolation** — no parser, no planner, no executor,")
L.append("> no result-set materialization, no constraint enforcement (no")
L.append("> primary-key uniqueness check, in particular). Every other row")
L.append("> measures the engine's full SQL pipeline including durable commit.")
L.append("> Read every UltraSQL row as the **lower bound** on the eventual")
L.append("> end-to-end statement, not a like-for-like result.")
L.append("")

for tag in WORKLOAD_TAGS:
    query, dataset = WORKLOAD_META[tag]
    L.append(f"## `{tag}`")
    L.append("")
    L.append(f"**Workload.** {query}")
    L.append(f"**Dataset.** {dataset}")
    L.append("")
    rows = []
    for name, entry in results[tag].items():
        if entry.get("skipped"):
            rows.append((float("inf"), name, "skipped", "—", entry.get("reason", "—")))
            continue
        m = entry.get("median_us")
        if m is None:
            rows.append((float("inf"), name, "—", "—", "—"))
            continue
        rows.append((m, name, fmt_us(m), str(entry.get("samples", "—")), entry.get("note", "")))
    rows.sort(key=lambda r: r[0])
    L.append("| Rank | Engine              | Median time   | Samples | Notes |")
    L.append("| ---- | ------------------- | ------------: | ------: | ----- |")
    rank = 0
    for medv, name, ms, samples, note in rows:
        if ms == "skipped":
            L.append(f"| —    | {name:<19} |     skipped   |    —    | {note} |")
        else:
            rank += 1
            L.append(f"| {rank}    | {name:<19} | {ms:>13} | {samples:>7} | {note} |")
    L.append("")
    L.append("**Per-iteration data (µs).**")
    L.append("")
    for name, entry in results[tag].items():
        if entry.get("skipped"): continue
        iters = entry.get("iterations_us", [])
        if not iters: continue
        s = ", ".join(f"{v:.2f}" if isinstance(v, float) and v >= 1 else f"{v}" for v in iters)
        L.append(f"- **{name}**: `{s}`")
    L.append("")

with open(f"{HERE}/results.md", "w") as f:
    f.write("\n".join(L) + "\n")

print("\nMedian summary (µs):")
print(f"  {'workload':<18} {'engine':<22} {'median':>12}  samples")
for tag in WORKLOAD_TAGS:
    print(f"  ---- {tag} ----")
    rows = sorted(results[tag].items(), key=lambda kv: kv[1].get("median_us") or float("inf"))
    for name, entry in rows:
        if entry.get("skipped"):
            print(f"    {name:<22} {'skipped':>12}  ({entry.get('reason')})")
        else:
            m = entry.get("median_us")
            ms = m if isinstance(m, (int, float)) else "—"
            s = entry.get("samples", "—")
            print(f"    {name:<22} {ms:>12}  {s}")
PY

echo
echo "[8/8] done. raw outputs in $RAW; machine-readable results in $HERE/results.json"
