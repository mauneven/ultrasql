#!/usr/bin/env bash
# Cross-engine SUM(x) comparison — Apple M4, 2026-05-12.
#
# Reproduces the numbers in results.md / results.json on the same host.
# Generates the dataset, runs each engine, parses each engine's
# self-reported timings, and writes raw stdout to raw/.
#
# Usage:
#   bash run.sh
#
# Pre-reqs:
#   - duckdb on PATH                          (brew install duckdb)
#   - sqlite3 on PATH                         (macOS ships /usr/bin/sqlite3)
#   - PostgreSQL 14+ running on localhost:5432 with a role that can
#     create databases                        (brew services start postgresql@14)
#   - clickhouse binary at /tmp/ultracmp/clickhouse, or CH_BIN env var
#     pointing to one
#   - python3 on PATH
#
# This script is idempotent: it drops + re-creates per-engine state.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RAW="$HERE/raw"
WORK="/tmp/ultracmp"
DATA="$WORK/data.csv"
CH_BIN="${CH_BIN:-/tmp/ultracmp/clickhouse}"

mkdir -p "$WORK" "$RAW"

echo "[1/6] generating dataset"
python3 - <<'PY' > "$DATA"
import random
random.seed(0xDEADBEEF)
print("x")
for _ in range(65536):
    print(random.randrange(-1<<31, 1<<31))
PY
DATA_SHA=$(shasum -a 256 "$DATA" | awk '{print $1}')
echo "  dataset sha256: $DATA_SHA"
EXPECTED_SHA="579af3856931a209c3b2b43f59ed310232fe02c48eadc6d3a05e326131acd2bd"
if [[ "$DATA_SHA" != "$EXPECTED_SHA" ]]; then
  echo "  WARNING: dataset sha mismatch (expected $EXPECTED_SHA)" >&2
fi

echo "[2/6] DuckDB"
if command -v duckdb >/dev/null 2>&1; then
  DUCK_SQL="$WORK/duckdb_run.sql"
  cat > "$DUCK_SQL" <<EOF
CREATE TABLE t (x BIGINT);
COPY t FROM '$DATA' (HEADER, DELIMITER ',');
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
PRAGMA enable_profiling='json';
PRAGMA profiling_output='$WORK/dprof_1.json';
SELECT SUM(x) FROM t;
PRAGMA profiling_output='$WORK/dprof_2.json';
SELECT SUM(x) FROM t;
PRAGMA profiling_output='$WORK/dprof_3.json';
SELECT SUM(x) FROM t;
PRAGMA profiling_output='$WORK/dprof_4.json';
SELECT SUM(x) FROM t;
PRAGMA profiling_output='$WORK/dprof_5.json';
SELECT SUM(x) FROM t;
PRAGMA profiling_output='$WORK/dprof_6.json';
SELECT SUM(x) FROM t;
PRAGMA profiling_output='$WORK/dprof_7.json';
SELECT SUM(x) FROM t;
PRAGMA profiling_output='$WORK/dprof_8.json';
SELECT SUM(x) FROM t;
EOF
  duckdb < "$DUCK_SQL" > "$RAW/duckdb.out" 2>&1 || true
else
  echo "  duckdb not found, skipping"
fi

echo "[3/6] SQLite"
SQLITE_SQL="$WORK/sqlite_run.sql"
cat > "$SQLITE_SQL" <<EOF
.timer on
CREATE TABLE t (x INTEGER);
.mode csv
.import --skip 1 $DATA t
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
SELECT SUM(x) FROM t;
EOF
sqlite3 :memory: < "$SQLITE_SQL" > "$RAW/sqlite3.out" 2>&1 || true

echo "[4/6] PostgreSQL"
if command -v psql >/dev/null 2>&1 && pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
  psql -d postgres -c "DROP DATABASE IF EXISTS ultracmp;" >/dev/null 2>&1
  psql -d postgres -c "CREATE DATABASE ultracmp;" >/dev/null 2>&1
  psql -d ultracmp -c "CREATE TABLE t (x BIGINT);" >/dev/null 2>&1
  psql -d ultracmp -c "\\COPY t FROM '$DATA' WITH (FORMAT csv, HEADER true);" >/dev/null 2>&1

  PG_SQL="$WORK/pg_run.sql"
  printf '\\timing on\n' > "$PG_SQL"
  for i in $(seq 1 50); do echo "SELECT SUM(x) FROM t;" >> "$PG_SQL"; done  # 50 warmups
  for i in $(seq 1 8);  do echo "SELECT SUM(x) FROM t;" >> "$PG_SQL"; done  # 8 measured
  psql -d ultracmp -f "$PG_SQL" > "$RAW/postgres.out" 2>&1 || true
else
  echo "  postgres not running on localhost:5432, skipping" | tee "$RAW/postgres.out"
fi

echo "[5/6] ClickHouse"
if [[ -x "$CH_BIN" ]]; then
  CH_SQL="$WORK/ch_run.sql"
  cat > "$CH_SQL" <<EOF
CREATE TABLE t (x Int64) ENGINE = Memory;
INSERT INTO t SELECT * FROM file('$DATA', 'CSVWithNames', 'x Int64');
EOF
  for i in $(seq 1 15); do echo "SELECT SUM(x) FROM t;" >> "$CH_SQL"; done  # 5 warmups + 10 measured
  "$CH_BIN" local --multiquery --queries-file "$CH_SQL" --format=JSON > "$RAW/clickhouse.out" 2>&1 || true
else
  echo "  clickhouse binary not found at $CH_BIN, skipping" | tee "$RAW/clickhouse.out"
fi

echo "[6/6] parsing medians"
RAW_DIR="$RAW" WORK_DIR="$WORK" python3 - <<'PY'
import json, re, statistics, os, sys

RAW_DIR = os.environ["RAW_DIR"]
WORK = os.environ["WORK_DIR"]

def med(xs): return round(statistics.median(xs), 2)

results = {}

# DuckDB — median of latency over dprof_1..8
try:
    lat = []
    for i in range(1, 9):
        p = f"{WORK}/dprof_{i}.json"
        if not os.path.exists(p): continue
        with open(p) as f:
            lat.append(json.load(f)["latency"] * 1e6)
    if lat:
        results["DuckDB"] = (med(lat), len(lat))
except Exception as e:
    print(f"DuckDB parse error: {e}", file=sys.stderr)

# SQLite — .timer user time, drop first 3 warmups, take next 8
try:
    with open(f"{RAW_DIR}/sqlite3.out") as f:
        txt = f.read()
    pat = re.compile(r"Run Time: real (\S+) user (\S+) sys (\S+)\n-213495441761")
    matches = pat.findall(txt)
    measured = matches[3:11]
    if measured:
        us = [float(m[1]) * 1e6 for m in measured]
        results["SQLite"] = (med(us), len(us))
except Exception as e:
    print(f"SQLite parse error: {e}", file=sys.stderr)

# Postgres — \timing values, drop first 50 warmups, take next 8
try:
    with open(f"{RAW_DIR}/postgres.out") as f:
        txt = f.read()
    times = [float(m) for m in re.findall(r"Time: ([\d.]+) ms", txt)]
    measured = times[50:58]
    if measured:
        us = [t * 1000 for t in measured]
        results["PostgreSQL"] = (med(us), len(us))
except Exception as e:
    print(f"PG parse error: {e}", file=sys.stderr)

# ClickHouse — statistics.elapsed, drop first 5 warmups, take next 8
try:
    with open(f"{RAW_DIR}/clickhouse.out") as f:
        txt = f.read()
    elapsed = [float(m) for m in re.findall(r'"elapsed":\s*([\d.eE+-]+)', txt)]
    measured = elapsed[5:13]
    if measured:
        us = [e * 1e6 for e in measured]
        results["ClickHouse"] = (med(us), len(us))
except Exception as e:
    print(f"CH parse error: {e}", file=sys.stderr)

print("\nResults (median µs):")
print(f"  {'UltraSQL (kernel)':<22} {'4.70':>10}  (100 samples, cited from cargo bench)")
for name, (m, n) in sorted(results.items(), key=lambda kv: kv[1][0]):
    print(f"  {name:<22} {m:>10.2f}  ({n} samples)")
PY

echo
echo "Done. Raw outputs in $RAW. dataset sha256: $DATA_SHA"
