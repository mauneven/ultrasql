#!/usr/bin/env bash
# Cross-engine extended-workload comparison — Apple M4, 2026-05-12.
#
# Reproduces the numbers in results.md / results.json on the same host.
# Generates 1M and 10M row deterministic datasets, runs each engine
# through eight workloads, parses each engine's own timings, and
# writes per-engine stdout to raw/.
#
# Usage:
#   bash run.sh
#
# Pre-reqs (verified on owner's M4 host 2026-05-12):
#   - duckdb on PATH                       (/opt/homebrew/bin/duckdb, 1.5.2)
#   - sqlite3 on PATH                      (/usr/bin/sqlite3, 3.51.0)
#   - PostgreSQL 14+ running on localhost  (brew services start postgresql@14)
#   - clickhouse binary at /tmp/ultracmp/clickhouse, or CH_BIN env var
#   - python3 on PATH
#   - cargo on PATH; UltraSQL workspace at $REPO
#
# Workloads:
#   sum-1m       — SUM(x) over 1,000,000 i64 rows
#   sum-10m      — SUM(x) over 10,000,000 i64 rows
#   count-10m    — COUNT(*) over 10M
#   minmax-10m   — MIN(x), MAX(x) over 10M
#   avg-10m      — AVG(x) over 10M
#   filter-10m   — SUM(x) WHERE y > 0 over 10M (i64 x, i64 y)
#   range-10m    — COUNT(*) WHERE x BETWEEN -1e9 AND 1e9 over 10M
#   point-10m    — point lookup on 10M-row indexed table (SQL engines)
#                  UltraSQL row uses 1M because the v0.5 buffer pool
#                  refuses to evict dirty pages.
#   topk-1m      — SELECT x ORDER BY x LIMIT 10 over 1M (UltraSQL skipped)
#
# This script is idempotent: it drops + re-creates per-engine state.

set -euo pipefail

# Pick up rustup / cargo from the user's profile if it isn't already in PATH.
if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
RAW="$HERE/raw"
WORK="/tmp/ultracmp"
REPO="$(cd "$HERE/../../.." && pwd)"
DATA_X_1M="$WORK/data_x_1m.csv"
DATA_X_10M="$WORK/data_x_10m.csv"
DATA_Y_10M="$WORK/data_y_10m.csv"
DATA_XY_10M="$WORK/data_xy_10m.csv"
DATA_ID_10M="$WORK/data_id_10m.csv"
CH_BIN="${CH_BIN:-/tmp/ultracmp/clickhouse}"

mkdir -p "$WORK" "$RAW"

# Record engine versions up front so results.md can quote them.
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
# [1] datasets
# -----------------------------------------------------------------------------
echo "[1/8] generating datasets (deterministic seed 0xDEADBEEF)"

python3 - "$DATA_X_1M" "$DATA_X_10M" "$DATA_Y_10M" "$DATA_XY_10M" "$DATA_ID_10M" <<'PY'
import sys, random

p_x_1m, p_x_10m, p_y_10m, p_xy_10m, p_id_10m = sys.argv[1:6]

random.seed(0xDEADBEEF)
# 10M x values, then take the first 1M as the 1M dataset.
x = [random.randrange(-1<<31, 1<<31) for _ in range(10_000_000)]
# y stream uses a separate seed so the two columns are independent.
random.seed(0xBADC0DE)
y = [random.randrange(-1<<31, 1<<31) for _ in range(10_000_000)]

# Single-col 1M (x only).
with open(p_x_1m, 'w') as f:
    f.write("x\n")
    f.writelines(f"{v}\n" for v in x[:1_000_000])

# Single-col 10M (x only).
with open(p_x_10m, 'w') as f:
    f.write("x\n")
    f.writelines(f"{v}\n" for v in x)

# Single-col 10M (y only) — for the UltraSQL filter workload which
# loads x and y as separate columns.
with open(p_y_10m, 'w') as f:
    f.write("y\n")
    f.writelines(f"{v}\n" for v in y)

# Two-col 10M (x, y) — for the SQL engines doing SUM(x) WHERE y>0.
with open(p_xy_10m, 'w') as f:
    f.write("x,y\n")
    for a, b in zip(x, y):
        f.write(f"{a},{b}\n")

# (id, x) 10M for point-lookup workloads on indexed tables. id is
# a permutation of 0..N so every PK exists exactly once. We use a
# deterministic xorshift to avoid loading the SQL-engine indexer
# with sequential keys.
random.seed(0xC0FFEE)
ids = list(range(10_000_000))
random.shuffle(ids)
with open(p_id_10m, 'w') as f:
    f.write("id,x\n")
    for i, a in zip(ids, x):
        f.write(f"{i},{a}\n")
PY

shasum -a 256 "$DATA_X_1M" "$DATA_X_10M" "$DATA_Y_10M" "$DATA_XY_10M" "$DATA_ID_10M" | tee "$RAW/dataset_sha256.txt"

# -----------------------------------------------------------------------------
# [2] UltraSQL via cross_compare driver
# -----------------------------------------------------------------------------
echo "[2/8] UltraSQL — building cross_compare in release"
( cd "$REPO" && cargo build --release -p ultrasql-bench --bin cross_compare ) 2>&1 \
  | tail -5
CROSS="$REPO/target/release/cross_compare"

echo "[2/8] UltraSQL — running kernel workloads"
ULTRA_OUT="$RAW/ultrasql.jsonl"
: > "$ULTRA_OUT"

run_ultra() {
  local tag="$1"; shift
  echo "  ultrasql: $tag"
  echo -n "$tag	" >> "$ULTRA_OUT"
  "$CROSS" "$@" >> "$ULTRA_OUT" 2>>"$RAW/ultrasql.stderr.txt" || echo '{"error":true}' >> "$ULTRA_OUT"
}

run_ultra sum-1m       --workload sum   --data "$DATA_X_1M"
run_ultra sum-10m      --workload sum   --data "$DATA_X_10M"
run_ultra count-10m    --workload count --data "$DATA_X_10M"
run_ultra minmax-10m   --workload minmax --data "$DATA_X_10M"
run_ultra avg-10m      --workload avg   --data "$DATA_X_10M"
run_ultra range-10m    --workload range --data "$DATA_X_10M" --range-lo=-1000000000 --range-hi=1000000000
run_ultra filter-10m   --workload filter --data "$DATA_X_10M" --data2 "$DATA_Y_10M"
# Point lookup capped at 1M for UltraSQL (see methodology).
run_ultra point-1m     --workload point --data "$DATA_X_1M" --point-n 1000000 --point-batch 10000

# -----------------------------------------------------------------------------
# [3] DuckDB
# -----------------------------------------------------------------------------
echo "[3/8] DuckDB"
if command -v duckdb >/dev/null 2>&1; then
  DUCK_SQL="$WORK/duckdb_ext.sql"
  cat > "$DUCK_SQL" <<EOF
PRAGMA threads=1;
CREATE TABLE t1m (x BIGINT);
COPY t1m FROM '$DATA_X_1M' (HEADER, DELIMITER ',');
CREATE TABLE t10m (x BIGINT);
COPY t10m FROM '$DATA_X_10M' (HEADER, DELIMITER ',');
CREATE TABLE txy (x BIGINT, y BIGINT);
COPY txy FROM '$DATA_XY_10M' (HEADER, DELIMITER ',');
CREATE TABLE tid (id BIGINT PRIMARY KEY, x BIGINT);
COPY tid FROM '$DATA_ID_10M' (HEADER, DELIMITER ',');
ANALYZE;

-- warmup once per query, then enable JSON profiling.
SELECT SUM(x) FROM t1m;
SELECT SUM(x) FROM t10m;
SELECT COUNT(*) FROM t10m;
SELECT MIN(x), MAX(x) FROM t10m;
SELECT AVG(x) FROM t10m;
SELECT SUM(x) FROM txy WHERE y > 0;
SELECT COUNT(*) FROM t10m WHERE x BETWEEN -1000000000 AND 1000000000;
SELECT x FROM tid WHERE id = 12345;
SELECT x FROM t1m ORDER BY x LIMIT 10;
SELECT x FROM t10m ORDER BY x LIMIT 10;

PRAGMA enable_profiling='json';
EOF
  # For each workload we run 8 measured iterations and capture JSON
  # profiles. Each query type gets its own profile prefix.
  emit_block() {
    local tag="$1"; shift
    local sql="$1"; shift
    for i in 1 2 3 4 5 6 7 8; do
      echo "PRAGMA profiling_output='$WORK/duck_${tag}_${i}.json';" >> "$DUCK_SQL"
      echo "$sql;" >> "$DUCK_SQL"
    done
  }
  emit_block sum-1m     "SELECT SUM(x) FROM t1m"
  emit_block sum-10m    "SELECT SUM(x) FROM t10m"
  emit_block count-10m  "SELECT COUNT(*) FROM t10m"
  emit_block minmax-10m "SELECT MIN(x), MAX(x) FROM t10m"
  emit_block avg-10m    "SELECT AVG(x) FROM t10m"
  emit_block filter-10m "SELECT SUM(x) FROM txy WHERE y > 0"
  emit_block range-10m  "SELECT COUNT(*) FROM t10m WHERE x BETWEEN -1000000000 AND 1000000000"
  # Point lookup — pick 8 deterministic id values that exist.
  for i in 1 2 3 4 5 6 7 8; do
    ID=$((i * 1234567))
    echo "PRAGMA profiling_output='$WORK/duck_point-10m_${i}.json';" >> "$DUCK_SQL"
    echo "SELECT x FROM tid WHERE id = $ID;" >> "$DUCK_SQL"
  done
  emit_block topk-1m    "SELECT x FROM t1m ORDER BY x LIMIT 10"
  emit_block topk-10m   "SELECT x FROM t10m ORDER BY x LIMIT 10"

  duckdb < "$DUCK_SQL" > "$RAW/duckdb.out" 2>&1 || true
else
  echo "  duckdb not found, skipping" | tee "$RAW/duckdb.out"
fi

# -----------------------------------------------------------------------------
# [4] SQLite
# -----------------------------------------------------------------------------
echo "[4/8] SQLite (in :memory:)"
SQLITE_SQL="$WORK/sqlite_ext.sql"
cat > "$SQLITE_SQL" <<EOF
.timer on
PRAGMA journal_mode=MEMORY;
PRAGMA synchronous=OFF;
PRAGMA temp_store=MEMORY;
CREATE TABLE t1m (x INTEGER);
CREATE TABLE t10m (x INTEGER);
CREATE TABLE txy (x INTEGER, y INTEGER);
CREATE TABLE tid (id INTEGER PRIMARY KEY, x INTEGER);
.mode csv
.import --skip 1 $DATA_X_1M t1m
.import --skip 1 $DATA_X_10M t10m
.import --skip 1 $DATA_XY_10M txy
.import --skip 1 $DATA_ID_10M tid
ANALYZE;
EOF

emit_sqlite() {
  local sql="$1"; shift
  # 3 warmups + 8 measured = 11 runs.
  for _ in 1 2 3; do echo "$sql;" >> "$SQLITE_SQL"; done
  echo ".print --measure-$1--" >> "$SQLITE_SQL"
  shift
  for _ in 1 2 3 4 5 6 7 8; do echo "$sql;" >> "$SQLITE_SQL"; done
  echo ".print --end-measure--" >> "$SQLITE_SQL"
}
emit_sqlite "SELECT SUM(x) FROM t1m"                                                   sum-1m
emit_sqlite "SELECT SUM(x) FROM t10m"                                                  sum-10m
emit_sqlite "SELECT COUNT(*) FROM t10m"                                                count-10m
emit_sqlite "SELECT MIN(x), MAX(x) FROM t10m"                                          minmax-10m
emit_sqlite "SELECT AVG(x) FROM t10m"                                                  avg-10m
emit_sqlite "SELECT SUM(x) FROM txy WHERE y > 0"                                       filter-10m
emit_sqlite "SELECT COUNT(*) FROM t10m WHERE x BETWEEN -1000000000 AND 1000000000"     range-10m
emit_sqlite "SELECT x FROM tid WHERE id = 12345"                                       point-10m
emit_sqlite "SELECT x FROM t1m ORDER BY x LIMIT 10"                                    topk-1m
emit_sqlite "SELECT x FROM t10m ORDER BY x LIMIT 10"                                   topk-10m
sqlite3 :memory: < "$SQLITE_SQL" > "$RAW/sqlite3.out" 2>&1 || true

# -----------------------------------------------------------------------------
# [5] PostgreSQL
# -----------------------------------------------------------------------------
echo "[5/8] PostgreSQL"
if command -v psql >/dev/null 2>&1 && pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
  psql -d postgres -c "DROP DATABASE IF EXISTS ultracmp;" >/dev/null 2>&1
  psql -d postgres -c "CREATE DATABASE ultracmp;" >/dev/null 2>&1
  psql -d ultracmp <<EOF >/dev/null 2>&1
CREATE TABLE t1m (x BIGINT);
\\COPY t1m FROM '$DATA_X_1M' WITH (FORMAT csv, HEADER true);
CREATE TABLE t10m (x BIGINT);
\\COPY t10m FROM '$DATA_X_10M' WITH (FORMAT csv, HEADER true);
CREATE TABLE txy (x BIGINT, y BIGINT);
\\COPY txy FROM '$DATA_XY_10M' WITH (FORMAT csv, HEADER true);
CREATE TABLE tid (id BIGINT PRIMARY KEY, x BIGINT);
\\COPY tid FROM '$DATA_ID_10M' WITH (FORMAT csv, HEADER true);
ANALYZE;
EOF

  PG_SQL="$WORK/pg_ext.sql"
  printf '\\timing on\nSET max_parallel_workers_per_gather = 0;\n' > "$PG_SQL"
  emit_pg() {
    local tag="$1"; shift
    local sql="$1"
    # We use \echo (not SQL comments) because psql strips comments
    # from its output by default; \echo writes the marker straight
    # to stdout where the parser can find it later.
    echo "\\echo -- BEGIN $tag" >> "$PG_SQL"
    # 10 warmup runs to stabilize buffer/parse cache.
    for _ in $(seq 1 10); do echo "$sql;" >> "$PG_SQL"; done
    echo "\\echo -- MEASURE $tag" >> "$PG_SQL"
    for _ in $(seq 1 8); do echo "$sql;" >> "$PG_SQL"; done
    echo "\\echo -- END $tag" >> "$PG_SQL"
  }
  emit_pg sum-1m       "SELECT SUM(x) FROM t1m"
  emit_pg sum-10m      "SELECT SUM(x) FROM t10m"
  emit_pg count-10m    "SELECT COUNT(*) FROM t10m"
  emit_pg minmax-10m   "SELECT MIN(x), MAX(x) FROM t10m"
  emit_pg avg-10m      "SELECT AVG(x) FROM t10m"
  emit_pg filter-10m   "SELECT SUM(x) FROM txy WHERE y > 0"
  emit_pg range-10m    "SELECT COUNT(*) FROM t10m WHERE x BETWEEN -1000000000 AND 1000000000"
  emit_pg point-10m    "SELECT x FROM tid WHERE id = 12345"
  emit_pg topk-1m      "SELECT x FROM t1m ORDER BY x LIMIT 10"
  emit_pg topk-10m     "SELECT x FROM t10m ORDER BY x LIMIT 10"

  psql -d ultracmp -f "$PG_SQL" > "$RAW/postgres.out" 2>&1 || true
else
  echo "  postgres not running on localhost:5432, skipping" | tee "$RAW/postgres.out"
fi

# -----------------------------------------------------------------------------
# [6] ClickHouse
# -----------------------------------------------------------------------------
echo "[6/8] ClickHouse"
if [[ -x "$CH_BIN" ]]; then
  CH_SQL="$WORK/ch_ext.sql"
  cat > "$CH_SQL" <<EOF
CREATE TABLE t1m (x Int64) ENGINE = Memory;
INSERT INTO t1m SELECT * FROM file('$DATA_X_1M', 'CSVWithNames', 'x Int64');
CREATE TABLE t10m (x Int64) ENGINE = Memory;
INSERT INTO t10m SELECT * FROM file('$DATA_X_10M', 'CSVWithNames', 'x Int64');
CREATE TABLE txy (x Int64, y Int64) ENGINE = Memory;
INSERT INTO txy SELECT * FROM file('$DATA_XY_10M', 'CSVWithNames', 'x Int64, y Int64');
-- ClickHouse Memory engine has no native PK index; the point lookup
-- against ClickHouse will scan. We still emit it for honesty.
CREATE TABLE tid (id Int64, x Int64) ENGINE = Memory;
INSERT INTO tid SELECT * FROM file('$DATA_ID_10M', 'CSVWithNames', 'id Int64, x Int64');
EOF
  emit_ch() {
    local sql="$1"
    # 3 warmups + 8 measured = 11 runs.
    for _ in 1 2 3; do echo "$sql;" >> "$CH_SQL"; done
    echo "-- MEASURE" >> "$CH_SQL"
    for _ in 1 2 3 4 5 6 7 8; do echo "$sql;" >> "$CH_SQL"; done
  }
  emit_ch "SELECT SUM(x) FROM t1m"
  emit_ch "SELECT SUM(x) FROM t10m"
  emit_ch "SELECT COUNT(*) FROM t10m"
  emit_ch "SELECT min(x), max(x) FROM t10m"
  emit_ch "SELECT AVG(x) FROM t10m"
  emit_ch "SELECT SUM(x) FROM txy WHERE y > 0"
  emit_ch "SELECT COUNT(*) FROM t10m WHERE x BETWEEN -1000000000 AND 1000000000"
  emit_ch "SELECT x FROM tid WHERE id = 12345"
  emit_ch "SELECT x FROM t1m ORDER BY x LIMIT 10"
  emit_ch "SELECT x FROM t10m ORDER BY x LIMIT 10"

  "$CH_BIN" local --multiquery --queries-file "$CH_SQL" --format=JSON > "$RAW/clickhouse.out" 2>&1 || true
else
  echo "  clickhouse binary not found at $CH_BIN, skipping" | tee "$RAW/clickhouse.out"
fi

# -----------------------------------------------------------------------------
# [7] parse + medians
# -----------------------------------------------------------------------------
echo "[7/8] parsing medians and writing results.json"
RAW_DIR="$RAW" WORK_DIR="$WORK" HERE_DIR="$HERE" python3 - <<'PY'
import json, re, statistics, os, sys, glob

RAW_DIR = os.environ["RAW_DIR"]
WORK = os.environ["WORK_DIR"]
HERE = os.environ["HERE_DIR"]

def med(xs): return round(statistics.median(xs), 2)
def mn(xs):  return round(min(xs), 2)

# Workload labels mirror the run.sh tags. We declare a master list so
# downstream consumers iterate in a stable order.
WORKLOADS = [
    "sum-1m", "sum-10m", "count-10m", "minmax-10m", "avg-10m",
    "filter-10m", "range-10m", "point-10m", "topk-1m", "topk-10m",
]

results = {wl: {} for wl in WORKLOADS}

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
            if "error" in obj:
                continue
            # Map the harness tag back to the workload key.
            # UltraSQL's point row is point-1m; we put it under
            # point-10m's row in results.md with a footnote.
            wl_key = tag
            if tag == "point-1m":
                wl_key = "point-10m"  # logical slot for the comparison table
            iters = obj.get("iterations_us", [])
            entry = {
                "median_us": med(iters) if iters else None,
                "min_us":    mn(iters) if iters else None,
                "samples":   obj.get("samples"),
                "iterations_us": iters,
                "answer": obj.get("answer"),
                "n_rows": obj.get("n_rows"),
                "note": (
                    "vec kernels; kernel only, no SQL pipeline"
                    if tag != "point-1m" else
                    "BTree<i64> point lookup; tree capped at 1M keys "
                    "(v0.5 buffer pool refuses to evict dirty pages)"
                ),
            }
            if wl_key in results:
                results[wl_key]["UltraSQL (kernel)"] = entry
except Exception as e:
    print(f"UltraSQL parse error: {e}", file=sys.stderr)

# UltraSQL has no top-K kernel yet; mark explicit skip.
for tk in ("topk-1m", "topk-10m"):
    results[tk]["UltraSQL (kernel)"] = {
        "skipped": True,
        "reason": "no ORDER BY / Top-K kernel in vec yet (v0.5 scope)",
    }

# -------- DuckDB ----------
try:
    for wl in WORKLOADS:
        lat = []
        for i in range(1, 9):
            p = f"{WORK}/duck_{wl}_{i}.json"
            if not os.path.exists(p): continue
            try:
                with open(p) as f:
                    lat.append(json.load(f)["latency"] * 1e6)
            except Exception:
                pass
        if lat:
            results[wl]["DuckDB"] = {
                "median_us": med(lat),
                "min_us":    mn(lat),
                "samples":   len(lat),
                "iterations_us": [round(x, 2) for x in lat],
            }
        else:
            results[wl]["DuckDB"] = {"skipped": True, "reason": "no profile output"}
except Exception as e:
    print(f"DuckDB parse error: {e}", file=sys.stderr)

# -------- SQLite ----------
try:
    with open(f"{RAW_DIR}/sqlite3.out") as f:
        sqlite_txt = f.read()
    # The .timer output looks like:
    #   Run Time: real X user Y sys Z
    # We split the file into per-workload blocks via the
    # `--measure-<tag>--` and `--end-measure--` markers we injected.
    pat = re.compile(r"--measure-([\w-]+)--\n(.*?)--end-measure--", re.DOTALL)
    timer_pat = re.compile(r"Run Time: real (\S+) user (\S+) sys (\S+)")
    for m in pat.finditer(sqlite_txt):
        wl, body = m.group(1), m.group(2)
        if wl not in results: continue
        us = [float(t[1]) * 1e6 for t in timer_pat.findall(body)]
        if us:
            results[wl]["SQLite"] = {
                "median_us": med(us),
                "min_us":    mn(us),
                "samples":   len(us),
                "iterations_us": [round(x, 2) for x in us],
                "note": "user time, microsecond resolution; :memory: db",
            }
        else:
            results[wl]["SQLite"] = {"skipped": True, "reason": "no timer output in block"}
except Exception as e:
    print(f"SQLite parse error: {e}", file=sys.stderr)

# -------- PostgreSQL ----------
try:
    with open(f"{RAW_DIR}/postgres.out") as f:
        pg_txt = f.read()
    # Split into per-tag blocks via `-- MEASURE <tag>` markers (we
    # also emitted a `-- BEGIN` line). Each `Time: X ms` line is the
    # timing for the immediately preceding query.
    blocks = re.split(r"-- BEGIN (\S+)", pg_txt)
    # blocks[0] is preamble; thereafter alternates [tag, body, tag, body, ...]
    for i in range(1, len(blocks), 2):
        wl = blocks[i].strip()
        body = blocks[i + 1] if i + 1 < len(blocks) else ""
        # Take only the body after `-- MEASURE` (drop warmups).
        parts = body.split("-- MEASURE")
        measured = parts[1] if len(parts) >= 2 else ""
        times = [float(t) for t in re.findall(r"Time: ([\d.]+) ms", measured)]
        if times:
            # Defensive cap at 8 — if extra Time lines snuck in via
            # `\echo` itself getting timed, we still keep only the 8
            # measured runs.
            us = [round(t * 1000.0, 2) for t in times[:8]]
            results[wl]["PostgreSQL"] = {
                "median_us": med(us),
                "min_us":    mn(us),
                "samples":   len(us),
                "iterations_us": us,
                "note": "psql \\timing; same session; 10 warmup queries per workload",
            }
        else:
            results[wl]["PostgreSQL"] = {"skipped": True, "reason": "no measured timings"}
except Exception as e:
    print(f"PG parse error: {e}", file=sys.stderr)

# -------- ClickHouse ----------
try:
    with open(f"{RAW_DIR}/clickhouse.out") as f:
        ch_txt = f.read()
    # The CH JSON output writes one `{"meta":..., "data":..., "statistics":{"elapsed": X}}`
    # per query, concatenated. We split by `"elapsed"` and walk the
    # `statistics.elapsed` values. The script emits 11 statements per
    # workload (3 warmups + 8 measured), in the same order as WORKLOADS.
    elapsed = [float(x) for x in re.findall(r'"elapsed":\s*([\d.eE+-]+)', ch_txt)]
    # Each workload contributes 11 values; we keep the last 8.
    cursor = 0
    for wl in WORKLOADS:
        chunk = elapsed[cursor:cursor + 11]
        cursor += 11
        if len(chunk) >= 11:
            measured = chunk[3:11]
            us = [round(e * 1e6, 2) for e in measured]
            results[wl]["ClickHouse"] = {
                "median_us": med(us),
                "min_us":    mn(us),
                "samples":   len(us),
                "iterations_us": us,
                "note": "statistics.elapsed; Memory engine",
            }
        else:
            results[wl]["ClickHouse"] = {"skipped": True, "reason": "insufficient elapsed entries"}
except Exception as e:
    print(f"CH parse error: {e}", file=sys.stderr)

# Read dataset SHA-256 lines so we can record them in results.json.
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

doc = {
    "comparison": "extended cross-engine, 2026-05-12, Apple M4",
    "host": "Apple M4 Mac mini, 16 GiB, macOS 26.5",
    "workloads": WORKLOADS,
    "datasets_sha256": sha,
    "results": results,
}
with open(f"{HERE}/results.json", "w") as f:
    json.dump(doc, f, indent=2)

# ---- emit results.md ----
WORKLOAD_META = {
    "sum-1m":      ("`SELECT SUM(x) FROM t`",                                   "1,000,000 i64 rows"),
    "sum-10m":     ("`SELECT SUM(x) FROM t`",                                   "10,000,000 i64 rows"),
    "count-10m":   ("`SELECT COUNT(*) FROM t`",                                 "10,000,000 i64 rows"),
    "minmax-10m":  ("`SELECT MIN(x), MAX(x) FROM t`",                           "10,000,000 i64 rows"),
    "avg-10m":     ("`SELECT AVG(x) FROM t`",                                   "10,000,000 i64 rows"),
    "filter-10m":  ("`SELECT SUM(x) FROM t WHERE y > 0`",                       "10,000,000 (i64 x, i64 y)"),
    "range-10m":   ("`SELECT COUNT(*) FROM t WHERE x BETWEEN -1e9 AND 1e9`",    "10,000,000 i64 rows"),
    "point-10m":   ("`SELECT x FROM t WHERE id = ?`",                           "10,000,000 i64 indexed table (UltraSQL row uses 1,000,000-row B-tree; see note)"),
    "topk-1m":     ("`SELECT x FROM t ORDER BY x LIMIT 10`",                    "1,000,000 i64 rows"),
    "topk-10m":    ("`SELECT x FROM t ORDER BY x LIMIT 10`",                    "10,000,000 i64 rows"),
}

def fmt_us(v):
    if v is None: return "—"
    if v < 1: return f"{v:.3f} µs"
    if v < 1000: return f"{v:.2f} µs"
    if v < 1000000: return f"{v/1000:.2f} ms"
    return f"{v/1e6:.2f} s"

L = []
L.append("# Extended cross-engine comparison — 2026-05-12 (Apple M4)")
L.append("")
L.append("**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.")
L.append("macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.")
L.append("Reproduce via `bash run.sh` in this directory.")
L.append("")
L.append("**Engines.**")
L.append("")
L.append("| Engine            | Version                          | How measured                                    |")
L.append("| ----------------- | -------------------------------- | ----------------------------------------------- |")
L.append("| UltraSQL (kernel) | 0.0.1                            | `cross_compare` driver — vec kernels in isolation |")
L.append("| DuckDB            | 1.5.2 (Variegata) 8a5851971f     | `PRAGMA enable_profiling=json` `latency`, threads=1 |")
L.append("| SQLite            | 3.51.0 2025-06-12                | `.timer on` user time; `:memory:` db            |")
L.append("| PostgreSQL        | 14.22 (Homebrew)                 | `psql \\timing`; `max_parallel_workers_per_gather=0` |")
L.append("| ClickHouse        | 26.5.1.587 (official build)      | `statistics.elapsed`; `Memory` engine           |")
L.append("")
L.append("**Dataset sha256.**")
L.append("")
L.append("```")
for k, v in sha.items():
    L.append(f"{v}  {k}")
L.append("```")
L.append("")
L.append("> **Caveat at the top — applies to every table below.** The UltraSQL")
L.append("> row measures the relevant vec kernel **in isolation**. UltraSQL")
L.append("> has no SQL pipeline end-to-end yet (parser → plan → execute lands")
L.append("> at v0.5; see `ROADMAP.md`). Every other row measures the engine's")
L.append("> full SQL pipeline (parse, plan, dispatch, execute, materialize).")
L.append("> Treat the UltraSQL row as a **lower bound** on what the eventual")
L.append("> end-to-end query will achieve, not as a like-for-like result.")
L.append("")

for wl in WORKLOADS:
    query, dataset = WORKLOAD_META[wl]
    L.append(f"## `{wl}`")
    L.append("")
    L.append(f"**Workload.** {query}")
    L.append(f"**Dataset.** {dataset}")
    L.append("")
    rows = []
    for name, entry in results[wl].items():
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
    for name, entry in results[wl].items():
        if entry.get("skipped"): continue
        iters = entry.get("iterations_us", [])
        if not iters: continue
        s = ", ".join(f"{v:.2f}" if isinstance(v, float) and v >= 1 else f"{v}" for v in iters)
        L.append(f"- **{name}**: `{s}`")
    L.append("")

L.append("## What this is and isn't")
L.append("")
L.append("This is nine cross-engine micro-comparisons on a single host. It is")
L.append("not TPC-H, not TPC-DS, and not an endorsement of any engine. Each")
L.append("engine is measured by its own most-honest self-reported facility")
L.append("(see `methodology.md`). Raw stdout per engine is in `raw/`.")
L.append("")
L.append("The UltraSQL lines are in a different category: they measure SIMD")
L.append("kernels (and, for the point lookup, the v0.5 B+ tree's `lookup`)")
L.append("without any parser, planner, executor, or result-set")
L.append("materialization. They exist in these tables because the kernel is")
L.append("what the eventual UltraSQL end-to-end query will pay for the actual")
L.append("data plane. When v0.5 ships, this directory should be re-run and")
L.append("each UltraSQL row will be the engine's measured end-to-end time,")
L.append("not the kernel.")
L.append("")
L.append("Top-K rows are explicitly skipped on UltraSQL because no ORDER BY")
L.append("kernel exists in `vec` yet. Filling in this row is part of the v0.5")
L.append("scope; we do not fabricate a number from `Vec::sort` because that")
L.append("would not be representative of the eventual sort kernel.")

with open(f"{HERE}/results.md", "w") as f:
    f.write("\n".join(L) + "\n")

print("\nMedian summary (µs):")
print(f"  {'workload':<14} {'engine':<22} {'median':>10}  samples")
for wl in WORKLOADS:
    print(f"  ---- {wl} ----")
    rows = sorted(results[wl].items(), key=lambda kv: kv[1].get("median_us") or float("inf"))
    for name, entry in rows:
        if entry.get("skipped"):
            print(f"    {name:<22} {'skipped':>10}  ({entry.get('reason')})")
        else:
            m = entry.get("median_us")
            ms = m if isinstance(m, (int, float)) else "—"
            s = entry.get("samples", "—")
            print(f"    {name:<22} {ms:>10}  {s}")
PY

# -----------------------------------------------------------------------------
# [8] done
# -----------------------------------------------------------------------------
echo
echo "[8/8] done. raw outputs in $RAW; machine-readable results in $HERE/results.json"
