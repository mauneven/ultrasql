#!/usr/bin/env bash
# Cross-engine point-lookup comparison — Apple M4, 2026-05-12.
#
# Builds a 10,000,000-row (id BIGINT PRIMARY KEY, x BIGINT) table for
# each engine, opens a hot session, primes a prepared statement, and
# times 100,000 random point lookups across 3 runs. Reports median
# nanoseconds per probe and total wall time per run.
#
# Fairness rule (see methodology.md): every engine pays for its own
# session setup once (table load, index build, prepared statement,
# 10,000-probe warmup); we then measure only the probe loop. This
# corrects the prior `comparison-2026-05-12-m4-extended/point-10m`
# row's per-batch-vs-per-probe confusion (the prior row reported the
# per-iteration cost of a 10,000-probe batch and called it
# "per-probe" — off by a factor of 10,000).
#
# Usage:
#   bash run.sh
#
# Pre-reqs (verified on owner's M4 host 2026-05-12):
#   - duckdb on PATH                       (/opt/homebrew/bin/duckdb, 1.5.2)
#   - sqlite3 on PATH                      (/usr/bin/sqlite3, 3.51.0)
#   - PostgreSQL 14+ running on localhost  (brew services start postgresql@14)
#   - clickhouse binary at /tmp/ultracmp/clickhouse, or CH_BIN env var
#   - python3 on PATH with `psycopg` and `duckdb` (pip3 install --user psycopg duckdb)
#   - cargo on PATH; UltraSQL workspace at $REPO
#
# Caps (documented; not enforced by a hard timeout):
#   - Per-engine wall-clock target: 5 minutes. UltraSQL's 10M-key
#     BTree build exceeds this on v0.5 (see methodology); we run it
#     anyway and the JSON records `build_ns` so consumers see the
#     overage. ClickHouse's 100k-probe loop also exceeds the target
#     because its OLTP-unfit per-query overhead dwarfs the data plane;
#     we cap ClickHouse at `CH_RUNS=1` to keep the script's total
#     wall clock in the 20-minute budget.
#   - Total script wall clock budget: ~20 minutes on the reference host
#     (≈6.5 min UltraSQL build + ≈1 min probe + 30 s SQLite + 1 min
#     DuckDB + 1 min PostgreSQL + 10 min ClickHouse one-run).
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
DATA_ID_10M="$WORK/data_id_10m.csv"
PROBES_FILE="$WORK/probes_100k.txt"
CH_BIN="${CH_BIN:-/tmp/ultracmp/clickhouse}"
N_ROWS=10000000
PROBES=100000
WARMUP=10000
RUNS=3
# ClickHouse caps fewer runs because each 100k-probe pass through
# `clickhouse local` takes ~10 minutes (per-query startup dominates).
CH_RUNS="${CH_RUNS:-1}"

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
  echo "python3: $(python3 --version 2>/dev/null || echo NOT_FOUND)"
  echo "host:  $(uname -a)"
} > "$RAW/versions.txt"
cat "$RAW/versions.txt"

# Sanity-check python deps.
python3 -c "import sqlite3, duckdb, psycopg; print('python deps OK:', duckdb.__version__, psycopg.__version__)" \
  > "$RAW/python_deps.txt" 2>&1 || {
    echo "Missing python deps. Run: pip3 install --user psycopg duckdb" >&2
    exit 1
}

# -----------------------------------------------------------------------------
# [1] datasets
# -----------------------------------------------------------------------------
echo "[1/6] generating dataset (deterministic seed 0xDEADBEEF + permutation 0xC0FFEE)"

if [[ ! -s "$DATA_ID_10M" ]]; then
  python3 - "$DATA_ID_10M" <<'PY'
import sys, random

p_id_10m = sys.argv[1]
random.seed(0xDEADBEEF)
x = [random.randrange(-1<<31, 1<<31) for _ in range(10_000_000)]
random.seed(0xC0FFEE)
ids = list(range(10_000_000))
random.shuffle(ids)
with open(p_id_10m, 'w') as f:
    f.write("id,x\n")
    for i, a in zip(ids, x):
        f.write(f"{i},{a}\n")
PY
else
  echo "  reusing existing $DATA_ID_10M"
fi

# Probe set: same xorshift seed the UltraSQL Rust binary uses (so both
# halves of the comparison sample the same ids). Generate once and dump
# to a text file the SQL-engine drivers can stream in.
echo "[1/6] generating probe set (xorshift seed 0xCAFE_BABE_F00D_1234, N=$PROBES)"
PROBES="$PROBES" python3 - "$PROBES_FILE" <<'PY'
import os, sys
out = sys.argv[1]
n_rows = 10_000_000
probes = int(os.environ['PROBES'])
s = 0xCAFE_BABE_F00D_1234
MASK = (1 << 64) - 1
SIGN = 1 << 63
keys = []
for _ in range(probes):
    s ^= (s << 13) & MASK
    s ^= (s >> 7) & MASK
    s ^= (s << 17) & MASK
    # Reinterpret as signed i64 (two's complement) then rem_euclid.
    raw = s - (1 << 64) if (s & SIGN) else s
    keys.append(raw % n_rows)  # Python's % is Euclidean for positive divisor.
with open(out, 'w') as f:
    f.write('\n'.join(str(k) for k in keys))
    f.write('\n')
print(f"wrote {len(keys)} probe keys to {out}")
PY

shasum -a 256 "$DATA_ID_10M" "$PROBES_FILE" | tee "$RAW/dataset_sha256.txt"

# -----------------------------------------------------------------------------
# [2] UltraSQL — native Rust point_lookup binary
# -----------------------------------------------------------------------------
echo "[2/6] UltraSQL — building point_lookup in release"
( cd "$REPO" && cargo build --release -p ultrasql-bench --bin point_lookup ) 2>&1 \
  | tail -5
ULTRA_BIN="$REPO/target/release/point_lookup"

echo "[2/6] UltraSQL — building $N_ROWS-key BTree + running $PROBES-probe loop x $RUNS"
ULTRA_OUT="$RAW/ultrasql.jsonl"
: > "$ULTRA_OUT"
"$ULTRA_BIN" \
    --point-n "$N_ROWS" \
    --probes "$PROBES" \
    --warmup-probes "$WARMUP" \
    --runs "$RUNS" \
    > "$ULTRA_OUT" \
    2> "$RAW/ultrasql.stderr.txt" \
  || echo '{"error":true}' > "$ULTRA_OUT"
cat "$ULTRA_OUT"

# -----------------------------------------------------------------------------
# [3] SQLite via python3 sqlite3 (:memory:)
# -----------------------------------------------------------------------------
echo "[3/6] SQLite — :memory: db; prepared statement; $PROBES probes x $RUNS"
SQLITE_OUT="$RAW/sqlite.json"
DATA_ID_10M="$DATA_ID_10M" PROBES_FILE="$PROBES_FILE" \
WARMUP="$WARMUP" RUNS="$RUNS" \
python3 - > "$SQLITE_OUT" 2> "$RAW/sqlite.stderr.txt" <<'PY' || echo '{"error":true}' > "$SQLITE_OUT"
import json, os, sqlite3, statistics, time

data = os.environ['DATA_ID_10M']
probes_file = os.environ['PROBES_FILE']
warmup = int(os.environ['WARMUP'])
runs = int(os.environ['RUNS'])

with open(probes_file) as f:
    probes = [int(line) for line in f if line.strip()]
n_probes = len(probes)

t_setup0 = time.perf_counter_ns()
con = sqlite3.connect(':memory:')
con.execute('PRAGMA journal_mode=MEMORY')
con.execute('PRAGMA synchronous=OFF')
con.execute('PRAGMA temp_store=MEMORY')
con.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)')

# Load CSV. sqlite's `.import` is shell-only; do it via executemany.
def rows():
    with open(data) as f:
        next(f)  # header
        for line in f:
            i, x = line.split(',', 1)
            yield int(i), int(x)

con.executemany('INSERT INTO t (id, x) VALUES (?, ?)', rows())
con.execute('ANALYZE')
t_setup1 = time.perf_counter_ns()

# Prepared statement (cursor.execute caches the parsed statement; on
# repeated calls with the same SQL it reuses the prepared form).
cur = con.cursor()
sql = 'SELECT x FROM t WHERE id = ?'

# Warmup. We do a 10,000-probe pre-warm (cycling) to amortize the
# prepared-plan cache, then one full pass through every probe id so
# that every page the measurement run will touch is resident in the
# OS page cache. This eliminates first-run cold-cache outliers (one
# extra full pass costs ~1 s and is throwaway).
for k in (probes[i % n_probes] for i in range(warmup)):
    cur.execute(sql, (k,))
    cur.fetchone()
for k in probes:
    cur.execute(sql, (k,))
    cur.fetchone()

run_ns = []
for _ in range(runs):
    t0 = time.perf_counter_ns()
    for k in probes:
        cur.execute(sql, (k,))
        cur.fetchone()
    run_ns.append(time.perf_counter_ns() - t0)

per_probe = [t / n_probes for t in run_ns]
out = {
    'workload': 'point-10m-probes',
    'engine': 'SQLite',
    'n_rows': 10_000_000,
    'probes': n_probes,
    'runs': runs,
    'warmup_probes': warmup,
    'median_ns_per_probe': round(statistics.median(per_probe), 3),
    'min_ns_per_probe': round(min(per_probe), 3),
    'max_ns_per_probe': round(max(per_probe), 3),
    'total_wall_ns_median_run': int(statistics.median(run_ns)),
    'run_ns': run_ns,
    'per_probe_ns': [round(p, 3) for p in per_probe],
    'setup_ns': t_setup1 - t_setup0,
    'note': 'python3 sqlite3 :memory:; INTEGER PRIMARY KEY (rowid index)',
}
print(json.dumps(out))
PY
cat "$SQLITE_OUT"

# -----------------------------------------------------------------------------
# [4] DuckDB via python3 duckdb (in-process)
# -----------------------------------------------------------------------------
echo "[4/6] DuckDB — in-process; prepared statement; $PROBES probes x $RUNS"
DUCK_OUT="$RAW/duckdb.json"
DATA_ID_10M="$DATA_ID_10M" PROBES_FILE="$PROBES_FILE" \
WARMUP="$WARMUP" RUNS="$RUNS" \
python3 - > "$DUCK_OUT" 2> "$RAW/duckdb.stderr.txt" <<'PY' || echo '{"error":true}' > "$DUCK_OUT"
import json, os, statistics, time

import duckdb

data = os.environ['DATA_ID_10M']
probes_file = os.environ['PROBES_FILE']
warmup = int(os.environ['WARMUP'])
runs = int(os.environ['RUNS'])

with open(probes_file) as f:
    probes = [int(line) for line in f if line.strip()]
n_probes = len(probes)

t_setup0 = time.perf_counter_ns()
con = duckdb.connect(':memory:')
con.execute('PRAGMA threads=1')
con.execute('CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT)')
con.execute(f"COPY t FROM '{data}' (HEADER, DELIMITER ',')")
con.execute('ANALYZE')
t_setup1 = time.perf_counter_ns()

# duckdb-python's `con.execute(sql, params)` is the prepared-statement
# path — the connection caches the parsed plan and reuses it across
# repeat calls with the same SQL string.
def probe(k):
    return con.execute('SELECT x FROM t WHERE id = ?', [k]).fetchone()

# Warmup — 10k-probe pre-warm + one full pass to make every page warm.
for k in (probes[i % n_probes] for i in range(warmup)):
    probe(k)
for k in probes:
    probe(k)

run_ns = []
for _ in range(runs):
    t0 = time.perf_counter_ns()
    for k in probes:
        probe(k)
    run_ns.append(time.perf_counter_ns() - t0)

per_probe = [t / n_probes for t in run_ns]
out = {
    'workload': 'point-10m-probes',
    'engine': 'DuckDB',
    'n_rows': 10_000_000,
    'probes': n_probes,
    'runs': runs,
    'warmup_probes': warmup,
    'median_ns_per_probe': round(statistics.median(per_probe), 3),
    'min_ns_per_probe': round(min(per_probe), 3),
    'max_ns_per_probe': round(max(per_probe), 3),
    'total_wall_ns_median_run': int(statistics.median(run_ns)),
    'run_ns': run_ns,
    'per_probe_ns': [round(p, 3) for p in per_probe],
    'setup_ns': t_setup1 - t_setup0,
    'note': 'python3 duckdb 1.5.2 in-process; threads=1; PK auto-index',
}
print(json.dumps(out))
PY
cat "$DUCK_OUT"

# -----------------------------------------------------------------------------
# [5] PostgreSQL via python3 psycopg (3.x)
# -----------------------------------------------------------------------------
echo "[5/6] PostgreSQL — Unix socket; PREPARE/EXECUTE; $PROBES probes x $RUNS"
PG_OUT="$RAW/postgres.json"
if pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
  # Reset the db so the table exists with our shape.
  psql -d postgres -c "DROP DATABASE IF EXISTS ultracmp_point;" >/dev/null 2>&1
  psql -d postgres -c "CREATE DATABASE ultracmp_point;" >/dev/null 2>&1
  psql -d ultracmp_point <<EOF >/dev/null 2>&1
CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT);
\\COPY t FROM '$DATA_ID_10M' WITH (FORMAT csv, HEADER true);
ANALYZE;
EOF

  DATA_ID_10M="$DATA_ID_10M" PROBES_FILE="$PROBES_FILE" \
  WARMUP="$WARMUP" RUNS="$RUNS" \
  python3 - > "$PG_OUT" 2> "$RAW/postgres.stderr.txt" <<'PY' || echo '{"error":true}' > "$PG_OUT"
import json, os, statistics, time

import psycopg

probes_file = os.environ['PROBES_FILE']
warmup = int(os.environ['WARMUP'])
runs = int(os.environ['RUNS'])

with open(probes_file) as f:
    probes = [int(line) for line in f if line.strip()]
n_probes = len(probes)

t_setup0 = time.perf_counter_ns()
# Tell psycopg to use the server-side prepared-statement path.
con = psycopg.connect('dbname=ultracmp_point', prepare_threshold=0)
con.autocommit = True
con.execute('SET max_parallel_workers_per_gather = 0')
cur = con.cursor()
# Prime the prepared statement by running it once before measurement.
sql = 'SELECT x FROM t WHERE id = %s'
cur.execute(sql, (probes[0],), prepare=True)
cur.fetchone()
t_setup1 = time.perf_counter_ns()

# Warmup — 10k-probe pre-warm + one full pass to make every backend
# index page resident in shared_buffers (or the OS page cache).
for k in (probes[i % n_probes] for i in range(warmup)):
    cur.execute(sql, (k,), prepare=True)
    cur.fetchone()
for k in probes:
    cur.execute(sql, (k,), prepare=True)
    cur.fetchone()

run_ns = []
for _ in range(runs):
    t0 = time.perf_counter_ns()
    for k in probes:
        cur.execute(sql, (k,), prepare=True)
        cur.fetchone()
    run_ns.append(time.perf_counter_ns() - t0)

per_probe = [t / n_probes for t in run_ns]
out = {
    'workload': 'point-10m-probes',
    'engine': 'PostgreSQL',
    'n_rows': 10_000_000,
    'probes': n_probes,
    'runs': runs,
    'warmup_probes': warmup,
    'median_ns_per_probe': round(statistics.median(per_probe), 3),
    'min_ns_per_probe': round(min(per_probe), 3),
    'max_ns_per_probe': round(max(per_probe), 3),
    'total_wall_ns_median_run': int(statistics.median(run_ns)),
    'run_ns': run_ns,
    'per_probe_ns': [round(p, 3) for p in per_probe],
    'setup_ns': t_setup1 - t_setup0,
    'note': 'python3 psycopg 3; same connection; server-side prepared statement; Unix socket',
}
print(json.dumps(out))
PY
  cat "$PG_OUT"
else
  echo "  postgres not running on localhost:5432, skipping" | tee "$PG_OUT"
  echo '{"skipped":true,"reason":"postgres not running"}' > "$PG_OUT"
fi

# -----------------------------------------------------------------------------
# [6] ClickHouse via `clickhouse local` MergeTree
# -----------------------------------------------------------------------------
# ClickHouse is not designed for OLTP point lookups. Each probe in
# `clickhouse local` pays the full parse + plan + part-open cost; on
# the reference host that's roughly 6 ms per probe at 10M rows. To
# stay within the 20-minute total budget we run `CH_RUNS=$CH_RUNS`
# measured runs (default 1) instead of 3, and document the cap.
echo "[6/6] ClickHouse — clickhouse local; MergeTree PK on id; $PROBES probes x $CH_RUNS (capped)"
CH_OUT="$RAW/clickhouse.json"
if [[ -x "$CH_BIN" ]]; then
  CH_DB_DIR="$WORK/ch_pointlookup_db"
  rm -rf "$CH_DB_DIR"
  mkdir -p "$CH_DB_DIR"

  # Setup SQL.
  cat > "$WORK/ch_setup.sql" <<EOF
CREATE TABLE t (id Int64, x Int64) ENGINE = MergeTree() ORDER BY id PRIMARY KEY id;
INSERT INTO t SELECT * FROM file('$DATA_ID_10M', 'CSVWithNames', 'id Int64, x Int64');
OPTIMIZE TABLE t FINAL;
EOF

  # Time setup.
  ch_setup_t0=$(python3 -c 'import time; print(time.perf_counter_ns())')
  "$CH_BIN" local --path "$CH_DB_DIR" --queries-file "$WORK/ch_setup.sql" > "$RAW/ch_setup.txt" 2>&1 || true
  ch_setup_t1=$(python3 -c 'import time; print(time.perf_counter_ns())')
  setup_ns=$((ch_setup_t1 - ch_setup_t0))

  # Generate the warmup and per-run scripts. Each is a 1-statement-
  # per-probe script with literal-substituted ids (clickhouse local
  # does not expose bound-parameter EXECUTE from a queries-file).
  PROBES_FILE="$PROBES_FILE" CH_WARMUP="$WARMUP" python3 - "$WORK/ch_warmup.sql" "$WORK/ch_run.sql" <<'PY'
import os, sys
warmup_out, run_out = sys.argv[1:3]
probes_file = os.environ['PROBES_FILE']
warmup = int(os.environ['CH_WARMUP'])
keys = open(probes_file).read().split()
with open(warmup_out, 'w') as f:
    for k in keys[:warmup]:
        f.write(f'SELECT x FROM t WHERE id = {k} FORMAT Null;\n')
with open(run_out, 'w') as f:
    for k in keys:
        f.write(f'SELECT x FROM t WHERE id = {k} FORMAT Null;\n')
PY

  # Warmup pass.
  "$CH_BIN" local --path "$CH_DB_DIR" --queries-file "$WORK/ch_warmup.sql" \
    > "$RAW/ch_warmup.txt" 2>&1 || true

  run_ns_list=()
  status="ok"
  for r in $(seq 1 "$CH_RUNS"); do
    echo "  ClickHouse: run $r/$CH_RUNS — $PROBES probes (expect ~10 min)"
    t0=$(python3 -c 'import time; print(time.perf_counter_ns())')
    "$CH_BIN" local --path "$CH_DB_DIR" --queries-file "$WORK/ch_run.sql" \
      > "$RAW/ch_run_${r}.txt" 2>&1 || status="error"
    t1=$(python3 -c 'import time; print(time.perf_counter_ns())')
    run_ns_list+=("$((t1 - t0))")
  done

  RUN_NS_CSV=$(IFS=,; echo "${run_ns_list[*]}")
  CH_PROBES="$PROBES" CH_SETUP_NS="$setup_ns" \
  CH_STATUS="$status" CH_RUN_NS_CSV="$RUN_NS_CSV" CH_WARMUP="$WARMUP" \
  python3 - > "$CH_OUT" <<'PY'
import json, os, statistics
ns_csv = os.environ['CH_RUN_NS_CSV']
run_ns = [int(x) for x in ns_csv.split(',') if x.strip()]
probes = int(os.environ['CH_PROBES'])
status = os.environ['CH_STATUS']
setup  = int(os.environ['CH_SETUP_NS'])

if not run_ns:
    print(json.dumps({'skipped': True, 'reason': 'clickhouse produced no measured runs'}))
else:
    per_probe = [t / probes for t in run_ns]
    out = {
        'workload': 'point-10m-probes',
        'engine': 'ClickHouse',
        'n_rows': 10_000_000,
        'probes': probes,
        'runs': len(run_ns),
        'warmup_probes': int(os.environ['CH_WARMUP']),
        'median_ns_per_probe': round(statistics.median(per_probe), 3),
        'min_ns_per_probe': round(min(per_probe), 3),
        'max_ns_per_probe': round(max(per_probe), 3),
        'total_wall_ns_median_run': int(statistics.median(run_ns)),
        'run_ns': run_ns,
        'per_probe_ns': [round(p, 3) for p in per_probe],
        'setup_ns': setup,
        'status': status,
        'note': 'clickhouse local; MergeTree PK on id; 100k literal SELECTs per run; capped at 1 run (per-engine target overage; OLTP unfit, see methodology.md)',
    }
    print(json.dumps(out))
PY
  cat "$CH_OUT"
else
  echo "  clickhouse not found at $CH_BIN, skipping" | tee "$CH_OUT"
  echo '{"skipped":true,"reason":"clickhouse binary missing"}' > "$CH_OUT"
fi

# -----------------------------------------------------------------------------
# parse + emit results.json + results.md
# -----------------------------------------------------------------------------
echo "parsing per-engine JSON and writing results.json + results.md"
RAW_DIR="$RAW" HERE_DIR="$HERE" python3 - <<'PY'
import json, os, glob

RAW = os.environ['RAW_DIR']
HERE = os.environ['HERE_DIR']

ENGINES = [
    ('UltraSQL (kernel, BTree<i64>)', f'{RAW}/ultrasql.jsonl'),
    ('SQLite',                         f'{RAW}/sqlite.json'),
    ('DuckDB',                         f'{RAW}/duckdb.json'),
    ('PostgreSQL',                     f'{RAW}/postgres.json'),
    ('ClickHouse',                     f'{RAW}/clickhouse.json'),
]

def load(path):
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    return json.loads(line)
    except Exception as e:
        return {'skipped': True, 'reason': f'parse error: {e}'}
    return {'skipped': True, 'reason': f'no rows in {path}'}

results = {'point-10m-probes': {}}
for name, path in ENGINES:
    obj = load(path)
    if obj is None or obj.get('error'):
        results['point-10m-probes'][name] = {'skipped': True, 'reason': 'engine reported error'}
        continue
    if obj.get('skipped'):
        results['point-10m-probes'][name] = obj
        continue
    # Project only the fields the comparison table needs; preserve the
    # full record under `raw` for the curious.
    results['point-10m-probes'][name] = {
        'median_ns_per_probe':      obj.get('median_ns_per_probe'),
        'min_ns_per_probe':         obj.get('min_ns_per_probe'),
        'max_ns_per_probe':         obj.get('max_ns_per_probe'),
        'total_wall_ns_median_run': obj.get('total_wall_ns_median_run'),
        'probes':                   obj.get('probes'),
        'runs':                     obj.get('runs'),
        'note':                     obj.get('note'),
        'raw':                      obj,
    }

# dataset shas
sha = {}
try:
    with open(f'{RAW}/dataset_sha256.txt') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            h, p = line.split('  ', 1)
            sha[os.path.basename(p)] = h
except Exception:
    pass

doc = {
    'comparison': 'point-lookup, cross-engine, hot-session, 2026-05-12 (Apple M4)',
    'host': 'Apple M4 Mac mini, 16 GiB, macOS 26.5',
    'workload': 'point-10m-probes',
    'description': '100,000 random point lookups by id against a 10,000,000-row (id BIGINT PRIMARY KEY, x BIGINT) table in a single hot session.',
    'datasets_sha256': sha,
    'methodology_md': 'methodology.md',
    'prior_comparison_path': 'benchmarks/results/comparison-2026-05-12-m4-extended/results.json#point-10m',
    'results': results,
}
with open(f'{HERE}/results.json', 'w') as f:
    json.dump(doc, f, indent=2)

# ---- results.md ----
def fmt_ns_per_probe(v):
    if v is None: return '—'
    if v < 1_000:         return f'{v:.1f} ns'
    if v < 1_000_000:     return f'{v/1_000:.2f} µs'
    if v < 1_000_000_000: return f'{v/1_000_000:.2f} ms'
    return f'{v/1e9:.2f} s'

def fmt_wall_ns(v):
    if v is None: return '—'
    if v < 1_000_000:     return f'{v/1_000:.2f} µs'
    if v < 1_000_000_000: return f'{v/1_000_000:.2f} ms'
    return f'{v/1e9:.2f} s'

L = []
L.append('# Point-lookup cross-engine comparison — 2026-05-12 (Apple M4)')
L.append('')
L.append('**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.')
L.append('macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.')
L.append('Reproduce via `bash run.sh` in this directory.')
L.append('')
L.append('**Workload.** `SELECT x FROM t WHERE id = ?` over a 10,000,000-row')
L.append('`(id BIGINT PRIMARY KEY, x BIGINT)` table. Same deterministic-seed')
L.append('CSV and same 100,000-probe set across every engine.')
L.append('')
L.append('**Methodology (fair shape).** Each engine builds the table, opens a')
L.append('hot session, prepares the statement, runs 10,000 throwaway warmup')
L.append('probes, then runs measurement runs of **100,000 probes** (3 runs for')
L.append('every engine except ClickHouse, which is capped at 1 run; see')
L.append('`methodology.md`). We report the median nanoseconds-per-probe across')
L.append('the runs and the total wall time of the median run. Setup and warmup')
L.append('are explicitly excluded from the timed region.')
L.append('')
L.append('**Why this comparison exists.** The prior')
L.append('`comparison-2026-05-12-m4-extended/point-10m` row reported the')
L.append('per-iteration wall time of a 10,000-probe **batch** (~6.78 ms) and')
L.append('treated it as per-probe in some prose. That was off by a factor of')
L.append('10,000. This directory measures probes-only, in a hot session,')
L.append('across all five engines — the methodology every other row of the')
L.append('extended comparison already uses for non-batched workloads.')
L.append('')

L.append('**Dataset sha256.**')
L.append('')
L.append('```')
for k, v in sha.items():
    L.append(f'{v}  {k}')
L.append('```')
L.append('')

L.append('## `point-10m-probes`')
L.append('')
L.append('| Rank | Engine                              | Median per probe   | Total wall (100k probes) | Runs | Notes |')
L.append('| ---- | ----------------------------------- | -----------------: | -----------------------: | ---: | ----- |')

rows = []
for engine, entry in results['point-10m-probes'].items():
    if entry.get('skipped'):
        rows.append((float('inf'), engine, 'skipped', '—', 0, entry.get('reason', '—')))
        continue
    m = entry.get('median_ns_per_probe')
    if m is None:
        rows.append((float('inf'), engine, '—', '—', 0, '—'))
        continue
    rows.append((
        m,
        engine,
        fmt_ns_per_probe(m),
        fmt_wall_ns(entry.get('total_wall_ns_median_run')),
        entry.get('runs', 0) or 0,
        entry.get('note', ''),
    ))
rows.sort(key=lambda r: r[0])
rank = 0
for medv, name, mstr, wstr, runs_n, note in rows:
    if mstr == 'skipped':
        L.append(f'| —    | {name:<35} |     skipped      |              —          |    — | {note} |')
    else:
        rank += 1
        L.append(f'| {rank}    | {name:<35} | {mstr:>17} | {wstr:>23} | {runs_n:>3} | {note} |')

L.append('')
L.append('### Per-run distribution')
L.append('')
L.append('Each engine ran N runs of 100,000 probes (3 for everyone except')
L.append('ClickHouse, which is capped at 1). The list below shows')
L.append('`total_wall_ns_per_run / 100,000` for each run.')
L.append('')
for engine, entry in results['point-10m-probes'].items():
    if entry.get('skipped'): continue
    pp = entry.get('raw', {}).get('per_probe_ns', [])
    if not pp: continue
    parts = []
    for v in pp:
        if v < 1000:        parts.append(f'{v:.2f} ns')
        elif v < 1_000_000: parts.append(f'{v/1000:.2f} µs')
        else:               parts.append(f'{v/1e6:.2f} ms')
    L.append(f'- **{engine}**: `[{", ".join(parts)}]`')
L.append('')

L.append('## Reading this table')
L.append('')
L.append('Every engine paid for its own session setup once (table load, index')
L.append('build, prepared statement, 10,000-probe warmup). Only the probe')
L.append('loop is timed. The UltraSQL row uses the v0.5 `BTree<i64>` directly')
L.append('via the native Rust API — there is no SQL pipeline yet, and there')
L.append('is no client/IPC overhead in the timed region; treat it as a lower')
L.append('bound on what the eventual end-to-end query will achieve. SQLite,')
L.append('DuckDB and PostgreSQL are measured through their Python bindings;')
L.append('the constant per-call binding overhead is included (a few hundred')
L.append('ns) and is the same shape a real Python or psycopg client would')
L.append('pay. ClickHouse is measured through `clickhouse local`, which pays')
L.append('full per-query startup cost on each lookup — ClickHouse is not')
L.append('designed for OLTP point-lookup workloads and we document the')
L.append('result as a loss for that engine.')

with open(f'{HERE}/results.md', 'w') as f:
    f.write('\n'.join(L) + '\n')

print('done. results.json and results.md written.')
PY

echo
echo "done. raw outputs in $RAW; machine-readable results in $HERE/results.json"
