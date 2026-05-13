#!/usr/bin/env bash
# Cross-engine fill-in comparison — Apple M4, 2026-05-12.
#
# Same methodology as ../comparison-2026-05-12-m4-extended/; this
# directory measures four additional workload-size combinations to
# round out the README headline set. See methodology.md for the short
# version and ../comparison-2026-05-12-m4-extended/methodology.md for
# the full treatment.
#
# Workloads (all single-thread, hot cache, 8 measured iterations after
# warmup, median reported):
#
#   sum-256k    — SUM(x)   over   256,000 i64 rows
#   sum-4m      — SUM(x)   over 4,000,000 i64 rows
#   count-1m    — COUNT(*) over 1,000,000 i64 rows
#   avg-1m      — AVG(x)   over 1,000,000 i64 rows
#
# Usage:
#   bash run.sh
#
# Pre-reqs (same as the parent comparison):
#   - duckdb on PATH
#   - sqlite3 on PATH
#   - PostgreSQL running on localhost
#   - clickhouse binary at /tmp/ultracmp/clickhouse, or CH_BIN env var
#   - python3 on PATH
#   - cargo on PATH; UltraSQL workspace at $REPO
#
# Idempotent: drops and re-creates per-engine state each run.

set -euo pipefail

if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
RAW="$HERE/raw"
WORK="/tmp/ultracmp-fillin"
REPO="$(cd "$HERE/../../.." && pwd)"
DATA_256K="$WORK/data_x_256k.csv"
DATA_1M="$WORK/data_x_1m.csv"
DATA_4M="$WORK/data_x_4m.csv"
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
# [1] datasets — same deterministic seed pattern as the parent comparison.
#     We materialize one 4M-row x stream and slice it for the smaller
#     sizes so cross-size comparisons share data.
# -----------------------------------------------------------------------------
echo "[1/8] generating datasets (deterministic seed 0xDEADBEEF)"

python3 - "$DATA_256K" "$DATA_1M" "$DATA_4M" <<'PY'
import sys, random

p_256k, p_1m, p_4m = sys.argv[1:4]

random.seed(0xDEADBEEF)
# 4M x values; the 1M and 256k datasets are prefixes of this stream
# so smaller-size workloads see a deterministic subset of the bigger
# one (matches how the parent comparison reuses the first 1M of the
# 10M stream).
x = [random.randrange(-1<<31, 1<<31) for _ in range(4_000_000)]

with open(p_256k, 'w') as f:
    f.write("x\n")
    f.writelines(f"{v}\n" for v in x[:256_000])

with open(p_1m, 'w') as f:
    f.write("x\n")
    f.writelines(f"{v}\n" for v in x[:1_000_000])

with open(p_4m, 'w') as f:
    f.write("x\n")
    f.writelines(f"{v}\n" for v in x)
PY

shasum -a 256 "$DATA_256K" "$DATA_1M" "$DATA_4M" | tee "$RAW/dataset_sha256.txt"

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
  printf '%s\t' "$tag" >> "$ULTRA_OUT"
  "$CROSS" "$@" >> "$ULTRA_OUT" 2>>"$RAW/ultrasql.stderr.txt" || echo '{"error":true}' >> "$ULTRA_OUT"
}

run_ultra sum-256k  --workload sum   --data "$DATA_256K"
run_ultra sum-4m    --workload sum   --data "$DATA_4M"
run_ultra count-1m  --workload count --data "$DATA_1M"
run_ultra avg-1m    --workload avg   --data "$DATA_1M"

# -----------------------------------------------------------------------------
# [3] DuckDB
# -----------------------------------------------------------------------------
echo "[3/8] DuckDB"
if command -v duckdb >/dev/null 2>&1; then
  DUCK_SQL="$WORK/duckdb_fillin.sql"
  cat > "$DUCK_SQL" <<EOF
PRAGMA threads=1;
CREATE TABLE t256k (x BIGINT);
COPY t256k FROM '$DATA_256K' (HEADER, DELIMITER ',');
CREATE TABLE t1m   (x BIGINT);
COPY t1m   FROM '$DATA_1M'   (HEADER, DELIMITER ',');
CREATE TABLE t4m   (x BIGINT);
COPY t4m   FROM '$DATA_4M'   (HEADER, DELIMITER ',');
ANALYZE;

-- One warmup per query type.
SELECT SUM(x)    FROM t256k;
SELECT SUM(x)    FROM t4m;
SELECT COUNT(*)  FROM t1m;
SELECT AVG(x)    FROM t1m;

PRAGMA enable_profiling='json';
EOF
  emit_block() {
    local tag="$1"; shift
    local sql="$1"; shift
    for i in 1 2 3 4 5 6 7 8; do
      echo "PRAGMA profiling_output='$WORK/duck_${tag}_${i}.json';" >> "$DUCK_SQL"
      echo "$sql;" >> "$DUCK_SQL"
    done
  }
  emit_block sum-256k "SELECT SUM(x) FROM t256k"
  emit_block sum-4m   "SELECT SUM(x) FROM t4m"
  emit_block count-1m "SELECT COUNT(*) FROM t1m"
  emit_block avg-1m   "SELECT AVG(x) FROM t1m"

  duckdb < "$DUCK_SQL" > "$RAW/duckdb.out" 2>&1 || true
else
  echo "  duckdb not found, skipping" | tee "$RAW/duckdb.out"
fi

# -----------------------------------------------------------------------------
# [4] SQLite
# -----------------------------------------------------------------------------
echo "[4/8] SQLite (in :memory:)"
SQLITE_SQL="$WORK/sqlite_fillin.sql"
cat > "$SQLITE_SQL" <<EOF
.timer on
PRAGMA journal_mode=MEMORY;
PRAGMA synchronous=OFF;
PRAGMA temp_store=MEMORY;
CREATE TABLE t256k (x INTEGER);
CREATE TABLE t1m   (x INTEGER);
CREATE TABLE t4m   (x INTEGER);
.mode csv
.import --skip 1 $DATA_256K t256k
.import --skip 1 $DATA_1M   t1m
.import --skip 1 $DATA_4M   t4m
ANALYZE;
EOF

emit_sqlite() {
  local sql="$1"; shift
  local tag="$1"; shift
  # 3 warmups + 8 measured.
  for _ in 1 2 3; do echo "$sql;" >> "$SQLITE_SQL"; done
  echo ".print --measure-$tag--" >> "$SQLITE_SQL"
  for _ in 1 2 3 4 5 6 7 8; do echo "$sql;" >> "$SQLITE_SQL"; done
  echo ".print --end-measure--" >> "$SQLITE_SQL"
}
emit_sqlite "SELECT SUM(x) FROM t256k"  sum-256k
emit_sqlite "SELECT SUM(x) FROM t4m"    sum-4m
emit_sqlite "SELECT COUNT(*) FROM t1m"  count-1m
emit_sqlite "SELECT AVG(x) FROM t1m"    avg-1m
sqlite3 :memory: < "$SQLITE_SQL" > "$RAW/sqlite3.out" 2>&1 || true

# -----------------------------------------------------------------------------
# [5] PostgreSQL
# -----------------------------------------------------------------------------
echo "[5/8] PostgreSQL"
if command -v psql >/dev/null 2>&1 && pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
  psql -d postgres -c "DROP DATABASE IF EXISTS ultracmp_fillin;" >/dev/null 2>&1
  psql -d postgres -c "CREATE DATABASE ultracmp_fillin;" >/dev/null 2>&1
  psql -d ultracmp_fillin <<EOF >/dev/null 2>&1
CREATE TABLE t256k (x BIGINT);
\\COPY t256k FROM '$DATA_256K' WITH (FORMAT csv, HEADER true);
CREATE TABLE t1m   (x BIGINT);
\\COPY t1m   FROM '$DATA_1M'   WITH (FORMAT csv, HEADER true);
CREATE TABLE t4m   (x BIGINT);
\\COPY t4m   FROM '$DATA_4M'   WITH (FORMAT csv, HEADER true);
ANALYZE;
EOF

  PG_SQL="$WORK/pg_fillin.sql"
  printf '\\timing on\nSET max_parallel_workers_per_gather = 0;\n' > "$PG_SQL"
  emit_pg() {
    local tag="$1"; shift
    local sql="$1"
    echo "\\echo -- BEGIN $tag" >> "$PG_SQL"
    for _ in $(seq 1 10); do echo "$sql;" >> "$PG_SQL"; done
    echo "\\echo -- MEASURE $tag" >> "$PG_SQL"
    for _ in $(seq 1 8); do echo "$sql;" >> "$PG_SQL"; done
    echo "\\echo -- END $tag" >> "$PG_SQL"
  }
  emit_pg sum-256k "SELECT SUM(x) FROM t256k"
  emit_pg sum-4m   "SELECT SUM(x) FROM t4m"
  emit_pg count-1m "SELECT COUNT(*) FROM t1m"
  emit_pg avg-1m   "SELECT AVG(x) FROM t1m"

  psql -d ultracmp_fillin -f "$PG_SQL" > "$RAW/postgres.out" 2>&1 || true
else
  echo "  postgres not running on localhost:5432, skipping" | tee "$RAW/postgres.out"
fi

# -----------------------------------------------------------------------------
# [6] ClickHouse
# -----------------------------------------------------------------------------
echo "[6/8] ClickHouse"
if [[ -x "$CH_BIN" ]]; then
  CH_SQL="$WORK/ch_fillin.sql"
  cat > "$CH_SQL" <<EOF
CREATE TABLE t256k (x Int64) ENGINE = Memory;
INSERT INTO t256k SELECT * FROM file('$DATA_256K', 'CSVWithNames', 'x Int64');
CREATE TABLE t1m   (x Int64) ENGINE = Memory;
INSERT INTO t1m   SELECT * FROM file('$DATA_1M',   'CSVWithNames', 'x Int64');
CREATE TABLE t4m   (x Int64) ENGINE = Memory;
INSERT INTO t4m   SELECT * FROM file('$DATA_4M',   'CSVWithNames', 'x Int64');
EOF
  emit_ch() {
    local sql="$1"
    # 3 warmups + 8 measured.
    for _ in 1 2 3; do echo "$sql;" >> "$CH_SQL"; done
    echo "-- MEASURE" >> "$CH_SQL"
    for _ in 1 2 3 4 5 6 7 8; do echo "$sql;" >> "$CH_SQL"; done
  }
  # Order MUST match WORKLOADS in the parser block.
  emit_ch "SELECT SUM(x) FROM t256k"
  emit_ch "SELECT SUM(x) FROM t4m"
  emit_ch "SELECT COUNT(*) FROM t1m"
  emit_ch "SELECT AVG(x) FROM t1m"

  "$CH_BIN" local --multiquery --queries-file "$CH_SQL" --format=JSON > "$RAW/clickhouse.out" 2>&1 || true
else
  echo "  clickhouse binary not found at $CH_BIN, skipping" | tee "$RAW/clickhouse.out"
fi

# -----------------------------------------------------------------------------
# [7] parse + medians
# -----------------------------------------------------------------------------
echo "[7/8] parsing medians and writing results.json"
RAW_DIR="$RAW" WORK_DIR="$WORK" HERE_DIR="$HERE" python3 - <<'PY'
import json, re, statistics, os, sys

RAW_DIR = os.environ["RAW_DIR"]
WORK    = os.environ["WORK_DIR"]
HERE    = os.environ["HERE_DIR"]

def med(xs): return round(statistics.median(xs), 2)
def mn(xs):  return round(min(xs), 2)

WORKLOADS = ["sum-256k", "sum-4m", "count-1m", "avg-1m"]

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
            if "error" in obj or tag not in results:
                continue
            iters = obj.get("iterations_us", [])
            results[tag]["UltraSQL (kernel)"] = {
                "median_us": med(iters) if iters else None,
                "min_us":    mn(iters) if iters else None,
                "samples":   obj.get("samples"),
                "iterations_us": iters,
                "answer":  obj.get("answer"),
                "n_rows":  obj.get("n_rows"),
                "note":   "vec kernels; kernel only, no SQL pipeline",
            }
except Exception as e:
    print(f"UltraSQL parse error: {e}", file=sys.stderr)

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
    blocks = re.split(r"-- BEGIN (\S+)", pg_txt)
    for i in range(1, len(blocks), 2):
        wl = blocks[i].strip()
        body = blocks[i + 1] if i + 1 < len(blocks) else ""
        parts = body.split("-- MEASURE")
        measured = parts[1] if len(parts) >= 2 else ""
        times = [float(t) for t in re.findall(r"Time: ([\d.]+) ms", measured)]
        if times and wl in results:
            us = [round(t * 1000.0, 2) for t in times[:8]]
            results[wl]["PostgreSQL"] = {
                "median_us": med(us),
                "min_us":    mn(us),
                "samples":   len(us),
                "iterations_us": us,
                "note": "psql \\timing; same session; 10 warmup queries per workload",
            }
        elif wl in results:
            results[wl]["PostgreSQL"] = {"skipped": True, "reason": "no measured timings"}
except Exception as e:
    print(f"PG parse error: {e}", file=sys.stderr)

# -------- ClickHouse ----------
try:
    with open(f"{RAW_DIR}/clickhouse.out") as f:
        ch_txt = f.read()
    elapsed = [float(x) for x in re.findall(r'"elapsed":\s*([\d.eE+-]+)', ch_txt)]
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

doc = {
    "comparison": "fill-in cross-engine, 2026-05-12, Apple M4",
    "host": "Apple M4 Mac mini, 16 GiB, macOS 26.5",
    "workloads": WORKLOADS,
    "datasets_sha256": sha,
    "results": results,
}
with open(f"{HERE}/results.json", "w") as f:
    json.dump(doc, f, indent=2)

# ---- emit results.md ----
WORKLOAD_META = {
    "sum-256k":  ("`SELECT SUM(x) FROM t`",                                   "256,000 i64 rows"),
    "sum-4m":    ("`SELECT SUM(x) FROM t`",                                   "4,000,000 i64 rows"),
    "count-1m":  ("`SELECT COUNT(*) FROM t`",                                 "1,000,000 i64 rows"),
    "avg-1m":    ("`SELECT AVG(x) FROM t`",                                   "1,000,000 i64 rows"),
}

def fmt_us(v):
    if v is None: return "—"
    if v < 1: return f"{v:.3f} µs"
    if v < 1000: return f"{v:.2f} µs"
    if v < 1000000: return f"{v/1000:.2f} ms"
    return f"{v/1e6:.2f} s"

L = []
L.append("# Fill-in cross-engine comparison — 2026-05-12 (Apple M4)")
L.append("")
L.append("**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.")
L.append("macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.")
L.append("Reproduce via `bash run.sh` in this directory.")
L.append("")
L.append("Identical methodology to `../comparison-2026-05-12-m4-extended/`;")
L.append("see that directory's `methodology.md` for the full treatment. The")
L.append("workloads here measure four additional size points to round out")
L.append("the README headline set.")
L.append("")
L.append("**Engines.** Same five engines as the parent comparison.")
L.append("")
L.append("**Dataset sha256.**")
L.append("")
L.append("```")
for k, v in sha.items():
    L.append(f"{v}  {k}")
L.append("```")
L.append("")
L.append("> **Caveat.** The UltraSQL row measures the relevant vec kernel **in")
L.append("> isolation** (no parser, no planner, no executor, no result-set")
L.append("> materialization). Every other row measures the engine's full SQL")
L.append("> pipeline. Read every UltraSQL row as a **lower bound** on the")
L.append("> eventual end-to-end query, not a like-for-like result.")
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

echo
echo "[8/8] done. raw outputs in $RAW; machine-readable results in $HERE/results.json"
