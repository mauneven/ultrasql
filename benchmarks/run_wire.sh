#!/usr/bin/env bash
# Run the full wire-protocol cross-engine benchmark suite.
#
# For UltraSQL: drives the `cross_compare_sql` binary, which spins up
# an in-process `ultrasqld` and runs each workload through real
# `tokio-postgres`. For every other engine: invokes the matching
# `benchmarks/scripts/run_<engine>_writes.sh` runner which drives the
# same workload set through that engine's native client.
#
# Output: one JSON per (workload × engine) under
# `benchmarks/results/latest/raw/`, plus `results.md` + `results.json`
# from `results-render`.
#
# Usage:
#   benchmarks/run_wire.sh                # full suite, all engines
#   benchmarks/run_wire.sh quick          # 4 iters / 1 warmup smoke run
#
# Environment overrides:
#   CH_BIN     path to clickhouse binary (default /tmp/ultracmp/clickhouse)
#   N_ITERS    sample count for competitor scripts (default 32 in full mode)
#   BENCH_WIRE_OUT_DIR  output directory (default benchmarks/results/latest)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

mode="${1:-full}"
case "$mode" in
    full)  ITERS=32; WARMUP=2 ;;
    quick) ITERS=4;  WARMUP=1 ;;
    *) echo "unknown mode '$mode' (full|quick)" >&2; exit 2 ;;
esac

out="${BENCH_WIRE_OUT_DIR:-benchmarks/results/latest}"
raw="$out/raw"
mkdir -p "$raw"

echo "=== UltraSQL wire-protocol bench  mode=$mode  iters=$ITERS  warmup=$WARMUP ==="

# ---------------------------------------------------------------------------
# Step 1: Build all bench binaries.
# ---------------------------------------------------------------------------
echo "--- Building bench binaries ---"
cargo build --release \
    --package ultrasql-bench \
    --features sql-bench \
    --bin cross_compare_sql \
    --bin results-render \
    --bin readme-render
BIN="target/release"

# ---------------------------------------------------------------------------
# Step 2: UltraSQL — drive cross_compare_sql for each workload.
# ---------------------------------------------------------------------------
# Each workload-id maps to a Workload enum variant + row count. The
# row counts mirror the competitor scripts (10 k for the OLTP shapes,
# 65 k for sum/window, 1 M for filter/avg).
echo "--- Running UltraSQL workloads ---"
declare -a UL_WORKLOADS=(
    "insert-bulk        10000   insert_throughput_10k"
    "update-bulk        10000   update_throughput_10k"
    "delete-bulk        10000   delete_throughput_10k"
    "mixed-oltp         10000   mixed_oltp_pgbench_like"
    "select-scan        10000   select_scan_10k"
    "sum-scalar         65536   select_sum_65k_i64"
    "avg-scalar         1000000 select_avg_1m_i64"
    "filter-sum         1000000 filter_sum_1m_i64"
    "window-row-number  65536   window_row_number_65k_i64"
)
for spec in "${UL_WORKLOADS[@]}"; do
    read -r wl rows wid <<<"$spec"
    echo "  workload: $wid (rows=$rows)"
    "$BIN/cross_compare_sql" \
        --workload "$wl" \
        --rows "$rows" \
        --warmup "$WARMUP" \
        --iters "$ITERS" \
        > "$raw/${wid}-ultrasql.json"
done

# ---------------------------------------------------------------------------
# Step 3: Competitors.
# ---------------------------------------------------------------------------
export RAW_DIR="$raw"
export N_ITERS="$ITERS"
export N_ROWS=10000
export PGUSER="${PGUSER:-$(id -un)}"
export PGDATABASE="${PGDATABASE:-ultrasql_bench}"
export CH_BIN="${CH_BIN:-/tmp/ultracmp/clickhouse}"

echo "--- Running PostgreSQL workloads ---"
bash benchmarks/scripts/run_postgres_writes.sh   || echo "    postgres skipped/failed"

echo "--- Running DuckDB workloads ---"
bash benchmarks/scripts/run_duckdb_writes.sh     || echo "    duckdb skipped/failed"

echo "--- Running SQLite workloads ---"
bash benchmarks/scripts/run_sqlite3_writes.sh    || echo "    sqlite3 skipped/failed"

echo "--- Running ClickHouse workloads ---"
bash benchmarks/scripts/run_clickhouse_writes.sh || echo "    clickhouse skipped/failed"

# ---------------------------------------------------------------------------
# Step 4: Render results.md + results.json + README badges.
# ---------------------------------------------------------------------------
echo "--- Rendering aggregated tables ---"
"$BIN/results-render" \
    --raw-dir "$raw" \
    --output-md "$out/results.md" \
    --output-json "$out/results.json"

echo "--- Refreshing README benchmark tables ---"
"$BIN/readme-render"

echo ""
echo "=== Done. Results in $out/ ==="
ls -1 "$raw" | head -20
