#!/usr/bin/env bash
# Run the full UltraSQL benchmark suite at the chosen tier.
#
# Usage:
#   benchmarks/run.sh [low|ultra] [engines]
#   benchmarks/run.sh ultra writes
#
# Arguments:
#   tier     "low" (default) or "ultra"
#            low:   100 000 rows, 5 iters, 1 warmup — fast feedback / CI.
#            ultra: 10 000 000 rows, 8 iters, 2 warmup — publishable numbers.
#
#   engines  Comma-separated list of engines to include.
#            Defaults to: postgres17,duckdb,sqlite3,clickhouse,cockroachdb
#            Special value: "writes" — run only the write-side workloads ×
#            all available competitor engines (postgres17, duckdb, sqlite3).
#
# Pre-requisites:
#   - Rust toolchain on PATH (cargo).
#   - Engine binaries on PATH: duckdb, sqlite3, psql, clickhouse.
#   - PostgreSQL running on localhost:5432 (postgres17 engine).
#   - ClickHouse binary: /tmp/ultracmp/clickhouse or CH_BIN env var.
#
# Output:
#   benchmarks/results/latest/raw/<workload>-<engine>.json  — raw per-workload
#   benchmarks/results/latest/results.md                    — rendered table
#   benchmarks/results/latest/results.json                  — machine-readable
#
# The script is idempotent: re-running overwrites previous output.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

tier="${1:-low}"
engines="${2:-postgres17,duckdb,sqlite3,clickhouse,cockroachdb}"

# ---------------------------------------------------------------------------
# "ultra writes" subcommand: write-side workloads × all available engines
# ---------------------------------------------------------------------------
if [[ "$tier" == "ultra" && "$engines" == "writes" ]]; then
    out="benchmarks/results/latest"
    raw="$out/raw"
    mkdir -p "$raw"

    echo "=== UltraSQL write-side competitor benchmark  tier=ultra ==="

    # Build the UltraSQL write binary and the renderer.
    echo "--- Building bench binaries ---"
    cargo build --release \
        --package ultrasql-bench \
        --bin cross_compare_writes \
        --bin results-render

    BIN="target/release"

    # Generate write dataset (10 000 rows — matches the workload spec).
    DATADIR="/tmp/ultracmp-writes-$$"
    mkdir -p "$DATADIR"
    trap 'rm -rf "$DATADIR"' EXIT

    echo "--- Generating write dataset (10 000 rows) in $DATADIR ---"
    python3 - "$DATADIR" <<'PYEOF'
import sys, random, csv
datadir = sys.argv[1]
n = 10_000
rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-2**31, 2**31 - 1) for _ in range(n)]
with open(f"{datadir}/data_write.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id", "val"])
    for i, v in zip(ids, vals):
        w.writerow([i, v])
print(f"Generated data_write.csv ({n} rows)")
PYEOF

    # UltraSQL kernel write workloads.
    echo "--- Running UltraSQL kernel write workloads ---"
    for wl in insert-bulk update delete; do
        echo "  workload: $wl"
        "$BIN/cross_compare_writes" \
            --workload "$wl" \
            --tier low \
            --iters 8 \
            --warmup 1 \
            --data "$DATADIR/data_write.csv" \
            > "$raw/${wl}-ultrasql.json"
    done

    # Competitor write workloads.
    echo "--- Running competitor write workloads ---"
    export RAW_DIR="$raw"
    export N_ITERS=8
    export N_ROWS=10000
    export PGUSER="${PGUSER:-$(id -un)}"
    export PGDATABASE="${PGDATABASE:-ultrasql_bench}"

    bash benchmarks/scripts/run_postgres_writes.sh
    bash benchmarks/scripts/run_duckdb_writes.sh
    bash benchmarks/scripts/run_sqlite3_writes.sh

    # Render results.
    echo "--- Rendering results ---"
    "$BIN/results-render" \
        --raw-dir "$raw" \
        --output-md "$out/results.md" \
        --output-json "$out/results.json"

    echo ""
    echo "=== Done. Results in $out/ ==="
    echo "    $out/results.md"
    echo "    $out/results.json"
    exit 0
fi

if [[ "$tier" != "low" && "$tier" != "ultra" ]]; then
    echo "Error: tier must be 'low' or 'ultra', got '$tier'" >&2
    exit 1
fi

out="benchmarks/results/latest"
raw="$out/raw"
mkdir -p "$raw"

echo "=== UltraSQL benchmark suite  tier=$tier  engines=$engines ==="

# ---------------------------------------------------------------------------
# Step 1: Build the bench binaries in release mode.
# ---------------------------------------------------------------------------
echo "--- Building bench binaries ---"
cargo build --release \
    --package ultrasql-bench \
    --bin cross_compare \
    --bin cross_compare_writes \
    --bin cross_concurrency \
    --bin point_lookup \
    --bin results-render

BIN="target/release"

# ---------------------------------------------------------------------------
# Step 2: Generate synthetic datasets.
# ---------------------------------------------------------------------------
# Datasets are generated once and reused across workloads. Each engine
# reads from the same CSV file so the input bytes are identical.
DATADIR="/tmp/ultracmp-run-$$"
mkdir -p "$DATADIR"
trap 'rm -rf "$DATADIR"' EXIT

echo "--- Generating datasets in $DATADIR ---"
python3 - "$DATADIR" "$tier" <<'PYEOF'
import sys, random, csv

datadir = sys.argv[1]
tier    = sys.argv[2]

rows_read  = 100_000    if tier == "low" else 10_000_000
rows_write = 100_000    if tier == "low" else 1_000_000

rng = random.Random(0xDEADBEEF)
xs  = [rng.randint(-2**31, 2**31 - 1) for _ in range(rows_read)]

rng2 = random.Random(0xBADC0DE)
ys   = [rng2.randint(-2**31, 2**31 - 1) for _ in range(rows_read)]

# Single-column read dataset.
with open(f"{datadir}/data_x.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["x"])
    for v in xs:
        w.writerow([v])

# Two-column read dataset.
with open(f"{datadir}/data_y.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["y"])
    for v in ys:
        w.writerow([v])

# Write dataset (id, val).
rng3 = random.Random(0xC0FFEE)
ids  = list(range(rows_write))
rng3.shuffle(ids)
vals = [rng3.randint(-2**31, 2**31 - 1) for _ in range(rows_write)]
with open(f"{datadir}/data_write.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id", "val"])
    for i, v in zip(ids, vals):
        w.writerow([i, v])

print(f"Generated: data_x.csv ({rows_read} rows), data_y.csv, data_write.csv ({rows_write} rows)")
PYEOF

# ---------------------------------------------------------------------------
# Step 3: UltraSQL kernel workloads (cross_compare).
# ---------------------------------------------------------------------------
echo "--- Running UltraSQL kernel read workloads ---"
for wl in sum count min max minmax avg range; do
    echo "  workload: $wl"
    "$BIN/cross_compare" \
        --workload "$wl" \
        --tier "$tier" \
        --data "$DATADIR/data_x.csv" \
        > "$raw/${wl}-ultrasql.json"
done

echo "  workload: filter"
"$BIN/cross_compare" \
    --workload filter \
    --tier "$tier" \
    --data "$DATADIR/data_x.csv" \
    --data2 "$DATADIR/data_y.csv" \
    > "$raw/filter-ultrasql.json"

echo "  workload: point"
"$BIN/cross_compare" \
    --workload point \
    --tier "$tier" \
    --data "$DATADIR/data_x.csv" \
    > "$raw/point-ultrasql.json"

# ---------------------------------------------------------------------------
# Step 4: UltraSQL write workloads (cross_compare_writes).
# ---------------------------------------------------------------------------
echo "--- Running UltraSQL kernel write workloads ---"
for wl in insert-bulk update delete; do
    echo "  workload: $wl"
    "$BIN/cross_compare_writes" \
        --workload "$wl" \
        --tier "$tier" \
        --data "$DATADIR/data_write.csv" \
        > "$raw/${wl}-ultrasql.json"
done

# ---------------------------------------------------------------------------
# Step 5: UltraSQL concurrency workloads (cross_concurrency).
# ---------------------------------------------------------------------------
echo "--- Running UltraSQL concurrency workloads ---"
for wl in conc-read-sum conc-read-point conc-insert conc-update; do
    echo "  workload: $wl (threads=4)"
    "$BIN/cross_concurrency" \
        --workload "$wl" \
        --tier "$tier" \
        --threads 4 \
        > "$raw/${wl}-ultrasql.json"
done

# ---------------------------------------------------------------------------
# Step 6: Render results.md + results.json from raw/.
# ---------------------------------------------------------------------------
echo "--- Rendering results ---"
"$BIN/results-render" \
    --raw-dir "$raw" \
    --output-md "$out/results.md" \
    --output-json "$out/results.json"

echo ""
echo "=== Done. Results in $out/ ==="
echo "    $out/results.md"
echo "    $out/results.json"
