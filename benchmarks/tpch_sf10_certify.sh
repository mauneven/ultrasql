#!/usr/bin/env bash
# Reproducible TPC-H SF10 certification runner.
#
# This script writes timing baselines for DuckDB and UltraSQL, then writes a
# certification summary under benchmarks/results/latest/.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

TPCH_DATA_DIR="${TPCH_DATA_DIR:-target/tpch-scale10-real}"
DUCKDB_BIN="${TPCH_DUCKDB:-$(command -v duckdb || true)}"
RUNS="${TPCH_RUNS:-5}"
WARMUP="${TPCH_WARMUP:-1}"
QUERIES="${TPCH_QUERIES:-all}"
OUT_DIR="benchmarks/results/latest"
RAW_DIR="$OUT_DIR/raw"
ULTRA_OUT="$RAW_DIR/tpch_sf10-ultrasql.json"
DUCKDB_OUT="$RAW_DIR/tpch_sf10-duckdb.json"
SUMMARY_OUT="$OUT_DIR/tpch_sf10_certification.json"

mkdir -p "$RAW_DIR"

if [[ ! -d "$TPCH_DATA_DIR" ]]; then
    echo "TPC-H data dir missing: $TPCH_DATA_DIR" >&2
    echo "Run: target/release/tpch gen-data 10 $TPCH_DATA_DIR" >&2
    exit 2
fi

if [[ -z "$DUCKDB_BIN" || ! -x "$DUCKDB_BIN" ]]; then
    echo "DuckDB binary missing. Set TPCH_DUCKDB=/path/to/duckdb" >&2
    exit 2
fi

cargo build --release --package ultrasql-bench --features sql-bench --bin tpch

echo "Running DuckDB TPC-H SF10: queries=$QUERIES runs=$RUNS warmup=$WARMUP"
target/release/tpch run-queries duckdb \
    --duckdb "$DUCKDB_BIN" \
    --data-dir "$TPCH_DATA_DIR" \
    --runs "$RUNS" \
    --warmup "$WARMUP" \
    --queries "$QUERIES" \
    --scale 10 \
    --out "$DUCKDB_OUT"

echo "Running UltraSQL TPC-H SF10: queries=$QUERIES runs=$RUNS warmup=$WARMUP"
target/release/tpch run-queries ultrasql \
    --data-dir "$TPCH_DATA_DIR" \
    --runs "$RUNS" \
    --warmup "$WARMUP" \
    --queries "$QUERIES" \
    --scale 10 \
    --out "$ULTRA_OUT"

python3 - "$DUCKDB_OUT" "$ULTRA_OUT" "$SUMMARY_OUT" <<'PY'
import json
import math
import pathlib
import sys

duckdb_path, ultrasql_path, out_path = map(pathlib.Path, sys.argv[1:])
duckdb = json.loads(duckdb_path.read_text())
ultrasql = json.loads(ultrasql_path.read_text())

def gm(doc):
    vals = [
        timing["median_ms"]
        for timing in doc["queries"].values()
        if timing["median_ms"] and math.isfinite(timing["median_ms"]) and timing["median_ms"] > 0
    ]
    if len(vals) != len(doc["queries"]):
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))

duckdb_gm = gm(duckdb)
ultrasql_gm = gm(ultrasql)
passed = (
    duckdb_gm is not None
    and ultrasql_gm is not None
    and ultrasql_gm <= duckdb_gm * 2.0
)
summary = {
    "workload": "tpch_sf10",
    "scale_factor": 10,
    "duckdb_geomean_ms": duckdb_gm,
    "ultrasql_geomean_ms": ultrasql_gm,
    "target": "UltraSQL geometric mean <= 2x DuckDB geometric mean",
    "passed": passed,
    "duckdb_result": str(duckdb_path),
    "ultrasql_result": str(ultrasql_path),
}
out_path.write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
sys.exit(0 if passed else 1)
PY
