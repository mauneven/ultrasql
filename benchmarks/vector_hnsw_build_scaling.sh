#!/usr/bin/env bash
# Page-backed HNSW build-time scaling benchmark.
#
# Builds the page-backed (server-path) HNSW index at several row counts and
# reports `hnsw_build_time_us` from each artifact, so the build's scaling can be
# read directly. Used to certify the O(N²)→sub-quadratic graph-traversal build
# fix (see operator-reports/2026-06-hnsw-build-scaling.md). This measures
# UltraSQL's own access method; it makes no competitor claim.
#
# Usage:
#   benchmarks/vector_hnsw_build_scaling.sh
#   HNSW_BUILD_ROWS="2000 5000 10000 20000" HNSW_BUILD_DIMS=128 \
#       benchmarks/vector_hnsw_build_scaling.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${VECTOR_HNSW_BUILD_OUT_DIR:-benchmarks/results/latest/raw}"
ROWS="${HNSW_BUILD_ROWS:-2000 5000 10000 20000}"
DIMS="${HNSW_BUILD_DIMS:-128}"
M="${HNSW_BUILD_M:-16}"
EF_SEARCH="${HNSW_BUILD_EF_SEARCH:-64}"

mkdir -p "$OUT_DIR"

if (( DIMS <= 0 || M <= 0 || EF_SEARCH <= 0 )); then
    echo "vector_hnsw_build_scaling.sh: invalid DIMS/M/EF_SEARCH" >&2
    exit 2
fi

echo "=== Building bench binary ==="
CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --bin ultrasql-bench

echo "=== Page-backed HNSW build scaling (dims=$DIMS m=$M ef_search=$EF_SEARCH) ==="
printf '%8s  %16s\n' "rows" "hnsw_build_ms"
for rows in $ROWS; do
    out="$OUT_DIR/hnsw_build_scaling_${rows}_${DIMS}d-ultrasql.json"
    target/release/ultrasql-bench vector-memory \
        --rows "$rows" \
        --dims "$DIMS" \
        --m "$M" \
        --ef-search "$EF_SEARCH" \
        --output "$out" >/dev/null 2>&1
    python3 - "$rows" "$out" <<'PY'
import json
import sys

rows = sys.argv[1]
with open(sys.argv[2], "r", encoding="utf-8") as f:
    doc = json.load(f)
build_us = doc.get("hnsw_build_time_us")
if build_us is None:
    raise SystemExit("vector-memory artifact missing hnsw_build_time_us")
print(f"{int(rows):8d}  {build_us / 1000.0:16.1f}")
PY
done

echo "=== Done. Raw artifacts under $OUT_DIR ==="
