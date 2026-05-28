#!/usr/bin/env bash
# Runtime HNSW ANN vector benchmark.
#
# Emits a reproducible artifact with recall@k, p50/p95/p99 query latency,
# build time, and estimated graph memory. This benchmark measures UltraSQL's
# in-process HNSW access method directly; it does not make competitor claims.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${VECTOR_ANN_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
ROWS="${VECTOR_ANN_ROWS:-10000}"
DIMS="${VECTOR_ANN_DIMS:-8}"
TOP_K="${VECTOR_ANN_K:-10}"
QUERIES="${VECTOR_ANN_QUERIES:-50}"
WARMUP="${VECTOR_ANN_WARMUP:-5}"
M="${VECTOR_ANN_M:-16}"
EF_SEARCH="${VECTOR_ANN_EF_SEARCH:-64}"
SEED="${VECTOR_ANN_SEED:-1367265502}"

mkdir -p "$RAW_DIR"

if (( ROWS <= 0 || DIMS <= 0 || TOP_K <= 0 || QUERIES <= 0 || WARMUP < 0 || M <= 0 || EF_SEARCH <= 0 )); then
    echo "vector_ann_hnsw.sh: invalid ROWS/DIMS/TOP_K/QUERIES/WARMUP/M/EF_SEARCH" >&2
    exit 2
fi

row_label="$(
    python3 - "$ROWS" <<'PY'
import sys
n = int(sys.argv[1])
if n >= 1_000_000 and n % 1_000_000 == 0:
    print(f"{n // 1_000_000}m")
elif n >= 1_000 and n % 1_000 == 0:
    print(f"{n // 1_000}k")
elif n == 65_536:
    print("65k")
else:
    print(str(n))
PY
)"
WORKLOAD="vector_ann_hnsw_${row_label}_${DIMS}d_k${TOP_K}"
OUT_FILE="$RAW_DIR/${WORKLOAD}-ultrasql_hnsw.json"

echo "=== UltraSQL HNSW ANN benchmark rows=$ROWS dims=$DIMS k=$TOP_K queries=$QUERIES warmup=$WARMUP m=$M ef_search=$EF_SEARCH ==="
echo "--- Building bench binary ---"
CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release \
        --package ultrasql-bench \
        --bin ultrasql-bench

echo "--- Running ANN benchmark ---"
target/release/ultrasql-bench ann-vector \
    --rows "$ROWS" \
    --dims "$DIMS" \
    --top-k "$TOP_K" \
    --queries "$QUERIES" \
    --warmup "$WARMUP" \
    --m "$M" \
    --ef-search "$EF_SEARCH" \
    --seed "$SEED" \
    --output "$OUT_FILE"

python3 - "$OUT_FILE" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    doc = json.load(f)

required = [
    "status",
    "samples",
    "median_us",
    "min_us",
    "iterations_us",
    "recall_at_k",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "build_time_us",
    "memory_bytes",
]
missing = [key for key in required if key not in doc]
if missing:
    raise SystemExit(f"ANN artifact missing fields: {', '.join(missing)}")

print(
    "status={status} samples={samples} median_us={median_us} "
    "recall_at_k={recall_at_k} p50_latency_us={p50_latency_us} "
    "p95_latency_us={p95_latency_us} p99_latency_us={p99_latency_us} "
    "build_time_us={build_time_us} memory_bytes={memory_bytes}".format(**doc)
)
PY

echo "=== Done. Raw artifact: $OUT_FILE ==="
