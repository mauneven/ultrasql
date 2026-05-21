#!/usr/bin/env bash
# Object-store Parquet range certification smoke.
#
# Runs UltraSQL against a local S3-compatible range-only mock inside
# `cross_compare_sql`. The raw artifact records request ranges and fails if the
# SQL path fetches the whole object or projected-out column chunks.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${OBJECT_PARQUET_RANGE_PROFILE:-${1:-smoke}}"
OUT_DIR="${OBJECT_PARQUET_RANGE_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
MANIFEST="$OUT_DIR/object_parquet_range_manifest.json"

case "$PROFILE" in
    smoke)
        ROWS="${OBJECT_PARQUET_RANGE_ROWS:-3}"
        WARMUP="${OBJECT_PARQUET_RANGE_WARMUP:-0}"
        ITERS="${OBJECT_PARQUET_RANGE_ITERS:-1}"
        ;;
    full)
        ROWS="${OBJECT_PARQUET_RANGE_ROWS:-3}"
        WARMUP="${OBJECT_PARQUET_RANGE_WARMUP:-2}"
        ITERS="${OBJECT_PARQUET_RANGE_ITERS:-8}"
        ;;
    *)
        echo "object_parquet_range.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

mkdir -p "$RAW_DIR"
artifact="$RAW_DIR/object_parquet_range_${PROFILE}-ultrasql.json"

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --features sql-bench \
        --bin cross_compare_sql >/dev/null

target/release/cross_compare_sql \
    --workload object-parquet-range \
    --rows "$ROWS" \
    --warmup "$WARMUP" \
    --iters "$ITERS" \
    --workload-id "object_parquet_range_${PROFILE}" \
    --output "$artifact"

python3 - "$PROFILE" "$ROWS" "$WARMUP" "$ITERS" "$MANIFEST" "$artifact" <<'PY'
import json
import pathlib
import sys
import time

profile, rows, warmup, iters, manifest_path, artifact = sys.argv[1:]
raw = json.loads(pathlib.Path(artifact).read_text(encoding="utf-8"))
status = "passed" if raw.get("status") == "measured" else "failed"
doc = {
    "schema_version": 1,
    "suite": "object_parquet_range",
    "profile": profile,
    "n_rows": int(rows),
    "warmup": int(warmup),
    "iters": int(iters),
    "generated_at_unix": int(time.time()),
    "status": status,
    "passed": status == "passed",
    "artifacts": [
        {
            "engine": "ultrasql",
            "status": raw.get("status", "unknown"),
            "artifact": artifact,
        }
    ],
    "policy": (
        "Object Parquet range certification requires a measured artifact whose "
        "raw request log proves ranged object-store reads and no whole-object fetch."
    ),
}
pathlib.Path(manifest_path).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(json.dumps(doc, indent=2))
PY
