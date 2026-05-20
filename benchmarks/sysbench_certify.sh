#!/usr/bin/env bash
# Sysbench-style OLTP certification runner.
#
# Uses UltraSQL's PostgreSQL-wire mixed OLTP workload as the committed
# sysbench-shaped smoke/full artifact until native sysbench Lua integration
# lands.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${SYSBENCH_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
ROWS="${SYSBENCH_ROWS:-10000}"
ITERS="${SYSBENCH_ITERS:-5}"
WARMUP="${SYSBENCH_WARMUP:-1}"
SUMMARY_OUT="$OUT_DIR/sysbench_certification.json"

mkdir -p "$RAW_DIR"

if (( ROWS <= 0 || ITERS <= 0 || WARMUP < 0 )); then
    echo "sysbench_certify.sh: invalid SYSBENCH_ROWS/SYSBENCH_ITERS/SYSBENCH_WARMUP" >&2
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
WORKLOAD="sysbench_oltp_read_write_${row_label}"
RAW_OUT="$RAW_DIR/${WORKLOAD}-ultrasql.json"

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --features sql-bench \
        --bin cross_compare_sql >/dev/null

target/release/cross_compare_sql \
    --workload mixed-oltp \
    --rows "$ROWS" \
    --iters "$ITERS" \
    --warmup "$WARMUP" \
    --workload-id "$WORKLOAD" \
    --output "$RAW_OUT"

python3 - "$SUMMARY_OUT" "$RAW_OUT" "$ROWS" "$ITERS" "$WARMUP" <<'PY'
import json
import math
import pathlib
import sys

summary_path, raw_path, rows_raw, iters_raw, warmup_raw = sys.argv[1:]
raw = json.loads(pathlib.Path(raw_path).read_text())
median_us = float(raw.get("median_us", 0.0) or 0.0)
samples = int(raw.get("samples", 0) or 0)
iters = int(iters_raw)
passed = samples == iters and math.isfinite(median_us) and median_us > 0.0
doc = {
    "workload": "sysbench_oltp_read_write",
    "target": "UltraSQL PostgreSQL-wire mixed OLTP artifact completes with positive latency samples",
    "passed": passed,
    "reason": None if passed else "missing_or_invalid_samples",
    "rows": int(rows_raw),
    "iterations": iters,
    "warmup": int(warmup_raw),
    "ultrasql_result": raw_path,
    "median_us": median_us,
    "samples": samples,
    "competitor_claim": None,
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(0 if passed else 1)
PY
