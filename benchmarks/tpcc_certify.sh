#!/usr/bin/env bash
# TPC-C certification placeholder.
#
# This writes an explicit artifact instead of silently pretending TPC-C exists.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${TPCC_OUT_DIR:-benchmarks/results/latest}"
SUMMARY_OUT="$OUT_DIR/tpcc_certification.json"
mkdir -p "$OUT_DIR"

python3 - "$SUMMARY_OUT" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
doc = {
    "workload": "tpcc_5types",
    "target": (
        "TPC-C NewOrder, Payment, OrderStatus, Delivery, and StockLevel "
        "with durable commits and concurrent PostgreSQL-wire sessions"
    ),
    "passed": False,
    "reason": "runner_not_implemented",
    "next_step": (
        "Replace crates/ultrasql-bench/src/runs/tpcc.rs placeholder with a "
        "real TPC-C runner, then make this script measure UltraSQL and "
        "PostgreSQL on the same host."
    ),
}
path.write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY

exit 2
