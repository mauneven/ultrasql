#!/usr/bin/env bash
# Reproducible TPC-B v0.9 certification runner.
#
# This runner records the required artifact even when certification cannot be
# attempted. A pass requires both engines to be measured on the same host with
# 32 clients, UltraSQL throughput >= 2x PostgreSQL 17, and UltraSQL p99 < 5 ms.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

POSTGRES_RESULT="${POSTGRES_TPCB_RESULT:-}"
ULTRASQL_RESULT="${ULTRASQL_TPCB_RESULT:-}"
OUT_DIR="benchmarks/results/latest"
RAW_DIR="$OUT_DIR/raw"
SUMMARY_OUT="$OUT_DIR/tpcb_certification.json"

mkdir -p "$RAW_DIR"

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --bin regression-gate >/dev/null

target/release/regression-gate --stage v0_9 --smoke \
    > "$RAW_DIR/tpcb_32conn-ultrasql-kernel.json" 2>&1 || true

python3 - "$SUMMARY_OUT" "$POSTGRES_RESULT" "$ULTRASQL_RESULT" <<'PY'
import json
import pathlib
import sys

summary_path, pg_path, ultra_path = sys.argv[1:]

def load(path):
    if not path:
        return None
    p = pathlib.Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text())

pg = load(pg_path)
ultra = load(ultra_path)
reason = None
passed = False
if pg is None or ultra is None:
    reason = "missing_cross_engine_results"
else:
    pg_tps = float(pg.get("throughput_per_sec", 0.0))
    ul_tps = float(ultra.get("throughput_per_sec", 0.0))
    ul_p99_us = float(ultra.get("p99_latency_us", 10**12))
    passed = pg_tps > 0 and ul_tps >= pg_tps * 2.0 and ul_p99_us < 5000.0
    if not passed:
        reason = "target_not_met"

doc = {
    "workload": "tpcb_32conn",
    "target": "UltraSQL throughput >= 2x PostgreSQL 17 and p99 < 5 ms at 32 clients",
    "passed": passed,
    "reason": reason,
    "postgres_result": pg_path or None,
    "ultrasql_result": ultra_path or None,
    "kernel_smoke_result": "benchmarks/results/latest/raw/tpcb_32conn-ultrasql-kernel.json",
    "next_step": (
        "Capture POSTGRES_TPCB_RESULT and ULTRASQL_TPCB_RESULT from same-host "
        "32-client PostgreSQL-wire runs, then rerun benchmarks/tpcb_certify.sh."
    ),
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(0 if passed else 1)
PY
