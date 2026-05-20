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
POSTGRES_DSN="${POSTGRES_DSN:-}"
ULTRASQL_DSN="${ULTRASQL_DSN:-}"
TPCB_SCALE="${TPCB_SCALE:-1}"
TPCB_ACCOUNTS="${TPCB_ACCOUNTS:-}"
TPCB_DURATION="${TPCB_DURATION:-60}"
TPCB_WARMUP="${TPCB_WARMUP:-30}"
TPCB_CONNECTIONS="${TPCB_CONNECTIONS:-32}"
ALLOW_ULTRASQL_ONLY="${TPCB_ALLOW_ULTRASQL_ONLY:-0}"
OUT_DIR="benchmarks/results/latest"
RAW_DIR="$OUT_DIR/raw"
SUMMARY_OUT="$OUT_DIR/tpcb_certification.json"

mkdir -p "$RAW_DIR"

if [[ -z "$POSTGRES_RESULT" && -z "$POSTGRES_DSN" && "$ALLOW_ULTRASQL_ONLY" != "1" ]]; then
    python3 - "$SUMMARY_OUT" <<'PY'
import json
import pathlib
import sys

summary_path = sys.argv[1]
doc = {
    "workload": "tpcb_32conn",
    "target": "UltraSQL throughput >= 2x PostgreSQL 17 and p99 < 5 ms at 32 clients",
    "passed": False,
    "reason": "postgres_dsn_missing",
    "postgres_result": None,
    "ultrasql_result": None,
    "next_step": (
        "Set POSTGRES_DSN or POSTGRES_TPCB_RESULT for certification. "
        "Set TPCB_ALLOW_ULTRASQL_ONLY=1 only for local UltraSQL-only smoke."
    ),
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
    exit 2
fi

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --features sql-bench \
        --bin regression-gate --bin ultrasql-bench >/dev/null

target/release/regression-gate --stage v0_9 --smoke \
    > "$RAW_DIR/tpcb_32conn-ultrasql-kernel.json" 2>&1 || true

if [[ -z "$ULTRASQL_RESULT" ]]; then
    ULTRASQL_RESULT="$RAW_DIR/tpcb_32conn-ultrasql.json"
    ULTRASQL_ARGS=(
        tpcb
        --engine ultrasql
        --scale "$TPCB_SCALE"
        --duration "$TPCB_DURATION"
        --warmup "$TPCB_WARMUP"
        --connections "$TPCB_CONNECTIONS"
        --output "$ULTRASQL_RESULT"
    )
    if [[ -n "$TPCB_ACCOUNTS" ]]; then
        ULTRASQL_ARGS+=(--accounts "$TPCB_ACCOUNTS")
    fi
    if [[ -n "$ULTRASQL_DSN" ]]; then
        ULTRASQL_ARGS+=(--dsn "$ULTRASQL_DSN")
    fi
    target/release/ultrasql-bench "${ULTRASQL_ARGS[@]}" || true
fi

if [[ -z "$POSTGRES_RESULT" ]]; then
    POSTGRES_RESULT="$RAW_DIR/tpcb_32conn-postgres17.json"
    if [[ -n "$POSTGRES_DSN" ]]; then
        POSTGRES_ARGS=(
            tpcb
            --engine postgres17
            --dsn "$POSTGRES_DSN"
            --scale "$TPCB_SCALE"
            --duration "$TPCB_DURATION"
            --warmup "$TPCB_WARMUP"
            --connections "$TPCB_CONNECTIONS"
            --output "$POSTGRES_RESULT"
        )
        if [[ -n "$TPCB_ACCOUNTS" ]]; then
            POSTGRES_ARGS+=(--accounts "$TPCB_ACCOUNTS")
        fi
        target/release/ultrasql-bench "${POSTGRES_ARGS[@]}" || true
    fi
fi

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
pg_tps = None
ul_tps = None
ul_p99_us = None
ratio = None
pg_correct = None
ul_correct = None
if pg is not None:
    pg_tps = float(pg.get("throughput_per_sec", 0.0))
    pg_correct = bool(pg.get("correctness", {}).get("passed", False))
if ultra is not None:
    ul_tps = float(ultra.get("throughput_per_sec", 0.0))
    ul_p99_us = float(ultra.get("p99_latency_us", 10**12))
    ul_correct = bool(ultra.get("correctness", {}).get("passed", False))
if pg_tps and ul_tps:
    ratio = ul_tps / pg_tps
if pg is None or ultra is None:
    reason = "missing_cross_engine_results"
else:
    passed = (
        pg_correct
        and ul_correct
        and pg_tps > 0
        and ul_tps >= pg_tps * 2.0
        and ul_p99_us < 5000.0
    )
    if not passed:
        reason = "target_not_met"

doc = {
    "workload": "tpcb_32conn",
    "target": "UltraSQL throughput >= 2x PostgreSQL 17 and p99 < 5 ms at 32 clients",
    "passed": passed,
    "reason": reason,
    "postgres_result": pg_path or None,
    "ultrasql_result": ultra_path or None,
    "postgres_throughput_per_sec": pg_tps,
    "ultrasql_throughput_per_sec": ul_tps,
    "throughput_ratio_ultrasql_vs_postgres": ratio,
    "ultrasql_p99_latency_us": ul_p99_us,
    "kernel_smoke_result": "benchmarks/results/latest/raw/tpcb_32conn-ultrasql-kernel.json",
    "next_step": (
        "Set POSTGRES_DSN for PostgreSQL 17 and rerun the same command on a quiet host "
        "if certification is not yet passed."
    ),
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(2 if reason == "missing_cross_engine_results" else (0 if passed else 1))
PY
