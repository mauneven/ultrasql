#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${TPCC_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
SUMMARY="$OUT_DIR/tpcc_certification.json"
ITERS="${TPCC_ITERS:-5}"
WARMUP="${TPCC_WARMUP:-1}"
POSTGRES_RESULT="${POSTGRES_TPCC_RESULT:-}"
ULTRASQL_RESULT="$RAW_DIR/tpcc_5types-ultrasql.json"

mkdir -p "$RAW_DIR"

cargo build -p ultrasql-bench --release >/dev/null
target/release/ultrasql-bench tpcc \
  --profile local-kernel \
  --iterations "$ITERS" \
  --warmup "$WARMUP" \
  --output "$ULTRASQL_RESULT"

python3 - "$ULTRASQL_RESULT" "$POSTGRES_RESULT" "$SUMMARY" <<'PY'
import json
import sys
from pathlib import Path

ultrasql_path = Path(sys.argv[1])
postgres_arg = sys.argv[2]
summary_path = Path(sys.argv[3])


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def throughput_per_sec(document: dict) -> float:
    for key in (
        "throughput_per_sec",
        "transactions_per_sec",
        "tps",
        "tpmC",
        "new_order_tpmC",
    ):
        value = document.get(key)
        if isinstance(value, (int, float)):
            if key in {"tpmC", "new_order_tpmC"}:
                return float(value) / 60.0
            return float(value)
    raise SystemExit(f"missing throughput field in {document!r}")


ultrasql = load_json(ultrasql_path)
ultrasql_tps = throughput_per_sec(ultrasql)
postgres_path = Path(postgres_arg) if postgres_arg else None

summary = {
    "schema_version": 1,
    "workload": "tpcc_5types",
    "target": "ultrasql throughput >= 2x postgres17 throughput",
    "ultrasql_result": str(ultrasql_path),
    "ultrasql_scope": ultrasql.get("certification_scope", "unknown"),
    "ultrasql_throughput_per_sec": ultrasql_tps,
    "postgres_result": str(postgres_path) if postgres_path else None,
    "postgres_throughput_per_sec": None,
    "required_ultrasql_throughput_per_sec": None,
    "passed": False,
    "status": "partial",
    "reason": "postgres_result_missing",
}
exit_code = 2

if postgres_path is not None:
    postgres = load_json(postgres_path)
    postgres_tps = throughput_per_sec(postgres)
    required_tps = postgres_tps * 2.0
    passed = ultrasql_tps >= required_tps
    summary.update(
        {
            "postgres_throughput_per_sec": postgres_tps,
            "required_ultrasql_throughput_per_sec": required_tps,
            "passed": passed,
            "status": "passed" if passed else "failed",
            "reason": "target_met" if passed else "target_missed",
        }
    )
    exit_code = 0 if passed else 1

summary_path.parent.mkdir(parents=True, exist_ok=True)
summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
raise SystemExit(exit_code)
PY
