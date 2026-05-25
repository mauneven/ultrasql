#!/usr/bin/env bash
# Same-host UltraSQL vs PostgreSQL + pgvector exact-vector certification.
#
# This runner is intentionally strict: both UltraSQL and pgvector artifacts
# must be measured from the same host descriptor with the same deterministic
# workload and answer checksum. Missing pgvector writes a summary artifact and
# exits 2 so outer certification runners can record "unavailable" without
# publishing a fake benchmark claim.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${AI_VECTOR_PGVECTOR_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
SUMMARY_OUT="$OUT_DIR/ai_vector_pgvector_certification.json"
ROWS="${AI_VECTOR_PGVECTOR_ROWS:-${VECTOR_TOPK_ROWS:-10000}}"
DIMS="${AI_VECTOR_PGVECTOR_DIMS:-${VECTOR_TOPK_DIMS:-8}}"
TOP_K="${AI_VECTOR_PGVECTOR_K:-${VECTOR_TOPK_K:-10}}"
ITERS="${AI_VECTOR_PGVECTOR_ITERS:-${N_ITERS:-8}}"
WARMUP="${AI_VECTOR_PGVECTOR_WARMUP:-${WARMUP:-2}}"

mkdir -p "$RAW_DIR"

set +e
RAW_DIR="$RAW_DIR" \
    VECTOR_TOPK_OUT_DIR="$OUT_DIR" \
    VECTOR_TOPK_ROWS="$ROWS" \
    VECTOR_TOPK_DIMS="$DIMS" \
    VECTOR_TOPK_K="$TOP_K" \
    N_ITERS="$ITERS" \
    WARMUP="$WARMUP" \
    VECTOR_TOPK_REQUIRE_PGVECTOR=1 \
    VECTOR_TOPK_RENDER_RESULTS=0 \
    benchmarks/vector_topk_exact.sh
runner_status=$?
set -e

python3 - "$SUMMARY_OUT" "$RAW_DIR" "$ROWS" "$DIMS" "$TOP_K" "$runner_status" <<'PY'
import json
import math
import pathlib
import sys
import time

summary_path, raw_dir, rows, dims, top_k, runner_status = sys.argv[1:]
summary_path = pathlib.Path(summary_path)
raw_dir = pathlib.Path(raw_dir)
rows = int(rows)
dims = int(dims)
top_k = int(top_k)
runner_status = int(runner_status)

def row_label(n: int) -> str:
    if n >= 1_000_000 and n % 1_000_000 == 0:
        return f"{n // 1_000_000}m"
    if n >= 1_000 and n % 1_000 == 0:
        return f"{n // 1_000}k"
    if n == 65_536:
        return "65k"
    return str(n)

workload = f"vector_topk_exact_{row_label(rows)}_{dims}d_k{top_k}"
ultrasql_path = raw_dir / f"{workload}-ultrasql.json"
pgvector_path = raw_dir / f"{workload}-postgres17_pgvector.json"
required_metrics = [
    "recall_at_k",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "build_time_us",
    "memory_bytes",
    "index_size_bytes",
]

def load(path: pathlib.Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))

def host_key(doc: dict) -> tuple:
    host = doc.get("host") or {}
    return (
        str(host.get("cpu", "")),
        int(host.get("cores", 0) or 0),
        int(host.get("ram_gb", 0) or 0),
        str(host.get("os", "")),
    )

def percentile(values: list[float], rank: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * rank) - 1))
    return ordered[idx]

def write_summary(passed: bool, reason: str, details: dict | None = None) -> int:
    doc = {
        "schema_version": 1,
        "suite": "ai_vector_pgvector_same_host",
        "workload": workload,
        "status": "passed" if passed else "unavailable" if runner_status == 2 else "failed",
        "passed": passed,
        "reason": None if passed else reason,
        "same_host": bool(details and details.get("same_host")),
        "generated_at_unix": int(time.time()),
        "artifacts": {
            "ultrasql": str(ultrasql_path),
            "postgres17_pgvector": str(pgvector_path),
        },
        "required_metrics": required_metrics,
        "policy": (
            "Certification requires measured UltraSQL and PostgreSQL+pgvector "
            "artifacts on the same host descriptor with identical workload "
            "shape and top-k answer checksum. Missing pgvector is unavailable, "
            "not a benchmark claim."
        ),
    }
    if details:
        doc.update(details)
    summary_path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(doc, indent=2))
    if passed:
        return 0
    return 2 if runner_status == 2 else 1

if not ultrasql_path.exists():
    raise SystemExit(write_summary(False, "ultrasql_artifact_missing"))
if not pgvector_path.exists():
    raise SystemExit(write_summary(False, "pgvector_artifact_missing"))

ultrasql = load(ultrasql_path)
pgvector = load(pgvector_path)

for label, doc in [("ultrasql", ultrasql), ("postgres17_pgvector", pgvector)]:
    if doc.get('status') != 'measured':
        raise SystemExit(
            write_summary(False, f"{label}_not_measured", {"engine_status": doc.get("status")})
        )
    for key in required_metrics:
        if key not in doc:
            raise SystemExit(write_summary(False, f"{label}_{key}_missing"))

shape_keys = ["workload", "n_rows", "vector_dims", "top_k", "metric"]
for key in shape_keys:
    if ultrasql.get(key) != pgvector.get(key):
        raise SystemExit(write_summary(False, f"shape_mismatch_{key}"))

if ultrasql.get("answer") != pgvector.get("answer"):
    raise SystemExit(
        write_summary(
            False,
            "answer_mismatch",
            {
                "ultrasql_answer": ultrasql.get("answer"),
                "pgvector_answer": pgvector.get("answer"),
            },
        )
    )

same_host = host_key(ultrasql) == host_key(pgvector)
if not same_host:
    raise SystemExit(
        write_summary(
            False,
            "host_mismatch",
            {
                "same_host": False,
                "ultrasql_host": ultrasql.get("host"),
                "pgvector_host": pgvector.get("host"),
            },
        )
    )

details = {
    "same_host": True,
    "host": ultrasql.get("host"),
    "metrics": {
        "ultrasql": {
            "recall_at_k": ultrasql.get("recall_at_k"),
            "p50_latency_us": ultrasql.get("p50_latency_us"),
            "p95_latency_us": ultrasql.get("p95_latency_us"),
            "p99_latency_us": ultrasql.get("p99_latency_us"),
            "build_time_us": ultrasql.get("build_time_us"),
            "memory_bytes": ultrasql.get("memory_bytes"),
            "memory_status": ultrasql.get("memory_status"),
            "index_size_bytes": ultrasql.get("index_size_bytes"),
            "index_size_status": ultrasql.get("index_size_status"),
            "throughput_queries_per_sec": (
                1_000_000.0 / ultrasql["p50_latency_us"]
                if ultrasql.get("p50_latency_us")
                else None
            ),
        },
        "postgres17_pgvector": {
            "recall_at_k": pgvector.get("recall_at_k"),
            "p50_latency_us": pgvector.get("p50_latency_us"),
            "p95_latency_us": pgvector.get("p95_latency_us"),
            "p99_latency_us": pgvector.get("p99_latency_us"),
            "build_time_us": pgvector.get("build_time_us"),
            "memory_bytes": pgvector.get("memory_bytes"),
            "memory_status": pgvector.get("memory_status"),
            "index_size_bytes": pgvector.get("index_size_bytes"),
            "index_size_status": pgvector.get("index_size_status"),
            "throughput_queries_per_sec": (
                1_000_000.0 / pgvector["p50_latency_us"]
                if pgvector.get("p50_latency_us")
                else None
            ),
        },
    },
    "answer": ultrasql.get("answer"),
    "latency_ratio_pgvector_over_ultrasql_p50": (
        pgvector["p50_latency_us"] / ultrasql["p50_latency_us"]
        if pgvector.get("p50_latency_us") and ultrasql.get("p50_latency_us")
        else None
    ),
    "p99_latency_us": {
        "ultrasql": percentile(ultrasql.get("iterations_us", []), 0.99),
        "postgres17_pgvector": percentile(pgvector.get("iterations_us", []), 0.99),
    },
}

raise SystemExit(write_summary(True, "passed", details))
PY
