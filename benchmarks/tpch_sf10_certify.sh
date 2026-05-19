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
AUTO_RETRY_POOL="${TPCH_AUTO_RETRY_POOL:-1}"
POOL_RETRIES="${TPCH_POOL_RETRIES:-4}"
PAGE_BYTES=8192
POOL_BUDGET_PERCENT="${TPCH_POOL_BUDGET_PERCENT:-75}"
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

host_ram_bytes() {
    if command -v sysctl >/dev/null 2>&1; then
        sysctl -n hw.memsize 2>/dev/null && return 0
    fi
    if [[ -r /proc/meminfo ]]; then
        awk '/MemTotal/ { print $2 * 1024 }' /proc/meminfo && return 0
    fi
    echo 0
}

HOST_RAM_BYTES="${TPCH_HOST_RAM_BYTES:-$(host_ram_bytes)}"
MAX_POOL_BYTES=0
if [[ "$HOST_RAM_BYTES" =~ ^[0-9]+$ && "$HOST_RAM_BYTES" -gt 0 ]]; then
    MAX_POOL_BYTES=$((HOST_RAM_BYTES * POOL_BUDGET_PERCENT / 100))
fi

write_ultrasql_failure_summary() {
    local reason="$1"
    local pool_frames="$2"
    local log_path="$3"

    python3 - "$SUMMARY_OUT" "$reason" "$pool_frames" "$PAGE_BYTES" \
        "$HOST_RAM_BYTES" "$POOL_BUDGET_PERCENT" "$TPCH_DATA_DIR" \
        "$DUCKDB_OUT" "$ULTRA_OUT" "$log_path" <<'PY'
import json
import pathlib
import sys

(
    summary_path,
    reason,
    pool_frames_raw,
    page_bytes_raw,
    host_ram_raw,
    budget_percent_raw,
    data_dir,
    duckdb_out,
    ultra_out,
    log_path,
) = sys.argv[1:]

pool_frames = int(pool_frames_raw)
page_bytes = int(page_bytes_raw)
host_ram = int(host_ram_raw) if host_ram_raw.isdigit() else 0
log = pathlib.Path(log_path)
tail = ""
if log.exists():
    lines = log.read_text(errors="replace").splitlines()
    tail = "\n".join(lines[-40:])

doc = {
    "workload": "tpch_sf10",
    "scale_factor": 10,
    "target": "UltraSQL geometric mean <= 2x DuckDB geometric mean across all 22 queries",
    "passed": False,
    "reason": reason,
    "data_dir": data_dir,
    "ultrasql_pool_frames": pool_frames,
    "ultrasql_pool_bytes": pool_frames * page_bytes,
    "host_ram_bytes": host_ram,
    "pool_budget_percent": int(budget_percent_raw),
    "duckdb_result": duckdb_out,
    "duckdb_result_exists": pathlib.Path(duckdb_out).exists(),
    "ultrasql_result": ultra_out,
    "ultrasql_result_exists": pathlib.Path(ultra_out).exists(),
    "ultrasql_log_tail": tail,
    "next_step": (
        "Run on a certification host with enough RAM for the memory-backed "
        "benchmark server, or add a disk-backed UltraSQL benchmark server path "
        "before claiming SF10 certification."
    ),
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
}

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
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
run_ultrasql() {
    local pool_frames="${ULTRASQL_TPCH_POOL_FRAMES:-262144}"
    local attempt=0
    local log

    while :; do
        local pool_bytes=$((pool_frames * PAGE_BYTES))
        if [[ "$MAX_POOL_BYTES" -gt 0 && "$pool_bytes" -gt "$MAX_POOL_BYTES" ]]; then
            log="$(mktemp)"
            {
                echo "requested UltraSQL pool $pool_frames frames ($pool_bytes bytes)"
                echo "exceeds safe host RAM budget $MAX_POOL_BYTES bytes"
                echo "host_ram_bytes=$HOST_RAM_BYTES budget_percent=$POOL_BUDGET_PERCENT"
            } >"$log"
            write_ultrasql_failure_summary "pool_budget_exceeded" "$pool_frames" "$log"
            cat "$log" >&2
            rm -f "$log"
            return 2
        fi

        log="$(mktemp)"
        if ULTRASQL_TPCH_POOL_FRAMES="$pool_frames" \
            target/release/tpch run-queries ultrasql \
                --data-dir "$TPCH_DATA_DIR" \
                --runs "$RUNS" \
                --warmup "$WARMUP" \
                --queries "$QUERIES" \
                --scale 10 \
                --out "$ULTRA_OUT" \
                >"$log" 2>&1; then
            cat "$log"
            rm -f "$log"
            return 0
        fi

        if [ "$AUTO_RETRY_POOL" = "1" ] && [ "$attempt" -lt "$POOL_RETRIES" ] &&
            grep -q "buffer pool exhausted: every frame is pinned" "$log"; then
            attempt=$((attempt + 1))
            pool_frames=$((pool_frames * 2))
            echo "UltraSQL SF10 load hit buffer pool exhaustion. Retrying with ULTRASQL_TPCH_POOL_FRAMES=$pool_frames (attempt $attempt/$POOL_RETRIES)." >&2
            cat "$log" >&2
            rm -f "$log"
            continue
        fi

        if grep -q "buffer pool exhausted: every frame is pinned" "$log"; then
            write_ultrasql_failure_summary "buffer_pool_exhausted" "$pool_frames" "$log"
        else
            write_ultrasql_failure_summary "ultrasql_run_failed" "$pool_frames" "$log"
        fi
        cat "$log" >&2
        rm -f "$log"
        return 1
    done
}

run_ultrasql

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
