#!/usr/bin/env bash
# Reproducible TPC-H SF10 certification runner.
#
# This script writes timing baselines for DuckDB and UltraSQL, then writes a
# certification summary under benchmarks/results/latest/.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

TPCH_DATA_DIR="${TPCH_DATA_DIR:-${ULTRASQL_BENCH_SCRATCH:-${TMPDIR:-/tmp}/ultrasql-bench}/tpch-scale10-real}"
DUCKDB_BIN="${TPCH_DUCKDB:-$(command -v duckdb || true)}"
RUNS="${TPCH_RUNS:-5}"
WARMUP="${TPCH_WARMUP:-1}"
QUERIES="${TPCH_QUERIES:-all}"
TPCH_TIMEOUT_SECONDS="${TPCH_TIMEOUT_SECONDS:-21600}"
ULTRASQL_PROGRESS="${ULTRASQL_TPCH_PROGRESS:-1}"
ULTRASQL_SPILL_BACKING="${ULTRASQL_PAGE_SPILL_BACKING:-memory}"
AUTO_RETRY_POOL="${TPCH_AUTO_RETRY_POOL:-1}"
POOL_RETRIES="${TPCH_POOL_RETRIES:-4}"
PAGE_BYTES=8192
POOL_BUDGET_PERCENT="${TPCH_POOL_BUDGET_PERCENT:-75}"
OUT_DIR="${BENCH_CERT_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
ULTRA_OUT="$RAW_DIR/tpch_sf10-ultrasql.json"
DUCKDB_OUT="$RAW_DIR/tpch_sf10-duckdb.json"
SUMMARY_OUT="$OUT_DIR/tpch_sf10_certification.json"
TMP_DUCKDB_OUT="$RAW_DIR/.tpch_sf10-duckdb.$$.tmp.json"
TMP_ULTRA_OUT="$RAW_DIR/.tpch_sf10-ultrasql.$$.tmp.json"

mkdir -p "$RAW_DIR"

cleanup() {
    rm -f "$TMP_DUCKDB_OUT" "$TMP_ULTRA_OUT"
}
trap cleanup EXIT

write_setup_summary() {
    local reason="$1"
    local detail="$2"
    python3 - "$SUMMARY_OUT" "$reason" "$detail" "$TPCH_DATA_DIR" \
        "$DUCKDB_BIN" "$DUCKDB_OUT" "$ULTRA_OUT" <<'PY'
import json
import pathlib
import sys

summary_path, reason, detail, data_dir, duckdb_bin, duckdb_out, ultra_out = sys.argv[1:]
doc = {
    "schema_version": 1,
    "workload": "tpch_sf10",
    "scale_factor": 10,
    "target": "UltraSQL geometric mean <= DuckDB geometric mean across all 22 TPC-H queries",
    "passed": False,
    "status": "not_available",
    "reason": reason,
    "detail": detail,
    "data_dir": data_dir,
    "duckdb_bin": duckdb_bin or None,
    "duckdb_result": duckdb_out,
    "ultrasql_result": ultra_out,
    "next_step": (
        "Provide real SF10 .tbl data and a DuckDB binary, then rerun "
        "benchmarks/tpch_sf10_certify.sh. Synthetic data is smoke-only and "
        "not a TPC-H certification."
    ),
    "policy": "TPC-H SF10 artifacts are only moved into place after the full q1..q22 query set is present.",
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
}

if [[ "$QUERIES" != "all" ]]; then
    write_setup_summary \
        "partial_query_set_refused" \
        "TPC-H SF10 certification writes artifacts only for the full q1..q22 set."
    echo "TPC-H SF10 certification requires TPCH_QUERIES=all, got: $QUERIES" >&2
    exit 2
fi

if [[ ! -d "$TPCH_DATA_DIR" ]]; then
    write_setup_summary "data_dir_missing" "TPC-H SF10 data directory is missing."
    echo "TPC-H data dir missing: $TPCH_DATA_DIR" >&2
    echo "Run: target/release/tpch gen-data 10 $TPCH_DATA_DIR" >&2
    exit 2
fi

run_with_watchdog() {
    local label="$1"
    shift
    local timeout="$TPCH_TIMEOUT_SECONDS"

    if [[ "$timeout" == "0" ]]; then
        "$@"
        return $?
    fi

    "$@" &
    local pid=$!
    local started
    started="$(date +%s)"

    while kill -0 "$pid" 2>/dev/null; do
        local now
        now="$(date +%s)"
        if (( now - started >= timeout )); then
            echo "$label exceeded TPCH_TIMEOUT_SECONDS=$timeout; killing run." >&2
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            return 124
        fi
        sleep 5
    done

    wait "$pid"
}

if [[ -z "$DUCKDB_BIN" || ! -x "$DUCKDB_BIN" ]]; then
    write_setup_summary "duckdb_missing" "DuckDB binary missing or not executable."
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
DEFAULT_POOL_FRAMES=262144
if [[ "$ULTRASQL_SPILL_BACKING" != "memory" && "$HOST_RAM_BYTES" =~ ^[0-9]+$ && "$HOST_RAM_BYTES" -gt 0 ]]; then
    DEFAULT_POOL_FRAMES=$((HOST_RAM_BYTES / 2 / PAGE_BYTES))
    if [[ "$DEFAULT_POOL_FRAMES" -lt 262144 ]]; then
        DEFAULT_POOL_FRAMES=262144
    fi
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
    "schema_version": 1,
    "workload": "tpch_sf10",
    "scale_factor": 10,
    "target": "UltraSQL geometric mean <= DuckDB geometric mean across all 22 queries",
    "passed": False,
    "status": "failed",
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
            "Inspect ultrasql_log_tail and rerun with ULTRASQL_TPCH_PROGRESS=1; "
            "benchmark certification remains open until UltraSQL completes all "
            "22 queries and the geometric mean is no higher than DuckDB."
    ),
    "policy": "TPC-H SF10 raw artifacts are not published when either engine fails before q1..q22 complete.",
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
}

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --features sql-bench --bin tpch

rm -f "$DUCKDB_OUT" "$ULTRA_OUT" "$TMP_DUCKDB_OUT" "$TMP_ULTRA_OUT"

echo "Running DuckDB TPC-H SF10: queries=$QUERIES runs=$RUNS warmup=$WARMUP"
if ! run_with_watchdog \
    "DuckDB TPC-H SF10" \
    target/release/tpch run-queries duckdb \
        --duckdb "$DUCKDB_BIN" \
        --data-dir "$TPCH_DATA_DIR" \
        --runs "$RUNS" \
        --warmup "$WARMUP" \
        --queries "$QUERIES" \
        --scale 10 \
        --out "$TMP_DUCKDB_OUT"; then
    write_setup_summary "duckdb_run_failed" "DuckDB did not complete all TPC-H SF10 queries."
    exit 1
fi

echo "Running UltraSQL TPC-H SF10: queries=$QUERIES runs=$RUNS warmup=$WARMUP"
run_ultrasql() {
    local pool_frames="${ULTRASQL_TPCH_POOL_FRAMES:-$DEFAULT_POOL_FRAMES}"
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
        echo "UltraSQL SF10 attempt: pool_frames=$pool_frames progress=$ULTRASQL_PROGRESS spill_backing=$ULTRASQL_SPILL_BACKING" >&2
        if run_with_watchdog \
            "UltraSQL TPC-H SF10" \
            env \
                ULTRASQL_TPCH_POOL_FRAMES="$pool_frames" \
                ULTRASQL_TPCH_PROGRESS="$ULTRASQL_PROGRESS" \
                ULTRASQL_PAGE_SPILL_BACKING="$ULTRASQL_SPILL_BACKING" \
            target/release/tpch run-queries ultrasql \
                --data-dir "$TPCH_DATA_DIR" \
                --runs "$RUNS" \
                --warmup "$WARMUP" \
                --queries "$QUERIES" \
                --scale 10 \
                --out "$TMP_ULTRA_OUT" >"$log" 2>&1; then
            cat "$log"
            rm -f "$log"
            return 0
        fi
        cat "$log" >&2

        if [ "$AUTO_RETRY_POOL" = "1" ] && [ "$attempt" -lt "$POOL_RETRIES" ] &&
            grep -q "buffer pool exhausted: every frame is pinned" "$log"; then
            attempt=$((attempt + 1))
            pool_frames=$((pool_frames * 2))
            echo "UltraSQL SF10 load hit buffer pool exhaustion. Retrying with ULTRASQL_TPCH_POOL_FRAMES=$pool_frames (attempt $attempt/$POOL_RETRIES)." >&2
            rm -f "$log"
            continue
        fi

        if grep -q "buffer pool exhausted: every frame is pinned" "$log"; then
            write_ultrasql_failure_summary "buffer_pool_exhausted" "$pool_frames" "$log"
        else
            write_ultrasql_failure_summary "ultrasql_run_failed" "$pool_frames" "$log"
        fi
        rm -f "$log"
        return 1
    done
}

run_ultrasql

set +e
python3 - "$TMP_DUCKDB_OUT" "$TMP_ULTRA_OUT" "$DUCKDB_OUT" "$ULTRA_OUT" "$SUMMARY_OUT" <<'PY'
import json
import math
import pathlib
import sys

duckdb_read_path, ultrasql_read_path, duckdb_final_path, ultrasql_final_path, out_path = map(pathlib.Path, sys.argv[1:])
duckdb = json.loads(duckdb_read_path.read_text())
ultrasql = json.loads(ultrasql_read_path.read_text())

expected_queries = {f"q{i}" for i in range(1, 23)}
duckdb_queries = set(duckdb.get("queries", {}))
ultrasql_queries = set(ultrasql.get("queries", {}))

base_summary = {
    "schema_version": 1,
    "workload": "tpch_sf10",
    "scale_factor": 10,
    "target": "UltraSQL geometric mean <= DuckDB geometric mean across all 22 TPC-H queries",
    "expected_query_count": len(expected_queries),
    "duckdb_query_count": len(duckdb_queries),
    "ultrasql_query_count": len(ultrasql_queries),
    "duckdb_queries": sorted(duckdb_queries, key=lambda q: int(q[1:]) if q.startswith("q") and q[1:].isdigit() else q),
    "ultrasql_queries": sorted(ultrasql_queries, key=lambda q: int(q[1:]) if q.startswith("q") and q[1:].isdigit() else q),
    "missing_duckdb_queries": sorted(expected_queries - duckdb_queries, key=lambda q: int(q[1:])),
    "missing_ultrasql_queries": sorted(expected_queries - ultrasql_queries, key=lambda q: int(q[1:])),
    "extra_duckdb_queries": sorted(duckdb_queries - expected_queries),
    "extra_ultrasql_queries": sorted(ultrasql_queries - expected_queries),
    "duckdb_result": str(duckdb_final_path),
    "ultrasql_result": str(ultrasql_final_path),
    "policy": "TPC-H SF10 certification publishes complete q1..q22 raw artifacts only; no partial query set is retained.",
}

if duckdb_queries != expected_queries or ultrasql_queries != expected_queries:
    summary = {
        **base_summary,
        "duckdb_geomean_ms": None,
        "ultrasql_geomean_ms": None,
        "passed": False,
        "status": "failed",
        "reason": "incomplete_query_set",
        "next_step": (
            "Rerun benchmarks/tpch_sf10_certify.sh with TPCH_QUERIES=all; "
            "certification remains open until both raw artifacts contain q1..q22."
        ),
    }
    out_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    sys.exit(3)

def gm(doc):
    vals = []
    for query_id in sorted(expected_queries, key=lambda q: int(q[1:])):
        timing = doc["queries"][query_id]
        median_ms = timing.get("median_ms")
        if not median_ms or not math.isfinite(median_ms) or median_ms <= 0:
            return None
        vals.append(median_ms)
    return math.exp(sum(math.log(v) for v in vals) / len(vals))

duckdb_gm = gm(duckdb)
ultrasql_gm = gm(ultrasql)
passed = (
    duckdb_gm is not None
    and ultrasql_gm is not None
    and ultrasql_gm <= duckdb_gm
)
summary = {
    **base_summary,
    "duckdb_geomean_ms": duckdb_gm,
    "ultrasql_geomean_ms": ultrasql_gm,
    "passed": passed,
    "status": "passed" if passed else "failed",
    "reason": None if passed else "performance_target_missed_or_query_failed",
}
out_path.write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
sys.exit(0 if passed else 1)
PY
summary_status=$?
set -e

if [[ "$summary_status" != "3" ]]; then
    mv "$TMP_DUCKDB_OUT" "$DUCKDB_OUT"
    mv "$TMP_ULTRA_OUT" "$ULTRA_OUT"
fi
exit "$summary_status"
