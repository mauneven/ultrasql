#!/usr/bin/env bash
# Firebolt aggregating-index competitor benchmark.
#
# This runner targets Firebolt's documented dashboard/reporting strength:
# repeated filtered GROUP BY queries backed by CREATE AGGREGATING INDEX.
# It publishes raw artifacts only. Firebolt uses local Firebolt Core Docker
# through benchmarks/firebolt_core_local.sh; setup gaps record not_available.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${FIREBOLT_AGG_PROFILE:-${1:-smoke}}"
OUT_DIR="${FIREBOLT_AGG_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
MANIFEST="$OUT_DIR/firebolt_aggregate_index_manifest.json"
ENGINES="${FIREBOLT_AGG_ENGINES:-ultrasql,firebolt}"
FIREBOLT_CORE_ENDPOINT="${FIREBOLT_CORE_ENDPOINT:-http://127.0.0.1:3473}"
FIREBOLT_CORE_IMAGE="${FIREBOLT_CORE_IMAGE:-ghcr.io/firebolt-db/firebolt-core:preview-rc}"
FIREBOLT_CORE_HELPER="${FIREBOLT_CORE_HELPER:-benchmarks/firebolt_core_local.sh}"

case "$PROFILE" in
    smoke)
        ROWS="${FIREBOLT_AGG_ROWS:-10000}"
        WARMUP="${FIREBOLT_AGG_WARMUP:-1}"
        ITERS="${FIREBOLT_AGG_ITERS:-3}"
        ;;
    full)
        ROWS="${FIREBOLT_AGG_ROWS:-1000000}"
        WARMUP="${FIREBOLT_AGG_WARMUP:-2}"
        ITERS="${FIREBOLT_AGG_ITERS:-8}"
        ;;
    *)
        echo "firebolt_aggregate_index.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

mkdir -p "$RAW_DIR"

STATUS_FILE="$(mktemp)"
trap 'rm -f "$STATUS_FILE"' EXIT
failed=0
unavailable=0

row_label() {
    local rows="$1"
    if (( rows >= 1000000 && rows % 1000000 == 0 )); then
        echo "$(( rows / 1000000 ))m"
    elif (( rows >= 1000 && rows % 1000 == 0 )); then
        echo "$(( rows / 1000 ))k"
    else
        echo "$rows"
    fi
}

ROW_LABEL="$(row_label "$ROWS")"
WORKLOAD_ID="firebolt_aggregate_index_${ROW_LABEL}"

firebolt_unavailable_reason() {
    local status
    status="$(
        FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" \
        FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" \
            "$FIREBOLT_CORE_HELPER" status 2>/dev/null || true
    )"
    if [[ "$status" == *docker_unavailable* ]]; then
        echo "docker_unavailable"
    else
        echo "firebolt_core_unavailable"
    fi
}

record_engine() {
    local engine="$1"
    local status="$2"
    local code="$3"
    local artifact="$4"
    printf '%s\t%s\t%s\t%s\n' "$engine" "$status" "$code" "$artifact" >>"$STATUS_FILE"
}

emit_not_available() {
    local engine="$1"
    local reason="$2"
    local out="$RAW_DIR/${WORKLOAD_ID}-${engine}.json"

    python3 - "$out" "$engine" "$PROFILE" "$ROWS" "$WARMUP" "$ITERS" "$reason" "$WORKLOAD_ID" "$FIREBOLT_CORE_IMAGE" <<'PY'
import json
import math
import os
import pathlib
import platform
import sys
import time

out, engine, profile, rows, warmup, iters, reason, workload, docker_image = sys.argv[1:]

def host_memory_bytes():
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if isinstance(pages, int) and isinstance(page_size, int):
            return pages * page_size
    except (AttributeError, OSError, ValueError):
        return None
    return None

doc = {
    "schema_version": 1,
    "engine": engine,
    "workload": workload,
    "suite": "firebolt_aggregate_index",
    "profile": profile,
    "n_rows": int(rows),
    "warmup": int(warmup),
    "iters": int(iters),
    "docker_image": docker_image,
    "firebolt_version": None,
    "core_mode": "local_docker",
    "host_cpu": os.environ.get("ULTRASQL_HOST_CPU") or platform.processor() or platform.machine(),
    "host_memory": host_memory_bytes(),
    "dataset_rows": int(rows),
    "samples": 0,
    "median_us": None,
    "p95_us": None,
    "status": "not_available",
    "reason": reason,
    "generated_at_unix": int(time.time()),
    "required_shape": {
        "ddl": "CREATE AGGREGATING INDEX idx ON fact_events (tenant_id, bucket, SUM(amount), COUNT(*))",
        "query": (
            "SELECT tenant_id, bucket, SUM(amount), COUNT(*) "
            "FROM fact_events WHERE tenant_id = 7 "
            "GROUP BY tenant_id, bucket ORDER BY tenant_id, bucket"
        ),
        "firebolt_plan_marker": "Aggregating Index",
        "http_output_format": "output_format=JSON_Compact",
    },
    "policy": (
        "No Firebolt aggregate-index benchmark claim exists until a "
        "local Firebolt Core Docker run emits measured samples and an "
        "EXPLAIN artifact showing Aggregating Index usage."
    ),
}
pathlib.Path(out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out)
PY
}

run_ultrasql() {
    local out="$RAW_DIR/${WORKLOAD_ID}-ultrasql.json"
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --features sql-bench \
            --bin cross_compare_sql >/dev/null
    target/release/cross_compare_sql \
        --workload dashboard-aggregate \
        --rows "$ROWS" \
        --warmup "$WARMUP" \
        --iters "$ITERS" \
        --output "$out"
    echo "$out"
}

run_firebolt() {
    local out="$RAW_DIR/${WORKLOAD_ID}-firebolt.json"
    if ! FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" "$FIREBOLT_CORE_HELPER" wait >/dev/null; then
        emit_not_available "firebolt" "$(firebolt_unavailable_reason)"
        return 2
    fi

    FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" \
    FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" \
    FIREBOLT_ROWS="$ROWS" \
    FIREBOLT_WARMUP="$WARMUP" \
    FIREBOLT_ITERS="$ITERS" \
    FIREBOLT_OUT="$out" \
    FIREBOLT_WORKLOAD="$WORKLOAD_ID" \
    python3 <<'PY'
import json
import os
import pathlib
import platform
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request


endpoint = os.environ["FIREBOLT_CORE_ENDPOINT"]
rows = int(os.environ["FIREBOLT_ROWS"])
warmup = int(os.environ["FIREBOLT_WARMUP"])
iters = int(os.environ["FIREBOLT_ITERS"])
out = pathlib.Path(os.environ["FIREBOLT_OUT"])
workload = os.environ["FIREBOLT_WORKLOAD"]
timeout = float(os.environ.get("FIREBOLT_TIMEOUT_SECS", "120"))
table = f"ultrasql_firebolt_agg_{int(time.time())}_{os.getpid()}"
index = f"{table}_idx"


def host_memory_bytes():
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if isinstance(pages, int) and isinstance(page_size, int):
            return pages * page_size
    except (AttributeError, OSError, ValueError):
        return None
    return None


def percentile_nearest_rank(values, percentile):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * percentile) - 1))
    return ordered[index]


def formatted_endpoint() -> str:
    separator = "&" if urllib.parse.urlparse(endpoint).query else "?"
    return f"{endpoint}{separator}output_format=JSON_Compact"


def request(sql: str):
    headers = {"Content-Type": "text/plain; charset=utf-8"}
    req = urllib.request.Request(
        formatted_endpoint(),
        data=sql.encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Firebolt HTTP {exc.code}: {body}") from exc
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {"raw": body}


def row_values(row_id: int) -> str:
    tenant_id = row_id % 32
    bucket = (row_id // 32) % 64
    amount = ((row_id * 17) % 1000) - 500
    return f"({row_id},{tenant_id},{bucket},{amount})"


query = (
    f"SELECT tenant_id, bucket, SUM(amount), COUNT(*) FROM {table} "
    "WHERE tenant_id = 7 "
    "GROUP BY tenant_id, bucket "
    "ORDER BY tenant_id, bucket"
)
index_ddl = (
    f"CREATE AGGREGATING INDEX {index} ON {table} "
    "(tenant_id, bucket, SUM(amount), COUNT(*))"
)

try:
    request(f"CREATE TABLE {table} (id INT, tenant_id INT, bucket INT, amount BIGINT)")

    started = time.perf_counter()
    request(index_ddl)
    index_build_us = (time.perf_counter() - started) * 1_000_000.0

    started = time.perf_counter()
    chunk_rows = int(os.environ.get("FIREBOLT_INSERT_CHUNK_ROWS", "5000"))
    for start in range(0, rows, chunk_rows):
        end = min(start + chunk_rows, rows)
        values = ",".join(row_values(row_id) for row_id in range(start, end))
        request(f"INSERT INTO {table} VALUES {values}")
    load_time_us = (time.perf_counter() - started) * 1_000_000.0

    explain_doc = request(f"EXPLAIN {query}")
    explain_text = json.dumps(explain_doc, sort_keys=True)
    aggregating_index_used = "aggregating index" in explain_text.lower()
    if not aggregating_index_used:
        raise RuntimeError("Firebolt EXPLAIN did not report Aggregating Index")

    iterations_us = []
    result_row_count = 0
    for iteration in range(warmup + iters):
        started = time.perf_counter()
        result_doc = request(query)
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        if iteration >= warmup:
            iterations_us.append(elapsed_us)
            result_row_count = len(result_doc.get("data", []))

    if not iterations_us:
        raise RuntimeError("no measured Firebolt iterations")

    try:
        version_doc = request("SELECT version()")
        version_rows = version_doc.get("data", [])
        firebolt_version = str(version_rows[0][0]) if version_rows and version_rows[0] else None
    except Exception:
        firebolt_version = None

    doc = {
        "schema_version": 1,
        "engine": "firebolt",
        "workload": workload,
        "suite": "firebolt_aggregate_index",
        "profile": os.environ.get("FIREBOLT_AGG_PROFILE", "smoke"),
        "n_rows": rows,
        "docker_image": os.environ["FIREBOLT_CORE_IMAGE"],
        "firebolt_version": firebolt_version,
        "core_mode": "local_docker",
        "host_cpu": os.environ.get("ULTRASQL_HOST_CPU") or platform.processor() or platform.machine(),
        "host_memory": host_memory_bytes(),
        "dataset_rows": rows,
        "samples": len(iterations_us),
        "median_us": statistics.median(iterations_us),
        "p95_us": percentile_nearest_rank(iterations_us, 0.95),
        "min_us": min(iterations_us),
        "iterations_us": iterations_us,
        "load_time_us": load_time_us,
        "index_build_us": index_build_us,
        "result_row_count": result_row_count,
        "query": query,
        "index_ddl": index_ddl,
        "explain_contains": "Aggregating Index",
        "aggregating_index_used": aggregating_index_used,
        "http_output_format": "output_format=JSON_Compact",
        "status": "measured",
        "policy": (
            "No Firebolt aggregate-index benchmark claim may be made unless "
            "this raw artifact is committed with its runner inputs."
        ),
    }
    out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(out)
finally:
    try:
        request(f"DROP TABLE IF EXISTS {table}")
    except Exception:
        pass
PY
}

run_engine() {
    local engine="$1"
    local artifact
    local code

    set +e
    case "$engine" in
        ultrasql)
            artifact="$(run_ultrasql)"
            code=$?
            ;;
        firebolt)
            artifact="$(run_firebolt)"
            code=$?
            ;;
        *)
            artifact="$(emit_not_available "$engine" "runner_not_implemented_for_engine")"
            code=2
            ;;
    esac
    set -e

    case "$code" in
        0)
            record_engine "$engine" "measured" "$code" "$artifact"
            ;;
        2)
            record_engine "$engine" "unavailable" "$code" "$artifact"
            unavailable=1
            ;;
        *)
            record_engine "$engine" "failed" "$code" "$artifact"
            failed=1
            ;;
    esac
}

IFS=',' read -r -a REQUESTED_ENGINES <<<"$ENGINES"
for engine in "${REQUESTED_ENGINES[@]}"; do
    run_engine "$engine"
done

python3 - "$PROFILE" "$ENGINES" "$ROWS" "$WARMUP" "$ITERS" "$MANIFEST" "$STATUS_FILE" <<'PY'
import json
import pathlib
import sys
import time

profile, engines, rows, warmup, iters, manifest_path, status_path = sys.argv[1:]
entries = []
for line in pathlib.Path(status_path).read_text(encoding="utf-8").splitlines():
    engine, status, code, artifact = line.split("\t")
    entries.append(
        {
            "engine": engine,
            "status": status,
            "exit_code": int(code),
            "artifact": artifact,
        }
    )

has_failed = any(entry["status"] == "failed" for entry in entries)
has_unavailable = any(entry["status"] == "unavailable" for entry in entries)
doc = {
    "schema_version": 1,
    "suite": "firebolt_aggregate_index",
    "profile": profile,
    "requested_engines": [engine for engine in engines.split(",") if engine],
    "n_rows": int(rows),
    "warmup": int(warmup),
    "iters": int(iters),
    "generated_at_unix": int(time.time()),
    "status": "failed" if has_failed else "partial" if has_unavailable else "passed",
    "passed": not has_failed and not has_unavailable,
    "engines": entries,
    "policy": (
        "No Firebolt aggregate-index benchmark claim exists unless Firebolt "
        "has a measured local Core artifact and EXPLAIN shows "
        "Aggregating Index usage. Missing local Core is recorded as "
        "not_available, not as a result."
    ),
}
pathlib.Path(manifest_path).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(json.dumps(doc, indent=2))
PY

if (( failed != 0 )); then
    exit 1
fi
if (( unavailable != 0 )); then
    exit 2
fi
exit 0
