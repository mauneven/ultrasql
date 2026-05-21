#!/usr/bin/env bash
# Late-materialization smoke/full runner.
#
# Measures UltraSQL's wide fact-table payload projection behind a selective
# indexed filter. The raw artifact is only valid when EXPLAIN ANALYZE reports
# Late Materialization counters.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

profile="${1:-smoke}"
OUT_DIR="${LATE_MAT_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
ENGINES="${LATE_MAT_ENGINES:-ultrasql-late,ultrasql-eager,duckdb,clickhouse,firebolt}"
FIREBOLT_CORE_ENDPOINT="${FIREBOLT_CORE_ENDPOINT:-http://127.0.0.1:3473}"
FIREBOLT_CORE_IMAGE="${FIREBOLT_CORE_IMAGE:-ghcr.io/firebolt-db/firebolt-core:preview-rc}"
FIREBOLT_CORE_HELPER="${FIREBOLT_CORE_HELPER:-benchmarks/firebolt_core_local.sh}"

case "$profile" in
    smoke)
        rows="${LATE_MAT_ROWS:-10000}"
        warmup="${LATE_MAT_WARMUP:-0}"
        iters="${LATE_MAT_ITERS:-1}"
        ;;
    full)
        rows="${LATE_MAT_ROWS:-1000000}"
        warmup="${LATE_MAT_WARMUP:-2}"
        iters="${LATE_MAT_ITERS:-5}"
        ;;
    *)
        echo "late_materialization.sh: profile must be smoke or full, got '$profile'" >&2
        exit 2
        ;;
esac

mkdir -p "$RAW_DIR"
cargo build --release -p ultrasql-bench --features sql-bench --bin cross_compare_sql

if (( rows >= 1000000 && rows % 1000000 == 0 )); then
    row_label="$((rows / 1000000))m"
elif (( rows >= 1000 && rows % 1000 == 0 )); then
    row_label="$((rows / 1000))k"
else
    row_label="$rows"
fi

emit_not_available() {
    local engine="$1"
    local reason="$2"
    local out="$RAW_DIR/late_materialization_${row_label}-${engine}.json"
    python3 - "$out" "$engine" "$rows" "$row_label" "$warmup" "$iters" "$reason" <<'PY'
import json
import os
import pathlib
import platform
import sys
import time

out, engine, rows, row_label, warmup, iters, reason = sys.argv[1:]

def host_memory_bytes():
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if isinstance(pages, int) and isinstance(page_size, int):
            return pages * page_size
    except (AttributeError, OSError, ValueError):
        return None
    return None

is_firebolt = engine == "firebolt"
doc = {
    "schema_version": 1,
    "suite": "late_materialization",
    "engine": engine,
    "workload": f"late_materialization_{row_label}",
    "wide_columns": 100,
    "projected_columns": ["amount", "pad003", "pad096"],
    "dataset_rows": int(rows),
    "warmup": int(warmup),
    "iters": int(iters),
    "status": "not_available",
    "reason": reason,
    "median_us": None,
    "p95_us": None,
    "samples": 0,
    "docker_image": os.environ.get("FIREBOLT_CORE_IMAGE") if is_firebolt else None,
    "firebolt_version": None,
    "core_mode": "local_docker" if is_firebolt else None,
    "local_docker": is_firebolt,
    "host_cpu": os.environ.get("ULTRASQL_HOST_CPU") or platform.processor() or platform.machine(),
    "host_memory": host_memory_bytes(),
    "generated_at_unix": int(time.time()),
    "policy": "No late-materialization competitor claim exists without measured raw samples.",
}
pathlib.Path(out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out)
PY
}

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

run_ultrasql() {
    target/release/cross_compare_sql \
        --workload late-materialization \
        --rows "$rows" \
        --warmup "$warmup" \
        --iters "$iters" \
        --output "$RAW_DIR/late_materialization_${row_label}-ultrasql.json"
}

run_duckdb() {
    local out="$RAW_DIR/late_materialization_${row_label}-duckdb.json"
    python3 - "$out" "$rows" "$row_label" "$warmup" "$iters" <<'PY'
import json
import math
import pathlib
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

out, rows, row_label, warmup, iters = sys.argv[1:]
rows = int(rows)
warmup = int(warmup)
iters = int(iters)
duckdb = shutil.which("duckdb")
if duckdb is None:
    raise SystemExit("duckdb missing")

def percentile_nearest_rank(values, percentile):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * percentile) - 1))
    return ordered[index]

def run(db, sql, csv=False):
    args = [duckdb, db]
    if csv:
        args.append("-csv")
    args.extend(["-c", sql])
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout

pad_exprs = []
for idx in range(1, 97):
    pad_exprs.append(f"'p{idx}_' || (row_id % {idx + 17})::VARCHAR AS pad{idx:03}")
setup_sql = (
    "CREATE TABLE late_mat AS SELECT "
    "row_id::INT AS id, "
    "(row_id % 32)::INT AS tenant_id, "
    "((row_id // 32) % 128)::INT AS bucket, "
    "((row_id * 19) % 2000 - 1000)::BIGINT AS amount, "
    + ", ".join(pad_exprs)
    + f" FROM range({rows}) AS r(row_id);"
)
query = "SELECT amount, pad003, pad096 FROM late_mat WHERE tenant_id = 7"

with tempfile.TemporaryDirectory(prefix="ultrasql-late-mat-duckdb-") as tmp:
    db = str(pathlib.Path(tmp) / "bench.duckdb")
    started = time.perf_counter()
    run(db, setup_sql)
    load_time_us = (time.perf_counter() - started) * 1_000_000.0
    samples = []
    result_row_count = 0
    for iteration in range(warmup + iters):
        started = time.perf_counter()
        output = run(db, query, csv=True)
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        if iteration >= warmup:
            samples.append(elapsed_us)
            lines = [line for line in output.splitlines() if line.strip()]
            result_row_count = max(0, len(lines) - 1)

version = subprocess.run([duckdb, "--version"], check=True, text=True, capture_output=True).stdout.strip()
doc = {
    "schema_version": 1,
    "suite": "late_materialization",
    "engine": "duckdb",
    "workload": f"late_materialization_{row_label}",
    "wide_columns": 100,
    "projected_columns": ["amount", "pad003", "pad096"],
    "dataset_rows": rows,
    "warmup": warmup,
    "iters": iters,
    "samples": len(samples),
    "median_us": statistics.median(samples),
    "p95_us": percentile_nearest_rank(samples, 0.95),
    "iterations_us": samples,
    "load_time_us": load_time_us,
    "result_row_count": result_row_count,
    "duckdb_version": version,
    "status": "measured",
    "policy": "DuckDB late-materialization competitor artifact is a same-shape local CLI measurement; no claim without raw artifact.",
}
pathlib.Path(out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out)
PY
}

run_firebolt() {
    local out="$RAW_DIR/late_materialization_${row_label}-firebolt.json"
    if ! FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" "$FIREBOLT_CORE_HELPER" wait >/dev/null; then
        emit_not_available "firebolt" "$(firebolt_unavailable_reason)"
        return 0
    fi

    FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" \
    FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" \
    FIREBOLT_ROWS="$rows" \
    FIREBOLT_WARMUP="$warmup" \
    FIREBOLT_ITERS="$iters" \
    FIREBOLT_OUT="$out" \
    FIREBOLT_WORKLOAD="late_materialization_${row_label}" \
    FIREBOLT_LATE_PROFILE="$profile" \
    python3 <<'PY'
import json
import math
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
profile = os.environ.get("FIREBOLT_LATE_PROFILE", "smoke")
timeout = float(os.environ.get("FIREBOLT_TIMEOUT_SECS", "120"))
table = f"ultrasql_late_mat_{int(time.time())}_{os.getpid()}"
projected_columns = ["amount", "pad003", "pad096"]
wide_columns = 100


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


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def row_values(row_id: int) -> str:
    tenant_id = row_id % 32
    bucket = (row_id // 32) % 128
    amount = ((row_id * 19) % 2000) - 1000
    pads = [f"p{idx}_{row_id % (idx + 17)}" for idx in range(1, 97)]
    values = [
        str(row_id),
        str(tenant_id),
        str(bucket),
        str(amount),
        *(sql_string(pad) for pad in pads),
    ]
    return "(" + ",".join(values) + ")"


pad_ddl = ", ".join(f"pad{idx:03} TEXT" for idx in range(1, 97))
query = f"SELECT amount, pad003, pad096 FROM {table} WHERE tenant_id = 7"

try:
    request(
        f"CREATE TABLE {table} "
        f"(id INT, tenant_id INT, bucket INT, amount BIGINT, {pad_ddl})"
    )

    started = time.perf_counter()
    chunk_rows = int(os.environ.get("FIREBOLT_LATE_INSERT_CHUNK_ROWS", "250"))
    for start in range(0, rows, chunk_rows):
        end = min(start + chunk_rows, rows)
        values = ",".join(row_values(row_id) for row_id in range(start, end))
        request(f"INSERT INTO {table} VALUES {values}")
    load_time_us = (time.perf_counter() - started) * 1_000_000.0

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
        "suite": "late_materialization",
        "engine": "firebolt",
        "workload": workload,
        "profile": profile,
        "wide_columns": wide_columns,
        "projected_columns": projected_columns,
        "dataset_rows": rows,
        "n_rows": rows,
        "warmup": warmup,
        "iters": iters,
        "docker_image": os.environ["FIREBOLT_CORE_IMAGE"],
        "firebolt_version": firebolt_version,
        "core_mode": "local_docker",
        "local_docker": True,
        "host_cpu": os.environ.get("ULTRASQL_HOST_CPU") or platform.processor() or platform.machine(),
        "host_memory": host_memory_bytes(),
        "samples": len(iterations_us),
        "median_us": statistics.median(iterations_us),
        "p95_us": percentile_nearest_rank(iterations_us, 0.95),
        "min_us": min(iterations_us),
        "iterations_us": iterations_us,
        "load_time_us": load_time_us,
        "result_row_count": result_row_count,
        "query": query,
        "status": "measured",
        "policy": (
            "No Firebolt late-materialization comparison claim may be made "
            "unless this local Core artifact and the matching UltraSQL "
            "artifact are committed from the same host/profile."
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

ultrasql_ran=0
IFS=',' read -r -a REQUESTED_ENGINES <<<"$ENGINES"
for engine in "${REQUESTED_ENGINES[@]}"; do
    case "$engine" in
        ultrasql-late|ultrasql-eager|ultrasql)
            if (( ultrasql_ran == 0 )); then
                run_ultrasql
                ultrasql_ran=1
            fi
            ;;
        duckdb)
            if command -v duckdb >/dev/null 2>&1; then
                run_duckdb
            else
                emit_not_available duckdb "duckdb_unavailable"
            fi
            ;;
        clickhouse)
            if command -v clickhouse >/dev/null 2>&1 || command -v clickhouse-local >/dev/null 2>&1; then
                emit_not_available clickhouse "clickhouse_runner_pending"
            else
                emit_not_available clickhouse "clickhouse_unavailable"
            fi
            ;;
        firebolt)
            run_firebolt || emit_not_available firebolt "firebolt_late_materialization_failed" >/dev/null
            ;;
        *)
            emit_not_available "$engine" "unknown_engine"
            ;;
    esac
done
