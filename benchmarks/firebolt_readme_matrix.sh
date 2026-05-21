#!/usr/bin/env bash
# Firebolt Core rows for the generic README benchmark matrix.
#
# This runner measures the same SQL shapes used by the README tables against
# local Firebolt Core Docker only. It writes raw `*-firebolt.json` artifacts
# under `benchmarks/results/latest/raw/`; `readme-render` appends measured
# rows into the existing generic tables.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${FIREBOLT_README_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
ITERS="${FIREBOLT_README_ITERS:-32}"
WARMUP="${FIREBOLT_README_WARMUP:-1}"
MIXED_WINDOW_SECS="${FIREBOLT_README_MIXED_WINDOW_SECS:-1.0}"
FIREBOLT_CORE_ENDPOINT="${FIREBOLT_CORE_ENDPOINT:-http://127.0.0.1:3473}"
FIREBOLT_CORE_IMAGE="${FIREBOLT_CORE_IMAGE:-ghcr.io/firebolt-db/firebolt-core:preview-rc}"
FIREBOLT_CORE_HELPER="${FIREBOLT_CORE_HELPER:-benchmarks/firebolt_core_local.sh}"

mkdir -p "$RAW_DIR"

emit_not_available() {
    local workload="$1"
    local rows="$2"
    local reason="$3"
    local out="$RAW_DIR/${workload}-firebolt.json"

    python3 - "$out" "$workload" "$rows" "$ITERS" "$WARMUP" "$reason" "$FIREBOLT_CORE_IMAGE" <<'PY'
import json
import pathlib
import sys
import time

out, workload, rows, iters, warmup, reason, image = sys.argv[1:]
doc = {
    "schema_version": 1,
    "engine": "firebolt",
    "workload": workload,
    "n_rows": int(rows),
    "samples": 0,
    "iters": int(iters),
    "warmup": int(warmup),
    "median_us": None,
    "min_us": None,
    "iterations_us": [],
    "status": "not_available",
    "reason": reason,
    "docker_image": image,
    "core_mode": "local_docker",
    "generated_at_unix": int(time.time()),
    "policy": "README Firebolt rows require measured local Firebolt Core artifacts for the same SQL shape.",
}
pathlib.Path(out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out)
PY
}

if (( ITERS <= 0 || WARMUP < 0 )); then
    echo "firebolt_readme_matrix.sh: invalid FIREBOLT_README_ITERS/FIREBOLT_README_WARMUP" >&2
    exit 2
fi

if ! FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" "$FIREBOLT_CORE_HELPER" wait >/dev/null; then
    for spec in \
        "select_sum_65k_i64 65536" \
        "filter_sum_1m_i64 1000000" \
        "select_avg_1m_i64 1000000" \
        "insert_throughput_10k 10000" \
        "select_scan_10k 10000" \
        "update_throughput_10k 10000" \
        "delete_throughput_10k 10000" \
        "mixed_oltp_pgbench_like 10000" \
        "window_row_number_65k_i64 65536"; do
        emit_not_available ${spec} "firebolt_core_unavailable"
    done
    exit 2
fi

FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" \
FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" \
FIREBOLT_README_ITERS="$ITERS" \
FIREBOLT_README_WARMUP="$WARMUP" \
FIREBOLT_README_MIXED_WINDOW_SECS="$MIXED_WINDOW_SECS" \
FIREBOLT_README_RAW_DIR="$RAW_DIR" \
python3 <<'PY'
import json
import math
import os
import pathlib
import platform
import random
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request


endpoint = os.environ["FIREBOLT_CORE_ENDPOINT"]
raw_dir = pathlib.Path(os.environ["FIREBOLT_README_RAW_DIR"])
iters = int(os.environ["FIREBOLT_README_ITERS"])
warmup = int(os.environ["FIREBOLT_README_WARMUP"])
mixed_window_secs = float(os.environ["FIREBOLT_README_MIXED_WINDOW_SECS"])
timeout = float(os.environ.get("FIREBOLT_TIMEOUT_SECS", "120"))
run_id = f"{int(time.time())}_{os.getpid()}"


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


def drop_table(table: str):
    request(f"DROP TABLE IF EXISTS {table}")


def ctas(table: str, rows: int, projection: str):
    drop_table(table)
    request(
        f"CREATE TABLE {table} AS SELECT {projection} "
        f"FROM generate_series(0, {rows - 1}) AS t(i)"
    )


def measure_sql(sql_factory, count_rows=None):
    samples = []
    row_count = None
    for iteration in range(warmup + iters):
        sql = sql_factory(iteration)
        started = time.perf_counter()
        doc = request(sql)
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        if iteration >= warmup:
            samples.append(elapsed_us)
            if count_rows is True:
                row_count = int(doc.get("rows", 0) or 0)
    return samples, row_count


def write_artifact(workload, rows, samples, query, result_row_count=None, detail=None):
    try:
        version_doc = request("SELECT version()")
        version_rows = version_doc.get("data", [])
        firebolt_version = str(version_rows[0][0]) if version_rows and version_rows[0] else None
    except Exception:
        firebolt_version = None
    if not samples:
        raise RuntimeError(f"{workload}: no measured samples")
    doc = {
        "schema_version": 1,
        "engine": "firebolt",
        "workload": workload,
        "n_rows": rows,
        "samples": len(samples),
        "iters": iters,
        "warmup": warmup,
        "median_us": statistics.median(samples),
        "min_us": min(samples),
        "p95_us": percentile_nearest_rank(samples, 0.95),
        "iterations_us": samples,
        "result_row_count": result_row_count,
        "query": query,
        "status": "measured",
        "docker_image": os.environ["FIREBOLT_CORE_IMAGE"],
        "firebolt_version": firebolt_version,
        "core_mode": "local_docker",
        "local_docker": True,
        "host_cpu": os.environ.get("ULTRASQL_HOST_CPU") or platform.processor() or platform.machine(),
        "host_memory": host_memory_bytes(),
        "generated_at_unix": int(time.time()),
        "detail": detail,
        "policy": "README Firebolt row is eligible only from local Firebolt Core on the same SQL shape.",
    }
    out = raw_dir / f"{workload}-firebolt.json"
    out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(out)


def write_not_available(workload, rows, reason, detail=None):
    doc = {
        "schema_version": 1,
        "engine": "firebolt",
        "workload": workload,
        "n_rows": rows,
        "samples": 0,
        "iters": iters,
        "warmup": warmup,
        "median_us": None,
        "min_us": None,
        "iterations_us": [],
        "status": "not_available",
        "reason": reason,
        "detail": detail,
        "docker_image": os.environ["FIREBOLT_CORE_IMAGE"],
        "core_mode": "local_docker",
        "generated_at_unix": int(time.time()),
        "policy": "README Firebolt rows require measured local Firebolt Core artifacts for the same SQL shape.",
    }
    out = raw_dir / f"{workload}-firebolt.json"
    out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(out)


def run_select_sum():
    workload = "select_sum_65k_i64"
    rows = 65_536
    table = f"ultrasql_readme_sum_{run_id}"
    query = f"SELECT SUM(x) FROM {table}"
    ctas(table, rows, "i AS id, i AS x")
    try:
        samples, result_rows = measure_sql(lambda _i: query, count_rows=True)
        write_artifact(workload, rows, samples, query, result_rows)
    finally:
        drop_table(table)


def run_filter_sum():
    workload = "filter_sum_1m_i64"
    rows = 1_000_000
    table = f"ultrasql_readme_filter_{run_id}"
    query = f"SELECT SUM(x) FROM {table} WHERE x > 500000"
    ctas(table, rows, "i AS id, i AS x")
    try:
        samples, result_rows = measure_sql(lambda _i: query, count_rows=True)
        write_artifact(workload, rows, samples, query, result_rows)
    finally:
        drop_table(table)


def run_avg():
    workload = "select_avg_1m_i64"
    rows = 1_000_000
    table = f"ultrasql_readme_avg_{run_id}"
    query = f"SELECT AVG(x) FROM {table}"
    ctas(table, rows, "i AS id, i AS x")
    try:
        samples, result_rows = measure_sql(lambda _i: query, count_rows=True)
        write_artifact(workload, rows, samples, query, result_rows)
    finally:
        drop_table(table)


def run_insert():
    workload = "insert_throughput_10k"
    rows = 10_000
    query_template = "INSERT INTO {table} SELECT i AS id, (i * 17) AS val FROM generate_series(0, 9999) AS t(i)"
    samples = []
    for iteration in range(warmup + iters):
        table = f"ultrasql_readme_insert_{run_id}_{iteration}"
        drop_table(table)
        request(f"CREATE TABLE {table} (id INT, val INT)")
        query = query_template.format(table=table)
        started = time.perf_counter()
        request(query)
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        drop_table(table)
        if iteration >= warmup:
            samples.append(elapsed_us)
    write_artifact(workload, rows, samples, query_template, None)


def run_select_scan():
    workload = "select_scan_10k"
    rows = 10_000
    table = f"ultrasql_readme_scan_{run_id}"
    query = f"SELECT id, val FROM {table}"
    ctas(table, rows, "i AS id, (i * 17) AS val")
    try:
        samples, result_rows = measure_sql(lambda _i: query, count_rows=True)
        write_artifact(workload, rows, samples, query, result_rows)
    finally:
        drop_table(table)


def run_update():
    workload = "update_throughput_10k"
    rows = 10_000
    query_template = "UPDATE {table} SET val = val + 1"
    samples = []
    for iteration in range(warmup + iters):
        table = f"ultrasql_readme_update_{run_id}_{iteration}"
        ctas(table, rows, "i AS id, i AS val")
        query = query_template.format(table=table)
        started = time.perf_counter()
        request(query)
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        drop_table(table)
        if iteration >= warmup:
            samples.append(elapsed_us)
    write_artifact(workload, rows, samples, query_template, None)


def run_delete():
    workload = "delete_throughput_10k"
    rows = 10_000
    query_template = "DELETE FROM {table}"
    samples = []
    for iteration in range(warmup + iters):
        table = f"ultrasql_readme_delete_{run_id}_{iteration}"
        ctas(table, rows, "i AS id, i AS val")
        query = query_template.format(table=table)
        started = time.perf_counter()
        request(query)
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        drop_table(table)
        if iteration >= warmup:
            samples.append(elapsed_us)
    write_artifact(workload, rows, samples, query_template, None)


def run_mixed():
    workload = "mixed_oltp_pgbench_like"
    rows = 10_000
    samples = []
    op_counts = []
    for iteration in range(warmup + iters):
        table = f"ultrasql_readme_mixed_{run_id}_{iteration}"
        ctas(table, rows, "i AS id, i AS val")
        rng = random.Random(0xDEAD + iteration)
        next_id = rows
        started = time.perf_counter()
        deadline = started + mixed_window_secs
        count = 0
        while time.perf_counter() < deadline:
            r = rng.random()
            if r < 0.50:
                row_id = rng.randrange(rows)
                request(f"SELECT val FROM {table} WHERE id = {row_id}")
            elif r < 0.80:
                row_id = rng.randrange(rows)
                request(f"UPDATE {table} SET val = val + 1 WHERE id = {row_id}")
            else:
                value = rng.randint(-(2**31), 2**31 - 1)
                request(f"INSERT INTO {table} (id, val) VALUES ({next_id}, {value})")
                next_id += 1
            count += 1
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        drop_table(table)
        if iteration >= warmup:
            samples.append(elapsed_us / max(count, 1))
            op_counts.append(count)
    write_artifact(
        workload,
        rows,
        samples,
        "50% point SELECT, 30% point UPDATE, 20% INSERT over a 1-second window",
        None,
        {"op_counts": op_counts, "window_secs": mixed_window_secs},
    )


def run_window():
    workload = "window_row_number_65k_i64"
    rows = 65_536
    table = f"ultrasql_readme_window_{run_id}"
    query = f"SELECT x, ROW_NUMBER() OVER (ORDER BY x) FROM {table}"
    ctas(table, rows, "i AS x")
    try:
        samples, result_rows = measure_sql(lambda _i: query, count_rows=True)
        write_artifact(workload, rows, samples, query, result_rows)
    finally:
        drop_table(table)


for runner in [
    run_select_sum,
    run_filter_sum,
    run_avg,
    run_insert,
    run_select_scan,
    run_update,
    run_delete,
    run_mixed,
    run_window,
]:
    workload_name = runner.__name__.removeprefix("run_")
    try:
        runner()
    except Exception as exc:
        workload_map = {
            "select_sum": ("select_sum_65k_i64", 65_536),
            "filter_sum": ("filter_sum_1m_i64", 1_000_000),
            "avg": ("select_avg_1m_i64", 1_000_000),
            "insert": ("insert_throughput_10k", 10_000),
            "select_scan": ("select_scan_10k", 10_000),
            "update": ("update_throughput_10k", 10_000),
            "delete": ("delete_throughput_10k", 10_000),
            "mixed": ("mixed_oltp_pgbench_like", 10_000),
            "window": ("window_row_number_65k_i64", 65_536),
        }
        workload, rows = workload_map[workload_name]
        write_not_available(workload, rows, "firebolt_readme_workload_failed", str(exc))
PY
