#!/usr/bin/env bash
# Reproducible ClickBench certification runner for PostgreSQL-wire engines.
#
# The official ClickBench SQL/schema are downloaded at a pinned upstream
# commit to avoid vendoring CC BY-NC-SA benchmark files into this repository.
# The dataset is not downloaded unless CLICKBENCH_DOWNLOAD=1 is set.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

CLICKBENCH_REF="${CLICKBENCH_REF:-c5eb5d8e9c10fbef5ce16e8750f3e67de24cef0a}"
CLICKBENCH_WORK="${CLICKBENCH_WORK:-target/clickbench}"
CLICKBENCH_TSV="${CLICKBENCH_TSV:-$CLICKBENCH_WORK/hits.tsv}"
POSTGRES_DSN="${POSTGRES_DSN:-}"
ULTRASQL_DSN="${ULTRASQL_DSN:-}"
CLICKBENCH_ENGINES="${CLICKBENCH_ENGINES:-postgres,ultrasql,duckdb,clickhouse,firebolt}"
DUCKDB_BIN="${CLICKBENCH_DUCKDB:-$(command -v duckdb || true)}"
RUNS="${CLICKBENCH_RUNS:-3}"
ALLOW_PARTIAL="${CLICKBENCH_ALLOW_PARTIAL:-0}"
OUT_DIR="${BENCH_CERT_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
SUMMARY_OUT="$OUT_DIR/clickbench_certification.json"
POSTGRES_OUT="$RAW_DIR/clickbench-postgres17.json"
ULTRA_OUT="$RAW_DIR/clickbench-ultrasql.json"
DUCKDB_OUT="$RAW_DIR/clickbench-duckdb.json"
CLICKHOUSE_OUT="$RAW_DIR/clickbench-clickhouse.json"
FIREBOLT_OUT="$RAW_DIR/clickbench-firebolt.json"

mkdir -p "$CLICKBENCH_WORK" "$OUT_DIR" "$RAW_DIR"

base="https://raw.githubusercontent.com/ClickHouse/ClickBench/$CLICKBENCH_REF/postgresql"
schema="$CLICKBENCH_WORK/create.sql"
queries="$CLICKBENCH_WORK/queries.sql"

emit_not_available_artifacts() {
    local reason="$1"
    local detail="$2"
    python3 - "$RAW_DIR" "$CLICKBENCH_ENGINES" "$CLICKBENCH_REF" "$CLICKBENCH_TSV" \
        "$reason" "$detail" <<'PY'
import json
import os
import pathlib
import platform
import subprocess
import sys
import time

raw_dir, engines_raw, ref, dataset, reason, detail = sys.argv[1:]
raw_dir = pathlib.Path(raw_dir)
raw_dir.mkdir(parents=True, exist_ok=True)

def memory_bytes():
    try:
        if sys.platform == "darwin":
            return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip())
        meminfo = pathlib.Path("/proc/meminfo")
        if meminfo.exists():
            for line in meminfo.read_text(encoding="utf-8").splitlines():
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except (OSError, subprocess.CalledProcessError, ValueError):
        return 0
    return 0

host_memory = memory_bytes()
host_cpu = os.environ.get("BENCH_CPU_MODEL") or platform.processor() or platform.machine()
host = {
    "cpu": host_cpu,
    "cores": os.cpu_count() or 0,
    "ram_gb": round(host_memory / (1024 ** 3)) if host_memory else 0,
    "os": platform.platform(),
    "memory_bytes": host_memory,
}
files = {
    "postgres": ("postgres17", "clickbench-postgres17.json"),
    "ultrasql": ("ultrasql", "clickbench-ultrasql.json"),
    "duckdb": ("duckdb", "clickbench-duckdb.json"),
    "clickhouse": ("clickhouse", "clickbench-clickhouse.json"),
    "firebolt": ("firebolt", "clickbench-firebolt.json"),
}
for requested in [engine for engine in engines_raw.split(",") if engine]:
    engine, filename = files.get(requested, (requested, f"clickbench-{requested}.json"))
    doc = {
        "schema_version": 1,
        "suite": "clickbench",
        "engine": engine,
        "workload": "clickbench",
        "upstream_ref": ref,
        "dataset": dataset,
        "dataset_rows": None,
        "query_count": None,
        "samples": [],
        "median_us": None,
        "p95_us": None,
        "geomean_ms": None,
        "status": "not_available",
        "reason": reason,
        "detail": detail,
        "host": host,
        "host_cpu": host_cpu,
        "host_memory": host_memory,
        "generated_at_unix": int(time.time()),
        "policy": (
            "No ClickBench claim exists for this engine until this artifact "
            "contains measured samples from the pinned dataset."
        ),
    }
    if engine == "firebolt":
        doc["core_mode"] = "local_docker"
        doc["docker_image"] = os.environ.get("FIREBOLT_CORE_IMAGE", "firebolt-core")
    (raw_dir / filename).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
PY
}

write_failure_summary() {
    local reason="$1"
    local detail="$2"
    emit_not_available_artifacts "$reason" "$detail"
    python3 - "$SUMMARY_OUT" "$reason" "$detail" "$CLICKBENCH_REF" \
        "$CLICKBENCH_TSV" "$POSTGRES_DSN" "$ULTRASQL_DSN" "$CLICKBENCH_ENGINES" \
        "$POSTGRES_OUT" "$ULTRA_OUT" "$DUCKDB_OUT" "$CLICKHOUSE_OUT" "$FIREBOLT_OUT" <<'PY'
import json
import pathlib
import sys

(
    summary,
    reason,
    detail,
    ref,
    dataset,
    pg_dsn,
    ul_dsn,
    engines,
    postgres_out,
    ultra_out,
    duckdb_out,
    clickhouse_out,
    firebolt_out,
) = sys.argv[1:]
doc = {
    "schema_version": 1,
    "workload": "clickbench",
    "upstream_ref": ref,
    "target": "UltraSQL and requested competitors publish honest ClickBench measured artifacts when local prerequisites exist",
    "requested_engines": [engine for engine in engines.split(",") if engine],
    "passed": False,
    "reason": reason,
    "detail": detail,
    "dataset": dataset,
    "postgres_dsn_set": bool(pg_dsn),
    "ultrasql_dsn_set": bool(ul_dsn),
    "artifacts": {
        "postgres17": postgres_out,
        "ultrasql": ultra_out,
        "duckdb": duckdb_out,
        "clickhouse": clickhouse_out,
        "firebolt": firebolt_out,
    },
    "next_step": (
        "Provide the dataset plus any requested local engine prerequisites, "
        "then rerun benchmarks/clickbench_certify.sh."
    ),
}
pathlib.Path(summary).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
}

curl -L --fail --silent "$base/create.sql" -o "$schema"
curl -L --fail --silent "$base/queries.sql" -o "$queries"

if [[ ! -f "$CLICKBENCH_TSV" ]]; then
    if [[ "${CLICKBENCH_DOWNLOAD:-0}" == "1" ]]; then
        curl -L --fail "https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz" \
            | gunzip > "$CLICKBENCH_TSV"
    else
        write_failure_summary "dataset_missing" "Set CLICKBENCH_DOWNLOAD=1 or CLICKBENCH_TSV=/path/to/hits.tsv."
        cat >&2 <<EOF
ClickBench dataset missing: $CLICKBENCH_TSV
Download explicitly:
  CLICKBENCH_DOWNLOAD=1 benchmarks/clickbench_certify.sh
or set:
  CLICKBENCH_TSV=/path/to/hits.tsv
EOF
        exit 2
    fi
fi

if ! command -v psql >/dev/null 2>&1; then
    write_failure_summary "psql_missing" "psql must be available on PATH."
    echo "psql missing. Install PostgreSQL client tools or add psql to PATH." >&2
    exit 2
fi

if [[ "$ALLOW_PARTIAL" != "1" && ( -z "$POSTGRES_DSN" || -z "$ULTRASQL_DSN" ) ]]; then
    echo "ClickBench missing PostgreSQL-wire DSN; runner will emit not_available artifacts for those engines." >&2
fi

python3 - "$schema" "$queries" "$CLICKBENCH_TSV" "$RUNS" "$POSTGRES_DSN" "$ULTRASQL_DSN" "$SUMMARY_OUT" "$RAW_DIR" "$CLICKBENCH_REF" "$CLICKBENCH_ENGINES" "$DUCKDB_BIN" <<'PY'
import json
import math
import os
import pathlib
import platform
import statistics
import subprocess
import sys
import time

(
    schema_path,
    queries_path,
    data_path,
    runs_raw,
    pg_dsn,
    ul_dsn,
    out_path,
    raw_dir,
    upstream_ref,
    engines_raw,
    duckdb_bin,
) = sys.argv[1:]
runs = int(runs_raw)
queries = [q.strip() for q in pathlib.Path(queries_path).read_text().split(";") if q.strip()]
schema = pathlib.Path(schema_path).read_text()
data_path = pathlib.Path(data_path)
out_path = pathlib.Path(out_path)
raw_dir = pathlib.Path(raw_dir)
raw_dir.mkdir(parents=True, exist_ok=True)
requested_engines = [engine for engine in engines_raw.split(",") if engine]

def memory_bytes():
    try:
        if sys.platform == "darwin":
            return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip())
        meminfo = pathlib.Path("/proc/meminfo")
        if meminfo.exists():
            for line in meminfo.read_text(encoding="utf-8").splitlines():
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except (OSError, subprocess.CalledProcessError, ValueError):
        return 0
    return 0

host_memory = memory_bytes()
host_cpu = os.environ.get("BENCH_CPU_MODEL") or platform.processor() or platform.machine()
host = {
    "cpu": host_cpu,
    "cores": os.cpu_count() or 0,
    "ram_gb": round(host_memory / (1024 ** 3)) if host_memory else 0,
    "os": platform.platform(),
    "memory_bytes": host_memory,
}

def dataset_rows():
    with data_path.open("rb") as data:
        return sum(1 for _ in data)

row_count = dataset_rows()

def artifact_base(engine, status):
    return {
        "schema_version": 1,
        "suite": "clickbench",
        "engine": engine,
        "workload": "clickbench",
        "upstream_ref": upstream_ref,
        "dataset": str(data_path),
        "dataset_rows": row_count,
        "query_count": len(queries),
        "status": status,
        "host": host,
        "host_cpu": host_cpu,
        "host_memory": host_memory,
        "policy": (
            "ClickBench artifact records raw measured samples only; no ranking "
            "or winner claim is made unless all compared engines are measured."
        ),
    }

def not_available(engine, reason, detail=None):
    doc = artifact_base(engine, "not_available")
    doc.update({
        "reason": reason,
        "detail": detail,
        "samples": [],
        "median_us": None,
        "p95_us": None,
        "geomean_ms": None,
    })
    if engine == "firebolt":
        doc["core_mode"] = "local_docker"
        doc["docker_image"] = os.environ.get("FIREBOLT_CORE_IMAGE", "firebolt-core")
    return doc

def psql(dsn, sql, *, capture=False):
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", dsn, "-q", "-c", sql]
    return subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE,
        check=True,
    )

def load_engine(name, dsn):
    psql(dsn, "DROP TABLE IF EXISTS hits")
    psql(dsn, schema)
    with data_path.open("rb") as input_file:
        subprocess.run(
            ["psql", "-v", "ON_ERROR_STOP=1", dsn, "-q", "-c", "COPY hits FROM STDIN"],
            stdin=input_file,
            stderr=subprocess.PIPE,
            check=True,
        )
    try:
        psql(dsn, "VACUUM ANALYZE hits")
    except subprocess.CalledProcessError:
        pass

def run_engine(name, dsn):
    try:
        load_engine(name, dsn)
    except subprocess.CalledProcessError as exc:
        doc = artifact_base(name, "failed")
        doc.update({
            "load_error": (exc.stderr or str(exc)).strip(),
            "queries": [],
            "geomean_ms": None,
        })
        return doc

    q_results = []
    for idx, query in enumerate(queries, start=1):
        samples = []
        error = None
        for _ in range(runs):
            start = time.perf_counter()
            try:
                psql(dsn, query, capture=True)
            except subprocess.CalledProcessError as exc:
                error = (exc.stderr or str(exc)).strip()
                break
            samples.append((time.perf_counter() - start) * 1000.0)
        q_results.append({
            "query": idx,
            "median_ms": statistics.median(samples) if samples else None,
            "runs_ms": samples,
            "error": error,
        })
    doc = artifact_base(name, "measured")
    doc["queries"] = q_results
    doc["samples"] = runs
    return doc

def duckdb_exec(db_path, sql):
    return subprocess.run(
        [duckdb_bin, str(db_path), "-c", sql],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )

def run_duckdb():
    if not duckdb_bin:
        return not_available("duckdb", "duckdb_missing", "duckdb binary not found on PATH")
    db_path = raw_dir / "clickbench.duckdb"
    try:
        db_path.unlink(missing_ok=True)
        duckdb_exec(db_path, "DROP TABLE IF EXISTS hits")
        duckdb_exec(db_path, schema)
        escaped_data = str(data_path).replace("'", "''")
        duckdb_exec(
            db_path,
            f"COPY hits FROM '{escaped_data}' (DELIMITER '\\t', HEADER false)",
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        stderr = getattr(exc, "stderr", None)
        return not_available(
            "duckdb",
            "duckdb_load_failed",
            (stderr or str(exc)).strip(),
        )

    q_results = []
    for idx, query in enumerate(queries, start=1):
        samples = []
        error = None
        for _ in range(runs):
            start = time.perf_counter()
            try:
                duckdb_exec(db_path, query)
            except subprocess.CalledProcessError as exc:
                error = (exc.stderr or str(exc)).strip()
                break
            samples.append((time.perf_counter() - start) * 1000.0)
        q_results.append({
            "query": idx,
            "median_ms": statistics.median(samples) if samples else None,
            "runs_ms": samples,
            "error": error,
        })
    doc = artifact_base("duckdb", "measured")
    doc["queries"] = q_results
    doc["samples"] = runs
    return doc

def geomean(result):
    vals = [q["median_ms"] for q in result["queries"] if q["median_ms"]]
    if len(vals) != len(result["queries"]):
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))

results = []
if "postgres" in requested_engines and pg_dsn:
    results.append(run_engine("postgres17", pg_dsn))
elif "postgres" in requested_engines:
    results.append(not_available("postgres17", "dsn_missing", "POSTGRES_DSN is not set"))
if "ultrasql" in requested_engines and ul_dsn:
    results.append(run_engine("ultrasql", ul_dsn))
elif "ultrasql" in requested_engines:
    results.append(not_available("ultrasql", "dsn_missing", "ULTRASQL_DSN is not set"))
if "duckdb" in requested_engines:
    results.append(run_duckdb())
if "clickhouse" in requested_engines:
    results.append(not_available(
        "clickhouse",
        "runner_not_implemented_for_engine",
        "ClickHouse ClickBench local loader/query runner is not wired yet.",
    ))
if "firebolt" in requested_engines:
    results.append(not_available(
        "firebolt",
        "runner_not_implemented_for_engine",
        "Firebolt Core local ClickBench loader/query runner is not wired yet.",
    ))
if not results:
    doc = {
        "schema_version": 1,
        "workload": "clickbench",
        "upstream_ref": upstream_ref,
        "target": "requested ClickBench engines produce honest artifacts",
        "passed": False,
        "reason": "engine_list_empty",
        "results": [],
    }
    out_path.write_text(json.dumps(doc, indent=2) + "\n")
    print(json.dumps(doc, indent=2))
    raise SystemExit(2)

for result in results:
    if "geomean_ms" not in result:
        result["geomean_ms"] = geomean(result)
    result["median_us"] = (
        result["geomean_ms"] * 1000.0
        if result.get("geomean_ms") is not None
        else None
    )
    query_medians = [
        query["median_ms"] * 1000.0
        for query in result.get("queries", [])
        if query.get("median_ms") is not None
    ]
    result["p95_us"] = (
        sorted(query_medians)[max(0, math.ceil(len(query_medians) * 0.95) - 1)]
        if query_medians
        else None
    )
    (raw_dir / f"clickbench-{result['engine']}.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )

pg = next((r for r in results if r["engine"] == "postgres17"), None)
ul = next((r for r in results if r["engine"] == "ultrasql"), None)
passed = (
    pg is not None
    and ul is not None
    and pg["geomean_ms"] is not None
    and ul["geomean_ms"] is not None
    and ul["geomean_ms"] * 5.0 <= pg["geomean_ms"]
)
doc = {
    "schema_version": 1,
    "workload": "clickbench",
    "upstream_ref": upstream_ref,
    "dataset": str(data_path),
    "dataset_rows": row_count,
    "query_count": len(queries),
    "target": "UltraSQL geometric mean at least 5x faster than PostgreSQL when both are measured; other requested engines publish measured or not_available artifacts",
    "requested_engines": requested_engines,
    "passed": passed,
    "status": "passed" if passed else "partial" if any(r.get("status") == "not_available" for r in results) else "failed",
    "postgres_result": str(raw_dir / "clickbench-postgres17.json"),
    "ultrasql_result": str(raw_dir / "clickbench-ultrasql.json"),
    "duckdb_result": str(raw_dir / "clickbench-duckdb.json"),
    "clickhouse_result": str(raw_dir / "clickbench-clickhouse.json"),
    "firebolt_result": str(raw_dir / "clickbench-firebolt.json"),
    "results": results,
    "host": host,
}
out_path.write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
if passed:
    sys.exit(0)
if any(r.get("status") == "failed" for r in results):
    sys.exit(1)
if any(r.get("status") == "not_available" for r in results):
    sys.exit(2)
sys.exit(1)
PY
