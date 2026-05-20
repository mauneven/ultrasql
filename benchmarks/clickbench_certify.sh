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
RUNS="${CLICKBENCH_RUNS:-3}"
ALLOW_PARTIAL="${CLICKBENCH_ALLOW_PARTIAL:-0}"
OUT_DIR="${BENCH_CERT_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
SUMMARY_OUT="$OUT_DIR/clickbench_certification.json"

mkdir -p "$CLICKBENCH_WORK" "$OUT_DIR" "$RAW_DIR"

base="https://raw.githubusercontent.com/ClickHouse/ClickBench/$CLICKBENCH_REF/postgresql"
schema="$CLICKBENCH_WORK/create.sql"
queries="$CLICKBENCH_WORK/queries.sql"

write_failure_summary() {
    local reason="$1"
    local detail="$2"
    python3 - "$SUMMARY_OUT" "$reason" "$detail" "$CLICKBENCH_REF" \
        "$CLICKBENCH_TSV" "$POSTGRES_DSN" "$ULTRASQL_DSN" <<'PY'
import json
import pathlib
import sys

summary, reason, detail, ref, dataset, pg_dsn, ul_dsn = sys.argv[1:]
doc = {
    "workload": "clickbench",
    "upstream_ref": ref,
    "target": "UltraSQL geometric mean at least 5x faster than PostgreSQL",
    "passed": False,
    "reason": reason,
    "detail": detail,
    "dataset": dataset,
    "postgres_dsn_set": bool(pg_dsn),
    "ultrasql_dsn_set": bool(ul_dsn),
    "next_step": (
        "Provide the dataset plus both POSTGRES_DSN and ULTRASQL_DSN, then "
        "rerun benchmarks/clickbench_certify.sh."
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
    write_failure_summary "dsn_missing" "Certification requires both POSTGRES_DSN and ULTRASQL_DSN."
    echo "ClickBench certification requires both POSTGRES_DSN and ULTRASQL_DSN." >&2
    exit 2
fi

python3 - "$schema" "$queries" "$CLICKBENCH_TSV" "$RUNS" "$POSTGRES_DSN" "$ULTRASQL_DSN" "$SUMMARY_OUT" "$RAW_DIR" "$CLICKBENCH_REF" <<'PY'
import json
import math
import pathlib
import statistics
import subprocess
import sys
import time

schema_path, queries_path, data_path, runs_raw, pg_dsn, ul_dsn, out_path, raw_dir, upstream_ref = sys.argv[1:]
runs = int(runs_raw)
queries = [q.strip() for q in pathlib.Path(queries_path).read_text().split(";") if q.strip()]
schema = pathlib.Path(schema_path).read_text()
data_path = pathlib.Path(data_path)
out_path = pathlib.Path(out_path)
raw_dir = pathlib.Path(raw_dir)
raw_dir.mkdir(parents=True, exist_ok=True)

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
        return {
            "engine": name,
            "load_error": (exc.stderr or str(exc)).strip(),
            "queries": [],
            "geomean_ms": None,
        }

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
    return {"engine": name, "queries": q_results}

def geomean(result):
    vals = [q["median_ms"] for q in result["queries"] if q["median_ms"]]
    if len(vals) != len(result["queries"]):
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))

results = []
if pg_dsn:
    results.append(run_engine("postgres17", pg_dsn))
if ul_dsn:
    results.append(run_engine("ultrasql", ul_dsn))
if not results:
    doc = {
        "workload": "clickbench",
        "upstream_ref": upstream_ref,
        "target": "UltraSQL geometric mean at least 5x faster than PostgreSQL",
        "passed": False,
        "reason": "dsn_missing",
        "results": [],
    }
    out_path.write_text(json.dumps(doc, indent=2) + "\n")
    print(json.dumps(doc, indent=2))
    raise SystemExit(2)

for result in results:
    if "geomean_ms" not in result:
        result["geomean_ms"] = geomean(result)
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
    "workload": "clickbench",
    "upstream_ref": upstream_ref,
    "dataset": str(data_path),
    "query_count": len(queries),
    "target": "UltraSQL geometric mean at least 5x faster than PostgreSQL",
    "passed": passed,
    "postgres_result": str(raw_dir / "clickbench-postgres17.json"),
    "ultrasql_result": str(raw_dir / "clickbench-ultrasql.json"),
    "results": results,
}
out_path.write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(0 if passed else 1)
PY
