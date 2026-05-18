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
OUT_DIR="benchmarks/results/latest"
SUMMARY_OUT="$OUT_DIR/clickbench_certification.json"

mkdir -p "$CLICKBENCH_WORK" "$OUT_DIR"

base="https://raw.githubusercontent.com/ClickHouse/ClickBench/$CLICKBENCH_REF/postgresql"
schema="$CLICKBENCH_WORK/create.sql"
queries="$CLICKBENCH_WORK/queries.sql"

curl -L --fail --silent "$base/create.sql" -o "$schema"
curl -L --fail --silent "$base/queries.sql" -o "$queries"

if [[ ! -f "$CLICKBENCH_TSV" ]]; then
    if [[ "${CLICKBENCH_DOWNLOAD:-0}" == "1" ]]; then
        curl -L --fail "https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz" \
            | gunzip > "$CLICKBENCH_TSV"
    else
        cat > "$SUMMARY_OUT" <<EOF
{
  "workload": "clickbench",
  "upstream_ref": "$CLICKBENCH_REF",
  "target": "UltraSQL geometric mean at least 5x faster than PostgreSQL",
  "passed": false,
  "reason": "dataset missing",
  "dataset": "$CLICKBENCH_TSV",
  "next_step": "Set CLICKBENCH_DOWNLOAD=1 or CLICKBENCH_TSV=/path/to/hits.tsv, then rerun benchmarks/clickbench_certify.sh"
}
EOF
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

python3 - "$schema" "$queries" "$CLICKBENCH_TSV" "$RUNS" "$POSTGRES_DSN" "$ULTRASQL_DSN" "$SUMMARY_OUT" <<'PY'
import json
import math
import pathlib
import statistics
import subprocess
import sys
import time

schema_path, queries_path, data_path, runs_raw, pg_dsn, ul_dsn, out_path = sys.argv[1:]
runs = int(runs_raw)
queries = [q.strip() for q in pathlib.Path(queries_path).read_text().split(";") if q.strip()]
schema = pathlib.Path(schema_path).read_text()
data_path = pathlib.Path(data_path)
out_path = pathlib.Path(out_path)

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
    copy = rf"\copy hits FROM '{data_path}'"
    subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", dsn, "-q", "-c", copy],
        text=True,
        check=True,
    )
    try:
        psql(dsn, "VACUUM ANALYZE hits")
    except subprocess.CalledProcessError:
        pass

def run_engine(name, dsn):
    load_engine(name, dsn)
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
    raise SystemExit("set POSTGRES_DSN and/or ULTRASQL_DSN")

for result in results:
    result["geomean_ms"] = geomean(result)

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
    "upstream_ref": "c5eb5d8e9c10fbef5ce16e8750f3e67de24cef0a",
    "query_count": len(queries),
    "target": "UltraSQL geometric mean at least 5x faster than PostgreSQL",
    "passed": passed,
    "results": results,
}
out_path.write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(0 if passed else 1)
PY
