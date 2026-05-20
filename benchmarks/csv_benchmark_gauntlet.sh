#!/usr/bin/env bash
# Reproducible CSV benchmark gauntlet.
#
# Measures the same generated CSV inputs across UltraSQL, DuckDB, and
# ClickHouse when those engines are installed. Missing engines write
# not_available artifacts; those are visible gaps, not benchmark claims.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${CSV_GAUNTLET_PROFILE:-${1:-smoke}}"
OUT_DIR="${CSV_GAUNTLET_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
MANIFEST="$OUT_DIR/csv_benchmark_gauntlet_manifest.json"
ENGINES="${CSV_GAUNTLET_ENGINES:-ultrasql,duckdb,clickhouse}"

case "$PROFILE" in
    smoke)
        ROWS="${CSV_GAUNTLET_ROWS:-1000}"
        ITERS="${N_ITERS:-${CSV_GAUNTLET_ITERS:-1}}"
        WARMUP="${WARMUP:-${CSV_GAUNTLET_WARMUP:-1}}"
        ;;
    full)
        ROWS="${CSV_GAUNTLET_ROWS:-10000000}"
        ITERS="${N_ITERS:-${CSV_GAUNTLET_ITERS:-8}}"
        WARMUP="${WARMUP:-${CSV_GAUNTLET_WARMUP:-2}}"
        ;;
    *)
        echo "csv_benchmark_gauntlet.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

if (( ROWS <= 0 || ITERS <= 0 || WARMUP < 0 )); then
    echo "csv_benchmark_gauntlet.sh: invalid ROWS/ITERS/WARMUP" >&2
    exit 2
fi

mkdir -p "$RAW_DIR"

row_label="$(
    python3 - "$ROWS" <<'PY'
import sys
n = int(sys.argv[1])
if n >= 1_000_000 and n % 1_000_000 == 0:
    print(f"{n // 1_000_000}m")
elif n >= 1_000 and n % 1_000 == 0:
    print(f"{n // 1_000}k")
else:
    print(str(n))
PY
)"

WORKLOADS=(
    csv_cold_read
    csv_warm_read
    csv_copy_import
    csv_group_by
    csv_filter
    csv_join_table
    csv_malformed_behavior
)

DATA_DIR="$(mktemp -d /tmp/ultrasql-csv-gauntlet-XXXXXX)"
STATUS_FILE="$(mktemp)"
trap 'rm -rf "$DATA_DIR"; rm -f "$STATUS_FILE"' EXIT

CSV_PATH="$DATA_DIR/facts.csv"
BAD_CSV_PATH="$DATA_DIR/facts_bad.csv"

python3 - "$ROWS" "$CSV_PATH" "$BAD_CSV_PATH" <<'PY'
import csv
import sys

rows = int(sys.argv[1])
csv_path = sys.argv[2]
bad_path = sys.argv[3]
categories = ["alpha", "beta", "gamma", "delta"]

with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "category", "metric", "fact_dim"])
    for row_id in range(rows):
        writer.writerow(
            [
                row_id,
                categories[row_id % len(categories)],
                (row_id * 17) % 1000 - 500,
                f"d{row_id % 16}",
            ]
        )

with open(bad_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "category", "metric", "fact_dim"])
    writer.writerow([1, "alpha", 10, "d1"])
    writer.writerow(["bad", "beta", 20, "d2"])
    writer.writerow([2, "gamma", 30, "d3"])
    f.write("3,delta,40,d4,extra\n")
    writer.writerow([4, "alpha", 50, "d4"])
PY

record_artifact() {
    local engine="$1"
    local workload="$2"
    local status="$3"
    local artifact="$4"
    printf '%s\t%s\t%s\t%s\n' "$engine" "$workload" "$status" "$artifact" >>"$STATUS_FILE"
}

contains_engine() {
    local needle="$1"
    case ",$ENGINES," in
        *",$needle,"*) return 0 ;;
        *) return 1 ;;
    esac
}

emit_not_available() {
    local engine="$1"
    local reason="$2"
    local workload out
    for workload in "${WORKLOADS[@]}"; do
        out="$RAW_DIR/${workload}_${row_label}-${engine}.json"
        printf '{"schema_version":1,"suite":"csv_benchmark_gauntlet","engine":"%s","workload":"%s_%s","profile":"%s","status":"not_available","reason":"%s","policy":"No CSV benchmark claim exists for this engine/workload until this artifact records measured samples from reproducible inputs."}\n' \
            "$engine" "$workload" "$row_label" "$PROFILE" "$reason" >"$out"
        record_artifact "$engine" "$workload" "not_available" "$out"
    done
}

run_ultrasql() {
    echo "=== CSV gauntlet: ultrasql profile=$PROFILE rows=$ROWS ==="
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
        cargo build --release --package ultrasql-bench --features sql-bench \
            --bin cross_compare_sql >/dev/null

    local workload out cli_workload warmup iters
    for workload in "${WORKLOADS[@]}"; do
        out="$RAW_DIR/${workload}_${row_label}-ultrasql.json"
        cli_workload="${workload//_/-}"
        warmup="$WARMUP"
        iters="$ITERS"
        if [[ "$workload" == "csv_cold_read" ]]; then
            warmup=0
            iters=1
        fi
        target/release/cross_compare_sql \
            --workload "$cli_workload" \
            --rows "$ROWS" \
            --warmup "$warmup" \
            --iters "$iters" \
            --csv-path "$CSV_PATH" \
            --csv-bad-path "$BAD_CSV_PATH" \
            --output "$out"
        record_artifact "ultrasql" "$workload" "measured" "$out"
    done
}

run_duckdb() {
    if ! python3 -c "import duckdb" >/dev/null 2>&1; then
        emit_not_available "duckdb" "python_duckdb_not_found"
        return
    fi
    python3 - "$RAW_DIR" "$PROFILE" "$ROWS" "$row_label" "$CSV_PATH" "$BAD_CSV_PATH" "$WARMUP" "$ITERS" "$STATUS_FILE" <<'PY'
import json
import pathlib
import statistics
import sys
import time

import duckdb

raw_dir, profile, rows_s, row_label, csv_path, bad_path, warmup_s, iters_s, status_file = sys.argv[1:]
rows = int(rows_s)
warmup = int(warmup_s)
iters = int(iters_s)

def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

def emit(workload: str, samples: list[float], answer):
    out = pathlib.Path(raw_dir) / f"{workload}_{row_label}-duckdb.json"
    doc = {
        "schema_version": 1,
        "suite": "csv_benchmark_gauntlet",
        "engine": "duckdb",
        "workload": f"{workload}_{row_label}",
        "profile": profile,
        "n_rows": rows,
        "samples": len(samples),
        "median_us": statistics.median(samples),
        "min_us": min(samples),
        "iterations_us": samples,
        "answer": answer,
    }
    out.write_text(json.dumps(doc, separators=(",", ":")) + "\n", encoding="utf-8")
    with open(status_file, "a", encoding="utf-8") as f:
        f.write(f"duckdb\t{workload}\tmeasured\t{out}\n")

def measure(query_factory, workload: str, answer_factory=lambda result: result):
    con = duckdb.connect(":memory:")
    samples = []
    answer = None
    total = warmup + iters
    if workload == "csv_cold_read":
        total = 1
    for i in range(total):
        query = query_factory(con, i)
        start = time.perf_counter()
        result = con.execute(query).fetchall()
        elapsed = (time.perf_counter() - start) * 1e6
        if i >= (0 if workload == "csv_cold_read" else warmup):
            samples.append(elapsed)
            answer = answer_factory(result)
    emit(workload, samples, answer)

path = sql_string(csv_path)
bad = sql_string(bad_path)
read_csv = f"read_csv_auto({path}, all_varchar=true)"

measure(lambda _con, _i: f"SELECT COUNT(*) FROM {read_csv}", "csv_cold_read")
measure(lambda _con, _i: f"SELECT COUNT(*) FROM {read_csv}", "csv_warm_read")

def copy_query(con, i):
    table = f"csv_copy_import_{i}"
    con.execute(f"CREATE TABLE {table} (id INTEGER, category VARCHAR, metric INTEGER, fact_dim VARCHAR)")
    return f"COPY {table} FROM {path} (HEADER, DELIMITER ',')"

def copy_answer(_result):
    return {"mode": "copy_import"}

measure(copy_query, "csv_copy_import", copy_answer)
measure(
    lambda _con, _i: (
        f"SELECT category, COUNT(*) FROM {read_csv} "
        "GROUP BY category ORDER BY category"
    ),
    "csv_group_by",
)
measure(lambda _con, _i: f"SELECT COUNT(*) FROM {read_csv} WHERE category = 'alpha'", "csv_filter")

def join_query(con, _i):
    con.execute("CREATE OR REPLACE TABLE csv_dim AS SELECT 'd' || range::VARCHAR AS dim_id FROM range(16)")
    return f"SELECT COUNT(*) FROM {read_csv} JOIN csv_dim ON fact_dim = dim_id"

measure(join_query, "csv_join_table")

def malformed_query(con, i):
    table = f"csv_bad_import_{i}"
    con.execute(f"CREATE TABLE {table} (id INTEGER, category VARCHAR, metric INTEGER, fact_dim VARCHAR)")
    return f"COPY {table} FROM {bad} (HEADER, DELIMITER ',', IGNORE_ERRORS true)"

def malformed_answer(_result):
    return {"mode": "copy_ignore_errors"}

try:
    measure(malformed_query, "csv_malformed_behavior", malformed_answer)
except Exception as exc:
    out = pathlib.Path(raw_dir) / f"csv_malformed_behavior_{row_label}-duckdb.json"
    doc = {
        "schema_version": 1,
        "suite": "csv_benchmark_gauntlet",
        "engine": "duckdb",
        "workload": f"csv_malformed_behavior_{row_label}",
        "profile": profile,
        "n_rows": rows,
        "status": "error",
        "error": str(exc),
        "policy": "Malformed CSV behavior artifact records engine behavior; this is not a performance claim.",
    }
    out.write_text(json.dumps(doc, separators=(",", ":")) + "\n", encoding="utf-8")
    with open(status_file, "a", encoding="utf-8") as f:
        f.write(f"duckdb\tcsv_malformed_behavior\tmeasured\t{out}\n")
PY
}

run_clickhouse() {
    local ch_bin="${CLICKHOUSE_BIN:-}"
    if [[ -z "$ch_bin" ]]; then
        if command -v clickhouse-local >/dev/null 2>&1; then
            ch_bin="$(command -v clickhouse-local)"
        elif command -v clickhouse >/dev/null 2>&1; then
            ch_bin="$(command -v clickhouse)"
        else
            emit_not_available "clickhouse" "clickhouse_local_not_found"
            return
        fi
    fi
    python3 - "$RAW_DIR" "$PROFILE" "$ROWS" "$row_label" "$CSV_PATH" "$BAD_CSV_PATH" "$WARMUP" "$ITERS" "$STATUS_FILE" "$ch_bin" <<'PY'
import json
import pathlib
import statistics
import subprocess
import sys
import time

raw_dir, profile, rows_s, row_label, csv_path, bad_path, warmup_s, iters_s, status_file, ch_bin = sys.argv[1:]
rows = int(rows_s)
warmup = int(warmup_s)
iters = int(iters_s)

def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

def command(query: str, extra: list[str] | None = None) -> list[str]:
    extra = extra or []
    exe = pathlib.Path(ch_bin).name
    if exe == "clickhouse-local":
        return [ch_bin, *extra, "--query", query]
    return [ch_bin, "local", *extra, "--query", query]

def run_query(query: str, extra: list[str] | None = None) -> str:
    proc = subprocess.run(
        command(query, extra),
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc.stdout.strip()

def emit(workload: str, samples: list[float], answer):
    out = pathlib.Path(raw_dir) / f"{workload}_{row_label}-clickhouse.json"
    doc = {
        "schema_version": 1,
        "suite": "csv_benchmark_gauntlet",
        "engine": "clickhouse",
        "workload": f"{workload}_{row_label}",
        "profile": profile,
        "n_rows": rows,
        "samples": len(samples),
        "median_us": statistics.median(samples),
        "min_us": min(samples),
        "iterations_us": samples,
        "answer": answer,
        "driver": "clickhouse-local",
    }
    out.write_text(json.dumps(doc, separators=(",", ":")) + "\n", encoding="utf-8")
    with open(status_file, "a", encoding="utf-8") as f:
        f.write(f"clickhouse\t{workload}\tmeasured\t{out}\n")

schema = "id UInt64, category String, metric Int64, fact_dim String"
path = sql_string(csv_path)
bad = sql_string(bad_path)
source = f"file({path}, CSVWithNames, '{schema}')"

def measure(workload: str, query_factory, answer_factory=lambda out: out):
    samples = []
    answer = None
    total = warmup + iters
    start_index = warmup
    if workload == "csv_cold_read":
        total = 1
        start_index = 0
    for i in range(total):
        query, extra = query_factory(i)
        start = time.perf_counter()
        out = run_query(query, extra)
        elapsed = (time.perf_counter() - start) * 1e6
        if i >= start_index:
            samples.append(elapsed)
            answer = answer_factory(out)
    emit(workload, samples, answer)

measure("csv_cold_read", lambda _i: (f"SELECT count() FROM {source}", None))
measure("csv_warm_read", lambda _i: (f"SELECT count() FROM {source}", None))
measure(
    "csv_copy_import",
    lambda i: (
        "CREATE TABLE t (id UInt64, category String, metric Int64, fact_dim String) ENGINE = Memory; "
        f"INSERT INTO t SELECT * FROM {source}; SELECT count() FROM t",
        ["--multiquery"],
    ),
)
measure(
    "csv_group_by",
    lambda _i: (
        f"SELECT category, count() FROM {source} GROUP BY category ORDER BY category",
        None,
    ),
)
measure(
    "csv_filter",
    lambda _i: (f"SELECT count() FROM {source} WHERE category = 'alpha'", None),
)
measure(
    "csv_join_table",
    lambda _i: (
        f"SELECT count() FROM {source} AS f "
        "INNER JOIN (SELECT concat('d', toString(number)) AS dim_id FROM numbers(16)) AS d "
        "ON f.fact_dim = d.dim_id",
        None,
    ),
)

bad_source = f"file({bad}, CSVWithNames, '{schema}')"
try:
    measure(
        "csv_malformed_behavior",
        lambda _i: (
            f"SELECT count() FROM {bad_source}",
            ["--input_format_allow_errors_num=1000", "--input_format_allow_errors_ratio=1"],
        ),
        lambda out: {"mode": "allow_errors", "accepted_rows": out},
    )
except Exception as exc:
    out = pathlib.Path(raw_dir) / f"csv_malformed_behavior_{row_label}-clickhouse.json"
    doc = {
        "schema_version": 1,
        "suite": "csv_benchmark_gauntlet",
        "engine": "clickhouse",
        "workload": f"csv_malformed_behavior_{row_label}",
        "profile": profile,
        "n_rows": rows,
        "status": "error",
        "error": str(exc),
        "policy": "Malformed CSV behavior artifact records engine behavior; this is not a performance claim.",
    }
    out.write_text(json.dumps(doc, separators=(",", ":")) + "\n", encoding="utf-8")
    with open(status_file, "a", encoding="utf-8") as f:
        f.write(f"clickhouse\tcsv_malformed_behavior\tmeasured\t{out}\n")
PY
}

if contains_engine "ultrasql"; then
    run_ultrasql
fi
if contains_engine "duckdb"; then
    run_duckdb
fi
if contains_engine "clickhouse"; then
    run_clickhouse
fi

python3 - "$PROFILE" "$MANIFEST" "$STATUS_FILE" <<'PY'
import json
import pathlib
import sys
import time

profile, manifest_path, status_path = sys.argv[1:]
entries = []
if pathlib.Path(status_path).exists():
    for line in pathlib.Path(status_path).read_text(encoding="utf-8").splitlines():
        engine, workload, status, artifact = line.split("\t")
        entries.append(
            {
                "engine": engine,
                "workload": workload,
                "status": status,
                "artifact": artifact,
            }
        )

has_unavailable = any(entry["status"] == "not_available" for entry in entries)
doc = {
    "schema_version": 1,
    "suite": "csv_benchmark_gauntlet",
    "profile": profile,
    "generated_at_unix": int(time.time()),
    "status": "partial" if has_unavailable else "passed",
    "passed": not has_unavailable,
    "artifacts": entries,
    "policy": (
        "CSV gauntlet is publishable only when all requested engines emit "
        "measured artifacts on the same generated inputs. not_available "
        "artifacts are setup gaps, not benchmark claims."
    ),
}
pathlib.Path(manifest_path).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(json.dumps(doc, indent=2))
PY

if grep -q $'\tnot_available\t' "$STATUS_FILE"; then
    exit 2
fi
exit 0
