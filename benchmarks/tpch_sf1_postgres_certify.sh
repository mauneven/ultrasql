#!/usr/bin/env bash
# Reproducible TPC-H SF1 PostgreSQL 17 certification runner.
#
# A pass requires complete q1..q22 raw artifacts for UltraSQL and PostgreSQL
# 17 on the same host. UltraSQL passes only when its geometric mean query time
# is <= 0.5x PostgreSQL's geometric mean.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

TPCH_DATA_DIR="${TPCH_DATA_DIR:-target/tpch-scale1-real}"
POSTGRES_DSN="${POSTGRES_DSN:-}"
PSQL_BIN="${TPCH_PSQL:-$(command -v psql || true)}"
RUNS="${TPCH_RUNS:-3}"
WARMUP="${TPCH_WARMUP:-1}"
QUERIES="${TPCH_QUERIES:-all}"
TPCH_TIMEOUT_SECONDS="${TPCH_TIMEOUT_SECONDS:-10800}"
ULTRASQL_PROGRESS="${ULTRASQL_TPCH_PROGRESS:-1}"
ULTRASQL_SPILL_BACKING="${ULTRASQL_PAGE_SPILL_BACKING:-memory}"
ULTRASQL_POOL_FRAMES="${ULTRASQL_TPCH_POOL_FRAMES:-262144}"
OUT_DIR="${BENCH_CERT_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
POSTGRES_OUT="$RAW_DIR/tpch_sf1-postgres17.json"
ULTRA_OUT="$RAW_DIR/tpch_sf1-ultrasql.json"
SUMMARY_OUT="$OUT_DIR/tpch_sf1_postgres_certification.json"
TMP_POSTGRES_OUT="$RAW_DIR/.tpch_sf1-postgres17.$$.tmp.json"
TMP_ULTRA_OUT="$RAW_DIR/.tpch_sf1-ultrasql.$$.tmp.json"

mkdir -p "$RAW_DIR"

cleanup() {
    rm -f "$TMP_POSTGRES_OUT" "$TMP_ULTRA_OUT"
}
trap cleanup EXIT

write_setup_summary() {
    local reason="$1"
    local detail="$2"
    python3 - "$SUMMARY_OUT" "$reason" "$detail" "$TPCH_DATA_DIR" \
        "$POSTGRES_DSN" "$POSTGRES_OUT" "$ULTRA_OUT" <<'PY'
import json
import pathlib
import sys

summary_path, reason, detail, data_dir, postgres_dsn, postgres_out, ultra_out = sys.argv[1:]
doc = {
    "schema_version": 1,
    "workload": "tpch_sf1_postgres",
    "scale_factor": 1,
    "target": "UltraSQL geometric mean <= 0.5x PostgreSQL 17 geometric mean across all 22 TPC-H queries",
    "passed": False,
    "status": "not_available",
    "reason": reason,
    "detail": detail,
    "data_dir": data_dir,
    "postgres_dsn_present": bool(postgres_dsn),
    "postgres_result": postgres_out,
    "ultrasql_result": ultra_out,
    "postgres_physical_design": (
        "TPC-H primary keys plus disclosed secondary indexes from "
        "benchmarks/tpch_sf1_postgres_certify.sh; per-table autovacuum disabled "
        "during timed runs; ANALYZE after load and index build."
    ),
    "next_step": (
        "Provide real SF1 .tbl data and POSTGRES_DSN for PostgreSQL 17, then "
        "rerun benchmarks/tpch_sf1_postgres_certify.sh."
    ),
    "policy": "TPC-H SF1 PostgreSQL certification writes raw artifacts only after q1..q22 complete for both engines.",
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
}

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

analyze_postgres() {
    "$PSQL_BIN" "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -q -c "ANALYZE;" >/dev/null
}

create_postgres_indexes() {
    "$PSQL_BIN" "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
CREATE INDEX tpch_sf1_lineitem_part_qty_idx ON lineitem (l_partkey, l_quantity);
CREATE INDEX tpch_sf1_lineitem_part_supp_idx ON lineitem (l_partkey, l_suppkey);
CREATE INDEX tpch_sf1_lineitem_order_idx ON lineitem (l_orderkey);
CREATE INDEX tpch_sf1_lineitem_supp_idx ON lineitem (l_suppkey);
CREATE INDEX tpch_sf1_lineitem_shipdate_idx ON lineitem (l_shipdate);
CREATE INDEX tpch_sf1_lineitem_receipt_commit_idx ON lineitem (l_receiptdate, l_commitdate);
CREATE INDEX tpch_sf1_lineitem_shipmode_receipt_idx ON lineitem (l_shipmode, l_receiptdate);
CREATE INDEX tpch_sf1_orders_cust_date_idx ON orders (o_custkey, o_orderdate);
CREATE INDEX tpch_sf1_orders_orderdate_idx ON orders (o_orderdate);
CREATE INDEX tpch_sf1_customer_nation_idx ON customer (c_nationkey);
CREATE INDEX tpch_sf1_customer_phone_prefix_idx ON customer ((substring(c_phone from 1 for 2)));
CREATE INDEX tpch_sf1_supplier_nation_idx ON supplier (s_nationkey);
CREATE INDEX tpch_sf1_partsupp_supp_idx ON partsupp (ps_suppkey);
CREATE INDEX tpch_sf1_part_brand_container_idx ON part (p_brand, p_container, p_partkey);
CREATE INDEX tpch_sf1_part_type_size_idx ON part (p_type, p_size);
SQL
}

configure_postgres_tables() {
    "$PSQL_BIN" "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
ALTER TABLE region SET (autovacuum_enabled = false);
ALTER TABLE nation SET (autovacuum_enabled = false);
ALTER TABLE supplier SET (autovacuum_enabled = false);
ALTER TABLE customer SET (autovacuum_enabled = false);
ALTER TABLE part SET (autovacuum_enabled = false);
ALTER TABLE partsupp SET (autovacuum_enabled = false);
ALTER TABLE orders SET (autovacuum_enabled = false);
ALTER TABLE lineitem SET (autovacuum_enabled = false);
SQL
}

if [[ "$QUERIES" != "all" ]]; then
    write_setup_summary \
        "partial_query_set_refused" \
        "TPC-H SF1 PostgreSQL certification writes artifacts only for q1..q22."
    echo "TPC-H SF1 certification requires TPCH_QUERIES=all, got: $QUERIES" >&2
    exit 2
fi

if [[ ! -d "$TPCH_DATA_DIR" ]]; then
    write_setup_summary "data_dir_missing" "TPC-H SF1 data directory is missing."
    echo "TPC-H data dir missing: $TPCH_DATA_DIR" >&2
    echo "Run: target/release/tpch gen-data 1 $TPCH_DATA_DIR" >&2
    exit 2
fi

if [[ -z "$POSTGRES_DSN" ]]; then
    write_setup_summary "postgres_dsn_missing" "POSTGRES_DSN is required for PostgreSQL 17 certification."
    echo "POSTGRES_DSN is required" >&2
    exit 2
fi

if [[ -z "$PSQL_BIN" || ! -x "$PSQL_BIN" ]]; then
    write_setup_summary "psql_missing" "psql binary missing or not executable."
    echo "psql missing. Set TPCH_PSQL=/path/to/psql" >&2
    exit 2
fi

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --features sql-bench --bin tpch

rm -f "$POSTGRES_OUT" "$ULTRA_OUT" "$TMP_POSTGRES_OUT" "$TMP_ULTRA_OUT"

echo "Preparing PostgreSQL TPC-H SF1 schema"
"$PSQL_BIN" "$POSTGRES_DSN" -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
DROP TABLE IF EXISTS lineitem, orders, partsupp, part, customer, supplier, nation, region CASCADE;
SQL
target/release/tpch init-schema postgres | "$PSQL_BIN" "$POSTGRES_DSN" -v ON_ERROR_STOP=1 >/dev/null

echo "Configuring PostgreSQL TPC-H SF1 benchmark tables"
if ! run_with_watchdog \
    "PostgreSQL TPC-H SF1 table configuration" \
    configure_postgres_tables; then
    write_setup_summary "postgres_config_failed" "PostgreSQL TPC-H SF1 table configuration failed."
    exit 1
fi

echo "Loading PostgreSQL TPC-H SF1 from $TPCH_DATA_DIR"
if ! run_with_watchdog \
    "PostgreSQL TPC-H SF1 load" \
    target/release/tpch load postgres "$TPCH_DATA_DIR" --pg-dsn "$POSTGRES_DSN"; then
    write_setup_summary "postgres_load_failed" "PostgreSQL did not load TPC-H SF1 data."
    exit 1
fi

echo "Building PostgreSQL TPC-H SF1 indexes"
if ! run_with_watchdog \
    "PostgreSQL TPC-H SF1 index build" \
    create_postgres_indexes; then
    write_setup_summary "postgres_index_failed" "PostgreSQL did not build TPC-H SF1 indexes."
    exit 1
fi

echo "Analyzing PostgreSQL TPC-H SF1 tables"
if ! run_with_watchdog \
    "PostgreSQL TPC-H SF1 analyze" \
    analyze_postgres; then
    write_setup_summary "postgres_analyze_failed" "PostgreSQL did not analyze TPC-H SF1 tables."
    exit 1
fi

echo "Running PostgreSQL TPC-H SF1: runs=$RUNS warmup=$WARMUP"
if ! run_with_watchdog \
    "PostgreSQL TPC-H SF1 queries" \
    target/release/tpch run-queries postgres \
        --pg-dsn "$POSTGRES_DSN" \
        --runs "$RUNS" \
        --warmup "$WARMUP" \
        --queries "$QUERIES" \
        --scale 1 \
        --out "$TMP_POSTGRES_OUT"; then
    write_setup_summary "postgres_run_failed" "PostgreSQL did not complete all TPC-H SF1 queries."
    exit 1
fi

echo "Running UltraSQL TPC-H SF1: runs=$RUNS warmup=$WARMUP"
if ! run_with_watchdog \
    "UltraSQL TPC-H SF1 queries" \
    env \
        ULTRASQL_TPCH_POOL_FRAMES="$ULTRASQL_POOL_FRAMES" \
        ULTRASQL_TPCH_PROGRESS="$ULTRASQL_PROGRESS" \
        ULTRASQL_PAGE_SPILL_BACKING="$ULTRASQL_SPILL_BACKING" \
    target/release/tpch run-queries ultrasql \
        --data-dir "$TPCH_DATA_DIR" \
        --runs "$RUNS" \
        --warmup "$WARMUP" \
        --queries "$QUERIES" \
        --scale 1 \
        --out "$TMP_ULTRA_OUT"; then
    write_setup_summary "ultrasql_run_failed" "UltraSQL did not complete all TPC-H SF1 queries."
    exit 1
fi

set +e
python3 - "$TMP_POSTGRES_OUT" "$TMP_ULTRA_OUT" "$POSTGRES_OUT" "$ULTRA_OUT" "$SUMMARY_OUT" <<'PY'
import json
import math
import pathlib
import sys

postgres_read_path, ultrasql_read_path, postgres_final_path, ultrasql_final_path, out_path = map(pathlib.Path, sys.argv[1:])
postgres = json.loads(postgres_read_path.read_text())
ultrasql = json.loads(ultrasql_read_path.read_text())

expected_queries = {f"q{i}" for i in range(1, 23)}
postgres_queries = set(postgres.get("queries", {}))
ultrasql_queries = set(ultrasql.get("queries", {}))

def sort_queries(values):
    return sorted(values, key=lambda q: int(q[1:]) if q.startswith("q") and q[1:].isdigit() else q)

base_summary = {
    "schema_version": 1,
    "workload": "tpch_sf1_postgres",
    "scale_factor": 1,
    "target": "UltraSQL geometric mean <= 0.5x PostgreSQL 17 geometric mean across all 22 TPC-H queries",
    "expected_query_count": len(expected_queries),
    "postgres_query_count": len(postgres_queries),
    "ultrasql_query_count": len(ultrasql_queries),
    "postgres_queries": sort_queries(postgres_queries),
    "ultrasql_queries": sort_queries(ultrasql_queries),
    "missing_postgres_queries": sort_queries(expected_queries - postgres_queries),
    "missing_ultrasql_queries": sort_queries(expected_queries - ultrasql_queries),
    "extra_postgres_queries": sort_queries(postgres_queries - expected_queries),
    "extra_ultrasql_queries": sort_queries(ultrasql_queries - expected_queries),
    "postgres_result": str(postgres_final_path),
    "ultrasql_result": str(ultrasql_final_path),
    "postgres_physical_design": (
        "TPC-H primary keys plus disclosed secondary indexes from "
        "benchmarks/tpch_sf1_postgres_certify.sh; per-table autovacuum disabled "
        "during timed runs; ANALYZE after load and index build."
    ),
    "policy": "TPC-H SF1 PostgreSQL certification publishes complete q1..q22 raw artifacts only; no partial query set is retained.",
}

if postgres_queries != expected_queries or ultrasql_queries != expected_queries:
    summary = {
        **base_summary,
        "postgres_geomean_ms": None,
        "ultrasql_geomean_ms": None,
        "throughput_ratio_ultrasql_vs_postgres": None,
        "passed": False,
        "status": "failed",
        "reason": "incomplete_query_set",
        "next_step": (
            "Rerun benchmarks/tpch_sf1_postgres_certify.sh with TPCH_QUERIES=all; "
            "certification remains open until both raw artifacts contain q1..q22."
        ),
    }
    out_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    sys.exit(3)

def gm(doc):
    vals = []
    for query_id in sort_queries(expected_queries):
        timing = doc["queries"][query_id]
        median_ms = timing.get("median_ms")
        if not median_ms or not math.isfinite(median_ms) or median_ms <= 0:
            return None
        vals.append(median_ms)
    return math.exp(sum(math.log(v) for v in vals) / len(vals))

postgres_gm = gm(postgres)
ultrasql_gm = gm(ultrasql)
ratio = None
if postgres_gm and ultrasql_gm:
    ratio = postgres_gm / ultrasql_gm
passed = (
    postgres_gm is not None
    and ultrasql_gm is not None
    and ultrasql_gm <= postgres_gm * 0.5
)
summary = {
    **base_summary,
    "postgres_geomean_ms": postgres_gm,
    "ultrasql_geomean_ms": ultrasql_gm,
    "throughput_ratio_ultrasql_vs_postgres": ratio,
    "passed": passed,
    "status": "passed" if passed else "failed",
    "reason": None if passed else "performance_target_missed_or_query_failed",
    "next_step": (
        "Keep benchmarks/certify.sh full tpch-sf1-postgres green for release evidence."
        if passed
        else
        "Keep optimizing TPC-H SF1 until UltraSQL geometric mean is at least "
        "2x faster than PostgreSQL 17 on the same host."
    ),
}
out_path.write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
sys.exit(0 if passed else 1)
PY
summary_status=$?
set -e

if [[ "$summary_status" != "3" ]]; then
    mv "$TMP_POSTGRES_OUT" "$POSTGRES_OUT"
    mv "$TMP_ULTRA_OUT" "$ULTRA_OUT"
fi

exit "$summary_status"
exit "$summary_status"
