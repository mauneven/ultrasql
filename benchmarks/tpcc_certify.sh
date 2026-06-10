#!/usr/bin/env bash
# Reproducible TPC-C v1.0 certification runner.
#
# A pass requires same-host UltraSQL and PostgreSQL 17 results, all five
# transaction types correct, and UltraSQL throughput no lower than PostgreSQL.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

POSTGRES_RESULT="${POSTGRES_TPCC_RESULT:-}"
ULTRASQL_RESULT="${ULTRASQL_TPCC_RESULT:-}"
POSTGRES_DSN="${POSTGRES_DSN:-}"
ULTRASQL_DSN="${ULTRASQL_DSN:-}"
TPCC_WAREHOUSES="${TPCC_WAREHOUSES:-1}"
TPCC_ITEMS="${TPCC_ITEMS:-}"
TPCC_CUSTOMERS_PER_DISTRICT="${TPCC_CUSTOMERS_PER_DISTRICT:-}"
TPCC_INITIAL_ORDERS_PER_DISTRICT="${TPCC_INITIAL_ORDERS_PER_DISTRICT:-}"
TPCC_ORDER_LINES="${TPCC_ORDER_LINES:-5}"
TPCC_DURATION="${TPCC_DURATION:-60}"
TPCC_WARMUP="${TPCC_WARMUP:-30}"
TPCC_CONNECTIONS="${TPCC_CONNECTIONS:-32}"
AUTO_POSTGRES="${TPCC_AUTO_POSTGRES:-1}"
POSTGRES_IMAGE="${TPCC_POSTGRES_IMAGE:-postgres:17}"
POSTGRES_CONTAINER="${TPCC_POSTGRES_CONTAINER:-ultrasql-postgres-tpcc}"
POSTGRES_PORT="${TPCC_POSTGRES_PORT:-55433}"
POSTGRES_PASSWORD="${TPCC_POSTGRES_PASSWORD:-postgres}"
OUT_DIR="${TPCC_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="$OUT_DIR/raw"
SUMMARY_OUT="$OUT_DIR/tpcc_certification.json"

mkdir -p "$RAW_DIR"

docker_context_host() {
    local context
    context="${DOCKER_CONTEXT:-}"
    if [[ -z "$context" ]]; then
        context="$(docker context show 2>/dev/null || true)"
    fi
    if [[ -n "$context" ]]; then
        docker context inspect "$context" --format '{{ .Endpoints.docker.Host }}' 2>/dev/null || true
    fi
}

docker_without_desktop_creds() {
    local tmp_config
    local context_host
    tmp_config="$(mktemp -d)"
    printf '{"auths":{}}\n' > "$tmp_config/config.json"
    context_host="$(docker_context_host)"
    if [[ -n "$context_host" ]]; then
        DOCKER_CONFIG="$tmp_config" DOCKER_HOST="$context_host" docker "$@"
    else
        DOCKER_CONFIG="$tmp_config" docker "$@"
    fi
    local status=$?
    rm -rf "$tmp_config"
    return "$status"
}

docker_cmd() {
    docker_without_desktop_creds "$@"
}

postgres_tcp_ready() {
    python3 - "$POSTGRES_PORT" <<'PY'
import socket
import sys

port = int(sys.argv[1])
try:
    with socket.create_connection(("127.0.0.1", port), timeout=1.0):
        pass
except OSError:
    raise SystemExit(1)
PY
}

try_start_local_postgres() {
    if [[ "$AUTO_POSTGRES" == "0" ]] || ! command -v docker >/dev/null 2>&1; then
        return 1
    fi

    if docker_cmd inspect "$POSTGRES_CONTAINER" >/dev/null 2>&1; then
        docker_cmd start "$POSTGRES_CONTAINER" >/dev/null || return 1
    else
        docker_cmd run -d \
            --name "$POSTGRES_CONTAINER" \
            -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
            -e POSTGRES_DB=postgres \
            -p "127.0.0.1:${POSTGRES_PORT}:5432" \
            "$POSTGRES_IMAGE" >/dev/null || return 1
    fi

    for _ in $(seq 1 60); do
        if docker_cmd exec "$POSTGRES_CONTAINER" pg_isready -U postgres -d postgres >/dev/null 2>&1 && postgres_tcp_ready; then
            POSTGRES_DSN="host=127.0.0.1 port=${POSTGRES_PORT} user=postgres password=${POSTGRES_PASSWORD} dbname=postgres"
            return 0
        fi
        sleep 1
    done
    return 1
}

SHAPE_ARGS=(
    --warehouses "$TPCC_WAREHOUSES"
    --order-lines "$TPCC_ORDER_LINES"
    --duration "$TPCC_DURATION"
    --warmup "$TPCC_WARMUP"
    --connections "$TPCC_CONNECTIONS"
)
if [[ -n "$TPCC_ITEMS" ]]; then
    SHAPE_ARGS+=(--items "$TPCC_ITEMS")
fi
if [[ -n "$TPCC_CUSTOMERS_PER_DISTRICT" ]]; then
    SHAPE_ARGS+=(--customers-per-district "$TPCC_CUSTOMERS_PER_DISTRICT")
fi
if [[ -n "$TPCC_INITIAL_ORDERS_PER_DISTRICT" ]]; then
    SHAPE_ARGS+=(--initial-orders-per-district "$TPCC_INITIAL_ORDERS_PER_DISTRICT")
fi

if [[ -z "$POSTGRES_RESULT" && -z "$POSTGRES_DSN" ]]; then
    try_start_local_postgres || true
fi

if [[ -z "$POSTGRES_RESULT" && -z "$POSTGRES_DSN" ]]; then
    python3 - "$SUMMARY_OUT" <<'PY'
import json
import pathlib
import sys

summary_path = sys.argv[1]
doc = {
    "workload": "tpcc_5types",
    "target": "UltraSQL throughput >= PostgreSQL 17 with all five TPC-C transaction types correct",
    "passed": False,
    "reason": "postgres_dsn_missing",
    "postgres_result": None,
    "ultrasql_result": None,
    "next_step": (
        "Set POSTGRES_DSN or POSTGRES_TPCC_RESULT for certification, or "
        "install Docker so TPCC_AUTO_POSTGRES=1 can start a local postgres:17."
    ),
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
    exit 2
fi

CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release --package ultrasql-bench --features sql-bench \
        --bin ultrasql-bench >/dev/null

if [[ -z "$ULTRASQL_RESULT" ]]; then
    ULTRASQL_RESULT="$RAW_DIR/tpcc_5types-ultrasql.json"
    TMP_ULTRASQL_RESULT="${ULTRASQL_RESULT}.tmp.$$"
    ULTRASQL_ARGS=(tpcc --engine ultrasql "${SHAPE_ARGS[@]}")
    if [[ -n "$ULTRASQL_DSN" ]]; then
        ULTRASQL_ARGS+=(--dsn "$ULTRASQL_DSN")
    fi
    ULTRASQL_ARGS+=(--output "$TMP_ULTRASQL_RESULT")
    if target/release/ultrasql-bench "${ULTRASQL_ARGS[@]}"; then
        mv "$TMP_ULTRASQL_RESULT" "$ULTRASQL_RESULT"
    else
        rm -f "$TMP_ULTRASQL_RESULT" "$ULTRASQL_RESULT"
    fi
fi

if [[ -z "$POSTGRES_RESULT" ]]; then
    POSTGRES_RESULT="$RAW_DIR/tpcc_5types-postgres17.json"
    if [[ -n "$POSTGRES_DSN" ]]; then
        TMP_POSTGRES_RESULT="${POSTGRES_RESULT}.tmp.$$"
        POSTGRES_ARGS=(tpcc --engine postgres17 --dsn "$POSTGRES_DSN" "${SHAPE_ARGS[@]}")
        POSTGRES_ARGS+=(--output "$TMP_POSTGRES_RESULT")
        if target/release/ultrasql-bench "${POSTGRES_ARGS[@]}"; then
            mv "$TMP_POSTGRES_RESULT" "$POSTGRES_RESULT"
        else
            rm -f "$TMP_POSTGRES_RESULT" "$POSTGRES_RESULT"
        fi
    fi
fi

python3 - "$SUMMARY_OUT" "$POSTGRES_RESULT" "$ULTRASQL_RESULT" <<'PY'
import json
import pathlib
import sys

summary_path, pg_path, ultra_path = sys.argv[1:]

def load(path):
    if not path:
        return None
    p = pathlib.Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text())

def correct(doc):
    return bool(doc.get("correctness", {}).get("passed", False))

def all_five(doc):
    return bool(doc.get("correctness", {}).get("all_five_transaction_types", False))

pg = load(pg_path)
ultra = load(ultra_path)
reason = None
passed = False
pg_tps = None
ul_tps = None
ratio = None
pg_correct = None
ul_correct = None
pg_all_five = None
ul_all_five = None
if pg is not None:
    pg_tps = float(pg.get("throughput_per_sec", 0.0))
    pg_correct = correct(pg)
    pg_all_five = all_five(pg)
if ultra is not None:
    ul_tps = float(ultra.get("throughput_per_sec", 0.0))
    ul_correct = correct(ultra)
    ul_all_five = all_five(ultra)
if pg_tps and ul_tps:
    ratio = ul_tps / pg_tps
if pg is None or ultra is None:
    reason = "missing_cross_engine_results"
else:
    passed = (
        pg_correct
        and ul_correct
        and pg_all_five
        and ul_all_five
        and pg_tps > 0
        and ul_tps >= pg_tps
    )
    if not passed:
        reason = "target_not_met"

doc = {
    "workload": "tpcc_5types",
    "target": "UltraSQL throughput >= PostgreSQL 17 with all five TPC-C transaction types correct",
    "passed": passed,
    "reason": reason,
    "postgres_result": pg_path or None,
    "ultrasql_result": ultra_path or None,
    "postgres_correct": pg_correct,
    "ultrasql_correct": ul_correct,
    "postgres_all_five_transaction_types": pg_all_five,
    "ultrasql_all_five_transaction_types": ul_all_five,
    "postgres_throughput_per_sec": pg_tps,
    "ultrasql_throughput_per_sec": ul_tps,
    "throughput_ratio_ultrasql_vs_postgres": ratio,
    "next_step": (
        "Rerun benchmarks/tpcc_certify.sh on a quiet host after removing "
        "remaining UltraSQL OLTP bottlenecks."
    ),
}
pathlib.Path(summary_path).write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(2 if reason == "missing_cross_engine_results" else (0 if passed else 1))
PY
