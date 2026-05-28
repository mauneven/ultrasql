#!/usr/bin/env bash
# Exact vector top-k cross-engine benchmark.
#
# Measures UltraSQL through `cross_compare_sql --workload vector-top-k`, then
# attempts PostgreSQL + pgvector, DuckDB LIST/ARRAY distance, and ClickHouse
# Array(Float64) exact scans when those engines are already installed and
# reachable. No proprietary tests or benchmark assets are downloaded.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${VECTOR_TOPK_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
ROWS="${VECTOR_TOPK_ROWS:-10000}"
DIMS="${VECTOR_TOPK_DIMS:-8}"
TOP_K="${VECTOR_TOPK_K:-10}"
N_ITERS="${N_ITERS:-8}"
WARMUP="${WARMUP:-2}"
PGDATABASE="${PGDATABASE:-ultrasql_bench}"
PGUSER="${PGUSER:-$(id -un)}"
REQUIRE_PGVECTOR="${VECTOR_TOPK_REQUIRE_PGVECTOR:-0}"
AUTO_PGVECTOR="${VECTOR_TOPK_AUTO_PGVECTOR:-1}"
PGVECTOR_IMAGE="${VECTOR_TOPK_PGVECTOR_IMAGE:-pgvector/pgvector:pg17}"
PGVECTOR_CONTAINER="${VECTOR_TOPK_PGVECTOR_CONTAINER:-ultrasql-pgvector-topk}"
PGVECTOR_PORT="${VECTOR_TOPK_PGVECTOR_PORT:-55433}"
PGVECTOR_PASSWORD="${VECTOR_TOPK_PGVECTOR_PASSWORD:-postgres}"
CLICKHOUSE_BIN="${CLICKHOUSE_BIN:-clickhouse}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-default}"
REQUIRED_VECTOR_METRICS="recall_at_k,p50_latency_us,p95_latency_us,p99_latency_us,build_time_us,memory_bytes,index_size_bytes"

mkdir -p "$RAW_DIR"

if (( ROWS < 0 || DIMS <= 0 || TOP_K <= 0 || N_ITERS <= 0 || WARMUP < 0 )); then
    echo "vector_topk_exact.sh: invalid ROWS/DIMS/TOP_K/N_ITERS/WARMUP" >&2
    exit 2
fi

row_label="$(
    python3 - "$ROWS" <<'PY'
import sys
n = int(sys.argv[1])
if n >= 1_000_000 and n % 1_000_000 == 0:
    print(f"{n // 1_000_000}m")
elif n >= 1_000 and n % 1_000 == 0:
    print(f"{n // 1_000}k")
elif n == 65_536:
    print("65k")
else:
    print(str(n))
PY
)"
WORKLOAD="vector_topk_exact_${row_label}_${DIMS}d_k${TOP_K}"

emit_not_available() {
    local engine="$1"
    local reason="$2"
    local out="$RAW_DIR/${WORKLOAD}-${engine}.json"
    python3 - "$engine" "$reason" "$WORKLOAD" "$ROWS" "$DIMS" "$TOP_K" "$REQUIRED_VECTOR_METRICS" <<'PY' > "$out"
import json
import os
import platform
import subprocess
import sys

engine, reason, workload = sys.argv[1], sys.argv[2], sys.argv[3]
rows, dims, top_k = map(int, sys.argv[4:7])
required_metrics = sys.argv[7].split(",")

def host_info():
    cpu = os.environ.get("BENCH_CPU_MODEL")
    if not cpu and platform.system() == "Darwin":
        try:
            cpu = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
        except Exception:
            cpu = None
    if not cpu:
        cpu = platform.machine()
    cores = int(os.environ.get("BENCH_CPU_CORES") or (os.cpu_count() or 0))
    ram_gb = os.environ.get("BENCH_RAM_GB")
    if ram_gb is None:
        mem_bytes = 0
        if platform.system() == "Darwin":
            try:
                mem_bytes = int(
                    subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
                )
            except Exception:
                mem_bytes = 0
        else:
            try:
                for line in open("/proc/meminfo", encoding="utf-8"):
                    if line.startswith("MemTotal:"):
                        mem_bytes = int(line.split()[1]) * 1024
                        break
            except OSError:
                mem_bytes = 0
        ram_gb = str(mem_bytes // 1073741824) if mem_bytes else "0"
    os_name = {"Darwin": "macos", "Linux": "linux"}.get(platform.system(), platform.system().lower())
    os_version = os.environ.get("BENCH_OS_VERSION", "unknown")
    return {"cpu": cpu, "cores": cores, "ram_gb": int(ram_gb), "os": f"{os_name} {os_version}"}

doc = {
    "schema_version": 1,
    "engine": engine,
    "status": "not_available",
    "reason": reason,
    "workload": workload,
    "host": host_info(),
    "n_rows": rows,
    "vector_dims": dims,
    "top_k": top_k,
    "exact": True,
    "metric": "l2",
    "required_metrics": required_metrics,
    "recall_at_k": None,
    "p50_latency_us": None,
    "p95_latency_us": None,
    "p99_latency_us": None,
    "build_time_us": None,
    "build_time_scope": "table_load_before_timed_query",
    "memory_bytes": None,
    "memory_status": "not_measured",
    "index_size_bytes": None,
    "index_size_status": "not_applicable_exact_scan",
}
print(json.dumps(doc, separators=(",", ":")))
PY
}

emit_json() {
    local engine="$1"
    local out="$2"
    local answer="$3"
    local build_time_us="$4"
    shift 4
    python3 - "$engine" "$WORKLOAD" "$ROWS" "$DIMS" "$TOP_K" "$answer" "$build_time_us" "$REQUIRED_VECTOR_METRICS" "$@" <<'PY' > "$out"
import json
import math
import os
import platform
import statistics
import subprocess
import sys

engine, workload = sys.argv[1], sys.argv[2]
rows, dims, top_k = map(int, sys.argv[3:6])
answer = sys.argv[6]
build_time_us = float(sys.argv[7])
required_metrics = sys.argv[8].split(",")
samples = [float(value) for value in sys.argv[9:]]

# HostInfo-equivalent host metadata, matching `ultrasql-bench` artifacts.
def host_info():
    cpu = os.environ.get("BENCH_CPU_MODEL")
    if not cpu and platform.system() == "Darwin":
        try:
            cpu = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
        except Exception:
            cpu = None
    if not cpu:
        cpu = platform.machine()
    cores = int(os.environ.get("BENCH_CPU_CORES") or (os.cpu_count() or 0))
    ram_gb = os.environ.get("BENCH_RAM_GB")
    if ram_gb is None:
        mem_bytes = 0
        if platform.system() == "Darwin":
            try:
                mem_bytes = int(
                    subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
                )
            except Exception:
                mem_bytes = 0
        else:
            try:
                for line in open("/proc/meminfo", encoding="utf-8"):
                    if line.startswith("MemTotal:"):
                        mem_bytes = int(line.split()[1]) * 1024
                        break
            except OSError:
                mem_bytes = 0
        ram_gb = str(mem_bytes // 1073741824) if mem_bytes else "0"
    os_name = {"Darwin": "macos", "Linux": "linux"}.get(platform.system(), platform.system().lower())
    os_version = os.environ.get("BENCH_OS_VERSION", "unknown")
    return {"cpu": cpu, "cores": cores, "ram_gb": int(ram_gb), "os": f"{os_name} {os_version}"}

def percentile_nearest_rank(values, percentile):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * percentile) - 1))
    return ordered[index]

doc = {
    "schema_version": 1,
    "engine": engine,
    "status": "measured",
    "workload": workload,
    "host": host_info(),
    "n_rows": rows,
    "vector_dims": dims,
    "top_k": top_k,
    "exact": True,
    "metric": "l2",
    "required_metrics": required_metrics,
    "samples": len(samples),
    "median_us": statistics.median(samples),
    "min_us": min(samples),
    "recall_at_k": 1.0,
    "p50_latency_us": percentile_nearest_rank(samples, 0.50),
    "p95_latency_us": percentile_nearest_rank(samples, 0.95),
    "p99_latency_us": percentile_nearest_rank(samples, 0.99),
    "build_time_us": build_time_us,
    "build_time_scope": "table_load_before_timed_query",
    "memory_bytes": None,
    "memory_status": "not_measured",
    "index_size_bytes": None,
    "index_size_status": "not_applicable_exact_scan",
    "iterations_us": samples,
    "answer": answer,
    "policy": "Raw measured samples only; same-host certification requires paired UltraSQL and PostgreSQL+pgvector measured artifacts.",
}
print(json.dumps(doc, separators=(",", ":")))
PY
}

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

pgvector_tcp_ready() {
    python3 - "$PGVECTOR_PORT" <<'PY'
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

try_start_local_pgvector() {
    if [[ "$AUTO_PGVECTOR" == "0" ]] || [[ -n "${POSTGRES_DSN:-}" ]] || ! command -v docker >/dev/null 2>&1; then
        return 1
    fi

    if docker_cmd inspect "$PGVECTOR_CONTAINER" >/dev/null 2>&1; then
        docker_cmd start "$PGVECTOR_CONTAINER" >/dev/null || return 1
    else
        docker_cmd run -d \
            --name "$PGVECTOR_CONTAINER" \
            -e POSTGRES_PASSWORD="$PGVECTOR_PASSWORD" \
            -e POSTGRES_DB=postgres \
            -p "127.0.0.1:${PGVECTOR_PORT}:5432" \
            "$PGVECTOR_IMAGE" >/dev/null || return 1
    fi

    for _ in $(seq 1 60); do
        if docker_cmd exec "$PGVECTOR_CONTAINER" pg_isready -U postgres -d postgres >/dev/null 2>&1 && pgvector_tcp_ready; then
            POSTGRES_DSN="host=127.0.0.1 port=${PGVECTOR_PORT} user=postgres password=${PGVECTOR_PASSWORD} dbname=postgres"
            return 0
        fi
        sleep 1
    done
    return 1
}

write_setup_sql() {
    local dialect="$1"
    local sql_out="$2"
    local expected_out="$3"
    python3 - "$dialect" "$ROWS" "$DIMS" "$TOP_K" "$sql_out" "$expected_out" <<'PY'
import sys

dialect, rows, dims, top_k, sql_out, expected_out = (
    sys.argv[1],
    int(sys.argv[2]),
    int(sys.argv[3]),
    int(sys.argv[4]),
    sys.argv[5],
    sys.argv[6],
)

def component(row_id: int, dim: int) -> int:
    return ((row_id * 31) + (dim * 17) + ((row_id % 7) * 13)) % 101 - 50

def probe_component(dim: int) -> int:
    return ((dim * 7) + 3) % 23 - 11

def vector_literal(row_id: int) -> str:
    return "[" + ",".join(str(component(row_id, dim)) for dim in range(dims)) + "]"

def dist2(row_id: int) -> int:
    return sum((component(row_id, dim) - probe_component(dim)) ** 2 for dim in range(dims))

expected = ",".join(
    str(row_id) for _, row_id in sorted((dist2(row_id), row_id) for row_id in range(rows))[: min(top_k, rows)]
)
with open(expected_out, "w", encoding="utf-8") as f:
    f.write(expected)

with open(sql_out, "w", encoding="utf-8") as f:
    if dialect == "postgres":
        f.write("DROP TABLE IF EXISTS bench_vector_topk;\n")
        f.write(f"CREATE UNLOGGED TABLE bench_vector_topk (id INT NOT NULL, embedding vector({dims}));\n")
    elif dialect == "duckdb":
        f.write("CREATE OR REPLACE TABLE bench_vector_topk (id INTEGER, embedding DOUBLE[]);\n")
    elif dialect == "clickhouse":
        f.write("DROP TABLE IF EXISTS bench_vector_topk;\n")
        f.write("CREATE TABLE bench_vector_topk (id UInt32, embedding Array(Float64)) ENGINE=Memory;\n")
    else:
        raise SystemExit(f"unknown dialect: {dialect}")

    chunk = 1000
    for start in range(0, rows, chunk):
        end = min(start + chunk, rows)
        if dialect == "clickhouse":
            values = ",".join(f"({row_id},{vector_literal(row_id)})" for row_id in range(start, end))
        else:
            values = ",".join(f"({row_id},'{vector_literal(row_id)}')" for row_id in range(start, end))
        if values:
            f.write(f"INSERT INTO bench_vector_topk VALUES {values};\n")
PY
}

postgres_psql() {
    if [[ -n "${POSTGRES_DSN:-}" ]]; then
        psql "$POSTGRES_DSN" -q --no-align -t -v ON_ERROR_STOP=1 "$@"
    else
        psql -U "$PGUSER" -d "$PGDATABASE" -q --no-align -t -v ON_ERROR_STOP=1 "$@"
    fi
}

ensure_pgvector_database() {
    if [[ -n "${POSTGRES_DSN:-}" ]]; then
        return 0
    fi
    if createdb -U "$PGUSER" "$PGDATABASE" >/dev/null 2>&1; then
        return 0
    fi

    local exists
    local status
    set +e
    exists="$(psql -U "$PGUSER" -d postgres -q --no-align -t -v ON_ERROR_STOP=1 \
        --set=db="$PGDATABASE" 2>/dev/null <<'SQL'
SELECT 1 FROM pg_database WHERE datname = :'db';
SQL
)"
    status=$?
    set -e
    if [[ "$status" -eq 0 && "$exists" == "1" ]]; then
        return 0
    fi

    echo "vector_topk_exact.sh: ERROR: failed to create PostgreSQL database '$PGDATABASE'" >&2
    return 1
}

time_python() {
    python3 -c 'import time; print(time.perf_counter())'
}

sample_delta_us() {
    python3 - "$1" "$2" <<'PY'
import sys
print((float(sys.argv[2]) - float(sys.argv[1])) * 1e6)
PY
}

normalize_ids() {
    awk 'NF {print $1}' | paste -sd, -
}

run_postgres_pgvector() {
    local engine="postgres17_pgvector"
    if ! command -v psql >/dev/null 2>&1; then
        emit_not_available "$engine" "psql_not_found"
        return 1
    fi
    ensure_pgvector_database || return 1
    if ! postgres_psql -c "SELECT 1" >/dev/null 2>&1 || ! postgres_psql -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1; then
        if try_start_local_pgvector; then
            :
        else
            emit_not_available "$engine" "pgvector_extension_unavailable"
            return 1
        fi
    fi
    if ! postgres_psql -c "SELECT 1" >/dev/null 2>&1; then
        emit_not_available "$engine" "postgres_connection_failed"
        return 1
    fi
    if ! postgres_psql -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1; then
        emit_not_available "$engine" "pgvector_extension_unavailable"
        return 1
    fi

    local tmp_dir setup_sql expected_file expected probe query
    tmp_dir="$(mktemp -d /tmp/ultrasql-vector-pg-XXXXXX)"
    setup_sql="$tmp_dir/setup.sql"
    expected_file="$tmp_dir/expected.txt"
    write_setup_sql postgres "$setup_sql" "$expected_file"
    expected="$(cat "$expected_file")"
    probe="$(
        python3 - "$DIMS" <<'PY'
import sys
dims = int(sys.argv[1])
print("[" + ",".join(str(((dim * 7) + 3) % 23 - 11) for dim in range(dims)) + "]")
PY
    )"
    local build_t0 build_t1 build_time_us
    build_t0="$(time_python)"
    postgres_psql -f "$setup_sql" >/dev/null
    build_t1="$(time_python)"
    build_time_us="$(sample_delta_us "$build_t0" "$build_t1")"

    query="SELECT id FROM bench_vector_topk ORDER BY embedding <-> '${probe}'::vector, id LIMIT ${TOP_K};"
    local samples=()
    for ((i = 0; i < WARMUP + N_ITERS; i++)); do
        local t0 t1 observed dt
        t0="$(time_python)"
        observed="$(postgres_psql -c "$query" | normalize_ids)"
        t1="$(time_python)"
        if [[ "$observed" != "$expected" ]]; then
            echo "postgres pgvector top-k mismatch: expected $expected observed $observed" >&2
            rm -rf "$tmp_dir"
            return 1
        fi
        if (( i >= WARMUP )); then
            dt="$(sample_delta_us "$t0" "$t1")"
            samples+=("$dt")
        fi
    done

    emit_json "$engine" "$RAW_DIR/${WORKLOAD}-${engine}.json" "$expected" "$build_time_us" "${samples[@]}"
    rm -rf "$tmp_dir"
    return 0
}

detect_duckdb_distance_fn() {
    local candidate
    for candidate in list_distance array_distance; do
        if duckdb :memory: -c "SELECT ${candidate}([1.0,2.0]::DOUBLE[], [2.0,4.0]::DOUBLE[]);" >/dev/null 2>&1; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

run_duckdb_list() {
    local engine="duckdb_list"
    if ! command -v duckdb >/dev/null 2>&1; then
        emit_not_available "$engine" "duckdb_not_found"
        return 1
    fi

    local distance_fn
    if ! distance_fn="$(detect_duckdb_distance_fn)"; then
        emit_not_available "$engine" "duckdb_list_distance_unavailable"
        return 1
    fi

    local tmp_dir db_path setup_sql expected_file expected probe query
    tmp_dir="$(mktemp -d /tmp/ultrasql-vector-duckdb-XXXXXX)"
    db_path="$tmp_dir/vector_topk.duckdb"
    setup_sql="$tmp_dir/setup.sql"
    expected_file="$tmp_dir/expected.txt"
    write_setup_sql duckdb "$setup_sql" "$expected_file"
    expected="$(cat "$expected_file")"
    probe="$(
        python3 - "$DIMS" <<'PY'
import sys
dims = int(sys.argv[1])
print("[" + ",".join(str(((dim * 7) + 3) % 23 - 11) for dim in range(dims)) + "]")
PY
    )"
    local build_t0 build_t1 build_time_us
    build_t0="$(time_python)"
    duckdb "$db_path" < "$setup_sql" >/dev/null
    build_t1="$(time_python)"
    build_time_us="$(sample_delta_us "$build_t0" "$build_t1")"

    query="SELECT id FROM bench_vector_topk ORDER BY ${distance_fn}(embedding, ${probe}::DOUBLE[]), id LIMIT ${TOP_K};"
    local samples=()
    for ((i = 0; i < WARMUP + N_ITERS; i++)); do
        local t0 t1 observed dt
        t0="$(time_python)"
        observed="$(duckdb "$db_path" -csv -noheader -c "$query" | normalize_ids)"
        t1="$(time_python)"
        if [[ "$observed" != "$expected" ]]; then
            echo "duckdb LIST top-k mismatch: expected $expected observed $observed" >&2
            rm -rf "$tmp_dir"
            return 1
        fi
        if (( i >= WARMUP )); then
            dt="$(sample_delta_us "$t0" "$t1")"
            samples+=("$dt")
        fi
    done

    emit_json "$engine" "$RAW_DIR/${WORKLOAD}-${engine}.json" "$expected" "$build_time_us" "${samples[@]}"
    rm -rf "$tmp_dir"
    return 0
}

clickhouse_client() {
    local query="$1"
    local args=(client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user "$CLICKHOUSE_USER" --database "$CLICKHOUSE_DATABASE")
    if [[ -n "${CLICKHOUSE_PASSWORD:-}" ]]; then
        args+=(--password "$CLICKHOUSE_PASSWORD")
    fi
    "$CLICKHOUSE_BIN" "${args[@]}" --multiquery --query "$query"
}

clickhouse_client_file() {
    local sql_file="$1"
    local args=(client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user "$CLICKHOUSE_USER" --database "$CLICKHOUSE_DATABASE")
    if [[ -n "${CLICKHOUSE_PASSWORD:-}" ]]; then
        args+=(--password "$CLICKHOUSE_PASSWORD")
    fi
    "$CLICKHOUSE_BIN" "${args[@]}" --multiquery < "$sql_file"
}

run_clickhouse_vector() {
    local engine="clickhouse_vector"
    if ! command -v "$CLICKHOUSE_BIN" >/dev/null 2>&1; then
        emit_not_available "$engine" "clickhouse_not_found"
        return 1
    fi
    if ! clickhouse_client "SELECT 1" >/dev/null 2>&1; then
        emit_not_available "$engine" "clickhouse_connection_failed"
        return 1
    fi

    local tmp_dir setup_sql expected_file expected probe query
    tmp_dir="$(mktemp -d /tmp/ultrasql-vector-clickhouse-XXXXXX)"
    setup_sql="$tmp_dir/setup.sql"
    expected_file="$tmp_dir/expected.txt"
    write_setup_sql clickhouse "$setup_sql" "$expected_file"
    expected="$(cat "$expected_file")"
    probe="$(
        python3 - "$DIMS" <<'PY'
import sys
dims = int(sys.argv[1])
print("[" + ",".join(str(((dim * 7) + 3) % 23 - 11) for dim in range(dims)) + "]")
PY
    )"
    local build_t0 build_t1 build_time_us
    build_t0="$(time_python)"
    clickhouse_client_file "$setup_sql" >/dev/null
    build_t1="$(time_python)"
    build_time_us="$(sample_delta_us "$build_t0" "$build_t1")"

    query="SELECT id FROM bench_vector_topk ORDER BY arraySum(arrayMap((x, y) -> ((x - y) * (x - y)), embedding, ${probe})) ASC, id ASC LIMIT ${TOP_K};"
    local samples=()
    for ((i = 0; i < WARMUP + N_ITERS; i++)); do
        local t0 t1 observed dt
        t0="$(time_python)"
        observed="$(clickhouse_client "$query" | normalize_ids)"
        t1="$(time_python)"
        if [[ "$observed" != "$expected" ]]; then
            echo "clickhouse vector top-k mismatch: expected $expected observed $observed" >&2
            rm -rf "$tmp_dir"
            return 1
        fi
        if (( i >= WARMUP )); then
            dt="$(sample_delta_us "$t0" "$t1")"
            samples+=("$dt")
        fi
    done

    emit_json "$engine" "$RAW_DIR/${WORKLOAD}-${engine}.json" "$expected" "$build_time_us" "${samples[@]}"
    clickhouse_client "DROP TABLE IF EXISTS bench_vector_topk;" >/dev/null 2>&1 || true
    rm -rf "$tmp_dir"
    return 0
}

echo "=== UltraSQL exact vector top-k benchmark rows=$ROWS dims=$DIMS k=$TOP_K iters=$N_ITERS warmup=$WARMUP ==="
echo "--- Building bench binaries ---"
CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    cargo build --release \
        --package ultrasql-bench \
        --features sql-bench \
        --bin cross_compare_sql \
        --bin results-render

echo "--- Running UltraSQL wire exact top-k ---"
target/release/cross_compare_sql \
    --workload vector-top-k \
    --rows "$ROWS" \
    --vector-dims "$DIMS" \
    --top-k "$TOP_K" \
    --warmup "$WARMUP" \
    --iters "$N_ITERS" \
    --workload-id "$WORKLOAD" \
    --output "$RAW_DIR/${WORKLOAD}-ultrasql.json"

echo "--- Running competitor exact top-k ---"
pg_status=0
run_postgres_pgvector || pg_status=$?
if (( pg_status == 0 )); then
    echo "postgres17_pgvector measured"
elif (( pg_status == 1 )); then
    echo "postgres17_pgvector unavailable; recorded not_available"
    if [[ "$REQUIRE_PGVECTOR" == "1" ]]; then
        echo "VECTOR_TOPK_REQUIRE_PGVECTOR=1 requires measured PostgreSQL + pgvector artifact" >&2
        exit 2
    fi
else
    exit "$pg_status"
fi

duck_status=0
run_duckdb_list || duck_status=$?
if (( duck_status == 0 )); then
    echo "duckdb_list measured"
elif (( duck_status == 1 )); then
    echo "duckdb_list unavailable; recorded not_available"
else
    exit "$duck_status"
fi

clickhouse_status=0
run_clickhouse_vector || clickhouse_status=$?
if (( clickhouse_status == 0 )); then
    echo "clickhouse_vector measured"
elif (( clickhouse_status == 1 )); then
    echo "clickhouse_vector unavailable; recorded not_available"
else
    exit "$clickhouse_status"
fi

if [[ "${VECTOR_TOPK_RENDER_RESULTS:-1}" == "1" ]]; then
    echo "--- Rendering benchmark tables ---"
    target/release/results-render \
        --raw-dir "$RAW_DIR" \
        --output-md "$OUT_DIR/results.md" \
        --output-json "$OUT_DIR/results.json"
else
    echo "--- Skipping benchmark table render (VECTOR_TOPK_RENDER_RESULTS=0) ---"
fi

echo "=== Done. Raw artifacts in $RAW_DIR ==="
