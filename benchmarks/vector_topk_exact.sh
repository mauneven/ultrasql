#!/usr/bin/env bash
# Exact vector top-k cross-engine benchmark.
#
# Measures UltraSQL through `cross_compare_sql --workload vector-top-k`, then
# prefers PostgreSQL + pgvector when the extension already exists or can be
# created in the target database. If pgvector is unavailable, falls back to
# DuckDB only when an installed DuckDB build exposes LIST/ARRAY distance
# functions. No proprietary tests or benchmark assets are downloaded.

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
    printf '{"engine":"%s","status":"not_available","reason":"%s","workload":"%s","n_rows":%d,"vector_dims":%d,"top_k":%d}\n' \
        "$engine" "$reason" "$WORKLOAD" "$ROWS" "$DIMS" "$TOP_K" > "$out"
}

emit_json() {
    local engine="$1"
    local out="$2"
    local answer="$3"
    shift 3
    python3 - "$engine" "$WORKLOAD" "$ROWS" "$DIMS" "$TOP_K" "$answer" "$@" <<'PY' > "$out"
import json
import statistics
import sys

engine, workload = sys.argv[1], sys.argv[2]
rows, dims, top_k = map(int, sys.argv[3:6])
answer = sys.argv[6]
samples = [float(value) for value in sys.argv[7:]]
doc = {
    "engine": engine,
    "workload": workload,
    "n_rows": rows,
    "vector_dims": dims,
    "top_k": top_k,
    "exact": True,
    "metric": "l2",
    "samples": len(samples),
    "median_us": statistics.median(samples),
    "min_us": min(samples),
    "iterations_us": samples,
    "answer": answer,
}
print(json.dumps(doc, separators=(",", ":")))
PY
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
    else:
        raise SystemExit(f"unknown dialect: {dialect}")

    chunk = 1000
    for start in range(0, rows, chunk):
        end = min(start + chunk, rows)
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
    if [[ -z "${POSTGRES_DSN:-}" ]]; then
        createdb -U "$PGUSER" "$PGDATABASE" >/dev/null 2>&1 || true
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
    postgres_psql -f "$setup_sql" >/dev/null

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
            return 2
        fi
        if (( i >= WARMUP )); then
            dt="$(sample_delta_us "$t0" "$t1")"
            samples+=("$dt")
        fi
    done

    emit_json "$engine" "$RAW_DIR/${WORKLOAD}-${engine}.json" "$expected" "${samples[@]}"
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
    duckdb "$db_path" < "$setup_sql" >/dev/null

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
            return 2
        fi
        if (( i >= WARMUP )); then
            dt="$(sample_delta_us "$t0" "$t1")"
            samples+=("$dt")
        fi
    done

    emit_json "$engine" "$RAW_DIR/${WORKLOAD}-${engine}.json" "$expected" "${samples[@]}"
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
case "$pg_status" in
    0)
        echo "postgres17_pgvector measured"
        ;;
    1)
        echo "postgres17_pgvector unavailable; trying DuckDB LIST fallback"
        duck_status=0
        run_duckdb_list || duck_status=$?
        if (( duck_status == 2 )); then
            exit 2
        fi
        ;;
    *)
        exit "$pg_status"
        ;;
esac

echo "--- Rendering benchmark tables ---"
target/release/results-render \
    --raw-dir "$RAW_DIR" \
    --output-md "$OUT_DIR/results.md" \
    --output-json "$OUT_DIR/results.json"

echo "=== Done. Raw artifacts in $RAW_DIR ==="
