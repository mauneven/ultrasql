#!/usr/bin/env bash
# Firebolt HNSW vector-search competitor benchmark.
#
# Runs UltraSQL's runtime HNSW artifact and local Firebolt Core Docker.
# Missing Core or unsupported vector search is not_available, not a claim.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${FIREBOLT_VECTOR_PROFILE:-${1:-smoke}}"
OUT_DIR="${FIREBOLT_VECTOR_OUT_DIR:-benchmarks/results/latest}"
RAW_DIR="${RAW_DIR:-$OUT_DIR/raw}"
MANIFEST="$OUT_DIR/firebolt_vector_search_manifest.json"
ENGINES="${FIREBOLT_VECTOR_ENGINES:-ultrasql,firebolt}"
FIREBOLT_CORE_ENDPOINT="${FIREBOLT_CORE_ENDPOINT:-http://127.0.0.1:3473}"
FIREBOLT_CORE_IMAGE="${FIREBOLT_CORE_IMAGE:-ghcr.io/firebolt-db/firebolt-core:preview-rc}"
FIREBOLT_CORE_HELPER="${FIREBOLT_CORE_HELPER:-benchmarks/firebolt_core_local.sh}"

case "$PROFILE" in
    smoke)
        ROWS="${FIREBOLT_VECTOR_ROWS:-512}"
        DIMS="${FIREBOLT_VECTOR_DIMS:-8}"
        TOP_K="${FIREBOLT_VECTOR_K:-10}"
        QUERIES="${FIREBOLT_VECTOR_QUERIES:-4}"
        WARMUP="${FIREBOLT_VECTOR_WARMUP:-0}"
        ;;
    full)
        ROWS="${FIREBOLT_VECTOR_ROWS:-10000}"
        DIMS="${FIREBOLT_VECTOR_DIMS:-8}"
        TOP_K="${FIREBOLT_VECTOR_K:-10}"
        QUERIES="${FIREBOLT_VECTOR_QUERIES:-50}"
        WARMUP="${FIREBOLT_VECTOR_WARMUP:-5}"
        ;;
    *)
        echo "firebolt_vector_search.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

M="${FIREBOLT_VECTOR_M:-16}"
EF_CONSTRUCTION="${FIREBOLT_VECTOR_EF_CONSTRUCTION:-128}"
EF_SEARCH="${FIREBOLT_VECTOR_EF_SEARCH:-64}"
SEED="${FIREBOLT_VECTOR_SEED:-1367265502}"
REQUIRED_VECTOR_METRICS="recall_at_k,p50_latency_us,p95_latency_us,p99_latency_us,build_time_us,memory_bytes,index_size_bytes"

mkdir -p "$RAW_DIR"

STATUS_FILE="$(mktemp)"
trap 'rm -f "$STATUS_FILE"' EXIT
failed=0
unavailable=0

row_label() {
    local rows="$1"
    if (( rows >= 1000000 && rows % 1000000 == 0 )); then
        echo "$(( rows / 1000000 ))m"
    elif (( rows >= 1000 && rows % 1000 == 0 )); then
        echo "$(( rows / 1000 ))k"
    elif (( rows == 65536 )); then
        echo "65k"
    else
        echo "$rows"
    fi
}

ROW_LABEL="$(row_label "$ROWS")"
WORKLOAD_ID="vector_ann_hnsw_${ROW_LABEL}_${DIMS}d_k${TOP_K}"

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

record_engine() {
    local engine="$1"
    local status="$2"
    local code="$3"
    local artifact="$4"
    printf '%s\t%s\t%s\t%s\n' "$engine" "$status" "$code" "$artifact" >>"$STATUS_FILE"
}

emit_not_available() {
    local engine="$1"
    local reason="$2"
    local out="$RAW_DIR/${WORKLOAD_ID}-${engine}.json"

    python3 - "$out" "$engine" "$PROFILE" "$ROWS" "$DIMS" "$TOP_K" "$QUERIES" "$WARMUP" "$M" "$EF_SEARCH" "$SEED" "$reason" "$WORKLOAD_ID" "$REQUIRED_VECTOR_METRICS" "$FIREBOLT_CORE_IMAGE" <<'PY'
import json
import os
import pathlib
import platform
import sys
import time

(
    out,
    engine,
    profile,
    rows,
    dims,
    top_k,
    queries,
    warmup,
    m,
    ef_search,
    seed,
    reason,
    workload,
    metrics,
    docker_image,
) = sys.argv[1:]

def host_memory_bytes():
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if isinstance(pages, int) and isinstance(page_size, int):
            return pages * page_size
    except (AttributeError, OSError, ValueError):
        return None
    return None

doc = {
    "schema_version": 1,
    "engine": engine,
    "workload": workload,
    "suite": "firebolt_vector_search",
    "profile": profile,
    "n_rows": int(rows),
    "vector_dims": int(dims),
    "top_k": int(top_k),
    "queries": int(queries),
    "warmup_queries": int(warmup),
    "m": int(m),
    "ef_search": int(ef_search),
    "seed": int(seed),
    "docker_image": docker_image,
    "firebolt_version": None,
    "core_mode": "local_docker",
    "host_cpu": os.environ.get("ULTRASQL_HOST_CPU") or platform.processor() or platform.machine(),
    "host_memory": host_memory_bytes(),
    "dataset_rows": int(rows),
    "samples": 0,
    "median_us": None,
    "p95_us": None,
    "status": "not_available",
    "reason": reason,
    "generated_at_unix": int(time.time()),
    "required_metrics": [metric for metric in metrics.split(",") if metric],
    "required_shape": {
        "ddl": (
            "CREATE INDEX idx ON documents USING HNSW "
            "(embedding vector_l2sq_ops) WITH (dimension = ?, m = ?, "
            "ef_construction = ?, quantization = 'f32')"
        ),
        "query": "SELECT id FROM VECTOR_SEARCH(INDEX idx, target_vector => ?, top_k => ?, ef_search => ?)",
    },
    "policy": (
        "No Firebolt vector-search benchmark claim exists until local "
        "Firebolt Core Docker emits measured VECTOR_SEARCH samples from "
        "reproducible inputs."
    ),
}
pathlib.Path(out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out)
PY
}

run_ultrasql() {
    local out="$RAW_DIR/${WORKLOAD_ID}-ultrasql_hnsw.json"
    VECTOR_ANN_OUT_DIR="$OUT_DIR" \
    RAW_DIR="$RAW_DIR" \
    VECTOR_ANN_ROWS="$ROWS" \
    VECTOR_ANN_DIMS="$DIMS" \
    VECTOR_ANN_K="$TOP_K" \
    VECTOR_ANN_QUERIES="$QUERIES" \
    VECTOR_ANN_WARMUP="$WARMUP" \
    VECTOR_ANN_M="$M" \
    VECTOR_ANN_EF_SEARCH="$EF_SEARCH" \
    VECTOR_ANN_SEED="$SEED" \
        benchmarks/vector_ann_hnsw.sh >/dev/null
    echo "$out"
}

run_firebolt() {
    local out="$RAW_DIR/${WORKLOAD_ID}-firebolt_hnsw.json"
    if ! FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" "$FIREBOLT_CORE_HELPER" wait >/dev/null; then
        emit_not_available "firebolt_hnsw" "$(firebolt_unavailable_reason)"
        return 2
    fi

    FIREBOLT_CORE_ENDPOINT="$FIREBOLT_CORE_ENDPOINT" \
    FIREBOLT_CORE_IMAGE="$FIREBOLT_CORE_IMAGE" \
    FIREBOLT_ROWS="$ROWS" \
    FIREBOLT_DIMS="$DIMS" \
    FIREBOLT_TOP_K="$TOP_K" \
    FIREBOLT_QUERIES="$QUERIES" \
    FIREBOLT_WARMUP="$WARMUP" \
    FIREBOLT_M="$M" \
    FIREBOLT_EF_CONSTRUCTION="$EF_CONSTRUCTION" \
    FIREBOLT_EF_SEARCH="$EF_SEARCH" \
    FIREBOLT_SEED="$SEED" \
    FIREBOLT_OUT="$out" \
    FIREBOLT_WORKLOAD="$WORKLOAD_ID" \
    FIREBOLT_VECTOR_PROFILE="$PROFILE" \
    FIREBOLT_REQUIRED_METRICS="$REQUIRED_VECTOR_METRICS" \
    python3 <<'PY'
import json
import math
import os
import pathlib
import platform
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


MASK = (1 << 64) - 1
endpoint = os.environ["FIREBOLT_CORE_ENDPOINT"]
rows = int(os.environ["FIREBOLT_ROWS"])
dims = int(os.environ["FIREBOLT_DIMS"])
top_k = int(os.environ["FIREBOLT_TOP_K"])
queries = int(os.environ["FIREBOLT_QUERIES"])
warmup = int(os.environ["FIREBOLT_WARMUP"])
m = int(os.environ["FIREBOLT_M"])
ef_construction = int(os.environ["FIREBOLT_EF_CONSTRUCTION"])
ef_search = int(os.environ["FIREBOLT_EF_SEARCH"])
seed = int(os.environ["FIREBOLT_SEED"])
out = pathlib.Path(os.environ["FIREBOLT_OUT"])
workload = os.environ["FIREBOLT_WORKLOAD"]
profile = os.environ.get("FIREBOLT_VECTOR_PROFILE", "smoke")
required_metrics = os.environ["FIREBOLT_REQUIRED_METRICS"].split(",")
timeout = float(os.environ.get("FIREBOLT_TIMEOUT_SECS", "120"))
table = f"ultrasql_firebolt_vec_{int(time.time())}_{os.getpid()}"
index = f"{table}_hnsw"


def host_memory_bytes():
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if isinstance(pages, int) and isinstance(page_size, int):
            return pages * page_size
    except (AttributeError, OSError, ValueError):
        return None
    return None


def u64(value: int) -> int:
    return value & MASK


def mix(seed_value: int, left: int, right: int, salt: int) -> int:
    x = u64(seed_value ^ salt)
    x = u64(x + u64(left * 0x9E3779B97F4A7C15))
    x ^= x >> 30
    x = u64(x * 0xBF58476D1CE4E5B9)
    x = u64(x + u64(right * 0x94D049BB133111EB))
    x ^= x >> 27
    x = u64(x * 0x94D049BB133111EB)
    return u64(x ^ (x >> 31))


def vector_for_row(row_id):
    return [((mix(seed, row_id, dim, 0x9E3779B97F4A7C15) % 2003) - 1001) / 37.0 for dim in range(dims)]


def vector_for_probe(query_id):
    probe_seed = seed ^ 0xA5A5A5A5A5A5A5A5
    return [((mix(probe_seed, query_id, dim, 0) % 2003) - 1001) / 41.0 for dim in range(dims)]


def l2_distance(vector, probe):
    return math.sqrt(sum((left - right) * (left - right) for left, right in zip(vector, probe)))


def exact_top_k(data, probe):
    scored = sorted((l2_distance(vector, probe), row_id) for row_id, vector in enumerate(data))
    return [row_id for _, row_id in scored[: min(top_k, len(scored))]]


def recall_at_k(exact, ann):
    if not exact:
        return 0.0
    exact_set = set(exact)
    return sum(1 for row_id in ann[: len(exact)] if row_id in exact_set) / len(exact)


def percentile_nearest_rank(values, percentile):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * percentile) - 1))
    return ordered[index]


def array_literal(values):
    return "[" + ",".join(f"{value:.9g}" for value in values) + "]"


def formatted_endpoint() -> str:
    separator = "&" if urllib.parse.urlparse(endpoint).query else "?"
    return f"{endpoint}{separator}output_format=JSON_Compact"


def write_not_available(reason, detail=None):
    doc = {
        "schema_version": 1,
        "engine": "firebolt_hnsw",
        "workload": workload,
        "suite": "firebolt_vector_search",
        "profile": profile,
        "n_rows": rows,
        "vector_dims": dims,
        "top_k": top_k,
        "queries": queries,
        "warmup_queries": warmup,
        "m": m,
        "ef_search": ef_search,
        "seed": seed,
        "docker_image": os.environ["FIREBOLT_CORE_IMAGE"],
        "firebolt_version": None,
        "core_mode": "local_docker",
        "host_cpu": os.environ.get("ULTRASQL_HOST_CPU") or platform.processor() or platform.machine(),
        "host_memory": host_memory_bytes(),
        "dataset_rows": rows,
        "samples": 0,
        "median_us": None,
        "p95_us": None,
        "status": "not_available",
        "reason": reason,
        "detail": detail,
        "required_metrics": required_metrics,
        "policy": (
            "No Firebolt vector-search benchmark claim exists until this "
            "artifact is measured via VECTOR_SEARCH."
        ),
    }
    out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(out)


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


try:
    data = [vector_for_row(row_id) for row_id in range(rows)]
    started = time.perf_counter()
    request(
        f"CREATE TABLE {table} ("
        "id INT, embedding ARRAY(DOUBLE NOT NULL) NOT NULL)"
    )
    request(
        f"CREATE INDEX {index} ON {table} USING HNSW "
        "(embedding vector_l2sq_ops) "
        f"WITH (dimension = {dims}, m = {m}, "
        f"ef_construction = {ef_construction}, quantization = 'f32')"
    )
    chunk_rows = int(os.environ.get("FIREBOLT_INSERT_CHUNK_ROWS", "1000"))
    for start in range(0, rows, chunk_rows):
        end = min(start + chunk_rows, rows)
        values = ",".join(
            f"({row_id},{array_literal(data[row_id])})" for row_id in range(start, end)
        )
        request(f"INSERT INTO {table} VALUES {values}")
    build_time_us = (time.perf_counter() - started) * 1_000_000.0

    iterations_us = []
    recall_iterations = []
    first_exact_answer = []
    first_ann_answer = []
    for query_id in range(warmup + queries):
        probe = vector_for_probe(query_id)
        expected = exact_top_k(data, probe)
        probe_sql = array_literal(probe)
        query = (
            f"SELECT id FROM VECTOR_SEARCH("
            f"INDEX {index}, target_vector => {probe_sql}, "
            f"top_k => {top_k}, ef_search => {ef_search}) "
            f"ORDER BY VECTOR_SQUARED_EUCLIDEAN_DISTANCE(embedding, {probe_sql}), id "
            f"LIMIT {top_k}"
        )
        started = time.perf_counter()
        result_doc = request(query)
        elapsed_us = (time.perf_counter() - started) * 1_000_000.0
        observed = [int(row[0]) for row in result_doc.get("data", [])]
        if query_id >= warmup:
            iterations_us.append(elapsed_us)
            recall_iterations.append(recall_at_k(expected, observed))
            if not first_exact_answer:
                first_exact_answer = expected
                first_ann_answer = observed

    if not iterations_us:
        write_not_available("no_measured_iterations")
        sys.exit(2)

    try:
        version_doc = request("SELECT version()")
        version_rows = version_doc.get("data", [])
        firebolt_version = str(version_rows[0][0]) if version_rows and version_rows[0] else None
    except Exception:
        firebolt_version = None

    doc = {
        "schema_version": 1,
        "engine": "firebolt_hnsw",
        "workload": workload,
        "suite": "firebolt_vector_search",
        "profile": profile,
        "n_rows": rows,
        "vector_dims": dims,
        "top_k": top_k,
        "queries": queries,
        "warmup_queries": warmup,
        "metric": "l2",
        "m": m,
        "ef_construction": ef_construction,
        "ef_search": ef_search,
        "seed": seed,
        "docker_image": os.environ["FIREBOLT_CORE_IMAGE"],
        "firebolt_version": firebolt_version,
        "core_mode": "local_docker",
        "host_cpu": os.environ.get("ULTRASQL_HOST_CPU") or platform.processor() or platform.machine(),
        "host_memory": host_memory_bytes(),
        "dataset_rows": rows,
        "samples": len(iterations_us),
        "median_us": statistics.median(iterations_us),
        "p95_us": percentile_nearest_rank(iterations_us, 0.95),
        "recall_at_k": statistics.mean(recall_iterations),
        "p50_latency_us": percentile_nearest_rank(iterations_us, 0.50),
        "p95_latency_us": percentile_nearest_rank(iterations_us, 0.95),
        "p99_latency_us": percentile_nearest_rank(iterations_us, 0.99),
        "build_time_us": build_time_us,
        "build_time_scope": "create_table_index_and_insert",
        "memory_bytes": None,
        "memory_status": "not_measured",
        "index_size_bytes": None,
        "index_size_status": "not_measured",
        "query_iterations_us": iterations_us,
        "recall_iterations": recall_iterations,
        "first_exact_answer": first_exact_answer,
        "first_ann_answer": first_ann_answer,
        "status": "measured",
        "required_metrics": required_metrics,
        "policy": (
            "No Firebolt vector-search benchmark claim may be made unless "
            "this raw artifact is committed with its runner inputs."
        ),
    }
    out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(out)
except Exception as exc:
    write_not_available("firebolt_vector_search_unavailable", str(exc))
    sys.exit(2)
finally:
    try:
        request(f"DROP TABLE IF EXISTS {table}")
    except Exception:
        pass
PY
}

run_engine() {
    local engine="$1"
    local artifact
    local code

    set +e
    case "$engine" in
        ultrasql)
            artifact="$(run_ultrasql)"
            code=$?
            ;;
        firebolt)
            artifact="$(run_firebolt)"
            code=$?
            ;;
        *)
            artifact="$(emit_not_available "$engine" "runner_not_implemented_for_engine")"
            code=2
            ;;
    esac
    set -e

    case "$code" in
        0)
            record_engine "$engine" "measured" "$code" "$artifact"
            ;;
        2)
            record_engine "$engine" "unavailable" "$code" "$artifact"
            unavailable=1
            ;;
        *)
            record_engine "$engine" "failed" "$code" "$artifact"
            failed=1
            ;;
    esac
}

IFS=',' read -r -a REQUESTED_ENGINES <<<"$ENGINES"
for engine in "${REQUESTED_ENGINES[@]}"; do
    run_engine "$engine"
done

python3 - "$PROFILE" "$ENGINES" "$ROWS" "$DIMS" "$TOP_K" "$QUERIES" "$WARMUP" "$MANIFEST" "$STATUS_FILE" <<'PY'
import json
import pathlib
import sys
import time

profile, engines, rows, dims, top_k, queries, warmup, manifest_path, status_path = sys.argv[1:]
entries = []
for line in pathlib.Path(status_path).read_text(encoding="utf-8").splitlines():
    engine, status, code, artifact = line.split("\t")
    entries.append(
        {
            "engine": engine,
            "status": status,
            "exit_code": int(code),
            "artifact": artifact,
        }
    )

has_failed = any(entry["status"] == "failed" for entry in entries)
has_unavailable = any(entry["status"] == "unavailable" for entry in entries)
doc = {
    "schema_version": 1,
    "suite": "firebolt_vector_search",
    "profile": profile,
    "requested_engines": [engine for engine in engines.split(",") if engine],
    "n_rows": int(rows),
    "vector_dims": int(dims),
    "top_k": int(top_k),
    "queries": int(queries),
    "warmup_queries": int(warmup),
    "generated_at_unix": int(time.time()),
    "status": "failed" if has_failed else "partial" if has_unavailable else "passed",
    "passed": not has_failed and not has_unavailable,
    "engines": entries,
    "policy": (
        "No Firebolt vector-search benchmark claim exists unless Firebolt has "
        "a measured local Core VECTOR_SEARCH artifact. Missing Core or "
        "unsupported VECTOR_SEARCH is recorded as not_available."
    ),
}
pathlib.Path(manifest_path).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(json.dumps(doc, indent=2))
PY

if (( failed != 0 )); then
    exit 1
fi
if (( unavailable != 0 )); then
    exit 2
fi
exit 0
