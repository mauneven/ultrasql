#!/usr/bin/env bash
# run_clickhouse_writes.sh — measure cross_compare_sql workloads against
# ClickHouse via the native TCP protocol (`clickhouse_driver` Python
# bindings against a long-lived `clickhouse-server` on localhost).
#
# Workloads (identical SQL semantics across every engine script):
#   insert_throughput_10k    — INSERT 10 000 rows into a fresh MergeTree.
#   update_throughput_10k    — ALTER TABLE … UPDATE … (mutations_sync=2).
#   delete_throughput_10k    — ALTER TABLE … DELETE … (mutations_sync=2).
#   mixed_oltp_pgbench_like  — 1-second window: 50% point reads, 30%
#                              point UPDATE-mutations, 20% inserts.
#   select_scan_10k          — drain full `SELECT id, val FROM t`.
#   select_sum_65k_i64       — `SELECT SUM(x) FROM bench_analytical`.
#   select_avg_1m_i64        — `SELECT AVG(x) FROM bench_analytical`.
#   filter_sum_1m_i64        — `SELECT SUM(x) FROM bench_analytical
#                              WHERE x > 5_000_000`.
#
# ClickHouse engine choices follow the project documentation:
#
#   - Analytical / SELECT-heavy: `ENGINE = MergeTree() ORDER BY tuple()`.
#     The empty ORDER BY clause disables the primary-key index, which is
#     what every comparable benchmark uses to isolate scan / aggregation
#     cost without an index-skipping shortcut.
#   - OLTP point-mutation: `ENGINE = MergeTree() ORDER BY id`. ClickHouse
#     does not have row-level UPDATE / DELETE; the equivalent surface is
#     `ALTER TABLE … UPDATE WHERE …` (async mutations). We force
#     synchronous behaviour with `mutations_sync = 2` so the bench
#     measures the full mutation latency.
#
# Output: one JSON file per workload in $RAW_DIR:
#   <workload>-clickhouse.json
#
# An optional positional argument selects a single workload (e.g.
# `select_scan_10k`); with no argument all workloads run.
#
# Environment (with defaults):
#   RAW_DIR   (default: benchmarks/results/latest/raw)
#   N_ITERS   (default: 8)
#   N_ROWS    (default: 10000)
#   ANALYTICAL_ROWS  row count for SUM/AVG/filter/window workloads
#   CH_BIN    (default: /tmp/ultracmp/clickhouse)
#   CH_TCP_PORT  (default: 19000)
#   INSERT_CHUNK_ROWS (default: 10000)
#
# Pre-requisites:
#   - `clickhouse_driver` Python module on PATH.
#   - `clickhouse server` reachable at 127.0.0.1:$CH_TCP_PORT.
#     The script starts an isolated instance under /tmp/ch_bench/ if one
#     is not already running, and tears it down at exit when it started
#     the process itself.

set -euo pipefail

ENGINE="clickhouse"
RAW_DIR="${RAW_DIR:-benchmarks/results/latest/raw}"
N_ITERS="${N_ITERS:-8}"
N_ROWS="${N_ROWS:-10000}"
ANALYTICAL_ROWS="${ANALYTICAL_ROWS:-}"
CH_BIN="${CH_BIN:-/tmp/ultracmp/clickhouse}"
CH_TCP_PORT="${CH_TCP_PORT:-19000}"
CH_HTTP_PORT="${CH_HTTP_PORT:-18123}"
CH_DATA_DIR="${CH_DATA_DIR:-/tmp/ch_bench_$$}"
INSERT_CHUNK_ROWS="${INSERT_CHUNK_ROWS:-10000}"
WORKLOAD="${1:-all}"

row_suffix() {
    local rows="$1"
    if [[ "$rows" -eq 65536 ]]; then
        echo "65k"
    elif [[ "$rows" -ge 1000000 && $((rows % 1000000)) -eq 0 ]]; then
        echo "$((rows / 1000000))m"
    elif [[ "$rows" -ge 1000 && $((rows % 1000)) -eq 0 ]]; then
        echo "$((rows / 1000))k"
    else
        echo "$rows"
    fi
}

workload_rows() {
    local wl="$1"
    case "$wl" in
        insert_throughput_*|update_throughput_*|delete_throughput_*|select_scan_*)
            echo "$N_ROWS" ;;
        mixed_oltp_pgbench_like)
            echo "$N_ROWS" ;;
        select_sum_*_i64|select_avg_*_i64|filter_sum_*_i64|window_row_number_*_i64)
            case "$wl" in
                select_sum_*_i64) echo "${ANALYTICAL_ROWS:-65536}" ;;
                select_avg_*_i64|filter_sum_*_i64) echo "${ANALYTICAL_ROWS:-1000000}" ;;
                window_row_number_*_i64) echo "${ANALYTICAL_ROWS:-65536}" ;;
            esac
            ;;
        *) echo "$N_ROWS" ;;
    esac
}

target_workloads() {
    case "$WORKLOAD" in
        insert_throughput_10k)   echo "insert_throughput_$(row_suffix "$N_ROWS")" ;;
        update_throughput_10k)   echo "update_throughput_$(row_suffix "$N_ROWS")" ;;
        delete_throughput_10k)   echo "delete_throughput_$(row_suffix "$N_ROWS")" ;;
        mixed_oltp_pgbench_like) echo "mixed_oltp_pgbench_like" ;;
        select_scan_10k)         echo "select_scan_$(row_suffix "$N_ROWS")" ;;
        select_sum_65k_i64)
            local rows="${ANALYTICAL_ROWS:-65536}"
            echo "select_sum_$(row_suffix "$rows")_i64"
            ;;
        select_avg_1m_i64)
            local rows="${ANALYTICAL_ROWS:-1000000}"
            echo "select_avg_$(row_suffix "$rows")_i64"
            ;;
        filter_sum_1m_i64)
            local rows="${ANALYTICAL_ROWS:-1000000}"
            echo "filter_sum_$(row_suffix "$rows")_i64"
            ;;
        window_row_number_65k_i64)
            local rows="${ANALYTICAL_ROWS:-65536}"
            echo "window_row_number_$(row_suffix "$rows")_i64"
            ;;
        all)
            echo "insert_throughput_$(row_suffix "$N_ROWS")"
            echo "update_throughput_$(row_suffix "$N_ROWS")"
            echo "delete_throughput_$(row_suffix "$N_ROWS")"
            echo "mixed_oltp_pgbench_like"
            echo "select_scan_$(row_suffix "$N_ROWS")"
            echo "select_sum_65k_i64"
            echo "select_avg_1m_i64"
            echo "filter_sum_1m_i64"
            echo "window_row_number_65k_i64"
            ;;
        *)
            echo "run_clickhouse_writes.sh: unknown workload '$WORKLOAD'" >&2
            exit 2
            ;;
    esac
}

mark_unavailable() {
    local reason="$1"
    echo "run_clickhouse_writes.sh: WARNING: ${reason} — emitting unavailable stubs" >&2
    mkdir -p "$RAW_DIR"
    target_workloads | while IFS= read -r wl; do
        local rows
        rows="$(workload_rows "$wl")"
        python3 - "$RAW_DIR/${wl}-${ENGINE}.json" "$ENGINE" "$wl" "$rows" "$reason" <<'PY'
import json
import sys
from pathlib import Path

out, engine, workload, rows, reason = sys.argv[1:]
doc = {
    "schema_version": 1,
    "engine": engine,
    "status": "not_available",
    "workload": workload,
    "n_rows": int(rows),
    "reason": reason,
    "policy": "No ClickHouse benchmark claim exists until this artifact records measured samples from the same scale-sweep run.",
}
Path(out).write_text(json.dumps(doc, sort_keys=True) + "\n")
PY
    done
    exit 0
}

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! python3 -c "import clickhouse_driver" >/dev/null 2>&1; then
    mark_unavailable "python3 clickhouse_driver module not installed"
fi
if [[ ! -x "$CH_BIN" ]]; then
    mark_unavailable "clickhouse binary not found at $CH_BIN"
fi

mkdir -p "$RAW_DIR"

# ---------------------------------------------------------------------------
# Bring up an isolated clickhouse-server if one is not already listening.
# ---------------------------------------------------------------------------
STARTED_SERVER=0
if ! python3 -c "
import socket
s = socket.socket()
s.settimeout(0.3)
try:
    s.connect(('127.0.0.1', ${CH_TCP_PORT}))
    s.close()
    raise SystemExit(0)
except Exception:
    raise SystemExit(1)
" 2>/dev/null; then
    mkdir -p "${CH_DATA_DIR}/data" "${CH_DATA_DIR}/tmp" "${CH_DATA_DIR}/user_files"
    cat > "${CH_DATA_DIR}/config.xml" <<XMLCONF
<clickhouse>
    <logger><level>error</level><log>${CH_DATA_DIR}/clickhouse-server.log</log><errorlog>${CH_DATA_DIR}/clickhouse-server.err.log</errorlog><size>10M</size><count>1</count></logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>${CH_TCP_PORT}</tcp_port>
    <http_port>${CH_HTTP_PORT}</http_port>
    <path>${CH_DATA_DIR}/data/</path>
    <tmp_path>${CH_DATA_DIR}/tmp/</tmp_path>
    <user_files_path>${CH_DATA_DIR}/user_files/</user_files_path>
    <users_config>${CH_DATA_DIR}/users.xml</users_config>
    <max_connections>16</max_connections>
    <mark_cache_size>536870912</mark_cache_size>
    <mlock_executable>false</mlock_executable>
</clickhouse>
XMLCONF
    cat > "${CH_DATA_DIR}/users.xml" <<XMLUSERS
<clickhouse>
    <users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
XMLUSERS
    "$CH_BIN" server --config-file="${CH_DATA_DIR}/config.xml" >/dev/null 2>&1 &
    STARTED_SERVER=$!
    # Wait until TCP port responds.
    for _ in $(seq 1 40); do
        if python3 -c "
import socket
s = socket.socket()
s.settimeout(0.3)
try:
    s.connect(('127.0.0.1', ${CH_TCP_PORT}))
    s.close()
except Exception:
    raise SystemExit(1)
" 2>/dev/null; then
            break
        fi
        sleep 0.25
    done
    trap '[[ ${STARTED_SERVER} -ne 0 ]] && kill ${STARTED_SERVER} 2>/dev/null || true; rm -rf "${CH_DATA_DIR}"' EXIT
fi

# ---------------------------------------------------------------------------
# Helpers shared with run_postgres_writes.sh / run_sqlite3_writes.sh:
# compute_median + emit_json.
# ---------------------------------------------------------------------------
compute_median() {
    python3 - "$@" <<'PYEOF'
import statistics, sys
vals = [float(x) for x in sys.argv[1:]]
if not vals:
    print("0")
else:
    print(f"{statistics.median(vals):.3f}")
PYEOF
}

emit_json() {
    local workload="$1"
    local n_rows="$2"
    local median_us="$3"
    shift 3
    local n_samples="$#"
    local samples_json
    samples_json="$(python3 -c "
import json,sys
print(json.dumps([float(x) for x in sys.argv[1:]]))
" "$@")"
    printf '{"engine":"%s","workload":"%s","n_rows":%s,"samples":%s,"median_us":%s,"min_us":%s,"iterations_us":%s}\n' \
        "$ENGINE" "$workload" "$n_rows" "$n_samples" "$median_us" \
        "$(python3 -c "import sys; vals=[float(x) for x in sys.argv[1:]]; print(min(vals) if vals else 0)" "$@")" \
        "$samples_json"
}

# ---------------------------------------------------------------------------
# Workload: insert_throughput_10k
# ---------------------------------------------------------------------------
run_insert() {
    local wl="insert_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$INSERT_CHUNK_ROWS" "$CH_TCP_PORT" <<'PYEOF'
import sys, time, random
from clickhouse_driver import Client

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
chunk_rows = int(sys.argv[3])
port = int(sys.argv[4])

rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-(2**31), 2**31 - 1) for _ in range(n)]
rows = list(zip(ids, vals))

c = Client(host="127.0.0.1", port=port)

for _ in range(n_iters):
    c.execute("DROP TABLE IF EXISTS bench_write SYNC")
    c.execute(
        "CREATE TABLE bench_write (id Int64, val Int64) "
        "ENGINE = MergeTree() ORDER BY id"
    )
    t0 = time.perf_counter()
    for start in range(0, n, chunk_rows):
        c.execute("INSERT INTO bench_write (id, val) VALUES", rows[start:start + chunk_rows])
    t1 = time.perf_counter()
    print((t1 - t0) * 1e6)
PYEOF
)"
    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"
    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Workload: update_throughput_10k
# ALTER TABLE ... UPDATE WHERE with mutations_sync=2.
# ---------------------------------------------------------------------------
run_update() {
    local wl="update_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$CH_TCP_PORT" <<'PYEOF'
import sys, time, random
from clickhouse_driver import Client

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
port = int(sys.argv[3])

rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-(2**31), 2**31 - 1) for _ in range(n)]
rows = list(zip(ids, vals))

c = Client(host="127.0.0.1", port=port, settings={"mutations_sync": 2})
c.execute("DROP TABLE IF EXISTS bench_write SYNC")
c.execute("CREATE TABLE bench_write (id Int64, val Int64) ENGINE = MergeTree() ORDER BY id")
c.execute("INSERT INTO bench_write (id, val) VALUES", rows)

for _ in range(2):
    c.execute(f"ALTER TABLE bench_write UPDATE val = val + 1 WHERE id BETWEEN 0 AND {n-1}")

for _ in range(n_iters):
    t0 = time.perf_counter()
    c.execute(f"ALTER TABLE bench_write UPDATE val = val + 1 WHERE id BETWEEN 0 AND {n-1}")
    t1 = time.perf_counter()
    print((t1 - t0) * 1e6)
PYEOF
)"
    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"
    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Workload: delete_throughput_10k
# ALTER TABLE ... DELETE WHERE with mutations_sync=2.
# ---------------------------------------------------------------------------
run_delete() {
    local wl="delete_throughput_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$CH_TCP_PORT" <<'PYEOF'
import sys, time, random
from clickhouse_driver import Client

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
port = int(sys.argv[3])

rng = random.Random(0xC0FFEE)
ids = list(range(n))
rng.shuffle(ids)
vals = [rng.randint(-(2**31), 2**31 - 1) for _ in range(n)]
rows = list(zip(ids, vals))

c = Client(host="127.0.0.1", port=port, settings={"mutations_sync": 2})

# Each iteration recreates the table so deletes are measured against a
# fresh starting state (matches the rollback / drop pattern in peer scripts).
for _ in range(n_iters):
    c.execute("DROP TABLE IF EXISTS bench_write SYNC")
    c.execute("CREATE TABLE bench_write (id Int64, val Int64) ENGINE = MergeTree() ORDER BY id")
    c.execute("INSERT INTO bench_write (id, val) VALUES", rows)
    t0 = time.perf_counter()
    c.execute(f"ALTER TABLE bench_write DELETE WHERE id BETWEEN 0 AND {n-1}")
    t1 = time.perf_counter()
    print((t1 - t0) * 1e6)
PYEOF
)"
    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"
    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Workload: mixed_oltp_pgbench_like
# 50% point reads, 30% point updates, 20% inserts in a 1-second window.
# ---------------------------------------------------------------------------
run_mixed() {
    local wl="mixed_oltp_pgbench_like"
    echo "  workload: ${wl}"
    local samples=()
    local window_secs=1
    for (( i=0; i<N_ITERS; i++ )); do
        local us_per_op
        us_per_op="$(python3 - "$N_ROWS" "$window_secs" "$i" "$CH_TCP_PORT" <<'PYEOF'
import sys, time, random
from clickhouse_driver import Client

n = int(sys.argv[1])
window = float(sys.argv[2])
seed = int(sys.argv[3])
port = int(sys.argv[4])
rng = random.Random(0xBEEF + seed)

rng2 = random.Random(0xC0FFEE)
ids = list(range(n))
rng2.shuffle(ids)
vals = [rng2.randint(-(2**31), 2**31 - 1) for _ in range(n)]
rows = list(zip(ids, vals))

c = Client(host="127.0.0.1", port=port, settings={"mutations_sync": 2})
c.execute("DROP TABLE IF EXISTS bench_write SYNC")
c.execute("CREATE TABLE bench_write (id Int64, val Int64) ENGINE = MergeTree() ORDER BY id")
c.execute("INSERT INTO bench_write (id, val) VALUES", rows)

deadline = time.perf_counter() + window
count = 0
next_id = n
while time.perf_counter() < deadline:
    r = rng.random()
    if r < 0.50:
        row_id = rng.randint(0, n - 1)
        c.execute(f"SELECT val FROM bench_write WHERE id = {row_id}")
    elif r < 0.80:
        row_id = rng.randint(0, n - 1)
        c.execute(f"ALTER TABLE bench_write UPDATE val = val + 1 WHERE id = {row_id}")
    else:
        new_val = rng.randint(-(2**31), 2**31 - 1)
        c.execute(f"INSERT INTO bench_write (id, val) VALUES", [(next_id, new_val)])
        next_id += 1
    count += 1

elapsed = time.perf_counter() - (deadline - window)
print(elapsed * 1e6 / max(count, 1))
PYEOF
)"
        samples+=("$us_per_op")
    done
    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs/op"
}

# ---------------------------------------------------------------------------
# Workload: select_scan_10k
# ---------------------------------------------------------------------------
run_select_scan() {
    local wl="select_scan_$(row_suffix "$N_ROWS")"
    echo "  workload: ${wl}"
    local samples_raw
    samples_raw="$(python3 - "$N_ROWS" "$N_ITERS" "$CH_TCP_PORT" <<'PYEOF'
import sys, time
from clickhouse_driver import Client

n = int(sys.argv[1])
n_iters = int(sys.argv[2])
port = int(sys.argv[3])

c = Client(host="127.0.0.1", port=port)
c.execute("DROP TABLE IF EXISTS bench_select_scan SYNC")
c.execute("CREATE TABLE bench_select_scan (id Int32, val Int32) ENGINE = MergeTree() ORDER BY tuple()")
rows = [(j, j * 10) for j in range(n)]
c.execute("INSERT INTO bench_select_scan (id, val) VALUES", rows)

for _ in range(2):
    c.execute("SELECT id, val FROM bench_select_scan")

for _ in range(n_iters):
    t0 = time.perf_counter()
    rs = c.execute("SELECT id, val FROM bench_select_scan")
    t1 = time.perf_counter()
    if len(rs) != n:
        sys.stderr.write(f"run_select_scan: row mismatch (got {len(rs)}, expected {n})\n")
    print((t1 - t0) * 1e6)
PYEOF
)"
    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"
    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$N_ROWS" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

# ---------------------------------------------------------------------------
# Helper: run a SELECT against bench_analytical with the given query.
# Args: workload_id, n_rows, query_sql
# ---------------------------------------------------------------------------
run_analytical() {
    local wl="$1"
    local n_rows="$2"
    local query="$3"
    echo "  workload: ${wl} (n_rows=${n_rows})"
    local samples_raw
    samples_raw="$(python3 - "$n_rows" "$query" "$N_ITERS" "$CH_TCP_PORT" <<'PYEOF'
import sys, time
from clickhouse_driver import Client

n = int(sys.argv[1])
query = sys.argv[2]
n_iters = int(sys.argv[3])
port = int(sys.argv[4])

c = Client(host="127.0.0.1", port=port)
c.execute("DROP TABLE IF EXISTS bench_analytical SYNC")
c.execute("CREATE TABLE bench_analytical (id Int32, x Int32) ENGINE = MergeTree() ORDER BY tuple()")
rows = [(j, j * 10) for j in range(n)]
c.execute("INSERT INTO bench_analytical (id, x) VALUES", rows)

for _ in range(2):
    c.execute(query)

for _ in range(n_iters):
    t0 = time.perf_counter()
    rs = c.execute(query)
    t1 = time.perf_counter()
    if not rs:
        sys.stderr.write("run_analytical: empty result set\n")
    print((t1 - t0) * 1e6)
PYEOF
)"
    local samples=()
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        samples+=("$line")
    done <<< "$samples_raw"
    local median_us
    median_us="$(compute_median "${samples[@]}")"
    emit_json "$wl" "$n_rows" "$median_us" "${samples[@]}" \
        > "${RAW_DIR}/${wl}-${ENGINE}.json"
    echo "    median: ${median_us} µs"
}

run_sum_scalar() {
    local rows="${ANALYTICAL_ROWS:-65536}"
    run_analytical "select_sum_$(row_suffix "$rows")_i64" "$rows" \
        "SELECT SUM(x) FROM bench_analytical"
}

run_avg_scalar() {
    local rows="${ANALYTICAL_ROWS:-1000000}"
    run_analytical "select_avg_$(row_suffix "$rows")_i64" "$rows" \
        "SELECT AVG(x) FROM bench_analytical"
}

run_filter_sum() {
    local rows="${ANALYTICAL_ROWS:-1000000}"
    local threshold=$((rows * 5))
    run_analytical "filter_sum_$(row_suffix "$rows")_i64" "$rows" \
        "SELECT SUM(x) FROM bench_analytical WHERE x > ${threshold}"
}

run_window_row_number() {
    local rows="${ANALYTICAL_ROWS:-65536}"
    run_analytical "window_row_number_$(row_suffix "$rows")_i64" "$rows" \
        "SELECT id, row_number() OVER (ORDER BY x) FROM bench_analytical"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
case "$WORKLOAD" in
    insert_throughput_10k)   run_insert ;;
    update_throughput_10k)   run_update ;;
    delete_throughput_10k)   run_delete ;;
    mixed_oltp_pgbench_like) run_mixed ;;
    select_scan_10k)         run_select_scan ;;
    select_sum_65k_i64)      run_sum_scalar ;;
    select_avg_1m_i64)       run_avg_scalar ;;
    filter_sum_1m_i64)       run_filter_sum ;;
    window_row_number_65k_i64) run_window_row_number ;;
    all)
        run_insert
        run_update
        run_delete
        run_mixed
        run_select_scan
        run_sum_scalar
        run_avg_scalar
        run_filter_sum
        run_window_row_number
        ;;
    *)
        echo "run_clickhouse_writes.sh: unknown workload '$WORKLOAD'" >&2
        exit 2
        ;;
esac

echo "run_clickhouse_writes.sh: done — results in ${RAW_DIR}/"
