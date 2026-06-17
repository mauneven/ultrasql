#!/usr/bin/env bash
# Release-artifact DB-vs-DB scale sweep.
#
# UltraSQL is measured as an external `ultrasqld` binary over TCP. If
# ULTRASQLD_BIN is unset, this script installs a GitHub Release archive into a
# temporary directory through scripts/install.sh, then benchmarks that binary.
# Competitors use installed local clients and the same raw artifact schema.
#
# Usage:
#   benchmarks/run_scale_sweep.sh quick
#   benchmarks/run_scale_sweep.sh full
#
# Environment:
#   ULTRASQLD_BIN              path to an existing release ultrasqld binary
#   ULTRASQL_RELEASE_VERSION   release tag for scripts/install.sh (default latest)
#   SCALE_SWEEP_ROWS           row counts (default "10000 100000 1000000")
#   SCALE_SWEEP_OUT            artifact dir (default benchmarks/results/latest/scale-sweep)
#   SCALE_SWEEP_STORAGE        memory|data-dir (default memory)
#   SCALE_SWEEP_DATA_ROOT      data-dir benchmark root (default $SCALE_SWEEP_OUT/data-dirs)
#   N_ITERS                    measured samples override
#   WARMUP                     warmup samples override
#
# Bulk INSERT uses fresh UltraSQL server processes per measured sample so
# every engine times a fresh table load. INSERT chunks are 10k rows across
# UltraSQL, DuckDB, SQLite, PostgreSQL, and ClickHouse.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

mode="${1:-full}"
case "$mode" in
    full)  ITERS="${N_ITERS:-32}"; WARMUP="${WARMUP:-8}" ;;
    quick) ITERS="${N_ITERS:-8}";  WARMUP="${WARMUP:-2}" ;;
    *) echo "unknown mode '$mode' (full|quick)" >&2; exit 2 ;;
esac

OUT="${SCALE_SWEEP_OUT:-benchmarks/results/latest/scale-sweep}"
RAW="$OUT/raw"
ROWS="${SCALE_SWEEP_ROWS:-10000 100000 1000000}"
INSERT_CHUNK_ROWS="${INSERT_CHUNK_ROWS:-10000}"
STORAGE_MODE="${SCALE_SWEEP_STORAGE:-memory}"
case "$STORAGE_MODE" in
    memory|data-dir) ;;
    *) echo "unknown SCALE_SWEEP_STORAGE '$STORAGE_MODE' (memory|data-dir)" >&2; exit 2 ;;
esac
DATA_ROOT="${SCALE_SWEEP_DATA_ROOT:-$OUT/data-dirs}"
BENCH_COMPETITOR_DATA_ROOT="$DATA_ROOT/competitors"
mkdir -p "$RAW"
if [[ "${SCALE_SWEEP_APPEND:-0}" != "1" ]]; then
    rm -f "$RAW"/*.json
    rm -rf "$DATA_ROOT"
fi

tmp_dir="$(mktemp -d)"
server_pid=""
cleanup() {
    if [[ -n "${server_pid:-}" ]] && kill -0 "$server_pid" >/dev/null 2>&1; then
        kill "$server_pid" >/dev/null 2>&1 || true
        wait "$server_pid" >/dev/null 2>&1 || true
    fi
    rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

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

workload_id() {
    local workload="$1"
    local rows="$2"
    local suffix
    suffix="$(row_suffix "$rows")"
    case "$workload" in
        insert-bulk)   echo "insert_throughput_${suffix}" ;;
        select-scan)   echo "select_scan_${suffix}" ;;
        sum-scalar)    echo "select_sum_${suffix}_i64" ;;
        avg-scalar)    echo "select_avg_${suffix}_i64" ;;
        filter-sum)    echo "filter_sum_${suffix}_i64" ;;
        update-bulk)   echo "update_throughput_${suffix}" ;;
        delete-bulk)   echo "delete_throughput_${suffix}" ;;
        mixed-oltp)    echo "mixed_oltp_pgbench_like" ;;
        mixed-correctness) echo "mixed_correctness_${suffix}" ;;
        window-row-number) echo "window_row_number_${suffix}_i64" ;;
        *) echo "unknown workload '$workload'" >&2; exit 2 ;;
    esac
}

free_port() {
    python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
}

wait_for_port() {
    python3 - "$1" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.time() + 15.0
last_error = None
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            sys.exit(0)
    except OSError as exc:
        last_error = exc
        time.sleep(0.05)
print(f"timed out waiting for ultrasqld on 127.0.0.1:{port}: {last_error}", file=sys.stderr)
sys.exit(1)
PY
}

durability_mode() {
    if [[ "$1" == "data-dir" ]]; then
        echo "durable"
    else
        echo "volatile"
    fi
}

annotate_raw_profile() {
    local path="$1"
    local storage_mode="$2"
    local durability
    durability="$(durability_mode "$storage_mode")"
    python3 - "$path" "$storage_mode" "$durability" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
doc = json.loads(path.read_text())
doc["storage_mode"] = sys.argv[2]
doc["durability_mode"] = sys.argv[3]
path.write_text(json.dumps(doc, sort_keys=True) + "\n")
PY
}

echo "=== scale sweep mode=$mode storage=$STORAGE_MODE iters=$ITERS warmup=$WARMUP rows=[$ROWS] ==="

PROFILE="${SCALE_SWEEP_PROFILE:-release-ship}"
echo "--- Building benchmark driver (profile=$PROFILE) ---"
# Release/bench builds disable incremental compilation: it adds gigabytes of
# cache under target/ with no benefit for a one-shot measured build.
CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
cargo build --profile "$PROFILE" \
    --package ultrasql-bench \
    --features sql-bench \
    --bin cross_compare_sql \
    --bin results-render
BIN="target/$PROFILE"

if [[ -z "${ULTRASQLD_BIN:-}" ]]; then
    install_dir="$tmp_dir/bin"
    install_source="scripts/install.sh ${ULTRASQL_RELEASE_VERSION:-latest}"
    echo "--- Installing UltraSQL release artifact ---"
    ULTRASQL_INSTALL_DIR="$install_dir" \
        scripts/install.sh "${ULTRASQL_RELEASE_VERSION:-latest}"
    ULTRASQLD_BIN="$install_dir/ultrasqld"
else
    install_source="ULTRASQLD_BIN"
fi

if [[ ! -x "$ULTRASQLD_BIN" ]]; then
    echo "run_scale_sweep.sh: ULTRASQLD_BIN is not executable: $ULTRASQLD_BIN" >&2
    exit 1
fi

ULTRASQL_VERSION_TEXT="$("$ULTRASQLD_BIN" --version 2>&1 | head -n 1)"
echo "--- UltraSQL artifact: $ULTRASQL_VERSION_TEXT ($ULTRASQLD_BIN) ---"

run_ultrasql_workload() {
    local workload="$1"
    local rows="$2"
    local wid="$3"
    local port log tmp_json err_log

    if [[ "$workload" == "insert-bulk" ]]; then
        run_ultrasql_fresh_insert_samples "$workload" "$rows" "$wid"
        return
    fi

    port="$(free_port)"
    log="$OUT/ultrasqld-${wid}.log"
    tmp_json="$RAW/${wid}-ultrasql.json.tmp"
    err_log="$OUT/cross_compare-${wid}.err"
    local data_dir args
    data_dir="$DATA_ROOT/${wid}"
    args=("$ULTRASQLD_BIN" --listen "127.0.0.1:${port}" --log-level warn)
    if [[ "$STORAGE_MODE" == "data-dir" ]]; then
        rm -rf "$data_dir"
        mkdir -p "$(dirname "$data_dir")"
        args+=(--data-dir "$data_dir")
    fi
    "${args[@]}" >"$log" 2>&1 &
    server_pid="$!"
    wait_for_port "$port"
    if "$BIN/cross_compare_sql" \
        --server "127.0.0.1:${port}" \
        --workload "$workload" \
        --rows "$rows" \
        --storage-mode "$STORAGE_MODE" \
        --warmup "$WARMUP" \
        --iters "$ITERS" \
        > "$tmp_json" 2>"$err_log"; then
        mv "$tmp_json" "$RAW/${wid}-ultrasql.json"
        annotate_raw_profile "$RAW/${wid}-ultrasql.json" "$STORAGE_MODE"
    else
        cat "$err_log" >&2 || true
        rm -f "$tmp_json"
        python3 - "$RAW/${wid}-ultrasql.json" "$wid" "$rows" "$err_log" "$log" \
            "$STORAGE_MODE" "$(durability_mode "$STORAGE_MODE")" <<'PY'
import json
import sys
from pathlib import Path

out, workload, rows, err_log, server_log, storage_mode, durability_mode = sys.argv[1:]
reason_parts = []
for path in [Path(err_log), Path(server_log)]:
    if path.exists():
        text = path.read_text(errors="replace").strip()
        if text:
            reason_parts.append(text[-2000:])
doc = {
    "schema_version": 1,
    "engine": "ultrasql",
    "workload": workload,
    "status": "not_available",
    "n_rows": int(rows),
    "server_mode": "external",
    "storage_mode": storage_mode,
    "durability_mode": durability_mode,
    "reason": "\n".join(reason_parts) or "benchmark command failed",
    "policy": "Failure is recorded as not_available; no benchmark claim is made for this row.",
}
Path(out).write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
PY
    fi
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
    server_pid=""
}

run_ultrasql_fresh_insert_samples() {
    local workload="$1"
    local rows="$2"
    local wid="$3"
    local sample sample_dir total
    sample_dir="$RAW/.${wid}-ultrasql-samples"
    rm -rf "$sample_dir"
    mkdir -p "$sample_dir"
    total=$((WARMUP + ITERS))

    for ((sample = 0; sample < total; sample++)); do
        local port log tmp_json err_log
        port="$(free_port)"
        log="$OUT/ultrasqld-${wid}-sample-${sample}.log"
        tmp_json="$sample_dir/sample-${sample}.json"
        err_log="$OUT/cross_compare-${wid}-sample-${sample}.err"
        local data_dir args
        data_dir="$DATA_ROOT/${wid}-sample-${sample}"
        args=("$ULTRASQLD_BIN" --listen "127.0.0.1:${port}" --log-level warn)
        if [[ "$STORAGE_MODE" == "data-dir" ]]; then
            rm -rf "$data_dir"
            mkdir -p "$(dirname "$data_dir")"
            args+=(--data-dir "$data_dir")
        fi
        "${args[@]}" >"$log" 2>&1 &
        server_pid="$!"
        wait_for_port "$port"
        if ! "$BIN/cross_compare_sql" \
            --server "127.0.0.1:${port}" \
            --workload "$workload" \
            --rows "$rows" \
            --storage-mode "$STORAGE_MODE" \
            --warmup 0 \
            --iters 1 \
            > "$tmp_json" 2>"$err_log"; then
            cat "$err_log" >&2 || true
            kill "$server_pid" >/dev/null 2>&1 || true
            wait "$server_pid" >/dev/null 2>&1 || true
            server_pid=""
            python3 - "$RAW/${wid}-ultrasql.json" "$wid" "$rows" "$err_log" "$log" \
                "$STORAGE_MODE" "$(durability_mode "$STORAGE_MODE")" <<'PY'
import json
import sys
from pathlib import Path

out, workload, rows, err_log, server_log, storage_mode, durability_mode = sys.argv[1:]
reason_parts = []
for path in [Path(err_log), Path(server_log)]:
    if path.exists():
        text = path.read_text(errors="replace").strip()
        if text:
            reason_parts.append(text[-2000:])
doc = {
    "schema_version": 1,
    "engine": "ultrasql",
    "workload": workload,
    "status": "not_available",
    "n_rows": int(rows),
    "server_mode": "external",
    "storage_mode": storage_mode,
    "durability_mode": durability_mode,
    "reason": "\n".join(reason_parts) or "benchmark command failed",
    "policy": "Failure is recorded as not_available; no benchmark claim is made for this row.",
}
Path(out).write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
PY
            rm -rf "$sample_dir"
            return
        fi
        kill "$server_pid" >/dev/null 2>&1 || true
        wait "$server_pid" >/dev/null 2>&1 || true
        server_pid=""
    done

    python3 - "$RAW/${wid}-ultrasql.json" "$wid" "$rows" "$WARMUP" \
        "$STORAGE_MODE" "$(durability_mode "$STORAGE_MODE")" "$sample_dir"/*.json <<'PY'
import json
import statistics
import sys
from pathlib import Path

out, workload, rows, warmup, storage_mode, durability_mode, *sample_paths = sys.argv[1:]
warmup = int(warmup)
def sample_index(path: str) -> int:
    stem = Path(path).stem
    return int(stem.rsplit("-", 1)[1])

docs = [json.loads(Path(path).read_text()) for path in sorted(sample_paths, key=sample_index)]
measured = docs[warmup:]
iters = [float(doc["median_us"]) for doc in measured]
base = dict(measured[0] if measured else docs[-1])
base.update(
    {
        "schema_version": 1,
        "engine": "ultrasql",
        "workload": workload,
        "status": "measured",
        "n_rows": int(rows),
        "server_mode": "external",
        "storage_mode": storage_mode,
        "durability_mode": durability_mode,
        "samples": len(iters),
        "iterations_us": iters,
        "median_us": statistics.median(iters),
        "min_us": min(iters),
        "policy": "Raw measured samples only; no ranking or winner claim.",
    }
)
Path(out).write_text(json.dumps(base, sort_keys=True) + "\n")
PY
    rm -rf "$sample_dir"
}

record_competitor_failure() {
    local engine="$1"
    local selector="$2"
    local rows="$3"
    local err_log="$4"
    python3 - "$RAW/${selector}-${engine}.json" "$engine" "$selector" "$rows" "$err_log" \
        "$STORAGE_MODE" "$(durability_mode "$STORAGE_MODE")" <<'PY'
import json
import sys
from pathlib import Path

out, engine, workload, rows, err_log, storage_mode, durability_mode = sys.argv[1:]
reason = "competitor benchmark command failed"
path = Path(err_log)
if path.exists():
    text = path.read_text(errors="replace").strip()
    if text:
        reason = text[-2000:]
doc = {
    "schema_version": 1,
    "engine": engine,
    "workload": workload,
    "status": "not_available",
    "n_rows": int(rows),
    "storage_mode": storage_mode,
    "durability_mode": durability_mode,
    "reason": reason,
    "policy": "Failure is recorded as not_available; no benchmark claim is made for this row.",
}
Path(out).write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
PY
}

run_competitor_script() {
    local engine="$1"
    local script="$2"
    local selector="$3"
    local rows="$4"
    local row_mode="$5"
    local raw_file="$RAW/${selector}-${engine}.json"
    local err_log="$OUT/competitor-${selector}-${engine}.err"

    rm -f "$err_log"
    if [[ "$row_mode" == "analytical" ]]; then
        if RAW_DIR="$RAW" N_ITERS="$ITERS" ANALYTICAL_ROWS="$rows" \
            BENCH_STORAGE_MODE="$STORAGE_MODE" BENCH_DATA_ROOT="$DATA_ROOT/competitors" \
            bash "$script" "$selector" 2>"$err_log"; then
            return
        fi
    else
        if RAW_DIR="$RAW" N_ITERS="$ITERS" N_ROWS="$rows" INSERT_CHUNK_ROWS="$INSERT_CHUNK_ROWS" \
            BENCH_STORAGE_MODE="$STORAGE_MODE" BENCH_DATA_ROOT="$DATA_ROOT/competitors" \
            bash "$script" "$selector" 2>"$err_log"; then
            return
        fi
    fi

    cat "$err_log" >&2 || true
    if [[ ! -s "$raw_file" ]]; then
        record_competitor_failure "$engine" "$selector" "$rows" "$err_log"
    fi
}

run_competitors() {
    local selector="$1"
    local rows="$2"
    case "$selector" in
        insert_throughput_*|select_scan_*|update_throughput_*|delete_throughput_*|mixed_oltp_pgbench_like|mixed_correctness_*)
            run_competitor_script duckdb benchmarks/scripts/run_duckdb_writes.sh "$selector" "$rows" row
            run_competitor_script sqlite3 benchmarks/scripts/run_sqlite3_writes.sh "$selector" "$rows" row
            run_competitor_script postgres benchmarks/scripts/run_postgres_writes.sh "$selector" "$rows" row
            run_competitor_script clickhouse benchmarks/scripts/run_clickhouse_writes.sh "$selector" "$rows" row
            ;;
        select_sum_*_i64|select_avg_*_i64|filter_sum_*_i64|window_row_number_*_i64)
            run_competitor_script duckdb benchmarks/scripts/run_duckdb_writes.sh "$selector" "$rows" analytical
            run_competitor_script sqlite3 benchmarks/scripts/run_sqlite3_writes.sh "$selector" "$rows" analytical
            run_competitor_script postgres benchmarks/scripts/run_postgres_writes.sh "$selector" "$rows" analytical
            run_competitor_script clickhouse benchmarks/scripts/run_clickhouse_writes.sh "$selector" "$rows" analytical
            ;;
        *) echo "run_scale_sweep.sh: unknown competitor selector $selector" >&2; exit 2 ;;
    esac
}

declare -a SPECS=(
    "insert-bulk  insert_throughput_10k"
    "select-scan  select_scan_10k"
    "sum-scalar   select_sum_65k_i64"
    "avg-scalar   select_avg_1m_i64"
    "filter-sum   filter_sum_1m_i64"
    "update-bulk  update_throughput_10k"
    "delete-bulk  delete_throughput_10k"
)

declare -a FIXED_SPECS=(
    "mixed-oltp         mixed_oltp_pgbench_like      10000"
    "mixed-correctness  mixed_correctness_100k       100000"
    "window-row-number  window_row_number_65k_i64    65536"
)

for rows in $ROWS; do
    for spec in "${SPECS[@]}"; do
        read -r workload selector <<<"$spec"
        wid="$(workload_id "$workload" "$rows")"
        echo "--- UltraSQL $wid rows=$rows ---"
        run_ultrasql_workload "$workload" "$rows" "$wid"
        echo "--- Competitors $wid rows=$rows ---"
        run_competitors "$selector" "$rows"
    done
done

for spec in "${FIXED_SPECS[@]}"; do
    read -r workload selector rows <<<"$spec"
    wid="$(workload_id "$workload" "$rows")"
    echo "--- UltraSQL $wid rows=$rows ---"
    run_ultrasql_workload "$workload" "$rows" "$wid"
    echo "--- Competitors $wid rows=$rows ---"
    run_competitors "$selector" "$rows"
done

echo "--- Rendering scale sweep artifacts ---"
"$BIN/results-render" \
    --raw-dir "$RAW" \
    --output-md "$OUT/results.md" \
    --output-json "$OUT/results.json"

python3 benchmarks/scripts/render_scale_sweep.py \
    --raw-dir "$RAW" \
    --output-md "$OUT/scale_sweep.md" \
    --output-json "$OUT/scale_sweep.json"

python3 - "$OUT/scale_sweep_manifest.json" "$mode" "$ITERS" "$WARMUP" "$ROWS" "$ULTRASQL_VERSION_TEXT" "$install_source" "${SCALE_SWEEP_APPEND:-0}" "$STORAGE_MODE" <<'PY'
import json
import os
import platform
import socket
import subprocess
import sys
from pathlib import Path

path, mode, iters, warmup, rows, version, install_source, append, storage_mode = sys.argv[1:]

def cmd_output(*cmd: str) -> str | None:
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return None

def cpu_model() -> str | None:
    if platform.system() == "Darwin":
        return cmd_output("sysctl", "-n", "machdep.cpu.brand_string")
    if Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text(errors="replace").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor() or None

def memory_bytes() -> int | None:
    if platform.system() == "Darwin":
        value = cmd_output("sysctl", "-n", "hw.memsize")
        return int(value) if value and value.isdigit() else None
    if Path("/proc/meminfo").exists():
        for line in Path("/proc/meminfo").read_text(errors="replace").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    return None

def postgres_server_version() -> str | None:
    user = os.environ.get("PGUSER") or cmd_output("id", "-un") or ""
    database = os.environ.get("PGDATABASE", "ultrasql_bench")
    try:
        return subprocess.check_output(
            ["psql", "-U", user, "-d", database, "-q", "--no-align", "-t", "-c", "SHOW server_version;"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None

path_obj = Path(path)
existing_rows = []
if append == "1" and path_obj.exists():
    try:
        existing_rows = json.loads(path_obj.read_text()).get("rows", [])
    except json.JSONDecodeError:
        existing_rows = []
merged_rows = sorted(set(int(part) for part in rows.split()) | set(int(row) for row in existing_rows))
doc = {
    "schema_version": 1,
    "mode": mode,
    "iters": int(iters),
    "warmup": int(warmup),
    "rows": merged_rows,
    "ultrasql_version": version,
    "ultrasql_install_source": install_source,
    "ultrasql_storage_mode": storage_mode,
    "methodology": "UltraSQL external release artifact over TCP; competitors installed local clients including ClickHouse when available; bulk INSERT uses a fresh UltraSQL server per measured sample and 10k-row INSERT chunks across engines.",
    "host": {
        "hostname": socket.gethostname(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": cpu_model(),
        "logical_cpus": os.cpu_count(),
        "memory_bytes": memory_bytes(),
        "rustc": cmd_output("rustc", "--version"),
        "git_commit": cmd_output("git", "rev-parse", "HEAD"),
    },
    "engine_versions": {
        "ultrasql": version,
        "duckdb": cmd_output("duckdb", "--version"),
        "clickhouse": cmd_output(os.environ.get("CH_BIN", "clickhouse"), "--version"),
        "sqlite": cmd_output("sqlite3", "--version"),
        "postgres": postgres_server_version(),
    },
}
with open(path_obj, "w", encoding="utf-8") as fh:
    json.dump(doc, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY

python3 benchmarks/scripts/check_supremacy.py "$RAW"

echo "=== Done. Scale sweep in $OUT ==="
