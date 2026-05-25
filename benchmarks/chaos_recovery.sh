#!/usr/bin/env bash
# Chaos recovery smoke/full harness.
#
# Exercises three production failure classes against a real ultrasqld process:
# random kill, WAL truncation, and safe disk-full simulation. The disk-full
# leg uses a child-process file-size cap (`ulimit -f`) so it never fills the
# host filesystem. Missing prerequisites emit `"status": "not_available"`.

set -euo pipefail

PROFILE="${CHAOS_PROFILE:-${1:-smoke}}"
OUT_DIR="${CHAOS_OUT_DIR:-benchmarks/results/latest}"
WORK_DIR="${CHAOS_WORK_DIR:-target/chaos-recovery}"
MANIFEST="$OUT_DIR/chaos_recovery_manifest.json"
ULTRASQL_BIN="${ULTRASQL_BIN:-target/release/ultrasql}"
ULTRASQLD_BIN="${ULTRASQLD_BIN:-target/release/ultrasqld}"
PSQL="${PSQL:-psql}"
CHAOS_SEED="${CHAOS_SEED:-20260525}"
STATUS_FILE="$(mktemp)"
ACTIVE_PIDS=()

case "$PROFILE" in
    smoke)
        RANDOM_ROWS="${CHAOS_RANDOM_ROWS:-12}"
        WAL_TRUNC_ROWS="${CHAOS_WAL_TRUNC_ROWS:-8}"
        DISK_FULL_MAX_INSERTS="${CHAOS_DISK_FULL_MAX_INSERTS:-80}"
        DISK_FULL_PAYLOAD_BYTES="${CHAOS_DISK_FULL_PAYLOAD_BYTES:-2048}"
        DISK_FULL_MARGIN_BYTES="${CHAOS_DISK_FULL_MARGIN_BYTES:-8192}"
        ;;
    full)
        RANDOM_ROWS="${CHAOS_RANDOM_ROWS:-96}"
        WAL_TRUNC_ROWS="${CHAOS_WAL_TRUNC_ROWS:-64}"
        DISK_FULL_MAX_INSERTS="${CHAOS_DISK_FULL_MAX_INSERTS:-512}"
        DISK_FULL_PAYLOAD_BYTES="${CHAOS_DISK_FULL_PAYLOAD_BYTES:-8192}"
        DISK_FULL_MARGIN_BYTES="${CHAOS_DISK_FULL_MARGIN_BYTES:-16384}"
        ;;
    *)
        echo "chaos_recovery.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

cleanup() {
    if [ "${#ACTIVE_PIDS[@]}" -gt 0 ]; then
        for pid in "${ACTIVE_PIDS[@]}"; do
            if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
                kill "$pid" >/dev/null 2>&1 || true
                wait "$pid" >/dev/null 2>&1 || true
            fi
        done
    fi
    rm -f "$STATUS_FILE"
}
trap cleanup EXIT

remember_pid() {
    ACTIVE_PIDS+=("$1")
}

sanitize() {
    printf '%s' "$*" | tr '\t\n' '  ' | cut -c 1-800
}

record_case() {
    local name="$1"
    local status="$2"
    local reason="$3"
    local restarted_after_kill="$4"
    local truncated_wal_recovered="$5"
    local disk_full_recovered_without_corruption="$6"
    local row_count_verified="$7"
    local expected_rows="$8"
    local recovered_rows="$9"
    local detail="${10:-}"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$name" \
        "$status" \
        "$(sanitize "$reason")" \
        "$restarted_after_kill" \
        "$truncated_wal_recovered" \
        "$disk_full_recovered_without_corruption" \
        "$row_count_verified" \
        "$expected_rows" \
        "$recovered_rows" \
        "$(sanitize "$detail")" >>"$STATUS_FILE"
}

finish() {
    mkdir -p "$OUT_DIR"
    python3 - "$PROFILE" "$MANIFEST" "$STATUS_FILE" "$CHAOS_SEED" "$RANDOM_ROWS" "$WAL_TRUNC_ROWS" "$DISK_FULL_MAX_INSERTS" <<'PY'
import json
import os
import pathlib
import platform
import sys
import time

(
    profile,
    manifest_path,
    status_path,
    seed,
    random_rows,
    wal_trunc_rows,
    disk_full_max_inserts,
) = sys.argv[1:]

def bool_text(value):
    return value == "1"

cases = []
if pathlib.Path(status_path).exists():
    for line in pathlib.Path(status_path).read_text(encoding="utf-8").splitlines():
        (
            name,
            status,
            reason,
            restarted_after_kill,
            truncated_wal_recovered,
            disk_full_recovered_without_corruption,
            row_count_verified,
            expected_rows,
            recovered_rows,
            detail,
        ) = line.split("\t", 9)
        cases.append(
            {
                "name": name,
                "status": status,
                "reason": reason or None,
                "restarted_after_kill": bool_text(restarted_after_kill),
                "truncated_wal_recovered": bool_text(truncated_wal_recovered),
                "disk_full_recovered_without_corruption": bool_text(
                    disk_full_recovered_without_corruption
                ),
                "row_count_verified": bool_text(row_count_verified),
                "expected_rows": int(expected_rows) if expected_rows else None,
                "recovered_rows": int(recovered_rows) if recovered_rows else None,
                "detail": detail or None,
            }
        )

has_failed = any(case["status"] == "failed" for case in cases)
has_unavailable = any(case["status"] == "not_available" for case in cases)
passed = bool(cases) and not has_failed and not has_unavailable
doc = {
    "schema_version": 1,
    "suite": "chaos_recovery",
    "profile": profile,
    "status": "measured" if passed else "failed" if has_failed else "not_available",
    "passed": passed,
    "generated_at_unix": int(time.time()),
    "chaos_seed": seed,
    "random_kill_rows": int(random_rows),
    "wal_truncation_rows": int(wal_trunc_rows),
    "disk_full_max_inserts": int(disk_full_max_inserts),
    "cases": cases,
    "host": {
        "cpu": platform.processor() or platform.machine(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "cores": os.cpu_count() or 0,
    },
    "policy": (
        "Chaos recovery is measured only when random kill restart, WAL "
        "truncation restart, and safe disk-full recovery all verify row counts "
        "and data-directory validation."
    ),
}
pathlib.Path(manifest_path).write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
print(json.dumps(doc, indent=2, sort_keys=True))
if passed:
    sys.exit(0)
if has_failed:
    sys.exit(1)
sys.exit(2)
PY
}

need_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        record_case "setup" "not_available" "${cmd}_missing" "0" "0" "0" "0" "" "" ""
        finish
        exit "$?"
    fi
}

build_bins() {
    if [ "${CHAOS_BUILD:-1}" != "1" ]; then
        return
    fi
    cargo build --release -p ultrasql-cli --bin ultrasql
    cargo build --release -p ultrasql-server --bin ultrasqld
}

pick_port() {
    python3 - <<'PY'
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

start_server() {
    local data_dir="$1"
    local port="$2"
    local log_path="$3"
    "$ULTRASQLD_BIN" \
        --data-dir "$data_dir" \
        --listen "127.0.0.1:$port" \
        --log-level warn \
        --autovacuum-interval-ms 0 \
        >"$log_path" 2>&1 &
    local pid="$!"
    remember_pid "$pid"
    echo "$pid"
}

start_server_with_fsize_limit() {
    local data_dir="$1"
    local port="$2"
    local log_path="$3"
    local fsize_blocks="$4"
    (
        ulimit -f "$fsize_blocks"
        exec "$ULTRASQLD_BIN" \
            --data-dir "$data_dir" \
            --listen "127.0.0.1:$port" \
            --log-level warn \
            --autovacuum-interval-ms 0
    ) >"$log_path" 2>&1 &
    local pid="$!"
    remember_pid "$pid"
    echo "$pid"
}

stop_pid() {
    local pid="$1"
    if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
        kill "$pid" >/dev/null 2>&1 || true
        wait "$pid" >/dev/null 2>&1 || true
    fi
}

kill_pid_hard() {
    local pid="$1"
    if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
        kill -9 "$pid" >/dev/null 2>&1 || true
        wait "$pid" >/dev/null 2>&1 || true
    fi
}

wait_psql_ready() {
    local dsn="$1"
    for _ in $(seq 1 100); do
        if "$PSQL" "$dsn" -At -c "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

run_psql() {
    local dsn="$1"
    local sql="$2"
    "$PSQL" "$dsn" -v ON_ERROR_STOP=1 -X -q -c "$sql"
}

query_scalar() {
    local dsn="$1"
    local sql="$2"
    "$PSQL" "$dsn" -v ON_ERROR_STOP=1 -X -q -At -c "$sql"
}

validate_data_dir() {
    local data_dir="$1"
    "$ULTRASQL_BIN" --data-dir "$data_dir" validate >/dev/null
}

kill_after_row() {
    python3 - "$CHAOS_SEED" "$RANDOM_ROWS" <<'PY'
import random
import sys

seed, rows = sys.argv[1:]
print(random.Random(seed).randint(1, int(rows)))
PY
}

truncate_last_wal_segment() {
    local data_dir="$1"
    local bytes="${2:-7}"
    python3 - "$data_dir/pg_wal" "$bytes" <<'PY'
import pathlib
import sys

wal_dir = pathlib.Path(sys.argv[1])
drop = int(sys.argv[2])
segments = sorted(path for path in wal_dir.glob("segment_*") if path.is_file())
if not segments:
    raise SystemExit("no WAL segments found")
path = segments[-1]
before = path.stat().st_size
after = max(0, before - drop)
with path.open("r+b") as handle:
    handle.truncate(after)
print(f"{path}\t{before}\t{after}")
PY
}

fsize_blocks_for_disk_full() {
    local data_dir="$1"
    python3 - "$data_dir/pg_wal" "$DISK_FULL_MARGIN_BYTES" <<'PY'
import math
import pathlib
import sys

wal_dir = pathlib.Path(sys.argv[1])
margin = int(sys.argv[2])
segments = sorted(path for path in wal_dir.glob("segment_*") if path.is_file())
current_size = segments[-1].stat().st_size if segments else 0
limit = max(current_size + margin, 64 * 1024)
print(max(1, math.ceil(limit / 512)))
PY
}

payload_literal() {
    python3 - "$DISK_FULL_PAYLOAD_BYTES" <<'PY'
import sys

print("x" * int(sys.argv[1]))
PY
}

run_random_kill_case() {
    local data_dir="$WORK_DIR/random-kill-data"
    local log1="$WORK_DIR/random-kill-1.log"
    local log2="$WORK_DIR/random-kill-2.log"
    local port
    port="$(pick_port)"
    local dsn="postgresql://ultrasql@127.0.0.1:$port/ultrasql?sslmode=disable"
    local pid
    pid="$(start_server "$data_dir" "$port" "$log1")"
    if ! wait_psql_ready "$dsn"; then
        record_case "random_kill" "failed" "server_not_ready_before_kill" "0" "0" "0" "0" "" "" "$log1"
        return
    fi

    run_psql "$dsn" "CREATE TABLE chaos_random_kill (id INT, payload TEXT)" >/dev/null \
        || {
            record_case "random_kill" "failed" "create_table_failed" "0" "0" "0" "0" "" "" "$log1"
            stop_pid "$pid"
            return
        }

    local kill_after
    kill_after="$(kill_after_row)"
    local inserted=0
    for i in $(seq 1 "$RANDOM_ROWS"); do
        run_psql "$dsn" "INSERT INTO chaos_random_kill VALUES ($i, 'row-$i')" >/dev/null \
            || break
        inserted="$i"
        if [ "$i" = "$kill_after" ]; then
            kill_pid_hard "$pid"
            break
        fi
    done
    if kill -0 "$pid" >/dev/null 2>&1; then
        kill_pid_hard "$pid"
    fi

    port="$(pick_port)"
    dsn="postgresql://ultrasql@127.0.0.1:$port/ultrasql?sslmode=disable"
    pid="$(start_server "$data_dir" "$port" "$log2")"
    if ! wait_psql_ready "$dsn"; then
        record_case "random_kill" "failed" "server_not_ready_after_kill" "0" "0" "0" "0" "$inserted" "" "$log2"
        return
    fi
    local recovered
    recovered="$(query_scalar "$dsn" "SELECT COUNT(*) FROM chaos_random_kill")" \
        || {
            record_case "random_kill" "failed" "count_after_kill_failed" "1" "0" "0" "0" "$inserted" "" "$log2"
            stop_pid "$pid"
            return
        }
    stop_pid "$pid"
    if [ "$recovered" = "$inserted" ] && validate_data_dir "$data_dir"; then
        record_case "random_kill" "passed" "ok" "1" "0" "0" "1" "$inserted" "$recovered" "kill_after=$kill_after"
    else
        record_case "random_kill" "failed" "row_count_mismatch_after_kill" "1" "0" "0" "0" "$inserted" "$recovered" "kill_after=$kill_after"
    fi
}

run_wal_truncation_case() {
    local data_dir="$WORK_DIR/wal-truncation-data"
    local log1="$WORK_DIR/wal-truncation-1.log"
    local log2="$WORK_DIR/wal-truncation-2.log"
    local port
    port="$(pick_port)"
    local dsn="postgresql://ultrasql@127.0.0.1:$port/ultrasql?sslmode=disable"
    local pid
    pid="$(start_server "$data_dir" "$port" "$log1")"
    if ! wait_psql_ready "$dsn"; then
        record_case "wal_truncation" "failed" "server_not_ready_before_truncate" "0" "0" "0" "0" "" "" "$log1"
        return
    fi
    run_psql "$dsn" "CREATE TABLE chaos_wal_truncation (id INT, payload TEXT)" >/dev/null \
        || {
            record_case "wal_truncation" "failed" "create_table_failed" "0" "0" "0" "0" "" "" "$log1"
            stop_pid "$pid"
            return
        }
    for i in $(seq 1 "$WAL_TRUNC_ROWS"); do
        run_psql "$dsn" "INSERT INTO chaos_wal_truncation VALUES ($i, 'row-$i')" >/dev/null \
            || {
                record_case "wal_truncation" "failed" "insert_before_truncate_failed" "0" "0" "0" "0" "$WAL_TRUNC_ROWS" "$i" "$log1"
                stop_pid "$pid"
                return
            }
    done
    local expected
    expected="$(query_scalar "$dsn" "SELECT COUNT(*) FROM chaos_wal_truncation")" \
        || {
            record_case "wal_truncation" "failed" "count_before_truncate_failed" "0" "0" "0" "0" "$WAL_TRUNC_ROWS" "" "$log1"
            stop_pid "$pid"
            return
        }
    stop_pid "$pid"

    local truncation
    if ! truncation="$(truncate_last_wal_segment "$data_dir" 7 2>&1)"; then
        record_case "wal_truncation" "failed" "truncate_last_wal_segment_failed" "0" "0" "0" "0" "$expected" "" "$truncation"
        return
    fi

    port="$(pick_port)"
    dsn="postgresql://ultrasql@127.0.0.1:$port/ultrasql?sslmode=disable"
    pid="$(start_server "$data_dir" "$port" "$log2")"
    if ! wait_psql_ready "$dsn"; then
        record_case "wal_truncation" "failed" "server_not_ready_after_truncate" "0" "0" "0" "0" "$expected" "" "$log2"
        return
    fi
    local recovered
    recovered="$(query_scalar "$dsn" "SELECT COUNT(*) FROM chaos_wal_truncation")" \
        || {
            record_case "wal_truncation" "failed" "count_after_truncate_failed" "0" "1" "0" "0" "$expected" "" "$log2"
            stop_pid "$pid"
            return
        }
    stop_pid "$pid"
    if [ "$recovered" = "$expected" ] && validate_data_dir "$data_dir"; then
        record_case "wal_truncation" "passed" "ok" "0" "1" "0" "1" "$expected" "$recovered" "$truncation"
    else
        record_case "wal_truncation" "failed" "row_count_mismatch_after_truncate" "0" "1" "0" "0" "$expected" "$recovered" "$truncation"
    fi
}

run_disk_full_case() {
    if ! (ulimit -f 1024) >/dev/null 2>&1; then
        record_case "disk_full" "not_available" "ulimit_f_unavailable" "0" "0" "0" "0" "" "" ""
        return
    fi

    local data_dir="$WORK_DIR/disk-full-data"
    local log1="$WORK_DIR/disk-full-1.log"
    local log2="$WORK_DIR/disk-full-2.log"
    local log3="$WORK_DIR/disk-full-3.log"
    local port
    port="$(pick_port)"
    local dsn="postgresql://ultrasql@127.0.0.1:$port/ultrasql?sslmode=disable"
    local pid
    pid="$(start_server "$data_dir" "$port" "$log1")"
    if ! wait_psql_ready "$dsn"; then
        record_case "disk_full" "failed" "server_not_ready_before_disk_full" "0" "0" "0" "0" "" "" "$log1"
        return
    fi
    run_psql "$dsn" "CREATE TABLE chaos_disk_full (id INT, payload TEXT)" >/dev/null \
        || {
            record_case "disk_full" "failed" "create_table_failed" "0" "0" "0" "0" "" "" "$log1"
            stop_pid "$pid"
            return
        }
    run_psql "$dsn" "INSERT INTO chaos_disk_full VALUES (0, 'baseline')" >/dev/null \
        || {
            record_case "disk_full" "failed" "baseline_insert_failed" "0" "0" "0" "0" "1" "" "$log1"
            stop_pid "$pid"
            return
        }
    stop_pid "$pid"

    local fsize_blocks
    fsize_blocks="$(fsize_blocks_for_disk_full "$data_dir")"
    port="$(pick_port)"
    dsn="postgresql://ultrasql@127.0.0.1:$port/ultrasql?sslmode=disable"
    pid="$(start_server_with_fsize_limit "$data_dir" "$port" "$log2" "$fsize_blocks")"
    if ! wait_psql_ready "$dsn"; then
        record_case "disk_full" "failed" "server_not_ready_under_disk_full_limit" "0" "0" "0" "0" "1" "" "fsize_blocks=$fsize_blocks"
        return
    fi

    local payload
    payload="$(payload_literal)"
    local successful=1
    local failure_seen=0
    local failure_detail=""
    for i in $(seq 1 "$DISK_FULL_MAX_INSERTS"); do
        if run_psql "$dsn" "INSERT INTO chaos_disk_full VALUES ($i, '$payload')" >/dev/null 2>"$WORK_DIR/disk-full-insert.err"; then
            successful="$((successful + 1))"
        else
            failure_seen=1
            failure_detail="$(cat "$WORK_DIR/disk-full-insert.err" 2>/dev/null || true)"
            break
        fi
    done

    stop_pid "$pid"
    if [ "$failure_seen" != "1" ]; then
        record_case "disk_full" "not_available" "disk_full_not_triggered" "0" "0" "0" "0" "$successful" "" "fsize_blocks=$fsize_blocks"
        return
    fi

    port="$(pick_port)"
    dsn="postgresql://ultrasql@127.0.0.1:$port/ultrasql?sslmode=disable"
    pid="$(start_server "$data_dir" "$port" "$log3")"
    if ! wait_psql_ready "$dsn"; then
        record_case "disk_full" "failed" "server_not_ready_after_disk_full" "0" "0" "0" "0" "$successful" "" "$log3"
        return
    fi
    local recovered
    recovered="$(query_scalar "$dsn" "SELECT COUNT(*) FROM chaos_disk_full")" \
        || {
            record_case "disk_full" "failed" "count_after_disk_full_failed" "0" "0" "0" "0" "$successful" "" "$log3"
            stop_pid "$pid"
            return
        }
    stop_pid "$pid"
    if [ "$recovered" = "$successful" ] && validate_data_dir "$data_dir"; then
        record_case "disk_full" "passed" "ok" "0" "0" "1" "1" "$successful" "$recovered" "fsize_blocks=$fsize_blocks; failure=$(sanitize "$failure_detail")"
    else
        record_case "disk_full" "failed" "row_count_mismatch_after_disk_full" "0" "0" "0" "0" "$successful" "$recovered" "fsize_blocks=$fsize_blocks"
    fi
}

need_cmd python3
need_cmd cargo
need_cmd "$PSQL"

build_bins
if [ ! -x "$ULTRASQL_BIN" ]; then
    record_case "setup" "not_available" "ultrasql_binary_missing" "0" "0" "0" "0" "" "" "$ULTRASQL_BIN"
    finish
    exit "$?"
fi
if [ ! -x "$ULTRASQLD_BIN" ]; then
    record_case "setup" "not_available" "ultrasqld_binary_missing" "0" "0" "0" "0" "" "" "$ULTRASQLD_BIN"
    finish
    exit "$?"
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" "$OUT_DIR"

run_random_kill_case
run_wal_truncation_case
run_disk_full_case
finish
