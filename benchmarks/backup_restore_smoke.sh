#!/usr/bin/env bash
# Backup/restore smoke runner.
#
# Uses: ultrasql --basebackup, ultrasql --pg-dump, ultrasql --pg-restore.
# The artifact is measured only after row-count and indexed-point-query checks
# pass against a restored server.
# Missing prerequisites emit `"status": "not_available"`.

set -euo pipefail

PROFILE="${BACKUP_RESTORE_PROFILE:-${1:-smoke}}"
OUT_DIR="${BACKUP_RESTORE_OUT_DIR:-benchmarks/results/latest}"
WORK_DIR="${BACKUP_RESTORE_WORK_DIR:-target/backup-restore-smoke}"
MANIFEST="$OUT_DIR/backup_restore_smoke_manifest.json"
ULTRASQL_BIN="${ULTRASQL_BIN:-target/release/ultrasql}"
ULTRASQLD_BIN="${ULTRASQLD_BIN:-target/release/ultrasqld}"
PSQL="${PSQL:-psql}"

SOURCE_DATA_DIR="${BACKUP_RESTORE_SOURCE_DATA_DIR:-$WORK_DIR/source-data}"
RESTORE_DATA_DIR="${BACKUP_RESTORE_RESTORE_DATA_DIR:-$WORK_DIR/restored-data}"
BASEBACKUP_DIR="$WORK_DIR/basebackup"
DUMP_PATH="$WORK_DIR/backup_restore_smoke-custom.dump"
FORMAT_RESULTS_PATH="$WORK_DIR/dump-format-results.jsonl"
DUMP_FORMATS="${BACKUP_RESTORE_DUMP_FORMATS:-custom directory tar}"
SOURCE_LOG="$WORK_DIR/source.log"

SOURCE_PID=""
RESTORE_PID=""

cleanup() {
    for pid in "$RESTORE_PID" "$SOURCE_PID"; do
        if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
            kill "$pid" >/dev/null 2>&1 || true
            wait "$pid" >/dev/null 2>&1 || true
        fi
    done
}
trap cleanup EXIT

dump_format_results_json() {
    if [ ! -s "$FORMAT_RESULTS_PATH" ]; then
        printf '[]\n'
        return
    fi
    FORMAT_RESULTS_PATH="$FORMAT_RESULTS_PATH" python3 - <<'PY'
import json
import os

with open(os.environ["FORMAT_RESULTS_PATH"], encoding="utf-8") as handle:
    print(json.dumps([json.loads(line) for line in handle if line.strip()]))
PY
}

json_emit() {
    local status="$1"
    local reason="$2"
    local row_count_verified="$3"
    local index_query_verified="$4"
    local source_row_count="${5:-}"
    local restored_row_count="${6:-}"
    local index_query_result="${7:-}"
    local dump_format_results="${8:-[]}"
    mkdir -p "$OUT_DIR"
    STATUS="$status" \
    REASON="$reason" \
    PROFILE="$PROFILE" \
    ROW_COUNT_VERIFIED="$row_count_verified" \
    INDEX_QUERY_VERIFIED="$index_query_verified" \
    SOURCE_ROW_COUNT="$source_row_count" \
    RESTORED_ROW_COUNT="$restored_row_count" \
    INDEX_QUERY_RESULT="$index_query_result" \
    DUMP_FORMAT_RESULTS="$dump_format_results" \
    SOURCE_DATA_DIR="$SOURCE_DATA_DIR" \
    RESTORE_DATA_DIR="$RESTORE_DATA_DIR" \
    BASEBACKUP_DIR="$BASEBACKUP_DIR" \
    DUMP_PATH="$DUMP_PATH" \
    python3 - <<'PY' > "$MANIFEST"
import json
import os
import platform
import time

def bool_env(name: str) -> bool:
    return os.environ.get(name) == "1"

dump_format_results = json.loads(os.environ["DUMP_FORMAT_RESULTS"])
doc = {
    "schema_version": 1,
    "suite": "backup_restore_smoke",
    "profile": os.environ["PROFILE"],
    "status": os.environ["STATUS"],
    "reason": os.environ["REASON"],
    "source_data_dir": os.environ["SOURCE_DATA_DIR"],
    "restore_data_dir": os.environ["RESTORE_DATA_DIR"],
    "basebackup_dir": os.environ["BASEBACKUP_DIR"],
    "dump_path": os.environ["DUMP_PATH"],
    "dump_format_results": dump_format_results,
    "dump_formats_verified": [
        item["format"]
        for item in dump_format_results
        if item.get("row_count_verified") and item.get("index_query_verified")
    ],
    "row_count_verified": bool_env("ROW_COUNT_VERIFIED"),
    "index_query_verified": bool_env("INDEX_QUERY_VERIFIED"),
    "source_row_count": os.environ.get("SOURCE_ROW_COUNT") or None,
    "restored_row_count": os.environ.get("RESTORED_ROW_COUNT") or None,
    "index_query_result": os.environ.get("INDEX_QUERY_RESULT") or None,
    "created_at_unix": time.time(),
    "host": {
        "cpu": platform.processor() or platform.machine(),
        "os": platform.platform(),
        "machine": platform.machine(),
    },
    "policy": (
        "Backup/restore is measured only when basebackup, pg-dump, "
        "pg-restore, restored row count, and restored indexed query all pass "
        "for every requested dump format."
    ),
}
print(json.dumps(doc, indent=2, sort_keys=True))
PY
}

need_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        json_emit "not_available" "${cmd}_missing" "0" "0"
        echo "backup_restore_smoke.sh: missing command: $cmd" >&2
        exit 2
    fi
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

build_bins() {
    if [ "${BACKUP_RESTORE_BUILD:-1}" != "1" ]; then
        return
    fi
    cargo build --release -p ultrasql-cli --bin ultrasql
    cargo build --release -p ultrasql-server --bin ultrasqld
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

start_server() {
    local data_dir="$1"
    local port="$2"
    local log_path="$3"
    "$ULTRASQLD_BIN" \
        --data-dir "$data_dir" \
        --listen "127.0.0.1:$port" \
        --log-level warn \
        >"$log_path" 2>&1 &
    echo "$!"
}

stop_restored_server() {
    if [ -n "$RESTORE_PID" ] && kill -0 "$RESTORE_PID" >/dev/null 2>&1; then
        kill "$RESTORE_PID" >/dev/null 2>&1 || true
        wait "$RESTORE_PID" >/dev/null 2>&1 || true
    fi
    RESTORE_PID=""
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

fail() {
    local reason="$1"
    json_emit "failed" "$reason" "0" "0" "" "" "" "$(dump_format_results_json)"
    echo "backup_restore_smoke.sh: $reason" >&2
    exit 1
}

dump_path_for_format() {
    local format="$1"
    case "$format" in
        custom) printf '%s\n' "$WORK_DIR/backup_restore_smoke-custom.dump" ;;
        directory) printf '%s\n' "$WORK_DIR/backup_restore_smoke-directory.dir" ;;
        tar) printf '%s\n' "$WORK_DIR/backup_restore_smoke-tar.tar" ;;
        plain) printf '%s\n' "$WORK_DIR/backup_restore_smoke-plain.sql" ;;
        *) fail "unsupported_dump_format_${format}" ;;
    esac
}

record_dump_format_result() {
    local format="$1"
    local dump_path="$2"
    local restore_data_dir="$3"
    local restored_count="$4"
    local index_result="$5"
    FORMAT="$format" \
    DUMP_PATH="$dump_path" \
    RESTORE_DATA_DIR="$restore_data_dir" \
    SOURCE_ROW_COUNT="$SOURCE_COUNT" \
    RESTORED_ROW_COUNT="$restored_count" \
    INDEX_QUERY_RESULT="$index_result" \
    python3 - <<'PY' >> "$FORMAT_RESULTS_PATH"
import json
import os

print(json.dumps({
    "format": os.environ["FORMAT"],
    "dump_path": os.environ["DUMP_PATH"],
    "restore_data_dir": os.environ["RESTORE_DATA_DIR"],
    "row_count_verified": os.environ["SOURCE_ROW_COUNT"] == os.environ["RESTORED_ROW_COUNT"],
    "index_query_verified": os.environ["INDEX_QUERY_RESULT"] == "bravo",
    "source_row_count": os.environ["SOURCE_ROW_COUNT"],
    "restored_row_count": os.environ["RESTORED_ROW_COUNT"],
    "index_query_result": os.environ["INDEX_QUERY_RESULT"],
}, sort_keys=True))
PY
}

verify_restored_dump() {
    local format="$1"
    local dump_path
    dump_path="$(dump_path_for_format "$format")"
    local restore_data_dir="${RESTORE_DATA_DIR}-${format}"
    local restore_log="$WORK_DIR/restored-${format}.log"
    local restore_port
    restore_port="$(pick_port)"
    local restore_dsn="postgresql://ultrasql@127.0.0.1:$restore_port/ultrasql?sslmode=disable"

    "$ULTRASQL_BIN" --data-dir "$SOURCE_DATA_DIR" --pg-dump "$dump_path" --dump-format "$format" \
        || fail "pg_dump_${format}_failed"
    "$ULTRASQL_BIN" --data-dir "$restore_data_dir" --pg-restore "$dump_path" \
        || fail "pg_restore_${format}_failed"
    chmod 700 "$restore_data_dir" \
        || fail "restore_chmod_${format}_failed"
    "$ULTRASQL_BIN" --data-dir "$restore_data_dir" validate \
        || fail "restore_validate_${format}_failed"

    RESTORE_PID="$(start_server "$restore_data_dir" "$restore_port" "$restore_log")"
    wait_psql_ready "$restore_dsn" || fail "restored_server_${format}_not_ready"

    local restored_count
    restored_count="$(query_scalar "$restore_dsn" "SELECT COUNT(*) FROM backup_restore_smoke")" \
        || fail "restored_count_${format}_failed"
    local index_result
    index_result="$(query_scalar "$restore_dsn" "SELECT payload FROM backup_restore_smoke WHERE id = 2")" \
        || fail "restored_index_query_${format}_failed"

    record_dump_format_result "$format" "$dump_path" "$restore_data_dir" "$restored_count" "$index_result"

    if [ "$restored_count" != "$SOURCE_COUNT" ]; then
        json_emit "failed" "restored_count_${format}_mismatch" "0" "0" \
            "$SOURCE_COUNT" "$restored_count" "$index_result" "$(dump_format_results_json)"
        exit 1
    fi
    if [ "$index_result" != "bravo" ]; then
        json_emit "failed" "restored_index_query_${format}_mismatch" "1" "0" \
            "$SOURCE_COUNT" "$restored_count" "$index_result" "$(dump_format_results_json)"
        exit 1
    fi

    stop_restored_server
}

case "$PROFILE" in
    smoke|full) ;;
    *)
        echo "backup_restore_smoke.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

need_cmd python3
need_cmd cargo
need_cmd "$PSQL"
build_bins

if [ ! -x "$ULTRASQL_BIN" ]; then
    json_emit "not_available" "ultrasql_binary_missing" "0" "0"
    exit 2
fi
if [ ! -x "$ULTRASQLD_BIN" ]; then
    json_emit "not_available" "ultrasqld_binary_missing" "0" "0"
    exit 2
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" "$OUT_DIR"

SOURCE_PORT="$(pick_port)"
SOURCE_DSN="postgresql://ultrasql@127.0.0.1:$SOURCE_PORT/ultrasql?sslmode=disable"

SOURCE_PID="$(start_server "$SOURCE_DATA_DIR" "$SOURCE_PORT" "$SOURCE_LOG")"
wait_psql_ready "$SOURCE_DSN" || fail "source_server_not_ready"

run_psql "$SOURCE_DSN" "CREATE TABLE backup_restore_smoke (id INT, payload TEXT)" \
    || fail "source_create_table_failed"
run_psql "$SOURCE_DSN" "INSERT INTO backup_restore_smoke VALUES (1, 'alpha')" \
    || fail "source_insert_1_failed"
run_psql "$SOURCE_DSN" "INSERT INTO backup_restore_smoke VALUES (2, 'bravo')" \
    || fail "source_insert_2_failed"
run_psql "$SOURCE_DSN" "INSERT INTO backup_restore_smoke VALUES (3, 'charlie')" \
    || fail "source_insert_3_failed"
run_psql "$SOURCE_DSN" "CREATE INDEX backup_restore_smoke_id_idx ON backup_restore_smoke (id)" \
    || fail "source_create_index_failed"

SOURCE_COUNT="$(query_scalar "$SOURCE_DSN" "SELECT COUNT(*) FROM backup_restore_smoke")" \
    || fail "source_count_failed"
[ "$SOURCE_COUNT" = "3" ] || fail "source_count_mismatch"

kill "$SOURCE_PID" >/dev/null 2>&1 || true
wait "$SOURCE_PID" >/dev/null 2>&1 || true
SOURCE_PID=""

"$ULTRASQL_BIN" --data-dir "$SOURCE_DATA_DIR" --basebackup "$BASEBACKUP_DIR" \
    || fail "basebackup_failed"
for format in $DUMP_FORMATS; do
    verify_restored_dump "$format"
done

json_emit "measured" "ok" "1" "1" "$SOURCE_COUNT" "$SOURCE_COUNT" "bravo" "$(dump_format_results_json)"
echo "backup/restore smoke measured: rows=$SOURCE_COUNT indexed_payload=bravo formats=$DUMP_FORMATS"
