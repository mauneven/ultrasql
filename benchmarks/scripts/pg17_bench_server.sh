#!/usr/bin/env bash
# pg17_bench_server.sh — bring up a fairly-tuned, same-host PostgreSQL 17
# cluster for the release-artifact scale sweep and OLTP certifications.
#
# Usage:
#   benchmarks/scripts/pg17_bench_server.sh start   # initdb (once) + start, print env
#   benchmarks/scripts/pg17_bench_server.sh env      # print export lines for the cluster
#   benchmarks/scripts/pg17_bench_server.sh status
#   benchmarks/scripts/pg17_bench_server.sh stop
#   benchmarks/scripts/pg17_bench_server.sh clean    # stop + delete the data dir
#
# The cluster is local-only (listens on 127.0.0.1) and is NOT a hosted
# endpoint. Tuning follows PostgreSQL's documented OLTP/analytics guidance so
# the comparison is fair rather than a stock out-of-the-box install:
#   shared_buffers, effective_cache_size, work_mem, maintenance_work_mem,
#   max_wal_size, and a warmed cache via pg_prewarm where available.
#
# Environment overrides:
#   PG17_PREFIX   PostgreSQL 17 install prefix
#                 (default: /opt/homebrew/opt/postgresql@17, else pg_config)
#   PG17_DATA     data directory
#                 (default: $ULTRASQL_BENCH_SCRATCH/pg17-bench-data, outside the repo)
#   PG17_PORT     TCP port (default: 55417)
#   PG17_DB       benchmark database (default: ultrasql_bench)
#   PG17_SHARED_BUFFERS / PG17_WORK_MEM / PG17_EFFECTIVE_CACHE_SIZE ...

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

PG17_PREFIX="${PG17_PREFIX:-/opt/homebrew/opt/postgresql@17}"
if [[ ! -x "$PG17_PREFIX/bin/initdb" ]]; then
    if command -v pg_config >/dev/null 2>&1; then
        PG17_PREFIX="$(pg_config --bindir | sed 's:/bin$::')"
    fi
fi
PG_BIN="$PG17_PREFIX/bin"
PG17_DATA="${PG17_DATA:-${ULTRASQL_BENCH_SCRATCH:-${TMPDIR:-/tmp}/ultrasql-bench}/pg17-bench-data}"
PG17_PORT="${PG17_PORT:-55417}"
PG17_DB="${PG17_DB:-ultrasql_bench}"
PG17_USER="${PG17_USER:-$(id -un)}"
LOGFILE="$PG17_DATA/server.log"

require_bin() {
    if [[ ! -x "$PG_BIN/$1" ]]; then
        echo "pg17_bench_server.sh: $PG_BIN/$1 not found; install postgresql@17" >&2
        exit 2
    fi
}

print_env() {
    cat <<ENV
export PGHOST=127.0.0.1
export PGPORT=$PG17_PORT
export PGUSER=$PG17_USER
export PGDATABASE=$PG17_DB
export TPCH_PSQL=$PG_BIN/psql
export POSTGRES_DSN="host=127.0.0.1 port=$PG17_PORT user=$PG17_USER dbname=$PG17_DB"
ENV
}

server_running() {
    "$PG_BIN/pg_ctl" -D "$PG17_DATA" status >/dev/null 2>&1
}

cmd_start() {
    require_bin initdb
    require_bin pg_ctl
    if [[ ! -f "$PG17_DATA/PG_VERSION" ]]; then
        echo "pg17_bench_server.sh: initdb $PG17_DATA" >&2
        mkdir -p "$(dirname "$PG17_DATA")"
        LC_ALL=C LANG=C "$PG_BIN/initdb" -D "$PG17_DATA" -U "$PG17_USER" \
            --encoding=UTF8 --locale=C --no-sync >/dev/null
        # Fair, documented OLTP/analytics tuning. Apply once at init time.
        {
            echo "listen_addresses = '127.0.0.1'"
            echo "port = $PG17_PORT"
            echo "shared_buffers = '${PG17_SHARED_BUFFERS:-2GB}'"
            echo "effective_cache_size = '${PG17_EFFECTIVE_CACHE_SIZE:-6GB}'"
            echo "work_mem = '${PG17_WORK_MEM:-64MB}'"
            echo "maintenance_work_mem = '${PG17_MAINTENANCE_WORK_MEM:-512MB}'"
            echo "max_wal_size = '${PG17_MAX_WAL_SIZE:-4GB}'"
            echo "min_wal_size = '${PG17_MIN_WAL_SIZE:-1GB}'"
            echo "checkpoint_timeout = '${PG17_CHECKPOINT_TIMEOUT:-15min}'"
            echo "checkpoint_completion_target = 0.9"
            echo "random_page_cost = ${PG17_RANDOM_PAGE_COST:-1.1}"
            # macOS lacks posix_fadvise; effective_io_concurrency must be 0 there.
            if [[ "$(uname -s)" == "Darwin" ]]; then
                echo "effective_io_concurrency = 0"
            else
                echo "effective_io_concurrency = ${PG17_EFFECTIVE_IO_CONCURRENCY:-200}"
            fi
            echo "wal_compression = ${PG17_WAL_COMPRESSION:-lz4}"
            echo "synchronous_commit = on"
            echo "fsync = on"
            echo "full_page_writes = on"
            echo "max_connections = 100"
        } >> "$PG17_DATA/postgresql.conf"
    fi
    if server_running; then
        echo "pg17_bench_server.sh: already running on port $PG17_PORT" >&2
    else
        # macOS PG17 aborts with "postmaster became multithreaded" unless the
        # postmaster environment pins a concrete locale.
        LC_ALL=C LANG=C "$PG_BIN/pg_ctl" -D "$PG17_DATA" -l "$LOGFILE" -w start >/dev/null
        echo "pg17_bench_server.sh: started PostgreSQL 17 on 127.0.0.1:$PG17_PORT" >&2
    fi
    "$PG_BIN/createdb" -h 127.0.0.1 -p "$PG17_PORT" -U "$PG17_USER" "$PG17_DB" >/dev/null 2>&1 || true
    local version
    version="$("$PG_BIN/psql" -h 127.0.0.1 -p "$PG17_PORT" -U "$PG17_USER" -d "$PG17_DB" \
        -tAc 'SHOW server_version;' 2>/dev/null | tr -d '[:space:]')"
    echo "pg17_bench_server.sh: server_version=$version" >&2
    print_env
}

cmd_stop() {
    require_bin pg_ctl
    if server_running; then
        "$PG_BIN/pg_ctl" -D "$PG17_DATA" -m fast stop >/dev/null
        echo "pg17_bench_server.sh: stopped" >&2
    else
        echo "pg17_bench_server.sh: not running" >&2
    fi
}

cmd_status() {
    if server_running; then
        "$PG_BIN/pg_ctl" -D "$PG17_DATA" status || true
    else
        echo "pg17_bench_server.sh: not running" >&2
        return 1
    fi
}

cmd_clean() {
    cmd_stop || true
    rm -rf "$PG17_DATA"
    echo "pg17_bench_server.sh: removed $PG17_DATA" >&2
}

case "${1:-start}" in
    start)  cmd_start ;;
    env)    print_env ;;
    status) cmd_status ;;
    stop)   cmd_stop ;;
    clean)  cmd_clean ;;
    *) echo "usage: $0 {start|env|status|stop|clean}" >&2; exit 2 ;;
esac
