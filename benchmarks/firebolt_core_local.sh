#!/usr/bin/env bash
# Local Firebolt Core Docker lifecycle helper for benchmark scripts.
#
# Firebolt Core is supplied as an external Docker image. UltraSQL does not
# vendor, copy, or redistribute Firebolt Core.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

COMMAND="${1:-status}"
FIREBOLT_CORE_ENDPOINT="${FIREBOLT_CORE_ENDPOINT:-http://127.0.0.1:3473}"
FIREBOLT_CORE_CONTAINER="${FIREBOLT_CORE_CONTAINER:-ultrasql-firebolt-core}"
FIREBOLT_CORE_REPO="${FIREBOLT_CORE_REPO:-ghcr.io/firebolt-db/firebolt-core}"
FIREBOLT_CORE_TAG="${FIREBOLT_CORE_TAG:-preview-rc}"
FIREBOLT_CORE_IMAGE="${FIREBOLT_CORE_IMAGE:-${FIREBOLT_CORE_REPO}:${FIREBOLT_CORE_TAG}}"
FIREBOLT_CORE_DATA_DIR="${FIREBOLT_CORE_DATA_DIR:-$REPO_ROOT/target/firebolt-core-data}"
FIREBOLT_CORE_WAIT_SECS="${FIREBOLT_CORE_WAIT_SECS:-30}"

endpoint_with_format() {
    case "$FIREBOLT_CORE_ENDPOINT" in
        *\?*) printf '%s&output_format=TabSeparatedWithNamesAndTypes' "$FIREBOLT_CORE_ENDPOINT" ;;
        *) printf '%s?output_format=TabSeparatedWithNamesAndTypes' "$FIREBOLT_CORE_ENDPOINT" ;;
    esac
}

endpoint_port() {
    python3 - "$FIREBOLT_CORE_ENDPOINT" <<'PY'
import sys
from urllib.parse import urlparse

parsed = urlparse(sys.argv[1])
if parsed.port is not None:
    print(parsed.port)
elif parsed.scheme == "https":
    print(443)
else:
    print(80)
PY
}

docker_available() {
    command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

container_running() {
    docker ps --filter "name=^/${FIREBOLT_CORE_CONTAINER}$" --format '{{.ID}}' | grep -q .
}

container_exists() {
    docker ps -a --filter "name=^/${FIREBOLT_CORE_CONTAINER}$" --format '{{.ID}}' | grep -q .
}

core_ready() {
    command -v curl >/dev/null 2>&1 || return 1
    local response
    response="$(curl -fsS "$(endpoint_with_format)" --data-binary "SELECT 42;" 2>/dev/null || true)"
    [[ "$response" == $'?column?\nint\n42' ]]
}

wait_ready() {
    local deadline="$((SECONDS + FIREBOLT_CORE_WAIT_SECS))"
    until core_ready; do
        if (( SECONDS >= deadline )); then
            echo "firebolt_core_local.sh: Firebolt Core not ready at $FIREBOLT_CORE_ENDPOINT" >&2
            return 2
        fi
        sleep 1
    done
}

start_core() {
    if core_ready; then
        echo "$FIREBOLT_CORE_ENDPOINT"
        return 0
    fi
    if ! docker_available; then
        echo "firebolt_core_local.sh: Docker unavailable; cannot start Firebolt Core" >&2
        return 2
    fi
    if container_running; then
        wait_ready
        echo "$FIREBOLT_CORE_ENDPOINT"
        return 0
    fi
    if container_exists; then
        docker rm -f "$FIREBOLT_CORE_CONTAINER" >/dev/null 2>&1 || true
    fi

    mkdir -p "$FIREBOLT_CORE_DATA_DIR"
    chmod 777 "$FIREBOLT_CORE_DATA_DIR" >/dev/null 2>&1 || true
    if ! docker pull --quiet "$FIREBOLT_CORE_IMAGE" >/dev/null; then
        echo "firebolt_core_local.sh: failed to pull $FIREBOLT_CORE_IMAGE" >&2
        return 2
    fi

    local core_user
    if [[ "$(uname)" == "Darwin" ]]; then
        core_user="${FIREBOLT_CORE_USER:-root}"
    else
        core_user="${FIREBOLT_CORE_USER:-firebolt-core}"
    fi

    local port
    port="${FIREBOLT_CORE_PORT:-$(endpoint_port)}"
    if ! docker run \
        --detach \
        --name "$FIREBOLT_CORE_CONTAINER" \
        --rm \
        --user "$core_user" \
        --ulimit memlock=8589934592:8589934592 \
        --security-opt seccomp=unconfined \
        -v "$FIREBOLT_CORE_DATA_DIR:/firebolt-core/volume" \
        -p "127.0.0.1:${port}:3473" \
        "$FIREBOLT_CORE_IMAGE" >/dev/null; then
        echo "firebolt_core_local.sh: failed to start $FIREBOLT_CORE_IMAGE" >&2
        return 2
    fi

    wait_ready
    echo "$FIREBOLT_CORE_ENDPOINT"
}

stop_core() {
    if command -v docker >/dev/null 2>&1; then
        docker rm -f "$FIREBOLT_CORE_CONTAINER" >/dev/null 2>&1 || true
    fi
}

status_core() {
    if core_ready; then
        echo "ready $FIREBOLT_CORE_ENDPOINT"
        return 0
    fi
    if docker_available && container_running; then
        echo "starting $FIREBOLT_CORE_CONTAINER"
        return 1
    fi
    if docker_available; then
        echo "stopped $FIREBOLT_CORE_CONTAINER"
        return 1
    fi
    echo "docker_unavailable"
    return 2
}

query_core() {
    local sql
    if [[ $# -gt 0 ]]; then
        sql="$*"
    else
        sql="$(cat)"
    fi
    start_core >/dev/null
    curl -fsS "$(endpoint_with_format)" --data-binary "$sql"
}

clean_core() {
    stop_core
    if [[ -n "$FIREBOLT_CORE_DATA_DIR" && "$FIREBOLT_CORE_DATA_DIR" == "$REPO_ROOT"/target/* ]]; then
        rm -rf "$FIREBOLT_CORE_DATA_DIR"
    else
        echo "firebolt_core_local.sh: refusing to remove custom data dir $FIREBOLT_CORE_DATA_DIR" >&2
    fi
}

case "$COMMAND" in
    start)
        start_core
        ;;
    stop)
        stop_core
        ;;
    status)
        status_core
        ;;
    wait)
        start_core >/dev/null
        ;;
    query)
        shift || true
        query_core "$@"
        ;;
    clean)
        clean_core
        ;;
    *)
        echo "usage: benchmarks/firebolt_core_local.sh {start|stop|status|wait|query|clean}" >&2
        exit 2
        ;;
esac
