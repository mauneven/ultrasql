#!/usr/bin/env bash
# filtered_ann_recall.sh — measure filtered-ANN recall@k vs an exact brute-force
# baseline across filter selectivities, end-to-end over the wire against a
# WAL-backed ultrasqld. Writes a committed artifact under
# benchmarks/results/latest/raw/.
#
# Usage:
#   ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/filtered_ann_recall.sh
#
# Environment:
#   ULTRASQLD_BIN   path to an ultrasqld binary (default target/release-ship/ultrasqld)
#   FANN_ROWS       row count (default 20000)
#   FANN_DIMS       embedding dimensions (default 16)
#   FANN_QUERIES    probe queries per selectivity (default 50)
#   FANN_OUT        artifact path

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

ULTRASQLD_BIN="${ULTRASQLD_BIN:-target/release-ship/ultrasqld}"
FANN_ROWS="${FANN_ROWS:-20000}"
FANN_DIMS="${FANN_DIMS:-16}"
FANN_QUERIES="${FANN_QUERIES:-50}"
FANN_OUT="${FANN_OUT:-benchmarks/results/latest/raw/filtered_ann_recall-ultrasql.json}"

if ! python3 -c "import numpy, psycopg" >/dev/null 2>&1; then
    echo "filtered_ann_recall.sh: numpy and psycopg are required" >&2
    exit 2
fi
if [[ ! -x "$ULTRASQLD_BIN" ]]; then
    echo "filtered_ann_recall.sh: ULTRASQLD_BIN not executable: $ULTRASQLD_BIN" >&2
    exit 2
fi

PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
DATA_DIR="$(mktemp -d)"
LOG="$(mktemp)"
"$ULTRASQLD_BIN" --listen "127.0.0.1:${PORT}" --log-level warn --data-dir "$DATA_DIR" >"$LOG" 2>&1 &
SERVER_PID=$!
cleanup() {
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
    rm -rf "$DATA_DIR" "$LOG"
}
trap cleanup EXIT INT TERM

python3 - "$PORT" <<'PY'
import socket, sys, time
port = int(sys.argv[1])
deadline = time.time() + 15
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            sys.exit(0)
    except OSError:
        time.sleep(0.05)
sys.exit("ultrasqld did not become ready")
PY

# The `ultrasql_` role-name prefix is reserved for system roles; connect as
# the bootstrap `ultrasql` superuser instead of a phantom `ultrasql_bench`.
ULTRASQL_DSN="host=127.0.0.1 port=${PORT} user=ultrasql dbname=ultrasql_bench sslmode=disable gssencmode=disable" \
    python3 benchmarks/scripts/filtered_ann_recall.py \
    --rows "$FANN_ROWS" --dims "$FANN_DIMS" --queries "$FANN_QUERIES" \
    --out "$FANN_OUT"

echo "filtered_ann_recall.sh: wrote $FANN_OUT"
