#!/usr/bin/env bash
# vector_ann_sift.sh — honest ANN recall@k-vs-latency on a real SIFT/TEXMEX
# dataset, comparing UltraSQL against pgvector (PG17), Qdrant, and LanceDB on
# the same host. Writes per-engine artifacts and a matched-point comparison
# under benchmarks/results/latest/raw/.
#
# Usage:
#   benchmarks/vector_ann_sift.sh                 # full SIFT1M
#   SIFT_DATASET=siftsmall benchmarks/vector_ann_sift.sh   # 10k smoke
#   SIFT_N_BASE=100000 benchmarks/vector_ann_sift.sh       # subset of SIFT1M
#
# Environment:
#   ULTRASQLD_BIN   ultrasqld binary (default target/release/ultrasqld)
#   SIFT_DATASET    "sift" (1M) or "siftsmall" (10k) (default sift)
#   SIFT_N_BASE     limit base vectors (default: full dataset)
#   SIFT_N_QUERIES  query count (default 100)
#   SIFT_K          recall@k (default 10)
#   SIFT_ENGINES    comma list (default ultrasql,pgvector,qdrant,lancedb)
#   QDRANT_URL      default http://localhost:6333
#   PG17 cluster is started via benchmarks/scripts/pg17_bench_server.sh

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

ULTRASQLD_BIN="${ULTRASQLD_BIN:-target/release/ultrasqld}"
SIFT_DATASET="${SIFT_DATASET:-sift}"
SIFT_N_QUERIES="${SIFT_N_QUERIES:-100}"
SIFT_K="${SIFT_K:-10}"
SIFT_ENGINES="${SIFT_ENGINES:-ultrasql,pgvector,qdrant,lancedb}"
QDRANT_URL="${QDRANT_URL:-http://localhost:6333}"
DATA_ROOT="benchmarks/datasets/${SIFT_DATASET}"
OUT_DIR="${VECTOR_ANN_OUT_DIR:-benchmarks/results/latest/raw}"

if ! python3 -c "import numpy, psycopg" >/dev/null 2>&1; then
    echo "vector_ann_sift.sh: numpy and psycopg are required" >&2
    exit 2
fi
if [[ ! -x "$ULTRASQLD_BIN" ]]; then
    echo "vector_ann_sift.sh: ULTRASQLD_BIN not executable: $ULTRASQLD_BIN" >&2
    exit 2
fi
BASE="$DATA_ROOT/${SIFT_DATASET}_base.fvecs"
QUERY="$DATA_ROOT/${SIFT_DATASET}_query.fvecs"
GT="$DATA_ROOT/${SIFT_DATASET}_groundtruth.ivecs"
for f in "$BASE" "$QUERY" "$GT"; do
    if [[ ! -f "$f" ]]; then
        echo "vector_ann_sift.sh: missing dataset file $f" >&2
        echo "  download: curl -L -o benchmarks/datasets/${SIFT_DATASET}.tar.gz \\" >&2
        echo "    ftp://ftp.irisa.fr/local/texmex/corpus/${SIFT_DATASET}.tar.gz && \\" >&2
        echo "    tar xzf benchmarks/datasets/${SIFT_DATASET}.tar.gz -C benchmarks/datasets/" >&2
        exit 2
    fi
done

# --- UltraSQL: start a fresh WAL-backed server on a free port ---
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
ULTRASQL_DSN="host=127.0.0.1 port=${PORT} user=ultrasql_bench dbname=ultrasql_bench sslmode=disable gssencmode=disable"

# --- pgvector: ensure the same-host PG17 cluster is up ---
PG_DSN=""
if [[ "$SIFT_ENGINES" == *pgvector* ]]; then
    if PG_ENV="$(benchmarks/scripts/pg17_bench_server.sh start 2>/dev/null)"; then
        eval "$PG_ENV"
        PG_DSN="${POSTGRES_DSN}"
    else
        echo "vector_ann_sift.sh: PG17 cluster unavailable; pgvector will record not_available" >&2
    fi
fi

echo "=== ANN benchmark: dataset=$SIFT_DATASET engines=$SIFT_ENGINES k=$SIFT_K ==="
ULTRASQL_DSN="$ULTRASQL_DSN" \
PG_DSN="$PG_DSN" \
QDRANT_URL="$QDRANT_URL" \
LANCEDB_DIR="${LANCEDB_DIR:-$(mktemp -d)}" \
    python3 benchmarks/scripts/vector_ann_bench.py \
    --base "$BASE" --query "$QUERY" --groundtruth "$GT" \
    ${SIFT_N_BASE:+--n-base "$SIFT_N_BASE"} \
    --n-queries "$SIFT_N_QUERIES" \
    --k "$SIFT_K" \
    --engines "$SIFT_ENGINES" \
    --dataset-name "$SIFT_DATASET" \
    --out-dir "$OUT_DIR"

echo "vector_ann_sift.sh: artifacts under $OUT_DIR"
