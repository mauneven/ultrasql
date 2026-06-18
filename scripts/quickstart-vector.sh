#!/usr/bin/env bash
# 60-second vector quickstart.
#
# From an already-built ultrasqld, this boots a local server, creates a table
# that holds text + embedding + JSON metadata, ingests a few rows in one
# transaction, and runs ONE hybrid query that ranks them by fused vector + BM25
# relevance under a tenant metadata filter — then prints how long the whole
# thing took. No external services, no second datastore.
#
#   cargo build --release --bin ultrasqld   # one-time (the slow part)
#   scripts/quickstart-vector.sh
#
# Requires: a built ultrasqld and a `psql` client (libpq). Override the binary
# with ULTRASQLD_BIN= and the client with PSQL=.

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

ULTRASQLD_BIN="${ULTRASQLD_BIN:-target/release/ultrasqld}"
PSQL="${PSQL:-psql}"

if [[ ! -x "$ULTRASQLD_BIN" ]]; then
    echo "ultrasqld is not built. Run first:" >&2
    echo "    cargo build --release --bin ultrasqld" >&2
    exit 2
fi
if ! command -v "$PSQL" >/dev/null 2>&1; then
    echo "psql not found. Install a PostgreSQL client (libpq), or try the" >&2
    echo "zero-dependency Node demo in examples/node-rag/ instead." >&2
    exit 2
fi

SCRATCH="${ULTRASQL_BENCH_SCRATCH:-${TMPDIR:-/tmp}/ultrasql-bench}"
mkdir -p "$SCRATCH"
DATA_DIR="$(mktemp -d "$SCRATCH/quickstart-vector-XXXXXX")"
LOG="$(mktemp)"
SQL_FILE="$(mktemp)"
PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"

cleanup() {
    kill "${SRV:-}" >/dev/null 2>&1 || true
    wait "${SRV:-}" >/dev/null 2>&1 || true
    rm -rf "$DATA_DIR" "$LOG" "$SQL_FILE"
}
trap cleanup EXIT INT TERM

cat >"$SQL_FILE" <<'SQL'
-- Text, embedding, and metadata are columns of ONE ACID table.
CREATE TABLE memories (
    id        INT NOT NULL,
    body      TEXT,
    embedding VECTOR(3),
    metadata  JSONB
);
CREATE INDEX memories_hnsw ON memories USING hnsw (embedding vector_l2_ops);

-- Ingest in one transaction.
BEGIN;
INSERT INTO memories VALUES
  (1, 'payment retry succeeded',             '[0.5,0.3,0]', '{"tenant":"acme","kind":"billing"}'),
  (2, 'invoice payment failed for customer', '[0.9,0.1,0]', '{"tenant":"acme","kind":"billing"}'),
  (3, 'user updated profile photo',          '[0,0,1]',     '{"tenant":"acme","kind":"profile"}');
COMMIT;

-- One query: tenant filter + RRF fusion of vector similarity and BM25 text.
SELECT id, body
FROM memories
WHERE metadata @> '{"tenant":"acme"}'
ORDER BY hybrid_search(body, 'failed invoice payment', embedding, VECTOR '[1,0,0]', 'rrf') DESC
LIMIT 3;
SQL

echo "Booting ultrasqld and running the first hybrid vector query…"
START="$(python3 -c 'import time;print(time.time())')"

"$ULTRASQLD_BIN" --listen "127.0.0.1:${PORT}" --log-level warn --data-dir "$DATA_DIR" >"$LOG" 2>&1 &
SRV=$!
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

DSN="host=127.0.0.1 port=${PORT} user=ultrasql dbname=ultrasql sslmode=disable gssencmode=disable"
echo
"$PSQL" "$DSN" -v ON_ERROR_STOP=1 -q -f "$SQL_FILE"
echo

END="$(python3 -c 'import time;print(time.time())')"
python3 -c "print(f'Zero to first hybrid vector result in {$END - $START:.1f}s (server boot + ingest + query).')"
echo "Same table, same transaction — vectors, text, and JSON ranked together."
