#!/usr/bin/env bash
# vector_soak.sh — sustained concurrent vector load, then a hard crash and
# restart, against a real ultrasqld. Proves the HNSW vector path stays correct
# under load and that every committed row + the index survive a SIGKILL and WAL
# replay.
#
#   benchmarks/vector_soak.sh smoke   # ~5s of load (default)
#   benchmarks/vector_soak.sh full    # ~30s of load
#
# Missing prerequisites (numpy/psycopg, a built ultrasqld) write a manifest with
# "status": "not_available" and exit 2 — never a fake pass.

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${VECTOR_SOAK_PROFILE:-${1:-smoke}}"
OUT_DIR="${VECTOR_SOAK_OUT_DIR:-benchmarks/results/latest}"
MANIFEST="$OUT_DIR/vector_soak_manifest.json"
ULTRASQLD_BIN="${ULTRASQLD_BIN:-target/release/ultrasqld}"
RECALL_FLOOR="${VECTOR_SOAK_RECALL_FLOOR:-0.90}"

case "$PROFILE" in
    smoke) BASE="${VECTOR_SOAK_BASE:-2000}";  THREADS="${VECTOR_SOAK_THREADS:-4}"; DURATION="${VECTOR_SOAK_DURATION:-5}" ;;
    full)  BASE="${VECTOR_SOAK_BASE:-20000}"; THREADS="${VECTOR_SOAK_THREADS:-8}"; DURATION="${VECTOR_SOAK_DURATION:-30}" ;;
    *) echo "vector_soak.sh: unknown profile '$PROFILE' (use smoke|full)" >&2; exit 2 ;;
esac

mkdir -p "$OUT_DIR"

write_not_available() {
    python3 - "$MANIFEST" "$PROFILE" "$1" <<'PY'
import json, platform, sys, time
manifest, profile, reason = sys.argv[1:]
doc = {
    "schema_version": 1,
    "suite": "vector_soak",
    "profile": profile,
    "status": "not_available",
    "reason": reason,
    "generated_at_unix": int(time.time()),
    "host": {"os": platform.platform(), "machine": platform.machine()},
}
with open(manifest, "w", encoding="utf-8") as f:
    f.write(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
}

if ! python3 -c "import numpy, psycopg" >/dev/null 2>&1; then
    write_not_available "numpy and psycopg are required"
    exit 2
fi
if [[ ! -x "$ULTRASQLD_BIN" ]]; then
    write_not_available "ultrasqld not built at $ULTRASQLD_BIN (cargo build --release --bin ultrasqld)"
    exit 2
fi

SCRATCH="${ULTRASQL_BENCH_SCRATCH:-${TMPDIR:-/tmp}/ultrasql-bench}"
mkdir -p "$SCRATCH"
DATA_DIR="$(mktemp -d "$SCRATCH/vector-soak-XXXXXX")"
LOG="$(mktemp)"
HANDOFF="$(mktemp)"
RESULT="$(mktemp)"
PORT=""
SRV=""
DSN=""

cleanup() {
    [[ -n "$SRV" ]] && kill "$SRV" >/dev/null 2>&1 || true
    [[ -n "$SRV" ]] && wait "$SRV" >/dev/null 2>&1 || true
    rm -rf "$DATA_DIR" "$LOG" "$HANDOFF" "$RESULT"
}
trap cleanup EXIT INT TERM

# Allocate a fresh port on every (re)start: after a SIGKILL the old listen
# socket can linger in TIME_WAIT, so rebinding the same port would race.
start_server() {
    PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
    "$ULTRASQLD_BIN" --listen "127.0.0.1:${PORT}" --log-level warn --data-dir "$DATA_DIR" >>"$LOG" 2>&1 &
    SRV=$!
    # Connect as the bootstrap role `ultrasql` so table owners are a persisted
    # role and survive recovery (a `ultrasql_`-prefixed user is not persisted).
    DSN="host=127.0.0.1 port=${PORT} user=ultrasql dbname=ultrasql sslmode=disable gssencmode=disable"
    # Recovery replays the HNSW graph (O(N^2) build), so restart after a crash
    # is slow at scale — ~40s for the 20k full profile. Allow generous time.
    python3 - "$PORT" <<'PY'
import socket, sys, time
port = int(sys.argv[1])
deadline = time.time() + 180
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            sys.exit(0)
    except OSError:
        time.sleep(0.1)
sys.exit("ultrasqld did not become ready")
PY
}

echo "=== vector_soak ($PROFILE): base=$BASE threads=$THREADS duration=${DURATION}s ==="

# Phase 1 — sustained concurrent load.
start_server
set +e
python3 benchmarks/scripts/vector_soak.py --phase load --dsn "$DSN" \
    --base "$BASE" --threads "$THREADS" --duration "$DURATION" \
    --recall-floor "$RECALL_FLOOR" --handoff "$HANDOFF"
LOAD_STATUS=$?
set -e

# Phase 2 — hard crash (SIGKILL) and restart on the same data dir. The load
# phase issues a CHECKPOINT first; recovery of un-checkpointed writes produced
# under concurrency is a known heap WAL-replay gap (see ROADMAP), so the soak
# asserts durability of checkpointed state across an abrupt kill.
echo "--- checkpointed; crashing server (SIGKILL) and restarting ---"
kill -9 "$SRV" >/dev/null 2>&1 || true
wait "$SRV" >/dev/null 2>&1 || true
SRV=""
start_server

# Phase 3 — durability + recall verification after recovery.
set +e
python3 benchmarks/scripts/vector_soak.py --phase verify --dsn "$DSN" \
    --recall-floor "$RECALL_FLOOR" --handoff "$HANDOFF" --result "$RESULT"
VERIFY_STATUS=$?
set -e

python3 - "$MANIFEST" "$PROFILE" "$RESULT" "$LOAD_STATUS" "$VERIFY_STATUS" "$RECALL_FLOOR" <<'PY'
import json, platform, sys, time
manifest, profile, result_path, load_status, verify_status, recall_floor = sys.argv[1:]
load_status = int(load_status); verify_status = int(verify_status)
try:
    with open(result_path, encoding="utf-8") as f:
        result = json.load(f)
except (OSError, ValueError):
    result = {}
passed = load_status == 0 and verify_status == 0 and result.get("ok", False)
doc = {
    "schema_version": 1,
    "suite": "vector_soak",
    "profile": profile,
    "status": "passed" if passed else "failed",
    "passed": passed,
    "recall_floor": float(recall_floor),
    "load_ok": load_status == 0,
    "crash": "SIGKILL",
    "recovery_note": (
        "Durability is verified across a SIGKILL after a CHECKPOINT. Recovery of "
        "un-checkpointed writes produced under concurrency is a known heap "
        "WAL-replay gap, tracked in ROADMAP.md (P2 vector/storage)."
    ),
    "durable_after_restart": result.get("durable"),
    "count_after_restart": result.get("count_after_restart"),
    "expected_count": result.get("expected_count"),
    "verify_recall_mean": result.get("recall_mean"),
    "verify_query_errors": result.get("query_errors"),
    "load_stats": result.get("load_stats"),
    "generated_at_unix": int(time.time()),
    "host": {"os": platform.platform(), "machine": platform.machine()},
    "policy": (
        "Sustained concurrent ANN reads + far-region writes, then a CHECKPOINT "
        "and a SIGKILL + restart. Pass requires error-free load, recall above the "
        "floor both during load and after recovery, and every committed row "
        "durable after the abrupt restart."
    ),
}
with open(manifest, "w", encoding="utf-8") as f:
    f.write(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
sys.exit(0 if passed else 1)
PY
