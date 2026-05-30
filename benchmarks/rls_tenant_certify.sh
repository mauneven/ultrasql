#!/usr/bin/env bash
# RLS tenant-isolation certification runner.
#
# Runs the wire-level RLS integration suite and writes a release artifact. This
# is a correctness/security certification, not a benchmark or win claim.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${RLS_TENANT_PROFILE:-${1:-smoke}}"
OUT_DIR="${RLS_TENANT_OUT_DIR:-benchmarks/results/latest}"
LOG_DIR="${RLS_TENANT_LOG_DIR:-target/rls-tenant-cert}"
MANIFEST="$OUT_DIR/rls_tenant_certification.json"
LOG_FILE="$LOG_DIR/rls_round_trip.log"
TEST_PACKAGE="${RLS_TENANT_TEST_PACKAGE:-ultrasql-server}"
TEST_TARGET="${RLS_TENANT_TEST_TARGET:-rls_round_trip}"

case "$PROFILE" in
    smoke | full) ;;
    *)
        echo "rls_tenant_certify.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

CHECKS=(
    "read filtering by current tenant setting"
    "same-tenant INSERT allowed"
    "cross-tenant INSERT rejected"
    "INSERT ... SELECT WITH CHECK enforced atomically"
    "UPDATE new-row WITH CHECK enforced"
    "restrictive policies narrow permissive policies"
    "role-scoped policies honor inherited roles"
    "owner, superuser, and BYPASSRLS semantics"
    "policy metadata and owner/bypass semantics survive restart"
)

checks_json="$(
    printf '%s\n' "${CHECKS[@]}" | python3 -c 'import json, sys; print(json.dumps([line.strip() for line in sys.stdin if line.strip()]))'
)"

write_manifest() {
    local status="$1"
    local reason="$2"
    local exit_code="$3"
    mkdir -p "$OUT_DIR"
    PROFILE="$PROFILE" \
    STATUS="$status" \
    REASON="$reason" \
    EXIT_CODE="$exit_code" \
    LOG_FILE="$LOG_FILE" \
    TEST_PACKAGE="$TEST_PACKAGE" \
    TEST_TARGET="$TEST_TARGET" \
    CHECKS_JSON="$checks_json" \
    python3 - <<'PY' > "$MANIFEST"
import json
import os
import platform
import time

checks = json.loads(os.environ["CHECKS_JSON"])
status = os.environ["STATUS"]
reason = os.environ["REASON"] or None
command = [
    "cargo",
    "test",
    "-p",
    os.environ["TEST_PACKAGE"],
    "--test",
    os.environ["TEST_TARGET"],
    "--",
    "--nocapture",
]
doc = {
    "schema_version": 1,
    "suite": "rls_tenant_certification",
    "profile": os.environ["PROFILE"],
    "status": status,
    "passed": status == "passed",
    "reason": reason,
    "exit_code": int(os.environ["EXIT_CODE"]),
    "command": command,
    "log_file": os.environ["LOG_FILE"],
    "required_checks": checks,
    "required_check_count": len(checks),
    "generated_at_unix": int(time.time()),
    "host": {
        "cpu": platform.processor() or platform.machine(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "cores": os.cpu_count() or 0,
    },
    "policy": (
        "RLS tenant certification is a correctness/security artifact. It "
        "requires the wire-level RLS suite to pass for reads, inserts, "
        "INSERT SELECT, updates, role scoping, bypass semantics, restrictive "
        "policies, and restart persistence. It is not a benchmark claim."
    ),
}
print(json.dumps(doc, indent=2, sort_keys=True))
PY
}

mkdir -p "$LOG_DIR"

set +e
cargo test -p "$TEST_PACKAGE" --test "$TEST_TARGET" -- --nocapture 2>&1 | tee "$LOG_FILE"
code=${PIPESTATUS[0]}
set -e

if [ "$code" -eq 0 ]; then
    write_manifest "passed" "" "$code"
    echo "rls_tenant_certification passed: $MANIFEST"
    exit 0
fi

write_manifest "failed" "cargo_test_failed" "$code"
echo "rls_tenant_certification failed: see $LOG_FILE" >&2
exit 1
