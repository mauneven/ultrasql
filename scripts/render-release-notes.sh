#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 6 ]; then
    echo "usage: $0 <release-tag> <operator-soak-status.json> <external-audit-status.json> <incident-drill-status.json> <driver-compatibility-status.json> <output.md>" >&2
    exit 2
fi

RELEASE_TAG="$1"
operator_status_json="$2"
audit_status_json="$3"
drill_status_json="$4"
driver_status_json="$5"
out="$6"
template="docs/release-notes-template.md"
RELEASE_RUN_URL="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-mauneven/ultrasql}/actions/runs/${GITHUB_RUN_ID:-local}"
# Driver compatibility input is produced by scripts/validate-driver-compatibility.py
# as driver_compatibility_status.json.

summarize_status() {
    python3 - "$1" <<'PY'
import json
import sys

doc = json.load(open(sys.argv[1], encoding="utf-8"))
status = doc.get("status")
if "required_independent_operator_reports" in doc:
    print(
        f"{status} "
        f"({doc.get('received_independent_operator_reports')}/"
        f"{doc.get('required_independent_operator_reports')} reports)"
    )
elif "independent_operator_count" in doc:
    print(
        f"{status} "
        f"({doc.get('valid_report_count')}/{doc.get('min_reports')} reports, "
        f"{doc.get('independent_operator_count')}/{doc.get('min_reports')} operators)"
    )
elif "independent_auditor_count" in doc:
    missing = sorted(
        set(doc.get("required_audit_types", []))
        - set(doc.get("covered_audit_types", []))
    )
    suffix = f", missing: {', '.join(missing)}" if missing else ""
    print(
        f"{status} "
        f"({doc.get('valid_report_count')}/{doc.get('min_reports')} reports, "
        f"{doc.get('independent_auditor_count')}/{doc.get('min_reports')} auditors"
        f"{suffix})"
    )
elif "required_driver_count" in doc:
    missing = doc.get("missing_required_drivers", [])
    suffix = f", missing: {', '.join(missing)}" if missing else ""
    print(
        f"{status} "
        f"({doc.get('passing_required_driver_count')}/"
        f"{doc.get('required_driver_count')} required drivers"
        f"{suffix})"
    )
else:
    missing = sorted(
        set(doc.get("required_drill_types", []))
        - set(doc.get("covered_drill_types", []))
    )
    suffix = f", missing: {', '.join(missing)}" if missing else ""
    print(
        f"{status} "
        f"({doc.get('valid_report_count')}/{len(doc.get('required_drill_types', []))} drills"
        f"{suffix})"
    )
PY
}

OPERATOR_SOAK_STATUS="$(summarize_status "$operator_status_json")"
EXTERNAL_AUDIT_STATUS="$(summarize_status "$audit_status_json")"
INCIDENT_DRILL_STATUS="$(summarize_status "$drill_status_json")"
DRIVER_COMPATIBILITY_STATUS="$(summarize_status "$driver_status_json")"

mkdir -p "$(dirname "$out")"
sed \
    -e "s|@RELEASE_TAG@|${RELEASE_TAG}|g" \
    -e "s|@RELEASE_RUN_URL@|${RELEASE_RUN_URL}|g" \
    -e "s|@OPERATOR_SOAK_STATUS@|${OPERATOR_SOAK_STATUS}|g" \
    -e "s|@EXTERNAL_AUDIT_STATUS@|${EXTERNAL_AUDIT_STATUS}|g" \
    -e "s|@INCIDENT_DRILL_STATUS@|${INCIDENT_DRILL_STATUS}|g" \
    -e "s|@DRIVER_COMPATIBILITY_STATUS@|${DRIVER_COMPATIBILITY_STATUS}|g" \
    "$template" > "$out"
