#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: $0 <release-tag> <operator-soak-status.json> <output.md>" >&2
    exit 2
fi

RELEASE_TAG="$1"
status_json="$2"
out="$3"
template="docs/release-notes-template.md"
RELEASE_RUN_URL="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-mauneven/ultrasql}/actions/runs/${GITHUB_RUN_ID:-local}"
OPERATOR_SOAK_STATUS="$(
    python3 - "$status_json" <<'PY'
import json
import sys

doc = json.load(open(sys.argv[1], encoding="utf-8"))
print(
    f"{doc.get('status')} "
    f"({doc.get('valid_report_count')}/{doc.get('min_reports')} reports, "
    f"{doc.get('independent_operator_count')}/{doc.get('min_reports')} operators)"
)
PY
)"

mkdir -p "$(dirname "$out")"
sed \
    -e "s|@RELEASE_TAG@|${RELEASE_TAG}|g" \
    -e "s|@RELEASE_RUN_URL@|${RELEASE_RUN_URL}|g" \
    -e "s|@OPERATOR_SOAK_STATUS@|${OPERATOR_SOAK_STATUS}|g" \
    "$template" > "$out"
