#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

if [[ -n "${ULTRASQL_SLT_RUNNER:-}" ]]; then
  RUNNER=("$ULTRASQL_SLT_RUNNER")
else
  RUNNER=(cargo run -p ultrasql-sqllogictest-runner --)
fi

if [[ -n "${SLT_SQL_REGRESSION_PATHS:-}" ]]; then
  # Space-separated by design: keep paths simple and reviewable.
  read -r -a SUITES <<< "$SLT_SQL_REGRESSION_PATHS"
else
  SUITES=()
  while IFS= read -r suite; do
    SUITES+=("$suite")
  done < <(find tests/slt/sql_regression -type f \( -name '*.slt' -o -name '*.test' \) | sort)
fi

if [[ "${#SUITES[@]}" -eq 0 ]]; then
  echo "run_sql_regression.sh: no engine-specific SQLLogicTest files selected" >&2
  exit 2
fi

for suite in "${SUITES[@]}"; do
  case "$suite" in
    tests/slt/sql_regression/*) ;;
    *)
      echo "run_sql_regression.sh: non-engine-specific SQLLogicTest path: $suite" >&2
      echo "run_sql_regression.sh: paths must stay under tests/slt/sql_regression/" >&2
      exit 2
      ;;
  esac
done

reference_url="${ULTRASQL_SLT_REFERENCE_URL:-${POSTGRES_URL:-}}"
if [[ -z "$reference_url" ]]; then
  echo "skip sql_regression: set ULTRASQL_SLT_REFERENCE_URL or POSTGRES_URL" >&2
  exit 0
fi

echo "run sql_regression differential: ${SUITES[*]}" >&2
"${RUNNER[@]}" --mode in-process --reference-engine postgres --reference-url "$reference_url" "${SUITES[@]}"
