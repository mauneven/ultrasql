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

if [[ -n "${SLT_DIFF_PATHS:-}" ]]; then
  # Space-separated by design: keep paths simple and reviewable.
  read -r -a SUITES <<< "$SLT_DIFF_PATHS"
else
  SUITES=()
  while IFS= read -r suite; do
    SUITES+=("$suite")
  done < <(find tests/slt/portable -maxdepth 1 -type f \( -name '*.slt' -o -name '*.test' \) | sort)
fi

if [[ "${#SUITES[@]}" -eq 0 ]]; then
  echo "run_differential.sh: no portable SQLLogicTest files selected" >&2
  exit 2
fi

for suite in "${SUITES[@]}"; do
  case "$suite" in
    tests/slt/portable/*.slt|tests/slt/portable/*.test) ;;
    *)
      echo "run_differential.sh: non-portable SQLLogicTest path: $suite" >&2
      echo "run_differential.sh: differential paths must stay under tests/slt/portable/*.slt or *.test" >&2
      exit 2
      ;;
  esac
done

IFS=',' read -r -a ENGINES <<< "${SLT_DIFF_ENGINES:-postgres,duckdb,sqlite}"

for raw_engine in "${ENGINES[@]}"; do
  engine="${raw_engine//[[:space:]]/}"
  case "$engine" in
    postgres)
      reference_url="${ULTRASQL_SLT_REFERENCE_URL:-${POSTGRES_URL:-}}"
      if [[ -z "$reference_url" ]]; then
        echo "skip postgres: set ULTRASQL_SLT_REFERENCE_URL or POSTGRES_URL" >&2
        continue
      fi
      echo "run postgres differential: ${SUITES[*]}" >&2
      "${RUNNER[@]}" --mode in-process --reference-engine postgres --reference-url "$reference_url" "${SUITES[@]}"
      ;;
    duckdb)
      if ! command -v duckdb >/dev/null 2>&1; then
        echo "skip duckdb: duckdb not found" >&2
        continue
      fi
      echo "run duckdb differential: ${SUITES[*]}" >&2
      env -u ULTRASQL_SLT_REFERENCE_URL "${RUNNER[@]}" --mode in-process --reference-engine duckdb "${SUITES[@]}"
      ;;
    sqlite)
      if ! command -v sqlite3 >/dev/null 2>&1; then
        echo "skip sqlite: sqlite3 not found" >&2
        continue
      fi
      echo "run sqlite differential: ${SUITES[*]}" >&2
      env -u ULTRASQL_SLT_REFERENCE_URL "${RUNNER[@]}" --mode in-process --reference-engine sqlite "${SUITES[@]}"
      ;;
    "")
      ;;
    *)
      echo "run_differential.sh: unknown reference engine: $engine" >&2
      exit 2
      ;;
  esac
done
