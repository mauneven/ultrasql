#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="${SLT_BENCH_OUT:-benchmarks/results/latest/slt_speed_comparison.json}"
RUNS="${SLT_BENCH_RUNS:-25}"
PATHS="${SLT_BENCH_PATHS:-tests/slt/portable}"
ENGINES="${SLT_BENCH_ENGINES:-sqlite duckdb}"
PROFILE="${SLT_BENCH_PROFILE:-release}"
CASE_LIMIT="${SLT_BENCH_CASE_LIMIT:-50}"

args=(run)
case "$PROFILE" in
  dev)
    ;;
  release)
    args+=(--release)
    ;;
  *)
    args+=(--profile "$PROFILE")
    ;;
esac
args+=(
  -p
  ultrasql-sqllogictest-runner
  --
  --mode
  in-process
  --benchmark-runs
  "$RUNS"
  --benchmark-output
  "$OUT"
)

if [[ "$CASE_LIMIT" != "all" && -n "$CASE_LIMIT" ]]; then
  args+=(--case-limit "$CASE_LIMIT")
fi

for engine in $ENGINES; do
  case "$engine" in
    sqlite)
      if command -v sqlite3 >/dev/null 2>&1; then
        args+=(--reference-engine sqlite)
      else
        echo "skip sqlite reference: sqlite3 not found" >&2
      fi
      ;;
    duckdb)
      if command -v duckdb >/dev/null 2>&1; then
        args+=(--reference-engine duckdb)
      else
        echo "skip duckdb reference: duckdb not found" >&2
      fi
      ;;
    postgres)
      if [[ -z "${POSTGRES_URL:-}" ]]; then
        echo "skip postgres reference: POSTGRES_URL unset" >&2
      else
        args+=(--reference-engine postgres --reference-url "$POSTGRES_URL")
      fi
      ;;
    ultrasql)
      ;;
    *)
      echo "unknown SLT_BENCH_ENGINES entry: $engine" >&2
      exit 2
      ;;
  esac
done

for path in $PATHS; do
  args+=("$path")
done

cargo "${args[@]}"
