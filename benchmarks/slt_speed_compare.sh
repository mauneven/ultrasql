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

python3 - "$OUT" <<'PY'
import json
import os
import pathlib
import platform
import subprocess
import sys

path = pathlib.Path(sys.argv[1])
doc = json.loads(path.read_text(encoding="utf-8"))

def host_memory_bytes():
    try:
        if sys.platform == "darwin":
            return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip())
        meminfo = pathlib.Path("/proc/meminfo")
        if meminfo.exists():
            for line in meminfo.read_text(encoding="utf-8").splitlines():
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except (OSError, subprocess.CalledProcessError, ValueError):
        return 0
    return 0

host_memory = host_memory_bytes()
host_cpu = os.environ.get("BENCH_CPU_MODEL") or platform.processor() or platform.machine()
doc["schema_version"] = 1
doc.pop("winner", None)
doc["status"] = "measured" if all(engine.get("ok") for engine in doc.get("engines", [])) else "failed"
doc["host"] = {
    "cpu": host_cpu,
    "cores": os.cpu_count() or 0,
    "ram_gb": round(host_memory / (1024 ** 3)) if host_memory else 0,
    "os": platform.platform(),
    "memory_bytes": host_memory,
}
doc["policy"] = (
    "SQLLogicTest speed artifact records per-engine timings only; no winner "
    "or ranking claim is emitted by the arena."
)
path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
PY
