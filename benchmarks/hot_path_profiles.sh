#!/usr/bin/env bash
# Generate reproducible hot-path flamegraphs.
#
# Usage:
#   benchmarks/hot_path_profiles.sh [smoke|full] [workload...]
#
# Workloads:
#   csv_copy parquet_filter vector_topk hash_aggregate joins tpch_q1 tpch_q5 tpch_q6

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${1:-smoke}"
case "$PROFILE" in
    smoke|full)
        shift || true
        ;;
    *)
        echo "hot_path_profiles.sh: profile must be smoke or full, got '$PROFILE'" >&2
        exit 2
        ;;
esac

ALL_WORKLOADS=(
    csv_copy
    parquet_filter
    vector_topk
    hash_aggregate
    joins
    tpch_q1
    tpch_q5
    tpch_q6
)

if [[ "$#" -gt 0 ]]; then
    WORKLOADS=("$@")
else
    WORKLOADS=("${ALL_WORKLOADS[@]}")
fi

for workload in "${WORKLOADS[@]}"; do
    found=0
    for known in "${ALL_WORKLOADS[@]}"; do
        if [[ "$workload" == "$known" ]]; then
            found=1
            break
        fi
    done
    if [[ "$found" != "1" ]]; then
        echo "hot_path_profiles.sh: unknown workload '$workload'" >&2
        exit 2
    fi
done

OUT_DIR="${HOT_PROFILE_OUT_DIR:-benchmarks/results/latest}"
PROFILE_DIR="$OUT_DIR/profiles/hot_path"
RAW_DIR="$OUT_DIR/raw"
DATA_DIR="${HOT_PROFILE_DATA_DIR:-${ULTRASQL_BENCH_SCRATCH:-${TMPDIR:-/tmp}/ultrasql-bench}/hot-path-profiles}"
MANIFEST="$OUT_DIR/hot_path_profiles_manifest.json"
ROWS="${HOT_PROFILE_ROWS:-$([[ "$PROFILE" == "smoke" ]] && echo 4096 || echo 100000)}"
PARQUET_ROWS="${HOT_PROFILE_PARQUET_ROWS:-$ROWS}"
VECTOR_ROWS="${HOT_PROFILE_VECTOR_ROWS:-$([[ "$PROFILE" == "smoke" ]] && echo 512 || echo 10000)}"
VECTOR_DIMS="${HOT_PROFILE_VECTOR_DIMS:-8}"
TOP_K="${HOT_PROFILE_TOP_K:-10}"
ITERS="${HOT_PROFILE_ITERS:-$([[ "$PROFILE" == "smoke" ]] && echo 1 || echo 5)}"
WARMUP="${HOT_PROFILE_WARMUP:-$([[ "$PROFILE" == "smoke" ]] && echo 0 || echo 1)}"
TPCH_DATA_DIR="${TPCH_DATA_DIR:-${ULTRASQL_BENCH_SCRATCH:-${TMPDIR:-/tmp}/ultrasql-bench}/tpch-scale10-real}"
TPCH_SCALE="${TPCH_SCALE:-10}"
TPCH_RUNS="${TPCH_PROFILE_RUNS:-$([[ "$PROFILE" == "smoke" ]] && echo 1 || echo 3)}"
TPCH_WARMUP="${TPCH_PROFILE_WARMUP:-$([[ "$PROFILE" == "smoke" ]] && echo 0 || echo 1)}"
FLAMEGRAPH_BIN="${HOT_PROFILE_FLAMEGRAPH_BIN:-$(command -v flamegraph || true)}"
ALLOW_ROOT="${HOT_PROFILE_ALLOW_ROOT:-0}"

mkdir -p "$PROFILE_DIR" "$RAW_DIR" "$DATA_DIR"

CROSS_COMPARE_BIN="target/release-with-debug/cross_compare_sql"
TPCH_BIN="target/release-with-debug/tpch"
CSV_PATH="$DATA_DIR/hot_path.csv"

write_csv() {
    python3 - "$CSV_PATH" "$ROWS" <<'PY'
import csv
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
rows = int(sys.argv[2])
path.parent.mkdir(parents=True, exist_ok=True)
with path.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "category", "metric", "fact_dim"])
    categories = ["alpha", "beta", "gamma", "delta"]
    for i in range(rows):
        writer.writerow([i, categories[i % len(categories)], (i * 17) % 100000, f"d{i % 16}"])
PY
}

write_profile_artifact() {
    local workload="$1"
    local status="$2"
    local reason="$3"
    local flamegraph_path="$4"
    local raw_path="$5"
    shift 5
    local artifact="$PROFILE_DIR/${workload}.json"
    python3 - "$artifact" "$workload" "$status" "$reason" "$flamegraph_path" \
        "$raw_path" "$PROFILE" "$FLAMEGRAPH_BIN" "$@" <<'PY'
import json
import os
import pathlib
import platform
import subprocess
import sys

(
    artifact_path,
    workload,
    status,
    reason,
    flamegraph_path,
    raw_path,
    profile,
    profiler,
    *command,
) = sys.argv[1:]

def host_cpu() -> str:
    try:
        out = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
        if out:
            return out
    except Exception:
        pass
    try:
        text = pathlib.Path("/proc/cpuinfo").read_text(errors="replace")
        for line in text.splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return platform.processor() or platform.machine() or "unknown"

def host_memory() -> int | None:
    try:
        out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
        return int(out)
    except Exception:
        pass
    try:
        text = pathlib.Path("/proc/meminfo").read_text(errors="replace")
        for line in text.splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except Exception:
        pass
    return None

doc = {
    "schema_version": 1,
    "suite": "hot_path_profiles",
    "workload": workload,
    "profile": profile,
    "status": status,
    "reason": reason or None,
    "profiler": pathlib.Path(profiler).name if profiler else None,
    "flamegraph": flamegraph_path or None,
    "raw_result": raw_path or None,
    "command": command,
    "host": {
        "cpu": host_cpu(),
        "memory_bytes": host_memory(),
        "os": platform.platform(),
    },
    "policy": "Hot-path profile artifacts are diagnostic flamegraphs only; they do not make benchmark ranking claims.",
}
pathlib.Path(artifact_path).write_text(json.dumps(doc, indent=2) + "\n")
PY
}

profile_command() {
    local workload="$1"
    local flamegraph_path="$2"
    local raw_path="$3"
    shift 3

    if [[ -z "$FLAMEGRAPH_BIN" || ! -x "$FLAMEGRAPH_BIN" ]]; then
        write_profile_artifact "$workload" "not_available" "flamegraph_missing" \
            "$flamegraph_path" "$raw_path" "$@"
        return 2
    fi

    local profiler=("$FLAMEGRAPH_BIN" -o "$flamegraph_path" --title "UltraSQL ${workload}" --deterministic)
    if [[ "$ALLOW_ROOT" == "1" ]]; then
        profiler+=(--root)
    fi

    set +e
    "${profiler[@]}" -- "$@"
    local status=$?
    set -e

    if [[ "$status" == "0" && -s "$flamegraph_path" ]]; then
        write_profile_artifact "$workload" "measured" "" "$flamegraph_path" "$raw_path" "$@"
        return 0
    fi

    write_profile_artifact "$workload" "failed" "profiler_or_workload_failed" \
        "$flamegraph_path" "$raw_path" "$@"
    return 1
}

run_cross_compare_profile() {
    local workload="$1"
    local bench_workload="$2"
    local raw_path="$RAW_DIR/hot_path_${workload}.json"
    local flamegraph_path="$PROFILE_DIR/${workload}.svg"
    shift 2
    profile_command "$workload" "$flamegraph_path" "$raw_path" \
        "$CROSS_COMPARE_BIN" \
        --workload "$bench_workload" \
        --rows "$ROWS" \
        --warmup "$WARMUP" \
        --iters "$ITERS" \
        --output "$raw_path" \
        "$@"
}

run_tpch_profile() {
    local workload="$1"
    local query="$2"
    local raw_path="$RAW_DIR/hot_path_${workload}.json"
    local flamegraph_path="$PROFILE_DIR/${workload}.svg"

    if [[ ! -d "$TPCH_DATA_DIR" ]]; then
        write_profile_artifact "$workload" "not_available" "tpch_data_dir_missing" \
            "$flamegraph_path" "$raw_path" \
            "$TPCH_BIN" run-queries ultrasql --data-dir "$TPCH_DATA_DIR" \
            --runs "$TPCH_RUNS" --warmup "$TPCH_WARMUP" --queries "$query" \
            --scale "$TPCH_SCALE" --out "$raw_path"
        return 2
    fi

    profile_command "$workload" "$flamegraph_path" "$raw_path" \
        "$TPCH_BIN" run-queries ultrasql \
        --data-dir "$TPCH_DATA_DIR" \
        --runs "$TPCH_RUNS" \
        --warmup "$TPCH_WARMUP" \
        --queries "$query" \
        --scale "$TPCH_SCALE" \
        --out "$raw_path"
}

write_manifest() {
    python3 - "$MANIFEST" "$PROFILE_DIR" "${WORKLOADS[@]}" <<'PY'
import json
import pathlib
import sys

manifest_path = pathlib.Path(sys.argv[1])
profile_dir = pathlib.Path(sys.argv[2])
required = sys.argv[3:]
profiles = []
for workload in required:
    path = profile_dir / f"{workload}.json"
    if path.exists():
        doc = json.loads(path.read_text())
    else:
        doc = {
            "schema_version": 1,
            "suite": "hot_path_profiles",
            "workload": workload,
            "status": "not_available",
            "reason": "profile_artifact_missing",
        }
    doc["artifact"] = str(path)
    profiles.append(doc)

statuses = {p.get("status") for p in profiles}
if statuses == {"measured"}:
    status = "measured"
elif "failed" in statuses:
    status = "failed"
else:
    status = "not_available"

doc = {
    "schema_version": 1,
    "suite": "hot_path_profiles",
    "status": status,
    "required_workloads": required,
    "profiles": profiles,
    "policy": "All required hot paths need measured flamegraph artifacts before profile-driven SIMD or executor changes are justified.",
}
manifest_path.write_text(json.dumps(doc, indent=2) + "\n")
print(json.dumps(doc, indent=2))
PY
}

write_csv

if [[ "${HOT_PROFILE_SKIP_BUILD:-0}" != "1" ]]; then
    cargo build --profile release-with-debug --package ultrasql-bench \
        --features sql-bench \
        --bin cross_compare_sql \
        --bin tpch
fi

overall=0
record_result() {
    local rc="$1"
    if [[ "$rc" == "0" ]]; then
        return
    fi
    if [[ "$rc" == "1" || "$overall" == "0" ]]; then
        overall="$rc"
    fi
}

for workload in "${WORKLOADS[@]}"; do
    case "$workload" in
        csv_copy)
            run_cross_compare_profile "$workload" csv-copy-import --csv-path "$CSV_PATH" || record_result "$?"
            ;;
        parquet_filter)
            ROWS="$PARQUET_ROWS" run_cross_compare_profile "$workload" parquet-smoke || record_result "$?"
            ;;
        vector_topk)
            ROWS="$VECTOR_ROWS" run_cross_compare_profile "$workload" vector-top-k \
                --vector-dims "$VECTOR_DIMS" --top-k "$TOP_K" || record_result "$?"
            ;;
        hash_aggregate)
            run_cross_compare_profile "$workload" dashboard-aggregate || record_result "$?"
            ;;
        joins)
            run_cross_compare_profile "$workload" csv-join-table --csv-path "$CSV_PATH" || record_result "$?"
            ;;
        tpch_q1)
            run_tpch_profile "$workload" 1 || record_result "$?"
            ;;
        tpch_q5)
            run_tpch_profile "$workload" 5 || record_result "$?"
            ;;
        tpch_q6)
            run_tpch_profile "$workload" 6 || record_result "$?"
            ;;
    esac
done

write_manifest
exit "$overall"
