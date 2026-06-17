#!/usr/bin/env bash
# Canonical scratch-directory convention for UltraSQL benchmark and
# certification runners.
#
# Transient database data dirs (heap/WAL/segment files) and cached benchmark
# inputs must live OUTSIDE the repository, so that running benchmarks never
# bloats the working tree or `target/`. Every runner defaults its work dir to a
# subdirectory of the scratch root resolved here. Override the location with:
#
#   ULTRASQL_BENCH_SCRATCH=/path/on/a/big/disk benchmarks/<runner>.sh
#
# Reclaim everything with `make clean-scratch`.
#
# This file is safe to `source` (it exports ULTRASQL_BENCH_SCRATCH and defines
# helpers) and to execute (it prints the resolved scratch root).

# Resolve the canonical scratch root: an explicit override, else a per-user
# temp location outside the repository. Never defaults into the repo tree.
ultrasql_bench_scratch_root() {
    local root="${ULTRASQL_BENCH_SCRATCH:-${TMPDIR:-/tmp}/ultrasql-bench}"
    printf '%s\n' "${root%/}"
}

ULTRASQL_BENCH_SCRATCH="$(ultrasql_bench_scratch_root)"
export ULTRASQL_BENCH_SCRATCH

# Echo (and create) a named work dir under the scratch root. Use for
# semi-persistent caches that are intentionally reused across runs.
ultrasql_bench_scratch_dir() {
    local name="${1:?scratch dir name required}"
    local dir="$ULTRASQL_BENCH_SCRATCH/$name"
    mkdir -p "$dir"
    printf '%s\n' "$dir"
}

# Echo (and create) a fresh per-run dir under the scratch root, and register it
# for removal when the calling shell exits (success or failure). Use for
# transient run data that must not survive the run.
ultrasql_bench_run_dir() {
    local label="${1:-run}"
    local dir
    dir="$(mktemp -d "$ULTRASQL_BENCH_SCRATCH/${label}-XXXXXX")"
    # shellcheck disable=SC2064
    trap "rm -rf '$dir'" EXIT
    printf '%s\n' "$dir"
}

if [[ "${BASH_SOURCE[0]:-}" == "${0:-}" ]]; then
    mkdir -p "$ULTRASQL_BENCH_SCRATCH"
    printf '%s\n' "$ULTRASQL_BENCH_SCRATCH"
fi
