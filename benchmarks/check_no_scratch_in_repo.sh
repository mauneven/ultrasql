#!/usr/bin/env bash
# Guard against benchmark/certification runners leaking database data dirs into
# the repository. Transient work dirs must live under the external scratch root
# (see benchmarks/scratch.sh), never under target/ or the repo root.
#
# Fails (exit 1) if a non-cargo directory is found at target/ root.
#
# Usage:
#   benchmarks/check_no_scratch_in_repo.sh [TARGET_DIR]
#
# TARGET_DIR defaults to the repo's target/. An explicit path is accepted so the
# guard can be unit-tested hermetically.
#
# Exit codes:
#   0  — the directory contains only standard cargo/docs directories.
#   1  — a leaked scratch/data directory was detected.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET_DIR="${1:-$REPO_ROOT/target}"

# Standard cargo / docs directories permitted directly under target/.
ALLOWED=(debug release release-with-debug release-ship release-ship-lto \
         doc docs-venv tmp package tools criterion llvm-cov llvm-cov-target)

is_allowed() {
    local name="$1"
    for a in "${ALLOWED[@]}"; do [[ "$name" == "$a" ]] && return 0; done
    return 1
}

[[ -d "$TARGET_DIR" ]] || { echo "check_no_scratch_in_repo: OK — no target/ directory."; exit 0; }

failed=0
while IFS= read -r -d '' entry; do
    name="$(basename "$entry")"
    if ! is_allowed "$name"; then
        echo "ERROR: leaked scratch/data directory under target/: $entry" >&2
        failed=1
    fi
done < <(find "$TARGET_DIR" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null)

if [[ "$failed" -ne 0 ]]; then
    echo "" >&2
    echo "Benchmark runners must write transient data dirs to the external scratch" >&2
    echo "root (\$ULTRASQL_BENCH_SCRATCH), not into target/. See benchmarks/scratch.sh." >&2
    echo "Reclaim leaked space with: make clean-scratch" >&2
    exit 1
fi

echo "check_no_scratch_in_repo: OK — target/ holds only standard cargo directories."
