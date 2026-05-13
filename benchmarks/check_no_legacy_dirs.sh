#!/usr/bin/env bash
# Verify that no stale per-run benchmark directories exist under
# benchmarks/results/.
#
# The only permitted entries under benchmarks/results/ are:
#   latest/     — most-recent run output (auto-rendered).
#   (nothing else)
#
# This script exits with status 1 and prints offending paths if any
# prohibited directory is found.  Run it in CI to prevent accidental
# re-introduction of dated or named per-run directories.
#
# Usage:
#   benchmarks/check_no_legacy_dirs.sh
#
# Exit codes:
#   0  — no legacy directories found.
#   1  — one or more legacy directories detected.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_DIR="$REPO_ROOT/benchmarks/results"

# Directories that are always permitted.
ALLOWED=("latest")

failed=0

while IFS= read -r -d '' entry; do
    name="$(basename "$entry")"
    allowed=0
    for a in "${ALLOWED[@]}"; do
        if [[ "$name" == "$a" ]]; then
            allowed=1
            break
        fi
    done
    if [[ "$allowed" -eq 0 ]]; then
        echo "ERROR: legacy benchmark directory found: $entry" >&2
        failed=1
    fi
done < <(find "$RESULTS_DIR" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null)

if [[ "$failed" -ne 0 ]]; then
    echo "" >&2
    echo "Remove the directories listed above. Only benchmarks/results/latest/ is permitted." >&2
    echo "Stage-gate baselines live in benchmarks/baselines/ (see BENCHMARKS.md)." >&2
    exit 1
fi

echo "check_no_legacy_dirs: OK — only permitted directories found."
