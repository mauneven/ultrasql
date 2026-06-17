#!/usr/bin/env bash
# Reclaim local disk used by builds and benchmark runs WITHOUT touching any
# git-tracked file or committed benchmark artifact.
#
# Removes:
#   - target/*/incremental         (Rust incremental compilation cache)
#   - the external benchmark scratch root (ULTRASQL_BENCH_SCRATCH, default
#     ${TMPDIR:-/tmp}/ultrasql-bench)
#   - stray non-cargo directories that leaked into target/ root
#
# Never removes: source, benchmarks/results/**, or the standard cargo build
# directories (debug, release, release-*, doc, docs-venv, tmp, package,
# criterion, llvm-cov*).
#
# Usage:
#   scripts/clean-scratch.sh            # reclaim
#   scripts/clean-scratch.sh --dry-run  # show what would be removed

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=benchmarks/scratch.sh
source "$REPO_ROOT/benchmarks/scratch.sh"

DRY_RUN=0
[[ "${1:-}" == "--dry-run" || "${1:-}" == "-n" ]] && DRY_RUN=1

# Standard cargo / docs directories that must never be treated as scratch.
KEEP=(debug release release-with-debug release-ship release-ship-lto \
      doc docs-venv tmp package tools criterion llvm-cov llvm-cov-target \
      .fingerprint .rustc_info.json CACHEDIR.TAG)

is_kept() {
    local name="$1"
    for k in "${KEEP[@]}"; do [[ "$name" == "$k" ]] && return 0; done
    return 1
}

remove() {
    local path="$1"
    [[ -e "$path" ]] || return 0
    if [[ "$DRY_RUN" -eq 1 ]]; then
        printf 'would remove  %s\n' "$path"
        return 0
    fi
    chmod -R u+rwX "$path" 2>/dev/null || true
    rm -rf "$path"
    printf 'removed       %s\n' "$path"
}

# 1) Incremental compilation caches (regenerate on next build).
if [[ -d "$REPO_ROOT/target" ]]; then
    while IFS= read -r -d '' inc; do remove "$inc"; done \
        < <(find "$REPO_ROOT/target" -mindepth 2 -maxdepth 2 -type d -name incremental -print0 2>/dev/null)
fi

# 2) External benchmark scratch root (data dirs + cached inputs).
remove "$ULTRASQL_BENCH_SCRATCH"

# 3) Stray non-cargo directories that leaked into target/ root.
if [[ -d "$REPO_ROOT/target" ]]; then
    while IFS= read -r -d '' entry; do
        name="$(basename "$entry")"
        is_kept "$name" || remove "$entry"
    done < <(find "$REPO_ROOT/target" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null)
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "clean-scratch: dry run complete (nothing removed)."
else
    echo "clean-scratch: done."
fi
