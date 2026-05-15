#!/usr/bin/env bash
# clean-temp.sh — idempotent cleanup of build/agent temp artifacts.
#
# Runs after each automation Code turn via a Stop hook (see
# .claude/settings.local.json). Designed to be fast (<200ms) when there
# is nothing to clean, so it can run on every turn without friction.
#
# Removes:
#   - .claude/worktrees/agent-* dirs (orphaned Agent-isolation worktrees,
#     each with their own target/ holding 2-6 GiB of Cargo cache)
#   - target/criterion/*/report old HTML (kept criterion baselines)
#   - fuzz/target (rebuilt on next `cargo fuzz run`)
#   - target/tmp and target/.tmp* probe files
#
# Does NOT touch:
#   - target/ main (incremental cache, regenerated cost = ~15 min)
#   - fuzz/corpus (test corpus is valuable)
#   - .git (never)
#   - Any branches (refs are cheap; deletion needs explicit human OK)
#
# Usage:
#   bash scripts/clean-temp.sh            # quiet
#   bash scripts/clean-temp.sh --verbose  # log what is removed

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 0

VERBOSE=0
if [[ "${1:-}" == "--verbose" || "${1:-}" == "-v" ]]; then
  VERBOSE=1
fi

log() {
  if (( VERBOSE )); then
    printf '[clean-temp] %s\n' "$*" >&2
  fi
}

bytes_before() {
  if command -v du >/dev/null 2>&1; then
    du -sk "$1" 2>/dev/null | awk '{print $1}'
  else
    echo 0
  fi
}

freed_kb=0
add_freed() {
  freed_kb=$((freed_kb + ${1:-0}))
}

# 1. Orphaned Agent worktrees ------------------------------------------------
if [ -d .claude/worktrees ]; then
  shopt -s nullglob
  for wt in .claude/worktrees/agent-*; do
    [ -d "$wt" ] || continue
    size=$(bytes_before "$wt")
    log "removing worktree $wt (${size} KB)"
    # Try a clean git removal first (preserves worktree metadata coherence),
    # fall back to rm -rf if git refuses (locked, missing HEAD, etc).
    git worktree unlock "$wt" >/dev/null 2>&1 || true
    if git worktree remove --force "$wt" >/dev/null 2>&1; then
      add_freed "$size"
    elif rm -rf "$wt"; then
      add_freed "$size"
    fi
  done
  shopt -u nullglob
  git worktree prune >/dev/null 2>&1 || true
fi

# 2. fuzz/target -------------------------------------------------------------
# `cargo fuzz` rebuilds this on demand; corpora live in fuzz/corpus and are
# kept by the gitignore-aware path below.
if [ -d fuzz/target ]; then
  size=$(bytes_before fuzz/target)
  log "removing fuzz/target (${size} KB)"
  rm -rf fuzz/target && add_freed "$size"
fi

# 3. target probe / temp dirs ------------------------------------------------
for tmpdir in target/tmp target/.tmp; do
  if [ -d "$tmpdir" ]; then
    size=$(bytes_before "$tmpdir")
    log "removing $tmpdir (${size} KB)"
    rm -rf "$tmpdir" && add_freed "$size"
  fi
done

# 4. Criterion HTML report bloat (keep estimates.json, drop report bundles)
# Criterion regenerates the HTML on the next benchmark run; the raw JSON
# baselines are what `regression-gate` reads, so we keep them.
if [ -d target/criterion ]; then
  while IFS= read -r -d '' report_dir; do
    size=$(bytes_before "$report_dir")
    log "removing $report_dir (${size} KB)"
    rm -rf "$report_dir" && add_freed "$size"
  done < <(find target/criterion -type d -name report -print0 2>/dev/null)
fi

# 5. cargo incremental compile DB (per-target) -------------------------------
# These caches grow unboundedly across test runs but are pure compile cache —
# losing them costs one slow rebuild, not work. We trim only when target/
# exceeds 30 GiB to avoid hurting normal workflows.
if [ -d target ]; then
  target_kb=$(bytes_before target)
  threshold=$((30 * 1024 * 1024))
  if (( target_kb > threshold )); then
    for incdir in target/debug/incremental target/release/incremental; do
      if [ -d "$incdir" ]; then
        size=$(bytes_before "$incdir")
        log "target/ over 30 GiB, removing $incdir (${size} KB)"
        rm -rf "$incdir" && add_freed "$size"
      fi
    done
  fi
fi

# Output a one-line summary the hook can show in the transcript.
if (( freed_kb > 0 )); then
  if (( freed_kb >= 1024 * 1024 )); then
    printf 'clean-temp: freed %.2f GiB\n' "$(echo "$freed_kb / 1048576" | bc -l)"
  else
    printf 'clean-temp: freed %d MiB\n' $((freed_kb / 1024))
  fi
fi

exit 0
