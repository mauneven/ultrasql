<!--
  Thank you for contributing to UltraSQL.

  Before submitting, please read CONTRIBUTING.md and AGENTS.md.
  Larger changes go through the RFC process — see RFC_PROCESS.md.
-->

## Summary

<!-- One sentence: what this PR changes. -->

## Motivation

<!--
  Why this change? What is broken or missing today? Link issues with
  `Refs:` / `Closes:`.
-->

## Approach

<!--
  How the change works. For non-trivial changes, sketch the design.
-->

## Testing

<!--
  How did you verify correctness? List the tests you added or
  exercised. A bug fix must include a regression test.
-->

## Performance

<!--
  Required for any PR that touches a hot path (storage, WAL, executor,
  vec, mvcc, txn). Quote criterion before/after numbers, link the
  benchmark file, and note the host (e.g. "M4 Mac mini").

  Format:

  - bench:     <bench name / path>
  - host:      <host description>
  - before:    median, p95
  - after:     median, p95
  - delta:     +/- %
-->

## Risk and Rollback

<!--
  What could break? How do we roll back?
-->

## Checklist

- [ ] `cargo fmt --all -- --check` clean.
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean.
- [ ] `cargo test --workspace --all-features` green.
- [ ] Doc comments updated for any public API change.
- [ ] ARCHITECTURE.md updated if subsystem boundaries change.
- [ ] Commit messages follow Conventional Commits (see AGENTS.md §10).
- [ ] No `unwrap()`/`expect()` introduced in non-test code without a
      named invariant.
