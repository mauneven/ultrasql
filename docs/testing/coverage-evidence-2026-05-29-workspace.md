# Workspace Coverage Evidence - 2026-05-29

This records the workflow-equivalent workspace coverage proof run on macOS.

## Commands

```bash
cargo fmt --all -- --check
cargo test --workspace --all-features --quiet
cargo llvm-cov clean --workspace
mkdir -p target/llvm-cov
cargo llvm-cov --workspace --all-features \
  --json --output-path target/llvm-cov/coverage.json
cargo llvm-cov report --lcov --output-path target/llvm-cov/lcov.info
python3 scripts/coverage_gate.py \
  target/llvm-cov/coverage.json \
  --root . \
  --min-lines 80 \
  --exclude-crate ultrasql-bench \
  --summary-json target/llvm-cov/per-crate-coverage.json \
  --summary-md target/llvm-cov/per-crate-coverage.md
```

## Result

- `cargo fmt --all -- --check`: passed.
- `cargo test --workspace --all-features --quiet`: passed.
- `cargo llvm-cov --workspace --all-features`: passed and wrote
  `target/llvm-cov/coverage.json`.
- `cargo llvm-cov report --lcov`: passed and wrote `target/llvm-cov/lcov.info`.
- `scripts/coverage_gate.py --min-lines 80 --exclude-crate ultrasql-bench`:
  passed for 19 checked crates, 0 below threshold.

`ultrasql-bench` is excluded from the line gate because it is a non-published
benchmark harness with external-engine driver code. It remains covered by
benchmark-profile, release-hardening, artifact-schema, and smoke certification
tests. The raw unexcluded workspace number is recorded here for honesty:
`ultrasql-bench` was `46.98%` lines (`7889/16792`) in this run.

## Per-Crate Gate Table

| Crate | Lines | Covered | Coverage | Gate |
|-------|------:|--------:|---------:|------|
| ultrasql-arrow | 618 | 540 | 87.38% | pass |
| ultrasql-catalog | 5341 | 4773 | 89.37% | pass |
| ultrasql-cli | 2428 | 2131 | 87.77% | pass |
| ultrasql-core | 4718 | 4066 | 86.18% | pass |
| ultrasql-executor | 28771 | 24098 | 83.76% | pass |
| ultrasql-iceberg | 1128 | 938 | 83.16% | pass |
| ultrasql-mvcc | 518 | 502 | 96.91% | pass |
| ultrasql-node | 125 | 105 | 84.00% | pass |
| ultrasql-objectstore | 982 | 877 | 89.31% | pass |
| ultrasql-optimizer | 9627 | 7917 | 82.24% | pass |
| ultrasql-parser | 8013 | 6895 | 86.05% | pass |
| ultrasql-planner | 13231 | 11435 | 86.43% | pass |
| ultrasql-protocol | 799 | 767 | 95.99% | pass |
| ultrasql-server | 44021 | 35475 | 80.59% | pass |
| ultrasql-sqllogictest-runner | 1541 | 1317 | 85.46% | pass |
| ultrasql-storage | 12385 | 10328 | 83.39% | pass |
| ultrasql-txn | 2037 | 1754 | 86.11% | pass |
| ultrasql-vec | 4797 | 4545 | 94.75% | pass |
| ultrasql-wal | 3041 | 2684 | 88.26% | pass |

## Artifacts

- `target/llvm-cov/coverage.json`
- `target/llvm-cov/lcov.info`
- `target/llvm-cov/per-crate-coverage.json`
- `target/llvm-cov/per-crate-coverage.md`
