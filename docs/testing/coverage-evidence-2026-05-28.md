# Coverage Evidence 2026-05-28

This records the local proof run for the per-crate 80% line coverage
gate. The workflow-equivalent run used `cargo-llvm-cov 0.8.7` on macOS
ARM64.

Commands:

```bash
cargo llvm-cov clean --workspace
mkdir -p target/llvm-cov
cargo llvm-cov --workspace --all-features \
  --json --output-path target/llvm-cov/coverage.json
python3 scripts/coverage_gate.py \
  target/llvm-cov/coverage.json \
  --root "$PWD" \
  --min-lines 80 \
  --summary-json target/llvm-cov/per-crate-coverage.json \
  --summary-md target/llvm-cov/per-crate-coverage.md
cargo llvm-cov report --lcov --output-path target/llvm-cov/lcov.info
```

The workspace tests executed by `cargo llvm-cov` passed. The per-crate
gate still failed: 10 of 20 crates are below the 80% line coverage
threshold. `ultrasql-node` now clears the gate at 84.00%.

| Crate | Lines | Covered | Coverage | Gate |
|-------|------:|--------:|---------:|------|
| ultrasql-arrow | 511 | 340 | 66.54% | fail |
| ultrasql-bench | 16598 | 7826 | 47.15% | fail |
| ultrasql-catalog | 5341 | 4773 | 89.37% | pass |
| ultrasql-cli | 1461 | 521 | 35.66% | fail |
| ultrasql-core | 4044 | 3193 | 78.96% | fail |
| ultrasql-executor | 24222 | 17856 | 73.72% | fail |
| ultrasql-iceberg | 835 | 634 | 75.93% | fail |
| ultrasql-mvcc | 518 | 502 | 96.91% | pass |
| ultrasql-node | 125 | 105 | 84.00% | pass |
| ultrasql-objectstore | 716 | 511 | 71.37% | fail |
| ultrasql-optimizer | 9627 | 7926 | 82.33% | pass |
| ultrasql-parser | 8013 | 6855 | 85.55% | pass |
| ultrasql-planner | 10788 | 7833 | 72.61% | fail |
| ultrasql-protocol | 799 | 767 | 95.99% | pass |
| ultrasql-server | 39815 | 28263 | 70.99% | fail |
| ultrasql-sqllogictest-runner | 1128 | 793 | 70.30% | fail |
| ultrasql-storage | 11951 | 10064 | 84.21% | pass |
| ultrasql-txn | 1942 | 1631 | 83.99% | pass |
| ultrasql-vec | 4716 | 4461 | 94.59% | pass |
| ultrasql-wal | 2988 | 2633 | 88.12% | pass |

Generated local artifacts:

- `target/llvm-cov/coverage.json`
- `target/llvm-cov/lcov.info`
- `target/llvm-cov/per-crate-coverage.json`
- `target/llvm-cov/per-crate-coverage.md`

Release status: this gate is not satisfied yet. Remaining P0 coverage
work should target `ultrasql-cli`, `ultrasql-bench`, `ultrasql-arrow`,
`ultrasql-server`, `ultrasql-objectstore`, `ultrasql-sqllogictest-runner`,
`ultrasql-planner`, `ultrasql-executor`, `ultrasql-iceberg`, and
`ultrasql-core`.
