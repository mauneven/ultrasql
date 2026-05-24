# Coverage Evidence 2026-05-24

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

The workspace tests executed by `cargo llvm-cov` passed, but the
per-crate gate failed: 10 of 19 crates are below the 80% line coverage
threshold.

| Crate | Lines | Covered | Coverage | Gate |
|-------|------:|--------:|---------:|------|
| ultrasql-arrow | 511 | 340 | 66.54% | fail |
| ultrasql-bench | 14963 | 7114 | 47.54% | fail |
| ultrasql-catalog | 4472 | 4092 | 91.50% | pass |
| ultrasql-cli | 1459 | 521 | 35.71% | fail |
| ultrasql-core | 2203 | 1758 | 79.80% | fail |
| ultrasql-executor | 20888 | 15763 | 75.46% | fail |
| ultrasql-iceberg | 835 | 634 | 75.93% | fail |
| ultrasql-mvcc | 518 | 502 | 96.91% | pass |
| ultrasql-objectstore | 716 | 511 | 71.37% | fail |
| ultrasql-optimizer | 9401 | 7718 | 82.10% | pass |
| ultrasql-parser | 6527 | 5696 | 87.27% | pass |
| ultrasql-planner | 8515 | 6161 | 72.35% | fail |
| ultrasql-protocol | 799 | 766 | 95.87% | pass |
| ultrasql-server | 35498 | 25021 | 70.49% | fail |
| ultrasql-sqllogictest-runner | 1128 | 793 | 70.30% | fail |
| ultrasql-storage | 11194 | 9605 | 85.80% | pass |
| ultrasql-txn | 1933 | 1621 | 83.86% | pass |
| ultrasql-vec | 4716 | 4460 | 94.57% | pass |
| ultrasql-wal | 2748 | 2476 | 90.10% | pass |

Generated local artifacts:

- `target/llvm-cov/coverage.json`
- `target/llvm-cov/lcov.info`
- `target/llvm-cov/per-crate-coverage.json`
- `target/llvm-cov/per-crate-coverage.md`

Release status: this gate is not satisfied yet. The coverage workflow
now creates machine-readable and Markdown per-crate artifacts before
failing on crates below threshold.
