# SQLLogicTest Runner Coverage Evidence 2026-05-29

This records the focused `ultrasql-sqllogictest-runner` package proof run for
the per-crate 80% line coverage gate. It does not close the production coverage
gate by itself; the release gate still requires a fresh full-workspace
`cargo llvm-cov --workspace` artifact.

Commands:

```bash
cargo llvm-cov clean --package ultrasql-sqllogictest-runner
mkdir -p target/llvm-cov
cargo llvm-cov -p ultrasql-sqllogictest-runner --all-features \
  --json --output-path target/llvm-cov/slt-coverage.json
python3 scripts/coverage_gate.py \
  target/llvm-cov/slt-coverage.json \
  --root "$PWD" \
  --min-lines 80 \
  --summary-json target/llvm-cov/slt-per-crate.json \
  --summary-md target/llvm-cov/slt-per-crate.md
```

Result:

| Crate | Lines | Covered | Coverage | Gate |
|-------|------:|--------:|---------:|------|
| ultrasql-sqllogictest-runner | 1541 | 1317 | 85.46% | pass |

Release status: `ultrasql-sqllogictest-runner` has focused package evidence
above 80%. Remove it from the roadmap's full-workspace failing list only after
a fresh workspace coverage artifact confirms the same threshold in the release
gate.
