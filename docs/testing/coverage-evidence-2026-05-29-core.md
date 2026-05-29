# Core Coverage Evidence 2026-05-29

This records the focused `ultrasql-core` package proof run for the per-crate
80% line coverage gate. It does not close the production coverage gate by
itself; the release gate still requires a fresh full-workspace
`cargo llvm-cov --workspace` artifact.

Commands:

```bash
cargo llvm-cov clean --package ultrasql-core
mkdir -p target/llvm-cov
cargo llvm-cov -p ultrasql-core --all-features \
  --json --output-path target/llvm-cov/core-coverage.json
python3 scripts/coverage_gate.py \
  target/llvm-cov/core-coverage.json \
  --root "$PWD" \
  --min-lines 80 \
  --summary-json target/llvm-cov/core-per-crate.json \
  --summary-md target/llvm-cov/core-per-crate.md
```

Result:

| Crate | Lines | Covered | Coverage | Gate |
|-------|------:|--------:|---------:|------|
| ultrasql-core | 4617 | 3715 | 80.46% | pass |

Release status: `ultrasql-core` has focused package evidence above 80%.
Remove it from the roadmap's full-workspace failing list only after a fresh
workspace coverage artifact confirms the same threshold in the release gate.
