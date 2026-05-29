# Planner Coverage Evidence 2026-05-29

This records the focused `ultrasql-planner` package proof run for the
per-crate 80% line coverage gate. It does not close the production coverage
gate by itself; the release gate still requires a fresh full-workspace
`cargo llvm-cov --workspace` artifact.

Commands:

```bash
cargo llvm-cov clean --package ultrasql-planner
mkdir -p target/llvm-cov
cargo llvm-cov -p ultrasql-planner --all-features \
  --json --output-path target/llvm-cov/planner-coverage.json
python3 scripts/coverage_gate.py \
  target/llvm-cov/planner-coverage.json \
  --root "$PWD" \
  --min-lines 80 \
  --summary-json target/llvm-cov/planner-per-crate.json \
  --summary-md target/llvm-cov/planner-per-crate.md
```

Result:

| Crate | Lines | Covered | Coverage | Gate |
|-------|------:|--------:|---------:|------|
| ultrasql-planner | 13138 | 10535 | 80.19% | pass |

Scope:

- Added focused planner tests for table-reference binding, local CSV/JSON
  inference, Arrow type mapping, expression literals/coercions, builtin
  validation, window binding, catalog OID resolution, expression
  display/accessors, logical plan display/schema/pipeline paths, and privilege
  binding matrices.
- Hardened negative literal extraction in window defaults with checked
  negation for integer, decimal, and money values.

Release status: `ultrasql-planner` has focused package evidence above 80%.
Remove it from the roadmap's full-workspace failing list only after a fresh
workspace coverage artifact confirms the same threshold in the release gate.
