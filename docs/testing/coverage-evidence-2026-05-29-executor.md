# Executor Coverage Evidence 2026-05-29

Focused `ultrasql-executor` package proof run for the per-crate 80% line
coverage gate. This does not close the final production coverage gate by
itself; the release gate still requires a fresh full-workspace
`cargo llvm-cov --workspace` artifact.

Commands:

```bash
cargo llvm-cov -p ultrasql-executor --all-features \
  --json --output-path target/executor-coverage.json
python3 scripts/coverage_gate.py \
  target/executor-coverage.json \
  --min-lines 80 \
  --summary-json target/executor-per-crate.json \
  --summary-md target/executor-per-crate.md
```

Result:

| Crate | Lines | Covered | Coverage | Gate |
|-------|------:|--------:|---------:|------|
| ultrasql-executor | 28771 | 23087 | 80.24% | pass |

Scope:

- Added focused executor tests for scalar compatibility helpers, catalog
  functions, array and JSON/XML/network edges, physical lowering, filter and
  projection column families, set/unique/sort aggregates, hash aggregate
  cancel/spill paths, modify-table constraints and index maintenance, row-codec
  storage families, profile counters, and window function edge behavior.
- Fixed window aggregation so non-contiguous rows with equal partition keys are
  grouped into the same partition while preserving first-seen partition order.
- Fixed row-codec BOOL builder finishing so decoded NULL bitmaps survive
  generic builder materialization.
- Added INTERVAL row-codec encode/decode/projected decode coverage and aligned
  BIT/VARBIT projected skipping with varlena storage.
- Added `quote_literal` binding/evaluation coverage through the server
  system-function round trip.

Verification:

- `cargo fmt --all -- --check`
- `cargo clippy -p ultrasql-executor -p ultrasql-planner -p ultrasql-server --all-targets --all-features -- -D warnings`
- `cargo test -p ultrasql-executor -p ultrasql-planner --all-features`
- `cargo test -p ultrasql-server --test system_functions_round_trip scalar_string_functions_return_postgres_shaped_values -- --nocapture`
- focused coverage command above, which executed the executor unit suite under
  llvm-cov.

Release status: `ultrasql-executor` has focused package evidence above 80%.
Remove it from the roadmap's full-workspace failing list only after a fresh
workspace coverage artifact confirms the same threshold in the release gate.
