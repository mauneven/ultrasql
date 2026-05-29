# Server Coverage Evidence 2026-05-29

Focused `ultrasql-server` package proof run for the per-crate 80% line
coverage gate. This does not close the final production coverage gate by
itself; the release gate still requires a fresh full-workspace
`cargo llvm-cov --workspace` artifact.

Commands:

```bash
cargo llvm-cov clean --package ultrasql-server
cargo llvm-cov -p ultrasql-server --all-features \
  --json --output-path target/server-coverage.json
python3 scripts/coverage_gate.py \
  target/server-coverage.json \
  --min-lines 80 \
  --summary-json target/server-per-crate.json \
  --summary-md target/server-per-crate.md
```

Result:

| Crate | Lines | Covered | Coverage | Gate |
|-------|------:|--------:|---------:|------|
| ultrasql-server | 42617 | 34121 | 80.06% | pass |

Scope:

- Added focused server tests for COPY text/binary edge cases, result encoding,
  transaction state transitions, EXPLAIN rendering, metadata statements,
  privilege collection/enforcement, JSON_TABLE lowering, recursive CTE set
  helpers, TPC-H sidecar caches, Q1 columnar summaries, ops HTTP paths, and WAL
  archive/restore edge handling.
- Fixed binary COPY UUID output to emit the raw 16-byte payload.
- Fixed text COPY `bytea` decoding to accept valid `\x...` hex and reject
  malformed hex input.
- Fixed recursive CTE DISTINCT filtering so numeric and boolean NULL bitmaps are
  preserved when rows are filtered.

Verification:

- `cargo fmt --all -- --check`
- `cargo clippy -p ultrasql-server --all-targets --all-features -- -D warnings`
- focused coverage command above, which executed the server unit, binary, and
  integration suite under llvm-cov.

Release status: `ultrasql-server` has focused package evidence above 80%.
Remove it from the roadmap's full-workspace failing list only after a fresh
workspace coverage artifact confirms the same threshold in the release gate.
