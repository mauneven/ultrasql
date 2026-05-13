# Methodology — Fill-in cross-engine comparison (2026-05-12, M4)

Fill-in measurements to round out the README headline set. **Identical
methodology to `../comparison-2026-05-12-m4-extended/`** — read that
directory's `methodology.md` for the full treatment. Summary:

- Same five engines (UltraSQL kernel via `cross_compare`, DuckDB,
  SQLite `:memory:`, PostgreSQL 14 single-thread, ClickHouse Memory
  engine).
- Same deterministic seed pattern (`0xDEADBEEF`); SHA-256 of every
  dataset CSV recorded in `raw/dataset_sha256.txt` and `results.json`.
- Each engine, each query: warmup + 8 measured iterations, hot cache,
  take median. Per-iteration values preserved in `results.json`.
- UltraSQL rows are kernel-only (no SQL pipeline yet — see ROADMAP.md
  for v0.5 scope); every other row measures the engine's full SQL
  surface.

Workloads in this directory:

| Tag        | Query                    | Dataset             |
| ---------- | ------------------------ | ------------------- |
| `sum-256k` | `SELECT SUM(x) FROM t`   | 256,000 i64 rows    |
| `sum-4m`   | `SELECT SUM(x) FROM t`   | 4,000,000 i64 rows  |
| `count-1m` | `SELECT COUNT(*) FROM t` | 1,000,000 i64 rows  |
| `avg-1m`   | `SELECT AVG(x) FROM t`   | 1,000,000 i64 rows  |

The `cross_compare` binary already accepts arbitrary row counts via
its `--data <csv>` argument, so no source changes are required.

## Reproduction

```sh
cd benchmarks/results/comparison-2026-05-12-m4-fillin
bash run.sh
```

Pre-reqs: identical to the parent comparison.
