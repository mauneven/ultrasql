# External SQL Test Reuse

UltraSQL can reuse external SQL test suites only through an auditable import
pipeline. SQLLogicTest is the compatibility layer; it does not replace
UltraSQL-specific tests for WAL, recovery, MVCC visibility, snapshot isolation,
undo/update behavior, page LSNs, full-page writes, protocol edge cases, parser
fuzzing, WAL decoder fuzzing, or planner fuzzing.

## Safe and unsafe sources

Use these sources conservatively:

- SQLLogicTest-style corpora: preferred for portable SQL behavior when license
  and provenance are recorded.
- PostgreSQL regression-derived cases: useful for PostgreSQL compatibility.
  Preserve upstream notices and record the exact source commit.
- DuckDB SQLLogicTest-style files: useful as inspiration. Check each file's
  license before copying concrete tests.

Do not use these sources:

- SQLite TH3. It is proprietary.
- Any SQLite testing asset whose license is unclear.
- Any third-party file without a copied license notice and immutable provenance.

## Repository layout

```text
third_party/sqllogictest/
  README.md
  LICENSE
  import.py
  upstream_commit.txt
  filters/unsupported.txt
tests/slt/
  portable/
  postgres_compat/
  ultrasql_specific/
crates/ultrasql-sqllogictest-runner/
  Cargo.toml
  src/main.rs
```

`third_party/sqllogictest/` is a control area. Imported test files should live
under `tests/slt/portable/imported/` or a reviewed suite-specific subdirectory,
not loose under `third_party/`.

## Runner design

`ultrasql-sqllogictest-runner` reads `.slt` and `.test` files with the common
SQLLogicTest record shapes:

- `statement ok`
- `statement error`
- `query <type-string> [nosort|sort|rowsort]`

The runner supports two UltraSQL execution modes:

- `--mode wire`: connect to an already-running UltraSQL PostgreSQL-wire
  endpoint with `--database-url`.
- `--mode in-process`: start an in-process UltraSQL server on an ephemeral TCP
  listener, then connect to it with `tokio-postgres`.

Both modes run through PostgreSQL wire protocol. That validates parser, binder,
planner, executor, MVCC-visible SQL behavior, and wire result formatting. A
future storage-direct mode can reuse the same parsed test model if needed.

Supported UltraSQL directives:

- `# ultrasql:skip <reason>` skips the next record.
- `# ultrasql:require <feature>` requires `--feature <feature>` for the next
  record.
- `# ultrasql:file-skip <reason>` skips the rest of the file.
- `# ultrasql:file-require <feature>` requires a feature for the rest of the
  file.

## Filters

`third_party/sqllogictest/filters/unsupported.txt` is a text denylist. Each
non-comment line is:

```text
pattern<TAB>reason
```

If the pattern appears in a test path or SQL body, the runner reports an
explicit skip. Skips are visible in the summary; unsupported syntax is not
silently ignored.

## Differential comparison

Primary mode checks expected SQLLogicTest output against UltraSQL.

Optional reference mode can add a reference engine:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --database-url "$ULTRASQL_URL" \
  --reference-url "$POSTGRES_URL" \
  tests/slt/portable
```

PostgreSQL references use `--reference-url`. SQLite and DuckDB references use
their command-line clients and a temporary database file:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --mode in-process \
  --reference-engine sqlite \
  tests/slt/portable

cargo run -p ultrasql-sqllogictest-runner -- \
  --mode in-process \
  --reference-engine duckdb \
  tests/slt/portable
```

Use `--reference-db PATH` to keep or reuse a SQLite/DuckDB reference database.
For statements, the runner compares success/error class. For queries, it
compares formatted row values after applying the same SQLLogicTest sort mode.

SQLite and DuckDB comparison is intended only for portable subsets. PostgreSQL
compatibility tests should use PostgreSQL as the reference.

Multiple reference engines can run in one pass:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --mode in-process \
  --reference-engine sqlite \
  --reference-engine duckdb \
  tests/slt/portable
```

## SQLLogicTest speed comparison

SQLLogicTest is a correctness suite, not a database performance benchmark. Its
own documentation says it is concerned with correct results, not performance.
UltraSQL still records SQLLogicTest replay timing because it catches obvious
wire/protocol/query-regression costs on portable SQL.

Use the committed speed script for reproducible smoke comparisons:

```sh
SLT_BENCH_RUNS=25 benchmarks/slt_speed_compare.sh
```

The script starts UltraSQL in-process, runs the portable SLT corpus for
correctness, then replays executable SQL records for timing. It uses Cargo's
release profile by default; set `SLT_BENCH_PROFILE=dev` only for iteration.
SQLite and DuckDB are included when `sqlite3` or `duckdb` are installed.
PostgreSQL can be added with:

```sh
POSTGRES_URL="host=127.0.0.1 port=5432 user=postgres dbname=ultrasql_slt" \
SLT_BENCH_ENGINES="sqlite duckdb postgres" \
benchmarks/slt_speed_compare.sh
```

Artifacts are written to:

```text
benchmarks/results/latest/slt_speed_comparison.json
benchmarks/results/latest/slt_speed_comparison.md
```

The artifact names the fastest engine for that replay. Treat it as a smoke
signal only. TPC-H, ClickBench, TPC-B, and targeted microbenchmarks remain the
authoritative performance gates.

## Importing external tests

Import from a local audited checkout:

```sh
python3 third_party/sqllogictest/import.py \
  --source /path/to/audited/sqllogictest-checkout \
  --commit <upstream-commit-sha> \
  --dest tests/slt/portable/imported
```

The importer:

- refuses to run without a source license or copyright file,
- copies only `.slt` and `.test` files by default,
- writes `IMPORT_MANIFEST.txt`,
- records `third_party/sqllogictest/upstream_commit.txt`,
- copies the upstream license to `third_party/sqllogictest/LICENSE.upstream`.

Review imported files before committing. Remove unsupported or license-risky
files rather than hiding them behind broad skips.

## Local commands

Start UltraSQL separately, then run the local smoke corpus:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --database-url "host=127.0.0.1 port=5432 user=ultrasql dbname=ultrasql" \
  tests/slt/portable
```

Use a disposable test database or schema. The authored smoke files clean up
their tables on success, but interrupted runs can still leave state behind.

Run without an external server by using in-process mode:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --mode in-process \
  tests/slt/portable
```

Run tagged UltraSQL-specific transaction tests:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --mode in-process \
  --feature transactions \
  tests/slt/ultrasql_specific
```

Run with PostgreSQL as a reference:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --database-url "$ULTRASQL_SLT_DATABASE_URL" \
  --reference-url "$POSTGRES_URL" \
  tests/slt/portable
```

## CI proposal

- Every PR: run `tests/slt/portable/basic.slt` against an in-process or spawned
  UltraSQL server once the CI server fixture exists.
- Every PR: run `benchmarks/slt_speed_compare.sh` with `SLT_BENCH_RUNS=3` and
  only installed local references, storing the JSON artifact.
- Nightly: run the full audited portable imported subset with skip reporting.
- Nightly optional: run `--reference-url` against PostgreSQL for
  PostgreSQL-compatible semantics.
- Nightly optional: run the portable subset with SQLite, DuckDB, and PostgreSQL
  references plus `SLT_BENCH_RUNS=25`.
- Manual: run larger imported suites after legal/provenance review.

Keep CI artifacts with counts for passed, skipped, and failed records. A growing
skip count is a compatibility signal, not noise.
