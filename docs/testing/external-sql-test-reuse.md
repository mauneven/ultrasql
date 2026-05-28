# External SQL Test Reuse

UltraSQL can reuse external SQL test suites only through an auditable import
pipeline. SQLLogicTest is the portable SQL layer; it does not replace
UltraSQL-specific tests for WAL, recovery, MVCC visibility, snapshot isolation,
undo/update behavior, page LSNs, full-page writes, protocol edge cases, parser
fuzzing, WAL decoder fuzzing, or planner fuzzing.

## Safe and unsafe sources

Use these sources conservatively:

- Hydromatic SQL Logic Test: MIT-licensed public SQLLogicTest corpus with 7M+
  core SQL tests. Preferred large open suite for portable correctness and
  replay timing.
- SQLLogicTest-style corpora: preferred for portable SQL behavior when license
  and provenance are recorded.
- Public regression-derived cases: useful for SQL behavior coverage.
  Preserve upstream notices and record the exact source commit.
- Hermitage isolation scenarios: useful for transaction-isolation coverage.
  Preserve CC BY 4.0 attribution and pin the exact source commit; port reviewed
  scenarios into local tests instead of vendoring the upstream Markdown dump.
- DuckDB SQLLogicTest-style files: useful as inspiration. Check each file's
  license before reusing concrete tests.

Do not use these sources:

- SQLite TH3. It is proprietary.
- Any SQLite testing asset whose license is unclear.
- Any third-party file without an included license notice and immutable
  provenance.

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

Transaction isolation suites live under `tests/isolation/` because they require
coordinated multi-connection schedules rather than single-connection
SQLLogicTest records.

## Runner design

`ultrasql-sqllogictest-runner` reads `.slt` and `.test` files with the common
SQLLogicTest record shapes:

- `statement ok`
- `statement error`
- `query <type-string> [nosort|sort|rowsort]`

The runner supports two UltraSQL execution modes:

- `--mode wire`: connect to an already-running UltraSQL wire
  endpoint with `--database-url`.
- `--mode in-process`: start an in-process UltraSQL server on an ephemeral TCP
  listener, then connect to it with `tokio-postgres`.

Both modes run through the wire protocol. That validates parser, binder,
planner, executor, MVCC-visible SQL behavior, and wire result formatting. A
future storage-direct mode can reuse the same parsed test model if needed.

Supported UltraSQL directives:

- `# ultrasql:skip <reason>` skips the next record.
- `# ultrasql:require <feature>` requires `--feature <feature>` for the next
  record.
- `# ultrasql:file-skip <reason>` skips the rest of the file.
- `# ultrasql:file-require <feature>` requires a feature for the rest of the
  file.

Skip reasons are mandatory. Empty skip directives fail parsing so unsupported
coverage remains auditable instead of becoming silent debt.

## Filters

`third_party/sqllogictest/filters/unsupported.txt` is a text denylist. Each
non-comment line is:

```text
pattern<TAB>reason
```

If the pattern appears in a test path or SQL body, the runner reports an
explicit skip. Skips are visible in the summary; unsupported syntax is not
silently ignored.
Filter lines without an explicit tab-separated reason are rejected.

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

Documented normalizations are intentionally narrow:

- Wire rows are formatted as SQLLogicTest scalar values.
- SQLite/DuckDB CLI output uses one value per line, `NULL` for nulls, and
  carriage-return stripping for cross-platform shells.
- `rowsort` sorts complete SQLLogicTest rows before value comparison.

Any row-value mismatch after those normalizations is a failure.

SQLite and DuckDB comparison is intended only for portable subsets. Engine-
specific public regression shards should use their matching reference.

Run the public regression-derived subset with:

```sh
POSTGRES_URL="host=127.0.0.1 port=5432 user=postgres dbname=ultrasql_slt" \
tests/slt/run_postgres_compat.sh
```

The first curated shard lives under
`tests/slt/postgres_compat/regression_subset/`. It pins PostgreSQL commit
`ddd12d1a5c4d980c5f31dc7d096012547b724e55`, records
`src/test/regress/sql/select.sql`, `char.sql`, `varchar.sql`, `numeric.sql`,
`text.sql`, `date.sql`, `time.sql`, `timestamp.sql`, `timetz.sql`,
`json.sql`, `jsonb.sql`, `arrays.sql`, `type_sanity.sql`,
`create_index.sql`, `constraints.sql`, `create_operator.sql`, and
`opr_sanity.sql` as sources, and carries the PostgreSQL license next to the
SLT files. The parser/type, index/constraint/operator, and type-specific
shards keep unsupported catalog breadth, user-defined operator
checks, numeric overflow breadth, collation, timezone-abbreviation, SQL/JSON,
and array-slice checks as explicit `# ultrasql:skip` records instead of
silently dropping them.

Multiple measured engines can run in one pass:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --mode in-process \
  --reference-engine sqlite \
  --reference-engine duckdb \
  tests/slt/portable
```

The first committed imported shard is from Hydromatic SQL Logic Test at commit
`0a809c530457bf0e56d637ef19fcaabd2964fd67`. License and notice files live
beside the imported shard under `tests/slt/portable/imported/hydromatic/`.
Smoke command:

```sh
cargo run -p ultrasql-sqllogictest-runner -- \
  --mode in-process \
  --case-limit 50 \
  --reference-engine sqlite \
  --reference-engine duckdb \
  tests/slt/portable/imported/hydromatic
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
It uses `SLT_BENCH_CASE_LIMIT=50` by default so imported public suites do not
turn PR smoke into a long-running nightly job. Set `SLT_BENCH_CASE_LIMIT=all`
for a full replay.
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
- Nightly optional: run `--reference-url` against PostgreSQL for reference
  semantics.
- Nightly optional: run the portable subset with SQLite, DuckDB, and PostgreSQL
  references plus `SLT_BENCH_RUNS=25`.
- Manual: run larger imported suites after legal/provenance review.

Keep CI artifacts with counts for passed, skipped, and failed records. A growing
skip count is a coverage signal, not noise.
