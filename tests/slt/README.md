# UltraSQL SQLLogicTest Corpus

This tree contains SQLLogicTest-style files used by
`ultrasql-sqllogictest-runner`.

Buckets:

- `portable/`: UltraSQL-authored or audited portable SQL subset tests.
- `postgres_compat/`: tests whose expected behavior intentionally follows
  PostgreSQL.
- `ultrasql_specific/`: tests for UltraSQL behavior exposed through SQL, not a
  replacement for storage/WAL/MVCC unit and integration tests.

External imports must go through `third_party/sqllogictest/import.py` and keep
license/provenance records. Imported shards stay small and filtered; expand the
portable corpus with reviewed slices, not broad third-party dumps.

Skips must always name a reason. Empty `# ultrasql:skip` directives and
skip-filter lines without `pattern<TAB>reason` are rejected.

Run portable correctness plus replay timing:

```sh
SLT_BENCH_RUNS=25 benchmarks/slt_speed_compare.sh
```

The benchmark artifact compares UltraSQL with installed SQLite/DuckDB
references. It is a smoke signal for portable SQL replay speed, not a
replacement for TPC-H, ClickBench, or UltraSQL-specific correctness tests.

Run portable differential correctness against reference engines:

```sh
tests/slt/run_differential.sh
```

The script selects only top-level `tests/slt/portable/*.slt` and
`tests/slt/portable/*.test` files by default. It compares against PostgreSQL
when `ULTRASQL_SLT_REFERENCE_URL` or `POSTGRES_URL` is set, against DuckDB when
`duckdb` is on `PATH`, and against SQLite when `sqlite3` is on `PATH`. Missing
engines are skipped with explicit reasons on stderr. Use `SLT_DIFF_PATHS` only
for reviewed portable paths and `SLT_DIFF_ENGINES=postgres,duckdb,sqlite` to
choose engines.

Run the PostgreSQL compatibility subset only against PostgreSQL as reference:

```sh
POSTGRES_URL="host=127.0.0.1 port=5432 user=postgres dbname=ultrasql_slt" \
tests/slt/run_postgres_compat.sh
```

`postgres_compat/regression_subset/` pins PostgreSQL `REL_17_STABLE` commit
`ddd12d1a5c4d980c5f31dc7d096012547b724e55` and preserves the upstream license
beside the curated SQLLogicTest translations. Current reviewed sources are
PostgreSQL `select.sql`, `char.sql`, `varchar.sql`, `numeric.sql`, and
`type_sanity.sql`.

The first imported open suite shard lives under
`portable/imported/hydromatic/`. It comes from the MIT-licensed Hydromatic SQL
Logic Test repository and preserves license, notice, commit, and manifest
files beside the imported tests.
