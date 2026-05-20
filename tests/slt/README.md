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

Run portable correctness plus replay timing:

```sh
SLT_BENCH_RUNS=25 benchmarks/slt_speed_compare.sh
```

The benchmark artifact compares UltraSQL with installed SQLite/DuckDB
references. It is a smoke signal for portable SQL replay speed, not a
replacement for TPC-H, ClickBench, or UltraSQL-specific correctness tests.

The first imported open suite shard lives under
`portable/imported/hydromatic/`. It comes from the MIT-licensed Hydromatic SQL
Logic Test repository and preserves license, notice, commit, and manifest
files beside the imported tests.
