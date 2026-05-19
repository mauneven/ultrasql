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
license/provenance records.

Run portable correctness plus replay timing:

```sh
SLT_BENCH_RUNS=25 benchmarks/slt_speed_compare.sh
```

The benchmark artifact compares UltraSQL with installed SQLite/DuckDB
references. It is a smoke signal for portable SQL replay speed, not a
replacement for TPC-H, ClickBench, or UltraSQL-specific correctness tests.
