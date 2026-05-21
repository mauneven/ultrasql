# Benchmark Arena Artifacts

- profile: `smoke`
- engines: `ultrasql,duckdb,clickhouse,postgres,firebolt`
- suites: `csv,parquet,object-parquet-range,clickbench,sqllogictest,vector,jsonb,aggregate-index,sparse-pruning,firebolt-vector`
- policy: artifacts only; no rankings or winner claims

| suite | status | exit | artifact |
| --- | --- | ---: | --- |
| `csv` | `unavailable` | 2 | `benchmarks/results/latest/csv_benchmark_gauntlet_manifest.json` |
| `csv:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_csv_smoke-postgres.json` |
| `csv:firebolt` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_csv_smoke-firebolt.json` |
| `parquet:ultrasql` | `passed` | 0 | `benchmarks/results/latest/raw/arena_parquet_smoke-ultrasql.json` |
| `parquet:duckdb` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_parquet_smoke-duckdb.json` |
| `parquet:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_parquet_smoke-clickhouse.json` |
| `parquet:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_parquet_smoke-postgres.json` |
| `parquet:firebolt` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_parquet_smoke-firebolt.json` |
| `object-parquet-range:ultrasql` | `passed` | 0 | `benchmarks/results/latest/object_parquet_range_manifest.json` |
| `object-parquet-range:duckdb` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_object-parquet-range_smoke-duckdb.json` |
| `object-parquet-range:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_object-parquet-range_smoke-clickhouse.json` |
| `object-parquet-range:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_object-parquet-range_smoke-postgres.json` |
| `object-parquet-range:firebolt` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_object-parquet-range_smoke-firebolt.json` |
| `clickbench` | `unavailable` | 2 | `benchmarks/results/latest/clickbench_certification.json` |
| `sqllogictest` | `passed` | 0 | `benchmarks/results/latest/slt_speed_comparison.json` |
| `sqllogictest:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_sqllogictest_smoke-clickhouse.json` |
| `sqllogictest:firebolt` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_sqllogictest_smoke-firebolt.json` |
| `vector` | `passed` | 0 | `benchmarks/results/latest/ai_benchmark_gauntlet_manifest.json` |
| `vector:firebolt` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_vector_smoke-firebolt.json` |
| `jsonb:ultrasql` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-ultrasql.json` |
| `jsonb:duckdb` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-duckdb.json` |
| `jsonb:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-clickhouse.json` |
| `jsonb:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-postgres.json` |
| `jsonb:firebolt` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-firebolt.json` |
| `aggregate-index` | `unavailable` | 2 | `benchmarks/results/latest/firebolt_aggregate_index_manifest.json` |
| `aggregate-index:duckdb` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_aggregate-index_smoke-duckdb.json` |
| `aggregate-index:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_aggregate-index_smoke-clickhouse.json` |
| `aggregate-index:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_aggregate-index_smoke-postgres.json` |
| `sparse-pruning` | `unavailable` | 2 | `benchmarks/results/latest/firebolt_sparse_pruning_manifest.json` |
| `sparse-pruning:duckdb` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_sparse-pruning_smoke-duckdb.json` |
| `sparse-pruning:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_sparse-pruning_smoke-clickhouse.json` |
| `sparse-pruning:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_sparse-pruning_smoke-postgres.json` |
| `firebolt-vector` | `unavailable` | 2 | `benchmarks/results/latest/firebolt_vector_search_manifest.json` |
| `firebolt-vector:duckdb` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_firebolt-vector_smoke-duckdb.json` |
| `firebolt-vector:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_firebolt-vector_smoke-clickhouse.json` |
| `firebolt-vector:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_firebolt-vector_smoke-postgres.json` |
