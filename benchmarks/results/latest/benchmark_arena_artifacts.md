# Benchmark Arena Artifacts

- profile: `smoke`
- engines: `ultrasql,duckdb,clickhouse,postgres`
- suites: `csv,parquet,vector,jsonb`
- policy: artifacts only; no rankings or winner claims

| suite | status | exit | artifact |
| --- | --- | ---: | --- |
| `csv` | `unavailable` | 2 | `benchmarks/results/latest/csv_benchmark_gauntlet_manifest.json` |
| `csv:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_csv_smoke-postgres.json` |
| `parquet:ultrasql` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_parquet_smoke-ultrasql.json` |
| `parquet:duckdb` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_parquet_smoke-duckdb.json` |
| `parquet:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_parquet_smoke-clickhouse.json` |
| `parquet:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_parquet_smoke-postgres.json` |
| `vector` | `unavailable` | 2 | `benchmarks/results/latest/ai_benchmark_gauntlet_manifest.json` |
| `jsonb:ultrasql` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-ultrasql.json` |
| `jsonb:duckdb` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-duckdb.json` |
| `jsonb:clickhouse` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-clickhouse.json` |
| `jsonb:postgres` | `unavailable` | 2 | `benchmarks/results/latest/raw/arena_jsonb_smoke-postgres.json` |
