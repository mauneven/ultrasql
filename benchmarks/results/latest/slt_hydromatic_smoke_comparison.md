# SQLLogicTest Speed Comparison

- suite: SQLLogicTest replay
- benchmark_runs: 1
- case_count: 50
- fastest_engine: `sqlite`

| engine | ok | statements | query records | query iterations | skipped | total ms | avg us/query iteration |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ultrasql` | true | 19 | 28 | 28 | 3 | 16.973 | 606.172 |
| `sqlite` | true | 19 | 28 | 28 | 3 | 9.435 | 336.947 |
| `duckdb` | true | 19 | 28 | 28 | 3 | 27.965 | 998.748 |

This is compatibility-suite replay timing, not TPC-H/ClickBench certification.
