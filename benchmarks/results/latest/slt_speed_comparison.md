# SQLLogicTest Speed Comparison

- suite: SQLLogicTest replay
- benchmark_runs: 5
- case_count: 50
- fastest_engine: `sqlite`

| engine | ok | statements | query records | query iterations | skipped | total ms | avg us/query iteration |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ultrasql` | true | 28 | 19 | 95 | 3 | 16.123 | 169.720 |
| `sqlite` | true | 28 | 19 | 95 | 3 | 11.237 | 118.285 |
| `duckdb` | true | 28 | 19 | 95 | 3 | 33.101 | 348.434 |

This is compatibility-suite replay timing, not TPC-H/ClickBench certification.
