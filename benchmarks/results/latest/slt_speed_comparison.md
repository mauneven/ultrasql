# SQLLogicTest Speed Comparison

- suite: SQLLogicTest replay
- benchmark_runs: 5
- case_count: 26
- fastest_engine: `ultrasql`

| engine | ok | statements | query records | query iterations | skipped | total ms | avg us/query iteration |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ultrasql` | true | 15 | 11 | 55 | 0 | 7.075 | 128.640 |
| `sqlite` | true | 15 | 11 | 55 | 0 | 7.701 | 140.009 |
| `duckdb` | true | 15 | 11 | 55 | 0 | 22.888 | 416.146 |

This is compatibility-suite replay timing, not TPC-H/ClickBench certification.
