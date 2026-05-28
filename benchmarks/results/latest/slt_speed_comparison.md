# SQLLogicTest Speed Comparison

- suite: SQLLogicTest replay
- benchmark_runs: 5
- case_count: 50
- fastest_engine: `ultrasql`

| engine | ok | statements | query records | query iterations | skipped | total ms | avg us/query iteration |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ultrasql` | true | 29 | 19 | 95 | 2 | 15.586 | 164.066 |
| `duckdb` | true | 29 | 19 | 95 | 2 | 38.818 | 408.615 |

This is SQLLogicTest replay replay timing, not TPC-H/ClickBench certification.
