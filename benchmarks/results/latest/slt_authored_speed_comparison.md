# SQLLogicTest Speed Comparison

- suite: SQLLogicTest replay
- benchmark_runs: 3
- case_count: 26
- fastest_engine: `sqlite`

| engine | ok | statements | query records | query iterations | skipped | total ms | avg us/query iteration |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ultrasql` | true | 15 | 11 | 33 | 0 | 63.940 | 1937.573 |
| `sqlite` | true | 15 | 11 | 33 | 0 | 18.427 | 558.381 |
| `duckdb` | true | 15 | 11 | 33 | 0 | 56.821 | 1721.845 |

This is SQLLogicTest replay replay timing, not TPC-H/ClickBench certification.
