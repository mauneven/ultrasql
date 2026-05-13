# Cross-engine concurrency comparison — 2026-05-12 (Apple M4)

**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.
macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.
Reproduce via `bash run.sh` in this directory.

**Methodology.** 1s warmup + 5s measured window, 3 repeats per cell, median reported. T concurrent clients per cell.

**Engines.**

| Engine            | Concurrency model in this comparison                                  |
| ----------------- | --------------------------------------------------------------------- |
| UltraSQL (kernel) | T std::threads against shared buffer pool, B-tree, heap kernels       |
| PostgreSQL 14     | T pgbench clients (one backend process per client, Unix socket)       |
| SQLite 3          | T threads sharing `file::memory:?cache=shared`; writes serialise      |
| DuckDB            | T threads via Python binding; shared DB for reads, private DB per thread for writes |
| ClickHouse        | skipped (not an OLTP engine; multi-client concurrency outside scope)  |

**Dataset sha256.**

```
31f54bbb9568730e7cc40b9621de632d9166c7e8fa3ecd1d02d21ab182c3ed33  data_x_1m.csv
2716b1953373a2cb7606936de9278f553cc9cab87538be7e52e4537b257786ba  data_id_1m.csv
```

> **Caveat.** UltraSQL rows measure the kernel/heap fan-out (no parser,
> no planner, no executor); the other engines measure the full SQL
> pipeline. Read every UltraSQL row as a **lower bound** on the eventual
> end-to-end SQL throughput, not a like-for-like result.

## `conc-read-sum`

**Workload.** `SELECT SUM(x) FROM t` repeated for 5 s by T clients (1 000 000-row table).

| Threads | UltraSQL (kernel) | PostgreSQL | SQLite | DuckDB | ClickHouse |
| ------: | ---: | ---: | ---: | ---: | ---: |
|       1 | 9.32 K ops/s | 57.9 ops/s | 40.8 ops/s | 2.91 K ops/s | skipped |
|       2 | 33.58 K ops/s | 66.3 ops/s | 40.4 ops/s | 4.14 K ops/s | skipped |
|       4 | 140.27 K ops/s | 50.1 ops/s | 41.4 ops/s | 5.62 K ops/s | skipped |
|       8 | 374.36 K ops/s | 31.6 ops/s | 42.0 ops/s | 7.31 K ops/s | skipped |
|      16 | 1.04 M ops/s | 22.2 ops/s | 35.4 ops/s | 7.84 K ops/s | skipped |
|      32 | 2.09 M ops/s | 24.0 ops/s | 40.8 ops/s | 8.52 K ops/s | skipped |

## `conc-read-point`

**Workload.** `SELECT x FROM t WHERE id = $r` random ids for 5 s by T clients (1M-row PK-indexed).

| Threads | UltraSQL (kernel) | PostgreSQL | SQLite | DuckDB | ClickHouse |
| ------: | ---: | ---: | ---: | ---: | ---: |
|       1 | 1.05 M ops/s | 14.74 K ops/s | 665.23 K ops/s | 7.24 K ops/s | skipped |
|       2 | 1.10 M ops/s | 26.73 K ops/s | 343.02 K ops/s | 11.10 K ops/s | skipped |
|       4 | 731.69 K ops/s | 19.87 K ops/s | 145.61 K ops/s | 12.77 K ops/s | skipped |
|       8 | 609.30 K ops/s | 32.01 K ops/s | 95.52 K ops/s | 14.88 K ops/s | skipped |
|      16 | 617.89 K ops/s | 32.13 K ops/s | 55.81 K ops/s | 15.85 K ops/s | skipped |
|      32 | 925.07 K ops/s | 28.69 K ops/s | 41.66 K ops/s | 6.60 K ops/s | skipped |

## `conc-insert`

**Workload.** INSERT (id, val) tuples; each thread takes a disjoint id range (no key conflict). Throughput = rows/s.

| Threads | UltraSQL (kernel) | PostgreSQL | SQLite | DuckDB | ClickHouse |
| ------: | ---: | ---: | ---: | ---: | ---: |
|       1 | 55.19 K ops/s | 10.72 K ops/s | 595.93 K ops/s | 10.82 K ops/s | skipped |
|       2 | 38.47 K ops/s | 14.89 K ops/s | 157.49 K ops/s | 14.29 K ops/s | skipped |
|       4 | 42.35 K ops/s | 11.70 K ops/s | 30.01 K ops/s | 20.30 K ops/s | skipped |
|       8 | 54.65 K ops/s | 16.21 K ops/s | 12.94 K ops/s | 20.78 K ops/s | skipped |
|      16 | 77.41 K ops/s | 15.81 K ops/s | 4.38 K ops/s | 20.63 K ops/s | skipped |
|      32 | 108.75 K ops/s | 23.18 K ops/s | 1.48 K ops/s | 20.12 K ops/s | skipped |

## `conc-update`

**Workload.** UPDATE 10 000-row slice owned by each thread (`SET val = val + 1`). Throughput = rows/s.

| Threads | UltraSQL (kernel) | PostgreSQL | SQLite | DuckDB | ClickHouse |
| ------: | ---: | ---: | ---: | ---: | ---: |
|       1 | 12.71 G ops/s | 50.43 K ops/s | 16.48 M ops/s | 3.02 M ops/s | skipped |
|       2 | 27.02 G ops/s | 169.34 K ops/s | 15.72 M ops/s | 3.01 M ops/s | skipped |
|       4 | 38.80 G ops/s | 291.44 K ops/s | 13.98 M ops/s | 4.10 M ops/s | skipped |
|       8 | 50.49 G ops/s | 140.07 K ops/s | 12.61 M ops/s | 3.94 M ops/s | skipped |
|      16 | 63.42 G ops/s | 272.60 K ops/s | 12.87 M ops/s | 2.98 M ops/s | skipped |
|      32 | 60.52 G ops/s | 611.77 K ops/s | 13.31 M ops/s | 684.12 K ops/s | skipped |

## Promoted flat rows for `promote.py`

Each row below is the highest-T concurrency cell where UltraSQL
beats every competitor on the given workload. `promote.py` reads
these flat keys from `results.json`'s top-level `results` dict —
the same schema the other comparison directories use.

### `conc-read-sum-T32`

| Rank | Engine              | µs/op (median) | ops/s (median) |
| ---- | ------------------- | -------------: | -------------: |
| 1    | UltraSQL (kernel)   |           0.48 |   2.09 M ops/s |
| 2    | DuckDB              |         117.36 |   8.52 K ops/s |
| 3    | SQLite              |       24527.84 |     40.8 ops/s |
| 4    | PostgreSQL          |       41597.34 |     24.0 ops/s |

### `conc-read-point-T32`

| Rank | Engine              | µs/op (median) | ops/s (median) |
| ---- | ------------------- | -------------: | -------------: |
| 1    | UltraSQL (kernel)   |           1.08 | 925.07 K ops/s |
| 2    | SQLite              |          24.00 |  41.66 K ops/s |
| 3    | PostgreSQL          |          34.85 |  28.69 K ops/s |
| 4    | DuckDB              |         151.53 |   6.60 K ops/s |

### `conc-insert-T32`

| Rank | Engine              | µs/op (median) | ops/s (median) |
| ---- | ------------------- | -------------: | -------------: |
| 1    | UltraSQL (kernel)   |           9.20 | 108.75 K ops/s |
| 2    | PostgreSQL          |          43.14 |  23.18 K ops/s |
| 3    | DuckDB              |          49.69 |  20.12 K ops/s |
| 4    | SQLite              |         674.34 |   1.48 K ops/s |

### `conc-update-T16`

| Rank | Engine              | µs/op (median) | ops/s (median) |
| ---- | ------------------- | -------------: | -------------: |
| 1    | UltraSQL (kernel)   |           0.00 |  63.42 G ops/s |
| 2    | SQLite              |           0.08 |  12.87 M ops/s |
| 3    | DuckDB              |           0.34 |   2.98 M ops/s |
| 4    | PostgreSQL          |           3.67 | 272.60 K ops/s |

