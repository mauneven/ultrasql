# Fill-in cross-engine comparison — 2026-05-12 (Apple M4)

**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.
macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.
Reproduce via `bash run.sh` in this directory.

Identical methodology to `../comparison-2026-05-12-m4-extended/`;
see that directory's `methodology.md` for the full treatment. The
workloads here measure four additional size points to round out
the README headline set.

**Engines.** Same five engines as the parent comparison.

**Dataset sha256.**

```
0650e0ffecc24b50f4144198a5d6f2db5ff6a5ddd7ebde2ae5aeafd17c5b6bc4  data_x_256k.csv
31f54bbb9568730e7cc40b9621de632d9166c7e8fa3ecd1d02d21ab182c3ed33  data_x_1m.csv
98cc59dc50ad51c79ce9132633231221f06b809b34861b6734338fe590b0250a  data_x_4m.csv
```

> **Caveat.** The UltraSQL row measures the relevant vec kernel **in
> isolation** (no parser, no planner, no executor, no result-set
> materialization). Every other row measures the engine's full SQL
> pipeline. Read every UltraSQL row as a **lower bound** on the
> eventual end-to-end query, not a like-for-like result.

## `sum-256k`

**Workload.** `SELECT SUM(x) FROM t`
**Dataset.** 256,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | UltraSQL (kernel)   |      27.23 µs |       8 | vec kernels; kernel only, no SQL pipeline |
| 2    | DuckDB              |     258.11 µs |       8 |  |
| 3    | ClickHouse          |     374.00 µs |       8 | statistics.elapsed; Memory engine |
| 4    | SQLite              |       4.90 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |       6.52 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `27.25, 27.21, 27.21, 34.38, 27.46, 27.71, 27.12, 27.17`
- **DuckDB**: `954.46, 244.21, 228.33, 225.62, 253.67, 328.08, 296.42, 262.54`
- **SQLite**: `4978.00, 5024.00, 4726.00, 4874.00, 4925.00, 4950.00, 4883.00, 4868.00`
- **PostgreSQL**: `6670.00, 6788.00, 6631.00, 6533.00, 6468.00, 6511.00, 6505.00, 6512.00`
- **ClickHouse**: `95118.08, 854.33, 427.04, 377.46, 364.17, 370.54, 280.62, 325.67`

## `sum-4m`

**Workload.** `SELECT SUM(x) FROM t`
**Dataset.** 4,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | UltraSQL (kernel)   |     401.37 µs |       8 | vec kernels; kernel only, no SQL pipeline |
| 2    | ClickHouse          |     666.54 µs |       8 | statistics.elapsed; Memory engine |
| 3    | DuckDB              |       1.95 ms |       8 |  |
| 4    | SQLite              |      75.98 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |     114.17 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `403.29, 396.79, 392.58, 400.96, 411.42, 394.50, 415.04, 401.79`
- **DuckDB**: `1807.54, 1826.62, 1864.08, 1944.12, 2115.54, 1974.08, 1960.42, 1978.04`
- **SQLite**: `76200.00, 76366.00, 75969.00, 75649.00, 75985.00, 83001.00, 75301.00, 75926.00`
- **PostgreSQL**: `113699.00, 112224.00, 112867.00, 114840.00, 209797.00, 117200.00, 114209.00, 114140.00`
- **ClickHouse**: `744.46, 686.88, 645.17, 663.79, 677.92, 669.29, 636.92, 650.54`

## `count-1m`

**Workload.** `SELECT COUNT(*) FROM t`
**Dataset.** 1,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | UltraSQL (kernel)   |      0.000 µs |       8 | vec kernels; kernel only, no SQL pipeline |
| 2    | SQLite              |      57.50 µs |       8 | user time, microsecond resolution; :memory: db |
| 3    | DuckDB              |     264.83 µs |       8 |  |
| 4    | ClickHouse          |     298.11 µs |       8 | statistics.elapsed; Memory engine |
| 5    | PostgreSQL          |      22.10 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `0.042, 0.042, 0.0, 0.0, 0.0, 0.042, 0.0, 0.0`
- **DuckDB**: `285.79, 289.79, 243.88, 235.38, 396.17, 221.50, 444.25, 225.75`
- **SQLite**: `59.00, 58.00, 57.00, 53.00, 53.00, 60.00, 56.00, 58.00`
- **PostgreSQL**: `21892.00, 22118.00, 22439.00, 22052.00, 22915.00, 22273.00, 21866.00, 22079.00`
- **ClickHouse**: `297.46, 277.38, 291.17, 331.42, 298.75, 292.33, 329.96, 372.46`

## `avg-1m`

**Workload.** `SELECT AVG(x) FROM t`
**Dataset.** 1,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | UltraSQL (kernel)   |     159.65 µs |       8 | vec kernels; kernel only, no SQL pipeline |
| 2    | ClickHouse          |     481.62 µs |       8 | statistics.elapsed; Memory engine |
| 3    | DuckDB              |     906.62 µs |       8 |  |
| 4    | SQLite              |      18.99 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |      29.17 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `114.00, 163.54, 143.50, 155.75, 166.67, 139.00, 206.96, 176.21`
- **DuckDB**: `900.13, 897.79, 904.08, 909.67, 908.58, 906.00, 907.25, 977.38`
- **SQLite**: `23212.00, 21625.00, 18882.00, 19103.00, 18732.00, 18698.00, 19266.00, 18585.00`
- **PostgreSQL**: `28955.00, 28921.00, 32349.00, 31069.00, 29308.00, 29508.00, 29040.00, 28129.00`
- **ClickHouse**: `11273.21, 513.62, 513.71, 474.17, 489.08, 459.58, 442.75, 400.96`

