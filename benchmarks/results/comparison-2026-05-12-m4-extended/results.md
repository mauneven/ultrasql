# Extended cross-engine comparison — 2026-05-12 (Apple M4)

**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.
macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.
Reproduce via `bash run.sh` in this directory.

**Engines.**

| Engine            | Version                          | How measured                                    |
| ----------------- | -------------------------------- | ----------------------------------------------- |
| UltraSQL (kernel) | 0.0.1                            | `cross_compare` driver — vec kernels in isolation |
| DuckDB            | 1.5.2 (Variegata) 8a5851971f     | `PRAGMA enable_profiling=json` `latency`, threads=1 |
| SQLite            | 3.51.0 2025-06-12                | `.timer on` user time; `:memory:` db            |
| PostgreSQL        | 14.22 (Homebrew)                 | `psql \timing`; `max_parallel_workers_per_gather=0` |
| ClickHouse        | 26.5.1.587 (official build)      | `statistics.elapsed`; `Memory` engine           |

**Dataset sha256.**

```
31f54bbb9568730e7cc40b9621de632d9166c7e8fa3ecd1d02d21ab182c3ed33  data_x_1m.csv
00f59a969cd4200a44a5f9ec30affcd289a40f4e501e66ef9843667a1ef8069b  data_x_10m.csv
02c09f9d3034e250bc530338a6d40c670bcc45ea184c5a05b7173f6f79704888  data_y_10m.csv
f315bb10fdd1b47142744d9aff861acf44045f50c7f4cef5d70aaa3bba06d46d  data_xy_10m.csv
3e249eae4d627a81e3aba6cbf869e47d2385561965b3e1c621fc3b2eee207e39  data_id_10m.csv
```

> **Caveat at the top — applies to every table below.** The UltraSQL
> row measures the relevant vec kernel **in isolation**. UltraSQL
> has no SQL pipeline end-to-end yet (parser → plan → execute lands
> at v0.5; see `ROADMAP.md`). Every other row measures the engine's
> full SQL pipeline (parse, plan, dispatch, execute, materialize).
> Treat the UltraSQL row as a **lower bound** on what the eventual
> end-to-end query will achieve, not as a like-for-like result.

## `sum-1m`

**Workload.** `SELECT SUM(x) FROM t`
**Dataset.** 1,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | UltraSQL (kernel)   |      77.27 µs |       8 | vec kernels; kernel only, no SQL pipeline |
| 2    | ClickHouse          |     491.77 µs |       8 | statistics.elapsed; Memory engine |
| 3    | DuckDB              |     640.10 µs |       8 |  |
| 4    | SQLite              |      19.36 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |      26.62 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `95.38, 86.67, 77.75, 78.38, 76.12, 76.79, 75.50, 76.42`
- **DuckDB**: `1494.04, 642.54, 604.21, 618.58, 637.67, 635.96, 688.42, 665.96`
- **SQLite**: `19260.00, 19227.00, 19565.00, 19702.00, 19446.00, 19368.00, 19174.00, 19357.00`
- **PostgreSQL**: `29112.00, 26918.00, 26768.00, 26745.00, 26498.00, 26478.00, 26179.00, 26026.00`
- **ClickHouse**: `84325.04, 650.04, 577.08, 556.29, 427.25, 402.67, 388.12, 385.29`

## `sum-10m`

**Workload.** `SELECT SUM(x) FROM t`
**Dataset.** 10,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | UltraSQL (kernel)   |       1.17 ms |       8 | vec kernels; kernel only, no SQL pipeline |
| 2    | ClickHouse          |       1.33 ms |       8 | statistics.elapsed; Memory engine |
| 3    | DuckDB              |       5.07 ms |       8 |  |
| 4    | SQLite              |     197.05 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |     270.45 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `1133.08, 1162.92, 1175.58, 1158.62, 1170.21, 1170.00, 1197.50, 1201.08`
- **DuckDB**: `4864.83, 4948.46, 5263.38, 5225.42, 4895.63, 5551.13, 5131.79, 5017.79`
- **SQLite**: `201547.00, 196146.00, 197170.00, 196858.00, 197936.00, 197363.00, 196452.00, 196936.00`
- **PostgreSQL**: `270966.00, 270556.00, 273420.00, 269517.00, 270979.00, 269300.00, 270339.00, 270073.00`
- **ClickHouse**: `1337.67, 1208.62, 1155.38, 1299.67, 1333.92, 1330.00, 1408.58, 1349.96`

## `count-10m`

**Workload.** `SELECT COUNT(*) FROM t`
**Dataset.** 10,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | UltraSQL (kernel)   |      0.000 µs |       8 | vec kernels; kernel only, no SQL pipeline |
| 2    | ClickHouse          |     501.61 µs |       8 | statistics.elapsed; Memory engine |
| 3    | DuckDB              |       1.09 ms |       8 |  |
| 4    | SQLite              |       1.60 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |     205.32 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `0.0, 0.0, 0.042, 0.0, 0.0, 0.041, 0.0, 0.0`
- **DuckDB**: `1137.87, 1124.21, 1098.79, 1092.00, 1097.37, 1063.46, 1055.00, 1080.62`
- **SQLite**: `1566.00, 1479.00, 1494.00, 1553.00, 2068.00, 2368.00, 1906.00, 1642.00`
- **PostgreSQL**: `206150.00, 216633.00, 205328.00, 204852.00, 203813.00, 205317.00, 205144.00, 206224.00`
- **ClickHouse**: `493.83, 543.21, 502.33, 493.79, 517.62, 516.46, 500.88, 496.96`

## `minmax-10m`

**Workload.** `SELECT MIN(x), MAX(x) FROM t`
**Dataset.** 10,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | ClickHouse          |       1.43 ms |       8 | statistics.elapsed; Memory engine |
| 2    | UltraSQL (kernel)   |       2.54 ms |       8 | vec kernels; kernel only, no SQL pipeline |
| 3    | DuckDB              |      14.22 ms |       8 |  |
| 4    | SQLite              |     253.55 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |     284.36 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `2541.50, 2544.79, 2544.46, 2566.46, 2535.04, 2538.25, 2549.96, 2557.08`
- **DuckDB**: `14041.50, 13783.12, 14430.92, 14537.92, 14267.92, 14179.54, 14252.08, 14119.08`
- **SQLite**: `254009.00, 256113.00, 259476.00, 251705.00, 257105.00, 251486.00, 253024.00, 253085.00`
- **PostgreSQL**: `286053.00, 284057.00, 287415.00, 282296.00, 284661.00, 300474.00, 281901.00, 281688.00`
- **ClickHouse**: `11225.58, 1448.50, 1614.33, 1527.33, 1382.96, 1330.92, 1402.46, 1311.04`

## `avg-10m`

**Workload.** `SELECT AVG(x) FROM t`
**Dataset.** 10,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | UltraSQL (kernel)   |       1.18 ms |       8 | vec kernels; kernel only, no SQL pipeline |
| 2    | ClickHouse          |       1.26 ms |       8 | statistics.elapsed; Memory engine |
| 3    | DuckDB              |       7.92 ms |       8 |  |
| 4    | SQLite              |     199.94 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |     269.94 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `1182.71, 1177.25, 1182.75, 1201.67, 1198.12, 1185.38, 1211.71, 1178.33`
- **DuckDB**: `8099.96, 7829.54, 8003.67, 7939.92, 7938.67, 7886.96, 7903.92, 7889.62`
- **SQLite**: `195542.00, 193791.00, 195621.00, 200377.00, 200048.00, 200028.00, 199856.00, 209441.00`
- **PostgreSQL**: `285580.00, 291871.00, 270313.00, 271990.00, 269257.00, 269251.00, 269338.00, 269561.00`
- **ClickHouse**: `9896.33, 1336.29, 1308.46, 1255.17, 1258.21, 1258.21, 1254.08, 1234.62`

## `filter-10m`

**Workload.** `SELECT SUM(x) FROM t WHERE y > 0`
**Dataset.** 10,000,000 (i64 x, i64 y)

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | ClickHouse          |       4.28 ms |       8 | statistics.elapsed; Memory engine |
| 2    | DuckDB              |      11.01 ms |       8 |  |
| 3    | UltraSQL (kernel)   |      33.20 ms |       8 | vec kernels; kernel only, no SQL pipeline |
| 4    | SQLite              |     257.03 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |     354.42 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `33204.29, 33225.50, 33144.00, 33127.12, 33197.12, 33193.92, 33247.25, 33252.17`
- **DuckDB**: `11105.12, 10929.58, 11002.25, 11061.38, 10998.67, 10814.04, 11016.79, 11175.38`
- **SQLite**: `258013.00, 256212.00, 261866.00, 256308.00, 257760.00, 253238.00, 255189.00, 268930.00`
- **PostgreSQL**: `340003.00, 403268.00, 390183.00, 353078.00, 355768.00, 347817.00, 375236.00, 345965.00`
- **ClickHouse**: `4357.08, 4427.96, 3654.58, 4366.08, 3897.88, 4197.33, 3745.79, 4395.29`

## `range-10m`

**Workload.** `SELECT COUNT(*) FROM t WHERE x BETWEEN -1e9 AND 1e9`
**Dataset.** 10,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | ClickHouse          |       6.12 ms |       8 | statistics.elapsed; Memory engine |
| 2    | DuckDB              |      10.71 ms |       8 |  |
| 3    | UltraSQL (kernel)   |      31.49 ms |       8 | vec kernels; kernel only, no SQL pipeline |
| 4    | SQLite              |     211.43 ms |       8 | user time, microsecond resolution; :memory: db |
| 5    | PostgreSQL          |     336.51 ms |       8 | psql \timing; same session; 10 warmup queries per workload |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `30761.04, 31187.71, 31430.58, 31440.12, 31556.54, 31529.92, 31868.67, 31632.17`
- **DuckDB**: `10871.29, 10728.00, 10630.21, 10635.63, 10639.83, 12517.96, 10818.87, 10698.17`
- **SQLite**: `209334.00, 210079.00, 218823.00, 211629.00, 211228.00, 214141.00, 210880.00, 218473.00`
- **PostgreSQL**: `336546.00, 336473.00, 328115.00, 339614.00, 357576.00, 328428.00, 328686.00, 353657.00`
- **ClickHouse**: `13874.88, 6242.62, 5996.33, 6408.75, 6639.08, 5752.88, 5945.83, 5971.58`

## `point-10m`

**Workload.** `SELECT x FROM t WHERE id = ?`
**Dataset.** 10,000,000 i64 indexed table (UltraSQL row uses 1,000,000-row B-tree; see note)

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | SQLite              |       2.00 µs |       8 | user time, microsecond resolution; :memory: db |
| 2    | PostgreSQL          |      17.50 µs |       8 | psql \timing; same session; 10 warmup queries per workload |
| 3    | DuckDB              |     116.27 µs |       8 |  |
| 4    | ClickHouse          |       1.84 ms |       8 | statistics.elapsed; Memory engine |
| 5    | UltraSQL (kernel)   |       6.78 ms |       8 | BTree<i64> point lookup; tree capped at 1M keys (v0.5 buffer pool refuses to evict dirty pages) |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `7230.71, 6575.33, 6714.83, 6452.08, 7104.12, 8129.33, 6847.46, 6418.25`
- **DuckDB**: `154.25, 126.04, 119.17, 116.54, 114.58, 116.00, 114.21, 115.42`
- **SQLite**: `2.00, 2.00, 2.00, 2.00, 2.00, 2.00, 2.00, 1.00`
- **PostgreSQL**: `19.00, 20.00, 17.00, 19.00, 18.00, 17.00, 17.00, 16.00`
- **ClickHouse**: `1907.33, 1990.92, 1852.96, 1784.62, 1778.08, 1829.29, 1971.25, 1816.33`

## `topk-1m`

**Workload.** `SELECT x FROM t ORDER BY x LIMIT 10`
**Dataset.** 1,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | ClickHouse          |     678.11 µs |       8 | statistics.elapsed; Memory engine |
| 2    | DuckDB              |     946.31 µs |       8 |  |
| 3    | SQLite              |      20.48 ms |       8 | user time, microsecond resolution; :memory: db |
| 4    | PostgreSQL          |      27.58 ms |       8 | psql \timing; same session; 10 warmup queries per workload |
| —    | UltraSQL (kernel)   |     skipped   |    —    | no ORDER BY / Top-K kernel in vec yet (v0.5 scope) |

**Per-iteration data (µs).**

- **DuckDB**: `1001.67, 957.25, 946.00, 946.62, 946.92, 943.79, 942.21, 942.29`
- **SQLite**: `22555.00, 24455.00, 20499.00, 21013.00, 20325.00, 20029.00, 20453.00, 19854.00`
- **PostgreSQL**: `27359.00, 26933.00, 28479.00, 28524.00, 29108.00, 27500.00, 27645.00, 27510.00`
- **ClickHouse**: `847.83, 897.62, 756.62, 666.46, 661.46, 670.67, 645.12, 685.54`

## `topk-10m`

**Workload.** `SELECT x FROM t ORDER BY x LIMIT 10`
**Dataset.** 10,000,000 i64 rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | DuckDB              |       1.29 ms |       8 |  |
| 2    | ClickHouse          |       3.56 ms |       8 | statistics.elapsed; Memory engine |
| 3    | SQLite              |     203.83 ms |       8 | user time, microsecond resolution; :memory: db |
| 4    | PostgreSQL          |     282.67 ms |       8 | psql \timing; same session; 10 warmup queries per workload |
| —    | UltraSQL (kernel)   |     skipped   |    —    | no ORDER BY / Top-K kernel in vec yet (v0.5 scope) |

**Per-iteration data (µs).**

- **DuckDB**: `1318.58, 1312.58, 1295.33, 1291.75, 1291.88, 1294.42, 1288.17, 1330.46`
- **SQLite**: `202958.00, 201421.00, 201857.00, 203997.00, 203825.00, 203841.00, 204612.00, 204883.00`
- **PostgreSQL**: `284576.00, 282585.00, 307387.00, 292195.00, 277305.00, 282762.00, 280175.00, 281521.00`
- **ClickHouse**: `3389.38, 3799.62, 3550.83, 3612.58, 3562.29, 3617.08, 3458.38, 3525.00`

## What this is and isn't

This is nine cross-engine micro-comparisons on a single host. It is
not TPC-H, not TPC-DS, and not an endorsement of any engine. Each
engine is measured by its own most-honest self-reported facility
(see `methodology.md`). Raw stdout per engine is in `raw/`.

The UltraSQL lines are in a different category: they measure SIMD
kernels (and, for the point lookup, the v0.5 B+ tree's `lookup`)
without any parser, planner, executor, or result-set
materialization. They exist in these tables because the kernel is
what the eventual UltraSQL end-to-end query will pay for the actual
data plane. When v0.5 ships, this directory should be re-run and
each UltraSQL row will be the engine's measured end-to-end time,
not the kernel.

Top-K rows are explicitly skipped on UltraSQL because no ORDER BY
kernel exists in `vec` yet. Filling in this row is part of the v0.5
scope; we do not fabricate a number from `Vec::sort` because that
would not be representative of the eventual sort kernel.
