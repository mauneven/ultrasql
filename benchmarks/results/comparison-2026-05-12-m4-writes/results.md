# Write cross-engine comparison — 2026-05-12 (Apple M4)

**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.
macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.
Reproduce via `bash run.sh` in this directory.

Companion to `../comparison-2026-05-12-m4-extended/` and `-fillin/`.
Five engines, same host, same deterministic seed pattern; this
directory covers the write side of the workload matrix.

**Durability.** Every engine runs in its **normal durable mode**.
PostgreSQL: `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`
(defaults). SQLite: tempfile, `journal_mode=WAL`,
`synchronous=NORMAL`. DuckDB: ATTACH to file + `CHECKPOINT` after
each measured statement. ClickHouse: `MergeTree` on disk.
UltraSQL: `WalWriter` group-commit fsync + segment-file fsync per
iteration. See `methodology.md` for full details.

**Dataset sha256.**

```
c69dc63efb95ce71472b526e4debd42366e27cab898cd4893b573b06635b8032  data_idval_100k.csv
eaecb87046bb4314fc88667551c1861b94ff7b12267f30b5bb63c525e176bd10  data_idval_1m.csv
```

> **Caveat.** The UltraSQL row measures the heap access method and
> WAL writer **in isolation** — no parser, no planner, no executor,
> no result-set materialization, no constraint enforcement (no
> primary-key uniqueness check, in particular). Every other row
> measures the engine's full SQL pipeline including durable commit.
> Read every UltraSQL row as the **lower bound** on the eventual
> end-to-end statement, not a like-for-like result.

## `insert-bulk-100k`

**Workload.** `INSERT 100k rows`
**Dataset.** 100,000 i64 PK rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | ClickHouse          |       5.00 ms |       4 | MergeTree on disk; --time stderr elapsed; UPDATE/DELETE via ALTER + mutations_sync=2 |
| 2    | SQLite              |      34.00 ms |       4 | tempfile; WAL mode; synchronous=NORMAL |
| 3    | DuckDB              |      36.50 ms |       4 | ATTACH-to-file; CHECKPOINT inside the timed region |
| 4    | PostgreSQL          |     333.56 ms |       4 | psql \timing; synchronous_commit=on; fsync=on |
| 5    | UltraSQL (kernel)   |        1.54 s |       4 | heap-access + WAL group-commit; kernel-level (no SQL pipeline) |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `1483828.17, 1455879.29, 1598046.75, 1687661.62`
- **DuckDB**: `37000.00, 36000.00, 37000.00, 36000.00`
- **SQLite**: `34000.00, 38000.00, 34000.00, 34000.00`
- **PostgreSQL**: `331774.00, 504384.00, 285751.00, 335345.00`
- **ClickHouse**: `5000.00, 5000.00, 4000.00, 5000.00`

## `insert-bulk-1m`

**Workload.** `INSERT 1M rows`
**Dataset.** 1,000,000 i64 PK rows

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | ClickHouse          |      51.00 ms |       4 | MergeTree on disk; --time stderr elapsed; UPDATE/DELETE via ALTER + mutations_sync=2 |
| 2    | DuckDB              |     348.00 ms |       4 | ATTACH-to-file; CHECKPOINT inside the timed region |
| 3    | SQLite              |     464.00 ms |       4 | tempfile; WAL mode; synchronous=NORMAL |
| 4    | PostgreSQL          |        3.79 s |       4 | psql \timing; synchronous_commit=on; fsync=on |
| 5    | UltraSQL (kernel)   |       91.69 s |       2 | heap-access + WAL group-commit; kernel-level (no SQL pipeline) |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `93526231.12, 89848984.08`
- **DuckDB**: `345000.00, 361000.00, 351000.00, 320000.00`
- **SQLite**: `449000.00, 469000.00, 531000.00, 459000.00`
- **PostgreSQL**: `3850087.00, 3667032.00, 3810956.00, 3767150.00`
- **ClickHouse**: `52000.00, 55000.00, 47000.00, 50000.00`

## `update-1m`

**Workload.** `UPDATE t SET val = val + 1`
**Dataset.** 1,000,000 rows preloaded

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | ClickHouse          |      28.00 ms |       4 | MergeTree on disk; --time stderr elapsed; UPDATE/DELETE via ALTER + mutations_sync=2 |
| 2    | DuckDB              |      46.00 ms |       4 | ATTACH-to-file; CHECKPOINT inside the timed region |
| 3    | SQLite              |      63.00 ms |       4 | tempfile; WAL mode; synchronous=NORMAL |
| 4    | PostgreSQL          |        6.57 s |       4 | psql \timing; synchronous_commit=on; fsync=on |
| —    | UltraSQL (kernel)   |     skipped   |    —    | each iteration ~30 min wall-clock at v0.5 (no FSM, O(blocks)/insert × 1M rows + WAL fsync); not runnable inside the 25-min cap |

**Per-iteration data (µs).**

- **DuckDB**: `46000.00, 46000.00, 45000.00, 53000.00`
- **SQLite**: `64000.00, 62000.00, 67000.00, 62000.00`
- **PostgreSQL**: `7214281.00, 5546649.00, 6590572.00, 6552142.00`
- **ClickHouse**: `28000.00, 29000.00, 27000.00, 28000.00`

## `delete-100k`

**Workload.** `DELETE FROM t WHERE val > 0`
**Dataset.** 100,000 rows preloaded; ~50% match

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | SQLite              |       6.50 ms |       4 | tempfile; WAL mode; synchronous=NORMAL |
| 2    | ClickHouse          |       9.00 ms |       4 | MergeTree on disk; --time stderr elapsed; UPDATE/DELETE via ALTER + mutations_sync=2 |
| 3    | PostgreSQL          |      12.21 ms |       4 | psql \timing; synchronous_commit=on; fsync=on |
| 4    | DuckDB              |      35.50 ms |       4 | ATTACH-to-file; CHECKPOINT inside the timed region |
| 5    | UltraSQL (kernel)   |      91.07 ms |       4 | heap-access + WAL group-commit; kernel-level (no SQL pipeline) |

**Per-iteration data (µs).**

- **UltraSQL (kernel)**: `94336.04, 89053.62, 85050.75, 93088.33`
- **DuckDB**: `35000.00, 35000.00, 36000.00, 37000.00`
- **SQLite**: `7000.00, 9000.00, 6000.00, 6000.00`
- **PostgreSQL**: `13770.00, 10644.00, 9934.00, 15372.00`
- **ClickHouse**: `9000.00, 10000.00, 9000.00, 9000.00`

## `upsert-100k`

**Workload.** `INSERT ... ON CONFLICT (id) DO UPDATE SET val = excluded.val`
**Dataset.** 100,000 rows preloaded; ~50% conflict

| Rank | Engine              | Median time   | Samples | Notes |
| ---- | ------------------- | ------------: | ------: | ----- |
| 1    | DuckDB              |       9.00 ms |       4 | ATTACH-to-file; CHECKPOINT inside the timed region |
| 2    | SQLite              |      12.00 ms |       4 | tempfile; WAL mode; synchronous=NORMAL |
| 3    | PostgreSQL          |     264.80 ms |       4 | psql \timing; synchronous_commit=on; fsync=on |
| —    | UltraSQL (kernel)   |     skipped   |    —    | no native ON CONFLICT path at v0.5 |
| —    | ClickHouse          |     skipped   |    —    | no native ON CONFLICT; ReplacingMergeTree changes semantics |

**Per-iteration data (µs).**

- **DuckDB**: `9000.00, 8000.00, 9000.00, 11000.00`
- **SQLite**: `12000.00, 12000.00, 16000.00, 12000.00`
- **PostgreSQL**: `263848.00, 265748.00, 249612.00, 269232.00`

