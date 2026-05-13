# Point-lookup cross-engine comparison — 2026-05-12 (Apple M4)

**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM, internal NVMe.
macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy workload.
Reproduce via `bash run.sh` in this directory.

**Workload.** `SELECT x FROM t WHERE id = ?` over a 10,000,000-row
`(id BIGINT PRIMARY KEY, x BIGINT)` table. Same deterministic-seed
CSV and same 100,000-probe set across every engine.

**Methodology (fair shape).** Each engine builds the table, opens a
hot session, prepares the statement, runs 10,000 throwaway warmup
probes, then runs measurement runs of **100,000 probes** (3 runs for
every engine except ClickHouse, which is capped at 1 run; see
`methodology.md`). We report the median nanoseconds-per-probe across
the runs and the total wall time of the median run. Setup and warmup
are explicitly excluded from the timed region.

**Why this comparison exists.** The prior
`comparison-2026-05-12-m4-extended/point-10m` row reported the
per-iteration wall time of a 10,000-probe **batch** (~6.78 ms) and
treated it as per-probe in some prose. That was off by a factor of
10,000. This directory measures probes-only, in a hot session,
across all five engines — the methodology every other row of the
extended comparison already uses for non-batched workloads.

**Dataset sha256.**

```
3e249eae4d627a81e3aba6cbf869e47d2385561965b3e1c621fc3b2eee207e39  data_id_10m.csv
228d6fef1f85ddd1249ace51f4c6bafc341e27963769279c67d3b3db2cc41879  probes_100k.txt
```

## `point-10m-probes`

| Rank | Engine                              | Median per probe   | Total wall (100k probes) | Runs | Notes |
| ---- | ----------------------------------- | -----------------: | -----------------------: | ---: | ----- |
| 1    | SQLite                              |           1.10 µs |               110.04 ms |   3 | python3 sqlite3 :memory:; INTEGER PRIMARY KEY (rowid index) |
| 2    | UltraSQL (kernel, BTree<i64>)       |           2.64 µs |               263.95 ms |   3 | None |
| 3    | PostgreSQL                          |          35.76 µs |                  3.58 s |   3 | python3 psycopg 3; same connection; server-side prepared statement; Unix socket |
| 4    | DuckDB                              |          50.75 µs |                  5.07 s |   3 | python3 duckdb 1.5.2 in-process; threads=1; PK auto-index |
| 5    | ClickHouse                          |          10.30 ms |               1030.13 s |   1 | clickhouse local; MergeTree PK on id; 100k literal SELECTs per run; capped at 1 run (per-engine target overage; OLTP unfit, see methodology.md) |

### Per-run distribution

Each engine ran N runs of 100,000 probes (3 for everyone except
ClickHouse, which is capped at 1). The list below shows
`total_wall_ns_per_run / 100,000` for each run.

- **UltraSQL (kernel, BTree<i64>)**: `[2.99 µs, 2.21 µs, 2.64 µs]`
- **SQLite**: `[1.10 µs, 1.12 µs, 1.10 µs]`
- **DuckDB**: `[53.19 µs, 50.75 µs, 49.82 µs]`
- **PostgreSQL**: `[35.76 µs, 30.18 µs, 45.08 µs]`
- **ClickHouse**: `[10.30 ms]`

## Reading this table

Every engine paid for its own session setup once (table load, index
build, prepared statement, 10,000-probe warmup). Only the probe
loop is timed. The UltraSQL row uses the v0.5 `BTree<i64>` directly
via the native Rust API — there is no SQL pipeline yet, and there
is no client/IPC overhead in the timed region; treat it as a lower
bound on what the eventual end-to-end query will achieve. SQLite,
DuckDB and PostgreSQL are measured through their Python bindings;
the constant per-call binding overhead is included (a few hundred
ns) and is the same shape a real Python or psycopg client would
pay. ClickHouse is measured through `clickhouse local`, which pays
full per-query startup cost on each lookup — ClickHouse is not
designed for OLTP point-lookup workloads and we document the
result as a loss for that engine.
