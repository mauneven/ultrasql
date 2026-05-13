# Cross-engine SUM(x) comparison — 2026-05-12 (Apple M4)

**Workload.** `SELECT SUM(x) FROM t` where `t` has 65,536 BIGINT
rows, hot cache, median of 8 measured iterations after warmup.

**Host.** Apple M4 Mac mini, 10 cores, 16 GiB unified RAM,
internal NVMe. macOS 26.5, Darwin 25.5.0. Plugged in, no other heavy
workload. Reproduce via `bash run.sh` in this directory.

**Dataset.** 65,536 i32 values printed as decimal text in a
single-column CSV. Deterministic seed `0xDEADBEEF`. SHA-256
`579af3856931a209c3b2b43f59ed310232fe02c48eadc6d3a05e326131acd2bd`.
The expected `SUM(x) = -213,495,441,761`; every engine matched.

> **Caveat at the top.** The UltraSQL line measures the kernel in
> isolation because the SQL surface lands at v0.5. Every other line
> measures the engine's full SQL pipeline (parse, plan, dispatch,
> execute, materialize). Treat the UltraSQL line as a *lower bound* on
> what the eventual end-to-end query will achieve, not as a like-for-like
> result.

## Results (sorted fastest → slowest)

| Rank | Engine             | Version                          | Median time  | Samples | Notes                                                                  |
| ---- | ------------------ | -------------------------------- | -----------: | ------: | ---------------------------------------------------------------------- |
| 1    | UltraSQL (kernel)  | 0.0.1                            |     4.70 µs  |     100 | `vec/sum_i64` microbench at N=65,536; **kernel only, no SQL surface**  |
| 2    | DuckDB             | 1.5.2 (Variegata) 8a5851971f     |   216.33 µs  |       8 | `PRAGMA enable_profiling=json`, `latency` field                        |
| 3    | ClickHouse         | 26.5.1.587 (official build)      |   339.27 µs  |       8 | `clickhouse local --format=JSON` `statistics.elapsed`; `Memory` engine |
| 4    | SQLite             | 3.51.0 2025-06-12                | 1 236.50 µs  |       8 | `.timer on` user time; single-threaded, `:memory:` database            |
| 5    | PostgreSQL         | 14.22 (Homebrew)                 | 1 688.50 µs  |       8 | `psql \timing` over Unix socket, 50 warmups before measure             |
| —    | MySQL              | —                                |     skipped  |       — | Not installed; daemon setup exceeded budget. See `methodology.md`.     |

## Per-iteration data

DuckDB latencies (µs):
`472.75, 296.50, 224.75, 230.13, 200.75, 179.58, 207.92, 185.17`

ClickHouse elapsed (µs):
`505.71, 356.63, 351.46, 351.08, 269.79, 289.50, 327.46, 275.17`

SQLite `user` time (µs):
`1233.00, 1240.00, 1224.00, 1241.00, 1223.00, 1247.00, 1225.00, 1262.00`

PostgreSQL `\timing` (µs):
`1820, 1710, 1627, 1644, 1667, 1978, 1591, 1779`

Raw stdout from each engine is in `raw/`.

## What this is and isn't

This is **one row of one comparison table**: a single aggregate over a
single small input on a single host. It is not a competitive workload
claim, not a TPC-H result, and not an endorsement of any engine.
PostgreSQL's number includes psql-libpq round-trip; ClickHouse's number
includes `clickhouse local` per-query overhead; DuckDB's number is the
engine's own `latency` self-report. Each engine is measured by its
fairest self-reported facility (see `methodology.md`).

The UltraSQL line is in a different category: it's a microbenchmark of
the SIMD reduction kernel without any parser, planner, executor, or
result-set materialization. It exists in this table because the kernel
is what the eventual UltraSQL end-to-end query will pay for the
arithmetic, and we want a recorded baseline before v0.5 lands. When
v0.5 ships, this directory should be re-run and the UltraSQL line will
be the engine's measured end-to-end time, not the kernel.
