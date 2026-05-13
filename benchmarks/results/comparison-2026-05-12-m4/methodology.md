# Methodology — Cross-engine SUM(x) comparison (2026-05-12, M4)

## Workload

`SELECT SUM(x) FROM t` where `t` is a single-column 64-bit-integer table
of 65,536 rows. Measure the median wall-clock time of the query in a
hot-cache, single-connection state, after a warmup phase.

The choice of N=65,536 mirrors the largest size in the existing
`vec/sum_i64` microbench
(`crates/ultrasql-vec/benches/kernels.rs`), so the UltraSQL kernel
number is comparable on the same input size.

## Dataset

A deterministic CSV of 65,536 32-bit-signed integers, generated with
Python's `random` module under seed `0xDEADBEEF`:

```python
import random
random.seed(0xDEADBEEF)
print("x")
for _ in range(65536):
    print(random.randrange(-1<<31, 1<<31))
```

The file lives at `/tmp/ultracmp/data.csv` and its SHA-256 is recorded
in `results.json` under `dataset_sha256`. Every engine loaded the same
file. The expected `SUM(x)` is **-213,495,441,761**; every engine
produced this value, which confirms the data was loaded identically.

## Per-engine procedure

Each engine's procedure is implemented in `run.sh` in this directory.
The general shape is:

1. Create a single-column BIGINT table.
2. Load the CSV via the engine's bulk-import facility (`COPY` for
   PostgreSQL/DuckDB, `.import` for SQLite, `INSERT … SELECT
   file()` for ClickHouse).
3. Run an engine-specific warmup phase that drives the query through
   any cold-cache effects (parse cache, page cache, JIT, shared-buffer
   population in PostgreSQL).
4. Run 8 timed iterations of `SELECT SUM(x) FROM t`.
5. Report the median over the 8 timed iterations.

The methodology uses each engine's own timing facility (which is
implemented inside the engine and excludes the harness's startup
cost) and falls back to wall-clock only when none exists. Every engine
in this run had a built-in microsecond-resolution facility usable from
its CLI.

### UltraSQL (kernel)

Number: `4.70 µs`, median of 100 samples.

This is the existing `vec/sum_i64` criterion benchmark at N=65,536,
already recorded in
`benchmarks/results/2026-05-12-m4/results.md` and reproducible with
`cargo bench -p ultrasql-vec --bench kernels`. We did **not** re-run
the bench for this comparison; we cite the committed result.

**This is a kernel measurement, not a SQL pipeline.** It measures only
the cost of the SIMD-friendly fold over a `&[i64]`. UltraSQL does not
yet have a query planner, executor, or wire protocol — that lands at
v0.5 (see `ROADMAP.md`). Treat the 4.70 µs line as a **lower bound**
on what the eventual end-to-end query will achieve.

### DuckDB

Built-in timing facility: `PRAGMA enable_profiling='json'` writes a
JSON file containing `latency` (full query wall-clock time, including
parsing, planning, execution, and result-set materialization) at
nanosecond precision.

Procedure:
1. `CREATE TABLE t (x BIGINT);`
2. `COPY t FROM '/tmp/ultracmp/data.csv' (HEADER, DELIMITER ',');`
3. Two warmup `SELECT SUM(x) FROM t` queries (no timing).
4. Eight timed queries; each produces a separate JSON profile file.
5. Report median of `latency` over the eight files.

`.timer on` was also tried but its resolution is 1 ms, so we used the
JSON profile.

### SQLite

Built-in timing facility: `.timer on` in `sqlite3` CLI emits
`Run Time: real X user Y sys Z` per statement. The `real` clock
on this CLI rounds to 1 ms but `user` and `sys` are reported at
microsecond precision. SQLite's executor is single-threaded, so for a
CPU-bound `SUM` query the `user` time is essentially the wall-clock
time minus scheduler jitter.

Procedure:
1. `CREATE TABLE t (x INTEGER);` (SQLite stores integers in a variable
   8-byte cell; the value range fits in a SQLite INTEGER which behaves
   as a 64-bit signed value).
2. `.mode csv` + `.import --skip 1 /tmp/ultracmp/data.csv t`.
3. Three warmup `SELECT SUM(x) FROM t` queries.
4. Eight timed queries.
5. Report median `user` time. The `real` time is also recorded in the
   raw log but its 1 ms granularity makes it unsuitable for a median.

The database was opened with `:memory:` so storage I/O does not enter
the path.

### PostgreSQL

Built-in timing facility: `\timing on` in `psql` emits
`Time: X ms` after each statement at ms+3-decimal-digit precision (1 µs
effective resolution).

Procedure:
1. `CREATE DATABASE ultracmp; CREATE TABLE t (x BIGINT);`
2. `\COPY t FROM '/tmp/ultracmp/data.csv' WITH (FORMAT csv, HEADER true);`
3. Fifty warmup `SELECT SUM(x) FROM t` queries (PostgreSQL's
   shared-buffer fill and parse-plan cache take ~30 queries to fully
   stabilize on this workload; the first measured query was ~3× slower
   than the median, so a longer warmup is required).
4. Eight timed queries in the same `psql` session (single connection,
   single backend process).
5. Report median.

The `\timing` value includes the round-trip from `psql` to the
backend over a Unix domain socket. Server-side execution time
(via `EXPLAIN ANALYZE`) was ~5.5 ms because of `EXPLAIN ANALYZE`'s
per-node timing instrumentation overhead; we chose the `\timing`
number because it represents what an application using libpq sees
and is comparable in shape to the other engines' end-to-end timings.

The server was started via `brew services start postgresql@14`; the
data directory at `/opt/homebrew/var/postgresql@14` already existed.

### ClickHouse

Built-in timing facility: the `JSON` output format emits a
`statistics.elapsed` field at nanosecond precision per query.

Procedure:
1. `clickhouse local --multiquery` running a script that
   `CREATE TABLE t (x Int64) ENGINE = Memory; INSERT INTO t SELECT *
   FROM file('/tmp/ultracmp/data.csv', 'CSVWithNames', 'x Int64');`.
2. Five warmup `SELECT SUM(x) FROM t` queries.
3. Eight timed queries; the per-query `elapsed` is parsed from the
   JSON output.
4. Report median.

The `clickhouse` binary was obtained from
`https://builds.clickhouse.com/master/macos-aarch64/clickhouse`
because the Homebrew cask was quarantined by macOS Gatekeeper and
shipped as a broken symlink. The downloaded binary is the upstream
official build, recorded as version `26.5.1.587`.

The `Memory` engine keeps the table fully resident in RAM, the
fairest comparison to the other engines' hot-cache state.

### MySQL — skipped

`mysql` was not pre-installed. Installing the Homebrew formula plus
`mysql_secure_installation` and bringing up `mysqld` requires either
sudo or a long-running daemon-init step that exceeded the per-engine
budget for this run. The skip is recorded in `results.json` so a
future contributor can fill the row.

## Hot-cache assumption

All engines were warmed before the measured iterations so that:
- the on-disk pages backing the table (where applicable) are resident
  in the OS page cache or the engine's buffer pool;
- the query plan is cached;
- any JIT is fully compiled (DuckDB and ClickHouse use JIT-style code
  generation; PostgreSQL JIT defaults to off for queries this small);
- the executor's per-operator state is allocated.

This matches the spirit of the `vec/sum_i64` microbench: the input is
a live `Vec<i64>` in a register, no cold-cache effects.

## What is *not* claimed

- These numbers do **not** represent any kind of TPC-H, TPC-DS, or
  industry-standard workload. The workload is one specific reduction
  over one specific input.
- The UltraSQL line measures the SIMD kernel only. Every other line
  measures the engine's full SQL pipeline (parse, plan, dispatch,
  execute, materialize result). The two are not directly comparable;
  see the caveat at the top of `results.md`.
- No assertion is made about engine quality, fitness for production
  workloads, or scaling behavior. This is one row of one table.
- macOS scheduler jitter, `cron`/Spotlight background activity, and
  thermal throttling are not controlled for beyond running on a
  plugged-in Mac mini with no other heavy workload. The medians
  smooth most of this out, but for a publishable claim a longer run
  on a dedicated host would be required.

## Reproduction

Run `bash run.sh` in this directory on the same hardware. The script
re-generates the dataset, runs each engine, and writes new raw output
files under `raw/`. The median computation is done in the same
`python3` block embedded in `run.sh`; the parsing logic matches what
this document describes.
