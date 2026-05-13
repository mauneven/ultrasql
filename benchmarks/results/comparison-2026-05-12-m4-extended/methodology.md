# Methodology — Extended cross-engine comparison (2026-05-12, M4)

## Scope

Extends the existing single-workload `SUM(x) @ 65,536 rows`
comparison (`benchmarks/results/comparison-2026-05-12-m4/`) to:

| Tag           | Query                                                    | Dataset                            |
| ------------- | -------------------------------------------------------- | ---------------------------------- |
| `sum-1m`      | `SELECT SUM(x) FROM t`                                   | 1,000,000 i64 rows                 |
| `sum-10m`     | `SELECT SUM(x) FROM t`                                   | 10,000,000 i64 rows                |
| `count-10m`   | `SELECT COUNT(*) FROM t`                                 | 10,000,000 i64 rows                |
| `minmax-10m`  | `SELECT MIN(x), MAX(x) FROM t`                           | 10,000,000 i64 rows                |
| `avg-10m`     | `SELECT AVG(x) FROM t`                                   | 10,000,000 i64 rows                |
| `filter-10m`  | `SELECT SUM(x) FROM t WHERE y > 0`                       | 10,000,000 (i64 x, i64 y)          |
| `range-10m`   | `SELECT COUNT(*) FROM t WHERE x BETWEEN -1e9 AND 1e9`    | 10,000,000 i64 rows                |
| `point-10m`   | `SELECT x FROM t WHERE id = ?`                           | 10,000,000-row indexed table       |
| `topk-1m`     | `SELECT x FROM t ORDER BY x LIMIT 10`                    | 1,000,000 i64 rows                 |
| `topk-10m`    | `SELECT x FROM t ORDER BY x LIMIT 10`                    | 10,000,000 i64 rows                |

Every engine runs every workload after warmup; the median of 8
measured iterations is reported.

## Datasets

A single deterministic generation step produces five CSV files in
`/tmp/ultracmp`. The seeds are recorded in `run.sh`; the SHA-256 of
every file is recorded in `results.json` and printed by `run.sh` at
startup. Every engine loads from the same byte stream.

| File              | Shape                  | Rows       | Seed         |
| ----------------- | ---------------------- | ---------- | ------------ |
| `data_x_1m.csv`   | `x` (i64)              | 1,000,000  | `0xDEADBEEF` (first 1M of the 10M stream) |
| `data_x_10m.csv`  | `x` (i64)              | 10,000,000 | `0xDEADBEEF` |
| `data_y_10m.csv`  | `y` (i64)              | 10,000,000 | `0xBADC0DE`  |
| `data_xy_10m.csv` | `x, y` (i64, i64)      | 10,000,000 | merged from x and y above |
| `data_id_10m.csv` | `id, x` (i64 PK, i64)  | 10,000,000 | `id` is a shuffled permutation of `0..N` (seed `0xC0FFEE`), `x` is the same x stream as above |

Integer range is `[-2^31, 2^31)` (so the row values fit in a 32-bit
signed range but every engine declares the column as `BIGINT` /
`Int64` so the aggregate types are i64-shaped).

## Caveat at the top of every results table

The UltraSQL line in every results table measures the relevant vec
kernel **in isolation**. UltraSQL has no SQL surface end-to-end at
v0.5 — no parser → plan → execute pipeline for aggregates yet (see
`ROADMAP.md`). The kernel measurement is a **lower bound** on what
the eventual end-to-end query will achieve, not a like-for-like
result. The other engines run their full SQL pipeline (parse, plan,
dispatch, execute, materialize). Read every UltraSQL row in that
light.

## Per-engine procedure

`run.sh` invokes one Python generation block + five engine blocks +
one parser block. Every engine block writes its raw stdout to
`raw/<engine>.out` (or `raw/ultrasql.jsonl` for UltraSQL).

### UltraSQL — kernel-level via `cross_compare`

A new binary, `crates/ultrasql-bench/src/bin/cross_compare.rs`, takes
`--workload <name>` and `--data <csv>` and emits a single JSON line to
stdout describing the median, min, and per-iteration distribution of
8 measured iterations after 3 warmups. Wall-clock is
`std::time::Instant`, nanosecond resolution.

Per-workload kernel choice:

| Tag           | Kernel composition                                             |
| ------------- | -------------------------------------------------------------- |
| `sum-1m`      | `vec::sum_i64` over the column                                 |
| `sum-10m`     | `vec::sum_i64` over the column                                 |
| `count-10m`   | `vec::count_i64` — O(1) because no NULL bitmap is present      |
| `minmax-10m`  | `vec::min_i64` + `vec::max_i64` in a single timed iteration    |
| `avg-10m`     | `vec::sum_i64` / `vec::count_i64` (integer division)           |
| `filter-10m`  | `vec::cmp_gt_i64(y, 0)` → Bitmap → `vec::sum_i64_with_mask(x)` |
| `range-10m`   | `vec::range_mask_i64(x, -1e9, 1e9)` → `Bitmap::count_ones()`   |
| `point-10m`   | `BTree<i64>::lookup` over a freshly-built B+ tree              |
| `topk-1m`     | **skipped** — no ORDER BY kernel in vec yet                    |
| `topk-10m`    | **skipped** — no ORDER BY kernel in vec yet                    |

The new kernels (`count_i64`, `min_i64`, `max_i64`, `cmp_gt_i64`,
`sum_i64_with_mask`, `range_mask_i64`) live in
`crates/ultrasql-vec/src/kernels.rs`, are re-exported from
`crates/ultrasql-vec/src/lib.rs`, and ship with unit tests including
spot-checks against a naive scalar reference.

#### UltraSQL caveat 1: `count-10m` is O(1)

The vec `count_i64` kernel returns either `column.len()` (for a
non-null column) or `Bitmap::count_ones()` (for a nullable column).
With our non-nullable input the operation is constant-time and the
measured value rounds to 0 µs. We report this row as the actual
measurement (`< 1 µs`) and call it out in the results.md note rather
than fabricating a synthetic number that would imply the kernel walks
the column. SQL engines do walk the storage, hence their numbers are
meaningfully higher.

#### UltraSQL caveat 2: point-lookup is at 1M, not 10M

The v0.5 buffer pool refuses to evict dirty pages — the storage
manager owns flushing, which is not yet wired up. With a 32-entry
leaf and a 16-entry internal node (see `btree.rs::MAX_LEAF_ENTRIES`),
a 10M-key B-tree would dirty ~330k pages = ~2.5 GiB resident, which
exceeds the harness's frame budget. The harness builds a 1M-key
B-tree (≈ 31k leaves + small internals ≈ 250 MiB resident) and runs
10,000 random probes per timed iteration. The reported median is the
**per-iteration** value; the `ns_per_lookup` field in the raw JSON
gives the per-probe cost.

#### UltraSQL caveat 3: top-K is skipped

`vec` has no `ORDER BY` / Top-K kernel at v0.5. Rather than fabricate
a number from `Vec::sort` (which would not be representative of the
eventual kernel), this row is marked **skipped** with the reason in
`results.json`.

### DuckDB

`PRAGMA enable_profiling='json'` writes a per-query JSON profile
containing the full query latency (parse + plan + execute) at
nanosecond precision. We set `PRAGMA threads=1` so DuckDB executes
single-threaded — matching the single-thread budget of the other
engines and removing the multi-core fan-out as a confounder. The
profile is written to a per-workload, per-iteration file under
`/tmp/ultracmp/duck_<tag>_<i>.json`; we parse the eight latency
values and report their median.

A single warmup pass (one query per type) precedes the measured pass
because DuckDB's parser/optimizer cache stabilizes after the first
hit.

### SQLite

`.timer on` emits `Run Time: real X user Y sys Z` after each
statement. We use the `user` time (microsecond resolution; the `real`
clock rounds to 1 ms). SQLite's executor is single-threaded so for a
CPU-bound query `user ≈ wall`.

The database is opened with `:memory:`, `PRAGMA journal_mode=MEMORY`,
`PRAGMA synchronous=OFF`, and `PRAGMA temp_store=MEMORY` so storage
I/O does not enter the path. Each workload runs 3 warmup queries +
8 measured queries; the boundary is marked with a `.print
--measure-<tag>--` line so the parser knows which timings to skip.

For `point-10m`, the table has `id INTEGER PRIMARY KEY`, which makes
the primary key the rowid and a covering index — SQLite's reported
latency is essentially the B-tree descent cost.

### PostgreSQL

`\timing on` emits `Time: X ms` after each statement at
microsecond-effective resolution. We set
`max_parallel_workers_per_gather = 0` so PostgreSQL executes
single-threaded.

Each workload runs in the same `psql` session (single connection,
single backend process). The session emits `\echo -- BEGIN <tag>` /
`\echo -- MEASURE <tag>` / `\echo -- END <tag>` markers so the parser
can isolate per-workload timings. **10 warmup queries** precede the
8 measured queries — fewer than the 50 used in the original
comparison because the same session shares its shared-buffer cache
and parse-plan cache across workloads, so the cold-cache amortization
happens once for the first workload.

For `point-10m`, the table has `id BIGINT PRIMARY KEY`, which
auto-creates a B-tree index. PostgreSQL's `\timing` includes the
round-trip from `psql` to the backend over a Unix domain socket.

### ClickHouse

The `JSON` output format writes one `statistics.elapsed` field per
query at nanosecond precision. We run `clickhouse local --multiquery`
with a script that creates each table with `ENGINE = Memory` (so the
table is fully RAM-resident, the fairest comparison to the other
engines' hot-cache state).

Each workload runs 3 warmup queries + 8 measured queries. The parser
splits the concatenated `elapsed` values into 11-wide chunks in the
order the workloads were emitted; the last 8 of each chunk are the
measured iterations.

ClickHouse's `Memory` engine has no primary-key index; the
`point-10m` row therefore runs a full scan on ClickHouse, which is
why ClickHouse is several orders of magnitude slower than the other
SQL engines on that row. We keep the row in the table rather than
mark it skipped because the engine *did* execute the query —
ClickHouse just doesn't optimize for the access pattern in the
`Memory` engine.

## What every column means

In `results.json` each engine entry has either:

```json
{
  "median_us":     <number>,
  "min_us":        <number>,
  "samples":       <int>,
  "iterations_us": [...],
  "note":          "..."          // optional, engine-specific caveat
}
```

…or, when an engine could not run a workload:

```json
{
  "skipped": true,
  "reason":  "human-readable explanation"
}
```

`iterations_us` is the full per-iteration distribution before
median-aggregation, in microseconds, in the order the engine emitted
them.

## Hot-cache assumption

All engines were warmed before the measured iterations so that:

- the on-disk pages backing the table (where applicable) are
  resident in the OS page cache or the engine's buffer pool;
- the query plan is cached;
- any JIT is fully compiled (DuckDB and ClickHouse use JIT-style
  code generation; PostgreSQL JIT defaults to off for queries this
  small);
- the executor's per-operator state is allocated.

This matches the spirit of the existing `vec/sum_i64` microbench:
the input is a live `Vec<i64>` in a register, no cold-cache effects.

## What is *not* claimed

- These numbers do **not** represent any kind of TPC-H, TPC-DS, or
  industry-standard workload. The workloads are nine specific
  reductions / scans over deterministic synthetic data.
- The UltraSQL lines measure SIMD kernels (plus the B+ tree for the
  point lookup). Every other line measures the engine's full SQL
  pipeline. The two are not directly comparable; the caveat
  reproduces at the top of every `results.md` table.
- No assertion is made about engine quality, fitness for production
  workloads, or scaling behavior. This is one host × one set of
  workloads, run once.
- macOS scheduler jitter, `cron`/Spotlight background activity, and
  thermal throttling are not controlled for beyond running on a
  plugged-in Mac mini with no other heavy workload. The medians
  smooth most of this out, but for a publishable claim a longer run
  on a dedicated host would be required.

## Reproduction

```sh
cd benchmarks/results/comparison-2026-05-12-m4-extended
bash run.sh
```

Pre-reqs:
- `duckdb`, `sqlite3`, `psql` on PATH.
- `postgres` running on localhost:5432 with the current user able to
  create databases.
- The official ClickHouse binary at `/tmp/ultracmp/clickhouse`, or
  point `CH_BIN` at one.
- The UltraSQL workspace builds in release.

The script is idempotent: it drops and re-creates per-engine state
each invocation. Raw stdout per engine lands in `raw/`. The Python
parsing block at the end writes `results.json` and prints a console
summary.
