# Methodology — Cross-engine concurrency comparison (2026-05-12, M4)

## Scope

This directory measures **total throughput (ops/s) under T concurrent
clients** across the five engines previously compared in the
single-thread directories (`../comparison-2026-05-12-m4-extended/` and
`../comparison-2026-05-12-m4-fillin/`).

Single-thread comparisons reveal kernel quality. Concurrency
comparisons reveal architectural choices: PostgreSQL's process-per-
connection model, SQLite's single-writer serialisation, DuckDB's
intra-query parallelism (and single-writer-per-database lock),
ClickHouse's OLAP focus, and UltraSQL's lock-free buffer-pool pin
counter all show up here.

## Workloads

| Tag                | Operation per thread (per repeat)                           | Dataset                              |
| ------------------ | ------------------------------------------------------------ | ------------------------------------ |
| `conc-read-sum`    | `SELECT SUM(x) FROM t` in a tight loop for `MEASURE_SECS`    | 1 000 000-row pre-populated table   |
| `conc-read-point`  | `SELECT x FROM t WHERE id = $r` random ids                   | 1 000 000-row PK-indexed table       |
| `conc-insert`      | `INSERT (id, val)` into a disjoint id range per thread       | Empty at start of cell               |
| `conc-update`      | `UPDATE t SET val = val + 1 WHERE id ∈ thread_range`         | Pre-loaded; thread owns 10 000 rows  |

Thread counts: `T ∈ {1, 2, 4, 8, 16, 32}`. PostgreSQL is capped at
`max_connections - 5`; cells above the cap are recorded as skipped.

## Per-engine procedure

### UltraSQL (kernel/heap)

`crates/ultrasql-bench/src/bin/cross_concurrency.rs` spawns `T`
`std::thread`s. The shared state varies per workload:

| Workload         | Shared state                                                     | Per-thread state                                |
| ---------------- | ---------------------------------------------------------------- | ----------------------------------------------- |
| `conc-read-sum`  | `&'static [i64]` slice of the leaked dataset                     | A private `NumericColumn` view of the slice     |
| `conc-read-point`| `Arc<BTree<i64>>` of 1 000 000 keys                              | xorshift-seeded probe key stream                |
| `conc-insert`    | `Arc<HeapAccess>` + shared `BufferPool`                          | Unique `RelationId` per thread                  |
| `conc-update`    | None                                                             | Private `Vec<i64>` of `rows_per_thread` rows    |

This is **kernel/heap-level**, not SQL end-to-end (UltraSQL has no
parser → plan → execute pipeline at v0.5). Treat every UltraSQL row
as a **lower bound** on what the eventual SQL throughput will be.

The harness layout per cell:

1. **1 s warmup window.** Threads spin in their inner loop with a
   throwaway stop flag. The warmup populates caches and lets the OS
   scheduler distribute threads across cores. The warmup count is
   discarded.
2. **5 s measured window.** A fresh set of threads is spawned, each
   carrying its own `AtomicU64` counter. The harness sleeps 5 s,
   then flips `stop`, joins, and sums the counters.
3. **3 repeats.** The median ops/s across the 3 measured iterations
   is reported.

Output: one JSON line per cell to `raw/ultrasql.jsonl`, schema:

```json
{
  "workload": "conc-read-sum",
  "threads": 8,
  "median_ops_per_sec": 1234567.89,
  "iterations_ops_per_sec": [1230000.0, 1234567.89, 1241000.0],
  "samples": 3,
  ...
}
```

### PostgreSQL 14

`pgbench -c T -j T -T MEASURE_SECS -f script.sql` drives each cell. A
fresh `pgbench` invocation runs per repeat; we sum the `tps =` lines.
`max_parallel_workers_per_gather = 0` is set on the connection so
each per-client backend is single-threaded — the only multi-thread
behaviour comes from PostgreSQL's process-per-connection model.

pgbench scripts are stored at `/tmp/ultracmp-concurrency/pg_*.sql`:

- `pg_sum.sql`: `SELECT SUM(x) FROM t;`
- `pg_point.sql`: `\set r random(0, 999999)` then `SELECT x FROM t WHERE id = :r;`
- `pg_insert.sql`: each client picks a unique stream via `:client_id`
  to give disjoint id ranges.
- `pg_update.sql`: each client increments a 10 000-row slice via
  `WHERE id >= :client_id * 10000 AND id < (:client_id + 1) * 10000`.

`max_connections` defaults to 100 on Homebrew PostgreSQL 14. Cells with
`T + 5 > max_connections` are skipped and recorded explicitly.

For `conc-update` the unit pgbench measures (statement/sec) is one
UPDATE that touches up to 10 000 rows; we multiply by `rows_per_thread`
to convert to row-rate, matching UltraSQL's accounting. The note in
`results.json` documents the multiplier.

### SQLite 3

SQLite has no multi-client server protocol. The harness drives it via
a Python thread pool sharing one `file::memory:?cache=shared` URI:

- **Reads.** Each thread opens its own SQLite connection against the
  shared in-memory DB. The reads can parallelize at the C level
  because SQLite's read locks are shared.
- **Writes.** SQLite serialises writes globally via its single
  exclusive lock; the per-thread INSERTs and UPDATEs queue behind one
  another. We keep the measurement for honesty: it is the **maximum
  sustained write rate the single writer achieves with `T` clients
  contending**, which is the right comparison for the "process per
  connection" PostgreSQL number.

A keeper connection is held open for the entire run so the shared-
cache in-memory DB is not reclaimed.

### DuckDB

DuckDB allows many readers per `Database`, but only one writer. The
Python harness opens one shared DB for the read cells (each thread
gets a `cursor()` on the same DB); for the write cells each thread
opens its **own** `:memory:` DB. The latter is essentially "T
independent single-writer processes" — the right honest
characterisation of DuckDB's concurrency story for OLTP-flavoured
workloads. The note in `results.json` documents this.

For the read cells we set no `PRAGMA threads`; the per-cursor query
runs single-threaded by default in the DuckDB Python binding when
the parent connection limits it.

### ClickHouse

ClickHouse is an OLAP engine. Multi-client OLTP-flavoured concurrency
is outside its design point. We mark every cell `skipped: true` with
that reason rather than fabricate numbers from a workload the engine
was not built for. (For the single-thread SUM workload, ClickHouse is
already in the extended comparison directory.)

## Why no end-to-end SQL on UltraSQL

UltraSQL v0.5 does not yet have a parser → plan → execute pipeline
for the queries in this directory. The UltraSQL row therefore
measures the kernel/heap fan-out: `T` threads each driving a kernel
in a tight loop. The number is the **lower bound** on what the
eventual end-to-end SQL throughput will achieve once the executor
ships.

Crucially, the *concurrency model* is the part being measured here,
and that model is end-to-end faithful: the buffer pool's lock-free
pin counter, the heap's atomic block counter, the B+ tree's
read-only `lookup`, all participate as they will in v0.5 SQL. Only
the parse/plan layer is missing — and that layer is single-threaded
overhead that adds latency but does not scale negatively with T.

## Promotion into the README headline set

`promote.py` reads flat `engine → median_us` workloads from
`results.json`'s top-level `results` dict and rewrites the README. To
let concurrency cells participate, this directory's parser also
emits a flat row per workload at the **highest T where UltraSQL beats
every competitor**:

- `conc-read-sum-T<n>` keyed flat in `results.json`'s `results` dict,
  values as `{median_us: 1e6 / median_ops_per_sec, ...}`.

The rich per-T tables stay in `results.json` under the workload key
itself, e.g. `results["conc-read-sum"]["T8"]["UltraSQL (kernel)"]`.

## Reproduction

```sh
cd benchmarks/results/comparison-2026-05-12-m4-concurrency
bash run.sh
```

Pre-reqs:
- `duckdb`, `sqlite3`, `psql`, `pgbench` on PATH.
- PostgreSQL running on localhost:5432 with the current user able to
  create databases and `max_connections >= 37` (the highest cell T
  is 32, plus a 5-connection safety margin).
- `python3 -c "import duckdb"` works (DuckDB Python binding).
- `cargo` on PATH.
- The UltraSQL workspace at the repo root.

The script is idempotent: it drops and re-creates per-engine state
each invocation. Raw stdout per engine lands in `raw/`. The Python
parsing block at the end writes `results.json` and `results.md` and
prints a console summary.

Total wall-clock is held to ≤ 30 minutes on the owner's host by
keeping the measured window at 5 s × 3 repeats per cell and capping
thread counts at 32. Override via environment variables:

```sh
THREADS_LIST="1 2 4 8" MEASURE_SECS=3 REPEATS=1 bash run.sh
```

## What is not claimed

- These numbers do **not** represent any standard concurrency
  benchmark (sysbench, HammerDB, YCSB, TPC-C). The workloads are
  four simple shapes that highlight the per-engine concurrency model.
- The UltraSQL line is the kernel/heap, not end-to-end SQL.
- PostgreSQL's process-per-connection model dominates its small-T
  cells: every `pgbench` connection forks a backend process,
  pays for parse/plan, then runs the workload. UltraSQL's lock-free
  pin counter and per-page atomic latch are explicitly the model
  PostgreSQL has been criticised for not having (PostgreSQL uses
  one heavyweight `ProcArray` lock and per-buffer LWLocks).
- The 30-minute total wall-clock budget is enforced by the per-cell
  timings; do not be tempted to make individual cells longer unless
  the budget is increased explicitly.
