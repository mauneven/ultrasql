# Methodology — Cross-engine **write** comparison (2026-05-12, M4)

## Scope

Companion to the read comparisons in
`../comparison-2026-05-12-m4-extended/` and `../comparison-2026-05-12-m4-fillin/`.
Same five engines, same host (Apple M4 Mac mini, 16 GiB, macOS 26.5),
same deterministic seed pattern. This directory measures the write
side of the workload matrix:

| Tag                 | Operation                                                                              | Dataset / scale          |
| ------------------- | -------------------------------------------------------------------------------------- | ------------------------ |
| `insert-bulk-100k`  | `INSERT INTO t (id, val) VALUES (...)` over 100,000 rows into an empty relation        | 100,000 i64 PK rows      |
| `insert-bulk-1m`    | `INSERT INTO t (id, val) VALUES (...)` over 1,000,000 rows into an empty relation      | 1,000,000 i64 PK rows    |
| `update-1m`         | `UPDATE t SET val = val + 1` over a 1,000,000-row preloaded table                      | 1,000,000 rows preloaded |
| `delete-100k`       | `DELETE FROM t WHERE val > 0` over a 100,000-row preloaded table (~half match)         | 100,000 rows preloaded   |
| `upsert-100k`       | `INSERT ... ON CONFLICT (id) DO UPDATE SET val = excluded.val` over 100,000 rows       | 100,000 rows preloaded; half conflict |

Every engine runs every workload **in its normal durable mode** —
this is a write benchmark, so cheap-but-non-durable settings would
mislead. The exact knobs per engine are spelled out below.

## Durability

| Engine     | Durability knobs                                                                                                                          |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| UltraSQL   | `WalWriter` group-commit fsync (`F_FULLFSYNC` on macOS) + explicit `SegmentFileManager::fsync_relation` after every measured workload     |
| DuckDB     | `ATTACH '/tmp/.../duck.db'`; `CHECKPOINT` at end of workload so the WAL hits storage; default durability otherwise                        |
| SQLite     | tempfile (not `:memory:`); `PRAGMA journal_mode = WAL`; `PRAGMA synchronous = NORMAL` (the default)                                       |
| PostgreSQL | `synchronous_commit = on` (default), `fsync = on` (default), `full_page_writes = on` (default); single backend, single Unix-socket session |
| ClickHouse | `MergeTree` engine on disk (the production write target) — not `Memory`, which would not be durable                                       |

## Per-engine procedure

`run.sh` invokes one Python generation block + five engine blocks +
one parser block. Every engine block writes its raw stdout to
`raw/<engine>.out` (or `raw/ultrasql.jsonl` for UltraSQL).

### UltraSQL — heap-access-method via `cross_compare_writes`

A new binary,
`crates/ultrasql-bench/src/bin/cross_compare_writes.rs`, takes
`--workload <name>` and `--data <csv>` and emits a single JSON line to
stdout describing the median, min, and per-iteration distribution of
the measured iterations after a warmup. Wall-clock is
`std::time::Instant`, nanosecond resolution.

Each iteration brings up a fresh tempdir hosting:

1. `SegmentFileManager` (8 KiB pages, mmap on macOS, segment cap 1 GiB)
2. `BufferPool` over the segment manager (sized large enough to hold
   every dirty page — the v0.5 pool refuses to evict dirty frames)
3. `HeapAccess` on top of the buffer pool (the
   `(id i64, val i64)` tuple is encoded as a 16-byte payload behind
   the 40-byte MVCC header — 56 bytes/tuple, ~130 tuples per 8 KiB page)
4. `WalBuffer` (128 MiB capacity) + `WalWriter` with the production
   default config (16 MiB segments, 200 µs group-commit window,
   256 KiB batch threshold)

For preloaded workloads (`update`, `delete`), the preload happens
outside the timed region and is followed by a `barrier` step that
fsyncs the segment files, fsyncs the WAL, and brings up a fresh WAL
writer for the timed body. This puts the preload's durability cost
where it belongs (untimed) — the same shape PostgreSQL sees when
`COPY` completes before `UPDATE` starts.

Per-workload mapping:

| Tag                 | Heap-access composition                                                                  |
| ------------------- | ---------------------------------------------------------------------------------------- |
| `insert-bulk-100k`  | 100k × `HeapAccess::insert` + final segment fsync + final WAL fsync (`WalWriter::shutdown`) |
| `insert-bulk-1m`    | 1M × `HeapAccess::insert` + final segment fsync + final WAL fsync                          |
| `update-1m`         | scan to collect tids → per row: fetch + `delete` + `insert` (val+1) → final fsyncs       |
| `delete-100k`       | scan with `val > 0` predicate → per match: `delete` → final fsyncs                       |
| `upsert-100k`       | **skipped** — no native ON CONFLICT path at v0.5                                          |

Every UltraSQL insert appends a `RecordType::HeapInsert` WAL record
with the payload bytes before issuing the heap call; every delete
appends a `RecordType::HeapDelete` record with the tid bytes. The
WAL writer drains the buffer continuously and flushes per the
group-commit window; the explicit `shutdown` at end of body forces
the final fsync.

### UltraSQL caveats

#### Caveat 1: O(N) insert path

`HeapAccess::insert` in v0.5 walks every existing block of the
relation looking for a page with room, *then* extends. With ~130
tuples/page and 1 M target rows, the relation grows to ~7 500 pages;
the last insert pays a 7 500-page walk to find that no existing page
has room and extend. The aggregate cost is `O(N²)` in the row count,
which is the v0.5 reality. Production-grade UltraSQL will track
free-space via the per-page header's `pd_free` byte (already present)
and a per-relation FSM — that's part of the v0.6 storage agenda.
PostgreSQL's per-tuple cost is `O(1)` thanks to its FSM; ClickHouse,
DuckDB, and SQLite all have bulk-load fast paths. **The bench is
honest about this cost** — we measure end-to-end including the
quadratic walk. If UltraSQL loses, `promote.py` will drop the row
from the README.

#### Caveat 2: skipped upsert

The heap has no `ON CONFLICT (id) DO UPDATE` path because v0.5 has no
unique index over user data — `BTree<i64>` exists but is not yet
wired to a user-defined `UNIQUE` constraint. Rather than fabricate
"lookup-via-scan + decide + insert/delete" composition that would
misrepresent the eventual planner-driven `ExecutorTuple::upsert`
path, the row is marked **skipped** with the reason in
`results.json`.

#### Caveat 3: iteration count and skipped workloads

The 25-minute wall-clock cap from the project benchmark policy (see
[BENCHMARKS.md](../../../BENCHMARKS.md)) combined with the v0.5
HeapAccess::insert cost (O(blocks)/insert — no FSM, so a 1M-row table
pays a ~7 500-block walk on every insert) forced a tighter iteration
schedule than the standard 8-measured-iter cadence:

| Workload            | UltraSQL iters | SQL iters | Rationale                                                                                                                                                  |
| ------------------- | -------------: | --------: | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `insert-bulk-100k`  | 4              | 4         | ~3 s/iter; reduced from 8 to fit overall budget; still gives a stable median                                                                               |
| `insert-bulk-1m`    | 2              | 4         | UltraSQL ~80 s/iter; below the 4-iter floor to keep total wall-clock under 25 min                                                                          |
| `update-1m`         | **skipped**    | 4         | A single UltraSQL iteration is ~30 min wall-clock (1M-row preload + 1M-row delete-then-insert with the O(blocks) walk paid on every re-insert); not runnable inside the 25-min cap. SQL engines still measure it. |
| `delete-100k`       | 4              | 4         | ~80 ms/iter; reduced from 8 to fit overall budget                                                                                                          |
| `upsert-100k`       | **skipped**    | 4         | No native ON CONFLICT path at v0.5 (planned for v0.7 — see ROADMAP.md). SQL engines still measure it.                                                       |

Every deviation is recorded under `iter_count_deviations` in
`results.json`. The per-workload `samples` field in each table is the
actual number of measured iterations.

### DuckDB

`PRAGMA enable_profiling='json'` writes a per-query JSON profile
containing the full query latency (parse + plan + execute) at
nanosecond precision. We set `PRAGMA threads=1` so DuckDB executes
single-threaded — matching the single-thread budget of the other
engines and removing the multi-core fan-out as a confounder. The
database is attached as `/tmp/ultracmp-writes/duck.db` (not
in-memory) so that DuckDB's WAL hits storage and `CHECKPOINT` after
each workload exercises the durability path.

A single warmup pass precedes the measured pass.

### SQLite

`.timer on` emits `Run Time: real X user Y sys Z` after each
statement. We use the `user` time (microsecond resolution; the `real`
clock rounds to 1 ms). SQLite's executor is single-threaded so for a
CPU-bound query `user ≈ wall`.

For this comparison the database is a **tempfile** with `PRAGMA
journal_mode = WAL` and `PRAGMA synchronous = NORMAL` (the default).
`PRAGMA temp_store = MEMORY` keeps SQLite's transient sort tapes off
disk, which is its usual production setting. We deliberately do **not**
use `PRAGMA synchronous = OFF` here — that would give SQLite a free
ride on durability against engines that fsync.

Each workload runs 3 warmup queries + 8 measured queries.

### PostgreSQL

Same instance the read comparisons use (`brew services start
postgresql@14`). `\timing on` emits `Time: X ms` after each
statement at microsecond-effective resolution.
`synchronous_commit = on`, `fsync = on`, `full_page_writes = on` —
all the defaults.

Each workload runs in the same `psql` session (single connection,
single backend). 3 warmup queries + 8 measured queries per workload.

For the `update-1m` workload the table also has an `id` BIGINT
PRIMARY KEY (auto-creating a btree). For `upsert-100k` we use the
classic `INSERT ... ON CONFLICT (id) DO UPDATE SET val = excluded.val`
spelling.

### ClickHouse

`clickhouse local` is invoked once with a multi-query script. Tables
use the `MergeTree(ORDER BY id)` engine on disk — not `Memory` —
because this is a durability comparison; an in-memory table would
give ClickHouse a free ride. ClickHouse is **not** designed for OLTP
single-row writes; we expect it to lose by a wide margin on the
update and delete rows, and we keep those rows rather than skip them
so the operating curve is visible.

For each workload, 3 warmup queries + 8 measured queries; the
parser splits the `statistics.elapsed` stream into 11-wide chunks in
the order the workloads were emitted.

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

## What is *not* claimed

- These are not TPC-C / TPC-B / industry-standard write workloads.
  Five specific operations on small synthetic data.
- The UltraSQL row measures the heap access method and WAL writer in
  isolation, with no SQL surface above them. Every other engine
  measures its full SQL pipeline. The comparison is therefore biased
  *in UltraSQL's favor* (it skips the parser/planner) and *against*
  UltraSQL's storage layer (the v0.5 heap has no FSM, no
  index-organized fast path). The two effects partially cancel; read
  every row as a research data point, not a marketing number.
- No assertion is made about engine quality, fitness for production
  workloads, or scaling behavior. One host × one workload set, run
  once.

## Reproduction

```sh
cd benchmarks/results/comparison-2026-05-12-m4-writes
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
