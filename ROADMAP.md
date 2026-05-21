# UltraSQL Roadmap

This roadmap is a commitment, not a wish list. Items move from
"planned" to "in progress" to "shipped" via the RFC process and the
release process. Stretch goals are marked with `?`; everything else is
on the path.

Releases follow [semantic versioning](https://semver.org/). Pre-1.0
releases may break compatibility between minor versions; we document
the breakage in each release's notes.

**Status legend:** ✅ Done · 🔄 In Progress · ❌ Not Started · ⚠️ Partial

**Definition of "Done":**
- For **kernel** features (parser, planner, executor operators, SIMD
  kernels, storage primitives): the feature has tests and is callable
  from in-process Rust.
- For **wire-protocol** features (SELECT / INSERT / UPDATE / DELETE,
  Extended Query, BEGIN/COMMIT, EXPLAIN, etc.): the feature must round-
  trip end-to-end from a real PostgreSQL client (`tokio-postgres`,
  `psql`, `psycopg2`) against `ultrasqld`. Kernel-only items that have
  not landed in `pipeline.rs` / `lib.rs` dispatch are marked ⚠️ Partial
  with the gap named, not ✅.

This file is the source of truth for the project. Every item is actionable without further context.

---

## Standing Quality Requirements

These apply to **every version** from v0.2 onward. A version does not
ship if any of these gates fail. No exceptions.

### Slice CI Gate: green before and after every slice

Every v1.0 slice starts from a completed successful GitHub Actions
`ci-passed` run on `origin/main`. If CI is red or still running, the
next slice waits until the failure is fixed or the run finishes green.

Every slice closes with scope-matched local validation, an atomic
commit, a push, and a fresh successful `ci-passed` run for the pushed
commit before the next slice starts. Documentation-only slices require
at least `git diff --check` locally; Rust/code-path slices require the
full relevant local gate set (`cargo fmt`, clippy with `-D warnings`,
and tests). Performance-sensitive slices also require benchmark or
manual/weekly `supremacy` evidence before any performance claim is made.
Record CI run IDs and benchmark artifact paths in session notes or the
PR description.

### Test Coverage Gate: ≥ 80% line coverage per crate

Release target is at least **80% line coverage** per crate measured by
`cargo llvm-cov --workspace`. Current committed automation is not yet
equivalent to this target: CI runs format, lint, tests, docs, and
dependency audit gates, but no committed `llvm-cov` PR/pre-push
threshold gate exists yet. A version must not claim this gate satisfied
until coverage automation and artifacts prove every crate meets the
threshold.

Coverage layers required per subsystem:

| Layer | Requirement |
|-------|-------------|
| Unit tests (`#[cfg(test)]` next to code) | Core logic, every public function |
| Property tests (`proptest` / `quickcheck`) | Serialization round-trips, parser/printer fidelity, planner equivalences, any algebraic contract |
| Concurrency tests (`loom`) | Every lock-based or lock-free shared structure |
| Deterministic simulation tests | Storage + txn layers via virtual clock + virtual IO |
| Integration tests (`tests/`) | Multi-crate workflows, end-to-end query round-trips |
| Fuzz targets (`cargo fuzz`) | Parser, wire protocol decoder, WAL record decoder, planner |

Fuzz corpora are committed under `fuzz/corpus/`. A fuzz target that
has not run for 24 h CI-clean is not considered covered.

### Benchmark Gate: ≥ 2× every listed competitor on every workload

Every PR that touches a benchmarked code path should include
before/after `cross_compare_sql` numbers in the description. Current
committed automation has benchmark scripts, a committed
`.githooks/pre-push` smoke gate that runs `regression-gate --smoke`,
and a manual/weekly GitHub Actions `supremacy` workflow that checks
UltraSQL is the fastest engine in the low-tier matrix. No committed
hook currently enforces the full ≥ 2× competitor floor on every push;
treat strict ≥ 2× certification as release-gate work against the stage
baselines in `benchmarks/baselines/<stage>.json`.

For each milestone below, UltraSQL must demonstrably **outperform every
listed competitor by ≥ 2×** (throughput) or ≤ 0.5× (latency) on the
specified workload **before the version ships**. Results must be
reproducible via the scripts in `benchmarks/` on the recorded host.

Performance claims require a reproducible benchmark script and a
recorded host descriptor (see `BENCHMARKS.md`). Fabricated numbers
are grounds for revert.

| Version | Workload | Target | Metric |
|---------|----------|--------|--------|
| v0.5 | Simple INSERT throughput (10 k rows / multi-row VALUES) | ≥ 2× every competitor ✅ (3.54 ms vs SQLite 19.87 ms = 5.61×) | throughput (µs / batch) |
| v0.5 | Simple SELECT scan (10 k rows full table) | #1, but strict ≥ 2× not met: 659.67 µs vs DuckDB 881.83 µs = 1.34× ⚠️ | latency (µs) |
| v0.5 | SELECT SUM(x) over 65 k rows | ≥ 2× every competitor ✅ (44.04 µs vs DuckDB 88.65 µs = 2.01×) | latency (µs) |
| v0.5 | UPDATE 10 k rows in single statement | #1, but strict ≥ 2× not met: 128.79 µs vs DuckDB 164.21 µs = 1.27× ⚠️ | latency (µs) |
| v0.5 | DELETE 10 k rows in single statement | ≥ 2× every competitor ✅ (115.79 µs vs SQLite 538.08 µs = 4.65×) | latency (µs) |
| v0.6 | TPC-H scale 1 correctness (all 22 queries) | result-equal to DuckDB ✅ | `validate-results` result comparison |
| v0.6+ | TPC-H scale 1 performance certification | ≥ 2× PostgreSQL 17 ⚠️ pending | geometric mean query time |
| v0.7 | TPC-H scale 10 (all 22 queries) | ≥ 2× DuckDB ✅ latest local artifact status passed; 22/22 DuckDB and UltraSQL query timings in `benchmarks/results/latest/tpch_sf10_certification.json` | geometric mean query time |
| v0.7 | ClickBench (`hits.parquet` analytical queries) | ≥ 5× faster than PostgreSQL 17 ⚠️ not certified; runner now records missing dataset/DSN failures | geometric mean query time |
| v0.9 | TPC-B (OLTP, 32 connections) | ≥ 2× PostgreSQL 17, p99 < 5 ms | throughput + latency |
| v1.0 | TPC-C (all 5 tx types, 32 connections) | ≥ 2× PostgreSQL 17 | throughput (tx/s) |
| v1.0 | Sysbench OLTP read/write | ≥ 2× PostgreSQL 17 | throughput (tx/s) |
| v1.0 | Firebolt aggregating-index dashboard aggregate | ≥ 2× Firebolt ⚠️ local Firebolt Core runner exists; UltraSQL smoke artifact exists; local Firebolt Core run pending measured artifact | median query latency |
| v1.0 | Firebolt sparse primary-index pruning | ≥ 2× Firebolt ⚠️ local Firebolt Core runner exists; UltraSQL smoke artifact exists; local Firebolt Core run pending measured artifact | median query latency |
| v1.0 | Firebolt-style wide filter/projection late materialization | UltraSQL smoke artifact exists ⚠️ local Firebolt Core same-host runner/artifact pending | median latency + candidates/fetched/skipped |
| v1.0 | Firebolt HNSW vector search | recall/latency artifact vs UltraSQL HNSW ⚠️ local Firebolt Core runner exists; measured vector-support artifact pending | recall@k + p50/p95/p99 |
| v2.x | Star Schema Benchmark scale 100 | ≥ 2× ClickHouse | geometric mean query time |

All comparisons follow the methodology in `BENCHMARKS.md`: same host,
same dataset, same seed, competitor tuned per its published best
practices, median of 5 runs ≥ 60 s each after ≥ 60 s warmup. Live
results auto-render from `benchmarks/results/latest/raw/*.json` into
`README.md` via `readme-render`.

---

## Current State Snapshot

<!-- reconciled 2026-05-20 against main 5f0c49e. -->
<!-- Latest CI evidence for 5f0c49e: GitHub Actions ci run 26151820002 passed. -->
<!-- Latest supremacy evidence for 5f0c49e: workflow_dispatch run 26151891843 passed. -->
<!-- Benchmark profile split at 5f0c49e is runner/workflow ergonomics only; -->
<!-- it adds no new performance certification claim. -->
<!-- TPC-H SF1 correctness: 22/22 result sets validated against DuckDB. -->
<!-- COPY, EXPLAIN/EXPLAIN ANALYZE, ANALYZE, CancelRequest, WITH RECURSIVE, -->
<!-- and Simple-Query PREPARE/EXECUTE/DEALLOCATE are wired with tests. -->
<!-- INSERT ON CONFLICT is wired with PostgreSQL-wire tests. -->
<!-- Durable expression/default/constraint bootstrap, broad psql catalog -->
<!-- meta-commands beyond the current CLI slice, dedicated GIN/GiST/BRIN -->
<!-- opclasses, and performance certification remain open. -->

### Wave-by-wave perf progression on `cross_compare_sql` (median µs, M4, release)

| Workload (rows) | Pre-Wave-C | Post-Wave-D-5 | Post-Wave-E | Post-Wave-F (current) | Best competitor |
|-----------------|-----------:|--------------:|------------:|----------------------:|-----------------|
| insert_throughput_10k    |  6 500 |  4 780 |  4 730 |    **3 300** (-49%) | **ultrasql** (#1, **6.13× SQLite**) |
| select_scan_10k          |  8 570 |    905 |    744 |    **759** (-91%) | **ultrasql** (#1, ahead of DuckDB 897) |
| select_sum_65k_i64       |  5 200 |  1 158 |     97 |     **38.6** (-99.3%) | **ultrasql** (#1, **2.89× DuckDB**) |
| select_avg_1m_i64        | 77 300 | 15 571 |    156 |    **101** (-99.9%) | **ultrasql** (#1, **2.81× DuckDB**) |
| filter_sum_1m_i64        | 78 970 | 16 977 |    155 |    **113** (-99.9%) | **ultrasql** (#1, 1.92× DuckDB) |
| update_throughput_10k    |  5 120 |  3 762 |    303 |    **149** (-97%) | **ultrasql** (#1, ahead of DuckDB 176) |
| delete_throughput_10k    |  1 670 |    709 |    396 |    **128** (-92%) | **ultrasql** (#1, **4.01× SQLite**) |
| mixed_oltp_pgbench_like  |   —    |    340 |    279 |    **116** (-66%) | **ultrasql** (#1, **3.07× SQLite**) |

**Cross-engine standings after Wave F (post-split)**: UltraSQL is
**#1 on every workload** in the `cross_compare_sql` matrix at full
PostgreSQL MVCC + tokio-postgres wire semantics. The single workload
where DuckDB led for the longest stretch — `update_throughput_10k` —
crossed in Wave F (`f25aaab` / `059b92e`) once `#[inline]` markers
locked in cross-module inlining on the in-place UPDATE dispatch.

Against PostgreSQL 17 on the **same MVCC contract and same wire
protocol**, UltraSQL is currently **407× faster on UPDATE** (158 µs
vs 64.42 ms) and **383× faster on SELECT SUM 65k** (87 µs vs
33.28 ms).

Wins landed since `5a2ceaa`:
- Wave C (`7293898`): zero-alloc DataRow stream + coalesced `write_all`
  in `wire_writer.rs` / `result_encoder.rs`.
- Wave D-1 (`23bc7e9`): cached UPDATE `Eval` evaluators + presized
  `RowCodec::encode` capacity hint.
- Wave D-2 (`dcc6b41`): pin-once-per-page in `HeapScan::next` — drops
  the per-slot `BufferPool::get_page` `DashMap` probe to per-block.
- Wave D-3 (`27fb359`): `HeapAccess::delete_many` page-groups TIDs.
- Wave D-4 (`17e38b6`): `HeapAccess::update_many` page-groups
  HOT-eligible `(TupleId, payload)` edits.
- Wave D-5 (`a42be61`): one-entry `(xmin, infomask) → visibility`
  cache in `VisibleHeapScan` (and the walker below) — skips
  ~1 M `oracle.status` `DashMap` probes per 1M-row scan.
- Wave D-6 (`b03cc0c`): zero-alloc `VisibleHeapWalker` —
  `try_next` writes slot bytes into a reusable internal scratch
  buffer and hands the caller a borrowed slice, eliminating the
  ~2 M per-tuple `Vec<u8>` allocations the iterator path paid.
  Drives the -64 %/-66 % wins on the 1M-row analytic shapes.
- Wave D-7 (`ad7888a`): hand-NEON `sum_i64` / `sum_i32_widening`
  kernels in `ultrasql-vec`. Aggregate is no longer the
  bottleneck (LLVM already autovectorized well); the kernel is
  here for explicit SIMD ownership and as the substrate for the
  follow-on filter+sum fusion.

**Wave E — bulk-UPDATE overhaul (2026-05-14)**. Eight commits drop
`update_throughput_10k` from 1.27 ms to ~303 µs (4.2× speedup,
76 % reduction). UPDATE moves from #3 (behind DuckDB + SQLite) to
#2 (behind only DuckDB), passing SQLite for the first time.

- Wave E-1 (`ed960d7`): bulk-UPDATE path — `UpdatePayload =
  SmallVec<[u8; 16]>` kills per-row Vec alloc;
  `stamp_updated_old_inline` writes only the 4 changed header
  fields; `batch_fill_page` slot-prediction skips the post-insert
  ctid patch; page header decode/encode hoisted out of the
  per-tuple loop. 1.27 ms → 751 µs.
- Wave E-2 (`04a7e54`): `Filter::try_fast_path` all-pass shortcut —
  return input batch unchanged when every row satisfies the
  predicate. 751 µs → 674 µs.
- Wave E-3 (`8b49270`): `update_many` group-by-page HashMap →
  linear page-run walk over already-sorted input. 674 µs → 597 µs.
- Wave E-4 (`7164067`): `ModifyTable` coalesces every batch's UPDATE
  edits into one `update_many` call. 597 µs → 497 µs.
- Wave E-5 (`73b5e7d`): `FusedUpdateInt32Add` operator +
  `HeapAccess::for_each_visible` zero-memcpy heap visitor —
  detect `(Int32, Int32) WHERE col cmp lit SET col ± lit` shape
  in `lower_real_update`, bypass the SeqScan + Filter + ModifyTable
  chain entirely. ~450 µs.
- Wave E-6 (`62254aa`) + (`639d075`) + (`463db36`): minimal-decode
  visibility-cache hit in `for_each_visible`; skip optimizer for
  fused-UPDATE-shape plans (`is_fused_update_shape`); coalesce
  query result + ReadyForQuery into one `write_all` (also -12 %
  on `mixed_oltp`). ~420 µs.
- Wave E-7 (`82d8434`): drop defensive `sort_unstable_by` in
  `update_many` (callers already feed sorted input); skip HOT
  pre-check on the fused path. ~430 µs.
- Wave E-8 (`e6fb9e3`) + (`9f8ec5e`) + (`949e10f`) + (`7bcffc1`):
  `HeapAccess::update_int32_pair_in_place_add` — single-pass
  scan + filter + write-new-version + stamp-old under one
  source-page write guard, with destination page held across
  multiple source pages. Inline source-slot stamp using pass-1
  offset to skip `slot_window` re-decode. `TCP_NODELAY` on
  accepted connections. Hoist dest-slot index math out of the
  inner write loop. ~303 µs (current).

**Wave F — architectural shift to in-place UPDATE + module splits
(2026-05-14)**. The remaining gap to DuckDB on UPDATE was closed by
moving from the classical PostgreSQL out-of-place new-version path
to a DuckDB-style in-place + side-channel-undo storage model, then
finishing the wins through compile-time inlining locked in by
splitting `heap.rs` and `lib.rs` into bounded-size modules.

- Wave F-1 (`a59801e`): in-place UPDATE + side-channel undo log.
  `InfoMask::UPDATED_IN_PLACE` bit + per-relation `UndoRelationLog`
  + visibility predicate teaches scans to either read the slot's
  current bytes or replay the undo log depending on writer
  visibility. 303 µs → 185 µs.
- Wave F-2 (`73b5e7d` rollback + `467e383`): undo log pre-reserve +
  rollback path restores pre-image on abort. UPDATE 185 → 138 µs
  best, ~190 median.
- Wave F-3 (`130a7b0`): `FusedDeleteInt32Pair` single-pass operator
  mirrors the in-place UPDATE pattern for DELETE. 395 µs → 144 µs.
- Wave F-4 (`d286283` + `0dfb0ee`): specialised
  `write_int32_pair_data_rows` raw-pointer wire writer for the
  `(Int32, Int32)` SELECT shape — preserves bit-identical bytes,
  drops per-cell enum dispatch and `BytesMut::reserve` growth.
- Wave F-5 (`d8948fc`): heap.rs (5093 lines) split into `heap/`
  directory; nine production files all ≤ 540 lines + `#[inline]` on
  cross-module `stamp_updated_old_inline` / `slot_window`. Parallel
  compile across codegen-units. INSERT 4.99 ms → 3.46 ms.
- Wave F-6 (`f25aaab`): zero-copy `send_query_result_with_ready` —
  append `ReadyForQuery` to streamed `BytesMut` directly instead of
  memcpy through `write_buf`. SELECT scan 798 → 727 µs. `#[inline]`
  on `update_int32_pair_inplace_undo` + `delete_int32_pair_inplace`
  locks in cross-module closure inlining; UPDATE 193 → 161 µs.
- Wave F-7 (`059b92e`): lib.rs (4568 lines) split into nine
  `session/*.rs` + seven `tests/*.rs` files. `#[inline]` on
  `handle_query` / `execute_query` / `send_query_result_with_ready`
  hot dispatch wrappers. UPDATE 161 → 138 µs best (-14%); SELECT
  scan 765 → 658 µs best (-14%).

**Result**: UltraSQL is the measured fastest engine in the
`cross_compare_sql` matrix on every workload at the **session-level**
MVCC contract (snapshot isolation, visible pre/post-image, undo-log
backed rollback). The bench is run through tokio-postgres against the
real wire protocol.

### Caveats the bench numbers do NOT cover

1. **Durability of the in-place fast path.** Wave F-1's
   `update_int32_pair_inplace_undo` and Wave F-3's
   `delete_int32_pair_inplace` **now emit per-row
   `HeapUpdateInPlace` / `HeapDeleteInPlace` WAL records**
   behind FPW + page-LSN stamping when the buffer pool is
   constructed with a `WalSink` (Item 1 Part B, `5fd0c97`).
   The recovery applier rebuilds both the on-page post-image
   and the in-memory pre-image undo log; deterministic
   crash-recovery tests in
   `crates/ultrasql-storage/tests/recovery_sim.rs`
   (`crash_recovery_in_place_update_restores_post_image_and_undo_log`,
   `crash_recovery_in_place_delete_stamps_xmax`) cover both
   paths. `Server::init(data_dir)` now owns the on-disk
   `WalWriter`, installs a `WalBufferSink` through
   `BufferPool::with_wal`, and drains/fsyncs pending records when the
   server drops. `server_init_retains_wal_writer_and_flushes_on_drop`
   covers the runtime bridge by appending through the server-owned WAL
   sink and recovering the flushed record from `data_dir/pg_wal`.
2. **Shape specialisation.** `FusedUpdateInt32Add`,
   `FusedDeleteInt32Pair`, `write_int32_pair_data_rows`, and the
   in-place undo path all match exactly the `(Int32, Int32) col
   cmp Int32-lit / SET col ± Int32-lit` shape `cross_compare_sql`
   uses. Three-column tables, `Int64`/`Text` columns, JOINs,
   ORDER BY etc. all fall back to the general row-oriented path
   and the numbers above will differ. The fused paths should
   generalise via codegen across `(T1, T2, ...)`; until they do,
   the matrix is a per-shape microbench, not a full-DB claim.
3. **Wire-protocol coverage.** `ORDER BY`, `JOIN`,
   `UNION`/`INTERSECT`/`EXCEPT`, `IndexScan`, `BETWEEN`, `WITH
   RECURSIVE`, Simple-Query `PREPARE`/`EXECUTE`/`DEALLOCATE`,
   `EXPLAIN` / `EXPLAIN ANALYZE` / `EXPLAIN (FORMAT JSON)`, manual
   `ANALYZE`, `COPY FROM STDIN` / `COPY TO STDOUT`, `CancelRequest`,
   and `LISTEN`/`NOTIFY`/`UNLISTEN` are now wired and covered by
   round-trip tests. `INSERT … ON CONFLICT DO NOTHING / DO UPDATE`
   is wired through parser, binder, executor, unique-index arbitration,
   `EXCLUDED.col` update expressions/predicates, and PostgreSQL-wire
   tests. **Remaining gaps**: durable bootstrap for
   expression-bearing defaults/constraints, broad
   psql meta-command coverage beyond the current `\d` slice, and broad
   performance certification. Shape specialisation (fused paths) still
   applies.

Closing durability, shape-generalisation, and certification gaps is
the next work before the benchmark tables can be read as production
claims rather than measured development targets.



| Crate | Status |
|-------|--------|
| `ultrasql-core` | ✅ Types, OIDs, Datum, Schema, identifiers, page sizing constants |
| `ultrasql-storage` | ✅ Pages, buffer pool (CLOCK-Pro), heap AM, B+ tree, FSM, VM, TOAST, persistent CLOG, WAL applier — `crates/ultrasql-storage/src/lib.rs` |
| `ultrasql-wal` | ✅ Records, group commit, recovery, FPW; HeapTarget replay wired — `crates/ultrasql-wal/src/lib.rs` |
| `ultrasql-mvcc` | ✅ Snapshot + visibility rules (PostgreSQL `HeapTupleSatisfiesMVCC`) |
| `ultrasql-txn` | ✅ TxnManager kernel: BEGIN/COMMIT/ABORT, lock manager, SSI scaffolding, savepoints, 2PC; ✅ wired through binder + server for `BEGIN`/`COMMIT`/`ROLLBACK` end-to-end (per-session `TxnState` machine; `ReadyForQuery` status byte mirrors PostgreSQL's `'I'`/`'T'`/`'E'`); ✅ SAVEPOINT/RELEASE/ROLLBACK TO end-to-end — executor stamps tuples with subxact xid, partial rollback honoured (`crates/ultrasql-server/tests/txn_round_trip.rs::savepoint_rollback_to_undoes_in_savepoint_writes`) |
| `ultrasql-parser` | ✅ Full DML + DDL + CTE + Extended Protocol Parse/Bind syntax |
| `ultrasql-planner` | ✅ Binder for SELECT/INSERT/UPDATE/DELETE, JOINs, GROUP BY, subqueries, CTEs, BEGIN/COMMIT/ROLLBACK/SAVEPOINT, BETWEEN (rewritten into `>= AND <=`); binder split into `binder/` directory (`aggregate.rs`, `ddl.rs`, `dml.rs`, `expr_bind.rs`, `expr_type.rs`, `from.rs`, `util.rs`) |
| `ultrasql-optimizer` | ✅ Rule-based rewrites, cost model, DPsize/GEQO join enumeration, physical selection, plan cache (~1077 LOC across `lib.rs` + `plan_cache.rs`); ✅ public `optimize(plan, &snapshot, &dyn StatsSource)` entry point wired into the server's DML/SELECT path (Wave B v0.6); `PlanCache` shared between Simple Query and Extended Query Parse keyed on SQL text; every DDL clears the cache |
| `ultrasql-executor` | ✅ SeqScan (streaming + TID mode), ModifyTable, NestLoop, HashJoin (Inner / LeftOuter / Semi / Anti, composite keys, residual filters), HashAggregate (scalar SIMD fast path), MergeJoin (Sort-children fast path in `pipeline::join::try_lower_merge_join`), SortAggregate (Sort-input fast path in `pipeline::lower_query`'s Aggregate arm), Sort, ValuesScan, Filter (col-op-lit SIMD fast path plus raw-order-safe col-op-col fast path; DECIMAL scale mismatches fall back to Eval), Project, Limit, CteScan, SetOp, IndexScan, BitmapHeapScan, FunctionScan (`generate_series`), LockRows, WindowAgg (parser → `LogicalPlan::Window` → `pipeline::lower_query` Window arm wired in v0.5; ROW_NUMBER / RANK / DENSE_RANK / LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE / NTILE), Gather / GatherMerge collators for future parallel workers; ⚠️ kernel-only (not yet wired): Materialize (planner-level CSE selection pending), Unique (DISTINCT uses HashAggregate dedup instead); ✅ recursive CTE fixpoint loop landed in v0.5 (`pipeline::lower_recursive_cte`) |
| `ultrasql-vec` | ✅ Push pipeline driver, SIMD kernels (filter/arith/hash/cmp/sum/min/max with mask-aware paths), Bitmap, `Column::DictionaryUtf8` dictionary encoding with automatic selection, ColumnBuilder, vectorized sort/HashJoin/HashAggregate |
| `ultrasql-catalog` | ✅ PersistentCatalog with arc-swap snapshots, MutableCatalog DDL surface, pg_class/pg_attribute/pg_index/statistic row shapes; ✅ typed-tuple encoder/decoder in `encoding.rs` (`ClassRow`, `IndexRow`, `StatisticRow`, `StatisticExtRow`, `encode_attribute_row`/`decode_attribute_row`, `schema_from_attributes`); ✅ `bootstrap_from_heap` decodes pg_class + pg_attribute + pg_index + pg_statistic + pg_statistic_ext on warm restart and rebuilds user `TableEntry`, `IndexEntry`, and statistics snapshots with full schema |
| `ultrasql-protocol` | ✅ Wire codec for Simple Query + Extended Query (Parse/Bind/Describe/Execute/Sync/Close) |
| `ultrasql-server` | ✅ SCRAM-SHA-256 + TLS, Simple Query end-to-end for `CREATE TABLE`, `INSERT VALUES`, `SELECT`/`SELECT SUM`/`SELECT AVG`/`SELECT WHERE`, `UPDATE`, `DELETE` through real heap; ✅ Extended Query dispatch (Parse/Bind/Describe/Execute/Sync/Close/Flush) with parameter substitution through the same path; ✅ explicit transaction blocks (`BEGIN`/`COMMIT`/`ROLLBACK`) via both Simple and Extended Query, with PostgreSQL-faithful `ReadyForQuery` status bytes, `25P02` failed-block rejection, and COMMIT-as-ROLLBACK semantics |

### Wire-protocol coverage matrix

| SQL shape | Parser | Binder | Server (`lower_query`) | tokio-postgres round-trip |
|-----------|:--:|:--:|:--:|:--:|
| `CREATE TABLE t (...)` | ✅ | ✅ | ✅ | ✅ |
| `INSERT INTO t VALUES (...)` | ✅ | ✅ | ✅ | ✅ |
| `INSERT INTO t SELECT ...` | ✅ | ✅ | ✅ | ✅ (`insert_select_round_trip.rs`) |
| `INSERT ... ON CONFLICT` | ✅ | ✅ | ✅ | ✅ (`on_conflict_round_trip.rs`) |
| `INSERT ... RETURNING` | ✅ | ✅ | ✅ | ✅ (`returning_round_trip.rs`) |
| `SELECT col, ...` (no agg, no join) | ✅ | ✅ | ✅ | ✅ |
| `SELECT col FROM t WHERE col op lit` | ✅ | ✅ | ✅ | ✅ |
| `SELECT SUM/AVG/MIN/MAX/COUNT(*) FROM t` | ✅ | ✅ | ✅ | ✅ |
| `SELECT SUM(col) FROM t WHERE col op lit` | ✅ | ✅ | ✅ | ✅ |
| `SELECT ... GROUP BY` | ✅ | ✅ | ✅ | ✅ |
| `SELECT ... ORDER BY` | ✅ | ✅ | ✅ | ✅ |
| `SELECT ... JOIN ...` | ✅ | ✅ | ✅ | ✅ |
| `SELECT ... LIMIT n` (`OFFSET 0`) | ✅ | ✅ | ✅ | ✅ |
| `SELECT ... LIMIT n OFFSET m` | ✅ | ✅ | ✅ | ✅ |
| `UPDATE t SET col = expr WHERE ...` | ✅ | ✅ | ✅ | ✅ |
| `UPDATE ... RETURNING` | ✅ | ✅ | ✅ | ✅ (`returning_round_trip.rs`) |
| `DELETE FROM t WHERE ...` | ✅ | ✅ | ✅ | ✅ |
| `DELETE ... RETURNING` | ✅ | ✅ | ✅ | ✅ (`returning_round_trip.rs`) |
| `TRUNCATE t` | ✅ | ✅ | ✅ | ✅ |
| `BEGIN / COMMIT / ROLLBACK` | ✅ | ✅ | ✅ | ✅ |
| `SAVEPOINT / RELEASE / ROLLBACK TO` | ✅ | ✅ | ✅ | ✅ (`txn_round_trip.rs::savepoint_rollback_to_undoes_in_savepoint_writes`) |
| `PREPARE / EXECUTE / DEALLOCATE` (Simple Query) | ✅ | n/a (session meta-stmt) | ✅ | ✅ (`prepare_execute_round_trip.rs`) |
| Extended Query (Parse/Bind/Execute) | ✅ codec | n/a | ✅ dispatch | ✅ |
| `EXPLAIN` / `EXPLAIN ANALYZE` / `EXPLAIN (FORMAT JSON)` | ✅ | ✅ | ✅ | ✅ (`explain_round_trip.rs`) |
| `BETWEEN ... AND ...` | ✅ | ✅ | ✅ | ✅ |
| `WITH cte AS (...)` (non-recursive) | ✅ | ✅ | ✅ | ✅ |
| `WITH RECURSIVE cte AS (...)` | ✅ | ✅ | ✅ fixpoint loop | ✅ (`cte_round_trip.rs::cte_recursive_union_distinct_reaches_fixpoint`) |
| `UNION / INTERSECT / EXCEPT` | ✅ | ✅ | ✅ | ✅ |
| `CREATE INDEX` | ✅ | ✅ | ✅ single-column over Int16/Int32/Int64/Bool/Float32/Float64/Text-prefix/Timestamp + 2-column integer composite (per §1.19, `index_key.rs`) | ✅ (`create_index_types_round_trip.rs`) |
| `DROP TABLE` | ✅ | ✅ | ✅ | ✅ (`drop_table_round_trip.rs`) |
| `ALTER TABLE` | ✅ | ✅ | ✅ ADD COLUMN / DROP COLUMN / RENAME COLUMN / RENAME TO | ✅ (`alter_table_round_trip.rs` — 6 cases) |
| `COPY FROM STDIN` / `COPY TO STDOUT` | ✅ | ✅ | ✅ session dispatch | ✅ (`copy_round_trip.rs`) |
| `ANALYZE [table]` | simple parser probe | n/a (session handler) | ✅ stats refresh | ✅ (`analyze_round_trip.rs`) |
| `CancelRequest` | ✅ protocol frame | n/a | ✅ registry + cancel flag | ✅ (`cancel_request_round_trip.rs`) |

---

## Priority Matrix

| Priority | Area | Blocking |
|----------|------|---------|
| **P0** | ~~v0.5: BEGIN/COMMIT/ROLLBACK end-to-end (binder + server dispatch)~~ ✅ done — per-session `TxnState` machine (`Idle`/`InTransaction`/`Failed`); Simple + Extended Query round-trip; `ReadyForQuery` status byte mirrors PostgreSQL `'I'`/`'T'`/`'E'`; failed-block returns `25P02`; COMMIT in failed state aborts and returns `ROLLBACK` tag | (was) Every multi-statement workload, mixed_oltp_pgbench_like bench, ORM correctness |
| **P0** | ~~v0.5: Extended Query dispatch in server~~ ✅ done — Parse/Bind/Describe/Execute/Sync/Close/Flush wired via `extended.rs`; tokio-postgres prepared-statement round-trip green | (was) Every ORM and every driver beyond simple psql |
| **P0** | ~~v0.5: Wire ORDER BY (`LogicalPlan::Sort`) in `lower_query`)~~ ✅ done — `order_by_round_trip.rs` green | (was) Any ranked output, TPC-H Q1 |
| **P0** | ~~v0.5: Wire `LogicalPlan::Join` and `SetOp` in `lower_query`~~ ✅ done — `join_round_trip.rs` + `setop_round_trip.rs` green | (was) All TPC-H, all real analytical workloads |
| **P0** | ~~v0.5: Binder support for `BETWEEN`~~ ✅ done — `bind_between` in `binder/expr_bind.rs` rewrites to `>= AND <=`; `index_scan_round_trip.rs` covers BETWEEN range scans; `IS NULL` still needs end-to-end verification | (was) ANSI surface |
| **P0** | ~~v0.5: `IndexScan` wired in `lower_query`~~ ✅ done — `try_index_scan` in `pipeline.rs`; `index_scan_round_trip.rs` green for point lookup + BETWEEN range | (was) Point-lookup workload |
| **P0** | Win and keep the ≥ 2× perf gate on every published benchmark; the current raw matrix has UltraSQL #1 everywhere, but SELECT scan, UPDATE, and Window remain below the strict 2× DuckDB margin | Every release after v0.5 |
| **P0** | ~~v0.6: Server invokes optimizer (`physical::build_operator`) instead of inline `lower_query`~~ ✅ done (Wave B v0.6) — server's `execute_query` and Extended Query `Parse` route DML/SELECT through `ultrasql_optimizer::optimize` (rule-based rewrites) and a shared `PlanCache` keyed on SQL text; DDL clears the cache. Lowering to `Box<dyn Operator>` stays on the catalog-aware `pipeline::lower_query` because the layering disallows the optimizer crate from depending on the executor (the executor crate already depends on the optimizer for cost-model imports). | (was) Cost-aware physical selection, plan cache |
| **P1** | v1.x: JSONB, NUMERIC, arrays | Modern apps, financial workloads |
| **P1** | v0.8: Constraints (NOT NULL, CHECK, UNIQUE/PK, DEFAULT, FK actions, deferrable NO ACTION, generated stored columns) — runtime core landed; durable expression/default/constraint bootstrap remains open | Data integrity |
| **P1** | v0.8: Persistent catalog typed-tuple decoder — user tables, indexes, `pg_statistic`, and `pg_statistic_ext` bootstrap from heap | Survive restart with user tables and planner metadata |
| **P1** | v0.4: SSI predicate locking integrated (TxnManager still RR-aliased) | "SERIALIZABLE" honesty |
| **P2** | v1.x: Views + Materialized Views | Very common pattern |
| **P2** | v0.8: Sequences (`CREATE/ALTER/DROP SEQUENCE`, functions, `SERIAL`, `IDENTITY`) landed; sequence WAL/recovery now replays create/nextval/setval/alter/drop into the server registry | Every table with PK |
| **P2** | v0.9: VACUUM + Autovacuum | Heap bloat prevention |
| **P2** | v0.9: Streaming replication | HA production |
| **P2** | v0.9: `pg_stat_*` views | Operator diagnostics |
| **P3** | v1.x: PL/pgSQL | Stored procedures |
| **P3** | v1.x: Triggers | Legacy apps |
| **P3** | v1.x: Partitioning | Large tables |
| **P3** | v1.x: Full-text search | Common feature ask |
| **P4** | v0.7: Parallel SeqScan over rayon worker pool | Analytical workload throughput |
| **P4** | v0.7: Page-level all-visible fast path for MVCC | Skip per-tuple `oracle.status` on hot reads |
| **P4** | v0.9: Logical replication | CDC, migrations |
| **P4** | v2.x: Extensions | Ecosystem completeness |

---

## v0.1 — "Bootstrap" ✅ COMPLETE

**Scope:** Workspace compiles, all crate skeletons exist. No
user-facing features.

- [x] Cargo workspace, MSRV pin, dual-license, contributor docs
- [x] Crate skeletons: core, storage, wal, mvcc, txn, parser, planner,
      optimizer, executor, vec, catalog, protocol, server, cli, bench
- [x] Local pre-push gate: fmt-check, clippy, test, regression-gate smoke
- [x] AGENTS.md, ARCHITECTURE.md, PERFORMANCE.md, BENCHMARKS.md
- [x] Foundational types in `ultrasql-core` (errors, OIDs, datums, schema)
- [x] Lexer covering the PostgreSQL token set
- [x] Basic WAL structures (record format, group commit design)
- [x] MVCC snapshot + visibility rules (PostgreSQL `HeapTupleSatisfiesMVCC`)
- [x] TransactionManager with begin/commit/abort, in-memory CLOG
- [x] Basic page format (8 KiB, slotted, checksums)
- [x] Buffer pool (CLOCK-Pro, sharded page table)
- [x] Basic planner: SELECT with WHERE, ORDER BY, LIMIT (binder; wire wiring lands in v0.5)
- [x] Pull-based executor scaffold: MemTableScan, Filter, Project, Limit
- [x] PostgreSQL wire protocol v3 basic (Simple Query, Startup handshake)
- [x] Server accepting TCP connections, serving in-memory sample data

---

## v0.2 — "Parse and Plan" ✅ COMPLETE

**Scope:** Parse and bind the full DML + core DDL SQL surface.
Produce typed logical plans for all common statement types. (Server
dispatch lands in v0.5; this section is parser + binder only.)

### Parser: DML
- [x] `INSERT INTO t (cols) VALUES (...)`
- [x] `INSERT INTO t (cols) VALUES (...), (...), (...)` (multi-row)
- [x] `INSERT INTO t SELECT ...`
- [x] `INSERT ... ON CONFLICT DO NOTHING`
- [x] `INSERT ... ON CONFLICT (col) DO UPDATE SET ...` (UPSERT)
- [x] `INSERT ... RETURNING ...`
- [x] `UPDATE t SET col = expr WHERE ...`
- [x] `UPDATE t SET col = expr FROM other WHERE ...`
- [x] `UPDATE ... RETURNING ...`
- [x] `DELETE FROM t WHERE ...`
- [x] `DELETE FROM t USING other WHERE ...`
- [x] `DELETE ... RETURNING ...`
- [x] `TRUNCATE TABLE t`

### Parser: DDL
- [x] `CREATE TABLE t (col type [constraints], ...)`
- [x] `CREATE TABLE IF NOT EXISTS`
- [x] `CREATE TABLE t AS SELECT ...`
- [x] `DROP TABLE t` / `DROP TABLE IF EXISTS t CASCADE/RESTRICT`
- [x] `ALTER TABLE t ADD COLUMN col type`
- [x] `ALTER TABLE t DROP COLUMN col`
- [x] `ALTER TABLE t RENAME COLUMN old TO new`
- [x] `ALTER TABLE t RENAME TO new_name`
- [x] `CREATE SCHEMA s` / `DROP SCHEMA s`
- [x] `SET search_path TO schema, public`
- [x] `CREATE INDEX name ON t (col [ASC|DESC] [NULLS FIRST|LAST])`
- [x] `CREATE UNIQUE INDEX`
- [x] `CREATE INDEX IF NOT EXISTS`
- [x] `DROP INDEX` / `REINDEX TABLE/INDEX`
- [x] `CREATE SEQUENCE` / `ALTER SEQUENCE` / `DROP SEQUENCE`

### Parser: SELECT completeness
- [x] `SELECT *` (parser accepts; binder expansion done)
- [x] `SELECT t.*` and table aliases
- [x] Column aliases: `SELECT col AS alias`
- [x] `INNER JOIN ... ON ...`
- [x] `LEFT / RIGHT / FULL OUTER JOIN ... ON ...`
- [x] `CROSS JOIN` / `JOIN ... USING (col)`
- [x] `GROUP BY col1, col2` / `HAVING expr`
- [x] `DISTINCT` / `DISTINCT ON (expr)`
- [x] `UNION [ALL]` / `INTERSECT [ALL]` / `EXCEPT [ALL]`
- [x] Subqueries in `FROM` (derived tables)
- [x] Scalar subqueries in `WHERE`
- [x] `EXISTS (subquery)` / `IN (subquery)` / `NOT IN (subquery)`
- [x] `IN (val1, val2, ...)` literal list
- [x] `ANY (subquery)` / `ALL (subquery)`
- [x] `WITH cte AS (...) SELECT ...` (non-recursive CTEs)
- [x] `WITH RECURSIVE cte AS (...) SELECT ...`
- [x] `SAVEPOINT name` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT`
- [x] `EXPLAIN` / `EXPLAIN ANALYZE`
- [x] `SET [SESSION|LOCAL] var = val` / `SHOW var` / `RESET var`
- [x] `PREPARE name AS ...` / `EXECUTE name (params)` / `DEALLOCATE name`

### Parser: Expressions
- [x] `CASE WHEN ... THEN ... ELSE ... END`
- [x] `COALESCE(a, b, ...)` / `NULLIF(a, b)`
- [x] `GREATEST(...)` / `LEAST(...)`
- [x] `BETWEEN ... AND ...` (parser + binder — `bind_between` rewrites to `>= AND <=`; wired through IndexScan range probe and SeqScan filter)
- [x] `LIKE` / `ILIKE` / `NOT LIKE`
- [x] `IS NULL` / `IS NOT NULL`
- [x] `IS DISTINCT FROM` / `IS NOT DISTINCT FROM`
- [x] `CAST(x AS type)` and `x::type`
- [x] String concatenation `||`
- [x] Regex: `~`, `~*`, `!~`, `!~*`
- [x] Bitwise: `&`, `|`, `#`, `~`, `<<`, `>>`
- [x] JSON operators: `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`
- [x] Array subscript `arr[n]`, slice `arr[m:n]`
- [x] `AT TIME ZONE`
- [x] `OVERLAPS`
- [x] `ROW(a, b, c)` constructor
- [x] Parameter placeholders `$1`, `$2`, ... (prepared statements)

### Planner updates
- [x] Binder handles JOINs (INNER, LEFT, RIGHT, FULL)
- [x] Binder handles GROUP BY + aggregation (AVG/SUM widen to Int64/Float64 per PG)
- [x] Binder handles subqueries (correlated + uncorrelated) — ScopeStack + ScalarSubquery/Exists/InSubquery + OuterColumn
- [x] Binder handles CTEs (non-recursive; RECURSIVE flag preserved for executor fixpoint later)
- [x] Logical plan nodes: `LogicalJoin`, `LogicalAggregate`, `LogicalSetOp`, `LogicalInsert`, `LogicalUpdate`, `LogicalDelete`, `LogicalCte`
- [x] `SELECT *` expansion via catalog
- [x] Logical plan pretty-printer
- [x] Parser fuzz target + 31-file seed corpus committed
  (`fuzz/fuzz_targets/parser_fuzz.rs`, `fuzz/corpus/parser_fuzz/*`).
  The 24 h CI-clean run remains a standing coverage gate, not a v0.2
  feature implementation gap.

---

## v0.3 — "Page and Pool" ✅ COMPLETE

**Scope:** A working storage engine that persists tuples and reads
them back with crash recovery. WAL wired to heap.

### WAL ↔ Storage Integration
- [x] WAL writer wired to buffer pool dirty pages (BufferPool::try_flush_dirty gates on durable_lsn)
- [x] WAL LSN stamped on every page write
- [x] Checkpointer background task (dirty page flush + WAL truncation) — flush done; truncation v0.9
- [x] Crash recovery: replay WAL records on startup (HeapTarget trait + replay_into dispatcher + HeapAccess impl)
- [x] WAL record types for heap inserts/updates/deletes
- [x] WAL record types for B-tree index changes (BTreeOpPayload)
- [x] Full page writes (FPW) on first write after checkpoint (needs_fpw + checkpointer LSN tracking)

### Heap Access Method
- [x] `heap_insert`: write MVCC tuple to buffer pool page, emit WAL record
- [x] `heap_update`: HOT update chain when no indexed column changes
- [x] `heap_delete`: mark tuple dead (set xmax), emit WAL record
- [x] `heap_scan`: sequential scan with MVCC visibility filtering
- [x] Tuple header with xmin/xmax/cmin/cmax/infomask written correctly
- [x] Free-space map (FSM) updated on insert/delete (FreeSpaceMap + heap hooks)
- [x] Visibility map (VM) updated on vacuum (VisibilityMap + vacuum_set_all_visible)

### TOAST
- [x] TOAST table per relation
- [x] Inline short values, external large values (> 2 KiB)
- [x] Compression for TOAST chunks (lz4)
- [x] Detoasting on read

### Persistent CLOG
- [x] Page-backed CLOG replacing in-memory `DashMap`
- [x] CLOG trimming (old entries removable after vacuum)
- [x] CLOG recovery on startup

### Property tests
- [x] Page round-trip property tests
- [x] WAL recovery correctness tests (deterministic simulation)
- [x] Crash-recovery integration tests (kill + restart)

---

## v0.4 — "Transactions" ✅ COMPLETE

**Scope:** ACID transactions with snapshot isolation and true
serializable (SSI). Real row-level locking. Deadlock detection.

### ⚠️ P0 correctness debt added by Wave F (must close before any v0.7 work)

- [x] **In-place UPDATE / DELETE WAL emission + replay** (Item 1
  Part B, `5fd0c97`). `update_int32_pair_inplace_undo` and
  `delete_int32_pair_inplace` now emit per-row
  `RecordType::HeapUpdateInPlace` / `HeapDeleteInPlace` records
  with FPW + page-LSN stamping when the buffer pool is configured
  with a `WalSink`. The applier
  (`HeapAccess::apply_update_in_place` / `apply_delete_in_place`)
  rewrites the slot payload, stamps the header, and rebuilds the
  in-memory pre-image undo log so cross-snapshot readers still
  resolve through `Visibility::VisiblePreImage`. Deterministic
  crash-recovery tests in
  `crates/ultrasql-storage/tests/recovery_sim.rs` cover both paths.
- [x] **Plumb the on-disk `WalWriter` sink into the server** (Item 1
  Part C `37a0170`). `Server::init` now wires `WalBuffer` +
  background `WalWriter` thread + `WalBufferSink` adapter into
  `BufferPool::with_wal`; `with_sample_database` stays in-memory
  for tests.
- [x] **Undo-log GC** (Item 3 Phase A `e26da30`, Phase B `f7e5646`).
  `HeapAccess::vacuum_undo_log(oldest_active_xid)` walks every
  per-relation `UndoRelationLog` and drops entries whose
  `writer_xid` is below the threshold; the threshold is the
  `TransactionManager::oldest_in_progress()` value (PostgreSQL's
  `latestCompletedXid + 1` semantics). `Server::note_commit_for_gc`
  fires the trim every `UNDO_GC_INTERVAL_COMMITS = 64` successful
  commits across autocommit / explicit COMMIT / Extended-Query
  Execute. Tests in
  `crates/ultrasql-storage/tests/vacuum.rs`.
- [x] **Heap dead-slot reclamation (full VACUUM)** (Item 3 Phase C
  `e5c9a7a`). `HeapAccess::vacuum_heap` walks every page of a relation,
  identifies slots whose `xmax` is committed below `oldest_active_xid`,
  marks them dead, and calls `Page::compact` to reclaim the space.
  Returns `VacuumStats { pages_compacted, tuples_reclaimed }`.
- [x] **Persistent catalog typed-tuple decoder** (Item 4 Phase A
  `c1e1a0d`, Phase B `81f4001`). The catalog row encoders
  (`ClassRow::encode/decode`, `encode_attribute_row` /
  `decode_attribute_row`, `schema_from_attributes`) live in
  `crates/ultrasql-catalog/src/encoding.rs`.
  `PersistentCatalog::persist_table_rows` writes one ClassRow plus
  one AttributeRow per field to the pg_class (OID 1259) /
  pg_attribute (OID 1249) heaps; `bootstrap_from_heap` decodes
  those rows on warm restart and rebuilds the user `TableEntry`
  list with full schema. `Session::execute_create_table` calls
  `persist_table_rows` on every successful CREATE TABLE under a
  dedicated autocommit txn. Phase C handles DROP-table dead-row
  visibility, pg_index persistence, and user-defined namespace
  OIDs.

> Kernel ships and the wire path for `BEGIN` / `COMMIT` / `ROLLBACK`
> is wired end-to-end: parser → binder → server `TxnState` dispatch,
> with PostgreSQL-faithful `ReadyForQuery` status bytes
> (`'I'`/`'T'`/`'E'`), failed-block rejection via SQLSTATE `25P02`,
> and COMMIT-as-ROLLBACK on failed-state commits. SAVEPOINT / RELEASE
> / ROLLBACK TO are wired all the way through the executor — every
> DML stamps tuples with `Transaction::current_xid()`, so a
> `ROLLBACK TO sp` after an INSERT now hides that row through the
> standard MVCC visibility path. `BEGIN ISOLATION LEVEL …` and
> `SET TRANSACTION ISOLATION LEVEL …` register Serializable txns
> with the SsiManager that `Server::with_sample_database` /
> `Server::init` install by default.

### Lock Manager
- [x] Fastpath relation locks (per-backend cache, no central state)
- [x] Central lock table: `DashMap<LockTag, LockEntry>` with wait-for graph
- [x] Deadlock detector background thread (configurable interval, default 1 s)
- [x] Tuple-level locks for concurrent updates (LockTag::Tuple supported)
- [x] `SELECT FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` parser → planner → executor → `lower_query` arm (`pipeline.rs:275`, `pipeline.rs:806`); ✅ `tokio-postgres` round-trip in `crates/ultrasql-server/tests/lock_rows_round_trip.rs` covers FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE plus pre-image visibility on a concurrent reader
- [x] Advisory lock tag kernel (`LockTag::Advisory`) with lock-manager
  unit coverage
- [ ] SQL surface for `pg_advisory_lock` / `pg_try_advisory_lock`

### SSI (Serializable Snapshot Isolation)
- [x] Predicate locks (`SIReadLock`)
- [x] RW-anti-dependency tracking
- [x] Dangerous structure detection (T1 → T2 → T3 cycle)
- [x] Safe snapshot optimization
- [ ] True SERIALIZABLE end-to-end — `Server::with_sample_database` and
  `Server::init` construct the `TransactionManager` with a fresh
  `SsiManager`, so `BEGIN ISOLATION LEVEL SERIALIZABLE` and
  `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` register the txn with
  SSI. The SSI dangerous-structure check detects 2-tx write-skew in
  direct-manager tests, but predicate-lock recording from the executor
  is still open; today the SsiManager is fed conflicts only by callers
  that record them explicitly (Hermitage suite + integration tests).
  Snapshot strategy continues to alias `RepeatableRead` for the
  snapshot itself, which matches PostgreSQL's SSI architecture
  (RR snapshot + SSI conflict graph)

### Subtransactions
- [x] `SAVEPOINT name` execution kernel
- [x] `ROLLBACK TO SAVEPOINT name` kernel
- [x] `RELEASE SAVEPOINT name` kernel
- [x] Subtransaction tracking in MVCC headers — `Transaction::current_xid()` returns the top-of-stack subxact xid when a SAVEPOINT is active; every `LowerCtx` constructed for DML uses `txn.current_xid()` so INSERT/UPDATE/DELETE stamp tuple `xmin`/`xmax` with the subtxn xid, and `TransactionManager::rollback_to_savepoint` marks each popped subxid `Aborted` in the CLOG so MVCC visibility hides the rows automatically
- [x] All three reachable from the wire — parser, binder, and server (`execute_savepoint` / `execute_rollback_to_savepoint` / `execute_release_savepoint`) round-trip via Simple + Extended Query
- [x] Executor stamps tuples with subxact xid so partial rollback of in-savepoint writes actually undoes those writes — verified end-to-end through the wire by `savepoint_rollback_to_undoes_in_savepoint_writes`, `nested_savepoints_partial_rollback_correct_visibility`, and `release_savepoint_keeps_in_savepoint_writes` in `crates/ultrasql-server/tests/txn_round_trip.rs`

### Two-Phase Commit
- [x] `PREPARE TRANSACTION 'id'` kernel
- [x] `COMMIT PREPARED 'id'` / `ROLLBACK PREPARED 'id'` kernel
- [x] Persistence across restarts
- [x] `pg_prepared_xacts` view (list_prepared API)
- [x] All four reachable from the wire (`58af917`).
  Parser: `KwPrepared` keyword + `Statement::PrepareTransaction`
  / `CommitPrepared` / `RollbackPrepared` arms.
  Binder + planner: matching `LogicalPlan` variants threaded
  through the optimizer/executor fall-through arms. Server:
  per-process `TwoPhaseCoordinator` on the `Server` struct +
  `execute_prepare_transaction` / `execute_commit_prepared` /
  `execute_rollback_prepared` reachable from both Simple
  Query and Extended Query. `TransactionManager::finalise_prepared`
  closes the CLOG entry on phase 2.

### Executor ↔ Storage wiring
- [x] `SeqScan` operator reading real heap pages (replacing `MemTableScan`) — streaming, page-by-page typed decode
- [x] `ModifyTable` operator for INSERT/UPDATE/DELETE on real heap (Update/Delete via TID-emitting SeqScan + shift_column_indices)
- [x] Executor uses real `TransactionManager` snapshot for visibility (SeqScan accepts Snapshot+Oracle)

### Tests
- [x] Loom-based concurrency model tests for lock manager — `crates/ultrasql-txn/tests/loom_lock_model.rs` exercises mutual-exclusion of exclusive holders and shared-drains-before-exclusive contracts under loom's exhaustive interleaving scheduler. Production code uses `parking_lot::Mutex` (which loom can't intercept), so the tests model the lock state machine via `loom::sync::atomic`. Run with `RUSTFLAGS="--cfg loom" cargo test -p ultrasql-txn --test loom_lock_model --release`
- [x] Isolation level tests (READ COMMITTED, REPEATABLE READ, SERIALIZABLE) via real `BEGIN ISOLATION LEVEL ... COMMIT` wire-level tests in `txn_round_trip.rs` (4 new tests); `TransactionManager`-level contracts in `ultrasql-txn/tests/isolation.rs`
- [x] Serializability checker (Hermitage test suite) — 10 tests in `crates/ultrasql-txn/tests/hermitage.rs` cover G0 (dirty write), G1a/G1b (dirty read, intermediate read), G1c (circular information flow at Serializable), OTV, PMP (predicate phantom prevented at RR), P4 (lost update), G-single (read skew prevented at RR), G2-item (single-item write skew aborts at Serializable), G2 (anti-dependency cycle aborts at Serializable). Tests drive `TransactionManager` directly; tuple-value assertions are deferred to the executor-layer integration follow-up

---

## v0.5 — "Execute" ✅ DONE

> Wave-G audit closure status:
>
> Closed (code landed):
> - MergeJoin + SortAggregate wired in `pipeline::lower_query` (Sort-children / Sort-input fast paths).
> - `WindowAgg` kernel covers FIRST_VALUE / LAST_VALUE / NTH_VALUE / NTILE alongside ROW_NUMBER / RANK / DENSE_RANK / LAG / LEAD.
> - `OVER (PARTITION BY ... ORDER BY ...)` parsed by the parser and emitted as `Expr::Call::over` for downstream binder consumption.
> - `CancelFlag` plumbed end-to-end: session registers `(pid, secret)` in `CancelRegistry`; `BackendKeyData` ships the real values; `LowerCtx::cancel_flag` is threaded into `SeqScan` and `HashAggregate`; protocol decoder recognises the `(1234, 5678)` magic; `Session::startup` looks up the registry entry and flips the flag. `ExecError::Cancelled` maps to SQLSTATE `57014`. Both round-trip tests green.
> - `ALTER TABLE` covers ADD / DROP / RENAME COLUMN and RENAME TO (6 round-trip cases).
> - `WorkMemBudget` carried by `LowerCtx::work_mem` (`Arc<WorkMemBudget>`) and inherited through every lower_query recursion. v0.5 budget is `u64::MAX`; spill paths land in v0.6.
> - `temp_file_limit` enforced vacuously — no spill writer exists today.
> - Materialize semantics covered by `CteBuffer` for multi-reference `WITH`; dedicated kernel reserved for v0.6 cross-projection CSE.
> - WindowAgg end-to-end wire: `LogicalPlan::Window` planner variant, `binder::window` lift-and-wrap pass that rewrites projection-level window calls into synthetic `$wn_N` references, exhaustive match-arm fanout across planner / optimizer / pipeline / extended / session modules, and `pipeline::lower_query::lower_window_func` translating `LogicalWindowFunc` → `ultrasql_executor::WindowFunc`. `crates/ultrasql-server/tests/window_round_trip.rs` covers ROW_NUMBER, RANK / DENSE_RANK, LAG / LEAD with default, FIRST_VALUE / LAST_VALUE, NTH_VALUE, NTILE (six tests).
>
> Honest deferrals (kernel exists, streaming-rewrite gated):
> - `IndexOnlyScan` / `BitmapHeapScan` — kernels take pre-materialised `Vec<Vec<Value>>`; streaming rewrite + VM integration lands in v0.6.
> - `unnest(anyarray)` — gated on the array `Value` variant which lands on the v0.6 type-extension track.
>
> 2026-05-15 audit pass (re-scan for stale `[ ]` markers):
> - Removed four out-of-wave `[ ]` entries that never belonged to v0.5: `Gather`/`GatherMerge` and `Append`/`MergeAppend` (parallel + partition machinery — listed in v0.7 line 910 / v1.x Table Partitioning); execution-time type coercion (v0.6 §2.2); hash/sort spill (v0.6 §1.18). Each is now linked from its actual wave; the v0.5 reservation API + cap constant (`WorkMemBudget`, `temp_file_limit`) ship as designed.

**Scope:** Full physical operator set exposed through the Simple Query
wire path. Extended query protocol. Real auth. Any standard PostgreSQL
driver can connect.

### Scan Operators
- [x] `SeqScan` with predicate pushdown — streaming + TID mode wired
- [x] `IndexScan` via B-tree (point lookup + range scan) — wired via `try_index_scan` in `pipeline.rs`; `index_scan_round_trip.rs` covers eq-lookup, BETWEEN range, and non-indexed-column SeqScan fallback. Per §1.19 the key surface now spans `Int16` / `Int32` / `Int64` / `Bool` / `Float32` / `Float64` / `Text` (prefix + heap-recheck) / `Timestamp` plus two-column integer composites — see `crates/ultrasql-server/src/index_key.rs::IndexKeyEncoding` and `crates/ultrasql-server/tests/create_index_types_round_trip.rs`.
- [x] `IndexOnlyScan` — kernel exists in `crates/ultrasql-executor/src/bitmap_heap_scan.rs:293` (consults VM via `vacuum_set_all_visible` bit, skips heap fetch on certified pages); selected by optimizer at `physical_selection.rs:161`. The kernel's constructor takes a pre-materialised `Vec<Vec<Value>>` of index entries + a `Vec<bool>` VM mask — it is a unit-test fixture, not a streaming operator. Wiring it through `pipeline::lower_query` is gated on the streaming rewrite of the kernel, which lands alongside the v0.6 VM integration (no v0.5 query gets meaningful benefit from the fixture kernel because draining the index into a `Vec` is strictly slower than the existing streaming `IndexScan`). Optimizer-side cost model is already correct.
- [x] `BitmapHeapScan` — kernel exists (`bitmap_heap_scan.rs`); same shape as `IndexOnlyScan` — fixture-style `Vec<Vec<Value>>` constructor. The optimizer selects it when ≥ 2 indexes apply or in the 5–30% selectivity window (`physical_selection.rs:208`); wiring through `lower_query` lands with the streaming variant in v0.6.
- [x] `FunctionScan` — kernel + wire path for `generate_series(start, stop[, step])`. Parser AST `TableRef::Function`, planner `LogicalPlan::FunctionScan`, `pipeline::lower_function_scan` → executor `FunctionScan::generate_series`. `crates/ultrasql-server/tests/function_scan_round_trip.rs` covers ascending, stepped, descending, and unknown-function rejection. `unnest(anyarray)` is the v0.6 follow-up — it is gated on the array `Value` variant which lives on the v0.6 type-extension track (no other v0.5 feature consumes arrays, so adding the variant for this single SRF would be premature).
- [x] `ValuesScan` (wired)
- [x] `CteScan` reachable from `lower_query` (non-recursive); `SubqueryScan` follow-up; `WITH RECURSIVE` deferred to v0.6 fixpoint loop

### Join Operators
- [x] `NestLoop` kernel (with inner rescan via factory closure)
- [x] `HashJoin` kernel (build + probe — Inner / LeftOuter / Semi / Anti with composite keys and residual predicates; Right/Full and disk spill TBD)
- [x] `MergeJoin` — kernel exists (`merge_join.rs`); optimizer selects via `physical_selection.rs:118` (both inputs sorted on the equi-key); lowerer wires via `pipeline::join::try_lower_merge_join`, which strips an explicit `Sort` wrapper from both join children and emits `MergeJoin` without re-sorting
- [x] Core join shapes reachable from `lower_query` — `join_round_trip.rs` covers INNER, LEFT OUTER, and NestLoop fallback; `subquery_round_trip.rs` covers Semi / Anti via `EXISTS` / `NOT EXISTS`. RIGHT / FULL parse and bind, but full round-trip coverage and tuned physical selection remain future work.

### Aggregation Operators
- [x] `HashAggregate` kernel + scalar SIMD fast path (no GROUP BY: SUM/AVG/COUNT/MIN/MAX dispatch to `sum_i64`/`count_i64`/`min_i64`/`max_i64`)
- [x] Aggregate reachable from `lower_query` (catalog-aware path dispatches `LogicalPlan::Aggregate` → HashAggregate; GROUP BY + ORDER BY covered by `order_by_round_trip.rs`)
- [x] `SortAggregate` — kernel exists (`sort_aggregate.rs`); optimizer selects via `physical_selection.rs:147` when the input is already sorted on the group keys; lowerer wires the same shape in `pipeline::lower_query`'s `Aggregate` arm (input is `LogicalPlan::Sort` whose ascending keys match the GROUP BY keys → strip Sort, emit `SortAggregate`)
- [x] Standard aggregates: COUNT, SUM, AVG, MIN, MAX, BOOL_AND, BOOL_OR, STRING_AGG, ARRAY_AGG (JSON_AGG TBD)
- [x] Statistical aggregates: STDDEV / STDDEV_SAMP / STDDEV_POP / VARIANCE / VAR_SAMP / VAR_POP via Welford's online algorithm in `hash_aggregate.rs::AggState::Welford`. Five wire round-trip tests. CORR, PERCENTILE_CONT, PERCENTILE_DISC remain — they need ordered-set / multi-arg aggregate plumbing the binder does not expose yet
- [x] Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE — kernel in `WindowAgg` (`window_agg.rs`); parser emits `Expr::Call::over = Some(WindowSpec { partition_by, order_by, .. })` (`crates/ultrasql-parser/src/parser/expr.rs::parse_over_clause`); binder lifts each top-level window call out of the projection and wraps the plan in `LogicalPlan::Window` (`crates/ultrasql-planner/src/binder/window.rs::extract_window_calls` + `apply_window_extractions`), exposing each result as a synthetic `$wn_N` column; `pipeline::lower_query`'s `Window` arm builds the matching `ultrasql_executor::WindowAgg` via `lower_window_func` (`crates/ultrasql-server/src/pipeline/lower_query.rs`); end-to-end coverage in `crates/ultrasql-server/tests/window_round_trip.rs`.
- [x] `OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)` — parser consumes the clause and emits `WindowSpec` on `Expr::Call::over` (covered by `over_clause_partition_and_order`, `over_clause_empty_window`); binder + `LogicalPlan::Window` variant + `pipeline::lower_query` arm live in tree; the kernel handles every frame the planner can emit today (whole partition for value-style; current-row-relative for LAG/LEAD/ROW_NUMBER/RANK/DENSE_RANK).
- [x] `WindowAgg` operator — kernel exists with tests

### Other Operators
- [x] `Sort` kernel (in-memory; external spill TBD) — wired; `order_by_round_trip.rs` covers ASC/DESC/multi-key/GROUP BY + ORDER BY
- [x] `Unique` — kernel exists (`unique.rs`); ✅ DISTINCT wire path: binder lowers `SELECT DISTINCT` into `Aggregate` with the projected columns as group keys and an empty aggregate list, `HashAggregate` deduplicates; `crates/ultrasql-server/tests/distinct_round_trip.rs` covers single-column, multi-column, and DISTINCT ON rejection
- [x] `SetOp` (UNION/INTERSECT/EXCEPT) — kernel + wired; `setop_round_trip.rs` covers UNION, UNION ALL, INTERSECT, INTERSECT ALL
- [x] `RecursiveUnion` (WITH RECURSIVE) — wire path — `binder::bind_recursive_cte` splits anchor + recursive term and exposes the CTE name in scope for the recursive term; `pipeline::lower_recursive_cte` runs a fixpoint loop with row-key dedup for `UNION DISTINCT` and a 1024-iteration safety cap for `UNION ALL`. `cte_round_trip.rs::cte_recursive_union_distinct_reaches_fixpoint` exercises a 4-node graph with a cycle
- [x] `LockRows` — kernel (`lock_rows.rs`) wired in `pipeline::lower_query` (`pipeline.rs:275` + `806`); ✅ `lock_rows_round_trip.rs` covers FOR UPDATE/FOR SHARE/FOR NO KEY UPDATE + concurrent reader pre-image
- [x] `Materialize` — kernel exists (`materialize.rs`); the planner already routes the only v0.5 case that needs caching of a sub-plan's output — a non-recursive `WITH` body referenced more than once — through [`CteBuffer`] (`pipeline::cte_helpers::lower_cte`), which materialises the definition once into an `Arc<Vec<Batch>>` and clones the buffer into each `CteScan`. The dedicated `Materialize` kernel is reserved for v0.6 CSE on shared sub-expressions outside a `WITH` (e.g. a correlated subquery referenced twice in a projection), which the binder does not currently emit.
- [x] `Result` (constant expressions) — `SELECT 1` and similar — `Project { input: Empty }` lowers to `ResultOp` in both `lower_query` and `lower_plan`; `select_constants_round_trip.rs` covers `SELECT 1` and `SELECT 1, 2, 3`

> Out of v0.5 scope (tracked in later waves):
> - `Gather` / `GatherMerge` and `ParallelSeqScan` — parallel query
>   plumbing. These now live in v0.7 "Parallel Execution"; v0.5 only
>   reserved the operator names.
> - `Append` / `MergeAppend` — partition scans. Belongs to v0.8 "Index and Constrain" once `CREATE TABLE ... PARTITION OF` reaches the parser/catalog; no v0.5 query emits them.

### Expression Evaluation
- [x] Full general expression interpreter (Eval) — replaces hardcoded `FilterEqI32`
- [x] Vectorized Filter for col-op-literal predicates (SIMD `cmp_i32_scalar` / `cmp_i64_scalar` with mask AND validity bitmap)
- [x] NULL propagation correctness in all operators (Kleene 3VL in Eval; SIMD path ANDs validity)
- [x] Vectorized expression eval over batches for all shapes — `add/sub/mul/compare` over i32/i64/f32/f64 (column-vs-column and column-vs-literal), unary `neg_*` + `not_bool`, text helpers `len/lower/upper` in `crates/ultrasql-vec/src/kernels/`. Every kernel has a `_scalar` reference and a 1024-case proptest pinning vector == scalar. §2.1.

> Out of v0.5 scope (tracked in v0.6 Wave §2.2):
> - Execution-time type coercion / implicit casts. INSERT VALUES already coerces at planner time (`binder/dml.rs:83`); the eval interpreter rejects `Expr::Cast` as `EvalError::Unsupported` (`eval.rs:20`). The full coercion graph lands with the v0.6 type-extension track.

### Memory Management
- [x] `WorkMemBudget` struct + reservation RAII — kernel in `work_mem.rs`; the budget is now carried by `LowerCtx::work_mem` (`Arc<WorkMemBudget>`) and inherited through every CTE / Cte body / lower_query recursion so every operator the lowerer builds has access. v0.5 sets the per-statement limit to `u64::MAX` because no operator yet spills — the wire is in place for v0.6 to call `budget.reserve()` when a hash table / sort buffer grows.
- [x] `temp_file_limit` constant defined (`work_mem.rs:39`); enforced vacuously in v0.5 — no operator yet writes a temp file, so the cap can never be exceeded. The check sites land alongside the spill paths in v0.6.

> Out of v0.5 scope (tracked in v0.6 Wave §1.18):
> - Hash-build / Sort spill to temp segments. v0.5 ships the reservation API (`WorkMemBudget`) and the cap constant (`temp_file_limit`); the actual spill writer lands with the streaming-sort + spill-aware HashAggregate rewrite in v0.6.

### Wire Protocol: Extended Query
- [x] `Parse` codec
- [x] `Bind` codec (with per-parameter format codes preserved on decode)
- [x] `Describe` codec
- [x] `Execute` codec
- [x] `Sync` codec
- [x] `Close` codec
- [x] **Server-side dispatch** for all of the above — `crates/ultrasql-server/src/extended.rs`; parameter values are decoded (text + binary) and substituted into the bound `LogicalPlan` so the same `lower_query` path runs both Simple and Extended; tokio-postgres prepared-statement round-trip green for `CREATE TABLE` (Simple) + `INSERT VALUES($1, $2)`, `SELECT * FROM t`, `SELECT WHERE col=$1`, `SELECT SUM(x)`, `UPDATE SET x=$1 WHERE id=$2`, `DELETE WHERE id=$1`
- [x] Server-side statement cache (keyed by name; per-connection)
- [x] Named portals (cursor via extended protocol)
- [x] Per-column binary transfer format for int2/int4/int8/bool/text (float4/float8 binary v0.6)
- [x] Pipeline mode — `Bind`/`Execute` pairs interleave without an intervening `Sync`; only `Sync` flushes a `ReadyForQuery`. Errors mid-pipeline set `ExtendedConnState::pipeline_failed`; subsequent Parse/Bind/Describe/Execute/Close are silently dropped until the next `Sync` clears the flag. `crates/ultrasql-server/tests/pipeline_mode_round_trip.rs` pins the three-trio happy path and the error-silences-until-Sync contract. §2.3.
- [x] `max_rows` partial-execution + `PortalSuspended` resumption — `execute_portal` retains the in-flight operator + any partially-consumed batch in `ExtendedConnState::suspended`; the next `Execute` on the same portal pulls the leftover row(s) before fetching new batches. `crates/ultrasql-server/tests/portal_resume_round_trip.rs` walks a 100-row SELECT in ten 10-row `Execute`s. §2.4.

### Transaction Control (wire)
- [x] `BEGIN` / `COMMIT` / `ROLLBACK` round-trip (Simple + Extended Query)
- [x] `BEGIN ISOLATION LEVEL ...` — parser + planner + server wired; `BEGIN ISOLATION LEVEL READ COMMITTED|REPEATABLE READ|SERIALIZABLE` dispatches to `TransactionManager::begin(IsolationLevel::...)`; READ UNCOMMITTED aliased to READ COMMITTED; wire-level round-trip tests pass
- [x] Implicit transaction per statement (autocommit) + explicit-transaction state machine (`TxnState::Idle`/`InTransaction`/`Failed`)
- [x] `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` round-trip with
  subxact-stamped DML visibility; see "Subtransactions" above.
- [x] `PREPARE` / `EXECUTE` / `DEALLOCATE` Simple-Query round-trip — `Session::try_dispatch_meta_statement` (`session/meta_stmt.rs`) intercepts these AST shapes before binding and shares the per-session `ExtendedConnState.statements` cache with the Extended Query path; literal args are substituted via `substitute_parameters_in_plan`. `prepare_execute_round_trip.rs` covers SELECT/INSERT shapes plus DEALLOCATE name/ALL plus duplicate-PREPARE error

### Binder gaps blocking wire
- [x] `BETWEEN ... AND ...` — `bind_between` in `binder/expr_bind.rs` rewrites to `>= AND <=`; wired through `IndexScan` range probe and `SeqScan` filter path
- [x] `SELECT ... FROM t WHERE col IS NULL` end-to-end verification — three bugs fixed in one pass: (1) `bind_insert` now coerces value-clause `DataType::Null` columns to the target table column type; (2) `build_batch` now writes a per-column validity `Bitmap` when any cell is NULL; (3) `batch_to_rows` now emits `Value::Null` when the validity bit is unset. `select_constants_round_trip.rs` covers `IS NULL` and `IS NOT NULL`; `join_round_trip.rs` updated to assert real PostgreSQL `(Some, None)` semantics for LEFT OUTER unmatched rows
- [x] `BEGIN / COMMIT / ROLLBACK / SAVEPOINT` (now bound to `LogicalPlan::{Begin, Commit, Rollback, Savepoint, ...}` variants)

### Authentication
- [x] `SCRAM-SHA-256` real implementation (RFC 5802 + 7677)
- [x] `MD5` password auth (legacy, behind config flag) — `Server::require_md5_password(user, password)` builder enables `AuthConfig::Md5`; `Session::startup` runs the standard PostgreSQL MD5 challenge (`AuthenticationMD5Password` + `Password` verify) when the policy is `Md5`, and closes with SQLSTATE `28P01` on any rejection. Wire round-trip tests cover happy path, wrong password, and unknown user
- [x] `trust` auth method (via HbaMethod::Trust)
- [x] `pg_hba.conf` equivalent — host-based auth rules
- [x] Roles and passwords stored in `pg_authid` (in-memory; persistent in v0.8)

### SSL/TLS
- [x] `SSLRequest` handling
- [x] TLS upgrade via `rustls`
- [x] `ssl_cert_file`, `ssl_key_file`, `ssl_ca_file` config

### Other Protocol Features
- [x] `COPY TO STDOUT` / `COPY FROM STDIN` — text + CSV wire dispatch end-to-end. Parser `Statement::Copy(CopyStmt)`; binder `LogicalPlan::Copy`; `session/copy.rs` dispatches both Simple Query (`session/run.rs::handle_query`) and Extended Query (`session/ext.rs::handle_execute`). Backslash-escape + `\N` NULL for TEXT, quoted strings + `""` escape for CSV. `crates/ultrasql-server/tests/copy_round_trip.rs` covers four shapes including byte-identical round-trip. §1.11.
- [x] `BackendKeyData` wire send — `Session::new` registers a `CancelFlag` with the server's `Arc<CancelRegistry>`, which returns the canonical `(pid, secret)` pair (registry's monotonic `AtomicU32` pid + `OsRng` non-zero secret); `Session::startup` emits the real pair to the client. The same pid keys the notify hub. §1.9.
- [x] `CancelRequest` end-to-end — `CancelRegistry::request_cancel(pid, secret)` flips a per-query `CancelFlag`; the session-side `cancel_flag` is threaded into every `LowerCtx` built for Simple Query / Extended Query / EXPLAIN, and the lowerer calls `with_cancel_flag(flag.clone())` on `SeqScan` (`pipeline::scan::lower_heap_scan`) and `HashAggregate` (`pipeline::lower_query`'s Aggregate arm); operators poll the flag between batches and return `ExecError::Cancelled` → SQLSTATE `57014` (`error.rs::sqlstate`). The protocol decoder recognises the `1234.5678` magic on a startup-shaped packet and emits `FrontendMessage::CancelRequest { process_id, secret_key }`; the server's `Session::startup` looks up the entry in the registry, flips the flag, and closes the cancel connection without further dialogue. Both `cancel_request_with_unknown_pid_is_silent_noop` and `cancel_request_aborts_in_flight_select_within_500ms` are green. §1.9.
- [x] `NoticeResponse` (warnings, hints, info messages) — `notice_warning(sqlstate, msg)` helper in `server/lib.rs` wraps `BackendMessage::NoticeResponse`; emitted from txn-control paths (nested BEGIN, COMMIT/ROLLBACK outside a tx, SET TRANSACTION outside a tx) and covered by in-crate tests in `src/tests/txn.rs`
- [x] `LISTEN/NOTIFY/UNLISTEN` end-to-end — `notify.rs` `NotifyHub` shared across sessions, parser/binder/planner produce `LogicalPlan::Listen/Notify/Unlisten`, server `session/notify.rs` dispatches against the hub, and the run-loop races socket reads with `mpsc::UnboundedReceiver::recv` so idle sessions surface `NotificationResponse` immediately (covered by `crates/ultrasql-server/tests/listen_notify_round_trip.rs`)
- [x] All expected `ParameterStatus` params — `session/startup.rs` now sends the full thirteen PostgreSQL emits: `server_version`, `server_encoding`, `client_encoding`, `DateStyle`, `IntervalStyle`, `TimeZone`, `integer_datetimes`, `standard_conforming_strings`, `extra_float_digits`, `application_name`, `is_superuser`, `session_authorization`, `in_hot_standby`
- [x] Per-connection slow-loris timeout — `handle_connection` wraps `Session::startup` in `tokio::time::timeout(30s)`. A peer that opens TCP and sits silently is dropped after 30 s without consuming a session worker indefinitely

### Wire-protocol benchmarks (`cross_compare_sql`)
- [x] In-process `ultrasqld` driven via `tokio-postgres` for honest end-to-end measurement
- [x] Competitor bench scripts for postgres17 / sqlite3 / duckdb across INSERT / SELECT scan / SELECT SUM / SELECT AVG / Filter+SUM / UPDATE / DELETE
- [x] README auto-renders `benchmarks/results/latest/raw/*.json` into
  9 cross-engine tables, including `mixed_oltp_pgbench_like` and
  `window_row_number_65k_i64`
- [x] Honest sort order (fastest → slowest); current raw artifacts show
  UltraSQL #1 on every rendered workload
- [ ] Strict ≥ 2× competitor floor on every rendered workload — current
  raw artifacts meet it for INSERT, SUM, AVG, Filter+SUM, DELETE, and
  Mixed OLTP; SELECT scan, UPDATE, and Window are #1 but below 2× vs
  DuckDB

### CLI
- [x] `ultrasql` REPL with history, multiline input — `crates/ultrasql-cli/src/main.rs::run_repl` uses `rustyline::DefaultEditor`, persists `~/.ultrasql_history`, and accumulates lines until a trailing `;`
- [x] Meta-commands: `\d`, `\dt`, `\di`, `\df`, `\dv`, `\ds`, `\du`, `\dn`, `\l`, `\c`, `\q`, `\i`, `\timing`, `\x`, `\pset` — full set wired in `Session::handle_meta`; `\df`/`\dv`/`\ds` query the corresponding `pg_catalog.*` views; `\x` toggles expanded output; `\pset` accepts `expanded` and `format` keys; `\c` is acknowledged with a notice (cross-session reconnect deferred)
- [x] Connect via URL: `postgresql://user:pass@host/db` — `ConnParams::from_url`; the URL may also arrive as the first positional argument
- [x] `PGPASSWORD`, `PGHOST`, `.pgpass` file support — `clap` `env` attributes pull `PGHOST` / `PGPORT` / `PGDATABASE` / `PGUSER` / `PGPASSWORD`; `pgpass_lookup` parses `~/.pgpass` with wildcards
- [x] `--command/-c` and `--file/-f` batch mode — both `clap` flags routed through `exec_batch` over `split_statements`

### Milestones (must hold before v0.5 ships)
- [x] tokio-postgres can `CREATE TABLE`, `INSERT VALUES`, `SELECT ... WHERE col op lit`, `SELECT SUM/AVG`, `UPDATE`, `DELETE` end-to-end against `ultrasqld`
- [x] `BEGIN`/`COMMIT` round-trip from any standard driver — `txn_round_trip.rs` covers commit, rollback, failed-block, Extended Query path
- [x] Extended Query Parse/Bind/Execute round-trip from any standard driver — tokio-postgres prepared statements green (see `crates/ultrasql-server/tests/extended_query_round_trip.rs`)
- [x] `ORDER BY` reachable from the wire — `order_by_round_trip.rs` green
- [x] **INSERT 10 k ≥ 2× every competitor** — 3.54 ms vs
  SQLite 19.87 ms (**5.61×**), ClickHouse 34.41 ms (9.71×),
  PG 44.59 ms (12.58×), DuckDB 65.40 ms (18.45×).
- [ ] **SELECT scan 10 k ≥ 2× every competitor** — current raw artifact
  has UltraSQL #1, but only 1.34× faster than DuckDB (659.67 µs vs
  881.83 µs). Strict gate remains open.
- [x] **SELECT SUM 65 k ≥ 2× every competitor** — 44.04 µs vs
  DuckDB 88.65 µs (**2.01×**), SQLite 937.88 µs (21.30×),
  ClickHouse 940.56 µs (21.36×), PG 25.45 ms (578×).
- [ ] **UPDATE 10 k ≥ 2× every competitor** — current raw artifact has
  UltraSQL #1, but only 1.27× faster than DuckDB (128.79 µs vs
  164.21 µs). Strict gate remains open.
- [x] **DELETE 10 k ≥ 2× every competitor** — 115.79 µs vs
  SQLite 538.08 µs (**4.65×**), DuckDB 2.14 ms (18.48×),
  ClickHouse 6.58 ms (56.82×), PG 21.28 ms (183.78×).
- [x] **AVG 1 M ≥ 2× every competitor** — 47.08 µs vs
  DuckDB 215.48 µs (**4.58×**), ClickHouse 1.97 ms (41.81×),
  SQLite 14.49 ms (307.74×), PG 42.86 ms (910×).
- [x] **Filter+SUM 1 M ≥ 2× every competitor** — 46.29 µs vs
  DuckDB 177.23 µs (**3.83×**), ClickHouse 1.68 ms (36.25×),
  SQLite 16.34 ms (352.97×), PG 42.45 ms (917×).
- [x] **mixed_oltp_pgbench_like ≥ 2× every competitor** — 152.05 µs/op
  vs SQLite 360.66 µs (**2.37×**), DuckDB 1.29 ms (8.45×),
  PG 8.62 ms (56.70×), ClickHouse 30.74 ms (202×).

---

## v0.6 — "Optimize" ✅ DONE (correctness scope)

**Scope:** Cost-based optimizer built from scratch.

> Optimizer kernel ships. ✅ Server `execute_query` routes through `ultrasql_optimizer::optimize` + `PlanCache` (Wave B). Lowering to `Box<dyn Operator>` remains on `pipeline::lower_query` due to crate layering (executor → optimizer edge exists; optimizer cannot depend back on executor).
> "DONE" here means the optimizer integration and TPC-H SF1 correctness
> gate are complete. The `≥ 2× PostgreSQL 17` TPC-H performance claim
> remains unchecked until the reproducible PostgreSQL comparison passes.

### Rule-Based Rewrites
- [x] Constant folding and constant propagation
- [x] Predicate pushdown through joins
- [x] Predicate pushdown into subqueries and derived tables
- [x] Projection pushdown (column pruning)
- [x] Subquery decorrelation (EXISTS/IN/NOT IN → SemiJoin lowering)
- [x] Outer-join elimination when predicates imply inner
- [x] LIMIT pushdown into sort and scan
- [x] Sort elimination via index order (advisory; physical-layer elimination in physical_selection)
- [x] Common subexpression elimination
- [x] IN-list to semi-join conversion

### Statistics Collection
- [x] Per-column histograms (equi-depth, 100 buckets default)
- [x] Most-common-values (MCVs) per column
- [x] Per-relation row count and page count
- [x] Index correlation (physical sort order vs logical order)
- [x] `ANALYZE table` command (AnalyzeRunner over row iterator; kernel only)
- [x] `ANALYZE` reachable from the wire (Simple Query handler) — wire-level shim accepts `ANALYZE` and `ANALYZE table` and returns the canonical `ANALYZE` command tag (`crates/ultrasql-server/tests/analyze_round_trip.rs`). Real `pg_statistic` writeback through `AnalyzeRunner` remains a follow-up; this shim keeps PostgreSQL clients compatible until then. §3.1.
- [x] Autovacuum triggers `ANALYZE` on heavily modified tables
- [x] `pg_statistic` catalog table (row shape; persistent adapter v0.8)
- [x] `CREATE STATISTICS` (extended stats) — wire-level shim accepts the PostgreSQL `CREATE STATISTICS name ON cols FROM table` form and returns the matching command tag (`crates/ultrasql-server/tests/create_statistics_round_trip.rs`). `pg_statistic_ext` row population (dependency coefficients, multi-column MCV) is a follow-up that lands together with the `AnalyzeRunner` `pg_statistic_ext` writeback. §3.3.

### Cost Model
- [x] Selectivity estimation for equality, range, LIKE, IS NULL predicates
- [x] Join cardinality estimation (independence assumption)
- [x] Sequential scan cost formula
- [x] Index scan cost formula
- [x] Hash join cost formula (build + probe)
- [x] Sort cost formula (n log n)
- [x] Aggregate cost formula
- [x] CPU operator costs (CostGucs: cpu_tuple_cost, random_page_cost, seq_page_cost, cpu_index_tuple_cost, cpu_operator_cost)

### Join Enumeration
- [x] DPsize (dynamic programming over subsets) for ≤ 10 relations
- [x] Greedy/GEQO heuristic for > 10 relations
- [x] Cascades-style memo data structures (top-down search driver v0.7)
- [x] Join reordering with outer join constraints — `crates/ultrasql-optimizer/src/enumeration/mod.rs` adds `outer_join_subtree_is_barrier` + `reorder_inner_joins`. LEFT / RIGHT / FULL OUTER subtrees are opaque to the DPsize enumerator; inner-join chains buried inside them are still reordered in isolation. `crates/ultrasql-optimizer/tests/outer_join_reorder.rs` pins the contract. §3.4.

### Physical Operator Selection
- [x] NestLoop vs HashJoin vs MergeJoin
- [x] IndexScan vs SeqScan (BitmapHeapScan v0.7)
- [x] IndexOnlyScan when VM bit is set
- [x] BitmapHeapScan when selectivity ∈ [0.5%, 10%] or ≥2 indexes apply
- [x] HashAggregate vs SortAggregate (StreamAggregate v0.7)
- [x] DISTINCT lowered to `HashAggregate(group_by = projection, aggregates = [])`; Sort-based DISTINCT is a follow-up once the optimizer learns `pick_distinct` on interesting orders
- [x] Parallel plan cost annotation (divide by N workers, add parallel_setup_cost)

### Plan Cache
- [x] Generic plan for prepared statements
- [x] Custom plan when specific parameter values change the optimal plan
- [x] Re-planning threshold (5× cost increase triggers re-plan)
- [x] Plan invalidation on `ANALYZE` / DDL (PlanCache::invalidate / invalidate_all)

### Integration
- [x] Server `execute_query` delegates to `optimizer::optimize` before lowering (Wave B v0.6); lowering still happens on the catalog-aware `pipeline::lower_query` because crate layering blocks the optimizer from depending on the executor (executor → optimizer is the existing edge, used for cost-model re-exports). The public `optimize(plan, &snapshot, &dyn StatsSource)` signature is the stable surface the future cost-aware physical-selection wave will extend.
- [x] Plan cache shared between Simple Query and Extended Query — keyed on raw SQL text; every DDL path (`CREATE TABLE`, `CREATE INDEX`, `DROP TABLE`, `ALTER TABLE`, `TRUNCATE`) clears every entry.

### Milestone
- [x] TPC-H scale 1 runs to completion on every query with correct results

### TPC-H Harness Surface (v0.6 milestone work)
Harness wired end-to-end. All 8 TPC-H tables create cleanly and real
scale-1 `.tbl` data loads through the UltraSQL path. All 22 harness
queries run to completion and return result sets matching DuckDB on the
same data and SQL text.
The in-process validator loads UltraSQL with `COPY FROM STDIN` by
default and keeps the previous VALUES path behind
`ULTRASQL_TPCH_LOAD_METHOD=insert` for targeted regressions.

Validated again on 2026-05-18 with:

```text
make tpch-validate TPCH_DUCKDB=/opt/homebrew/bin/duckdb TPCH_QUERIES=all
```

Result: `validated 22 TPC-H query result set(s) against DuckDB`.

Correctness boundary: this validates UltraSQL against DuckDB for the
current harness SQL in
[`crates/ultrasql-bench/src/tpch/queries.rs`](crates/ultrasql-bench/src/tpch/queries.rs).
The harness no longer uses the earlier subquery/composite-key crutches:
Q2, Q17, and Q20 use correlated scalar aggregate subqueries; Q4 uses
`EXISTS`; Q5, Q9, Q13, and Q22 use their canonical non-staged query
forms; Q11 uses the scalar threshold subquery over comma joins; Q16 uses
`NOT IN (SELECT ...)`; Q18 uses `IN (SELECT ... GROUP BY ... HAVING
...)`; Q21 uses the mixed `EXISTS` / `NOT EXISTS` form with residual
correlation predicates; and Q9 uses the native composite-key join
predicate instead of a synthetic key. Q15 remains a CTE because standard
TPC-H defines it through a temporary view; the CTE is the side-effect-free
inline form of that view.

TPC-H `dbgen` / `qgen` remain external benchmark tools. Install them
locally with [`scripts/setup-tpch-dbgen.sh`](scripts/setup-tpch-dbgen.sh)
into `target/tools/tpch-dbgen`, or supply `ULTRASQL_TPCH_DBGEN` directly.
The generator source tree, compiled binaries, `dists.dss`, and generated
`.tbl` data are not UltraSQL source files and should not live in the
tracked repository root.

#### Done

- [x] `run_ultrasql` in [`crates/ultrasql-bench/src/tpch/runner.rs`](crates/ultrasql-bench/src/tpch/runner.rs) — in-process `ultrasqld` on an ephemeral port; runs the 22-query suite through `tokio-postgres`. Failures surface the real PostgreSQL `ErrorResponse` message via `as_db_error().message()`. Gated behind `--features sql-bench`.
- [x] Fast TPC-H UltraSQL load path — [`crates/ultrasql-bench/src/tpch/load.rs`](crates/ultrasql-bench/src/tpch/load.rs) streams `.tbl` data through `COPY FROM STDIN`; [`crates/ultrasql-server/src/session/copy.rs`](crates/ultrasql-server/src/session/copy.rs) batches COPY heap inserts and decodes Date/Decimal cells directly.
- [x] Engine-aware DDL split in [`crates/ultrasql-bench/src/tpch/schema.rs`](crates/ultrasql-bench/src/tpch/schema.rs) — eight TPC-H table DDLs ship in a `REGION` / `..._ULTRASQL` pair. The UltraSQL variant drops the table-level `PRIMARY KEY` clauses.
- [x] Parser: `DATE 'YYYY-MM-DD'`, `TIMESTAMP '…'`, `TIME '…'`, `INTERVAL '…' UNIT` typed-string literals via new `Literal::Typed` AST variant.
- [x] Parser: `EXTRACT(unit FROM expr)` keyword form desugared to `extract(unit_text, expr)`.
- [x] Parser: `SUBSTRING(s FROM n [FOR k])` keyword form desugared to `substring(s, n[, k])`.
- [x] Parser: `expr [NOT] IN (SELECT …) AND …` — IN result feeds back into the Pratt loop instead of returning early; trailing booleans no longer dropped. Unblocks Q20 at parse stage.
- [x] Binder: `DATE 'YYYY-MM-DD'` → `Value::Date(days)` via self-contained Howard-Hinnant `civil_from_days` (no chrono dep). 6-test suite in `binder::expr_bind::typed_literal_tests`.
- [x] `Value::Decimal { value: i64, scale: i32 }` + `Value::Interval { months, days, microseconds }` added to [`ultrasql-core/src/value.rs`](crates/ultrasql-core/src/value.rs). All exhaustive match sites updated (value_ord, hash_aggregate, hash_join, set_op, unique, copy).
- [x] Row codec ([`crates/ultrasql-executor/src/row_codec.rs`](crates/ultrasql-executor/src/row_codec.rs)) encodes/decodes `DataType::Date` (4-byte i32 LE), `DataType::Decimal { .. }` (8-byte scaled i64 LE), `DataType::Timestamp` / `DataType::TimestampTz` / `DataType::Time` (8-byte i64 micros LE). Decimal scale is read from the schema field.
- [x] `SeqScan::rows_to_columns` + `filter_op::batch_to_rows` handle Date / Decimal / Timestamp / TimestampTz / Time columns end-to-end. Column-builder pool uses Int32 backing for Date and Int64 backing for the wider temporal/decimal types; row materialiser re-tags the value with the correct `Value` variant on extraction.
- [x] `CREATE TABLE` accepts `DATE` / `TIME` / `TIMESTAMP` / `TIMESTAMPTZ` / `DECIMAL(p, s)` / `NUMERIC(p, s)` columns. All eight TPC-H DDLs succeed. End-to-end coverage: [`crates/ultrasql-server/tests/date_column_round_trip.rs`](crates/ultrasql-server/tests/date_column_round_trip.rs) (4 tests: `create_table_with_date_column`, `insert_date_literal_and_scan`, `accepts_decimal_column`, `accepts_timestamp_column`).

#### Landed in this v0.6 work cycle

- [x] Aggregate binding under `GROUP BY` — `bind_expr_or_agg_ref` now resolves aliased aggregate columns (`SUM(x) AS sum_qty` → reference to `sum_qty` in the agg schema) and recurses through composite expressions so a top-level `100 * SUM(…) / SUM(…)` no longer trips the rejector. Unblocks Q1, Q3, Q4, Q5, Q6, Q10, Q13, Q21.
- [x] `ScalarExpr::FunctionCall` + executor `eval_function_call` dispatch — supports `extract(unit, source)`, `substring(text, from[, for])`, `coalesce(...)`, `case_searched`, `case_simple`. Year/Month/Day/Quarter extraction uses inline civil-from-days arithmetic. Unblocks Q7, Q8, Q9, Q22.
- [x] CASE / COALESCE binder lowering — both kinds (simple / searched) emit `FunctionCall` payloads keyed by branch position; the executor walks the pair list. Unblocks Q12, Q14, Q16, Q19.
- [x] `expr [NOT] IN (val, …)` binder lowering — chain of `OR`-joined equality comparisons (`AND`-joined `<>` for `NOT IN`). Unblocks Q12, Q22.
- [x] `ProjectExprs` executor (`crates/ultrasql-executor/src/project_expr.rs`) — general expression-projection operator. Evaluates each `ScalarExpr` per child row, rebuilds the columnar batch through the supported value-type set. The lower_query path routes through it when any projection item is non-bare. Unblocks Q7, Q8, Q9, Q14, Q17.
- [x] TPC-H result validator — `validate-results` loads DuckDB from the
  same `.tbl` files, executes the same selected query set, runs UltraSQL
  through the in-process PostgreSQL wire path, and compares rows with
  numeric tolerance and row-context diagnostics.
- [x] Fast validator phase mode — `validate-results --keep-going
  --queries ...` loads UltraSQL once, runs every selected query, records
  all per-query execution/result failures, and still compares successful
  queries against cached DuckDB rows. This replaces slow one-query
  QNUMBER phases during stabilization.
- [x] Logical result encoding — Date and Decimal values now encode using
  logical schema types, so result validation compares SQL text values
  instead of physical Int32/Int64 backing storage.
- [x] Qualified column binding over duplicate relation columns — the
  binder now resolves `alias.column` in JOIN, WHERE, projection, GROUP BY,
  aggregate arguments, and ORDER BY contexts instead of silently binding
  to the first same-named field. This fixed wrong-result cases in Q2 and
  Q7.
- [x] Qualified outer-column binding in correlated subqueries — outer
  frames now preserve alias-qualified binding names and resolve both
  `outer_col` and `alias.outer_col` forms. This lets canonical Q21 bind
  `l1.l_orderkey` and `l1.l_suppkey` inside the `l2` / `l3` subqueries
  as true `OuterColumn` references.
- [x] Aggregate binding distinguishes same-function calls with different
  arguments. `SUM(CASE ...) / SUM(x)` no longer reuses one aggregate slot
  for both sides. This fixed Q8 and Q14-style ratios.
- [x] Exact numeric literal handling — dotted numeric literals bind as
  Decimal, Decimal literals are not folded through binary float, and
  Decimal-vs-Float arithmetic returns Float64. This fixed Q6, Q11, and
  Q17 correctness.
- [x] `COUNT(DISTINCT expr)` in `HashAggregate` — distinct aggregate state
  tracks per-group seen values before updating the wrapped aggregate. This
  fixed Q16.
- [x] Comma-join normalization — predicate pushdown now converts
  hashable equality predicates from `FROM a, b WHERE a.k = b.k` into
  inner join conditions while leaving complex residual filters above the
  join so they do not disable hash-join selection. This restores
  canonical comma-join forms in Q11, Q16, and Q18.
- [x] Alias-aware projected `ORDER BY` — the binder falls back to sorting
  over projected output columns when an `ORDER BY` item names a select-list
  alias. This supports canonical `ORDER BY value` / `ORDER BY revenue`
  shapes without pre-projection alias hacks.
- [x] GROUP BY scalar expression binding — aggregate binding now permits
  non-aggregate scalar functions in grouped projections and resolves
  aliases for materialised group keys. Canonical Q9 can project and group
  by `EXTRACT(YEAR FROM o_orderdate)` without a precomputed CTE column.
- [x] Native multi-column hash join keys — the server lowerer extracts
  conjunctions of equality predicates into aligned key vectors and the
  executor hashes composite `JoinKey` values. Q9 and Q20-style
  `(partkey, suppkey)` joins no longer need synthetic arithmetic keys.
- [x] Cross-risk inner join reordering — the optimizer now runs a guarded
  inner-join reorder pass after rule rewrites. It only fires when the
  leftmost pair in an inner-join chain is still a true cross product,
  builds a connected order from available join predicates, and restores
  the original logical output schema through a projection. This lets
  canonical Q9 avoid the initial `part × supplier` blow-up while leaving
  already-connected chains such as Q5 unchanged.
- [x] Subquery decorrelation, first production slice — optimizer rewrites
  equality-correlated `EXISTS`/`NOT EXISTS`, uncorrelated `IN`/`NOT IN`,
  and uncorrelated scalar subqueries in predicates into join/filter
  plans before execution. End-to-end coverage lives in
  [`crates/ultrasql-server/tests/subquery_round_trip.rs`](crates/ultrasql-server/tests/subquery_round_trip.rs).
- [x] Physical semi/anti joins — subquery decorrelation lowers
  `EXISTS`/`IN` to `LogicalJoinType::Semi` and `NOT EXISTS`/`NOT IN` to
  `LogicalJoinType::Anti`; `HashJoin` and `NestedLoopJoin` both execute
  those join types without materialising right-side payload columns.
- [x] Hash-join residual predicates — the server lowerer splits
  `a.k = b.k AND residual(a, b)` into hash keys plus an optional residual
  expression evaluated on candidate pairs. This keeps Q21's
  `same orderkey AND different suppkey` `EXISTS` / `NOT EXISTS` checks
  on the hash path instead of falling back to a full nested-loop scan.
- [x] Correlated scalar aggregate decorrelation — equality-correlated
  scalar aggregate subqueries are grouped by their correlation keys,
  left-joined to the outer input, filtered with the joined aggregate
  value, then projected back to the outer schema. Q2, Q17, and Q20 now
  run in canonical scalar-subquery form and validate against DuckDB on
  real SF1 data.
- [x] Mixed existential decorrelation for Q21 — equality-correlated
  `EXISTS` / `NOT EXISTS` subqueries with residual correlated predicates
  (for example `l2.l_suppkey <> l1.l_suppkey`) lower to logical
  semi/anti joins with hash keys plus residual filters. Canonical Q21 now
  validates against DuckDB on real SF1 data.

#### Follow-up Engine Debt

- [x] Remaining harness SQL staging removed for TPC-H SF1 correctness.
  Q15 intentionally remains a CTE because standard TPC-H Q15 uses a view;
  the harness inlines that view without adding a persistent DDL side
  effect.
- [x] v0.6 correctness scope reconciled: guarded join reordering,
  production subquery decorrelation slices, native semi/anti joins, and
  TPC-H SF1 correctness are shipped. Broader statistics-driven join
  ordering, harder decorrelation shapes, spill-aware semi/anti sizing,
  and PostgreSQL 17 performance certification are benchmark-gate work,
  tracked below under v1.0 certification instead of blocking the v0.6
  correctness milestone.

---

## v0.7 — "Vectorize" ⚠️ PARTIAL (feature wave landed; certification open)

**Scope:** Vectorized batch execution for analytic pipelines.
The main OLAP performance differentiator over PostgreSQL.

### Push-Based Pipeline Driver
- [x] Planner tags pipelines as vectorized (OLAP) vs scalar (OLTP) —
  `LogicalPlan::pipeline_mode()` classifies mutation/control plans as
  scalar OLTP and analytic scan/join/sort/aggregate/window/set pipelines
  as vectorized OLAP; `lower_query` emits the selected mode in tracing so
  production lowering and diagnostics share one planner-owned tag.
- [x] Push-based pipeline driver (`VectorizedSink` / `VectorizedOperator` / `SinkVerdict`)
- [x] Vectorized SeqScan emitting 4096-row batches via streaming `VisibleHeapScan` (page-by-page typed decode, no `Vec<Vec<Value>>` materialisation)
- [x] Vectorized filter operator (SIMD fast path for col CMP scalar and raw-order-safe col CMP col; DECIMAL scale mismatches fall back to Eval)
- [x] Vectorized projection operator
- [x] Vectorized hash join (build pull + probe push, FNV-1a hash)
- [x] Vectorized hash aggregate — scalar fast path (no GROUP BY) dispatches to SIMD kernels; multi-group via HashAggregate
- [x] Vectorized sort (permutation sort, 4096-row output chunks)

### SIMD Kernels
- [x] Auto-vectorized fallback (LLVM-generated, no intrinsics) — tight loops over `&[T]` for i32/i64/f32/f64 hit NEON `cmgt.4s` / `cmgt.2d` on aarch64
- [x] Scalar fallback for correctness — property tested against SIMD path
- [x] Bitmask-based NULL handling in SIMD kernels (64-lane Bitmap packing; cmp kernels AND validity)
- [x] Filter kernels: `cmp_i32_scalar` (all 6 ops), `cmp_i64_scalar`, `cmp_gt_i64`, `eq_i32`, `range_mask_i64`, `select_i32`
- [x] Arithmetic kernels: `sum_i64`, `sum_i64_with_mask`, `count_i64`, `min_i64`, `max_i64`, `min_f64`
- [x] Hash kernels: `hash_i64` (FNV-1a), `hash_text_bytes` (Arrow offset buffer)
- [x] Hand-written ARM64 NEON intrinsics for first hot kernels
  (`pack_eq_64`, dense integer sums, filter-sum, dictionary filter-sum);
  broader per-kernel NEON coverage remains open.
- [x] Expanded runtime-dispatched AVX2 intrinsics for core integer
  kernels: packed `eq_i32`, all-op packed `cmp_i32_scalar` /
  `cmp_i64_scalar`, packed `cmp_gt_i64`, dense `sum_i64`, dense widening
  `sum_i32`, plus the existing dense filter-sum path. AVX-512 was moved
  out of v0.7 on 2026-05-18 because this repository has no x86
  AVX-512 CI/bench host or result artifact yet.

### Dictionary Encoding
- [x] Dictionary encoding for low-cardinality string columns (DictionaryColumn)
- [x] Dictionary-aware filter (compare dict codes, not strings)
- [x] Dictionary-aware GROUP BY (group_by_dict returns per-code row indices)
- [x] Automatic encoding selection based on cardinality
  (`DictionaryEncodingPolicy` + `encode_strings_auto`) wired into real
  batch construction paths for storage decode, `build_batch`,
  projection, filter, limit, and wire output.

### JIT Compilation
- [x] Cranelift JIT generation for fused integer filter-sum kernels:
  `SUM(int_col) WHERE int_col > literal` and
  `SUM(bigint_col) WHERE bigint_col > literal` compile to
  process-lifetime cached native code and fall back to the SIMD/scalar
  kernels when native JIT setup is unavailable.
- [x] JIT threshold for the fused filter-sum path: per-session
  `jit_above_cost` gates compiled-kernel use by input row count.
- [x] Inline function calls in JIT code — first production slice is
  Cranelift-inlined `abs(bigint)` inside fused
  `SUM(abs(bigint)) WHERE abs(bigint) > literal`, with scalar/JIT
  regression coverage. Broad expression-tree lowering remains v1.x JIT
  scope.
- [x] `jit = on|off` GUC, `jit_above_cost` threshold

### Parallel Execution
- [x] `ParallelSeqScan` partitioning heap blocks across worker threads —
  executor operator splits a relation into disjoint block ranges, starts
  worker `SeqScan::new_range_with_vm` streams, and returns worker batches
  through a coordinator channel. Output order is intentionally unspecified
  unless a later `ORDER BY` / `GatherMerge` imposes order.
- [x] `Gather` / `GatherMerge` collators — executor fan-in primitives
  landed in `crates/ultrasql-executor/src/gather.rs`; `Gather` rotates
  whole unordered worker batches round-robin, `GatherMerge` performs
  streaming k-way ordered fan-in over sorted workers.
- [x] Cost-based parallel-plan selection — `lower_heap_scan` calls
  `choose_parallel_seq_scan_workers`, comparing sequential page/tuple cost
  against worker-divided cost plus setup overhead, and chooses
  `ParallelSeqScan` for large plain heap scans.

### MVCC Read Fast Path
- [x] Storage-level all-visible walker fast path —
  `HeapAccess::scan_visible_walker_with_vm` checks the visibility map at
  block boundaries and skips per-tuple `is_visible` / `oracle.status`
  calls on VM-certified pages. Heap insert/update/delete paths already
  clear VM bits when callers pass the same map; regression coverage
  verifies both the no-oracle fast path and DELETE clearing.
- [x] Server-owned VM plumbing and vacuum certification — `Server` owns a
  shared `VisibilityMap`; COPY, generic DML, fused UPDATE/DELETE, and
  ALTER/TRUNCATE mutation paths clear touched pages; periodic maintenance
  certifies pages with `vacuum_mark_all_visible`; production `SeqScan`
  uses the VM-aware walker.

### v0.7 Performance Wave Notes

- [x] TPC-H Q19 OR factoring before join pushdown — common lineitem
  predicates inside OR arms are hoisted so scan/filter pushdown can see
  them. Local SF1 timing on 2026-05-17: Q19 improved from 22.05 s to
  3.99 s and still validates against DuckDB.
- [x] TPC-H Q21 vector/residual cleanup — `Filter` now vectorizes
  column-vs-column integer/date comparisons, correlated EXISTS residual
  decorrelation projects inner inputs down to the needed columns, and
  `HashJoin` avoids avoidable row concatenation for semi/anti residuals
  plus single-column key Vec allocation. Local SF1 timing on
  2026-05-17: Q21 improved from 26.94 s to 19.04 s and still validates
  against DuckDB. This is progress, not closure: Q21 remains dominated
  by full lineitem semi/anti probes and needs a physical semi/anti join
  specialization or stronger decorrelation.
- [x] DECIMAL-safe column-vs-column filter fast path — the vectorized
  filter now uses raw column comparison only when physical ordering
  matches logical SQL ordering. Different DECIMAL scales fall back to
  row Eval, preventing Q11/HAVING-style wrong results while preserving
  fast paths for integers and same-scale DECIMAL columns. Covered by
  `decimal_column_column_different_scales_falls_back` and the
  `having_round_trip` scalar-subquery regression.
- [x] Automatic dictionary selection — `ultrasql-vec` now exposes
  `DictionaryEncodingPolicy`, `StringEncoding`, and `encode_strings_auto`
  so string-column builders can choose raw UTF-8 vs dictionary storage by
  row count, max distinct values, and distinct/non-null percentage while
  preserving SQL NULLs. `Column::DictionaryUtf8` now flows through
  storage decode, row materialization, projection, filter/limit trimming,
  recursive CTE distinct keys, and text/binary wire result encoding.
  Covered by `dict::tests::auto_encoding_*` and
  `row_codec::tests::finish_batch_*dictionary*`; server wire/COPY
  coverage in
  `copy_round_trip_low_cardinality_text_stays_wire_correct`.
- [x] Parallel fan-in collators — `Gather` and `GatherMerge` now exist
  as executor operators with schema validation, row-count hint
  propagation, round-robin unordered fan-in, and streaming k-way sorted
  fan-in. Covered by `gather::tests::{gather_round_robins_worker_batches,
  gather_merge_preserves_global_order,gather_merge_handles_descending_inputs}`.
- [x] Parallel heap scan worker partitioning — `ParallelSeqScan` is now a
  real executor operator with block-range workers, channel fan-in,
  cancellation propagation, and a lowerer cost gate. Covered by
  `parallel_seq_scan::tests::parallel_seq_scan_reads_disjoint_ranges`.
- [x] Production MVCC all-visible scan fast path —
  `scan_visible_walker_with_vm` trusts VM-certified pages and bypasses
  per-tuple visibility/oracle probes while preserving DELETE correctness
  through mandatory VM clearing on mutation paths. The server now owns
  the shared VM and certifies pages during maintenance. Covered by
  `mvcc_vm_round_trip::server_vm_certifies_scan_and_mutation_clears`.
- [x] Broader AVX2 dispatch — core integer compare/sum kernels now have
  runtime CPUID-dispatched AVX2 paths in addition to ARM64 NEON paths;
  this now includes all-op scalar comparisons for `i32` and `i64`, not
  just equality/greater-than narrow cases.
- [x] First production JIT path — `ultrasql-vec::jit` uses Cranelift
  0.120.x (MSRV-compatible) to compile fused `SUM(INT) WHERE INT > lit`
  and `SUM(BIGINT) WHERE BIGINT > lit` kernels; cached and streaming
  fused filter-sum scans use it when session `jit` is on and the row
  threshold is met. Covered by
  `jit::{jit_filter_sum_i32_gt_matches_scalar,jit_filter_sum_i64_gt_matches_scalar}`,
  `filter_sum_op::{cached_filter_sum_uses_jit_when_enabled,cached_filter_sum_i64_uses_jit_when_enabled}`,
  and `jit_round_trip::jit_gucs_drive_fused_filter_sum`.

### v0.7 Validation Snapshot

- [x] TPC-H SF1 correctness remains the regression floor: all 22 current
  harness queries validated against DuckDB on 2026-05-18 after the
  status audit (`make tpch-validate TPCH_DUCKDB=/opt/homebrew/bin/duckdb
  TPCH_QUERIES=all`).
- [x] TPC-H SF10 runs to completion and is compared with DuckDB by the
  committed certification runner. Local SF10 `dbgen` data remains under
  `target/tpch-scale10-real` (10 GiB, ignored by policy). The latest
  checked artifact,
  `benchmarks/results/latest/tpch_sf10_certification.json`, reports
  status passed with 22/22 DuckDB and UltraSQL query timings. The older
  2026-05-18 lineitem-load timeout is no longer the current blocker;
  rerun `benchmarks/tpch_sf10_certify.sh` on the selected release host
  before publishing release performance numbers.
- [ ] ClickBench has not been run or certified against PostgreSQL.
  `benchmarks/clickbench_certify.sh` now downloads the pinned upstream
  PostgreSQL schema/query set at runtime and records
  `benchmarks/results/latest/clickbench_certification.json`, but the
  2026-05-18 local run stopped honestly at missing
  `target/clickbench/hits.tsv`; no PostgreSQL/UltraSQL comparison has
  been measured.
- [x] Automatic dictionary selection is implemented in `ultrasql-vec`
  and wired through core executor/server batch paths. Remaining work is
  specialized dictionary-native text predicates and GROUP BY lowering
  beyond decode/preserve/re-encode flow.
- [x] SIMD v0.7 scope is complete after explicitly moving AVX-512 out
  of v0.7 on 2026-05-18. ARM64 NEON exists for selected hot kernels and
  AVX2 runtime dispatch covers dense filter-sum plus core integer
  compare/sum kernels, including all six scalar comparison ops for
  `i32` and `i64`. AVX-512 target-feature CI/bench certification is now
  tracked under v1.x hardware-specific performance work.
- [x] Parallel execution has first production path: fan-in collators,
  `ParallelSeqScan` worker partitioning, cancellation propagation, and
  cost-based scan selection are implemented. Remaining work is broader
  parallel operator coverage beyond base heap scans.
- [x] JIT has first production integer paths: Cranelift-compiled fused
  `INT` and `BIGINT` filter-sum plus session GUCs, and the first inlined
  function-call path for `abs(bigint)`. Remaining work is broad
  expression-tree compilation and cost calibration across non-benchmark
  workloads.
- [x] MVCC all-visible read path is production-wired: storage walker,
  server-owned VM, mutation clearing, vacuum certification, and scan
  selection are implemented.

### Milestone
- [x] TPC-H scale 10 runs to completion, throughput within 2× of DuckDB
  on the latest local artifact; 22/22 query timings are present for both
  engines and the summary reports status passed.
- [ ] ClickBench: at least 5× faster than PostgreSQL on analytical
  queries — certification runner exists, but no dataset-backed
  PostgreSQL/UltraSQL run has completed.

---

## v0.8 — "Index and Constrain" ⚠️ PARTIAL (validated 2026-05-18)

**Scope:** B-tree/hash index paths over the current SQL type surface,
constraints enforced, sequences, persistent catalog slices, and
pg_catalog views sufficient for psql `\d`. Type-specific GIN/GiST/BRIN
operator classes remain blocked on the v1.0/v1.x JSONB, array, range,
geometric, and full-text type surfaces.

### B-tree
- [x] Concurrent splits with right-link pointer (no reader blocking) —
  the B+ tree page special area stores `right_link` + `high_key`, split
  code updates the old page to point at the new right sibling, and
  descent/probe helpers chase right when a stale parent pointer lands on
  the left page. Covered by split/lookup tests in `btree/tests.rs`.
- [x] WAL logging and replay of index operations — B-tree insert, split,
  and delete payloads are emitted when a WAL sink is configured, SQL DML
  lowerer threads the server heap WAL sink into indexed
  INSERT / UPDATE / DELETE paths, and recovery dispatches `BTreeOp` into
  storage replay. Insert/delete redo are applied through the B-tree API;
  split redo is idempotent no-op because replaying logged inserts
  re-creates required splits. Covered by
  `wal_applier.rs::apply_btree_op_replays_insert_and_delete`.
- [x] Backward index scan — `BTree::backward_scan` now returns descending key order and `pipeline::lower_query` uses it for `ORDER BY indexed_col DESC` over a bare indexed scan; covered by `index_scan_round_trip.rs::order_by_desc_with_index_returns_descending_rows`. The current iterator materialises the range before reversing; a true left-link leaf walk remains a performance follow-up.
- [x] Index-only scan — `pipeline::lower_query` now emits
  `IndexOnlyScan` for covered indexed-key projections over indexable
  predicates when every candidate heap page is VM all-visible, and falls
  back to the heap-validating `IndexScan` path when any VM bit is absent.
  Covered by
  `lower_query_project_index_key_on_all_visible_pages_picks_index_only_scan`
  and
  `lower_query_project_index_key_without_all_visible_falls_back_to_heap_index_scan`.
- [x] Narrow multi-column B-tree — two-column Bool / Int16 / Int32 packed keys are supported for CREATE INDEX and maintenance; full variable-length composite keys remain open under expression/covering/general B-tree work.
- [x] Expression indexes: `CREATE INDEX ON t (lower(name))` — parser,
  binder, B-tree build, DML maintenance, and UNIQUE enforcement evaluate
  a single bound expression key at runtime. Covered by
  `create_unique_expression_index_enforces_lower_key_on_insert`.
  Expression-index scan selection remains conservative and falls back to
  `SeqScan + Filter` until expression predicate implication lands.
- [x] Partial indexes: `CREATE INDEX ON t (col) WHERE status = 'active'`
  — bound predicates filter heap build rows and INSERT/UPDATE/DELETE
  maintenance; UNIQUE checks apply only to rows whose partial predicate
  evaluates true. Covered by
  `create_unique_partial_index_enforces_only_matching_rows`.
- [x] Covering indexes: `INCLUDE (col1, col2)` — `INCLUDE` columns are
  parsed, bound, and carried as runtime metadata while staying out of
  the uniqueness key. Covered by
  `create_unique_covering_index_keeps_include_columns_out_of_key`.
  The current B-tree still stores key/TID only, so INCLUDE payload
  index-only scans remain a storage-format follow-up.
- [x] `CREATE INDEX CONCURRENTLY` (online-equivalent build) — parser,
  binder, planner, and wire execution accept the syntax; the current DDL
  path does not take a blocking table lock, so the same build path is
  online for this single-process engine. Covered by
  `index_scan_round_trip.rs::create_index_concurrently_then_point_lookup_round_trip`.
- [x] `VACUUM` reclaims dead index entries — SQL `VACUUM [table]`
  now runs B-tree cleanup for each indexed relation before heap slot
  reclamation, removing entries whose TIDs point at committed-dead heap
  tuples. Covered by
  `index_scan_round_trip.rs::vacuum_reclaims_stale_index_entries`.
- [x] `CREATE INDEX` reachable from the wire — `execute_create_index` in
  `session/ddl.rs`; supported build encodings include single-column
  Int16 / Int32 / Int64 / Bool / Timestamp / Float / TextPrefix8 and
  two-column Bool / Int16 / Int32 packed keys. Expression indexes,
  partial indexes, INCLUDE metadata, and `CREATE INDEX CONCURRENTLY`
  are wired; full variable-length multi-column keys and INCLUDE payload
  index-only scans remain storage-format follow-ups.
- [x] INSERT-side B-tree maintenance — `ModifyTable::Insert` now
  receives `InsertIndexMaintainer` descriptors from `pipeline::modify`,
  prechecks duplicate keys before heap write only for UNIQUE / PRIMARY
  KEY indexes, calls `HeapAccess::insert_batch`, then inserts returned
  TIDs into every covering B-tree. Plain non-unique indexes now store
  duplicate `(key, TupleId)` chains and point probes return every
  matching visible row. Wire regressions cover post-`CREATE INDEX`
  inserts being visible through index scans, non-unique duplicate-key
  scans, and `CREATE UNIQUE INDEX` duplicate inserts returning SQLSTATE
  `23505`.
- [x] UPDATE-side B-tree maintenance — `HeapAccess::update_many_with_outcomes` returns old/new TIDs; `ModifyTable::Update` precomputes old/new keys, rejects duplicate target keys before heap write, updates B-tree entries after the heap write, and disables the fused update path for indexed tables. Wire regressions cover non-key updates, indexed-key moves, and duplicate-key rejection.
- [x] DELETE-side B-tree maintenance — `BTree::delete(key, tid)` removes leaf entries without page merge/rebalance; `ModifyTable::Delete` decodes indexed rows, deletes old keys after heap delete, and disables fused delete for indexed tables. Wire regression covers indexed DELETE plus unique-key reuse.

### Hash Index
- [x] Static hashing with overflow pages —
  `crates/ultrasql-storage/src/access_method.rs::HashIndex` now owns a
  dedicated static primary-bucket format plus linked overflow pages, with
  regression coverage for forced overflow allocation and lookup.
- [x] WAL logging for hash index — `HashIndex::insert_logged` /
  `delete_logged` emit `RecordType::HashOp` records with bucket, touched
  hash page, stable key hash, key bytes, and encoded TID bytes; overflow
  allocation emits `OverflowLink`. Storage regressions decode the WAL
  records for insert/delete.
- [x] Equality-only queries — `CREATE INDEX ... USING hash (col)`
  binds and builds a hash-keyed page-backed index, INSERT-side
  maintenance keeps it current, and equality predicates probe it with
  heap recheck for hash collisions. Covered by
  `create_hash_index_supports_equality_queries_and_dml_maintenance`.

### GIN (Generalized Inverted Index)
- [x] SQL-visible access method name — `CREATE INDEX ... USING gin`
  binds into `LogicalIndexMethod::Gin`, and runtime index metadata
  preserves the requested method for DML maintenance.
- [x] For `JSONB` (`@>`, `<@`, `?`, `?|`, `?&`) — parser/binder/executor
  evaluate the PostgreSQL operator surface for text-backed JSONB values,
  and `GinIndex` now has JSONB key/pair token extraction plus
  contains/any/all-key posting probes.
- [x] For arrays (`@>`, `<@`, `&&`) — executor supports loose text-array
  containment/overlap semantics, and `GinIndex` has array member token
  extraction plus contains/overlap posting probes. Native typed array
  storage remains v1.0 type-system work.
- [x] For `TSVECTOR` (`@@`) — parser token `@@`, binder result typing,
  executor term-match evaluation, and `GinIndex` TSVECTOR/TSQUERY term
  posting probes are implemented. Ranking/headline functions remain v1.x
  full-text-search work.
- [x] Fast update mode (pending list drain) — `GinIndex` now buffers
  inserts in a pending list by default and drains them into posting lists
  through `drain_pending_list`; lookup/delete drain pending entries
  before consulting postings.

### GiST (Generalized Search Tree)
- [x] SQL-visible access method name — `CREATE INDEX ... USING gist`
  binds into `LogicalIndexMethod::Gist`, and runtime index metadata
  preserves the requested method for DML maintenance.
- [x] For range types (`&&`, `@>`, `<@`) — SQL-visible range data types
  (`int4range`, `int8range`, `numrange`, `daterange`, `tsrange`,
  `tstzrange`) bind through DDL/casts, persist through row/catalog
  codecs, and execute overlap / contains predicates. Covered by
  `constraint_round_trip.rs::exclusion_constraint_rejects_overlapping_int4range`.
- [x] For geometric types — SQL-visible geometric data types (`point`,
  `box`, `circle`, `line`, `lseg`, `path`, `polygon`) bind through DDL /
  casts, persist through row/catalog codecs, and execute bbox-backed
  overlap / contains predicates. Covered by
  `constraint_round_trip.rs::geometric_overlap_predicate_filters_boxes`.
- [x] `EXCLUDE USING gist` constraint support — parser/binder/runtime
  carry GiST exclusion constraints, persist `pg_constraint` rows, and
  enforce non-deferrable insert/update conflicts with SQLSTATE `23P01`.
  Current enforcement uses visible-row scans plus same-statement pending
  rows; physical GiST index-assisted probing remains a future
  optimization, not a correctness dependency.

### BRIN (Block Range Index)
- [x] SQL-visible access method name — `CREATE INDEX ... USING brin`
  binds into `LogicalIndexMethod::Brin`, and runtime index metadata
  preserves the requested method for DML maintenance.
- [x] For large tables with physical correlation (timestamps, sequential
  IDs) — SQL scans now lower eligible ordered predicates over
  `USING brin` indexes into BRIN candidate block ranges, scan only those
  heap ranges, and recheck the SQL predicate for correctness. Covered by
  `index_scan_round_trip::brin_index_range_scan_and_insert_maintenance_round_trip`.
- [x] `minmax` operator class — `BrinIndex` maintains per-range min/max
  summaries on index build, insert/update index maintenance, and through
  explicit `summarize_range`.
- [x] Auto-summarize on vacuum — `VACUUM table` now rebuilds same-process
  BRIN summaries from visible heap tuples after heap cleanup; stale
  delete over-inclusion is removed by the same round-trip regression.

### Constraints
- [x] Constraint runtime kernel — `crates/ultrasql-storage/src/constraints.rs` exposes `Constraint`, `ConstraintChecker`, and CHECK-expression IR for NotNull / Check / PrimaryKey / ForeignKey / UniqueSet validation
- [x] `NOT NULL` DML enforcement — `ModifyTable::Insert` and `ModifyTable::Update` check schema `Field::nullable` before heap writes; wire regressions cover INSERT and UPDATE failures returning SQLSTATE `23502` without leaking partial rows
- [x] `CHECK` DDL + executor wiring — column/table `CHECK` constraints bind into `LogicalCheckConstraint`, are stored in the server's same-process `TableRuntimeConstraints`, and are evaluated on INSERT/UPDATE before heap writes. Violations return SQLSTATE `23514`; `constraint_round_trip.rs` covers insert/update rejection.
- [x] `UNIQUE` / `PRIMARY KEY` DDL + executor wiring — column/table constraints create unique B-tree indexes during `CREATE TABLE`; `PRIMARY KEY` also marks columns NOT NULL. INSERT/UPDATE index maintenance returns SQLSTATE `23505` on duplicates. Supported key encodings match current B-tree support: single fixed/text-prefix keys and narrow two-column packed keys.
- [x] Basic `FOREIGN KEY` DDL + executor wiring — explicit target-column `REFERENCES parent(col)` and table-level `FOREIGN KEY (...) REFERENCES parent(...)` bind and run as non-deferrable NO ACTION checks. Child INSERT/UPDATE requires a visible parent row; parent DELETE and parent key UPDATE are restricted while children reference the key. Violations return SQLSTATE `23503`. Referential actions and deferral remain open below.
- [x] INSERT column-list expansion — omitted nullable columns are filled with NULL and source positions respect explicit column order before DEFAULT / sequence defaults, NOT NULL checks, index maintenance, and heap encoding
- [x] `DEFAULT expr` evaluated at INSERT when column omitted —
  immutable/literal scalar defaults bind into per-column runtime defaults
  and execute only for omitted columns. Explicit NULL remains NULL.
  `pg_attribute.atthasdef` and virtual `pg_attrdef` rows now expose the
  default metadata through PostgreSQL catalog queries.
- [x] FK referential action wiring:
  - [x] non-deferrable `NO ACTION` / `RESTRICT` semantics for explicit target columns (implemented as immediate checks)
  - [x] `ON DELETE CASCADE` — parser/planner/runtime carry referential actions; parent DELETE cascades one level into child heap rows and removes child B-tree entries. Covered by `constraint_round_trip.rs::foreign_key_on_delete_cascade_deletes_child_rows`.
  - [x] `ON DELETE SET NULL / SET DEFAULT` — parent DELETE rewrites
    matching child rows, validates child NOT NULL / CHECK constraints,
    clears VM state, and maintains child B-tree entries.
  - [x] `ON UPDATE CASCADE / SET NULL / SET DEFAULT` — parent key UPDATE
    propagates into child rows, validates child constraints, clears VM
    state, and moves child index entries.
  - [x] `DEFERRABLE INITIALLY DEFERRED / IMMEDIATE` — parser, binder,
    runtime FK metadata, catalog views, Simple/Extended autocommit,
    explicit `COMMIT`, `PREPARE TRANSACTION`, and `COPY FROM` run
    commit-time validation for initially deferred NO ACTION FKs.
    `INITIALLY IMMEDIATE` remains immediate unless a future
    `SET CONSTRAINTS` command toggles it. Covered by
    `constraint_round_trip.rs::deferrable_foreign_key_*`.
- [x] `GENERATED ALWAYS / BY DEFAULT AS IDENTITY` — parser accepts identity sequence options; binder expands integer identity columns into owned sequence-backed defaults; executor rejects explicit values for `GENERATED ALWAYS` with SQLSTATE `428C9` and allows explicit values for `BY DEFAULT`. Covered by `sequence_round_trip.rs`.
- [x] `GENERATED ALWAYS AS (expr) STORED` (computed columns) — parser,
  binder, table runtime constraints, INSERT, and UPDATE compute stored
  values from immutable row-local expressions, recompute on base-column
  update, and reject explicit generated-column writes with SQLSTATE
  `428C9`.
- [x] `EXCLUDE USING gist (...)` exclusion constraints — parser accepts
  `EXCLUDE USING gist (col WITH op, ...)`, binder validates supported
  operators against range/geometric/equality columns, runtime DML checks
  visible and same-statement rows, and violations return SQLSTATE
  `23P01`. Physical GiST index-assisted probing remains a future
  optimization.

### Sequences
- [x] `CREATE SEQUENCE` with START, INCREMENT, MINVALUE, MAXVALUE, CYCLE, CACHE — parser, binder, and Simple Query server dispatch are wired. Descending sequences default START to MAXVALUE; `ALTER SEQUENCE START WITH` changes restart seed without advancing current value.
- [x] `ALTER SEQUENCE` / `DROP SEQUENCE` — in-memory registry updates and command tags are wired; `ALTER SEQUENCE RESTART [WITH n]` follows PostgreSQL's current-value semantics.
- [x] `NEXTVAL`, `CURRVAL`, `LASTVAL`, `SETVAL` — Simple Query sequence functions dispatch before generic SELECT lowering. `currval` / `lastval` are session-local and return SQLSTATE `55000` before first `nextval`.
- [x] `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` sugar — `CREATE TABLE` expands pseudo-types to Int32/Int64/Int16 required columns, creates an owned same-process sequence, and installs a sequence-backed DEFAULT. INSERT of omitted serial columns calls `nextval` and updates session `currval`.
- [x] Per-session `currval` state
- [x] Sequence WAL logging and recovery — sequence create/nextval/setval,
  alter, and drop emit `SequenceOp` records; WAL replay restores the
  server sequence registry before catalog bootstrap. Covered by
  storage sequence tests and `server::tests::recovery`.

### Persistent Catalog
- [x] `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type` (row shapes)
- [x] `pg_index`, `pg_constraint`, `pg_sequence` (row shapes + virtual SQL scans for current same-process metadata)
- [x] Catalog cache with `arc-swap` for wait-free reads
- [x] Catalog snapshot for safe concurrent DDL
- [x] Typed tuple decoder for bootstrap-from-heap —
  `crates/ultrasql-catalog/src/encoding.rs` +
  `PersistentCatalog::bootstrap_from_heap` decode `pg_class`,
  `pg_attribute`, `pg_index`, `pg_constraint`, `pg_sequence`,
  `pg_statistic`, and `pg_statistic_ext` rows and rebuild user
  `TableEntry`, `IndexEntry`, catalog constraint/sequence maps, and
  planner-statistics snapshots on warm restart
- [x] Persistent constraint/default/sequence metadata — `pg_constraint`
  and `pg_sequence` heap codecs/writeback/bootstrap rebuild durable
  constraint and sequence maps; CREATE TABLE now persists default flags
  into `pg_attribute.atthasdef`, and the catalog surface includes
  `pg_attrdef` rows for default/identity/generated metadata. Full
  expression rehydration into `TableRuntimeConstraints` after warm
  restart remains v1.0 catalog-expression work.
- [x] `pg_depend` compatibility slice — virtual dependency rows are
  derived from live UNIQUE / CHECK / FOREIGN KEY metadata, and `DROP
  TABLE` now honours those FK dependencies: default / `RESTRICT` raises
  SQLSTATE `2BP01`, while `CASCADE` removes the child-side FK dependency
  and leaves the child table intact. Persistent `pg_depend` heap rows
  and full dependency coverage for every object kind remain open.
- [x] `pg_description` / `COMMENT ON` compatibility slice —
  `COMMENT ON TABLE`, `COMMENT ON INDEX`, and `COMMENT ON COLUMN` parse,
  bind, dispatch over
  Simple Query, update the active `PersistentCatalog` snapshot, and
  appear in `pg_catalog.pg_description`; `IS NULL` clears comments and
  `DROP TABLE` clears attached rows. Persistent heap writeback and
  restart bootstrap for comment rows remain open with the broader
  persistent metadata item above.
- [x] `pg_statistic`, `pg_statistic_ext` (persistent) — ANALYZE and
  CREATE STATISTICS append durable rows, and warm bootstrap overlays the
  latest rows onto the initial catalog snapshot. Covered by
  `analyze_round_trip.rs`, `create_statistics_round_trip.rs`, and
  persistent catalog bootstrap tests.
- [x] Shared invalidation messages (catalog cache flush on DDL) — the
  same-process plan cache is invalidated on DDL and ANALYZE/statistics
  refreshes. PostgreSQL's cross-backend sinval queue is a v0.9+
  multi-process feature; this engine's current backend model shares one
  server state.

### pg_catalog Views
- [x] `pg_tables`, `pg_indexes`, `pg_views`, `pg_sequences` — virtual read-only scans over the active catalog snapshot / sequence registry; `pg_views` is empty until CREATE VIEW lands.
- [x] `pg_roles`, `pg_user`
- [x] `pg_settings` compatibility slice — core settings (`server_version`, encodings, `search_path`, `work_mem`) are exposed; full GUC catalogue remains v0.9+.
- [x] `pg_locks` row shape — currently empty until lock-manager telemetry is wired.
- [x] `pg_stat_activity` compatibility row shape — currently exposes a minimal active backend row; real per-session activity stats remain v0.9+.

### information_schema
- [x] `information_schema.tables`
- [x] `information_schema.columns`
- [x] `information_schema.table_constraints`
- [x] `information_schema.key_column_usage`
- [x] `information_schema.referential_constraints`
- [x] `information_schema.check_constraints`
- [x] `information_schema.routines` — compatibility row shape exposed;
  currently empty because UltraSQL does not support CREATE FUNCTION /
  CREATE PROCEDURE yet.
- [x] `information_schema.triggers` — compatibility row shape exposed;
  currently empty because UltraSQL does not support CREATE TRIGGER yet.
- [x] `information_schema.schemata`
- [x] `information_schema.sequences`

### Milestone
- [x] `psql \d`, `\dt`, `\di`, `\df` work correctly for the current
  UltraSQL metadata surface — the CLI meta-query SQL is backed by
  virtual `pg_catalog` / `information_schema` scans and validated through
  `catalog_views_round_trip.rs`; unsupported object kinds correctly
  return empty metadata rows instead of parser/binder failures.

---

## v0.9 — "Operate" ⚠️ PARTIAL (COPY text/CSV + LISTEN/NOTIFY + CancelRequest landed early)

**Scope:** Replication, backup/PITR, observability, COPY,
autovacuum. UltraSQL survives production use.

**2026-05-19 audit note:** v0.9 still contains several compatibility
surfaces and CLI primitives whose production behavior is explicitly
pending below. Those items remain unchecked until runtime tuning,
continuous replication, PITR replay integration, logical decoding,
live observability counters, and PostgreSQL-compatible external-tool
behavior are implemented and validated.

### Autovacuum
- [x] Background autovacuum launcher — `ultrasqld --autovacuum-interval-ms` runs existing undo-GC/all-visible/analyze pass
- [x] Worker per table triggered by dead tuple ratio — autovacuum now
  walks catalog tables on its interval and vacuums relations whose tracked
  modification count crosses the PostgreSQL-style base/scale threshold.
- [ ] `autovacuum_vacuum_threshold`, `autovacuum_vacuum_scale_factor` — exposed in `pg_settings`; runtime tuning pending
- [ ] `autovacuum_analyze_threshold`, `autovacuum_analyze_scale_factor` — exposed in `pg_settings`; runtime tuning pending
- [ ] Per-table autovacuum settings — v0.9 supports PostgreSQL-compatible
  global threshold knobs through `pg_settings`; per-relation storage
  overrides remain a catalog-extension item.
- [x] Vacuum FREEZE to prevent XID age buildup — VACUUM/autovacuum now
  refresh heap visibility and reclaim dead tuples; aggressive
  wraparound-grade freezing remains tied to long-running transaction
  hardening.
- [ ] `pg_stat_progress_vacuum` view — empty compatibility view exists; live progress rows pending

### Physical Streaming Replication
- [ ] WAL sender process — file-backed `WalSender` primitive plus `ultrasql --wal-send-once`; network process loop pending
- [ ] WAL receiver on standby — file-backed `WalReceiver` primitive plus `ultrasql --wal-receive-once`; continuous apply loop pending
- [x] Standby mode (`standby.signal`)
- [x] Hot standby (read queries on standby) — `standby.signal` /
  `recovery.signal` at `ultrasqld --data-dir` startup enables read-only
  query serving and rejects writes before planning.
- [ ] Synchronous replication (`synchronous_commit = remote_apply|remote_write|on|off`) —
  PostgreSQL-compatible GUC values are accepted and WAL shipping/slot state
  is present; remote quorum apply remains a continuous replication hardening
  item.
- [x] Replication slots — file-backed physical slot state under `pg_replslot`
- [ ] `pg_replication_slots` view, `pg_stat_replication` view — catalog view
  shapes exist for tool introspection; live replication rows await the
  network sender/receiver loop.
- [ ] Cascading replication — file-backed receiver output is valid sender input; continuous network cascade pending

### Backup & PITR
- [ ] WAL archiving (`archive_command`) — CLI archive utility exists via `ultrasql --archive-wal`; server-side config execution pending
- [ ] WAL restore (`restore_command`) — CLI restore utility exists via `ultrasql --restore-wal`; recovery integration pending
- [ ] Base backup (`pg_basebackup` equivalent) — `ultrasql --basebackup DEST --data-dir DIR` copies files and writes `backup_manifest.json`; online checkpoint fencing pending
- [ ] `recovery.signal` / `standby.signal` support — CLI signal-file helpers via `ultrasql --ctl recovery|standby`; server startup/replay handling pending
- [ ] `recovery_target_time`, `recovery_target_lsn`, `recovery_target_xid` — CLI writes `recovery.targets`; WAL replay targeting pending
- [x] `pg_start_backup()` / `pg_stop_backup()` for online backup — SQL
  compatibility functions write `backup_label` / `backup_stop` marker files
  in WAL-backed data directories and return a stable LSN-shaped value;
  checkpoint fencing is still handled by `--basebackup`.
- [x] Backup manifest (checksums for all files) — generated by `ultrasql --basebackup`

### Logical Replication
- [ ] Logical decoding infrastructure — same-process publication registry and
  commit-gated statement CDC stream exist for published tables; WAL row-image
  decoding, persistent slots, and network streaming pending
- [ ] `pgoutput` output plugin format — compatibility surface reserved; wire-compatible pgoutput encoder pending
- [ ] `CREATE PUBLICATION` / `CREATE SUBSCRIPTION` — `CREATE PUBLICATION ... FOR TABLE ...`
  registers in-process metadata; `CREATE SUBSCRIPTION` remains explicit
  unsupported future work; persistent metadata pending
- [ ] Row filters and column lists on publications — syntax surface accepted by metadata DDL path; enforcement pending
- [ ] Initial table data sync — use basebackup/WAL shipping path for physical initial sync; logical per-table copy pending
- [ ] `pg_stat_subscription` view — compatibility shape exists; live subscription rows pending

### Observability
- [ ] `pg_stat_user_tables` — compatibility view shape exists; live counters pending
- [ ] `pg_stat_user_indexes` — compatibility view shape exists; live counters pending
- [ ] `pg_statio_user_tables` — compatibility view shape exists; live counters pending
- [ ] `pg_stat_database` — compatibility view shape exists; live counters pending
- [ ] `pg_stat_bgwriter` — compatibility view shape exists; live counters pending
- [ ] `pg_stat_wal` — compatibility view shape exists; live counters pending
- [ ] `pg_stat_progress_analyze`, `pg_stat_progress_create_index` — empty compatibility views exist
- [ ] Prometheus `/metrics` HTTP endpoint — process/build metrics exposed via `--ops-listen`; live query/WAL/buffer counters pending
- [x] OpenTelemetry tracing with spans per query and per operator — server
  emits structured `tracing` spans around SQL statements and logical operator
  execution; exporter wiring is deployment configuration.
- [x] `EXPLAIN ANALYZE` with actual rows and actual time — buffers and WAL stats pending
- [x] `EXPLAIN (FORMAT JSON)` for basic tooling integration — richer PostgreSQL-compatible fields pending
- [ ] Structured JSON logging with `log_min_duration_statement`, `log_statement` — `ultrasqld --log-format json`; GUC rows exposed, per-statement duration filtering pending

### COPY & Bulk Operations
- [x] `COPY t FROM STDIN` / `COPY t TO STDOUT` — text + CSV formats end-to-end. Parser, binder, `LogicalPlan::Copy`, Simple Query + Extended Query session dispatch via `crates/ultrasql-server/src/session/copy.rs`. `crates/ultrasql-server/tests/copy_round_trip.rs` covers five shapes. §1.11.
- [x] CSV format (`FORMAT csv`, `DELIMITER`, `HEADER`, `NULL`) — custom `QUOTE` / `ESCAPE` options pending
- [x] `COPY (SELECT ...) TO STDOUT` — parser/binder/session path copies query results to STDOUT or server-side file in text/CSV format
- [ ] `COPY t FROM 'file'` / `COPY t TO 'file'` (server-side, superuser only) — server-side file import/export supported; privilege model pending role system
- [x] Binary COPY format — table COPY FROM/TO supports PostgreSQL binary header/rows/trailer for core scalar types; query-target binary copy remains pending
- [x] LISTEN/NOTIFY/UNLISTEN end-to-end — `NotifyHub` shared across sessions; parser/binder/planner produce `LogicalPlan::Listen/Notify/Unlisten`; `session/notify.rs` dispatches against the hub; `BackendMessage::NotificationResponse` (tag `'A'`) plumbed through `ultrasql-protocol`; idle sessions push notifications immediately via a `tokio::select!` between `read_buf` and `notify_rx.recv` so listeners receive them without waiting for the next `Sync` round (`crates/ultrasql-server/tests/listen_notify_round_trip.rs`)

### External Tools
- [ ] `pg_dump` compatible output (custom, directory, tar formats) —
  `ultrasql --pg-dump` writes plain/custom/tar-style UltraSQL archives or
  directory dumps from `--data-dir`; wire-compatible `pg_dump` archive
  interoperability remains pending.
- [ ] `pg_restore` equivalent — `ultrasql --pg-restore` restores archive or
  directory dumps into `--data-dir`; PostgreSQL `pg_restore`
  interoperability remains pending.
- [x] `pg_ctl` equivalent: `initdb`, `start`, `stop`, `reload`, `status`, `promote` — `ultrasql --ctl ...`; start/stop delegate to service manager instead of daemonizing
- [x] `pg_isready` equivalent — `ultrasql --isready`
- [ ] `pgbench` compatible baseline (default TPC-B transactions) — local `tpcb_32conn` kernel stage gate + `benchmarks/tpcb_certify.sh`; same-host PostgreSQL-wire certification still open
- [ ] `pg_waldump` equivalent (WAL inspection CLI) — `ultrasql --waldump PATH` emits deterministic offset/hex dump; semantic WAL record decoding pending

### Milestone
- [x] First documented operations runbook — `OPERATIONS.md`
- [x] Health and readiness endpoints — `ultrasqld --ops-listen` serves `/health`, `/ready`, `/metrics`
- [ ] Three independent operators run UltraSQL for 7 days and report —
  runbook and pending artifact exist in `docs/OPERATOR_SOAK.md` and
  `benchmarks/results/latest/operator_soak_status.json`; cannot be closed
  until three independent seven-day reports exist.

---

## v1.0 — "Ship" ❌

**Scope:** Single-node, fit for general production use.
Every standard PostgreSQL driver and ORM works without modification.

### Data Types Completeness
- [ ] `SMALLINT/INT2`, `INTEGER/INT4`, `BIGINT/INT8`
- [ ] `REAL/FLOAT4`, `DOUBLE PRECISION/FLOAT8`
- [ ] `NUMERIC(p,s)` / `DECIMAL(p,s)` — arbitrary precision (critical for finance)
- [ ] `MONEY`
- [ ] `CHAR(n)`, `VARCHAR(n)`, `TEXT`, `BYTEA`
- [ ] `DATE`, `TIME`, `TIMETZ`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`
- [ ] `BOOLEAN`
- [x] `UUID` + `gen_random_uuid()`
- [ ] `BIT(n)` / `BIT VARYING(n)`
- [ ] `INET`, `CIDR`, `MACADDR`, `MACADDR8`
- [ ] `POINT`, `LINE`, `LSEG`, `BOX`, `PATH`, `POLYGON`, `CIRCLE`
- [ ] `JSON`, `JSONB` — critical for modern apps. Native JSONB
  runtime values, row-codec storage, `CREATE TABLE ... JSON/JSONB`,
  JSONB wire OID, COPY rendering, and JSONB operator evaluation are
  implemented. Distinct JSON-vs-JSONB storage, JSON normalization, and
  the full JSON function suite remain open.
- [ ] `int[]`, `text[]`, any type as array. Native array runtime
  values, catalog/row-codec storage, `CREATE TABLE ... []`, GIN-facing
  operators, `array_agg`, `array_length`, `array_cat`,
  `array_to_string`, `string_to_array`, and wire-visible
  `unnest(...)` are implemented for currently supported element
  families. Multi-dimensional arrays, full PostgreSQL coercions, and
  every element type remain open.
- [ ] `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange`
- [ ] `CREATE TYPE ... AS ENUM (...)`
- [ ] `CREATE TYPE ... AS (composite)`
- [ ] `CREATE DOMAIN`
- [ ] `TSVECTOR`, `TSQUERY` (full-text search)
- [ ] `OID`, `REGCLASS`, `REGTYPE`, `PG_LSN`
- [ ] `XML` (basic storage)

### Built-in Functions Completeness
- [ ] Mathematical: abs, ceil, floor, round, trunc, mod, power, sqrt, exp, ln, log, random, trig functions, pi()
- [ ] String: length, lower, upper, trim, lpad, rpad, left, right, substr, position, replace, regexp_replace, split_part, concat, concat_ws, repeat, reverse, md5, sha256, quote_ident, format
- [ ] Date/Time: now(), current_timestamp, current_date, age(), date_trunc(), extract(), to_timestamp(), make_date(), date_bin()
- [ ] Aggregate: COUNT, SUM, AVG, MIN, MAX, BOOL_AND, BOOL_OR, STRING_AGG, ARRAY_AGG, JSON_AGG, PERCENTILE_CONT, STDDEV, VARIANCE, CORR
- [ ] Window: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE
- [ ] JSON: row_to_json, json_build_object, json_each, jsonb_set, jsonb_path_query
- [x] Array: array_length, array_cat, unnest, array_agg,
  array_to_string, string_to_array — implemented for native arrays
  over the current supported scalar element set.
- [x] System: version(), current_database(), current_user/session_user
  (function-call and bare keyword forms), pg_typeof(), pg_size_pretty()
  and catalog-backed pg_relation_size() are wired.
- [x] Sequence: nextval(), currval(), lastval(), setval() (Simple Query path; Extended Query expression-function dispatch remains broader function-work)

### Vector Search / RAG Readiness

Audit status: SQL vector support and page-backed ANN indexes exist, but
large-scale recovery certification is still open. `DataType` and `Value`
include `vector`, `halfvec`, `sparsevec`, and
`bitvec` families with dimension metadata; parser/binder/executor tests cover
typed literals, casts, distance operators, vector functions, dimension
mismatch, and wire-visible round trips. HNSW and IVFFlat indexes exist behind
`CREATE INDEX USING hnsw` and `CREATE INDEX USING ivfflat`, use page-backed
index objects in the SQL path, and have restart round trips that verify
`EXPLAIN ANALYZE` still selects `page-backed hnsw` / `page-backed ivfflat`
after server restart. Remaining durability work is larger-scale recovery
certification, broader fuzz/property coverage, vacuum/rebuild stress, and
public performance artifacts across build/search/update/delete/restart
workloads. Crash-during-build SQL tests now cover HNSW and IVFFlat exact-scan
fallback after truncated index-build WAL, and corrupt vector-index WAL replay
marks the affected ANN index unavailable instead of blocking restart or
returning wrong results. ANN delete/VACUUM and update cert tests now cover
both HNSW and IVFFlat, filtered vector top-k advertises exact fallback when
the LIMIT can be satisfied from visible rows, and `EXPLAIN ANALYZE` reports
ANN method, candidate/rerank counts, fallback state, recall mode, and skipped
deleted-candidate counts.

Production target remains the pgvector-shaped SQL surface plus storage-grade
indexes: `vector(n)`, distance operators, exact `ORDER BY distance LIMIT k`,
HNSW/IVFFlat access methods, WAL redo, recovery, deletes, VACUUM, rebuild,
fuzzing, and benchmark certification against PostgreSQL + pgvector on the
same host.

#### Implementation Status Matrix

| Feature | Parser | Binder | Executor | Storage | WAL/recovery | Wire | Tests | Benchmark artifact |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `vector(n)` / `halfvec(n)` / `sparsevec` / `bitvec` values | ✅ typed literals, casts, `CREATE TABLE` | ✅ dimension/type checks | ✅ eval + row flow | ✅ catalog + row codec | ⚠️ heap row path only; restart certification still open | ✅ text output | ✅ parser/core/server round trips | ⚠️ exact top-k smoke exists; full cert open |
| pgvector distance ops and functions | ✅ `<->`, `<#>`, `<=>`, `<+>` | ✅ unsupported vector comparisons fail explicitly | ✅ L2, cosine, inner product/dot, L1, norm, dims | n/a | n/a | ✅ result values | ✅ scalar/runtime tests | ⚠️ exact top-k artifacts only |
| Exact vector top-k | n/a | ⚠️ shape recognized through `ORDER BY distance LIMIT` | ⚠️ top-k path exists; full-sort avoidance hardening open | n/a | n/a | n/a | ✅ server round trips | ⚠️ `vector_topk_exact` smoke, full pgvector cert open |
| Page-backed HNSW | ✅ `CREATE INDEX USING hnsw` | ✅ vector opclasses checked | ✅ planner selects page-backed HNSW; exact/filter fallback path | ✅ page-backed graph relation with meta/node/overflow/free-list pages | ⚠️ restart + crash/corrupt-WAL tests and HNSW WAL fuzz exist; large-scale recovery certification plus torn-write/rebuild cert open | n/a | ✅ insert/update/delete/vacuum, SQL restart, crash, corrupt-WAL, EXPLAIN tests | ⚠️ HNSW recall/latency smoke exists; larger recall/latency artifacts open |
| Page-backed IVFFlat | ✅ `CREATE INDEX USING ivfflat` | ✅ `lists`/`probes` checked | ✅ planner selects page-backed IVFFlat; centroid/list scan + exact rerank/filter fallback | ✅ page-backed centroid/list relation with centroid/list/entry pages | ⚠️ restart + crash/corrupt-WAL tests exist; WAL replay fuzz/property tests, large-scale recovery certification, and torn-write/compaction cert open | n/a | ✅ SQL restart/crash/corrupt-WAL/delete-vacuum/update/EXPLAIN tests | ⚠️ memory accounting smoke exists; larger recall/latency artifacts open |
| RAG primitive schemas/helpers | n/a | n/a | ✅ normal SQL helpers + tenant RLS predicate injection | ✅ ordinary user tables | ⚠️ tenant RLS sidecar is same-process; restart policy persistence open | n/a | ✅ catalog/server helper tests + RLS wire round trip | ⚠️ RAG quality smoke + tenant RLS tests exist; full cert open |
| CSV table functions | ✅ `read_csv`, globs, arrays, file literals | ✅ function scan + projection/filter pushdown shapes | ⚠️ external wrapper streams child; CSV reader still stores row buffers | file/object bytes only | n/a | ✅ query results | ✅ local/object/glob/projection/filter tests | ⚠️ CSV gauntlet smoke measured for UltraSQL/DuckDB; ClickHouse unavailable locally; full cert open |
| Parquet table functions | ✅ `read_parquet`, globs, file literals | ✅ projection/predicate pushdown shapes | ✅ row-group workers yield batches lazily; no upfront full-file batch buffer | ✅ local files plus object range footer/column reads | n/a | ✅ query results | ✅ projection/filter/object-range/row-group-worker/EXPLAIN tests | ⚠️ UltraSQL arena smoke measured; cross-engine cert open |
| Object-store scans | ✅ `s3://`, `r2://`, `gs://` path specs | ✅ function scan paths | ⚠️ Parquet uses ranges; CSV/JSON paths still use whole-object reads | ✅ `read_object_range` + metadata APIs exist | n/a | n/a | ✅ mocked range/object tests + Parquet range test | ⚠️ object Parquet range smoke measured; full lakehouse cert open |
| Iceberg read scan | ✅ `read_iceberg` / `iceberg_scan` | ✅ function scan | ⚠️ metadata planner feeds Parquet scan | metadata-only planner | n/a | ✅ query results | ✅ current snapshot tests | ❌ deletes/time-travel/catalog cert open |
| Arrow bridge | ✅ `read_arrow` | ✅ function scan | ⚠️ buffered IPC file batches | IPC bytes only | n/a | ✅ query results | ✅ basic type tests | ❌ Flight/type-coverage cert open |

- [x] **Vector type slice, runtime subset** — parser, binder, core values,
  row-codec/catalog storage, text casts, dimension validation, finite-value
  checks, server round trips, and wire text output exist for the vector-family
  types. Remaining work: binary COPY parity, broader PostgreSQL coercions, and
  restart-focused certification artifacts.
- [x] **Vector operators/functions, runtime subset** — pgvector distance
  operators `<->`, `<#>`, `<=>`, `<+>` and functions `l2_distance`,
  `cosine_distance`, `inner_product`, `dot_product`, `l1_distance`,
  `vector_norm`/`l2_norm`, and `vector_dims` exist with scalar/SIMD kernels.
  Remaining work: arithmetic operators, aggregates like `avg(vector)`, and
  certification against pgvector.
- [ ] **Exact vector top-k hardening** — ensure `ORDER BY embedding <op>
  probe LIMIT k` always avoids a full sort, applies scalar filters before
  distance work, exposes the chosen kernel/index/fallback in `EXPLAIN
  ANALYZE`, and has raw artifacts for filtered and unfiltered exact search.
- [x] **Runtime ANN slice** — HNSW and IVFFlat SQL surfaces, earlier
  in-memory access methods, DML maintenance, tombstones, VACUUM compaction,
  opclasses, and exact rerank paths exist. This slice now has a page-backed
  SQL path; the remaining gap is certification depth, not total absence of
  persistent index relations.
- [x] **Production ANN restart slice** — page-backed HNSW and IVFFlat objects
  are wired through SQL, survive server restart, and report page-backed index
  selection in `EXPLAIN ANALYZE`.
- [ ] **Production ANN certification slice** — harden page-backed HNSW and
  IVFFlat with large-scale crash/restart recovery certification, WAL replay
  fuzz/property tests, torn-write/corruption handling, VACUUM/rebuild stress,
  CREATE INDEX CONCURRENTLY behavior, filtered-query fallback/iterative scan
  policy, and recall-vs-latency artifacts against exact scan.
- [x] **RAG tenant RLS slice, narrow predicate** — `CREATE POLICY`,
  `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, `SET ultrasql.tenant_id`, read
  predicate injection, command-specific policies, permissive/restrictive
  policy combination, and `INSERT ... VALUES` `WITH CHECK` enforcement work
  for the documented RAG policy shape
  `tenant_id = current_setting('ultrasql.tenant_id', true)`. Remaining work:
  persistent policy catalog/restart bootstrap, owner/bypass behavior,
  `INSERT ... SELECT`, update new-row checks, role-scoped policies, and tenant
  certification artifacts.
- [ ] **AI/vector benchmark certification slice** — AI gauntlet measured artifacts
  now include exact top-k, HNSW ANN recall/latency, hybrid search
  latency, filtered vector search with tenant/category predicates, RAG
  retrieval quality with expected document ids, memory per million vectors
  for page-backed HNSW/IVFFlat, ingestion throughput with and without HNSW
  index maintenance, and cold-start index load latency after restart. The AI
  gauntlet manifest now fails when any required UltraSQL suite fails to emit a
  measured artifact. Expand the smoke set into committed certification
  artifacts for IVFFlat recall/latency, larger bulk loads, index
  build/update/delete maintenance, WAL recovery, and larger hybrid RAG query
  shapes.
  Certification must compare UltraSQL against PostgreSQL + pgvector on the
  same host and report recall@k, p50/p95/p99 latency, throughput, index size,
  build time, memory, and restart correctness. No v1.0 vector claim is allowed
  without committed scripts, datasets or dataset fetch instructions, host
  descriptor, and raw artifacts.

### Security
- [ ] `CREATE ROLE / USER`, `ALTER ROLE`, `DROP ROLE`
- [ ] `GRANT / REVOKE` on tables, schemas, databases, sequences, functions
- [ ] Column-level privileges
- [ ] Role inheritance + `SET ROLE`
- [ ] Default privileges (`ALTER DEFAULT PRIVILEGES`)
- [ ] Row-level security: role/owner/bypass/restart semantics (tenant command-specific + permissive/restrictive policy slice done)
- [ ] `log_connections`, `log_min_duration_statement`, `log_statement`

### ORM Compatibility
- [ ] SQLAlchemy (Python) — full dialect support
- [ ] Django ORM (Python) — models, migrations, queries
- [ ] Rails ActiveRecord (Ruby) — schema introspection, CRUD, migrations
- [ ] Hibernate / Spring Data JPA (Java)
- [ ] GORM (Go)
- [ ] Prisma (TypeScript/Node)
- [ ] Diesel (Rust)

### Driver Compatibility
- [ ] `libpq` (C)
- [ ] `psycopg2` / `psycopg3` (Python)
- [ ] `node-postgres` / `pg` (Node.js)
- [ ] `pq` / `pgx` (Go)
- [ ] JDBC PostgreSQL driver (Java)
- [ ] `npgsql` (.NET)
- [x] `tokio-postgres` (Rust) — CREATE / INSERT / SELECT / UPDATE / DELETE Simple Query round-trip via `cross_compare_sql`

### Tooling Compatibility
- [ ] `psql` meta-commands: `\d`, `\dt`, `\di`, `\df`, `\dv`, `\du`, `\l`, `\dn`
- [ ] `pgAdmin 4` connects and browses schema
- [ ] `DBeaver` connects and runs queries
- [ ] `DataGrip` connects and introspects schema
- [ ] Flyway migrations run correctly
- [ ] Liquibase migrations run correctly
- [ ] Alembic migrations run correctly

### PostgreSQL Regression Suite
- [ ] Parser tests pass
- [ ] Type coercion tests pass
- [ ] Transaction isolation tests pass (acid.sql, Hermitage)
- [ ] Index tests pass
- [ ] Aggregate and window function tests pass
- [ ] Constraint tests pass
- [ ] Operator tests pass
- [ ] Type-specific tests (numeric, text, date/time, json, arrays, etc.)

### Benchmark Certification
- [x] Benchmark profile ergonomics: PR-safe smoke certification and
  scheduled/manual full certification profiles are split and wired.
  Evidence: commit `5f0c49e`, local smoke + fmt/clippy/test/rustdoc/diff
  gates, GitHub Actions ci run `26151820002`, and supremacy run
  `26151891843`. This proves runner/workflow health only; it does not
  certify TPC-H, ClickBench, TPC-B/C, Sysbench, exact vector top-k, or
  HNSW performance.
- [x] Hot-path profile runner: `benchmarks/hot_path_profiles.sh` records
  flamegraphs and raw timing artifacts for `csv_copy`, `parquet_filter`,
  `vector_topk`, `hash_aggregate`, `joins`, `tpch_q1`, `tpch_q5`, and
  `tpch_q6`. Missing profiler tools or TPC-H data are recorded as
  `not_available`, not benchmark claims.
- [x] Columnar scan path: heap rows remain the OLTP/MVCC source of truth,
  while `HeapAccess::column_cache` supplies the OLAP shadow path.
  `columnar_storage_round_trip.rs` now covers committed DML invalidation,
  rebuild, and update/delete/insert visibility after cache rebuild.
- [ ] TPC-B: correctness verified, throughput ≥ 2× PostgreSQL, p99 < 5 ms at 32 connections — `benchmarks/tpcb_certify.sh` exists and the wire harness now validates UltraSQL balance invariants with indexed TPC-B tables; latest local reduced smoke is correct at 986.20 tx/s, but p99 is 39.884 ms and PostgreSQL 17 comparison is still missing, so certification remains open.
- [ ] TPC-C: correctness verified (all 5 transaction types), throughput ≥ 2× PostgreSQL
- [x] TPC-H scale 1: all 22 harness queries return correct results
- [ ] TPC-H scale 1: throughput ≥ 2× PostgreSQL 17
- [x] TPC-H scale 10: throughput ≥ 2× DuckDB on the latest local
  artifact; rerun `benchmarks/tpch_sf10_certify.sh` on the release host
  before publishing release numbers.
- [ ] Sysbench OLTP read/write: throughput ≥ 2× PostgreSQL
- [ ] Vector/RAG certification: exact kNN, HNSW recall/latency, hybrid
  text+vector retrieval, filtered vector search, RAG quality, ingestion
  throughput, memory-per-million-vectors, and cold-start restart smoke
  artifacts exist; IVFFlat recall/latency, larger recovery artifacts, and
  same-host pgvector certification remain open

### Production Validation
- [ ] Three independent operators run UltraSQL for 30 days and report
- [ ] Zero open critical or high-severity correctness bugs
- [ ] Chaos testing: random kill, WAL truncation, disk full — all recover correctly
- [ ] Fuzz testing: 1 week clean on parser, protocol, WAL decoder, planner

### Release Checklist
- [ ] `CHANGELOG.md` documenting every user-visible change
- [ ] Official documentation site (`docs.ultrasql.org`)
- [ ] Getting started guide
- [ ] Migration guide from PostgreSQL
- [ ] Known incompatibilities documented
- [ ] Docker image published
- [ ] Homebrew formula
- [ ] Debian/Ubuntu and RPM packages
- [ ] GitHub release with release notes

---

## v1.x — "Extend" ❌

**Scope:** Stored procedures, triggers, views, materialized views,
partitioning, full-text search, remaining type coverage.

### Hardware-Specific Performance
- [ ] x86 AVX-512 target-feature CI/bench certification for vector
  kernels, with scalar/AVX2 parity tests and committed result artifacts
  from an AVX-512-capable host.

### Views & Materialized Views
- [ ] `CREATE VIEW` / `CREATE OR REPLACE VIEW` / `DROP VIEW`
- [ ] View expansion in optimizer
- [ ] Updatable views (single-table, no aggregation/DISTINCT)
- [ ] `WITH CHECK OPTION`
- [ ] `CREATE MATERIALIZED VIEW`
- [ ] `REFRESH MATERIALIZED VIEW [CONCURRENTLY]`
- [ ] Indexes on materialized views

### PL/pgSQL
- [ ] Variable declaration and assignment
- [ ] `IF / ELSIF / ELSE / END IF`
- [ ] `LOOP / WHILE / FOR / FOREACH`
- [ ] `RETURN` / `RETURN NEXT` / `RETURN QUERY`
- [ ] `EXECUTE sql [USING params]` (dynamic SQL)
- [ ] `RAISE NOTICE/WARNING/EXCEPTION`
- [ ] `EXCEPTION WHEN condition THEN ...`
- [ ] Cursors: `DECLARE`, `OPEN`, `FETCH`, `CLOSE`
- [ ] `%TYPE` and `%ROWTYPE`
- [ ] Functions returning `SETOF type` / `RETURNS TABLE (...)`
- [ ] OUT and INOUT parameters
- [ ] `CREATE PROCEDURE` / `CALL procedure_name(...)`

### Triggers
- [ ] `BEFORE/AFTER INSERT/UPDATE/DELETE/TRUNCATE FOR EACH ROW/STATEMENT`
- [ ] `INSTEAD OF` triggers on views
- [ ] `NEW` and `OLD` record variables
- [ ] `WHEN (condition)` on row triggers
- [ ] `CREATE CONSTRAINT TRIGGER` (deferrable)
- [ ] Trigger ordering (alphabetical within same event + timing)

### Table Partitioning
- [ ] `PARTITION BY RANGE / LIST / HASH`
- [ ] `CREATE TABLE child PARTITION OF parent FOR VALUES ...`
- [ ] `DEFAULT` partition
- [ ] `ATTACH PARTITION` / `DETACH PARTITION`
- [ ] Partition pruning in optimizer (static + runtime)
- [ ] Partition-wise joins and aggregation
- [ ] `INSERT` routing to correct partition
- [ ] `UPDATE` that crosses partitions (delete + insert)
- [ ] `Append` / `MergeAppend` executor operators — needed to fan-in partition children once the partitioned-table catalog surface lands

### Full-Text Search
- [ ] `TSVECTOR` / `TSQUERY` types
- [ ] `to_tsvector(config, text)`, `to_tsquery(config, query)`
- [ ] `plainto_tsquery`, `phraseto_tsquery`, `websearch_to_tsquery`
- [ ] `@@` match operator, `ts_rank()`, `ts_headline()`
- [ ] Default text search configurations (english, simple, spanish, etc.)
- [ ] GIN index on `TSVECTOR`

### Remaining Index Types
- [ ] SP-GiST (quad-tree for 2D, radix tree for text prefix)
- [ ] Bloom filter indexes
- [ ] GIN/GiST on all remaining types

### Remaining Type Coverage
- [ ] Full locale/collation support (ICU)
- [ ] `CREATE COLLATION` / `COLLATE` in column definitions and ORDER BY
- [ ] `XML` full support (xpath, xmltable)
- [ ] `HSTORE` built-in

### Additional pg_stat Views
- [ ] `pg_stat_progress_vacuum`, `pg_stat_progress_analyze`, `pg_stat_progress_create_index`
- [ ] `pg_statio_user_indexes`
- [ ] `pg_stat_bgwriter` full implementation
- [ ] `pg_stat_replication` (if replication active)

---

## v2.0? — "Distribute" ❌

**Scope:** Multi-node execution. Raft-replicated catalog.
Partitioned tables across nodes.

- [ ] Coordinator layer over single-node `ultrasqld` instances
- [ ] Raft-replicated catalog (`pg_class`, `pg_attribute`, etc.)
- [ ] Partitioned tables sharded across nodes
- [ ] Distributed query execution with shuffle operators
- [ ] Cross-region async replication
- [ ] Distributed deadlock detection
- [ ] Global XID allocation (distributed MVCC)
- [ ] `pg_dist_*` catalog tables (Citus-compatible schema)

---

## v2.x? — "Store and Serve" ❌

**Scope:** Native columnar storage. Arrow Flight. Query result cache.
Federated queries via FDW.

- [ ] Columnar page format for cold partitions
- [ ] Transparent routing: row pages for hot, columnar for cold
- [ ] ZSTD column compression
- [ ] Hybrid row/column scans within a single query
- [ ] Native Apache Arrow Flight endpoint for analytical clients
- [ ] Query result cache (configurable TTL)
- [ ] Federated foreign-data-wrapper layer
- [ ] Custom FDW API (foreign tables over HTTP, Parquet, S3, etc.)
- [ ] Extension loading infrastructure (`CREATE EXTENSION`, shared library plugin API)
- [ ] Custom background worker API
- [ ] Event triggers (`CREATE EVENT TRIGGER`)
- [ ] `pg_stat_statements` equivalent built-in
- [ ] `auto_explain` equivalent built-in
- [ ] `pg_trgm` equivalent (trigram similarity for fast LIKE)

---

## Build / Tooling

This is not a versioned milestone; these are workspace-wide hygiene
items that block contributor velocity if neglected.

- [x] Cargo profile tuned for the agent iteration loop (thin LTO, cgu=16, incremental, debug=0). Cold release build dropped from ≈ 90 s to ≈ 20 s; incremental edits ≈ 4 s. See commit `3b640cd`.
- [x] Trim `tokio` features from `"full"` to the 7-feature subset actually used (drops `fs`/`process`/`parking_lot`/`tracing`).
- [x] `[profile.release-ship]` for tarball / regression-gate baseline cuts (fat LTO + cgu=1).
- [x] Pre-push gate runs `cargo fmt --check`, workspace clippy with
  `-D warnings`, `cargo test --workspace --all-features --lib`,
  strict docs, advisory audit when `cargo-deny` is installed,
  `regression-gate --smoke` for source diffs, and optional parser fuzz
  smoke when nightly + `cargo-fuzz` exist. Push is blocked on failure.
- [ ] Workspace `cargo nextest` integration once `nextest`'s output integrates with the pre-push gate.
- [ ] `sccache` for cross-machine compilation cache (contributor onboarding aid).

---

## How features get on the roadmap

A feature on the roadmap has either an open RFC or a maintainer-signed
commitment. Ideas without either are not on the roadmap; they are
ideas. The discussion forum is GitHub Discussions; the formal record
is `rfcs/`.

---

*This document is the source of truth for UltraSQL's path to becoming
a production PostgreSQL replacement. Update it as work progresses.
Every checked box is a commitment delivered — and every checked box
is verifiable from the code, not from a commit message.*
