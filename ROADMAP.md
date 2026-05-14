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

This file is the source of truth for both humans and tool attributions —
every item is actionable without further context.

---

## Standing Quality Requirements

These apply to **every version** from v0.2 onward. A version does not
ship if any of these gates fail. No exceptions.

### Test Coverage Gate: ≥ 80% line coverage per crate

Every crate must maintain at least **80% line coverage** measured by
`cargo llvm-cov --workspace`. Coverage is checked on every PR via the
local pre-push gate. A PR that drops any crate below 80% is not
mergeable until coverage is restored — either by adding tests or by
justifying the exclusion in the PR description and annotating the code
with `#[cfg(not(tarpaulin_include))]` or `// coverage: exclude` with a
written reason.

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

Every PR that touches a benchmarked code path must include
before/after `cross_compare_sql` numbers in the description.
The pre-push hook runs the criterion suite and `regression-gate` and
fails the push on any statistically significant regression > 5% versus
the baseline recorded in `benchmarks/results/baseline.json`.

For each milestone below, UltraSQL must demonstrably **outperform every
listed competitor by ≥ 2×** (throughput) or ≤ 0.5× (latency) on the
specified workload **before the version ships**. Results must be
reproducible via the scripts in `benchmarks/` on the recorded host.

Performance claims require a reproducible benchmark script and a
recorded host descriptor (see `BENCHMARKS.md`). Fabricated numbers
are grounds for revert.

| Version | Workload | Target | Metric |
|---------|----------|--------|--------|
| v0.5 | Simple INSERT throughput (10 k rows / multi-row VALUES) | ≥ 2× PostgreSQL 17 ✅ | throughput (µs / batch) |
| v0.5 | Simple SELECT scan (10 k rows full table) | ≥ 2× every competitor ❌ | latency (µs) |
| v0.5 | SELECT SUM(x) over 65 k rows | ≥ 2× every competitor ❌ | latency (µs) |
| v0.5 | UPDATE 10 k rows in single statement | ≥ 2× every competitor ❌ | latency (µs) |
| v0.5 | DELETE 10 k rows in single statement | ≥ 2× every competitor ❌ | latency (µs) |
| v0.6 | TPC-H scale 1 (all 22 queries) | ≥ 2× PostgreSQL 17 | geometric mean query time |
| v0.7 | TPC-H scale 10 (all 22 queries) | ≥ 2× DuckDB | geometric mean query time |
| v0.7 | ClickBench (`hits.parquet` analytical queries) | ≥ 5× faster than PostgreSQL 17 | geometric mean query time |
| v0.9 | TPC-B (OLTP, 32 connections) | ≥ 2× PostgreSQL 17, p99 < 5 ms | throughput + latency |
| v1.0 | TPC-C (all 5 tx types, 32 connections) | ≥ 2× PostgreSQL 17 | throughput (tx/s) |
| v1.0 | Sysbench OLTP read/write | ≥ 2× PostgreSQL 17 | throughput (tx/s) |
| v2.x | Star Schema Benchmark scale 100 | ≥ 2× ClickHouse | geometric mean query time |

All comparisons follow the methodology in `BENCHMARKS.md`: same host,
same dataset, same seed, competitor tuned per its published best
practices, median of 5 runs ≥ 60 s each after ≥ 60 s warmup. Live
results auto-render from `benchmarks/results/latest/raw/*.json` into
`README.md` via `readme-render`.

---

## Current State Snapshot

<!-- reconciled 2026-05-14 against actual code (commits 800ab81..3b640cd) -->

| Crate | Status |
|-------|--------|
| `ultrasql-core` | ✅ Types, OIDs, Datum, Schema, identifiers, page sizing constants |
| `ultrasql-storage` | ✅ Pages, buffer pool (CLOCK-Pro), heap AM, B+ tree, FSM, VM, TOAST, persistent CLOG, WAL applier — `crates/ultrasql-storage/src/lib.rs` |
| `ultrasql-wal` | ✅ Records, group commit, recovery, FPW; HeapTarget replay wired — `crates/ultrasql-wal/src/lib.rs` |
| `ultrasql-mvcc` | ✅ Snapshot + visibility rules (PostgreSQL `HeapTupleSatisfiesMVCC`) |
| `ultrasql-txn` | ✅ TxnManager kernel: BEGIN/COMMIT/ABORT, lock manager, SSI scaffolding, savepoints, 2PC; ⚠️ none of `BEGIN`/`COMMIT`/`ROLLBACK` reaches the wire — binder rejects at `binder.rs:83` |
| `ultrasql-parser` | ✅ Full DML + DDL + CTE + Extended Protocol Parse/Bind syntax |
| `ultrasql-planner` | ✅ Binder for SELECT/INSERT/UPDATE/DELETE, JOINs, GROUP BY, subqueries, CTEs; ⚠️ `BETWEEN` not yet bound (cross_compare_sql workaround uses `id < N`); BEGIN/COMMIT/ROLLBACK rejected |
| `ultrasql-optimizer` | ✅ Rule-based rewrites, cost model, DPsize/GEQO join enumeration, physical selection, plan cache (~1077 LOC across `lib.rs` + `plan_cache.rs`); ⚠️ not exercised by server's inline `lower_query` (the server bypasses `physical::build_operator`) |
| `ultrasql-executor` | ✅ SeqScan (streaming + TID mode), ModifyTable, NestLoop, HashJoin, HashAggregate (scalar SIMD fast path), Sort, ValuesScan, Filter (col-op-lit SIMD fast path), Project, Limit, CteScan; ⚠️ recursive CTE fixpoint loop deferred to v0.6 |
| `ultrasql-vec` | ✅ Push pipeline driver, SIMD kernels (filter/arith/hash/cmp/sum/min/max with mask-aware paths), Bitmap, dictionary encoding, ColumnBuilder, vectorized sort/HashJoin/HashAggregate |
| `ultrasql-catalog` | ✅ PersistentCatalog with arc-swap snapshots, MutableCatalog DDL surface, pg_class/pg_attribute/pg_index row shapes; ⚠️ bootstrap-from-heap falls back to initial snapshot (no typed tuple decoder yet) |
| `ultrasql-protocol` | ✅ Wire codec for Simple Query + Extended Query (Parse/Bind/Describe/Execute/Sync/Close) |
| `ultrasql-server` | ✅ SCRAM-SHA-256 + TLS, Simple Query end-to-end for `CREATE TABLE`, `INSERT VALUES`, `SELECT`/`SELECT SUM`/`SELECT AVG`/`SELECT WHERE`, `UPDATE`, `DELETE` through real heap; ✅ Extended Query dispatch (Parse/Bind/Describe/Execute/Sync/Close/Flush) with parameter substitution through the same path; ⚠️ BEGIN/COMMIT + JOIN/ORDER BY/SET OPs not yet wired |

### Wire-protocol coverage matrix

| SQL shape | Parser | Binder | Server (`lower_query`) | tokio-postgres round-trip |
|-----------|:--:|:--:|:--:|:--:|
| `CREATE TABLE t (...)` | ✅ | ✅ | ✅ | ✅ |
| `INSERT INTO t VALUES (...)` | ✅ | ✅ | ✅ | ✅ |
| `INSERT INTO t SELECT ...` | ✅ | ✅ | ❌ | ❌ |
| `INSERT ... ON CONFLICT / RETURNING` | ✅ | ✅ | ❌ | ❌ |
| `SELECT col, ...` (no agg, no join) | ✅ | ✅ | ✅ | ✅ |
| `SELECT col FROM t WHERE col op lit` | ✅ | ✅ | ✅ | ✅ |
| `SELECT SUM/AVG/MIN/MAX/COUNT(*) FROM t` | ✅ | ✅ | ✅ | ✅ |
| `SELECT SUM(col) FROM t WHERE col op lit` | ✅ | ✅ | ✅ | ✅ |
| `SELECT ... GROUP BY` | ✅ | ✅ | ⚠️ HashAggregate runs; multi-group not yet bench-verified | ⚠️ |
| `SELECT ... ORDER BY` | ✅ | ✅ | ❌ | ❌ |
| `SELECT ... JOIN ...` | ✅ | ✅ | ❌ | ❌ |
| `SELECT ... LIMIT n` (`OFFSET 0`) | ✅ | ✅ | ✅ | ✅ |
| `SELECT ... LIMIT n OFFSET m` | ✅ | ✅ | ✅ | ✅ |
| `UPDATE t SET col = expr WHERE ...` | ✅ | ✅ | ✅ | ✅ |
| `DELETE FROM t WHERE ...` | ✅ | ✅ | ✅ | ✅ |
| `TRUNCATE t` | ✅ | ✅ | ✅ | ✅ |
| `BEGIN / COMMIT / ROLLBACK` | ✅ | ❌ | ❌ | ❌ |
| `SAVEPOINT / RELEASE / ROLLBACK TO` | ✅ | ❌ | ❌ | ❌ |
| `PREPARE / EXECUTE / DEALLOCATE` (Simple Query) | ✅ | ❌ | ❌ | ❌ |
| Extended Query (Parse/Bind/Execute) | ✅ codec | n/a | ✅ dispatch | ✅ |
| `EXPLAIN` / `EXPLAIN ANALYZE` | ✅ | ❌ | ❌ | ❌ |
| `BETWEEN ... AND ...` | ✅ | ❌ | ❌ | ❌ |
| `WITH cte AS (...)` (non-recursive) | ✅ | ✅ | ✅ | ✅ |
| `WITH RECURSIVE cte AS (...)` | ✅ | ✅ | ❌ rejected by lowerer | ❌ |
| `UNION / INTERSECT / EXCEPT` | ✅ | ✅ | ❌ | ❌ |
| `CREATE INDEX` | ✅ | ✅ | ❌ | ❌ |
| `DROP TABLE` | ✅ | ✅ | ❌ | ❌ |
| `ALTER TABLE` | ✅ | ✅ | ❌ | ❌ |

---

## Priority Matrix

| Priority | Area | Blocking |
|----------|------|---------|
| **P0** | v0.5: BEGIN/COMMIT/ROLLBACK end-to-end (binder + server dispatch) | Every multi-statement workload, mixed_oltp_pgbench_like bench, ORM correctness |
| **P0** | ~~v0.5: Extended Query dispatch in server~~ ✅ done — Parse/Bind/Describe/Execute/Sync/Close/Flush wired via `extended.rs`; tokio-postgres prepared-statement round-trips green | (was) Every ORM and every driver beyond simple psql |
| **P0** | v0.5: Wire ORDER BY (`LogicalPlan::Sort`) in `lower_query` | Any ranked output, TPC-H Q1 |
| **P0** | v0.5: Wire `LogicalPlan::Join` and `SetOp` in `lower_query` | All TPC-H, all real analytical workloads |
| **P0** | v0.5: Binder support for `BETWEEN`, `IS NULL` (latter for completeness) | UPDATE / DELETE benchmark parity, ANSI surface |
| **P0** | v0.5: `IndexScan` wired in `lower_query` (B-tree already exists) | Point-lookup workload — currently SeqScan only |
| **P0** | Win the ≥ 2× perf gate on every bench in README (currently only INSERT passes) | Every release after v0.5 |
| **P0** | v0.6: Server invokes optimizer (`physical::build_operator`) instead of inline `lower_query` | Cost-aware physical selection, plan cache |
| **P1** | v1.x: JSONB, NUMERIC, arrays | Modern apps, financial workloads |
| **P1** | v0.8: Constraints (NOT NULL, FK, CHECK, DEFAULT) | Data integrity |
| **P1** | v0.8: Persistent catalog typed-tuple decoder | Survive restart with user tables |
| **P1** | v0.4: SSI predicate locking integrated (TxnManager still RR-aliased) | "SERIALIZABLE" honesty |
| **P2** | v1.x: Views + Materialized Views | Very common pattern |
| **P2** | v0.8: Sequences (`SERIAL`/`IDENTITY`) end-to-end | Every table with PK |
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
- [x] `BETWEEN ... AND ...` (parser only — ⚠️ binder rejection, see v0.5)
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
- [ ] Parser fuzz target reaching 24 h CI-clean (cargo-fuzz target + 31-file seed corpus committed; 24 h gate is local-execution follow-up)

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

## v0.4 — "Transactions" ⚠️ PARTIAL

**Scope:** ACID transactions with snapshot isolation and true
serializable (SSI). Real row-level locking. Deadlock detection.

> Kernel ships but is **not yet integrated through the wire**: there
> is no way for a tokio-postgres client to send `BEGIN` / `COMMIT` /
> `ROLLBACK` / `SAVEPOINT` and have it traverse parser → binder →
> server dispatch. Lower binder rejects at `binder.rs:83`. This is a
> v0.5 P0.

### Lock Manager
- [x] Fastpath relation locks (per-backend cache, no central state)
- [x] Central lock table: `DashMap<LockTag, LockEntry>` with wait-for graph
- [x] Deadlock detector background thread (configurable interval, default 1 s)
- [x] Tuple-level locks for concurrent updates (LockTag::Tuple supported)
- [ ] `SELECT FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` end-to-end (executor wiring + lower_query arm)
- [x] Advisory locks: `pg_advisory_lock`, `pg_try_advisory_lock` (LockTag::Advisory; SQL surface still TODO)

### SSI (Serializable Snapshot Isolation)
- [x] Predicate locks (`SIReadLock`)
- [x] RW-anti-dependency tracking
- [x] Dangerous structure detection (T1 → T2 → T3 cycle)
- [x] Safe snapshot optimization
- [ ] True SERIALIZABLE end-to-end — SsiManager ships; TxnManager snapshot strategy still RR-aliased pending integration; no `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` round-trip

### Subtransactions
- [x] `SAVEPOINT name` execution kernel
- [x] `ROLLBACK TO SAVEPOINT name` kernel
- [x] `RELEASE SAVEPOINT name` kernel
- [ ] Subtransaction tracking in MVCC headers (SubtxnManager ships; header-bit wiring TBD)
- [ ] All three reachable from the wire (blocked on BEGIN/COMMIT)

### Two-Phase Commit
- [x] `PREPARE TRANSACTION 'id'` kernel
- [x] `COMMIT PREPARED 'id'` / `ROLLBACK PREPARED 'id'` kernel
- [x] Persistence across restarts
- [x] `pg_prepared_xacts` view (list_prepared API)
- [ ] All four reachable from the wire

### Executor ↔ Storage wiring
- [x] `SeqScan` operator reading real heap pages (replacing `MemTableScan`) — streaming, page-by-page typed decode
- [x] `ModifyTable` operator for INSERT/UPDATE/DELETE on real heap (Update/Delete via TID-emitting SeqScan + shift_column_indices)
- [x] Executor uses real `TransactionManager` snapshot for visibility (SeqScan accepts Snapshot+Oracle)

### Tests
- [ ] Loom-based concurrency model tests for lock manager
- [ ] Isolation level tests (READ COMMITTED, REPEATABLE READ, SERIALIZABLE) via real `BEGIN ... SET ISOLATION ... COMMIT`
- [ ] Serializability checker (Hermitage test suite)

---

## v0.5 — "Execute" 🔄 IN PROGRESS

**Scope:** Full physical operator set exposed through the Simple Query
wire path. Extended query protocol. Real auth. Any standard PostgreSQL
driver can connect.

### Scan Operators
- [x] `SeqScan` with predicate pushdown — streaming + TID mode wired
- [ ] `IndexScan` via B-tree (point lookup + range scan) — kernel ships in executor; **not yet reachable from `lower_query`**
- [ ] `IndexOnlyScan` (skip heap fetch when VM bit is set)
- [ ] `BitmapIndexScan` + `BitmapHeapScan` (OR multiple indexes)
- [ ] `FunctionScan` (`generate_series`, `unnest`, SRFs)
- [x] `ValuesScan` (wired)
- [x] `CteScan` reachable from `lower_query` (non-recursive); `SubqueryScan` follow-up; `WITH RECURSIVE` deferred to v0.6 fixpoint loop

### Join Operators
- [x] `NestLoop` kernel (with inner rescan via factory closure)
- [x] `HashJoin` kernel (build + probe — Inner+LeftOuter; Right/Full/Semi/Anti and disk spill TBD)
- [ ] `MergeJoin` (requires sorted input)
- [ ] All join types reachable from `lower_query` — currently `JOIN` returns Unsupported

### Aggregation Operators
- [x] `HashAggregate` kernel + scalar SIMD fast path (no GROUP BY: SUM/AVG/COUNT/MIN/MAX dispatch to `sum_i64`/`count_i64`/`min_i64`/`max_i64`)
- [x] Aggregate reachable from `lower_query` (server dispatches `LogicalPlan::Aggregate` → HashAggregate)
- [ ] `SortAggregate` (streaming over sorted input)
- [x] Standard aggregates: COUNT, SUM, AVG, MIN, MAX, BOOL_AND, BOOL_OR, STRING_AGG, ARRAY_AGG (JSON_AGG TBD)
- [ ] Statistical aggregates: STDDEV, VARIANCE, CORR, PERCENTILE_CONT, PERCENTILE_DISC
- [ ] Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE
- [ ] `OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)`
- [ ] `WindowAgg` operator

### Other Operators
- [x] `Sort` kernel (in-memory; external spill TBD); ❌ not yet reachable from `lower_query` (ORDER BY returns Unsupported)
- [ ] `Unique` (DISTINCT) — wire path
- [ ] `SetOp` (UNION/INTERSECT/EXCEPT, hashed and sorted) — kernel ships; wire path pending
- [ ] `RecursiveUnion` (WITH RECURSIVE) — wire path
- [ ] `LockRows` (SELECT FOR UPDATE/SHARE)
- [ ] `Materialize` (pipeline breaker)
- [ ] `Gather` / `GatherMerge` (parallel query)
- [ ] `Append` / `MergeAppend` (partition scans)
- [ ] `Result` (constant expressions) — `SELECT 1` and similar

### Expression Evaluation
- [x] Full general expression interpreter (Eval) — replaces hardcoded `FilterEqI32`
- [x] Vectorized Filter for col-op-literal predicates (SIMD `cmp_i32_scalar` / `cmp_i64_scalar` with mask AND validity bitmap)
- [x] NULL propagation correctness in all operators (Kleene 3VL in Eval; SIMD path ANDs validity)
- [ ] Vectorized expression eval over batches for all shapes (binary arith, function calls)
- [ ] Type coercion / implicit casts at execution time

### Memory Management
- [ ] Per-query `work_mem` budget enforced cooperatively
- [ ] Hash build and sort operators spill to temp segments
- [ ] `temp_file_limit` enforcement

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
- [ ] Pipeline mode (server-side multi-message handling beyond the current Sync-bounded pipeline)
- [ ] `max_rows` partial-execution + `PortalSuspended` resumption (currently sends `PortalSuspended` then drops the portal)

### Transaction Control (wire)
- [ ] `BEGIN` / `COMMIT` / `ROLLBACK` round-trip
- [ ] `BEGIN ISOLATION LEVEL ...`
- [ ] Implicit transaction per statement (current behaviour) + explicit-transaction state machine
- [ ] `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` round-trip
- [ ] `PREPARE` / `EXECUTE` / `DEALLOCATE` Simple-Query round-trip

### Binder gaps blocking wire
- [ ] `BETWEEN ... AND ...` (parser accepts; binder rejects)
- [ ] `SELECT ... FROM t WHERE col IS NULL` end-to-end verification
- [ ] `BEGIN / COMMIT / ROLLBACK / SAVEPOINT` (binder rejects at `binder.rs:83`)

### Authentication
- [x] `SCRAM-SHA-256` real implementation (RFC 5802 + 7677)
- [ ] `MD5` password auth (legacy, behind config flag)
- [x] `trust` auth method (via HbaMethod::Trust)
- [x] `pg_hba.conf` equivalent — host-based auth rules
- [x] Roles and passwords stored in `pg_authid` (in-memory; persistent in v0.8)

### SSL/TLS
- [x] `SSLRequest` handling
- [x] TLS upgrade via `rustls`
- [x] `ssl_cert_file`, `ssl_key_file`, `ssl_ca_file` config

### Other Protocol Features
- [ ] `COPY TO STDOUT` / `COPY FROM STDIN` wire format
- [ ] Real `BackendKeyData` with PID + secret for `CancelRequest`
- [ ] `CancelRequest` handling (cancel running query)
- [ ] `NoticeResponse` (warnings, hints, info messages)
- [ ] `NotificationResponse` (LISTEN/NOTIFY)
- [ ] All expected `ParameterStatus` params: `TimeZone`, `DateStyle`, `IntervalStyle`, `extra_float_digits`, `standard_conforming_strings`, `integer_datetimes`, `server_encoding`
- [ ] Per-connection slow-loris timeout

### Wire-protocol benchmarks (`cross_compare_sql`)
- [x] In-process `ultrasqld` driven via `tokio-postgres` for honest end-to-end measurement
- [x] Competitor bench scripts for postgres17 / sqlite3 / duckdb across INSERT / SELECT scan / SELECT SUM / SELECT AVG / Filter+SUM / UPDATE / DELETE
- [x] README auto-renders `benchmarks/results/latest/raw/*.json` into 7 cross-engine tables; UltraSQL appears in every one except `mixed_oltp_pgbench_like`
- [x] Honest sort order (fastest → slowest); ≥ 2× gate currently met only on INSERT, the rest are tracked optimisations

### CLI
- [ ] `ultrasql` REPL with history, multiline input
- [ ] Meta-commands: `\d`, `\dt`, `\di`, `\df`, `\dv`, `\ds`, `\du`, `\dn`, `\l`, `\c`, `\q`, `\i`, `\timing`, `\x`, `\pset`
- [ ] Connect via URL: `postgresql://user:pass@host/db`
- [ ] `PGPASSWORD`, `PGHOST`, `.pgpass` file support
- [ ] `--command/-c` and `--file/-f` batch mode

### Milestones (must hold before v0.5 ships)
- [x] tokio-postgres can `CREATE TABLE`, `INSERT VALUES`, `SELECT ... WHERE col op lit`, `SELECT SUM/AVG`, `UPDATE`, `DELETE` end-to-end against `ultrasqld`
- [ ] `BEGIN`/`COMMIT` round-trip from any standard driver
- [x] Extended Query Parse/Bind/Execute round-trip from any standard driver — tokio-postgres prepared statements green (see `crates/ultrasql-server/tests/extended_query_round_trip.rs`)
- [ ] `ORDER BY` reachable from the wire
- [ ] INSERT 10 k throughput ≥ 2× every competitor (currently 14× DuckDB ✅, 4× SQLite ✅, 10× PostgreSQL ✅)
- [ ] SELECT scan 10 k ≥ 2× every competitor (currently 9× behind DuckDB ❌)
- [ ] SELECT SUM 65 k ≥ 2× every competitor (currently 1.25× behind SQLite ❌)
- [ ] UPDATE 10 k ≥ 2× every competitor (currently 100× behind DuckDB ❌)
- [ ] DELETE 10 k ≥ 2× every competitor (currently 2.4× behind SQLite ❌)
- [ ] AVG 1 M ≥ 2× every competitor (currently 20× behind DuckDB ❌)
- [ ] Filter+SUM 1 M ≥ 2× every competitor (currently 5× behind SQLite ❌)

---

## v0.6 — "Optimize" 🔄 IN PROGRESS

**Scope:** Cost-based optimizer built from scratch.

> Kernel ships and is fully tested in-crate. ⚠️ The server's inline
> `lower_query` does NOT consult the optimizer — `physical::build_operator`
> is bypassed. Reconciling the two dispatch paths is a v0.6 P0.

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
- [ ] `ANALYZE` reachable from the wire (Simple Query handler)
- [ ] Autovacuum triggers `ANALYZE` on heavily modified tables
- [x] `pg_statistic` catalog table (row shape; persistent adapter v0.8)
- [ ] `CREATE STATISTICS` (extended stats: correlation, multi-column MCVs)

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
- [ ] Join reordering with outer join constraints

### Physical Operator Selection
- [x] NestLoop vs HashJoin vs MergeJoin
- [x] IndexScan vs SeqScan (BitmapHeapScan v0.7)
- [x] IndexOnlyScan when VM bit is set
- [x] BitmapHeapScan when selectivity ∈ [0.5%, 10%] or ≥2 indexes apply
- [x] HashAggregate vs SortAggregate (StreamAggregate v0.7)
- [ ] Hash-based DISTINCT vs Sort-based DISTINCT
- [x] Parallel plan cost annotation (divide by N workers, add parallel_setup_cost)

### Plan Cache
- [x] Generic plan for prepared statements
- [x] Custom plan when specific parameter values change the optimal plan
- [x] Re-planning threshold (5× cost increase triggers re-plan)
- [x] Plan invalidation on `ANALYZE` / DDL (PlanCache::invalidate / invalidate_all)

### Integration
- [ ] Server `execute_query` delegates to `optimizer::optimize` then `physical::build_operator` instead of inline `lower_query`
- [ ] Plan cache shared between Simple Query and Extended Query

### Milestone
- [ ] TPC-H scale 1 runs to completion on every query with correct results

---

## v0.7 — "Vectorize" 🔄 IN PROGRESS

**Scope:** Vectorized batch execution for analytic pipelines.
The main OLAP performance differentiator over PostgreSQL.

### Push-Based Pipeline Driver
- [ ] Planner tags pipelines as vectorized (OLAP) vs scalar (OLTP)
- [x] Push-based pipeline driver (`VectorizedSink` / `VectorizedOperator` / `SinkVerdict`)
- [x] Vectorized SeqScan emitting 4096-row batches via streaming `VisibleHeapScan` (page-by-page typed decode, no `Vec<Vec<Value>>` materialisation)
- [x] Vectorized filter operator (SIMD fast path for col CMP scalar, Eval fallback)
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
- [ ] Hand-written ARM64 NEON intrinsics for the hottest kernels (where LLVM auto-vec misses)
- [ ] Hand-written AVX2 / AVX-512 intrinsics (gated on CPUID)

### Dictionary Encoding
- [x] Dictionary encoding for low-cardinality string columns (DictionaryColumn)
- [x] Dictionary-aware filter (compare dict codes, not strings)
- [x] Dictionary-aware GROUP BY (group_by_dict returns per-code row indices)
- [ ] Automatic encoding selection based on cardinality

### JIT Compilation
- [ ] LLVM IR generation for hot expression trees (via `inkwell`)
- [ ] JIT threshold: queries above N rows trigger compilation
- [ ] Inline function calls in JIT code
- [ ] `jit = on|off` GUC, `jit_above_cost` threshold

### Parallel Execution
- [ ] `ParallelSeqScan` partitioning heap blocks across rayon workers (rejected in Wave 6 due to single-worker memory-bandwidth bound; revisit with per-worker buffer-pool partition)
- [ ] `Gather` / `GatherMerge` collators
- [ ] Cost-based parallel-plan selection

### MVCC Read Fast Path
- [ ] Page-level `PD_ALL_VISIBLE` flag — skip per-tuple `oracle.status` on certified pages (Wave 6 prototype reverted due to DELETE-correctness regression; redesign needed)

### Milestone
- [ ] TPC-H scale 10 runs to completion, throughput within 2× of DuckDB
- [ ] ClickBench: at least 5× faster than PostgreSQL on analytical queries

---

## v0.8 — "Index and Constrain" ❌

**Scope:** Full index types. Constraints enforced. Sequences.
Persistent catalog. pg_catalog views sufficient for psql `\d`.

### B-tree (complete)
- [ ] Concurrent splits with right-link pointer (no reader blocking)
- [ ] WAL logging of all index operations
- [ ] Backward index scan
- [ ] Index-only scan (skip heap fetch when VM bit is set)
- [ ] Multi-column B-tree
- [ ] Expression indexes: `CREATE INDEX ON t (lower(name))`
- [ ] Partial indexes: `CREATE INDEX ON t (col) WHERE status = 'active'`
- [ ] Covering indexes: `INCLUDE (col1, col2)`
- [ ] `CREATE INDEX CONCURRENTLY` (online build without lock)
- [ ] `VACUUM` reclaims dead index entries
- [ ] `CREATE INDEX` reachable from the wire (parser+binder done; `lower_query` arm missing)

### Hash Index
- [ ] Static hashing with overflow pages
- [ ] WAL logging for hash index
- [ ] Equality-only queries

### GIN (Generalized Inverted Index)
- [ ] For `JSONB` (`@>`, `<@`, `?`, `?|`, `?&`)
- [ ] For arrays (`@>`, `<@`, `&&`)
- [ ] For `TSVECTOR` (`@@`)
- [ ] Fast update mode (pending list drain)

### GiST (Generalized Search Tree)
- [ ] For range types (`&&`, `@>`, `<@`)
- [ ] For geometric types
- [ ] `EXCLUDE USING gist` constraint support

### BRIN (Block Range Index)
- [ ] For large tables with physical correlation (timestamps, sequential IDs)
- [ ] `minmax` operator class
- [ ] Auto-summarize on vacuum

### Constraints
- [ ] `NOT NULL` enforcement at INSERT/UPDATE
- [ ] `DEFAULT expr` evaluated at INSERT when column omitted
- [ ] `CHECK (expr)` validated before INSERT/UPDATE
- [ ] `UNIQUE` constraint (backed by unique index)
- [ ] `PRIMARY KEY` (NOT NULL + UNIQUE)
- [ ] `FOREIGN KEY ... REFERENCES t(col)` referential integrity
  - [ ] `ON DELETE CASCADE / SET NULL / SET DEFAULT / RESTRICT / NO ACTION`
  - [ ] `ON UPDATE CASCADE / SET NULL / SET DEFAULT / RESTRICT / NO ACTION`
  - [ ] `DEFERRABLE INITIALLY DEFERRED / IMMEDIATE`
- [ ] `GENERATED ALWAYS / BY DEFAULT AS IDENTITY`
- [ ] `GENERATED ALWAYS AS (expr) STORED` (computed columns)
- [ ] `EXCLUDE USING gist (...)` exclusion constraints

### Sequences
- [ ] `CREATE SEQUENCE` with START, INCREMENT, MINVALUE, MAXVALUE, CYCLE, CACHE
- [ ] `ALTER SEQUENCE` / `DROP SEQUENCE`
- [ ] `NEXTVAL`, `CURRVAL`, `LASTVAL`, `SETVAL`
- [ ] `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` sugar
- [ ] Per-session `currval` state
- [ ] Sequence WAL logging and recovery

### Persistent Catalog
- [x] `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type` (row shapes)
- [x] `pg_index`, `pg_constraint`, `pg_sequence` (row shapes)
- [x] Catalog cache with `arc-swap` for wait-free reads
- [x] Catalog snapshot for safe concurrent DDL
- [ ] Typed tuple decoder for bootstrap-from-heap (current path falls back to initial snapshot)
- [ ] `pg_depend` (required for CASCADE DROP)
- [ ] `pg_description` (COMMENT ON)
- [ ] `pg_statistic`, `pg_statistic_ext` (persistent — row shape ships, adapter pending)
- [ ] Shared invalidation messages (catalog cache flush on DDL)

### pg_catalog Views
- [ ] `pg_tables`, `pg_indexes`, `pg_views`, `pg_sequences`
- [ ] `pg_roles`, `pg_user`
- [ ] `pg_settings` (all GUC parameters)
- [ ] `pg_locks`
- [ ] `pg_stat_activity`

### information_schema
- [ ] `information_schema.tables`
- [ ] `information_schema.columns`
- [ ] `information_schema.table_constraints`
- [ ] `information_schema.key_column_usage`
- [ ] `information_schema.referential_constraints`
- [ ] `information_schema.check_constraints`
- [ ] `information_schema.routines`
- [ ] `information_schema.triggers`
- [ ] `information_schema.schemata`
- [ ] `information_schema.sequences`

### Milestone
- [ ] `psql \d`, `\dt`, `\di`, `\df` work correctly

---

## v0.9 — "Operate" ❌

**Scope:** Replication, backup/PITR, observability, COPY,
autovacuum. UltraSQL survives production use.

### Autovacuum
- [ ] Background autovacuum launcher
- [ ] Worker per table triggered by dead tuple ratio
- [ ] `autovacuum_vacuum_threshold`, `autovacuum_vacuum_scale_factor`
- [ ] `autovacuum_analyze_threshold`, `autovacuum_analyze_scale_factor`
- [ ] Per-table autovacuum settings
- [ ] Vacuum FREEZE to prevent XID age buildup
- [ ] `pg_stat_progress_vacuum` view

### Physical Streaming Replication
- [ ] WAL sender process
- [ ] WAL receiver on standby
- [ ] Standby mode (`standby.signal`)
- [ ] Hot standby (read queries on standby)
- [ ] Synchronous replication (`synchronous_commit = remote_apply|remote_write|on|off`)
- [ ] Replication slots
- [ ] `pg_replication_slots` view, `pg_stat_replication` view
- [ ] Cascading replication

### Backup & PITR
- [ ] WAL archiving (`archive_command`)
- [ ] WAL restore (`restore_command`)
- [ ] Base backup (`pg_basebackup` equivalent)
- [ ] `recovery.signal` / `standby.signal` support
- [ ] `recovery_target_time`, `recovery_target_lsn`, `recovery_target_xid`
- [ ] `pg_start_backup()` / `pg_stop_backup()` for online backup
- [ ] Backup manifest (checksums for all files)

### Logical Replication
- [ ] Logical decoding infrastructure
- [ ] `pgoutput` output plugin format
- [ ] `CREATE PUBLICATION` / `CREATE SUBSCRIPTION`
- [ ] Row filters and column lists on publications
- [ ] Initial table data sync
- [ ] `pg_stat_subscription` view

### Observability
- [ ] `pg_stat_user_tables` — seq_scan, idx_scan, n_live_tup, n_dead_tup
- [ ] `pg_stat_user_indexes` — idx_scan, idx_tup_read
- [ ] `pg_statio_user_tables` — heap_blks_read, heap_blks_hit
- [ ] `pg_stat_database` — connections, transactions, deadlocks
- [ ] `pg_stat_bgwriter` — checkpoint stats
- [ ] `pg_stat_wal` — WAL bytes, records, fpi, syncs
- [ ] `pg_stat_progress_analyze`, `pg_stat_progress_create_index`
- [ ] Prometheus `/metrics` HTTP endpoint
- [ ] OpenTelemetry tracing with spans per query and per operator
- [ ] `EXPLAIN ANALYZE` with actual rows, actual time, buffers, WAL stats
- [ ] `EXPLAIN (FORMAT JSON)` for tooling integration
- [ ] Structured JSON logging with `log_min_duration_statement`, `log_statement`

### COPY & Bulk Operations
- [ ] `COPY t FROM STDIN [WITH (FORMAT csv, DELIMITER ',', HEADER, NULL, QUOTE)]`
- [ ] `COPY t TO STDOUT [WITH ...]`
- [ ] `COPY (SELECT ...) TO STDOUT`
- [ ] `COPY t FROM 'file'` / `COPY t TO 'file'` (server-side, superuser only)
- [ ] Binary COPY format
- [ ] LISTEN/NOTIFY: `LISTEN channel`, `NOTIFY channel [, payload]`, `UNLISTEN`

### External Tools
- [ ] `pg_dump` compatible output (custom, directory, tar formats)
- [ ] `pg_restore` equivalent
- [ ] `pg_ctl` equivalent: `initdb`, `start`, `stop`, `reload`, `status`, `promote`
- [ ] `pg_isready` equivalent
- [ ] `pgbench` compatible baseline (default TPC-B transactions)
- [ ] `pg_waldump` equivalent (WAL inspection CLI)

### Milestone
- [ ] First documented operations runbook
- [ ] Health and readiness endpoints
- [ ] Three independent operators run UltraSQL for 7 days and report

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
- [ ] `UUID` + `gen_random_uuid()`
- [ ] `BIT(n)` / `BIT VARYING(n)`
- [ ] `INET`, `CIDR`, `MACADDR`, `MACADDR8`
- [ ] `POINT`, `LINE`, `LSEG`, `BOX`, `PATH`, `POLYGON`, `CIRCLE`
- [ ] `JSON`, `JSONB` — critical for modern apps
- [ ] `int[]`, `text[]`, any type as array
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
- [ ] Array: array_length, array_cat, unnest, array_agg, array_to_string, string_to_array
- [ ] System: version(), current_database(), current_user, pg_typeof(), pg_relation_size(), pg_size_pretty()
- [ ] Sequence: nextval(), currval(), lastval(), setval()

### Security
- [ ] `CREATE ROLE / USER`, `ALTER ROLE`, `DROP ROLE`
- [ ] `GRANT / REVOKE` on tables, schemas, databases, sequences, functions
- [ ] Column-level privileges
- [ ] Role inheritance + `SET ROLE`
- [ ] Default privileges (`ALTER DEFAULT PRIVILEGES`)
- [ ] Row-level security: `CREATE POLICY`, `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`
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
- [ ] TPC-B: correctness verified, throughput ≥ 2× PostgreSQL, p99 < 5 ms at 32 connections
- [ ] TPC-C: correctness verified (all 5 transaction types), throughput ≥ 2× PostgreSQL
- [ ] TPC-H scale 1: all 22 queries return correct results, ≥ 2× PostgreSQL
- [ ] TPC-H scale 10: throughput ≥ 2× DuckDB
- [ ] Sysbench OLTP read/write: throughput ≥ 2× PostgreSQL

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
- [x] Pre-push gate runs `fmt --check`, `clippy -D warnings`, `cargo test`, `regression-gate --smoke`. Push is blocked on failure.
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
