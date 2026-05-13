# UltraSQL Roadmap

This roadmap is a commitment, not a wish list. Items move from
"planned" to "in progress" to "shipped" via the RFC process and the
release process. Stretch goals are marked with `?`; everything else is
on the path.

Releases follow [semantic versioning](https://semver.org/). Pre-1.0
releases may break compatibility between minor versions; we document
the breakage in each release's notes.

**Status legend:** ✅ Done · 🔄 In Progress · ❌ Not Started · ⚠️ Partial

**How to use:** Check `[x]` when a task is done. Update the section
status emoji. This file is the source of truth for both humans and AI
contributors — every item is actionable without further context.

---

## Standing Quality Requirements

These apply to **every version** from v0.2 onward. A version does not
ship if any of these gates fail. No exceptions.

### Test Coverage Gate: ≥ 80% line coverage per crate

Every crate must maintain at least **80% line coverage** measured by
`cargo llvm-cov --workspace`. Coverage is checked on every PR via CI.
A PR that drops any crate below 80% is not mergeable until coverage
is restored — either by adding tests or by justifying the exclusion
in the PR description and annotating the code with `#[cfg(not(tarpaulin_include))]`
or `// coverage: exclude` with a written reason.

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

### Benchmark Gate: no regression > 5% on tagged hot paths

Every PR that touches a benchmarked code path must include
before/after `cargo bench` numbers in the description.
CI runs the criterion suite and fails the PR on any statistically
significant regression > 5% versus the baseline recorded in
`benchmarks/results/baseline.json`.

Performance claims require a reproducible benchmark script and a
recorded host descriptor (see `BENCHMARKS.md`). Fabricated numbers
are grounds for revert.

### Comparison Benchmarks: UltraSQL must beat or match alternatives

For each milestone below, UltraSQL must demonstrably outperform the
listed alternatives on the specified workload **before the version ships**.
Results must be reproducible via the scripts in `benchmarks/` on the
recorded host.

| Version | Workload | Target | Metric |
|---------|----------|--------|--------|
| v0.5 | Simple SELECT point lookup (pgbench SELECT only) | ≥ PostgreSQL 17 | throughput (tx/s) |
| v0.5 | Simple INSERT throughput | ≥ PostgreSQL 17 | throughput (tx/s) |
| v0.6 | TPC-H scale 1 (all 22 queries) | ≥ PostgreSQL 17 | geometric mean query time |
| v0.7 | TPC-H scale 10 (all 22 queries) | within 2× of DuckDB | geometric mean query time |
| v0.7 | ClickBench (`hits.parquet` analytical queries) | ≥ 5× faster than PostgreSQL 17 | geometric mean query time |
| v0.9 | TPC-B (OLTP, 32 connections) | ≥ PostgreSQL 17, p99 < 5 ms | throughput + latency |
| v1.0 | TPC-C (all 5 tx types, 32 connections) | ≥ PostgreSQL 17 | throughput (tx/s) |
| v1.0 | Sysbench OLTP read/write | ≥ PostgreSQL 17 | throughput (tx/s) |
| v2.x | Star Schema Benchmark scale 100 | ≥ ClickHouse | geometric mean query time |

All comparisons follow the methodology in `BENCHMARKS.md`:
same host, same dataset, same seed, competitor tuned per its published
best practices, median of 5 runs ≥ 60 s each after ≥ 60 s warmup.

---

## Current State Snapshot

| Crate | Status |
|-------|--------|
| `ultrasql-core` | ✅ Solid — types, OIDs, Datum, Schema, identifiers |
| `ultrasql-storage` | ⚠️ Structures present, WAL/MVCC integration = 0 |
| `ultrasql-wal` | ⚠️ WAL writes, recovery exists, not wired to storage |
| `ultrasql-mvcc` | ✅ Snapshot + visibility rules implemented |
| `ultrasql-txn` | ✅ TxnManager working, in-memory CLOG |
| `ultrasql-parser` | ⚠️ Only SELECT + BEGIN/COMMIT/ROLLBACK |
| `ultrasql-planner` | ⚠️ Basic binder, local catalog, no JOINs |
| `ultrasql-optimizer` | ❌ Empty file. Zero code. |
| `ultrasql-executor` | ⚠️ Only MemTableScan (in-memory, no real storage) |
| `ultrasql-vec` | ⚠️ Batch/column/kernels scaffolded |
| `ultrasql-catalog` | ⚠️ In-memory only, no persistence |
| `ultrasql-protocol` | ⚠️ Simple Query only; Extended Protocol rejected |
| `ultrasql-server` | ⚠️ No real auth, no real storage, hardcoded sample data |

---

## Priority Matrix

| Priority | Area | Blocking |
|----------|------|---------|
| **P0** | v0.3: WAL ↔ Storage integration | Everything |
| **P0** | v0.2: INSERT/UPDATE/DELETE parser + executor | Any real app |
| **P0** | v0.6: Optimizer (currently zero code) | Analytical queries |
| **P0** | v0.5: Extended protocol (prepared statements) | All ORMs + drivers |
| **P0** | v0.5: SCRAM-SHA-256 real auth | Production use |
| **P1** | v0.5: Full executor operators (JOINs, aggregates) | Most real queries |
| **P1** | v1.x: JSONB, arrays, NUMERIC types | Modern apps |
| **P1** | v0.8: Constraints (FK, CHECK, DEFAULT) | Data integrity |
| **P1** | v0.8: Persistent catalog + pg_catalog views | ORM introspection |
| **P1** | v0.4: SSI predicate locking (SERIALIZABLE real) | Correctness claim |
| **P2** | v1.x: Views + Materialized Views | Very common pattern |
| **P2** | v0.8: Sequences (SERIAL/IDENTITY) | Every table with PK |
| **P2** | v0.9: Autovacuum + VACUUM | Heap bloat prevention |
| **P2** | v0.9: Streaming replication | HA production |
| **P2** | v0.9: pg_stat_* views | Operator diagnostics |
| **P3** | v1.x: PL/pgSQL | Stored procedures |
| **P3** | v1.x: Triggers | Legacy apps |
| **P3** | v1.x: Partitioning | Large tables |
| **P3** | v1.x: Full-text search | Common feature ask |
| **P4** | v0.7: SIMD vectorized execution | Performance edge |
| **P4** | v0.9: Logical replication | CDC, migrations |
| **P4** | v2.x: Extensions | Ecosystem completeness |

---

## v0.1 — "Bootstrap" ✅ COMPLETE

**Scope:** Workspace compiles, CI works, all crate skeletons exist.
No user-facing features.

- [x] Cargo workspace, MSRV pin, dual-license, contributor docs
- [x] Crate skeletons: core, storage, wal, mvcc, txn, parser, planner,
      optimizer, executor, vec, catalog, protocol, server, cli, bench
- [x] CI: fmt-check, clippy, test
- [x] AGENTS.md, ARCHITECTURE.md, PERFORMANCE.md, BENCHMARKS.md
- [x] Foundational types in `ultrasql-core` (errors, OIDs, datums, schema)
- [x] Lexer covering the PostgreSQL token set
- [x] Basic WAL structures (record format, group commit design)
- [x] MVCC snapshot + visibility rules (PostgreSQL `HeapTupleSatisfiesMVCC`)
- [x] TransactionManager with begin/commit/abort, in-memory CLOG
- [x] Basic page format (8 KiB, slotted, checksums)
- [x] Buffer pool (CLOCK-Pro, sharded page table)
- [x] Basic planner: SELECT with WHERE, ORDER BY, LIMIT
- [x] Pull-based executor scaffold: MemTableScan, Filter, Project, Limit
- [x] PostgreSQL wire protocol v3 basic (Simple Query, Startup handshake)
- [x] Server accepting TCP connections, serving in-memory sample data

---

## v0.2 — "Parse and Plan" 🔄

**Scope:** Parse and bind the full DML + core DDL SQL surface.
Produce typed logical plans for all common statement types.

### Parser: DML
<!-- wave 1 completed: 21a16e4..008b457 -->
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
<!-- wave 2 completed: 7415867..7647f43 -->
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
- [ ] `SELECT *` (currently returns `NotSupported`)
- [ ] `SELECT t.*` and table aliases
- [ ] Column aliases: `SELECT col AS alias`
- [ ] `INNER JOIN ... ON ...`
- [ ] `LEFT / RIGHT / FULL OUTER JOIN ... ON ...`
- [ ] `CROSS JOIN` / `JOIN ... USING (col)`
- [ ] `GROUP BY col1, col2` / `HAVING expr`
- [ ] `DISTINCT` / `DISTINCT ON (expr)`
- [ ] `UNION [ALL]` / `INTERSECT [ALL]` / `EXCEPT [ALL]`
- [ ] Subqueries in `FROM` (derived tables)
- [ ] Scalar subqueries in `WHERE`
- [ ] `EXISTS (subquery)` / `IN (subquery)` / `NOT IN (subquery)`
- [ ] `IN (val1, val2, ...)` literal list
- [ ] `ANY (subquery)` / `ALL (subquery)`
- [ ] `WITH cte AS (...) SELECT ...` (non-recursive CTEs)
- [ ] `WITH RECURSIVE cte AS (...) SELECT ...`
- [ ] `SAVEPOINT name` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT`
- [ ] `EXPLAIN` / `EXPLAIN ANALYZE`
- [ ] `SET [SESSION|LOCAL] var = val` / `SHOW var` / `RESET var`
- [ ] `PREPARE name AS ...` / `EXECUTE name (params)` / `DEALLOCATE name`

### Parser: Expressions
- [ ] `CASE WHEN ... THEN ... ELSE ... END`
- [ ] `COALESCE(a, b, ...)` / `NULLIF(a, b)`
- [ ] `GREATEST(...)` / `LEAST(...)`
- [ ] `BETWEEN ... AND ...`
- [ ] `LIKE` / `ILIKE` / `NOT LIKE`
- [ ] `IS NULL` / `IS NOT NULL`
- [ ] `IS DISTINCT FROM` / `IS NOT DISTINCT FROM`
- [ ] `CAST(x AS type)` and `x::type`
- [ ] String concatenation `||`
- [ ] Regex: `~`, `~*`, `!~`, `!~*`
- [ ] Bitwise: `&`, `|`, `#`, `~`, `<<`, `>>`
- [ ] JSON operators: `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`
- [ ] Array subscript `arr[n]`, slice `arr[m:n]`
- [ ] `AT TIME ZONE`
- [ ] `OVERLAPS`
- [ ] `ROW(a, b, c)` constructor
- [ ] Parameter placeholders `$1`, `$2`, ... (prepared statements)

### Planner updates
<!-- wave 2 partial: 7415867..7647f43 -->
- [ ] Binder handles JOINs (INNER, LEFT, RIGHT, FULL)
- [ ] Binder handles GROUP BY + aggregation
- [ ] Binder handles subqueries (correlated + uncorrelated)
- [ ] Binder handles CTEs
- [x] Logical plan nodes: `LogicalJoin`, `LogicalAggregate`, `LogicalUnion`, `LogicalInsert`, `LogicalUpdate`, `LogicalDelete` (Insert/Update/Delete shipped; Join/Aggregate/Union remain for wave 3)
- [ ] `SELECT *` expansion via catalog
- [x] Logical plan pretty-printer (display() extended for all DML variants)
- [ ] Parser fuzz target reaching 24 h CI-clean

---

## v0.3 — "Page and Pool" 🔄

**Scope:** A working storage engine that persists tuples and reads
them back with crash recovery. WAL wired to heap. No more in-memory-only data.

### WAL ↔ Storage Integration
<!-- wave 1 partial: 21a16e4..008b457 -->
- [ ] WAL writer wired to buffer pool dirty pages
- [ ] WAL LSN stamped on every page write
- [ ] Checkpointer background task (dirty page flush + WAL truncation)
- [ ] Crash recovery: replay WAL records on startup
- [x] WAL record types for heap inserts/updates/deletes (payload codecs landed; storage emission in wave 2)
- [ ] WAL record types for B-tree index changes
- [ ] Full page writes (FPW) on first write after checkpoint

### Heap Access Method
<!-- wave 1+2 partial: 21a16e4..7647f43 -->
- [x] `heap_insert`: write MVCC tuple to buffer pool page, emit WAL record
- [x] `heap_update`: HOT update chain when no indexed column changes
- [x] `heap_delete`: mark tuple dead (set xmax), emit WAL record
- [x] `heap_scan`: sequential scan with MVCC visibility filtering
- [x] Tuple header with xmin/xmax/cmin/cmax/infomask written correctly
- [ ] Free-space map (FSM) updated on insert/delete
- [ ] Visibility map (VM) updated on vacuum

### TOAST
- [ ] TOAST table per relation
- [ ] Inline short values, external large values (> 2 KiB)
- [ ] Compression for TOAST chunks (lz4)
- [ ] Detoasting on read

### Persistent CLOG
- [ ] Page-backed CLOG replacing in-memory `DashMap`
- [ ] CLOG trimming (old entries removable after vacuum)
- [ ] CLOG recovery on startup

### Property tests
- [ ] Page round-trip property tests
- [ ] WAL recovery correctness tests (deterministic simulation)
- [ ] Crash-recovery integration tests (kill + restart)

---

## v0.4 — "Transactions" ❌

**Scope:** ACID transactions with snapshot isolation and true
serializable (SSI). Real row-level locking. Deadlock detection.

### Lock Manager
- [ ] Fastpath relation locks (per-backend cache, no central state)
- [ ] Central lock table: `DashMap<LockTag, LockEntry>` with wait-for graph
- [ ] Deadlock detector background thread (configurable interval, default 1 s)
- [ ] Tuple-level locks for concurrent updates
- [ ] `SELECT FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` enforcement
- [ ] Advisory locks: `pg_advisory_lock`, `pg_try_advisory_lock`

### SSI (Serializable Snapshot Isolation)
- [ ] Predicate locks (`SIReadLock`)
- [ ] RW-anti-dependency tracking
- [ ] Dangerous structure detection (T1 → T2 → T3 cycle)
- [ ] Safe snapshot optimization
- [ ] True SERIALIZABLE (not just RepeatableRead alias — remove the TODO in `txn/src/manager.rs`)

### Subtransactions
- [ ] `SAVEPOINT name` execution (not just parsing)
- [ ] `ROLLBACK TO SAVEPOINT name`
- [ ] `RELEASE SAVEPOINT name`
- [ ] Subtransaction tracking in MVCC headers

### Two-Phase Commit
- [ ] `PREPARE TRANSACTION 'id'`
- [ ] `COMMIT PREPARED 'id'` / `ROLLBACK PREPARED 'id'`
- [ ] Persistence across restarts
- [ ] `pg_prepared_xacts` view

### Executor ↔ Storage wiring
- [ ] `SeqScan` operator reading real heap pages (replacing `MemTableScan`)
- [ ] `ModifyTable` operator for INSERT/UPDATE/DELETE on real heap
- [ ] Executor uses real `TransactionManager` snapshot for visibility

### Tests
- [ ] Loom-based concurrency model tests for lock manager
- [ ] Isolation level tests (READ COMMITTED, REPEATABLE READ, SERIALIZABLE)
- [ ] Serializability checker (Hermitage test suite)

---

## v0.5 — "Execute" ❌

**Scope:** Full physical operator set. Extended query protocol.
Real auth. Any standard PostgreSQL driver can connect.

### Scan Operators
- [ ] `SeqScan` with predicate pushdown (qual evaluation per tuple)
- [ ] `IndexScan` via B-tree (point lookup + range scan)
- [ ] `IndexOnlyScan` (skip heap fetch when VM bit is set)
- [ ] `BitmapIndexScan` + `BitmapHeapScan` (OR multiple indexes)
- [ ] `FunctionScan` (`generate_series`, `unnest`, SRFs)
- [ ] `ValuesScan` (for VALUES clauses)
- [ ] `CteScan` / `SubqueryScan`

### Join Operators
- [ ] `NestLoop` (with inner rescan)
- [ ] `HashJoin` (build + probe, spill to disk)
- [ ] `MergeJoin` (requires sorted input)
- [ ] All join types: INNER, LEFT, RIGHT, FULL, ANTI, SEMI

### Aggregation Operators
- [ ] `HashAggregate` with spill-to-disk
- [ ] `SortAggregate` (streaming over sorted input)
- [ ] Standard aggregates: COUNT, SUM, AVG, MIN, MAX, BOOL_AND, BOOL_OR, STRING_AGG, ARRAY_AGG, JSON_AGG
- [ ] Statistical aggregates: STDDEV, VARIANCE, CORR, PERCENTILE_CONT, PERCENTILE_DISC
- [ ] Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE
- [ ] `OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)`
- [ ] `WindowAgg` operator

### Other Operators
- [ ] `Sort` with external sort (spill when exceeds `work_mem`)
- [ ] `Unique` (DISTINCT)
- [ ] `SetOp` (UNION/INTERSECT/EXCEPT, hashed and sorted)
- [ ] `RecursiveUnion` (WITH RECURSIVE)
- [ ] `LockRows` (SELECT FOR UPDATE/SHARE)
- [ ] `Materialize` (pipeline breaker)
- [ ] `Gather` / `GatherMerge` (parallel query)
- [ ] `Append` / `MergeAppend` (partition scans)
- [ ] `Result` (constant expressions)

### Expression Evaluation
- [ ] Full general expression interpreter (replace hardcoded `FilterEqI32`)
- [ ] Vectorized expression eval over batches (OLAP pipelines)
- [ ] NULL propagation correctness in all operators
- [ ] Type coercion / implicit casts at execution time

### Memory Management
- [ ] Per-query `work_mem` budget enforced cooperatively
- [ ] Hash build and sort operators spill to temp segments
- [ ] `temp_file_limit` enforcement

### Wire Protocol: Extended Query
- [ ] `Parse` — parse named/unnamed statement, return `ParseComplete`
- [ ] `Bind` — bind parameters to portal, return `BindComplete`
- [ ] `Describe` — return `RowDescription` / `ParameterDescription`
- [ ] `Execute` — execute named portal, stream `DataRow`, return `CommandComplete`
- [ ] `Sync` — sync after error, return `ReadyForQuery`
- [ ] `Close` — close statement or portal
- [ ] Pipeline mode (multiple Parse/Bind/Execute before Sync)
- [ ] Server-side statement cache (keyed by name)
- [ ] Named portals (cursor via extended protocol)
- [ ] Binary transfer format for numeric, timestamp, etc.

### Authentication
- [ ] `SCRAM-SHA-256` real implementation (currently bypassed)
- [ ] `MD5` password auth (legacy, behind config flag)
- [ ] `trust` auth method (for local dev)
- [ ] `pg_hba.conf` equivalent — host-based auth rules
- [ ] Roles and passwords stored in `pg_authid`

### SSL/TLS
- [ ] `SSLRequest` handling (currently rejected)
- [ ] TLS upgrade via `rustls`
- [ ] `ssl_cert_file`, `ssl_key_file`, `ssl_ca_file` config

### Other Protocol Features
- [ ] `COPY TO STDOUT` / `COPY FROM STDIN` wire format
- [ ] Real `BackendKeyData` with PID + secret for `CancelRequest`
- [ ] `CancelRequest` handling (cancel running query)
- [ ] `NoticeResponse` (warnings, hints, info messages)
- [ ] `NotificationResponse` (LISTEN/NOTIFY)
- [ ] All expected `ParameterStatus` params: `TimeZone`, `DateStyle`, `IntervalStyle`, `extra_float_digits`, `standard_conforming_strings`, `integer_datetimes`, `server_encoding`
- [ ] Per-connection slow-loris timeout (TODO in `server/src/lib.rs`)

### CLI
- [ ] `ultrasql` REPL with history, multiline input
- [ ] Meta-commands: `\d`, `\dt`, `\di`, `\df`, `\dv`, `\ds`, `\du`, `\dn`, `\l`, `\c`, `\q`, `\i`, `\timing`, `\x`, `\pset`
- [ ] Connect via URL: `postgresql://user:pass@host/db`
- [ ] `PGPASSWORD`, `PGHOST`, `.pgpass` file support
- [ ] `--command/-c` and `--file/-f` batch mode

---

## v0.6 — "Optimize" ❌

**Scope:** Cost-based optimizer built from scratch.
Currently zero code exists in `ultrasql-optimizer`.

### Rule-Based Rewrites
- [ ] Constant folding and constant propagation
- [ ] Predicate pushdown through joins
- [ ] Predicate pushdown into subqueries and derived tables
- [ ] Projection pushdown (column pruning)
- [ ] Subquery decorrelation (correlated subquery → join)
- [ ] Outer-join elimination when predicates imply inner
- [ ] LIMIT pushdown into sort and scan
- [ ] Sort elimination via index order
- [ ] Common subexpression elimination
- [ ] IN-list to semi-join conversion

### Statistics Collection
- [ ] Per-column histograms (equi-depth, 100 buckets default)
- [ ] Most-common-values (MCVs) per column
- [ ] Per-relation row count and page count
- [ ] Index correlation (physical sort order vs logical order)
- [ ] `ANALYZE table` command
- [ ] Autovacuum triggers `ANALYZE` on heavily modified tables
- [ ] `pg_statistic` catalog table
- [ ] `CREATE STATISTICS` (extended stats: correlation, multi-column MCVs)

### Cost Model
- [ ] Selectivity estimation for equality, range, LIKE, IS NULL predicates
- [ ] Join cardinality estimation (independence assumption + MCV matching)
- [ ] Sequential scan cost formula
- [ ] Index scan cost formula (height + correlation-adjusted leaf fetch)
- [ ] Hash join cost formula (build + probe)
- [ ] Sort cost formula (O(n log n) + spill cost)
- [ ] Aggregate cost formula
- [ ] CPU operator costs (configurable: `cpu_tuple_cost`, `random_page_cost`, `seq_page_cost`)

### Join Enumeration
- [ ] DPsize (dynamic programming over subsets) for ≤ 10 relations
- [ ] Greedy/GEQO heuristic for > 10 relations
- [ ] Cascades-style memo with physical property requirements
- [ ] Join reordering with outer join constraints

### Physical Operator Selection
- [ ] NestLoop vs HashJoin vs MergeJoin
- [ ] IndexScan vs SeqScan vs BitmapHeapScan
- [ ] IndexOnlyScan when VM bit is set
- [ ] HashAggregate vs SortAggregate vs StreamAggregate
- [ ] Hash-based DISTINCT vs Sort-based DISTINCT
- [ ] Parallel plan generation and cost estimation

### Plan Cache
- [ ] Generic plan for prepared statements
- [ ] Custom plan when specific parameter values change the optimal plan
- [ ] Re-planning threshold (5× cost increase triggers re-plan)
- [ ] Plan invalidation on `ANALYZE` / DDL

### Milestone
- [ ] TPC-H scale 1 runs to completion on every query with correct results

---

## v0.7 — "Vectorize" ❌

**Scope:** Vectorized batch execution for analytic pipelines.
This is the main OLAP performance differentiator over PostgreSQL.

### Push-Based Pipeline Driver
- [ ] Planner tags pipelines as vectorized (OLAP) vs scalar (OLTP)
- [ ] Push-based pipeline driver replacing pull-based scaffold
- [ ] Vectorized SeqScan emitting 4096-row batches
- [ ] Vectorized filter kernel (SIMD selection vector)
- [ ] Vectorized projection kernel
- [ ] Vectorized hash join (build + probe over batches)
- [ ] Vectorized hash aggregate
- [ ] Vectorized sort

### SIMD Kernels
- [ ] ARM64 NEON kernels for filter, comparison, arithmetic (aarch64)
- [ ] AVX2 kernels for filter, comparison, arithmetic (x86_64)
- [ ] AVX-512 kernels (optional, gated on CPUID check)
- [ ] Auto-vectorized fallback (LLVM-generated, no intrinsics)
- [ ] Scalar fallback for correctness — property tested against SIMD path
- [ ] Bitmask-based NULL handling in SIMD kernels

### Dictionary Encoding
- [ ] Dictionary encoding for low-cardinality string columns
- [ ] Dictionary-aware filter (compare dict codes, not strings)
- [ ] Dictionary-aware GROUP BY
- [ ] Automatic encoding selection based on cardinality

### JIT Compilation
- [ ] LLVM IR generation for hot expression trees (via `inkwell`)
- [ ] JIT threshold: queries above N rows trigger compilation
- [ ] Inline function calls in JIT code
- [ ] `jit = on|off` GUC, `jit_above_cost` threshold

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
- [ ] `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`
- [ ] `pg_index`, `pg_constraint`, `pg_sequence`
- [ ] `pg_depend` (required for CASCADE DROP)
- [ ] `pg_description` (COMMENT ON)
- [ ] `pg_statistic`, `pg_statistic_ext`
- [ ] Catalog cache with `arc-swap` for wait-free reads
- [ ] Catalog snapshot for safe concurrent DDL
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
- [ ] `tokio-postgres` (Rust)

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
- [ ] TPC-B: correctness verified, throughput ≥ PostgreSQL, p99 < 5 ms at 32 connections
- [ ] TPC-C: correctness verified (all 5 transaction types)
- [ ] TPC-H scale 1: all 22 queries return correct results
- [ ] TPC-H scale 10: throughput within 2× of DuckDB
- [ ] Sysbench OLTP read/write: throughput ≥ PostgreSQL

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

## How features get on the roadmap

A feature on the roadmap has either an open RFC or a maintainer-signed
commitment. Ideas without either are not on the roadmap; they are
ideas. The discussion forum is GitHub Discussions; the formal record
is `rfcs/`.

---

*This document is the source of truth for UltraSQL's path to becoming
a production PostgreSQL replacement. Update it as work progresses.
Every checked box is a commitment delivered.*
