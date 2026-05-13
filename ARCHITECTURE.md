# UltraSQL Architecture

This document explains how UltraSQL's subsystems fit together. It is the
reference companion to [AGENTS.md](AGENTS.md). Every subsystem section
records its responsibility, its public contract, its rationale, the
tradeoffs it accepts, and the failure modes its tests cover.

The document moves bottom-up. Lower layers know nothing about the layers
above them. Higher layers depend on the lower-layer crates explicitly,
not by reaching into private modules.

---

## 0. Layered architecture

```text
┌──────────────────────────────────────────────────────────────────────┐
│                       Wire Protocol (v3 / SCRAM)                     │
│                            ultrasql-protocol                          │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│   Parser   │   Binder / Planner   │   Optimizer   │    Executor       │
│ ultrasql-  │  ultrasql-planner    │ ultrasql-     │ ultrasql-executor │
│ parser     │                      │ optimizer     │ ultrasql-vec      │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    Transaction Manager (txn, locks)                  │
│                            ultrasql-txn                              │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                ┌─────────────────┴─────────────────┐
                ▼                                   ▼
┌──────────────────────────┐         ┌──────────────────────────────┐
│      MVCC subsystem      │         │    WAL subsystem             │
│    ultrasql-mvcc         │         │    ultrasql-wal              │
└──────────────────────────┘         └──────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│   Storage engine: pages, buffer pool, heap AM, B+ tree, FSM, VM      │
│                          ultrasql-storage                            │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│   Core: errors, OIDs, datums, schema, identifiers, constants         │
│                            ultrasql-core                             │
└──────────────────────────────────────────────────────────────────────┘
```

The catalog (`ultrasql-catalog`) is orthogonal to this stack and is
consumed by the parser/planner/optimizer/executor for metadata.

---

## 1. ultrasql-core

**Responsibility.** Foundational types used everywhere: error enum,
identifiers (`Oid`, `RelId`, `BlockId`, `TupleId`, `Lsn`, `Xid`),
scalar values (`Datum`, `Value`, `DataType`), schema descriptors
(`Field`, `Schema`), and shared constants (page size, tuple alignment).

**Contracts.**

- All on-wire and on-disk integer types are little-endian. The crate
  exposes `read_le_*` / `write_le_*` helpers to avoid endianness bugs.
- `Datum` is a tagged representation. Fast-path access is via concrete
  accessors (`Datum::as_i64`) that return `Option<T>` on type mismatch.
- The error enum is `thiserror`-derived and never carries panics. All
  unrecoverable conditions surface as `Error::Internal(&'static str)`
  to keep the error type `Copy`-cheap.

**Rationale.** A central core crate forces an explicit dependency
graph. No crate may reach around it. The cost is a one-time decision
about which types are foundational; the benefit is a clean DAG.

**Tradeoffs.** `Datum` is a tagged union, not a generic. This costs a
discriminant byte per value and a branch per access. The alternative —
generic specialization through trait dispatch — was rejected because it
forces every executor operator to be generic over column types, which
hurts both compile times and code size.

**Future evolution.** A future RFC may introduce a separate
`ultrasql-arrow` crate exposing a zero-copy Arrow-compatible view of
batches for clients that speak Arrow Flight.

---

## 2. ultrasql-storage

**Responsibility.** Owns everything on disk. Subsystems:

- **Page format.** Fixed 8 KiB pages with a 24-byte header (LSN,
  checksum, flags, page type, lower/upper free-space pointers,
  next/prev page links for table chains, ItemId array length) and a
  slotted body (item-pointer array growing from the front, tuple data
  growing from the back).
- **Segment / file manager.** Allocates and tracks 1 GiB segment
  files per relation. Each segment exposes a `read_page` /
  `write_page` API backed by either `mmap` (default on macOS) or
  positional `pread` / `pwrite` (default on Linux for predictability).
- **Buffer pool.** CLOCK-Pro replacement with sharded page-table
  buckets keyed by `PageId`. Pins are reference-counted; a pinned page
  cannot be evicted. The pool is sized at startup; resizing is an
  RFC-level change.
- **Heap access method.** Slotted-page tuple storage with HOT
  (heap-only-tuple) chains for non-indexed updates to non-key columns.
  Compatible with PostgreSQL's tuple layout for line items so future
  pg_dump interop is feasible.
- **B+ tree access method.** Lehman-Yao concurrent B-link tree. Page
  splits use right-link pointers so readers never block on splitters.
- **Free-space map (FSM).** Per-relation summary of free space per
  page used by inserters to find a target page in O(log N).
- **Visibility map (VM).** Per-relation bitmap recording all-visible
  pages used by index-only scans to skip heap fetches.

**Contracts.**

- Page checksums are xxh3-64 truncated to 32 bits, stored in the
  header. A page with a bad checksum is a hard error; the buffer pool
  refuses to hand it out and surfaces `Error::Corruption`.
- The buffer pool obeys the [latch order](#latch-order): per-page
  content latch, then page-table partition lock, in that direction
  only.
- Frame allocation never sleeps with a buffer-pool lock held; eviction
  uses try-locks and falls back to the CLOCK hand if the candidate is
  pinned.

**Rationale.** PostgreSQL's storage format is the most widely
deployed in the world. Matching tuple layout for the common case
costs little and buys interop. CLOCK-Pro beats classical LRU on mixed
OLTP/OLAP workloads where a single big scan would otherwise evict the
working set; the cost is a slightly more complex eviction state
machine.

**Tradeoffs.** Fixed 8 KiB pages match PostgreSQL but are smaller than
the natural unit on modern NVMe (4 KiB) or analytical engines (256 KiB).
We accept this for compatibility; a future segment format may introduce
larger physical pages with logical 8 KiB sub-pages for the analytical
heap.

**Performance implications.** The slotted-page layout is the same hot
path for OLTP inserts as PostgreSQL. The B-link tree variant scales
better than the lock-coupling tree used by PostgreSQL on concurrent
inserters; this is one of the main concurrency wins.

**Scalability implications.** The buffer pool's sharded page table
scales linearly with cores up to the number of shards (default 64).
Beyond that, contention on the global eviction state dominates; this
is a known bottleneck and an open performance RFC.

---

## 3. ultrasql-wal

**Responsibility.** Append-only redo log with group commit.

**Record layout.**

```text
┌──────────┬──────────┬───────────┬─────────────┬───────────┬─────────┐
│  total   │  prev    │   xid     │   record    │   crc32c  │ payload │
│  length  │   lsn    │           │    type     │   (4)     │  bytes  │
│   (4)    │   (8)    │   (8)     │    (1)      │           │   ...   │
└──────────┴──────────┴───────────┴─────────────┴───────────┴─────────┘
```

**Group commit.** Writers append to an in-memory ring; a single fsync
thread batches outstanding records into one WAL segment write per fsync
window. The window is the smaller of (a) wall-clock 200 µs or (b) the
batch size at which the next write would exceed 256 KiB. Both are
tunable.

**Contracts.**

- A record is durable when its LSN is ≤ `flushed_lsn` and
  `flushed_lsn` has been observed under acquire ordering.
- Recovery replays records in LSN order. A CRC mismatch terminates
  replay at that record; subsequent bytes are treated as torn-write
  residue.
- WAL segments are 16 MiB and recycle after archiving / the configured
  retention window.

**Rationale.** Group commit is the single largest OLTP throughput
multiplier on rotational storage and on NVMe under bursty write
patterns. The implementation here borrows from PostgreSQL's
`XLogFlush` design but uses lock-free channels rather than condition
variables.

**Tradeoffs.** A bounded fsync window adds at most one window of
latency to single-transaction commits. We default to 200 µs because
typical NVMe write latencies are 30 – 80 µs, so the window amortizes
across 3 – 6 in-flight transactions without adding meaningful tail
latency.

---

## 4. ultrasql-mvcc

**Responsibility.** Transaction identifiers, tuple visibility, and
snapshot construction.

**Tuple header.**

```text
struct TupleHeader {
    xmin:  Xid,     // inserter
    xmax:  Xid,     // deleter (0 if alive)
    cmin:  CommandId,
    cmax:  CommandId,
    infomask: u16,  // status bits: HASNULL, COMMITTED, ABORTED, ...
    null_bitmap_off: u16,
    attr_count: u16,
    flags:     u16,
}
```

**Visibility rules.** A tuple `T` is visible to a snapshot `S` iff:

1. `T.xmin` is committed before `S.xmin` and either
   `T.xmax = 0` or `T.xmax` is not committed before `S.xmin`, **or**
2. `T.xmin = S.current_xid` and `T.cmin < S.current_command`.

The rules match PostgreSQL's `HeapTupleSatisfiesMVCC`. Serializable
isolation layers additional predicate locking on top; see the txn
section.

**Rationale.** Matching PostgreSQL semantics buys behavioral
compatibility for application code that depends on snapshot isolation
quirks (visibility of own writes in the same statement, READ COMMITTED
re-snapshot per statement, etc.).

**XID width.** 64 bits. The PostgreSQL wraparound vacuum problem is
not a problem we want to inherit. The cost is 4 bytes per tuple header
field; at 8 bytes for xmin and xmax, the per-tuple cost is the same
as PostgreSQL's 32-bit xmin + 32-bit xmax + epoch tracking machinery.

---

## 5. ultrasql-txn

**Responsibility.** Transaction lifecycle, locking, deadlock detection,
serializable conflict tracking.

**Transaction lifecycle.** `Begin → Active → (Committing | Aborting) →
Done`. The transaction record carries the snapshot, the XID, the
read/write set summary, and a per-transaction WAL buffer that flushes
on commit.

**Locking.** Two layers:

- **Fastpath relation locks.** Per-backend cached AccessShare locks
  with no central state, modeled on PostgreSQL's fastpath locking.
- **Central lock table.** Sharded `DashMap<LockTag, LockEntry>` with a
  wait-for graph maintained per-entry. The deadlock detector runs on
  a dedicated thread at a configurable interval (default 1 s) and
  aborts the youngest victim in any cycle.

**Isolation levels.**

| Level             | Implementation                                       |
| ----------------- | ----------------------------------------------------- |
| READ COMMITTED    | Re-snapshot per statement.                            |
| REPEATABLE READ   | Snapshot per transaction; first-update wins.          |
| SERIALIZABLE      | SSI (predicate locks + RW-conflict graph).            |

**Rationale.** PostgreSQL's SSI is the only published serializable
implementation that retains MVCC's read-doesn't-block-write property.
We adopt it directly.

---

## 6. ultrasql-parser

**Responsibility.** Source text → AST.

**Lexer.** Hand-written, single-pass, SIMD-friendly. Token kinds cover
PostgreSQL keywords, operators (including custom operators `~~`,
`@@`, etc.), integer/float/hex/exponent literals, single-quoted /
E-prefix / dollar-quoted strings, double-quoted identifiers, line and
block comments (block comments nest), and the parameter placeholder
`$N`.

**Parser.** Recursive descent at statement level, Pratt at expression
level. Operator precedence matches PostgreSQL. Each AST node carries a
`Span { start, end }` referencing the source for error messages.

**AST.** A typed enum per statement category (`Select`, `Insert`,
`Update`, `Delete`, `CreateTable`, `CreateIndex`, `Begin`, `Commit`,
`Rollback`, `Set`, `Explain`, ...). Expressions are a tree of
`Expr::Column`, `Expr::Literal`, `Expr::Unary`, `Expr::Binary`,
`Expr::FunctionCall`, `Expr::Cast`, `Expr::Case`, `Expr::Subquery`, ...

**Rationale.** A hand-written parser is the only way to deliver
PostgreSQL-quality error messages with source spans. Generator-based
parsers (LALRPOP, pest) produce parsers, not error stories.

---

## 7. ultrasql-planner

**Responsibility.** AST → bound, type-checked logical plan.

**Stages.**

1. **Bind.** Resolve table names, alias scopes, function names, and
   column references. Produces a bound AST where every name maps to a
   stable `Oid` or local `BindingId`.
2. **Typecheck.** Annotate each expression with its `DataType` and
   insert implicit casts per the PostgreSQL coercion matrix.
3. **Lower.** Translate to a logical plan tree
   (`LogicalScan`, `LogicalFilter`, `LogicalProject`, `LogicalJoin`,
   `LogicalAggregate`, `LogicalSort`, `LogicalLimit`,
   `LogicalUnion`, `LogicalInsert`, `LogicalUpdate`, `LogicalDelete`,
   ...).

**Rationale.** A separate bound representation between the AST and
the logical plan keeps the optimizer free of name-resolution concerns.

---

## 8. ultrasql-optimizer

**Responsibility.** Logical plan → physical plan.

**Stage A: rule-based rewrites.**

- Constant folding.
- Predicate pushdown (through joins, into scans, into subqueries).
- Projection pushdown (don't read columns we won't use).
- Subquery decorrelation (apply → join).
- Outer-join elimination where predicates allow.
- Limit pushdown into sort and scan.

**Stage B: cost-based search.**

- Cascades-style top-down enumeration with memoization.
- Join enumeration: DPsize for small queries (≤ 10 relations),
  greedy heuristic above that.
- Physical operator selection: NLJ / Hash / Merge for joins,
  IndexScan / SeqScan for accesses, HashAggregate / SortAggregate /
  StreamAggregate for aggregates.

**Statistics.** Per-column histograms (equi-depth + most-common
values), per-relation row count, per-relation page count, per-index
correlation. Stats are refreshed by `ANALYZE` and incrementally by
the heap on heavy modifications.

**Rationale.** Cascades is the gold standard for cost-based search
and is the basis of SQL Server's optimizer. The complexity is worth it
because PostgreSQL's bottom-up dynamic programming becomes painful past
8 relations and we want to leave room for cross-block optimization.

---

## 9. ultrasql-executor

**Responsibility.** Physical plan → results.

**Models.** Push-based pipeline executor, with operators implemented
in two flavors: scalar (tuple-at-a-time) for OLTP and vectorized
(batch-at-a-time) for OLAP. The optimizer tags each pipeline with its
preferred flavor based on cardinality estimates and the operator mix.

**Parallelism.** Pipeline-level parallelism via partitioning. A
parallel scan emits to N partitioned downstream pipelines, joined at a
gather operator at the top of the plan. The degree of parallelism is
chosen per plan based on cost and available cores; the runtime obeys
`max_parallel_workers_per_query` (default 4 on Apple M-series, half of
host cores on Linux servers).

**Memory budget.** Hash-build and sort operators participate in a
per-query memory budget. Exceeding the budget spills to disk via
the storage layer's temp segments. The budget defaults to
`work_mem * max_concurrent_queries` and is enforced cooperatively by
operators on every batch boundary.

---

## 10. ultrasql-vec

**Responsibility.** Column-oriented batches and the SIMD kernels that
process them.

**Batch.** `Batch { columns: SmallVec<[Column; 8]>, len: u16 }`.
`Column = { dtype: DataType, validity: Bitmap, data: ColumnBuffer }`.
`ColumnBuffer` is one of: aligned numeric (`AlignedVec<T>`),
varbinary (offset + value buffers), dictionary (codes + dict).

**Kernels.** Filter, projection, comparison, arithmetic, hash, sort,
aggregate. Each kernel has:

- A portable scalar implementation that is the source of truth.
- An auto-vectorized loop that LLVM tends to vectorize well.
- An optional hand-written intrinsics path behind `cfg(target_arch)`
  guards, validated against the scalar path by property tests.

**Rationale.** SIMD is most of the speedup over PostgreSQL on
analytical queries. The scalar/auto/intrinsic three-tier approach keeps
the scalar version honest as the differential oracle.

---

## 11. ultrasql-catalog

**Responsibility.** The system catalog. Stored physically as regular
heap tables (`pg_namespace`, `pg_class`, `pg_attribute`, `pg_index`,
...). Read paths use an in-memory cache backed by `arc-swap` for
wait-free reads.

**MVCC of catalog data.** Catalog tuples carry the same MVCC headers
as user data. A statement obtains a *catalog snapshot* at the moment
it enters the planner; subsequent DDL in concurrent transactions does
not perturb that statement's view. This matches PostgreSQL semantics
and is what allows online DDL to be safe.

---

## 12. ultrasql-protocol

**Responsibility.** PostgreSQL wire protocol v3.

**Implementation.** The protocol layer is pure data shuffling. It
parses incoming messages into typed enums (`StartupMessage`,
`Parse`, `Bind`, `Describe`, `Execute`, `Sync`, `Query`, `Terminate`,
...) and serializes outgoing ones (`ReadyForQuery`, `RowDescription`,
`DataRow`, `CommandComplete`, `ErrorResponse`, ...).

**Authentication.** SCRAM-SHA-256 is the only supported auth method
out of the box. Cleartext and md5 are accepted only for migration
scenarios and are gated behind a config flag.

---

## 13. ultrasql-server

**Responsibility.** The `ultrasqld` binary. Owns the accept loop, the
per-connection task, and the lifecycle of the storage stack.

**Connection model.** One async task per connection on the Tokio
runtime. CPU-bound query execution offloads to a `rayon`-style worker
pool sized to (cores − 2) by default.

**Lifecycle.** `start` → load config → recover storage → initialize
catalog cache → start WAL writer → start checkpointer → bind listener
→ accept → on `SIGTERM`, drain → checkpoint → flush WAL → exit.

---

## 14. Latch order {#latch-order}

When acquiring multiple latches, follow this order strictly. A
violation is a deadlock waiting to happen and a tracked CI lint
target.

1. Catalog snapshot lock (read-only).
2. Transaction snapshot lock (read-only).
3. Lock-manager partition lock.
4. Buffer-pool page-table partition lock.
5. Per-page content latch (shared or exclusive).
6. WAL insert lock.
7. Per-segment file lock.

Helpers that acquire multiple latches document the order in their
contract.

---

## 15. On-disk format stability

UltraSQL's on-disk format is versioned. The page header carries a 4-bit
format version. The current version is `1`. Format changes go through
the RFC process and ship with a migration utility.

---

## 16. Future work

- **Distributed execution.** A future RFC introduces a coordinator
  layer over single-node `ultrasqld` instances with a Raft-replicated
  catalog and partitioned tables. The current single-node design
  leaves the relevant seams in place (catalog snapshot, executor
  shuffles via gather operators).
- **Compressed storage.** Page-level compression (lz4, zstd) with a
  per-page indirection in the FSM. The page format already has a
  `compressed` flag and reserves the layout slot.
- **Adaptive indexing.** The optimizer collects which predicates miss
  by index; an `ANALYZE`-equivalent background task can recommend (or
  in a future mode automatically build) the missing index.
