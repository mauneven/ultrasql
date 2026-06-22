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
│                          Wire Protocol (v3)                          │
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
`ultrasql-arrow` crate exposing a zero-copy Arrow-layout view of
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
  `write_page` API backed by positional `pread` / `pwrite`; the
  historical `use_mmap` knob is retained for older configurations but does
  not currently mmap heap segment files.
- **Buffer pool.** Classic CLOCK replacement (single rotating hand that
  clears each frame's reference bit and evicts on the next sweep) with
  sharded page-table buckets keyed by `PageId`. Pins are reference-counted;
  a pinned page cannot be evicted. The pool is sized at startup; resizing is
  an RFC-level change. CLOCK-Pro is a known follow-up; the eviction trait
  surface is kept stable so the upgrade is a drop-in. Under pressure the pool
  can also flush dirty pages to make eviction progress, gated by the WAL durable
  LSN (see Contracts).
- **Heap access method.** Slotted-page tuple storage with HOT
  (heap-only-tuple) chains for non-indexed updates to non-key columns.
  The tuple layout stays stable so archive/export tooling can inspect rows.
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
- On buffer-pool exhaustion the pool invokes an owning-service
  `EvictionRelief` hook (installed at startup, run only after all frame/miss
  latches are released) that flushes dirty frames whose page-LSN is at or below
  the WAL's `durable_lsn` — forcing the WAL durable to the oldest unflushable
  dirty LSN first when every dirty frame is blocked — then retries, up to a
  bounded round count before surfacing `Exhausted`. Write-ahead-log ordering is
  preserved (WAL before data) and relief never runs under a frame/miss latch.

**Rationale.** A compact slotted-page format keeps OLTP updates cheap while
remaining inspectable by tooling. Classic CLOCK approximates LRU at a fraction
of the bookkeeping cost — a single reference bit per frame and one rotating
hand — which is why it is the default replacement policy; the planned CLOCK-Pro
upgrade would further resist a single big scan evicting the OLTP working set on
mixed OLTP/OLAP workloads, at the cost of a more complex eviction state machine.

**Tradeoffs.** Fixed 8 KiB pages are smaller than the natural unit on modern
NVMe (4 KiB) or analytical engines (256 KiB). We accept this for predictable
OLTP behavior; a future segment format may introduce larger physical pages
with logical 8 KiB sub-pages for the analytical heap.

**Columnar secondary layout.** The row-store heap remains the source of
truth for OLTP, WAL, and MVCC. For OLAP scans, the server maintains a
same-table columnar shadow in `HeapAccess::column_cache`: committed DML
bumps the relation version, queues a background rebuild, and drops stale
columns. Rebuild scans use an MVCC snapshot, materialize fixed-width
numeric columns into typed buffers, and record logical segment row
counts for future on-disk segment spill. Both publishing to and reading from
the shadow go through a snapshot-coherence gate
(`ColumnCache::is_snapshot_coherent` / `get_for_snapshot`) that admits a
snapshot to the version-keyed projection only when its in-progress set is empty
and the version's last writer is invalid, own, or committed — i.e. the relation
is quiescent for that snapshot. Under any concurrency the reader falls back to a
correct heap scan, so stale or incoherent columnar data is never returned.

**Performance implications.** The slotted-page layout keeps OLTP inserts on a
compact row path. The B-link tree variant scales better than lock-coupling
trees on concurrent inserters; this is one of the main concurrency wins.

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
tunable. The fsync at the end of each window is `full_fsync`
(`crates/ultrasql-core/src/fsync.rs`), which on macOS issues `fcntl(F_FULLFSYNC)`
to force the drive to flush its own write cache (as PostgreSQL and SQLite do),
falling back to `sync_all` only on `ENOTSUP`/`EOPNOTSUPP`/`EINVAL`. The same
`full_fsync` backs every other file-data barrier — data segments, catalog/clog
snapshots, metadata, the WAL manifest, recovery truncation, and export — so a
plain `sync_all` (which does not flush the drive cache on macOS) is never the
last barrier before a committed write is reported durable.

**Contracts.**

- A record is durable when its LSN is ≤ `flushed_lsn` and
  `flushed_lsn` has been observed under acquire ordering.
- Recovery replays records in LSN order. Truncation or CRC mismatch is
  treated as torn-write residue only at the final segment tail; corruption
  before later bytes or later segments is fatal.
- WAL segments are 16 MiB and roll over inline when full. Checkpoint-driven
  segment recycling is implemented: `ultrasql_wal::truncate_below` removes whole
  segments below a crash-safe floor (min of the redo point, the oldest
  in-progress transaction LSN, and each vector-index snapshot LSN), written and
  fsynced via the manifest before any segment is unlinked, and an automatic
  checkpoint timer drives it — so WAL size and restart-replay time are bounded
  by un-checkpointed work, not total history. Archiving and retention-based
  reclamation beyond recycling remain open items (see ROADMAP).

**Rationale.** Group commit is the single largest OLTP throughput
multiplier on rotational storage and on NVMe under bursty write
patterns. The implementation batches flush requests through lock-free channels
rather than condition variables.

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

The rules implement snapshot visibility for committed rows and own writes.
Serializable isolation layers additional predicate locking on top; see the txn
section.

**Rationale.** Snapshot semantics must be predictable for application code
that depends on visibility of own writes in the same statement and
READ COMMITTED re-snapshot per statement.

**XID width.** 64 bits. 32-bit transaction-id wraparound is not a problem we
want to inherit. The cost is 4 bytes per tuple header field; at 8 bytes for
xmin and xmax, the per-tuple cost remains bounded and simple.

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
  with no central state.
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

**Rationale.** SSI retains MVCC's read-doesn't-block-write property while
detecting dangerous structures. UltraSQL implements that model directly.

---

## 6. ultrasql-parser

**Responsibility.** Source text → AST.

**Lexer.** Hand-written, single-pass, SIMD-friendly. Token kinds cover
SQL keywords, operators (including custom operators `~~`, `@@`, etc.),
integer/float/hex/exponent literals, single-quoted /
E-prefix / dollar-quoted strings, double-quoted identifiers, line and
block comments (block comments nest), and the parameter placeholder
`$N`.

**Parser.** Recursive descent at statement level, Pratt at expression
level. Operator precedence is explicit in the parser. Each AST node carries a
`Span { start, end }` referencing the source for error messages.

**AST.** A typed enum per statement category (`Select`, `Insert`,
`Update`, `Delete`, `CreateTable`, `CreateIndex`, `Begin`, `Commit`,
`Rollback`, `Set`, `Explain`, ...). Expressions are a tree of
`Expr::Column`, `Expr::Literal`, `Expr::Unary`, `Expr::Binary`,
`Expr::FunctionCall`, `Expr::Cast`, `Expr::Case`, `Expr::Subquery`, ...

**Rationale.** A hand-written parser is the only way to deliver high-quality
error messages with source spans. Generator-based parsers (LALRPOP, pest)
produce parsers, not error stories.

---

## 7. ultrasql-planner

**Responsibility.** AST → bound, type-checked logical plan.

**Stages.**

1. **Bind.** Resolve table names, alias scopes, function names, and
   column references. Produces a bound AST where every name maps to a
   stable `Oid` or local `BindingId`.
2. **Typecheck.** Annotate each expression with its `DataType` and
   insert implicit casts per the SQL coercion matrix.
3. **Lower.** Translate to a logical plan tree
   (`LogicalScan`, `LogicalFilter`, `LogicalProject`, `LogicalJoin`,
   `LogicalAggregate`, `LogicalWindow`, `LogicalSort`, `LogicalLimit`,
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

**Stage B: cost-based join reordering.**

- Cost-based inner-join reordering (DPsize for ≤ 10 relations, greedy
  heuristic above that) is the only cost-driven choice currently wired into
  the production plan path.
- A Cascades-style memo and per-operator cost formulas exist as scaffolding
  for a future search driver but are not yet wired.
- Physical operator selection (NLJ / Hash / Merge join, IndexScan / SeqScan,
  HashAggregate / SortAggregate / StreamAggregate) is currently made by
  structural rules during executor/server lowering, not by a cost search.

**Statistics.** Per-column histograms (equi-depth + most-common
values), per-relation row count, per-relation page count, per-index
correlation. Stats are refreshed by `ANALYZE` and incrementally by
the heap on heavy modifications.

**Rationale.** Cascades is widely used for cost-based search and is the
basis of SQL Server's optimizer. Bottom-up dynamic programming becomes painful
past 8 relations; we want to leave room for cross-block optimization.

---

## 9. ultrasql-executor

**Responsibility.** Physical plan → results.

**Models.** Push-based pipeline executor, with operators implemented
in two flavors: scalar (tuple-at-a-time) for OLTP and vectorized
(batch-at-a-time) for OLAP. The optimizer tags each pipeline with its
preferred flavor based on cardinality estimates and the operator mix.
The executor includes a window operator (`WindowAgg`) supporting explicit
frames (`ROWS`/`RANGE`/`GROUPS` bounds and `EXCLUDE`) and aggregate window
functions over frames.

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

**Rationale.** SIMD is central to fast analytical queries. The
scalar/auto/intrinsic three-tier approach keeps the scalar version honest as
the differential oracle.

---

## 11. ultrasql-catalog

**Responsibility.** The system catalog. Stored physically as regular
heap tables (`pg_namespace`, `pg_class`, `pg_attribute`, `pg_index`,
...). Read paths use an in-memory cache backed by `arc-swap` for
wait-free reads.

**MVCC of catalog data.** Catalog tuples carry the same MVCC headers
as user data. A statement obtains a *catalog snapshot* at the moment
it enters the planner; subsequent DDL in concurrent transactions does
not perturb that statement's view. This stable snapshot is what allows online
DDL to be safe.

**Constraint evolution.** `ALTER TABLE ... ADD CONSTRAINT` supports `CHECK`,
`UNIQUE`, and `PRIMARY KEY` (each validated against existing rows at `ADD` time
and persisted in `pg_constraint`), and `DROP CONSTRAINT [IF EXISTS]` tombstones
the constraint row and drops the backing unique index. `FOREIGN KEY` / `EXCLUDE`
via `ALTER TABLE` are not supported and return `0A000`; declare them in
`CREATE TABLE`.

---

## 12. ultrasql-protocol

**Responsibility.** Wire protocol v3.

**Implementation.** The protocol layer is pure data shuffling. It
parses incoming messages into typed enums (`StartupMessage`,
`Parse`, `Bind`, `Describe`, `Execute`, `Sync`, `Query`, `Terminate`,
...) and serializes outgoing ones (`ReadyForQuery`, `RowDescription`,
`DataRow`, `CommandComplete`, `ErrorResponse`, ...).

**Authentication.** Authentication wired on a live connection is `Trust`
(accept all — the default), a single global MD5 credential, per-role
SCRAM-SHA-256, or a `pg_hba` policy. The SCRAM-SHA-256 server state machine,
the rustls TLS loader, and the `pg_hba` matcher are negotiated live: an
`SSLRequest` is answered with `S` and the stream is upgraded to TLS in place
when a server certificate is configured (rejecting any plaintext buffered
before the handshake, per CVE-2021-23214), and `pg_hba` rules select
`hostssl`/`hostnossl` and run SCRAM against each role's own stored verifier. A
`GSSENCRequest` is still answered with an `N` decline. Open items: the
`md5`/`password` `pg_hba` methods are rejected because role credentials are
stored only as SCRAM verifiers, and client-certificate / `pg_ident` mapping
flows are not yet wired.

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

**Result path.** A large top-level Simple-Query `SELECT` whose encoded body
exceeds the streaming high-water mark is streamed to the socket in bounded
memory windows (carrying an autocommit transaction clone) rather than fully
buffered, so peak wire-buffer memory is bounded by result size. Streaming is
gated to the single-statement network path; the batch/embedded path always
fully buffers.

**Per-session transaction state.** Each connection owns a
`TxnState` machine with three variants — `Idle`, `InTransaction(txn)`,
`Failed(txn)` — that mirrors the wire readiness status bytes (`'I'`, `'T'`,
`'E'`). `BEGIN` transitions `Idle →
InTransaction`; `COMMIT`/`ROLLBACK` finalise via
`TxnManager::commit`/`abort` and return to `Idle`. Any executor error
inside `InTransaction` transitions to `Failed`, and every subsequent
non-transaction-control statement returns SQLSTATE `25P02` until the
user issues `COMMIT` (treated as `ROLLBACK`) or `ROLLBACK`. The state
is single-threaded per-session, so no synchronisation primitive
guards it.

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
