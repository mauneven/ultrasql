# UltraSQL Roadmap

This roadmap is a commitment, not a wish list. Items move from
"planned" to "in progress" to "shipped" via the RFC process and the
release process. Stretch goals are marked with `?`; everything else is
on the path.

Releases follow [semantic versioning](https://semver.org/). Pre-1.0
releases may break compatibility between minor versions; we document
the breakage in each release's notes.

---

## v0.1 — "Bootstrap" (foundation)

**Scope:** A workspace that compiles, runs CI, and contains the
foundational types and skeletons of every subsystem. No user-facing
features.

- [x] Cargo workspace, MSRV pin, dual-license, contributor docs.
- [x] Crate skeletons: core, storage, wal, mvcc, txn, parser, planner,
      optimizer, executor, vec, catalog, protocol, server, cli, bench.
- [x] CI: fmt-check, clippy, test.
- [x] AGENTS.md, ARCHITECTURE.md, PERFORMANCE.md, BENCHMARKS.md.
- [x] Foundational types in `ultrasql-core` (errors, OIDs, datums,
      schema).
- [x] Lexer covering the PostgreSQL token set.

---

## v0.2 — "Parse and Plan" (front end)

**Scope:** Parse and bind a meaningful subset of SQL; produce a typed
logical plan.

- [ ] Parser for SELECT (with WHERE / GROUP BY / ORDER BY / LIMIT /
      OFFSET), INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX,
      BEGIN, COMMIT, ROLLBACK, SET.
- [ ] Binder with column/alias resolution, type checking, implicit
      coercion.
- [ ] Logical plan tree with pretty-printer.
- [ ] EXPLAIN that prints both the logical and physical trees.
- [ ] Parser fuzz target reaching 24 h CI-clean.

---

## v0.3 — "Page and Pool" (storage)

**Scope:** A working storage engine that can persist tuples and read
them back, with crash recovery.

- [ ] 8 KiB slotted page format with checksums.
- [ ] Segment / file manager with mmap and direct-IO paths.
- [ ] Buffer pool (CLOCK-Pro) with sharded page table.
- [ ] Heap access method (insert, update, delete, scan).
- [ ] WAL with group commit and crash recovery.
- [ ] Property tests for page round-trips and recovery.

---

## v0.4 — "Transactions" (MVCC + locking)

**Scope:** ACID transactions with snapshot isolation, then serializable.

- [ ] XID, snapshot, visibility predicate.
- [ ] Tuple header with xmin/xmax/cmin/cmax/infomask.
- [ ] Lock manager with deadlock detection.
- [ ] READ COMMITTED and REPEATABLE READ.
- [ ] SERIALIZABLE via SSI.
- [ ] Loom-based concurrency model tests.

---

## v0.5 — "Execute" (executor + protocol)

**Scope:** End-to-end query execution exposed over the PostgreSQL wire
protocol.

- [ ] Push-based executor with seq scan, filter, projection, NLJ,
      sort, limit.
- [ ] Hash join and hash aggregate.
- [ ] Index scan via B+ tree.
- [ ] PostgreSQL wire v3 protocol: startup, simple query, extended
      query, ErrorResponse, NoticeResponse.
- [ ] SCRAM-SHA-256 auth.
- [ ] CLI client (`ultrasql`) with REPL.

---

## v0.6 — "Optimize" (cost-based planner)

**Scope:** A non-trivial optimizer that produces plans competitive on
analytic queries.

- [ ] Rule-based rewrites (predicate / projection pushdown, constant
      folding, subquery decorrelation).
- [ ] Cost model with per-column histograms.
- [ ] DPsize join enumeration up to 10 relations.
- [ ] Cascades-style memo for physical operator choice.
- [ ] TPC-H scale 1 runs to completion on every query.

---

## v0.7 — "Vectorize" (SIMD execution)

**Scope:** Vectorized batch execution for analytic pipelines.

- [ ] Column batch primitives in `ultrasql-vec`.
- [ ] Vectorized filter, projection, hash join build/probe, hash
      aggregate, sort.
- [ ] NEON kernels on ARM64; AVX2 kernels on x86_64.
- [ ] Planner chooses scalar vs vectorized per pipeline.
- [ ] TPC-H scale 10 runs to completion, throughput recorded.

---

## v0.8 — "Index and Constrain" (DDL + constraints)

- [ ] B+ tree concurrent (Lehman-Yao) index AM.
- [ ] UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK, NOT NULL.
- [ ] Online index build via concurrent scan + catchup.
- [ ] System catalog tables persisted (not just in-memory).
- [ ] `pg_catalog` shims sufficient for psql `\d`.

---

## v0.9 — "Operate" (operability)

- [ ] Hot backup and PITR via WAL archive + base backup.
- [ ] Replication (physical streaming) to a standby.
- [ ] Prometheus metrics endpoint.
- [ ] Tracing via OpenTelemetry.
- [ ] Health and readiness endpoints.
- [ ] First documented operations runbook.

---

## v1.0 — "Ship" (production-grade single-node)

**Definition of done:**

- Passes a curated subset of the PostgreSQL regression suite covering
  the planner, executor, types, and transactions.
- Passes TPC-B and TPC-C correctness.
- Sustains 50k tx/s on TPC-B 1× scale on M4 Mac mini at 32 connections
  with p99 latency under 5 ms.
- Sustains TPC-H scale 10 query throughput within 2× of DuckDB on the
  same host.
- Zero open critical or high-severity bugs.
- Three independent operators have run UltraSQL in their environments
  for a month and reported.

---

## Beyond v1.0

- **v1.x:** Stored procedures (PL/pgSQL subset), triggers, table
  partitioning, declarative partition pruning, parallel CREATE INDEX,
  bloom-filter indexes, BRIN indexes, GIN/GiST.
- **v2.0?**: Distributed execution with a coordinator, partitioned
  tables, Raft-replicated catalog, cross-region async replication.
- **v2.x?**: Native columnar storage tier for cold partitions, ZSTD
  page compression, native Apache Arrow Flight endpoint, query
  result cache, federated foreign-data-wrapper layer.

---

## How features get on the roadmap

A feature on the roadmap has either an open RFC or a maintainer-signed
commitment. Ideas without either are not on the roadmap; they are
ideas. The discussion forum is GitHub Discussions; the formal record
is `rfcs/`.
