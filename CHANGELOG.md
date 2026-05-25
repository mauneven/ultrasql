# Changelog

All notable UltraSQL user-visible changes are tracked here. UltraSQL follows
semantic versioning after v1.0; pre-1.0 releases may still break compatibility
and must document the break here.

## Unreleased

### Added

- PostgreSQL wire-compatible SQL server, CLI, and local runner binaries.
- MVCC heap storage, WAL, crash-recovery primitives, B-tree/BRIN/GiST/GIN
  foundations, COPY, backup/restore utilities, and catalog version guard.
- SQL coverage for common DDL/DML/SELECT paths, transactions, savepoints,
  JSONB operators, range/geometric types, vector types, HNSW/IVFFlat indexes,
  RAG helper functions, external scans, and lakehouse-oriented CSV/Parquet
  surfaces.
- Exact vector top-k fallback now uses bounded `TopK` instead of physical
  Sort and exposes exact kernel/fallback state in `EXPLAIN ANALYZE`.
- Same-host AI/vector certification runner now requires measured UltraSQL and
  PostgreSQL + pgvector exact top-k artifacts before passing.
- Same-host TPC-H SF1 PostgreSQL 17 certification now runs complete q1..q22
  raw artifacts and records the 2x geometric-mean pass/fail decision.
- HNSW/IVFFlat durability certification covers torn ANN WAL tails,
  crash/restart rebuild after DML, and typed WAL payload fuzz/property tests.
- SQLLogicTest runner and documented external-test import policy.
- PostgreSQL regression-derived SQLLogicTest parser/type baseline with public
  provenance and explicit unsupported-catalog skips.
- PostgreSQL regression-derived index/constraint/operator baseline with
  executable index, constraint, and comparison-operator cases plus explicit
  user-defined operator and catalog-sanity skips.
- PostgreSQL regression-derived type-specific baseline for numeric, text,
  date/time, JSONB, and array surfaces with explicit full-breadth skips.
- Isolation-suite baseline covering an UltraSQL-authored `acid.sql`, selected
  Hermitage PostgreSQL scenarios, and explicit relation-level SSI honesty notes.
- Reproducible benchmark scripts and result artifacts for PostgreSQL, DuckDB,
  SQLite, ClickHouse, and local Firebolt Core where available.
- Tagged release workflow for Linux, macOS, and Windows binary archives with
  checksum artifacts.

### Changed

- README benchmark tables are restricted to SQL-surface measurements. Kernel
  microbenchmarks stay internal and are not published as competitor claims.
- Roadmap items now distinguish implemented runtime surfaces from certification
  gaps such as long soak tests, full TPC-H/TPC-C evidence, and production ANN
  restart/recall artifacts.
- TPC-B certification now uses pgbench-shaped transaction batches, unique key
  indexes, and the server Simple Query path executes every semicolon-delimited
  statement in a batch. The certification target remains open.
- TPC-C certification now has a PostgreSQL-wire runner for NewOrder, Payment,
  OrderStatus, Delivery, and StockLevel, plus same-host PostgreSQL 17
  comparison artifacts. Correctness is verified, but the 2x throughput target
  remains open.

### Known gaps

- v1.0 is not declared until correctness, benchmark, security, and operator
  soak gates in `ROADMAP.md` and `docs/release-checklist.md` are satisfied.
- PostgreSQL compatibility is incomplete. See `docs/known-incompatibilities.md`.
- Production packaging beyond release archives, including Docker/Homebrew/deb/rpm
  publication, remains open until artifacts are published by CI.
