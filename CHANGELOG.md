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
- SQLLogicTest runner and documented external-test import policy.
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

### Known gaps

- v1.0 is not declared until correctness, benchmark, security, and operator
  soak gates in `ROADMAP.md` and `docs/release-checklist.md` are satisfied.
- PostgreSQL compatibility is incomplete. See `docs/known-incompatibilities.md`.
- Production packaging beyond release archives, including Docker/Homebrew/deb/rpm
  publication, remains open until artifacts are published by CI.
