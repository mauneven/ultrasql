# Changelog

All notable UltraSQL user-visible changes are tracked here. UltraSQL follows
semantic versioning after v1.0; pre-1.0 releases may still break public APIs
and must document the break here.

## Unreleased

### Added

- SQL server, CLI, and local runner binaries.
- MVCC heap storage, WAL, crash-recovery primitives, B-tree/BRIN/GiST/GIN
  foundations, COPY, backup/restore utilities, and catalog version guard.
- SQL coverage for common DDL/DML/SELECT paths, transactions, savepoints,
  JSONB operators, range/geometric types, vector types, HNSW/IVFFlat indexes,
  RAG helper functions, external scans, and lakehouse-oriented CSV/Parquet
  surfaces.
- Exact vector top-k fallback now uses bounded `TopK` instead of physical
  Sort and exposes exact kernel/fallback state in `EXPLAIN ANALYZE`.
- Same-host AI/vector certification runner now requires measured UltraSQL and
  pgvector exact top-k artifacts before passing.
- Same-host TPC-H SF1 PostgreSQL 17 certification now runs complete q1..q22
  raw artifacts and records the geometric-mean pass/fail decision.
- HNSW/IVFFlat durability certification covers torn ANN WAL tails,
  crash/restart rebuild after DML, and typed WAL payload fuzz/property tests.
- SQLLogicTest runner and documented external-test import policy.
- Public regression-derived SQLLogicTest parser/type baseline with public
  provenance and explicit unsupported-catalog skips.
- Public regression-derived index/constraint/operator baseline with
  executable index, constraint, and comparison-operator cases plus explicit
  user-defined operator and catalog-sanity skips.
- Public regression-derived type-specific baseline for numeric, text,
  date/time, JSONB, and array surfaces with explicit full-breadth skips.
- Isolation-suite baseline covering an UltraSQL-authored `acid.sql`, selected
  Hermitage scenarios, and explicit relation-level SSI honesty notes.
- Reproducible benchmark scripts and result artifacts for PostgreSQL, DuckDB,
  SQLite, ClickHouse, and local Firebolt Core where available.
- Same-host sysbench-style OLTP read/write certification runner with
  PostgreSQL 17 comparison artifacts and non-certifying UltraSQL smoke mode.
- Chaos recovery harness for random kill, WAL truncation, and safe disk-full
  recovery evidence.
- Packaging surface for docs site publication, GHCR Docker images, Homebrew
  formula rendering, and Debian/RPM packages.
- Final release evidence gates for 30-day operator reports, strict release
  workflow validation, and rendered GitHub release notes.
- Tagged release workflow for Linux, macOS, and Windows binary archives with
  checksum artifacts.
- npm package metadata now includes keywords, supported OS/CPU constraints, and
  a richer package README with Node.js `pg` usage, supported targets, and
  checksum behavior.
- Release workflow emits a source tarball for Homebrew and renders a
  source-built formula.

- Page-backed HNSW is now hierarchical (multi-layer), the standard HNSW
  structure: per-node deterministic levels + per-layer neighbor chains (v2 page
  format; v1 snapshots load as base-only), greedy top-down descent, and a
  canonical layer beam. At 100k×64d this roughly doubles recall@10 at every ef
  and reaches a given recall at ~3× lower ef than the prior single-layer graph
  (much cheaper queries for the same recall), with ~equal build time at 100k. The
  build stays deterministic (WAL-replay reconstructs an identical graph) and
  crash/recovery-tested. A SIFT1M structured-data artifact is still pending for a
  published 1M-scale claim.
- Page-backed HNSW index build is now sub-quadratic: large indexes gather a new
  node's neighbors by traversing the partially-built graph instead of scanning
  every live node, and a `node_id`-indexed in-memory graph mirror gives O(1)
  per-node access for both build and search (durable pages stay authoritative;
  snapshot/WAL format unchanged). Measured ~10× faster at 50k×128d (≈424 s→41 s),
  3.6× at 10k, with even small indexes faster and no regression; build stays
  deterministic and recall holds (recall@10 ≥ 0.95 at ef ≤ 128). SIFT1M-scale
  builds are now feasible; the pgvector-competitive "minutes at 1M" target folds
  into the hierarchical-layers work (see ROADMAP). The mirror duplicates vectors
  in RAM (~2× vector memory).
- README benchmark tables are restricted to SQL-surface measurements. Kernel
  microbenchmarks stay internal and are not published as DB-vs-DB claims.
- Roadmap items now distinguish implemented runtime surfaces from certification
  gaps such as long soak tests, full TPC-H/TPC-C evidence, and production ANN
  restart/recall artifacts.
- TPC-B certification now uses pgbench-shaped transaction batches, unique key
  indexes, and the server Simple Query path executes every semicolon-delimited
  statement in a batch. The certification target remains open.
- TPC-C certification now has a wire-protocol runner for NewOrder, Payment,
  OrderStatus, Delivery, and StockLevel, plus same-host PostgreSQL 17
  comparison artifacts. Correctness is verified, but throughput leadership
  remains open.
- Sysbench OLTP certification now reports correctness for both engines and an
  honest target failure rather than publishing the older UltraSQL-only latency
  smoke as a DB-vs-DB result.
- ClickBench certification now has a local ClickHouse runner path, concrete
  setup-failure reasons, and explicit UltraSQL/PostgreSQL target-ratio fields.
- Firebolt sparse primary-index pruning now treats the manifest as the
  certification gate, requiring both measured engines, Firebolt pruning
  evidence, and `target_ratio_ultrasql_vs_firebolt <= 1.0`.
- Homebrew packaging builds from source with Cargo instead of installing macOS
  binary archives, keeping the tap closer to `homebrew/core` expectations.
- Roadmap now tracks only open production gates; completed milestones moved to
  `DONE.md`.

### Security

- Parser statement-level recursion (nested `FROM (SELECT …)` subqueries and
  parenthesised joins) is now bounded by `MAX_PARSE_DEPTH`. Previously a
  multi-KB query of a few hundred nesting levels overflowed the worker stack
  and aborted the process — an uncatchable, pre-auth remote DoS under the
  default `Trust` policy. It now returns a recoverable `DepthExceeded` error.
- `COPY … FROM STDIN WITH (FORMAT binary)` now caps the cumulative stream at
  128 MiB (env-tunable via `ULTRASQL_COPY_BINARY_FILE_LIMIT_BYTES`), closing an
  unbounded-buffer OOM DoS where a client could stream `CopyData` frames until
  the process was killed.
- The fast-DML precheck cache now keys on `Arc` pointer identity instead of a
  bare heap address, closing an ABA-style false positive that could skip
  row-level-security / column-privilege checks for the wrong plan.

### Fixed

- B-tree deletion of a non-unique key whose duplicate group spans a same-key
  leaf split no longer silently fails for entries on the left side of the
  split. Previously `DELETE`/`UPDATE` index maintenance on a low-cardinality
  column (>32 rows sharing a key) could leave a dangling index entry —
  index/heap disagreement (phantom rows, false unique violations).
- Window functions now honor `ORDER BY … DESC` and `NULLS FIRST/LAST`.
  Previously `RANK()`/`ROW_NUMBER()`/`LAG`/`LEAD`/etc. `OVER (ORDER BY x DESC)`
  silently computed ascending results.
- Equality selectivity now decodes PostgreSQL's negative `n_distinct`
  convention, so high-cardinality columns are no longer costed as if `col = X`
  returned the whole table (which was poisoning join-order selection).
- SSI now garbage-collects committed conflict-graph entries
  (`commit_horizon` + `collect_garbage`), bounding memory and removing a source
  of spurious `40001` serialization aborts.
- `pg_index.indisprimary` now comes from an authoritative index-creation flag
  instead of the `*_pkey` name heuristic, so a user index named `*_pkey` is no
  longer mislabeled primary.
- Default `psql`/`libpq` clients (`sslmode=prefer`) now connect: the server
  answers an `SSLRequest`/`GSSENCRequest` with the mandatory `N` decline
  (plaintext fallback) instead of dropping the socket.
- B-tree root splits keep the persisted root block stable and reopened B-tree
  handles seed allocation above resident pages, preserving indexed point
  lookups after sysbench-style indexed DML churn.
- `ultrasql validate` no longer mis-decodes internal `pg_catalog` heap rows as
  SQL user rows during heap-visibility checks.

### Changed

- `^` (exponentiation) is now left-associative and boolean `NOT` binds looser
  than the comparison band, matching PostgreSQL (`2 ^ 3 ^ 2` = 64;
  `NOT a = b` parses as `NOT (a = b)`).
- The crate-level clippy gate
  `deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)` (under
  `cfg(not(test))`) is now enforced in every library crate, including
  `ultrasql-executor` and `ultrasql-server`, with no escape hatches.

### Known gaps

- v1.0 is not declared until correctness, benchmark, security, and operator
  soak gates in `ROADMAP.md` and `docs/release-checklist.md` are satisfied.
- SQL surface and operations coverage remain incomplete. See
  `docs/known-limitations.md`.
- Production packaging beyond release archives, including Docker/Homebrew/deb/rpm
  publication, remains open until artifacts are published by CI.
