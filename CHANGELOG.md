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
- `ALTER TABLE ... ADD CONSTRAINT ... CHECK` and
  `ALTER TABLE ... DROP CONSTRAINT [IF EXISTS] ... [CASCADE|RESTRICT]` so
  migration tools (Flyway, Liquibase, Rails, Django, Alembic, Prisma) can
  evolve constraints. `ADD CHECK` and `ADD UNIQUE`/`PRIMARY KEY` validate
  existing rows before the constraint is installed (`23514` / `23505`), the
  change persists across restart, and `DROP CONSTRAINT` removes the backing
  unique index. `ADD CONSTRAINT ... FOREIGN KEY` via `ALTER TABLE` is not yet
  supported and returns `0A000`; declare foreign keys in `CREATE TABLE`.
- SQL window frames over `OVER (...)`: explicit `ROWS` / `RANGE` / `GROUPS`
  units with frame bounds (`UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`,
  `n FOLLOWING`, `UNBOUNDED FOLLOWING`) and `EXCLUDE NO OTHERS / CURRENT ROW /
  GROUP / TIES`, plus aggregate window functions `SUM` / `AVG` / `COUNT` /
  `MIN` / `MAX(expr)` and `COUNT(*)` over those frames. `RANGE` offsets require
  a numeric `ORDER BY` column; peer groups follow SQL ordering semantics. This
  is in addition to the previously shipped named window functions
  (`row_number`, `rank`, `lag`, `lead`, etc.).
- Large top-level Simple-Query `SELECT` results whose encoded body exceeds the
  streaming high-water mark are now streamed to the socket in bounded memory
  windows instead of being fully buffered, so peak server wire-buffer memory is
  bounded independent of result cardinality and a slow client throttles the
  pull. `statement_timeout` stays armed across the whole windowed drain (a
  mid-stream cancel emits `57014` then `ReadyForQuery`). Streaming is gated to
  the single-statement network path.
- Under buffer-pool pressure the pool can now relieve eviction by flushing
  dirty pages, but only once a page's page-LSN is at or below the WAL's durable
  LSN (the write-ahead-log rule is preserved); when every dirty page is blocked
  it forces the WAL durable to the oldest unflushable dirty LSN to make
  progress. Pinned frames are never evicted or flushed.
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

- `EXPLAIN ANALYZE` on a hybrid search query now reports the executed retrieval
  path: candidates examined/ranked, top-k emitted, per-component score ranges
  (BM25, vector similarity), and a recall estimate on the `HybridSearch`
  operator, plus index/scan choice and per-filter pruning on its child
  operators.
- IVFFlat-indexed filtered vector queries (`WHERE … ORDER BY <vector> LIMIT k`)
  now use a probes-based ANN over-fetch (probes scaled by filter selectivity,
  exact predicate recheck) instead of falling back to an exact scan, and
  `search_with_probes` exposes a per-query probes override (the IVFFlat analog of
  HNSW `ef_search`). `EXPLAIN ANALYZE` now accurately reports the ANN index that
  serves a filtered top-k. Committed recall artifact: recall@10 climbs to 1.0 as
  probes reach the list count.
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
- SQL/JSON path parsing and predicate evaluation are now depth-bounded at 128;
  a deeply nested `jsonpath` expression returns a clean `jsonpath ... nested
  too deeply` error instead of overflowing the stack (DoS).
- The lock-table prune/acquire race is closed with an atomic `remove_if`:
  pruning re-checks the empty predicate under the shard lock, so a concurrent
  acquirer is no longer silently dropped.
- `EXPORT DATABASE` / `IMPORT DATABASE` now require superuser (mirroring
  server-side file `COPY`), closing a file-access escalation where any role
  running the command could read or write server-side files.
- The buffer pool now re-validates page identity after pinning a frame on the
  lock-free hit path and retries on mismatch, closing an eviction TOCTOU where
  a frame concurrently evicted and refilled with a different page could return
  the wrong page bytes.

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
- All file-data fsyncs now route through `F_FULLFSYNC` on macOS, not just the
  WAL writer. Data-page segments, the WAL manifest, recovery truncation,
  runtime metadata, catalog/clog snapshots, replication metadata, and
  `EXPORT` files are all forced to the drive's own write cache (falling back to
  `sync_all` only on `ENOTSUP`/`EOPNOTSUPP`/`EINVAL`). Previously only the WAL
  used `F_FULLFSYNC`; every other barrier used `sync_all`, which does not flush
  the drive cache and could silently lose committed data on power loss.
- Window function calls nested inside value expressions are now lifted and
  evaluated — e.g. `COALESCE(LAG(x) OVER w, 0)`,
  `CASE WHEN ... THEN row_number() OVER w END`, casts, `IN` lists, `BETWEEN`,
  and array/row constructors — instead of erroring `42703 column not found:
  $wn_0`. Window-in-window and aggregate-of-window remain rejected with
  `42P20`.
- The per-relation column cache is now published-to and read-from only from a
  quiescent, writer-visible snapshot (the snapshot's in-progress set is empty
  and the version's last writer is invalid/own/committed). This fixes
  stale/incoherent columnar reads under `REPEATABLE READ`/`SERIALIZABLE` and
  multi-writer concurrency (committed rows hidden, deleted rows resurrected, or
  dirty reads of uncommitted writes).
- Binary-format `timetz` now encodes the zone as seconds *west* of UTC on both
  read and write in the extended protocol and binary `COPY`, matching
  PostgreSQL, so binary `timetz` round-trips correctly with PostgreSQL clients.
- Float columns now advertise text format in `RowDescription` when an
  extended-protocol `Bind` sends an empty result-format list (the libpq/JDBC
  "text for every column" case), matching the `DataRow` encoding; previously
  floats were mis-advertised and clients mis-decoded them.
- `pg_indexes.indexdef` is now populated via `pg_get_indexdef` (a
  PostgreSQL-style `CREATE INDEX` statement) instead of `NULL`, so tools that
  reflect index definitions work.

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
