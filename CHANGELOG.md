# Changelog

All notable UltraSQL user-visible changes are tracked here. UltraSQL follows
semantic versioning after v1.0; pre-1.0 releases may still break public APIs
and must document the break here.

## Unreleased

### Integrity (2026-07-01 truthfulness pass)

- **Removed the TPC-H answer-cache fast paths and withdrew every TPC-H
  claim.** The server contained 21 per-query TPC-H shape-matching pipelines
  that served answers precomputed by the benchmark loader; published
  "certifications" (SF1 "351x vs PostgreSQL 17", SF10 "passed") timed cache
  replay against engines executing real queries. The pipelines, loader
  sidecars, and invalid artifacts are gone; TPC-H now runs through the real
  executor and has no published result until fresh runs land.
- **Result-cache replay is now a disclosed, switchable feature.**
  `ULTRASQL_RESULT_CACHE=off` disables the MVCC-version-gated scan/aggregate
  result-replay fast paths; the release scale sweep runs with replay
  disabled so scoreboard rows compare real compute (BENCHMARKS.md "Result
  caches").
- **Benchmark harness fairness fixes**: DuckDB/SQLite INSERT rows now run on
  persistent in-process driver connections instead of timing a CLI process
  per sample; warmup counts are symmetric across engines; mixed OLTP is one
  autocommitted operation per round trip for every engine (UltraSQL's
  20-op wire batching and PostgreSQL's one-transaction-per-window shapes are
  both gone); PostgreSQL's timed regions no longer include BEGIN/ROLLBACK
  round trips or table-reset work, and aborted-version bloat is vacuumed
  between samples. Scoreboard numbers published before these fixes are
  withdrawn pending a fresh sweep.
- The docs CI gate no longer fails when the scale sweep contains rows where
  UltraSQL is not the fastest engine — losses are reported as data. It also
  audits git-tracked docs only.
- Removed `DONE.md`, `GOVERNANCE.md`, and `RFC_PROCESS.md` (stale claims and
  process fiction for a single-maintainer project).

### Added

- Structured wire `ErrorResponse` fields: every server error now carries the
  non-localized severity (`V`) alongside `S`/`C`/`M`, in PostgreSQL's canonical
  field order, and advice that used to be jammed into the message text as a
  `\nHINT:` line (most visibly the `0A000` transactional-DDL rejection) now
  travels in the dedicated `H` (hint) field. psql renders it as its own
  `HINT:` line and drivers surface it via their `hint()` accessors; the
  primary message (`M`) contains only the primary text. The encoder also
  supports an optional `D` (detail) field; no details or hints are invented
  for errors that never had them.
- Server-side cursors: `DECLARE name [BINARY] [[NO] SCROLL] CURSOR
  [{WITH|WITHOUT} HOLD] FOR select`, `FETCH [count | NEXT | FORWARD count |
  ALL] [FROM|IN] name`, and `CLOSE {name | ALL}` over the simple-query
  protocol, inside an explicit transaction block. Cursors are forward-only and
  `WITHOUT HOLD`; the `SELECT` is materialized at `DECLARE` time and `FETCH`
  windows over the buffered rows with the SELECT's row description and a
  `FETCH n` command tag. `DECLARE` outside a transaction block is rejected
  with SQLSTATE `25P01`; a duplicate `DECLARE` is `42P03`
  (`duplicate_cursor`); `FETCH`/`CLOSE` on an unknown cursor is `34000`
  (`invalid_cursor_name`); `COMMIT`/`ROLLBACK`/`PREPARE TRANSACTION` close all
  cursors. `WITH HOLD`, `SCROLL`, backward/absolute fetch directions, `MOVE`,
  and `BINARY` cursors parse but are rejected with `0A000` plus a hint (see
  `docs/known-limitations.md` for the materialization trade-off).
- Runtime-settable statement-logging GUCs: `log_statement`
  (`none` | `ddl` | `mod` | `all`) and `log_min_duration_statement`
  (milliseconds; `-1` disables, `0` logs everything) now support per-session
  `SET` / `SHOW` / `RESET`, mirroring how `statement_timeout` inherits its
  server default (`RESET` restores the server-config value, not the built-in
  default). The statement-log call site consults the session-effective
  values, and `pg_settings` reports the session value with PostgreSQL's
  `superuser` context. Both remain configurable at startup via the existing
  CLI flags.
- `lock_timeout` session GUC (`SET` / `SHOW` / `RESET`, milliseconds, default
  `0` = disabled, PostgreSQL-faithful): a statement blocked waiting on a heap
  row lock (`SELECT ... FOR UPDATE/SHARE`, `UPDATE`, `DELETE`, `MERGE`) or a
  blocking advisory lock now fails with SQLSTATE `55P03`
  ("canceling statement due to lock timeout") once the timeout elapses,
  instead of waiting indefinitely. Blocking lock waits are now
  deadline-aware end to end: a `statement_timeout` deadline or client
  `CancelRequest` also interrupts a lock wait (SQLSTATE `57014`), and a
  timed-out / cancelled waiter is always removed from the lock manager's
  wait queue (no waiter leak). Additionally, a session that disconnects with
  an explicit transaction still open now has that transaction aborted at
  teardown, so its row locks release instead of blocking peers forever.
- Process-wide memory-admission ceiling: `ultrasqld --memory-ceiling-bytes`
  (env `ULTRASQL_MEMORY_CEILING_BYTES`, default `0` = auto = 75% of physical
  RAM detected at startup) caps the *aggregate* of per-statement `work_mem`
  budgets. Each statement's effective budget is now
  `min(session work_mem, ceiling / live connections)` (floored at 64 KiB),
  so many concurrent sessions can no longer multiply `work_mem` into an
  unbounded heap — over-budget statements engage the existing disk-spill
  paths instead. `SHOW effective_work_mem` reports the admitted budget. The
  divisor is live connections (not executing statements), a deliberately
  coarse first cut.
- Read-only transactions: `BEGIN` and `SET TRANSACTION` now parse and honor
  `READ ONLY` / `READ WRITE` (and accept `[NOT] DEFERRABLE`, currently inert).
  A data-modifying statement (`INSERT` / `UPDATE` / `DELETE` / `MERGE` /
  `COPY … FROM`) inside a read-only transaction is rejected with SQLSTATE
  `25006` (`read_only_sql_transaction`) and aborts the block; `SELECT` and
  sequence advancement (`nextval`) remain allowed, matching PostgreSQL.
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
  into the hierarchical-layers work (see TODO.md). The mirror duplicates vectors
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

- A restart no longer refuses to boot (`unknown RLS table metadata owner`)
  when an object owner was recorded by a trust-authenticated username that
  has no catalog role. Taking ownership (or granting) now durably registers
  the name as an implicit non-privileged login role in `pg_auth.meta`, and
  the boot-time metadata loaders recover an unknown recorded role by
  registering it with a warning instead of failing — data directories
  already affected by the old behavior boot cleanly and are healed.
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
- Queryable hot standby with continuous streaming: a standby brought up from
  `ultrasql --basebackup` output (now created `0700` so it boots directly)
  with a `standby.signal` and a `primary_conninfo` (flag/env/file; optional
  `slot=` uses a physical replication slot) auto-connects a walreceiver,
  streams physical WAL, applies it continuously, and serves read-only queries
  that observe the primary's post-backup commits — proven by a two-node
  live-apply round trip over the wire. Standby-local checkpoints/autovacuum
  are disabled so the standby never appends WAL that would diverge from the
  primary's stream.
- New sessions default to a 30-second `statement_timeout`
  (`ultrasqld --statement-timeout-ms` / `ULTRASQL_STATEMENT_TIMEOUT_MS`; `0`
  disables) so one runaway query cannot occupy a connection forever on a
  shared beta deployment. Any session may `SET statement_timeout` to any
  value including `0`; `RESET statement_timeout` now restores the server-wide
  default (PostgreSQL semantics) instead of hard-coding `0`. The timeout is
  enforced by a deadline carried in the per-query cancel flag — no
  per-statement timer thread — so arming it costs one clock read plus one
  atomic store per statement.
- All file-data durability barriers now route through one configurable
  primitive (`durability_sync`): data-page segments, the WAL writer/manifest,
  recovery truncation, runtime metadata, catalog/clog snapshots, replication
  metadata, 2PC prepared-state files, and `EXPORT` files all issue the
  configured `--wal-sync-method` (env `ULTRASQL_WAL_SYNC_METHOD`), mirroring
  PostgreSQL's `wal_sync_method`. The default is `fsync` — `fsync(2)` issued
  directly, the durability class PostgreSQL (`fsync = on` with its default
  `wal_sync_method`) and SQLite (`synchronous=FULL`, default `fullfsync` off)
  provide on every platform. `fsync_writethrough` additionally forces the
  drive's own write cache to stable media (`fcntl(F_FULLFSYNC)` on macOS,
  falling back to `sync_all` only on `ENOTSUP`/`EOPNOTSUPP`/`EINVAL`) for
  power-loss durability on drives with volatile caches, at a substantial
  per-commit cost (~60x on Apple SSDs). This changes the default macOS
  behavior from always-`F_FULLFSYNC` to PostgreSQL's default posture there;
  Linux behavior is unchanged (`fsync(2)` already forces the device cache
  through the block layer). See `docs/configuration.md` for the full
  per-platform power-loss semantics.
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
  soak gates in `TODO.md` and `docs/release-checklist.md` are satisfied.
- SQL surface and operations coverage remain incomplete. See
  `docs/known-limitations.md`.
- Production packaging beyond release archives, including Docker/Homebrew/deb/rpm
  publication, remains open until artifacts are published by CI.
