# Known Limitations

This document lists major SQL and operations gaps that are still open or only
partially implemented. `TODO.md` tracks open release gates; `DONE.md` tracks
completed evidence.

## Production readiness

- v1.0 is not released until correctness, benchmark, security, and operator
  soak gates are green.
- The current evidence-backed readiness verdict lives in
  [Production Readiness Audit](production-readiness.md).
- Three independent 30-day operator soaks are still required.
- Chaos, disk-full, long fuzz, and full benchmark certification remain release
  gates, not README claims.

## SQL and type system

- Some SQL data types are partial or missing, including full XML namespace /
  full `XMLTABLE` coverage beyond the first secure projection subset, full
  locale/collation behavior, and domain/composite type breadth.
- Transactional DDL is partial (milestones 1–5). `CREATE TABLE` (including with
  `PRIMARY KEY` / `UNIQUE`), `CREATE INDEX` (plain single/composite B-tree, on an
  existing table), the catalog-only `ALTER TABLE` subset (`RENAME TABLE`/`COLUMN`,
  `ALTER COLUMN SET`/`DROP DEFAULT`, `SET`/`DROP NOT NULL`, `SET (options)`), and
  plain `DROP TABLE` (`RESTRICT`, on a table with no sequence/RLS/view/partition/FK
  side effect) now work inside an explicit `BEGIN…COMMIT` block via a
  per-transaction catalog overlay: the issuing transaction sees the change, other
  sessions do not until `COMMIT`, the durable catalog rows ride the user xid (so
  `ROLLBACK` and crash recovery discard them — MVCC-invisible and hidden by the
  visibility-filtered bootstrap), and a per-name `AccessExclusive` lock serializes
  concurrent same-target DDL. Index btrees are **built at `COMMIT`, not at `CREATE`**
  (deferred build): uniqueness is enforced once at the COMMIT build, a duplicate fails
  the `COMMIT` with SQLSTATE `23505` and a full rollback (no half-committed schema),
  and a `ROLLBACK` or crash before `COMMIT` builds nothing so no index segment leaks;
  a committed index is re-persisted with its real root so it is built (uniqueness
  enforced) after restart. **Multiple schema-changing statements now ACCUMULATE in
  one transaction** — any sequence of in-txn `CREATE TABLE` / `CREATE INDEX`
  appends to the one overlay and commits atomically or rolls back together (a
  duplicate at any deferred build fails the whole `COMMIT` with `23505`; an earlier,
  valid `CREATE TABLE` does not half-commit). `CREATE INDEX` on a table created
  EARLIER in the same transaction is now supported (the index builds over the
  in-txn rows at `COMMIT`). **`DROP TABLE` uses a negative-mask overlay**: the
  table is hidden from the issuing transaction (a later in-txn read is `42P01`)
  while other sessions still see it until `COMMIT`; the global catalog is never
  mutated and no sidecar teardown runs in-txn, so `ROLLBACK` or a crash before
  `COMMIT` fully RESURRECTS the table with its rows and indexes (the drop tombstone
  rides the user xid and is hidden by the visibility-filtered bootstrap), while a
  committed drop is gone everywhere after restart. Still rejected (SQLSTATE
  `0A000` + `HINT`, block then `Failed`/`25P02`) inside a transaction: `FOREIGN KEY`,
  `serial`/`IDENTITY`/`DEFAULT nextval`, `CREATE TABLE AS SELECT`, `TEMP`,
  `PARTITION BY`, expression/partial/INCLUDE or non-B-tree `CREATE INDEX`,
  `CREATE INDEX … CONCURRENTLY`, the heap-rewriting / index-building `ALTER` actions
  (`ADD`/`DROP COLUMN`, `ALTER TYPE`, `ADD`/`DROP CONSTRAINT`, `ENABLE RLS`), `ALTER`
  of a time-partitioned table, `DROP TABLE … CASCADE` and `DROP` of a table that
  owns a sequence / has RLS / has a dependent view or matview / is partitioned (or a
  chunk) / is referenced by or owns a foreign key / has columnar storage, custom
  stats, or comments / is a system table / has a pending in-txn `ALTER`,
  DDL under an active `SAVEPOINT`, and
  `PREPARE TRANSACTION` over an uncommitted in-txn DDL. **All other DDL** (`GRANT`,
  `CREATE ROLE`, the out-of-subset `ALTER`/`DROP` cases above, etc.) is still
  rejected `0A000` inside a transaction; autocommit DDL is unchanged. Later
  milestones broaden `DROP` (CASCADE, sequence/FK/view owners), add the
  heap-rewriting `ALTER` actions, and two-phase commit,
  per [Transactional DDL Design](transactional-ddl-design.md), each behind the
  adversarial battery.
- Latent catalog-bootstrap corruption vector (crash-recovery durability): even
  for an **autocommit** DDL, a crash *between* the statement's catalog rows
  becoming durable and its commit marker becoming durable can resurrect
  uncommitted schema on restart. `bootstrap_from_heap` rebuilds the catalog with
  a raw, non-visibility heap scan that keeps the newest row per OID regardless of
  whether the writing transaction committed, so durably-written-but-uncommitted
  catalog rows reappear as live schema. The fix (a commit-aware, visibility-filtered
  bootstrap) is gated on a recovery re-ordering: the commit log (CLOG) is currently
  rebuilt *after* catalog bootstrap, so a visibility filter against the empty CLOG
  would hide all committed schema. Recovery must rebuild commit status before
  bootstrap and define a bootstrap snapshot first. This is tracked as the
  recommended first step (Increment A) in
  [Transactional DDL Design](transactional-ddl-design.md).
- Serializable transactions use column-range SSI for supported scalar
  comparisons and fully supported `AND` / `OR` predicate trees plus
  relation-level fallback, but not fully predicate-precise SSI. Tight column-range
  predicate locks are taken for `Bool`/`Int16`/`Int32`/`Int64`/`Timestamp`/
  `TimestampTz`/`Date`/`Time` columns, only when the literal is in the same
  i64 unit-class as the column (a cross-type temporal predicate such as a `Date`
  column compared against a `Timestamp` literal — different units, days vs
  microseconds — falls back to a relation-wide lock so it can never miss a
  conflict). Every other type (text/numeric/float/uuid), joins, function
  predicates, and subqueries degrade to a relation-wide lock — correct but coarse,
  the source of spurious `40001` aborts. The covered Hermitage write-skew case
  aborts one transaction with SQLSTATE `40001`, but broader isolation schedules
  (the upstream `src/test/isolation` schedules) and fully predicate-precise
  (PostgreSQL-style physical SIREAD tuple/page) locking remain open.
- Server-side cursors (`DECLARE` / `FETCH` / `CLOSE`) are forward-only and
  `WITHOUT HOLD` only, and the cursor's `SELECT` is **materialized at
  `DECLARE` time** (the full result is buffered in session memory and `FETCH`
  windows over it) rather than executed incrementally, so a cursor over a huge
  result set costs memory proportional to the result, not to the fetch window.
  `DECLARE` requires an explicit transaction block (`25P01` outside one, as in
  PostgreSQL); `COMMIT` / `ROLLBACK` / `PREPARE TRANSACTION` close every open
  cursor. `WITH HOLD`, `SCROLL` (and every backward/absolute `FETCH`
  direction), `MOVE`, and `BINARY` cursors parse but are rejected with
  SQLSTATE `0A000` and a hint. Cursor statements are simple-query-protocol
  surfaces: sending `DECLARE` / `FETCH` through an extended-protocol prepared
  statement is rejected at bind time. Cursors also ignore `ROLLBACK TO
  SAVEPOINT`: PostgreSQL closes a cursor that was opened inside the
  rolled-back savepoint's scope, while UltraSQL keeps it open until the
  transaction ends.
- `SAVEPOINT` / subtransaction *visibility* is implemented for DML: a
  transaction sees its own writes made under an active `SAVEPOINT`, `ROLLBACK
  TO` hides a savepoint's inserts and restores its deletes / in-place-update
  pre-images (verified from a second connection after `COMMIT`), `RELEASE`
  keeps a savepoint's writes without leaking them to other backends before the
  parent commits, and the parent's commit/abort folds the whole subxid family
  atomically. The read snapshot carries per-transaction own-subxid sets (live
  vs rolled-back), every DML write path stamps the active subtransaction id via
  `Transaction::write_xid()` (guarded by a debug stamp assertion), and the
  index access method follows PostgreSQL's lossy-index + heap-recheck model
  (MVCC `DELETE` / key-changing `UPDATE` no longer physically remove B-tree
  leaf entries; the read paths recheck the heap and `VACUUM` reclaims the dead
  leaves), so an index scan and a sequential scan agree on the visible row set
  through arbitrary savepoint nesting. The behaviour is covered by an
  adversarial two-connection battery on both an un-indexed int32-pair shape
  (fused fast paths + column cache) and an indexed multi-column shape, plus a
  crash-recovery replay test.

  Remaining gaps: `COPY` now participates in the enclosing transaction — its rows
  ride the session (sub)transaction xid, so `ROLLBACK` (and a `ROLLBACK TO`
  covering the `COPY`) undo them and `COMMIT` persists them atomically with the
  rest of the block; a mid-stream `COPY` error transitions the block to `Failed`.
  However `COPY FROM` still maintains **no secondary indexes** and enforces only
  `NOT NULL` (not `UNIQUE`/`CHECK`): a `COPY` into a table with a secondary or
  unique index does not update that index, so load such data with `INSERT` or
  rebuild the index after `COPY` (a `PRIMARY KEY`/`UNIQUE` index created in the
  same transaction as the `COPY` is built at `COMMIT` and does see the rows).
  `ALTER
  TABLE` heap-rewrite DDL stamps the parent transaction id rather than the
  active subtransaction id, so exact own-write rollback of a table rewrite
  performed under a savepoint is out of scope; run schema rewrites outside a
  savepoint when exact subtransaction rollback is required. Two-phase commit now
  carries the committed-subxid family end to end: `PREPARE TRANSACTION` captures
  the released/open-at-prepare savepoint subxids before the subtransaction stack
  is dropped and persists them durably in the prepared state file, and
  `COMMIT PREPARED` re-embeds that family in its single Commit WAL record (just
  as single-phase `COMMIT` does), so a row written under a released or still-open
  `SAVEPOINT` inside a two-phase-committed transaction survives crash recovery
  after the prepared commit. (`ROLLBACK PREPARED` carries no family, so those
  savepoint rows correctly stay aborted.)
- Broader aggregate coverage remains open beyond the covered `STDDEV`,
  `VARIANCE`, `CORR`, `PERCENTILE_CONT`, and
  `PERCENTILE_DISC` surfaces, including hypothetical-set aggregates,
  `DISTINCT` ordered-set forms, and additional multi-argument statistical
  functions. Separately, `avg()` over an integer column returns double
  precision (matching the DuckDB/SQLite differential oracle) rather than
  PostgreSQL's `numeric`, so `avg` results are not arbitrary-precision.
- PL/pgSQL, stored procedures, trigger semantics, event triggers, and
  extension loading are not complete.
- There are two HNSW implementations. The production page-backed index
  (`PageBackedHnswIndex`) is hierarchical multi-layer (per-node deterministic
  levels with per-layer neighbor chains and top-down greedy descent), its build
  is sub-quadratic (candidate selection traverses the partially-built graph
  above a calibrated work threshold instead of scanning every live node), and
  its on-disk page arena is captured in a versioned, `crc32c`-checksummed
  snapshot that is read back at restart — only the WAL tail above the snapshot's
  high-water LSN is replayed, with a full WAL replay as the fallback when the
  snapshot fails validation. The separate runtime in-memory index
  (`HnswIndex`), used as a fallback ANN target, is still single-layer with an
  exact O(N) insert scan and is not persisted: it is rebuilt by replaying the
  HNSW WAL records on restart. Recall/latency comparisons and the SIFT1M gate
  remain open release items (see `TODO.md`).
- Regular views support stored `SELECT` expansion, rename, schema moves, and
  restart metadata. Updatable views, `WITH CHECK OPTION`, dependency-safe
  `CREATE OR REPLACE VIEW`, materialized-view refresh/index parity, and general
  `RANGE`/`LIST`/`HASH` partitioning remain roadmap items.
- `ALTER TABLE ... ADD CONSTRAINT` covers `PRIMARY KEY`, `UNIQUE`, and `CHECK`
  (each validated against existing rows at `ADD` time and enforced on later
  DML), and `DROP CONSTRAINT [IF EXISTS] ... [CASCADE|RESTRICT]` removes any of
  them along with the backing unique index. `ADD CONSTRAINT ... FOREIGN KEY`
  and `ADD CONSTRAINT ... EXCLUDE` are not yet supported by `ALTER TABLE` and
  return `0A000` (`feature_not_supported`); declare foreign keys and exclusion
  constraints in `CREATE TABLE` instead, where they are fully enforced. The
  parsed `CASCADE` / `RESTRICT` qualifier on `DROP CONSTRAINT` is accepted but
  treated identically, since a dropped constraint has no dependent objects
  beyond its own backing index, which is always removed.

## Security and administration

- Connection authentication supports `Trust` (the default — no authentication),
  a single global MD5 credential, per-role SCRAM-SHA-256, and `pg_hba`-driven
  policy. An `SSLRequest` is answered with `S` and the stream is upgraded to TLS
  in place when a server certificate is configured (with a buffered-plaintext
  guard against CVE-2021-23214); `pg_hba` rules match `hostssl`/`hostnossl` and
  run SCRAM against each role's own stored verifier. `GSSENCRequest` is still
  declined. Remaining gaps: `md5`/`password` `pg_hba` methods are rejected
  because role credentials are stored only as SCRAM verifiers, and broader
  client-certificate / `pg_ident` mapping flows are not wired. Do not expose the
  server on an untrusted network without a soak-tested deployment.
- Role, privilege, default-privilege, and RLS persistence currently use runtime
  sidecar metadata; typed catalog rows and migrations remain open.
- GUI schema-browser introspection query families are certified for pgAdmin,
  DBeaver, and DataGrip, but full desktop UI launch/click smoke, admin-tool
  mutation workflows, and less common admin paths remain open.
- Row-level security covers the documented tenant policy shape with
  owner/bypass/restart semantics, role-scoped policies, mutation checks, and a
  release certification artifact; broader policy combinations remain open.

## Replication and backup

- Continuous streaming physical replication to a queryable hot standby works
  end to end: base-backup bring-up (`ultrasql --basebackup` + `standby.signal`
  + `primary_conninfo`), auto-connected walreceiver, continuous WAL apply, and
  read-only query serving — proven by a two-node live-apply round trip.
  Remaining gaps: synchronous replication modes (commit does not wait for the
  standby), promotion/failover tooling, cascading replication, standby-side
  WAL recycling (standby WAL grows until restart/promotion; checkpoints and
  autovacuum are intentionally disabled on a standby), authz changes reach a
  standby only via a new base backup (roles/privileges/RLS live in sidecar
  metadata, not WAL), and online backup fencing remains coarse.
- Logical decoding and `pgoutput` are not complete.
- Archive dump/restore is partial and must be validated per workload.

## Performance certification

- README performance notes must come from SQL-surface scripts and recorded
  artifacts.
- Current TPC-H SF1/SF10 certification artifacts pass their recorded
  PostgreSQL/DuckDB targets. TPC-B, TPC-C, Sysbench, and ClickBench release
  gates remain open in the latest committed artifacts.
- The README release-artifact scale sweep is a same-host fastest-table result,
  not full benchmark release certification. Full sign-off still needs the
  full benchmark profile and WAL-backed data-dir scale-sweep evidence.
- Firebolt comparisons use local Firebolt Core only, not hosted Firebolt URLs.
- Durable bulk `INSERT` at 1M rows is still recorded `not_available` in the
  committed scoreboard, but the underlying `wal buffer full` failure is fixed in
  code: per-record backpressure is wired and a single record larger than the
  8 MiB WAL buffer is now admitted over-capacity. The artifact needs a re-run to
  record a measurement. Checkpoint-driven WAL segment recycling is now
  implemented: at each checkpoint the WAL is truncated below a crash-safe floor
  (the minimum of the redo point, the oldest in-progress transaction's first
  written LSN, and each vector-index snapshot LSN) via
  `ultrasql_wal::truncate_below`, and an automatic checkpoint timer drives it, so
  segments no longer grow unbounded and restart no longer replays all history.
  Recycling is skipped when any required snapshot is not durable.

## Client ecosystem

- Driver certification exists for direct `libpq`, `psycopg2`, `psycopg3`,
  SQLAlchemy, Django ORM, Rails ActiveRecord, `node-postgres`, Go `lib/pq`, Go
  `pgx`, GORM, JDBC, Hibernate ORM, Npgsql, Prisma, Diesel traffic, and stock
  `psql` meta-commands `\d`, `\dt`, `\di`, `\df`, `\dv`, `\du`, `\l`, and
  `\dn`. GUI introspection probes cover pgAdmin, DBeaver, and DataGrip catalog
  schema-browser query families. Flyway, Liquibase, and Alembic certification
  covers version-table migration runs through their public APIs. Flyway uses
  `executeInTransaction(false)`, Liquibase uses nontransactional changesets,
  and Alembic uses `transactional_ddl=False` until transactional DDL lands.
  Full certification for desktop admin tool workflows, every migration CLI
  flag, and driver-specific advanced type adapters is not complete.
