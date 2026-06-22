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
- Transactional DDL is not complete; ORM schema-creation certification runs in
  autocommit mode until DDL inside explicit transaction blocks is implemented.
- Serializable transactions use column-range SSI for supported scalar
  comparisons and fully supported `AND` / `OR` predicate trees plus
  relation-level fallback, but not fully predicate-precise SSI. The covered
  Hermitage write-skew case aborts one transaction with SQLSTATE `40001`, but
  broader isolation schedules remain open.
- `SAVEPOINT` / subtransactions are parsed and the savepoint stack is
  maintained, but subtransaction *visibility* is incomplete: a transaction does
  not see its own writes made under an active `SAVEPOINT` (such rows are stamped
  with the subtransaction id, which the read snapshot does not yet treat as
  current). A first attempt at full subtransaction visibility was reverted after
  an adversarial review found it introduced data-corruption and B-tree
  incoherence on `ROLLBACK TO` across the fast/fused write paths. Correct
  support requires subxid stamping on every write fast-path (insert/update/
  delete, including the fused and COPY paths) plus per-subxid index undo so
  `ROLLBACK TO` restores physically-removed index entries; this is tracked as
  dedicated work.
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

- Physical WAL sender/receiver utilities exist, but continuous networked
  replication, synchronous replication modes, cascading replication, and
  online backup fencing remain open.
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
