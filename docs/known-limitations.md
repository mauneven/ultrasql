# Known Limitations

This document lists major SQL and operations gaps that are still open or only
partially implemented. `ROADMAP.md` tracks open release gates; `DONE.md` tracks
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
- Broader aggregate coverage remains open beyond the covered `STDDEV`,
  `VARIANCE`, `CORR`, `PERCENTILE_CONT`, and
  `PERCENTILE_DISC` surfaces, including hypothetical-set aggregates,
  `DISTINCT` ordered-set forms, and additional multi-argument statistical
  functions.
- PL/pgSQL, stored procedures, trigger semantics, event triggers, and
  extension loading are not complete.
- The HNSW vector index is currently single-layer (one navigable graph), not
  the full hierarchical multi-layer HNSW, and its insert path is an exact O(N)
  scan. Recall/latency at large scale will differ from a hierarchical
  implementation. The on-disk page arena is also not yet read back on restart:
  the index graph is rebuilt by replaying the HNSW WAL records, so startup time
  grows with total insert history.
- Regular views support stored `SELECT` expansion, rename, schema moves, and
  restart metadata. Updatable views, `WITH CHECK OPTION`, dependency-safe
  `CREATE OR REPLACE VIEW`, materialized-view refresh/index parity, and general
  `RANGE`/`LIST`/`HASH` partitioning remain roadmap items.

## Security and administration

- Connection authentication is limited to `Trust` (the default — no
  authentication) or a single global MD5 credential. A real TLS handshake
  (`SSLRequest` is declined with `N`, so connections are plaintext only),
  per-role SCRAM-SHA-256 negotiation, and `pg_hba` enforcement are not yet
  wired, though the SCRAM, rustls, and `pg_hba` primitives exist as library
  code. Do not expose the server on an untrusted network.
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
  record a measurement. Separately, the WAL is never truncated — segments grow
  unbounded and restart replays all history — which remains an open durability
  gate (see `ROADMAP.md`, "WAL retention / segment recycling").

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
