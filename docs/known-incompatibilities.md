# Known Incompatibilities

This document lists major PostgreSQL compatibility gaps that are still open or
only partially implemented. `ROADMAP.md` tracks open release gates; `DONE.md`
tracks completed evidence.

## Production readiness

- v1.0 is not released until correctness, benchmark, security, and operator
  soak gates are green.
- Three independent 30-day operator soaks are still required.
- Chaos, disk-full, long fuzz, and full benchmark certification remain release
  gates, not README claims.

## SQL and type system

- Some PostgreSQL data types are partial or missing, including full XML, full
  locale/collation behavior, domain/composite type parity, and several network
  address/operator details.
- Transactional DDL is not complete; ORM schema-creation certification runs in
  autocommit mode until DDL inside explicit transaction blocks is implemented.
- Serializable transactions use relation-level SSI, not predicate-precise
  PostgreSQL SSI. The covered Hermitage write-skew case aborts one transaction
  with SQLSTATE `40001`, but full PostgreSQL isolationtester parity remains
  open.
- Broader aggregate parity remains open for PostgreSQL variants beyond the
  covered `STDDEV`, `VARIANCE`, `CORR`, `PERCENTILE_CONT`, and
  `PERCENTILE_DISC` surfaces, including hypothetical-set aggregates,
  `DISTINCT` ordered-set forms, and additional multi-argument statistical
  functions.
- PL/pgSQL, stored procedures, trigger semantics, event triggers, and
  PostgreSQL extension loading are not complete.
- Plain view expansion, updatable views, `WITH CHECK OPTION`, materialized-view
  refresh/index parity, and general `RANGE`/`LIST`/`HASH` partitioning remain
  roadmap items.

## Security and administration

- Role/privilege persistence is incomplete compared with PostgreSQL; roles and
  ACLs still use in-memory catalogs.
- GUI schema-browser introspection query families are certified for pgAdmin,
  DBeaver, and DataGrip, but full desktop UI launch/click smoke, admin-tool
  mutation workflows, and less common admin paths remain open.
- Row-level security covers the documented tenant policy shape with
  owner/bypass/restart semantics but is not yet claimed as full PostgreSQL
  parity.

## Replication and backup

- Physical WAL sender/receiver utilities exist, but continuous networked
  replication, synchronous replication modes, cascading replication, and
  online backup fencing remain open.
- Logical decoding and `pgoutput` compatibility are not complete.
- `pg_dump`/`pg_restore` compatibility is partial and must be validated per
  workload.

## Performance certification

- README performance notes must come from SQL-surface scripts and recorded
  artifacts.
- TPC-H/TPC-C/Sysbench/ClickBench gates are open until same-host artifacts prove
  the target thresholds.
- Firebolt comparisons use local Firebolt Core only, not hosted Firebolt URLs.

## Client ecosystem

- Core PostgreSQL driver certification exists for direct `libpq`, `psycopg2`,
  `psycopg3`, SQLAlchemy, Django ORM, Rails ActiveRecord, `node-postgres`, Go
  `lib/pq`, Go `pgx`, GORM, the JDBC PostgreSQL driver, Hibernate ORM, Npgsql,
  Prisma, Diesel traffic, and stock `psql` meta-commands `\d`, `\dt`, `\di`,
  `\df`, `\dv`, `\du`, `\l`, and `\dn`. GUI introspection probes cover
  pgAdmin, DBeaver, and DataGrip catalog schema-browser query families. Flyway,
  Liquibase, and Alembic certification covers version-table migration runs
  through their public APIs. Flyway uses `executeInTransaction(false)`,
  Liquibase uses nontransactional changesets, and Alembic uses
  `transactional_ddl=False` until transactional DDL lands. Full certification
  for desktop admin tool workflows, every migration CLI flag, and
  driver-specific advanced type adapters is not complete.
