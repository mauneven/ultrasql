# Known Limitations

This document lists major SQL and operations gaps that are still open or only
partially implemented. `ROADMAP.md` tracks open release gates; `DONE.md` tracks
completed evidence.

## Production readiness

- v1.0 is not released until correctness, benchmark, security, and operator
  soak gates are green.
- Three independent 30-day operator soaks are still required.
- Chaos, disk-full, long fuzz, and full benchmark certification remain release
  gates, not README claims.

## SQL and type system

- Some SQL data types are partial or missing, including full XML namespace /
  `XMLTABLE` coverage, full locale/collation behavior, domain/composite type
  breadth, and several network address/operator details.
- Transactional DDL is not complete; ORM schema-creation certification runs in
  autocommit mode until DDL inside explicit transaction blocks is implemented.
- Serializable transactions use relation-level SSI, not predicate-precise SSI.
  The covered Hermitage write-skew case aborts one transaction with SQLSTATE
  `40001`, but broader isolation schedules remain open.
- Broader aggregate coverage remains open beyond the covered `STDDEV`,
  `VARIANCE`, `CORR`, `PERCENTILE_CONT`, and
  `PERCENTILE_DISC` surfaces, including hypothetical-set aggregates,
  `DISTINCT` ordered-set forms, and additional multi-argument statistical
  functions.
- PL/pgSQL, stored procedures, trigger semantics, event triggers, and
  extension loading are not complete.
- Plain view expansion, updatable views, `WITH CHECK OPTION`, materialized-view
  refresh/index parity, and general `RANGE`/`LIST`/`HASH` partitioning remain
  roadmap items.

## Security and administration

- Role/privilege persistence is incomplete; roles and ACLs still use in-memory
  catalogs.
- GUI schema-browser introspection query families are certified for pgAdmin,
  DBeaver, and DataGrip, but full desktop UI launch/click smoke, admin-tool
  mutation workflows, and less common admin paths remain open.
- Row-level security covers the documented tenant policy shape with
  owner/bypass/restart semantics; broader policy combinations remain open.

## Replication and backup

- Physical WAL sender/receiver utilities exist, but continuous networked
  replication, synchronous replication modes, cascading replication, and
  online backup fencing remain open.
- Logical decoding and `pgoutput` are not complete.
- Archive dump/restore is partial and must be validated per workload.

## Performance certification

- README performance notes must come from SQL-surface scripts and recorded
  artifacts.
- TPC-H/TPC-C/Sysbench/ClickBench gates are open until same-host artifacts prove
  the target thresholds.
- Firebolt comparisons use local Firebolt Core only, not hosted Firebolt URLs.

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
