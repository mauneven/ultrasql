# Known Incompatibilities

This document lists major PostgreSQL compatibility gaps that are still open or
only partially implemented. `ROADMAP.md` remains the source of truth for exact
release status.

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
- Ordered-set aggregates and multi-argument statistical aggregates such as
  `CORR`, `PERCENTILE_CONT`, and `PERCENTILE_DISC` need a richer aggregate plan
  shape before SQL-surface parity can be claimed.
- PL/pgSQL, stored procedures, trigger semantics, event triggers, and
  PostgreSQL extension loading are not complete.
- Views, materialized views, and partitioning remain roadmap items.

## Security and administration

- Role/privilege persistence is incomplete compared with PostgreSQL; roles and
  ACLs still use in-memory catalogs.
- Broad ecosystem admin-tool introspection remains open.
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

- README benchmark rows must come from SQL-surface scripts and recorded
  artifacts.
- TPC-H/TPC-C/Sysbench/ClickBench gates are open until same-host artifacts prove
  the target thresholds.
- Firebolt comparisons use local Firebolt Core only, not hosted Firebolt URLs.

## Client ecosystem

- PostgreSQL wire protocol support exists, but full certification for psql,
  SQLAlchemy, Django, Rails, Hibernate, Prisma, Diesel, JDBC, Npgsql, and admin
  tools is not complete.
