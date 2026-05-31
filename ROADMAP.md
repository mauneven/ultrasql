# UltraSQL Roadmap

Living file for open gates only. Completed/addressed work lives in
[DONE.md](DONE.md). When a gate closes, move the evidence there and keep this
file focused on what still blocks production.

## P0 - Release Blockers

### CI, Coverage, Release Evidence

- Keep `main` green before and after every slice: format, clippy, workspace
  tests, docs, cargo-audit, cargo-deny, driver certification, release build.
- Final release needs `operator soak reports`, `latest green CI workflow run id`,
  `release workflow run id`, `GitHub release notes`, and
  `operator_soak_status.json` recorded in the release checklist.
- Release candidate must rerun benchmark, fuzz, sanitizer, Miri, package, and
  smoke-install gates on the tagged commit.

### Operator Soak And Safety

- Three independent `operator-reports/*.json` files must pass strict validation
  for 30 continuous days through `.github/workflows/operator-soak.yml`.
- Zero open critical or high-severity correctness bugs.
- Fuzz testing: one clean week for parser, protocol, WAL decoder, and planner.
- Re-run chaos recovery on release candidates; completed random-kill,
  WAL-truncation, and disk-full runner evidence lives in `DONE.md`.

### Correctness Debt

- Serializable isolation remains `relation-level SSI`, `not predicate-precise`
  SSI. Implement predicate-precise SSI before broad serializable claims.
- Full public regression import still open for parser, type coercion,
  aggregate/window, and upstream isolation schedules.

### Performance Certification

- TPC-B: correctness verified, throughput leads PostgreSQL 17, p99 < 5 ms at
  32 connections.
- TPC-C: all five transaction types correct and throughput leads PostgreSQL 17.
- Sysbench OLTP read/write: full same-host PostgreSQL 17 certification remains
  open. Latest UltraSQL-only smoke passes, but is non-certifying until a
  `POSTGRES_DSN` artifact refreshes the comparison.
- ClickBench: dataset-backed same-host PostgreSQL 17 comparison, plus
  ClickHouse and Firebolt legs when available.
- Firebolt sparse primary-index pruning: pass
  `target_ratio_ultrasql_vs_firebolt <= 1.0`, require
  `Firebolt primary-index pruning evidence`, and keep the current local
  Firebolt Core state honest: `local Firebolt Core smoke measured`, but
  `Firebolt is not_available` when Core EXPLAIN does not expose pruning.
- Rerun TPC-H SF1/SF10 on the release host before publishing release
  performance claims; completed local evidence lives in `DONE.md`.
- Broaden release-artifact scale-sweep coverage beyond the current same-host
  table: larger row counts, WAL-backed `--data-dir` mode, repeatable
  ClickHouse artifacts, and Firebolt legs when local services are available.
- AI/vector competitor claims require same-host DuckDB, ClickHouse, and
  PostgreSQL+pgvector artifacts for exact top-k, ANN recall/latency, hybrid
  search, JSON-filtered retrieval, and RAG quality, with answer or recall gates
  before any README row is published.

## P1 - SQL Surface

### Type Surface

- `NUMERIC(p,s)` / `DECIMAL(p,s)`: arbitrary-precision runtime arithmetic and
  variable-scale bare-`NUMERIC` columnar result preservation remain open.
- `MONEY`: locale-sensitive formatting/input beyond deterministic
  `lc_monetary` GUC round trips, explicit typmod/precision edge casts, and
  range-parity evidence remain open.
- Date/time remaining: named time-zone database support plus date/time display
  changes for non-ISO `DateStyle` and locale variants beyond current
  `DateStyle` GUC validation / round trips.
- Arrays: broader coercion breadth and every supported element family.
- JSON/JSONB: full SQL/JSON path parity beyond the supported subset, including
  remaining `datetime(template)` grammar and non-ISO date/time coercions.
- Full-text search remaining: native lexeme/query storage beyond the current
  text-backed representation, headline, dictionaries, full ranking parity, and
  GIN planner integration.
- XML remaining surface: namespace URI mapping, remaining XPath axes/functions,
  `XMLTABLE`, and function catalog parity beyond the supported secure subset.
- Locale/collation remaining: ICU-backed collations, `CREATE COLLATION`, and
  `COLLATE` in definitions and `ORDER BY` beyond the base
  `pg_collation` catalog rows.

### Catalog, Roles, Privileges

- Replace role, membership, privilege, default-privilege, and RLS runtime
  sidecars with typed catalog rows and migrations before v1.0; restart
  persistence evidence for the current sidecars lives in `DONE.md`.
- Broaden remaining dependency tracking for every object kind and every
  `DROP ... CASCADE` / `RESTRICT` path.
- Keep security gates ethical: no proprietary tests, no closed-source
  code, no fake benchmark claims.

### Drivers, ORMs, Tools

- Keep certification green for `libpq`, `psql meta-commands`, `psycopg2`,
  `psycopg3`, `SQLAlchemy`, `Django ORM`, `Rails ActiveRecord`, `Hibernate ORM`,
  `GORM`, `Prisma`, `Diesel`, `node-postgres`, `pgx`, `lib/pq`,
  `JDBC`, and `Npgsql`.
- Keep stock psql meta-command coverage green: `\d`, `\dt`, `\di`, `\df`,
  `\dv`, `\du`, `\l`, `\dn`.
- GUI probes exist for `GUI introspection probes`, `pgAdmin`, `DBeaver`, and
  `DataGrip`; desktop launch/click smoke remains open.
- Migration tools must stay green: `Flyway`, `Liquibase`, and `Alembic`.

## P2 - Vector, Analytics, Lakehouse

### ANN And pgvector

- Production ANN certification: `Page-backed HNSW` and `Page-backed IVFFlat`
  need `large-scale recovery certification`, page-level torn-write handling,
  deeper VACUUM/rebuild stress, `CREATE INDEX CONCURRENTLY`, filtered-query
  fallback policy, and `larger recall/latency artifacts`.
- Keep ANN WAL coverage expanding: crash/restart DML rebuild, corrupt-WAL
  unavailable fallback, and `WAL replay fuzz/property tests`.
- pgvector parity: larger exact top-k profiles, filtered exact search,
  SQL-level HNSW/restart correctness, IVFFlat recall/latency, vector
  arithmetic, aggregates such as `avg(vector)`, and broader cast/function cert.

### AI Gauntlet

- AI memory engine: implement the strategy in
  `docs/ai-database-strategy.md` as a SQL table surface that combines MVCC
  state, columnar shadow scans, BM25, vector ANN, JSON metadata, tenant/time
  projections, and auditable `EXPLAIN ANALYZE` path evidence.
- Keep `AI gauntlet measured artifacts` expanding across: `exact top-k`,
  `HNSW ANN recall/latency`, `hybrid search latency`,
  `filtered vector search`, `RAG retrieval quality`,
  `memory per million vectors`, `ingestion throughput`, and
  `cold-start index load`.
- Broaden the completed UltraSQL AI gauntlet into competitor comparisons:
  DuckDB VSS/HNSW, ClickHouse vector similarity/HNSW, PostgreSQL+pgvector
  HNSW/IVFFlat, Firebolt Core vector search, and optional local vector-service
  adapters when reproducible local setup exists.
- Scale AI artifacts beyond the current full profile: larger rows, larger
  dimensions, more probes, metadata-filtered recall/latency, SQL-level HNSW
  restart correctness, and publishable p50/p95/p99 tables.
- For every new published AI/vector claim, commit dataset or fetch instructions,
  host descriptors, raw artifacts, answer checksums, recall target, failure
  reasons, and same-host competitor configuration.

### Columnar And External Data

- Broaden `Columnar scan path` certification beyond the completed contract that
  heap rows remain the OLTP/MVCC source of truth while `HeapAccess::column_cache`
  provides the OLAP shadow path with committed DML invalidation.
- CSV scans: remove row-buffer storage in the streaming wrapper and certify
  larger cross-engine runs.
- Parquet/object-store scans: certify predicate/projection pushdown, object
  range reads, and lakehouse workloads against external engines.
- Iceberg: deletes, time travel, catalog integration, and certification.
- Arrow: Flight endpoint and wider type coverage.

## P3 - Packaging, Distribution, Operations

- Promote packages from the `release workflow`: `docs.ultrasql.org`,
  `ghcr.io/mauneven/ultrasql`, `clean GHCR platform list`, `packages/npm`,
  `npm publish`, `Windows setup EXE`, `Chocolatey`, `AUR`,
  `yay -S ultrasql-bin`, `Homebrew tap`, `Homebrew`, `Debian`, and `RPM`.
- Homebrew core path: use the source-built formula as the base for upstream
  review once UltraSQL is stable/notable enough for plain `brew install
  ultrasql` without tapping `mauneven/tap`.
- Add or verify required secrets: `NPM_TOKEN`, `HOMEBREW_TAP_TOKEN`,
  `AUR_SSH_PRIVATE_KEY`, `CHOCOLATEY_API_KEY`, package signing, and Windows
  code-signing material.
- Harden VACUUM/autovacuum scheduling, freeze policy, deeper bloat metrics,
  and production docs; completed maintenance counters and split
  vacuum/analyze trigger evidence are tracked in `DONE.md`.
- Finish streaming replication, synchronous replication modes, backup/PITR
  restore drills, logical replication, and `pgoutput`.
- Broaden remaining `pg_stat_*` operator views, lock/io wait-event population,
  deeper lock/query timing precision, and production dashboards; completed
  activity lifecycle states, client-read waits, and xact/query start timing are
  tracked in `DONE.md`.

## P4 - Later SQL Surface

- Views and materialized views: expansion, updatable views, `WITH CHECK OPTION`,
  refresh, and materialized-view indexes.
- PL/pgSQL and procedures: variables, control flow, dynamic SQL, exceptions,
  cursors, `%TYPE`, `%ROWTYPE`, `SETOF`, OUT/INOUT, and `CALL`.
- Triggers: row/statement triggers, `INSTEAD OF`, `NEW`/`OLD`, `WHEN`,
  constraint triggers, and ordering.
- Table partitioning: RANGE/LIST/HASH, attach/detach, pruning, partition-wise
  joins/aggregates, insert routing, cross-partition updates, `Append`, and
  `MergeAppend`.
- Remaining indexes: SP-GiST, bloom filters, and complete GIN/GiST opclasses.
- Hardware performance: x86 AVX-512 CI/bench certification.
- Distributed/lakehouse future: coordinator, Raft catalog, sharding, distributed
  query execution, Arrow Flight, result cache, FDW API, extension loading,
  background workers, event triggers, `pg_stat_statements`, and trigram search.

## Roadmap Hygiene

- New entries need a measurable exit condition, not intent prose.
- Closed entries move to `DONE.md` with artifact paths, commands, or tests.
- No benchmark claim lands unless it is reproducible from committed scripts and
  raw artifacts.
