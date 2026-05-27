# DONE

Completed/addressed work moved out of [ROADMAP.md](ROADMAP.md). Keep this file
as a concise evidence ledger; roadmap stays for open gates only.

## Release And Packaging Automation

- Release workflow builds and attaches release archives for Linux, macOS, and
  Windows, including normal `Windows setup EXE` installer assets.
- Docker image path publishes `ghcr.io/mauneven/ultrasql:<tag>` and keeps GHCR
  attestations disabled so package UI shows a `clean GHCR platform list`.
- npm / pnpm installer support exists under `packages/npm`; package metadata uses
  the clean `ultrasql` name and supports `npm publish --access public
  --provenance` when credentials are configured.
- Homebrew support exists through a source-built `packaging/homebrew/ultrasql.rb.in`
  formula and `scripts/render-homebrew-formula.sh`.
- AUR support exists through `packaging/aur/PKGBUILD.in`,
  `packaging/aur/.SRCINFO.in`, and `scripts/render-aur-package.sh`, rendering
  `ultrasql-bin` for `yay -S ultrasql-bin`.
- Chocolatey support exists through `packaging/chocolatey/` and checksum-pinned
  `.nupkg` generation.
- Debian and RPM support exists through `packaging/nfpm.yaml.in` plus hardened
  systemd packaging files.
- Docs site path exists for `docs.ultrasql.org`; `docs/CNAME` and
  `.github/workflows/docs.yml` build with `mkdocs build --strict`.
- Release notes automation exists: `.github/workflows/release.yml`,
  `docs/release-notes-template.md`, `.github/release.yml`, and
  `scripts/render-release-notes.sh`.

## Baseline, CI, Security

- Baseline audit covered CI, coverage, benchmark, release, and roadmap drift.
- Statement timeout plus cancel propagation for long queries landed and is
  tested.
- Idle session slow-loris timeout is configurable and tested.
- Data-dir ownership and mmap-aliasing threat model fixes landed.
- `cargo audit` and `cargo deny` CI gates are wired.
- Coverage workflow proof exists; per-crate 80% enforcement remains open until
  every crate clears the threshold.
- Driver-certification CI was repaired, action runtimes refreshed, and release
  workflows validated on `main`.
- Chaos testing: random kill, WAL truncation, disk full recovery is implemented
  through `benchmarks/chaos_recovery.sh` and writes
  `benchmarks/results/latest/chaos_recovery_manifest.json`.
- Backup/restore smoke runner covers `ultrasql --basebackup`,
  `ultrasql --pg-dump`, `ultrasql --pg-restore`, row counts, and indexed lookup.
- Catalog upgrade story is documented and enforced with `catalog.version = 1`.
- Security/ethics audit docs cover no proprietary tests, no copied closed-source
  code, and no fake benchmark claims.

## Core SQL And Wire Protocol

- Simple Query and Extended Query dispatch are wired for parse, bind, describe,
  execute, sync, close, flush, and prepared-statement round trips.
- Explicit transactions work through Simple and Extended Query:
  `BEGIN`, `COMMIT`, `ROLLBACK`, failed-block SQLSTATE `25P02`, and PostgreSQL
  `ReadyForQuery` status bytes.
- `ORDER BY`, joins, set operations, `BETWEEN`, index scans, transaction blocks,
  plan cache, and optimizer routing are wired through server execution.
- SCRAM-SHA-256, optional MD5 auth, TLS, CancelRequest, COPY text/CSV, and
  LISTEN/NOTIFY base surfaces exist.
- Parser, binder, optimizer, executor, storage, MVCC, WAL, catalog, protocol,
  server, CLI, and benchmark crates have working public surfaces and regression
  tests.

## Type And Function Surface

- PostgreSQL-grade `NUMERIC` / `DECIMAL` base-10000 storage, row/COPY/wire
  payloads, exact scaled arithmetic, scale rounding, text casts, and OID
  coverage exist; arbitrary precision and precision enforcement remain open.
- `MONEY` type surface, signed-cent storage, OID 790, wire, COPY, catalog
  persistence, and compatibility tests exist.
- `CHAR(n)` / `bpchar` parser, binder, row codec, executor, OID 1042, COPY,
  catalog persistence, blank padding, assignment/cast truncation, and
  trailing-space comparison semantics exist.
- `DATE`, `TIME`, `TIMETZ`, `TIMESTAMP`, `TIMESTAMPTZ`, and `INTERVAL` runtime
  types exist. `TIMETZ` has parser, binder, row codec, executor, COPY, catalog
  persistence, OID 1266, ISO display, casts, coercions, and offset comparison.
- `BIT(n)` / `BIT VARYING(n)` storage, row codec, operators, wire OIDs, COPY,
  and end-to-end tests exist.
- `INET`, `CIDR`, `MACADDR`, and `MACADDR8` storage, operators, wire OIDs,
  COPY, and end-to-end tests exist.
- JSON and JSONB have distinct runtime/catalog/wire identity, JSON validation
  with text preservation, JSONB normalization, COPY, extended params, operator
  evaluation, and regression tests.
- JSON functions landed for `json_build_object`, `jsonb_set`, `json_each`,
  `jsonb_path_query`, `jsonb_path_exists`, JSON_TABLE subset paths, and
  whole-row `row_to_json`.
- Native arrays support multi-dimensional rectangular text/runtime round trips,
  GIN-facing operators, `array_agg`, `array_length`, `array_cat`,
  `array_to_string`, `string_to_array`, and wire-visible `unnest`.
- `CREATE TYPE ... AS ENUM`, `CREATE TYPE ... AS (composite)`, and
  `CREATE DOMAIN` have durable catalog storage, restart round trips, and wire
  type OID coverage.
- `OID`, `REGCLASS`, `REGTYPE`, and `PG_LSN` parser/binder/runtime/storage/wire
  support exists.
- Basic XML storage exists with validated text storage, OID 142 wire rendering,
  COPY, and restart round trip.
- Ordered-set aggregates `PERCENTILE_CONT` and `PERCENTILE_DISC` have plan shape
  and PostgreSQL-wire coverage.

## Security And Compatibility

- `CREATE ROLE / USER`, `ALTER ROLE`, and `DROP ROLE` work through the role
  catalog and `pg_roles` / `pg_user` visibility.
- `GRANT / REVOKE` on tables, schemas, databases, sequences, and functions work
  through privilege catalog checks.
- Column-level privileges enforce `SELECT`, `INSERT`, and `UPDATE` target
  access.
- Role inheritance and `SET ROLE` support transitive membership, cycle
  rejection, `INHERIT` / `NOINHERIT`, `RESET ROLE`, `current_user`, and
  `session_user`.
- Default privileges apply matching templates for future tables and sequences.
- Persistent RLS policies cover owner, superuser, `BYPASSRLS`, and restart
  semantics for the documented RAG tenant policy shape.
- Driver certification covers `libpq`, `psycopg2`, `psycopg3`,
  `node-postgres`, `pgx`, `lib/pq`, `JDBC PostgreSQL driver`, `Npgsql`, and
  `tokio-postgres`.
- ORM certification covers `SQLAlchemy`, `Django ORM`, `Rails ActiveRecord`,
  `Hibernate ORM`, `GORM`, `Prisma`, and `Diesel`.
- psql meta-command coverage exists for `\d`, `\dt`, `\di`, `\df`, `\dv`,
  `\du`, `\l`, and `\dn`.
- `GUI introspection probes` exist for `pgAdmin`, `DBeaver`, and `DataGrip`.
- Migration tool certification covers `Flyway`, `Liquibase`, and `Alembic`.

## Benchmarks And Performance Evidence

- Benchmark policy: published claims must trace to committed scripts, raw
  artifacts, and recorded host descriptions.
- SQL-surface benchmark work made UltraSQL lead the tracked low-tier workloads
  in the committed matrix; no blanket claim is allowed beyond recorded
  artifacts.
- Release-artifact scale sweep exists through `benchmarks/run_scale_sweep.sh`
  and artifacts under `benchmarks/results/latest/scale-sweep/`; the latest
  v0.0.6 same-host run launches external `ultrasqld` and records UltraSQL as
  the fastest engine on every published DuckDB/SQLite/PostgreSQL row.
- TPC-H SF1 local PostgreSQL 17 certification passed with all q1..q22 complete
  for both engines.
- TPC-H scale 10 (all 22 queries) is complete: latest local artifact
  `benchmarks/results/latest/tpch_sf10_certification.json` has `status passed`
  and `22/22 DuckDB and UltraSQL query timings`.
- Columnar scan path landed: heap rows remain the OLTP/MVCC source of truth,
  `HeapAccess::column_cache` supplies the OLAP shadow path, and committed DML
  invalidation/rebuild/update/delete/insert visibility tests exist.
- Exact vector top-k avoids full physical sort on fallback, reports exact
  kernel/fallback in `EXPLAIN ANALYZE`, and has same-host pgvector exact cert
  smoke.
- Page-backed HNSW and IVFFlat SQL paths exist, survive restart, and have
  crash/corrupt/torn-WAL, rebuild, EXPLAIN, insert/update/delete/VACUUM, and WAL
  payload fuzz/property tests.
- AI/vector smoke artifacts exist for exact top-k, HNSW recall/latency, hybrid
  search, filtered vector search, RAG quality, memory per million vectors,
  ingestion throughput, and cold-start index load.
- local Firebolt Core smoke measured for aggregating-index, wide
  filter/projection, and HNSW vector shapes; sparse primary-index pruning
  remains open because Core EXPLAIN did not expose pruning evidence.

## Regression Baselines

- Curated PostgreSQL parser/type baseline imports public SQLLogicTest cases from
  `char.sql`, `varchar.sql`, `numeric.sql`, and `type_sanity.sql`.
- Transaction isolation baseline covers `acid.sql`, Hermitage G1a/PMP/G2, and
  manager-level Hermitage matrix.
- Index regression baseline covers `CREATE INDEX`, `CREATE UNIQUE INDEX`,
  indexed equality/range reads, and unique violations.
- Constraint regression baseline covers primary key, check, not-null, foreign
  key, duplicate-key rejection, FK rejection, and check-on-update.
- Operator regression baseline covers comparison lexer/evaluator surfaces,
  `BETWEEN`, and `LIKE`.
- Type-specific regression baseline covers numeric, text, date/time/timetz,
  timestamp, JSON/JSONB, and arrays.
