# Driver Certification

UltraSQL certifies PostgreSQL-wire compatibility against stock client
drivers before claiming client ecosystem support.

The PR gate covers:

- stock psql meta-commands `\d`, `\dt`, `\di`, `\df`, `\dv`, `\du`,
  `\l`, and `\dn` through `psql -X -E`, covering the real catalog SQL
  emitted by upstream `psql`;
- GUI introspection probes for pgAdmin, DBeaver, and DataGrip schema
  browsers, covering representative `pg_catalog` and `information_schema`
  query families without launching desktop UI processes;
- Flyway `12.6.2` through its Java API, covering versioned SQL migrations,
  `flyway_schema_history`, DDL/DML migration bodies, and idempotent second
  `migrate` with `executeInTransaction(false)` while transactional DDL remains
  open;
- Liquibase `5.0.3` through its Java API, covering XML changelog update,
  `databasechangelog`, `databasechangeloglock`, and DDL/DML changesets with
  nontransactional changesets while transactional DDL remains open;
- Alembic `1.18.4` through its public Python command API on SQLAlchemy,
  covering `upgrade head`, `alembic_version`, and DDL/DML revision bodies
  with `transactional_ddl=False`;
- `libpq` through a compiled C program using `PQconnectdb`, `PQexec`, and
  `PQexecParams`;
- `psycopg2` through `psycopg2-binary==2.9.12`;
- `psycopg3` through `psycopg[binary]==3.3.4`;
- SQLAlchemy through `SQLAlchemy==2.0.50` on the psycopg3 PostgreSQL
  dialect, covering Core metadata creation in autocommit mode and ORM
  `Session` traffic;
- Django ORM through `Django==6.0.5`, covering model schema creation via
  `schema_editor(atomic=False)`, cursor parameters, `QuerySet` CRUD,
  rollback, and recovery after errors;
- Rails ActiveRecord through `activerecord==8.1.3` and `pg==1.6.3`,
  covering PostgreSQL adapter startup, schema creation, prepared model
  queries, model create/query, rollback, and recovery after errors;
- `node-postgres` through `pg==8.21.0` under Node.js 22;
- Go `lib/pq==1.12.3`, `pgx==5.9.2`, and GORM `1.31.1` with
  `gorm.io/driver/postgres==1.6.0`, covering `AutoMigrate`, model CRUD,
  rollback, and recovery after errors;
- the JDBC PostgreSQL driver `42.7.11`, downloaded from Maven Central and
  checked against a pinned SHA-256 digest;
- Hibernate ORM `7.3.5.Final`, built through a pinned Apache Maven runner
  when no system Maven is available, covering `SessionFactory`, annotated
  entity persist/query, rollback, and recovery after errors;
- `Npgsql==10.0.2` on .NET 8 with type loading disabled so the driver uses
  only core wire traffic;
- Prisma `7.8.0` with `@prisma/adapter-pg==7.8.0`, covering Prisma Client
  generation, raw parameter queries, model create/query, rollback, and
  recovery after errors;
- Diesel `2.3.9`, covering the PostgreSQL backend, query DSL parameter
  filtering, typed inserts, rollback, and recovery after errors.

Run locally:

```bash
cargo build -p ultrasql-server --bin ultrasqld
python3 -m venv /tmp/ultrasql-driver-cert
/tmp/ultrasql-driver-cert/bin/python -m pip install -r tests/driver_certification/requirements.txt
bundle install --gemfile tests/driver_certification/rails/Gemfile
pnpm --dir tests/driver_certification/node install --frozen-lockfile
go -C tests/driver_certification/go mod download
cargo fetch --manifest-path tests/driver_certification/diesel/Cargo.toml
dotnet restore --locked-mode tests/driver_certification/dotnet/Ultrasql.DriverCertification.csproj
/tmp/ultrasql-driver-cert/bin/python tests/driver_certification/driver_certification.py \
  --ultrasqld target/debug/ultrasqld
```

The harness starts a real `ultrasqld` process on an ephemeral localhost port
and verifies startup, simple query, extended query parameter binding, DDL/DML,
migration version tables, explicit transaction rollback, and failed-transaction
recovery. It writes a machine-readable report to
`target/driver-certification.json`.

Current scope is intentionally narrow: it proves the named drivers can connect
and run core SQL/ORM/migration traffic through public APIs. It does not
certify every ORM edge, desktop GUI launch/click path, admin-tool mutation
workflow, COPY mode, TLS mode, async notification edge, pipeline mode, every
migration command-line flag, or driver-specific type adapter. ORM and
migration schema creation runs outside explicit transaction blocks where the
upstream tool allows it because transactional DDL remains an open
compatibility gap.
