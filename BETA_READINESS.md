# UltraSQL Public Beta Readiness

This document says exactly what the public beta covers, what it does not,
and how to try it in two minutes. Every claim here is backed by a committed
artifact or a test in this repository; where evidence is still missing, this
document says so instead of claiming.

**Status: PUBLIC BETA (single node).** Not production-ready and not v1.0:
the GA release gate honestly reports `not_ready` until two external audits,
three executed incident drills, and three independent 30-day operator soaks
exist (see `benchmarks/results/latest/release_gate_status.json`). Those are
evidence-gathering gates that only time and third parties can close; nothing
in this repo fakes them.

## Quickstart (the boot smoke test exercises exactly this)

```bash
echo 'change-me-please' > pgpass
docker run --rm -p 5432:5432 \
  -e ULTRASQL_USER=app \
  -e ULTRASQL_PASSWORD_FILE=/run/secrets/pgpass \
  -v ultrasql-data:/var/lib/ultrasql \
  -v ./pgpass:/run/secrets/pgpass:ro \
  ghcr.io/mauneven/ultrasql:v0.0.9

PGPASSWORD=change-me-please psql "host=127.0.0.1 port=5432 user=app" \
  -c "CREATE TABLE t (id INT, msg TEXT);" -c "INSERT INTO t VALUES (1,'hi');"
```

Authentication is SCRAM-SHA-256 by default (`ULTRASQL_PASSWORD` /
`ULTRASQL_PASSWORD_FILE` enable it; `ULTRASQL_HOST_AUTH_METHOD=trust` is the
explicit unsafe opt-out). Data persists in `/var/lib/ultrasql`.
`tests/scripts/test_docker_entrypoint.py` and
`crates/ultrasql-server/tests/default_boot_smoke_round_trip.rs` gate these
entry points in CI; `docs/install.md` carries the tagged-release pull line.

## What the beta covers

- **PostgreSQL wire protocol v3**: simple + extended query protocols, TLS,
  SCRAM/MD5/pg_hba auth, cancel requests, LISTEN/NOTIFY, structured
  ErrorResponse fields (S, V, C, M, and D/H where a detail or hint exists).
  The driver certification suite runs real libpq, psycopg2/3, SQLAlchemy,
  Django, Rails ActiveRecord, node-postgres, Go lib/pq + pgx + GORM, JDBC,
  Hibernate, Npgsql, Prisma, Diesel, Flyway, Liquibase, Alembic, and
  psql/pgAdmin/DBeaver/DataGrip introspection query families against a real
  `ultrasqld`; the committed matrix is
  `benchmarks/results/latest/driver_compatibility_status.json` (regenerated
  by CI on the release commit — read the artifact, not this sentence, for
  the current pass/fail set).
- **SQL + ACID**: MVCC snapshot isolation (READ COMMITTED / REPEATABLE READ
  defaults, SERIALIZABLE via column-range SSI with a documented precision
  limitation), savepoints with own-write visibility, two-phase commit,
  WAL-before-commit durability, crash recovery with torn-page protection,
  online CHECKPOINT + WAL recycling. Durability/MVCC/isolation/recovery
  suites run in CI on every commit.
- **Operational safety by default**: 30 s default `statement_timeout`
  (per-session overridable, including to 0), idle-in-transaction timeout,
  connection cap, bounded result streaming for large SELECTs, per-query
  `work_mem` spill budget, 128 MiB binary-COPY bound.
- **Hot standby (single-follower)**: base-backup bring-up
  (`ultrasql --basebackup`), `standby.signal` + `primary_conninfo`
  auto-connected walreceiver, continuous WAL apply, read-only query serving —
  proven by a two-node over-the-wire test
  (`replication_standby_live_apply_round_trip`).
- **Observability**: `/health`, `/ready`, `/metrics` (WAL/standby LSN gauges,
  txn counters), `pg_stat_wal`, `EXPLAIN ANALYZE`.

## Performance

The committed scale-sweep artifacts under
`benchmarks/results/latest/scale-sweep/` are regenerated from real runs of
`benchmarks/run_scale_sweep.sh` (durable data-dir mode, equal-durability
settings per engine, methodology in `BENCHMARKS.md`). In the committed
2026-07-01 run, UltraSQL is the fastest measured engine on 21 of 24 workloads
against DuckDB, ClickHouse, SQLite, and a tuned PostgreSQL 17 on the same
host; the three losses (1M bulk UPDATE — DuckDB, 1M bulk DELETE — ClickHouse,
point-op Mixed OLTP — in-process SQLite) are reported in the same artifact.
Do not quote numbers that are not in those artifacts. The benchmark-fairness contract — same
host, same durability class per engine, failures recorded as
`not_available`, no winner claims over unmeasured engines — is enforced by
`benchmarks/scripts/check_supremacy.py` and the benchmark certification
gate.

## Known limitations (read before deploying)

The authoritative list is [docs/known-limitations.md](docs/known-limitations.md).
Highlights a beta user will actually hit:

- **HA/DR maturity**: streaming hot standby works (see above) but there is
  no synchronous commit mode, no promotion/failover tooling, no cascading
  replication; authorization changes (roles/GRANT/RLS) reach a standby only
  via a new base backup. Full HA/DR is coming, not a beta blocker.
- **Cursor holdability**: server-side cursors are forward-only and
  `WITHOUT HOLD` only; `WITH HOLD` and `SCROLL` are rejected with `0A000`.
- **Transactional DDL** covers a documented subset; out-of-subset DDL inside
  `BEGIN` is rejected with `0A000` and a hint (deterministic for
  ORMs/migration tools).
- **SERIALIZABLE** uses column-range SSI with relation-level fallback for
  unsupported predicate shapes — correct but coarse (spurious `40001` aborts
  are possible under contention).
- **macOS durability posture**: the default `wal_sync_method=fsync` matches
  PostgreSQL's and SQLite's defaults on every platform; on macOS that means
  a sudden power loss (not an OS crash) can lose drive-cached writes. Use
  `--wal-sync-method fsync_writethrough` for full power-loss durability on
  Apple hardware (see docs/configuration.md).
- **Single node**: no multi-node consensus, no sharding.

## GA gate (honest)

`benchmarks/results/latest/release_gate_status.json` is `not_ready` and
stays that way until real external evidence exists:

- two independent external audits (correctness, security) — outreach status
  in `docs/external-audits.md`;
- three executed incident drills — `docs/incident-drills.md`;
- three independent 30-day operator soak reports — `docs/OPERATOR_SOAK.md`.

None of these can be closed by code in this repository, and this beta does
not claim them.
