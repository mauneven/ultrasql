# UltraSQL Operations Runbook

This runbook covers the v0.9 operational surface that exists today and the
evidence required before a production claim is valid.

## Start

```bash
cargo run --bin ultrasqld -- \
  --data-dir target/ultrasql-data \
  --listen 127.0.0.1:5433 \
  --ops-listen 127.0.0.1:8080 \
  --autovacuum-interval-ms 1000 \
  --log-format json \
  --log-level info
```

## Health and readiness

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/ready
cargo run --bin ultrasql -- --isready --ops-endpoint 127.0.0.1:8080
```

`/health` reports process liveness. `/ready` additionally checks that the
PostgreSQL-wire listener accepts TCP connections.

## Metrics

```bash
curl -fsS http://127.0.0.1:8080/metrics
```

The endpoint emits Prometheus text format. Current counters are process/build
level only; query, WAL, and buffer counters must be backed by live engine
instrumentation before v0.9 can be called production-complete.

## Catalog diagnostics

Operators can query:

```sql
SELECT * FROM pg_stat_activity;
SELECT * FROM pg_stat_user_tables;
SELECT * FROM pg_stat_user_indexes;
SELECT * FROM pg_statio_user_tables;
SELECT * FROM pg_stat_database;
SELECT * FROM pg_stat_bgwriter;
SELECT * FROM pg_stat_wal;
SELECT * FROM pg_replication_slots;
SELECT * FROM pg_stat_replication;
```

Compatibility view shapes exist so tools do not fail during introspection.
Most counters are currently zero-valued until live stat collectors are wired.

## Control and WAL inspection

```bash
cargo run --bin ultrasql -- --ctl initdb --data-dir target/ultrasql-data
cargo run --bin ultrasql -- --ctl start --data-dir target/ultrasql-data --host 127.0.0.1 --port 5433
cargo run --bin ultrasql -- --ctl status --host 127.0.0.1 --port 5433
cargo run --bin ultrasql -- --ctl standby --data-dir target/standby-data
cargo run --bin ultrasql -- --ctl recovery --data-dir target/restore-data --recovery-target-lsn 0/16B6C50
cargo run --bin ultrasql -- --waldump path/to/wal.segment
cargo run --bin ultrasql -- --basebackup target/backup-001 --data-dir target/ultrasql-data
cargo run --bin ultrasql -- --archive-wal target/ultrasql-data/pg_wal/00000001 --archive-dir target/archive
cargo run --bin ultrasql -- --restore-wal 00000001 --archive-dir target/archive --restore-output target/restore/00000001
```

`--ctl initdb` creates a local directory skeleton. `--ctl start` prints the
server command for service managers. `--ctl status` checks readiness.
`--ctl standby` and `--ctl recovery` create PostgreSQL-style signal
files for orchestration. `--basebackup` copies the data directory and writes
`backup_manifest.json` with file sizes and checksums. `--archive-wal` and
`--restore-wal` provide shell-safe archive/restore commands. `--waldump`
prints deterministic offsets and bytes for inspection.

## Benchmark certification

```bash
benchmarks/tpcb_certify.sh
benchmarks/tpch_sf10_certify.sh
benchmarks/clickbench_certify.sh
```

TPC-B certification requires same-host PostgreSQL 17 and UltraSQL 32-client
wire-protocol result JSON files. The runner writes `passed: false` until both
artifacts exist and the target is met.

## Incident triage

1. Check `/ready`; if it fails, restart via the service manager.
2. Check `/metrics`; if `ultrasql_up` is absent, the process is not serving
   the ops endpoint.
3. Query `pg_stat_activity` for active sessions.
4. Dump recent WAL files with `--waldump` before deleting or truncating them.
5. Record host, command, logs, and result artifacts in the incident report.
