# UltraSQL

PostgreSQL-compatible OLTP+OLAP database in Rust. UltraSQL targets the
PostgreSQL wire protocol, PostgreSQL-like SQL/MVCC behavior, WAL-backed
storage, analytical execution, vector search, and production-grade operations
without hiding benchmark caveats.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

## What UltraSQL is

- PostgreSQL wire protocol server plus Rust in-process engine.
- Parser, binder, optimizer, executor, MVCC, WAL, heap storage, indexes, COPY,
  JSON/vector/text-search surfaces, and benchmark harnesses in one workspace.
- Built for honest competition against PostgreSQL, DuckDB, ClickHouse,
  SQLite, and local Firebolt Core using reproducible artifacts.

## Status

UltraSQL is pre-alpha. Many core database pieces exist, but release readiness
is tracked in [ROADMAP.md](ROADMAP.md), not claimed from README prose.

Current high-level truth:

- SQL wire path exists for many SELECT/DML/DDL shapes.
- Storage, MVCC, WAL, indexes, vector/ANN, JSON/text, COPY, and lakehouse
  pieces are actively developed.
- TPC-H SF10 and multiple Firebolt local-Core smoke artifacts exist.
- ClickBench, TPC-B/TPC-C/Sysbench certification and several production ops
  surfaces remain open until measured artifacts prove them.

## Benchmarks

All headline rows below are SQL-surface measurements. No README comparison row
may come from a storage kernel, executor-only path, `cargo bench`, or any
in-process helper that bypasses parser/planner/executor dispatch/client output.

Methodology:

- Same host: Apple M4 Mac mini, hot cache, median of recorded samples.
- UltraSQL: `tokio-postgres` against `ultrasqld`.
- PostgreSQL: libpq/`psql`.
- DuckDB: `duckdb` CLI.
- SQLite: `sqlite3` CLI.
- ClickHouse: `clickhouse-client` or local equivalent.
- Firebolt: local Firebolt Core SQL HTTP, not hosted Firebolt.

Raw data and rules live in [BENCHMARKS.md](BENCHMARKS.md) and
[`benchmarks/results/latest/`](benchmarks/results/latest/). Regenerate tables
with:

```bash
cargo run --package ultrasql-bench --bin readme-render
```

Refresh Firebolt rows first when needed:

```bash
benchmarks/firebolt_readme_matrix.sh
cargo run --package ultrasql-bench --bin readme-render
```

<!-- BEGIN AUTO: BENCH:select_sum_65k_i64 -->
### SELECT SUM(x) FROM t — 65 536 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 45.04 µs | — |
| DuckDB | 87.27 µs | 93.8% slower |
| ClickHouse | 802.29 µs | 1,681% slower |
| SQLite | 959.79 µs | 2,031% slower |
| Firebolt Core | 4.09 ms | 8,970% slower |
| PostgreSQL | 26.60 ms | 58,953% slower |
<!-- END AUTO: BENCH:select_sum_65k_i64 -->

<!-- BEGIN AUTO: BENCH:filter_sum_1m_i64 -->
### Filter + SUM — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 47.38 µs | — |
| DuckDB | 172.62 µs | 264.4% slower |
| ClickHouse | 1.66 ms | 3,411% slower |
| Firebolt Core | 2.66 ms | 5,510% slower |
| SQLite | 16.23 ms | 34,165% slower |
| PostgreSQL | 43.03 ms | 90,720% slower |
<!-- END AUTO: BENCH:filter_sum_1m_i64 -->

<!-- BEGIN AUTO: BENCH:select_avg_1m_i64 -->
### SELECT AVG(x) FROM t — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 47.67 µs | — |
| DuckDB | 217.67 µs | 356.6% slower |
| ClickHouse | 1.94 ms | 3,961% slower |
| Firebolt Core | 3.96 ms | 8,213% slower |
| SQLite | 14.77 ms | 30,894% slower |
| PostgreSQL | 42.10 ms | 88,215% slower |
<!-- END AUTO: BENCH:select_avg_1m_i64 -->

<!-- BEGIN AUTO: BENCH:insert_throughput_10k -->
### INSERT throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 4.09 ms | — |
| SQLite | 26.04 ms | 536.0% slower |
| Firebolt Core | 37.43 ms | 814.2% slower |
| PostgreSQL | 44.03 ms | 975% slower |
| ClickHouse | 57.11 ms | 1,295% slower |
| DuckDB | 66.13 ms | 1,515% slower |
<!-- END AUTO: BENCH:insert_throughput_10k -->

<!-- BEGIN AUTO: BENCH:select_scan_10k -->
### SELECT scan — 10 000 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 635.71 µs | — |
| DuckDB | 871.52 µs | 37.1% slower |
| SQLite | 1.95 ms | 206.6% slower |
| ClickHouse | 2.02 ms | 217.5% slower |
| Firebolt Core | 4.71 ms | 641.3% slower |
| PostgreSQL | 25.89 ms | 3,972% slower |
<!-- END AUTO: BENCH:select_scan_10k -->

<!-- BEGIN AUTO: BENCH:update_throughput_10k -->
### UPDATE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 148.71 µs | — |
| DuckDB | 168.00 µs | 13.0% slower |
| SQLite | 431.77 µs | 190.3% slower |
| ClickHouse | 5.29 ms | 3,454% slower |
| Firebolt Core | 44.54 ms | 29,854% slower |
| PostgreSQL | 47.45 ms | 31,810% slower |
<!-- END AUTO: BENCH:update_throughput_10k -->

<!-- BEGIN AUTO: BENCH:delete_throughput_10k -->
### DELETE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 133.17 µs | — |
| SQLite | 553.88 µs | 315.9% slower |
| DuckDB | 2.11 ms | 1,483% slower |
| ClickHouse | 7.33 ms | 5,405% slower |
| Firebolt Core | 10.84 ms | 8,040% slower |
| PostgreSQL | 21.29 ms | 15,887% slower |
<!-- END AUTO: BENCH:delete_throughput_10k -->

<!-- BEGIN AUTO: BENCH:mixed_oltp_pgbench_like -->
### Mixed OLTP (pgbench-like)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 163.30 µs | — |
| SQLite | 366.05 µs | 124.2% slower |
| DuckDB | 1.29 ms | 687.4% slower |
| PostgreSQL | 9.17 ms | 5,518% slower |
| ClickHouse | 26.33 ms | 16,023% slower |
| Firebolt Core | 28.08 ms | 17,093% slower |
<!-- END AUTO: BENCH:mixed_oltp_pgbench_like -->

<!-- BEGIN AUTO: BENCH:window_row_number_65k_i64 -->
### Window — row_number() OVER (ORDER BY x) over 65 536 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 4.32 ms | — |
| DuckDB | 8.42 ms | 95.1% slower |
| ClickHouse | 9.13 ms | 111.6% slower |
| Firebolt Core | 15.72 ms | 264.4% slower |
| SQLite | 30.38 ms | 604.1% slower |
| PostgreSQL | 55.49 ms | 1,186% slower |
<!-- END AUTO: BENCH:window_row_number_65k_i64 -->

Additional local Firebolt Core smoke comparisons target Firebolt-specific
query shapes. They are SQL-surface artifacts, but not general workload claims.

<!-- BEGIN AUTO: BENCH:firebolt_aggregate_index_10k -->
### Firebolt aggregating-index dashboard aggregate — 10 000 rows (local Core smoke)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 185.42 µs | — |
| Firebolt Core | 5.20 ms | 2,703% slower |
<!-- END AUTO: BENCH:firebolt_aggregate_index_10k -->

<!-- BEGIN AUTO: BENCH:late_materialization_10k -->
### Firebolt-style late materialization — 10 000 rows (local Core smoke)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 550.88 µs | — |
| Firebolt Core | 194.34 ms | 35,178% slower |
<!-- END AUTO: BENCH:late_materialization_10k -->

<!-- BEGIN AUTO: BENCH:vector_ann_hnsw_512_8d_k10 -->
### HNSW vector search — 512 vectors, 8 dims, k=10 (local Core smoke)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** (HNSW) | 138.29 µs | — |
| Firebolt Core (HNSW) | 13.79 ms | 9,869% slower |
<!-- END AUTO: BENCH:vector_ann_hnsw_512_8d_k10 -->

Kernel and in-process microbenchmarks stay internal under `crates/*/benches/`.
They are useful for regression tracking, but not for README comparison claims.

## Quick start

Install the latest release archive:

```bash
curl -fsSL https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.sh | sh
```

Windows PowerShell:

```powershell
iwr https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.ps1 -UseB | iex
```

See [docs/install.md](docs/install.md) for platform targets, checksum
verification, and manual install steps.

Build from source:

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql
git config core.hooksPath .githooks
cargo build --locked --profile release-ship --bin ultrasqld --bin ultrasql --bin ultrasql-local
cargo test --workspace
cargo run --release --bin ultrasqld
```

## Repository map

```text
crates/       Rust crates for core, storage, WAL, MVCC, parser, planner,
              optimizer, executor, protocol, server, CLI, benchmarks
benchmarks/   reproducible benchmark scripts, raw artifacts, baselines
docs/         design, testing, compatibility, and operations docs
tests/        workspace integration tests
fuzz/         fuzz targets and corpora
.github/      CI workflows
```

## Important docs

- [ROADMAP.md](ROADMAP.md) — release plan and open gaps.
- [docs/install.md](docs/install.md) — binary install and source build.
- [docs/getting-started.md](docs/getting-started.md) — local first steps.
- [docs/migration-from-postgresql.md](docs/migration-from-postgresql.md) — migration validation path.
- [docs/known-incompatibilities.md](docs/known-incompatibilities.md) — current PostgreSQL gaps.
- [docs/release-checklist.md](docs/release-checklist.md) — release gate.
- [BENCHMARKS.md](BENCHMARKS.md) — benchmark methodology and artifact policy.
- [ARCHITECTURE.md](ARCHITECTURE.md) — subsystem design.
- [PERFORMANCE.md](PERFORMANCE.md) — performance engineering rules.
- [CONTRIBUTING.md](CONTRIBUTING.md) — contributor workflow.
- [AGENTS.md](AGENTS.md) — operating manual for humans and development tools.
- [SECURITY.md](SECURITY.md) — vulnerability disclosure.

## License

Dual-licensed under [Apache License 2.0](LICENSE-APACHE) and
[MIT License](LICENSE-MIT).
