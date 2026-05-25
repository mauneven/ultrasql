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
| DuckDB | 78.69 µs | — |
| **UltraSQL** | 85.42 µs | 8.6% slower |
| SQLite | 945.52 µs | 1,102% slower |
| Firebolt Core | 4.09 ms | 5,092% slower |
| PostgreSQL | 31.00 ms | 39,301% slower |
<!-- END AUTO: BENCH:select_sum_65k_i64 -->

<!-- BEGIN AUTO: BENCH:filter_sum_1m_i64 -->
### Filter + SUM — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 66.29 µs | — |
| DuckDB | 202.69 µs | 205.8% slower |
| Firebolt Core | 2.66 ms | 3,909% slower |
| SQLite | 16.70 ms | 25,089% slower |
| PostgreSQL | 44.49 ms | 67,009% slower |
<!-- END AUTO: BENCH:filter_sum_1m_i64 -->

<!-- BEGIN AUTO: BENCH:select_avg_1m_i64 -->
### SELECT AVG(x) FROM t — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 218.04 µs | — |
| DuckDB | 288.36 µs | 32.2% slower |
| Firebolt Core | 3.96 ms | 1,717% slower |
| SQLite | 14.38 ms | 6,496% slower |
| PostgreSQL | 46.90 ms | 21,411% slower |
<!-- END AUTO: BENCH:select_avg_1m_i64 -->

<!-- BEGIN AUTO: BENCH:insert_throughput_10k -->
### INSERT throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 13.53 ms | — |
| SQLite | 22.45 ms | 66.0% slower |
| Firebolt Core | 37.43 ms | 176.6% slower |
| PostgreSQL | 60.21 ms | 345.0% slower |
| DuckDB | 73.65 ms | 444.4% slower |
<!-- END AUTO: BENCH:insert_throughput_10k -->

<!-- BEGIN AUTO: BENCH:select_scan_10k -->
### SELECT scan — 10 000 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| DuckDB | 887.92 µs | — |
| SQLite | 1.90 ms | 114.0% slower |
| **UltraSQL** | 3.19 ms | 258.9% slower |
| Firebolt Core | 4.71 ms | 430.8% slower |
| PostgreSQL | 29.46 ms | 3,218% slower |
<!-- END AUTO: BENCH:select_scan_10k -->

<!-- BEGIN AUTO: BENCH:update_throughput_10k -->
### UPDATE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| DuckDB | 173.17 µs | — |
| **UltraSQL** | 239.33 µs | 38.2% slower |
| SQLite | 418.79 µs | 141.8% slower |
| Firebolt Core | 44.54 ms | 25,623% slower |
| PostgreSQL | 53.63 ms | 30,870% slower |
<!-- END AUTO: BENCH:update_throughput_10k -->

<!-- BEGIN AUTO: BENCH:delete_throughput_10k -->
### DELETE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 199.00 µs | — |
| SQLite | 538.73 µs | 170.7% slower |
| DuckDB | 5.73 ms | 2,779% slower |
| Firebolt Core | 10.84 ms | 5,347% slower |
| PostgreSQL | 28.29 ms | 14,115% slower |
<!-- END AUTO: BENCH:delete_throughput_10k -->

<!-- BEGIN AUTO: BENCH:mixed_oltp_pgbench_like -->
### Mixed OLTP (pgbench-like)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 182.15 µs | — |
| SQLite | 388.25 µs | 113.1% slower |
| DuckDB | 3.39 ms | 1,759% slower |
| PostgreSQL | 12.20 ms | 6,595% slower |
| Firebolt Core | 28.08 ms | 15,314% slower |
<!-- END AUTO: BENCH:mixed_oltp_pgbench_like -->

<!-- BEGIN AUTO: BENCH:window_row_number_65k_i64 -->
### Window — row_number() OVER (ORDER BY x) over 65 536 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| DuckDB | 7.71 ms | — |
| Firebolt Core | 15.72 ms | 103.8% slower |
| **UltraSQL** | 21.11 ms | 173.7% slower |
| SQLite | 30.21 ms | 291.7% slower |
| PostgreSQL | 71.69 ms | 829.4% slower |
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
| **UltraSQL** (HNSW) | 278.83 µs | — |
| Firebolt Core (HNSW) | 13.79 ms | 4,844% slower |
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

Docker, Homebrew, Debian, and RPM packaging are covered by
[docs/packaging.md](docs/packaging.md). Tagged releases publish the container
as `ghcr.io/mauneven/ultrasql:<tag>` and attach generated package assets to the
GitHub release.

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
- [docs/packaging.md](docs/packaging.md) — docs site, Docker, Homebrew, Deb/RPM.
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
