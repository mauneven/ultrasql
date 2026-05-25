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
| **UltraSQL** | 46.25 µs | — |
| DuckDB | 94.54 µs | 104.4% slower |
| SQLite | 909.79 µs | 1,867% slower |
| Firebolt Core | 4.09 ms | 8,733% slower |
| PostgreSQL | 29.73 ms | 64,184% slower |
<!-- END AUTO: BENCH:select_sum_65k_i64 -->

<!-- BEGIN AUTO: BENCH:filter_sum_1m_i64 -->
### Filter + SUM — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 43.58 µs | — |
| DuckDB | 168.15 µs | 285.8% slower |
| Firebolt Core | 2.66 ms | 5,998% slower |
| SQLite | 15.48 ms | 35,406% slower |
| PostgreSQL | 41.97 ms | 96,187% slower |
<!-- END AUTO: BENCH:filter_sum_1m_i64 -->

<!-- BEGIN AUTO: BENCH:select_avg_1m_i64 -->
### SELECT AVG(x) FROM t — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 37.17 µs | — |
| DuckDB | 268.85 µs | 623.4% slower |
| Firebolt Core | 3.96 ms | 10,562% slower |
| SQLite | 14.03 ms | 37,657% slower |
| PostgreSQL | 41.35 ms | 111,163% slower |
<!-- END AUTO: BENCH:select_avg_1m_i64 -->

<!-- BEGIN AUTO: BENCH:insert_throughput_10k -->
### INSERT throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 6.18 ms | — |
| SQLite | 18.35 ms | 196.8% slower |
| Firebolt Core | 37.43 ms | 505.5% slower |
| PostgreSQL | 53.94 ms | 772.8% slower |
| DuckDB | 60.30 ms | 875.7% slower |
<!-- END AUTO: BENCH:insert_throughput_10k -->

<!-- BEGIN AUTO: BENCH:select_scan_10k -->
### SELECT scan — 10 000 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 701.75 µs | — |
| DuckDB | 882.06 µs | 25.7% slower |
| SQLite | 1.83 ms | 161.3% slower |
| Firebolt Core | 4.71 ms | 571.6% slower |
| PostgreSQL | 27.66 ms | 3,842% slower |
<!-- END AUTO: BENCH:select_scan_10k -->

<!-- BEGIN AUTO: BENCH:update_throughput_10k -->
### UPDATE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 95.83 µs | — |
| DuckDB | 172.56 µs | 80.1% slower |
| SQLite | 456.63 µs | 376.5% slower |
| Firebolt Core | 44.54 ms | 46,381% slower |
| PostgreSQL | 47.87 ms | 49,851% slower |
<!-- END AUTO: BENCH:update_throughput_10k -->

<!-- BEGIN AUTO: BENCH:delete_throughput_10k -->
### DELETE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 168.38 µs | — |
| SQLite | 520.40 µs | 209.1% slower |
| DuckDB | 1.91 ms | 1,036% slower |
| Firebolt Core | 10.84 ms | 6,338% slower |
| PostgreSQL | 23.86 ms | 14,074% slower |
<!-- END AUTO: BENCH:delete_throughput_10k -->

<!-- BEGIN AUTO: BENCH:mixed_oltp_pgbench_like -->
### Mixed OLTP (pgbench-like)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 166.75 µs | — |
| SQLite | 327.23 µs | 96.2% slower |
| DuckDB | 1.21 ms | 625.1% slower |
| PostgreSQL | 11.24 ms | 6,638% slower |
| Firebolt Core | 28.08 ms | 16,738% slower |
<!-- END AUTO: BENCH:mixed_oltp_pgbench_like -->

<!-- BEGIN AUTO: BENCH:window_row_number_65k_i64 -->
### Window — row_number() OVER (ORDER BY x) over 65 536 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 4.83 ms | — |
| DuckDB | 6.37 ms | 32.0% slower |
| Firebolt Core | 15.72 ms | 225.8% slower |
| SQLite | 28.60 ms | 492.7% slower |
| PostgreSQL | 51.49 ms | 967% slower |
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
