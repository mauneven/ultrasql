# UltraSQL

Fast SQL database in Rust.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

UltraSQL is a native Rust SQL database with durable storage, MVCC, WAL, query
execution, indexes, vector search, embedded Node/Bun support, and
release-grade benchmark tooling.

The project is pre-alpha. It is already fast on the tracked SQL-surface
benchmarks, but release readiness is evidence-based: correctness, driver
certification, security, coverage, packaging, and operator soak gates must all
close before v1.0.

## Current Shape

- Server, CLI, embedded Node/Bun package, and local runner binaries.
- Parser, binder, optimizer, vectorized executor, MVCC heap, WAL, indexes, COPY,
  JSON/JSONB, text search, vector types, HNSW/IVFFlat, and external scan
  surfaces.
- Driver certification for common application drivers, ORMs, CLI tools, GUI
  tools, and migration tools.
- Reproducible benchmark scripts for measured engines including DuckDB,
  ClickHouse, SQLite, PostgreSQL, local Firebolt Core, TPC-H, TPC-B, TPC-C,
  Sysbench-style OLTP, ClickBench, ANN/vector, and chaos recovery.
- CI gates for format, clippy, tests, cargo-audit, cargo-deny, docs, coverage,
  fuzz, sanitizers, driver certification, and releases.

## Performance Policy

UltraSQL publishes benchmark claims only from committed scripts and raw
artifacts. The release-artifact table below is DB-vs-DB: installed engines on
the same host, raw measurements, per-row fastest engine, and slower percentage
for every other measured engine.

That is a workload-specific artifact claim, not a blanket promise. If a number
is not reproducible from `benchmarks/` on a recorded host, it does not belong in
project docs.

Useful commands:

```bash
cargo run --package ultrasql-bench --features sql-bench --bin cross_compare_sql -- --help
cargo run --package ultrasql-bench --bin readme-render
benchmarks/certify.sh smoke
```

Raw benchmark data lives under
[`benchmarks/results/latest/`](benchmarks/results/latest/). Methodology lives in
[`BENCHMARKS.md`](BENCHMARKS.md).

## Release-Artifact DB-vs-DB Benchmark

Fresh run (2026-06-09):
`CH_BIN="$(command -v clickhouse)" SCALE_SWEEP_ROWS="10000 100000 1000000" ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`.
UltraSQL v0.0.9 was launched as an external `ultrasqld` over TCP on the same
Apple M4 host as installed DuckDB v1.5.2, ClickHouse 26.5.2.39, SQLite 3.51.0,
and PostgreSQL 17. Each row uses 16 measured samples; lower is better; bold
marks the fastest measured engine. Bulk INSERT uses fresh UltraSQL server
processes per measured sample and 10k-row INSERT chunks across engines.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **5.21 ms** | 66.42 ms (1174.4% slower) | 65.69 ms (1160.4% slower) | 20.14 ms (286.4% slower) | 52.97 ms (916.3% slower) | UltraSQL |
| INSERT throughput | 100 000 | **47.03 ms** | 418.06 ms (789% slower) | 641.93 ms (1265.1% slower) | 64.60 ms (37.4% slower) | 202.91 ms (331.5% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **491.91 ms** | 3980.24 ms (709.1% slower) | 6530.64 ms (1227.6% slower) | 667.36 ms (35.7% slower) | 2183.92 ms (344% slower) | UltraSQL |
| SELECT scan | 10 000 | **538.71 µs** | 862.52 µs (60.1% slower) | 1.06 ms (96.3% slower) | 1.89 ms (250.2% slower) | 27.60 ms (5024% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.23 ms** | 10.01 ms (60.6% slower) | 7.48 ms (20% slower) | 20.34 ms (226.5% slower) | 59.12 ms (848.8% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **59.96 ms** | 93.24 ms (55.5% slower) | 65.42 ms (9.1% slower) | 202.02 ms (236.9% slower) | 210.43 ms (251% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **50.79 µs** | 88.75 µs (74.7% slower) | 503.54 µs (891.4% slower) | 140.98 µs (177.6% slower) | 24.69 ms (48512% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **35.00 µs** | 101.10 µs (188.9% slower) | 732.48 µs (1992.8% slower) | 1.41 ms (3936% slower) | 39.46 ms (112644.5% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **62.17 µs** | 167.12 µs (168.8% slower) | 1.78 ms (2769.5% slower) | 14.18 ms (22712.2% slower) | 41.93 ms (67346.4% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **55.08 µs** | 96.38 µs (75% slower) | 464.81 µs (743.8% slower) | 140.02 µs (154.2% slower) | 25.55 ms (46278.1% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **60.58 µs** | 128.29 µs (111.8% slower) | 769.31 µs (1169.8% slower) | 1.42 ms (2243.7% slower) | 39.24 ms (64674.5% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **51.67 µs** | 266.00 µs (414.8% slower) | 1.77 ms (3320.3% slower) | 14.48 ms (27924.9% slower) | 42.66 ms (82464.1% slower) | UltraSQL |
| Filter + SUM | 10 000 | **42.83 µs** | 104.02 µs (142.8% slower) | 603.96 µs (1310% slower) | 155.54 µs (263.1% slower) | 24.61 ms (57359.3% slower) | UltraSQL |
| Filter + SUM | 100 000 | **70.00 µs** | 150.83 µs (115.5% slower) | 866.27 µs (1137.5% slower) | 1.60 ms (2183.4% slower) | 39.27 ms (56002% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **63.46 µs** | 203.85 µs (221.2% slower) | 1.52 ms (2289.5% slower) | 16.09 ms (25251.5% slower) | 42.83 ms (67386.7% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **105.58 µs** | 174.69 µs (65.4% slower) | 3.76 ms (3463.6% slower) | 420.42 µs (298.2% slower) | 45.86 ms (43337.7% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **527.12 µs** | 786.96 µs (49.3% slower) | 11.29 ms (2042.4% slower) | 4.21 ms (699.1% slower) | 168.08 ms (31786.3% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **1.62 ms** | 2.32 ms (43.1% slower) | 34.60 ms (2038.7% slower) | 43.77 ms (2605.5% slower) | 1997.97 ms (123393.5% slower) | UltraSQL |
| DELETE throughput | 10 000 | **141.00 µs** | 2.14 ms (1418.8% slower) | 4.97 ms (3421.4% slower) | 527.71 µs (274.3% slower) | 23.97 ms (16897.5% slower) | UltraSQL |
| DELETE throughput | 100 000 | **519.83 µs** | 20.11 ms (3768.6% slower) | 3.77 ms (625.3% slower) | 5.86 ms (1027.2% slower) | 38.32 ms (7271.5% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.30 ms** | 208.48 ms (8960.8% slower) | 2.65 ms (15.3% slower) | 59.24 ms (2474.6% slower) | 180.28 ms (7735.1% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **170.48 µs/op** | 1.23 ms/op (620% slower) | 25.72 ms/op (14986% slower) | 325.49 µs/op (90.9% slower) | 10.75 ms/op (6207.4% slower) | UltraSQL |
| Mixed correctness | 100 000 | **141.92 µs** | 268.29 µs (89% slower) | 80.11 ms (56349.8% slower) | 2.22 ms (1467% slower) | 3.69 ms (2502.6% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.13 ms** | 6.60 ms (60% slower) | 5.23 ms (26.7% slower) | 29.04 ms (603.6% slower) | 52.28 ms (1166.7% slower) | UltraSQL |

## Quick Start

Install the latest release archive:

```bash
curl -fsSL https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.sh | sh
```

Registry package managers, after their release publish secrets are configured:

```bash
npm install -g ultrasql
pnpm add -g ultrasql
bun add -g ultrasql
brew tap mauneven/tap
brew install ultrasql
yay -S ultrasql-bin
choco install ultrasql
```

Embedded Node/Bun:

```js
const { Database } = require("ultrasql");
const db = await Database.open(":memory:");

db.run("CREATE TABLE t (x int4)");
db.run("INSERT INTO t VALUES (?)", 42);
console.log(db.get("SELECT x FROM t"));
```

GitHub Release package fallback:

```bash
npm install -g https://github.com/mauneven/ultrasql/releases/download/v0.0.9/ultrasql-0.0.9.tgz
pnpm add -g https://github.com/mauneven/ultrasql/releases/download/v0.0.9/ultrasql-0.0.9.tgz
```

Windows PowerShell or setup EXE:

```powershell
iwr https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.ps1 -UseB | iex
iwr https://github.com/mauneven/ultrasql/releases/download/v0.0.9/ultrasql-v0.0.9-x86_64-pc-windows-msvc-setup.exe -OutFile ultrasql-setup.exe
Start-Process .\ultrasql-setup.exe -Wait
```

Build from source:

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql
git config core.hooksPath .githooks
cargo build --locked --profile release-ship --bin ultrasqld --bin ultrasql --bin ultrasql-local
cargo test --workspace --all-features
cargo run --release --bin ultrasqld
```

## Repository Map

```text
crates/       core engine, storage, WAL, MVCC, SQL, protocol, server, CLI, bench
benchmarks/   reproducible scripts, raw artifacts, baselines, certification
docs/         install, operations, limitations, packaging, release notes
tests/        workspace integration and driver certification
fuzz/         parser, wire, WAL, and planner fuzz targets
.github/      CI, docs, coverage, fuzz, sanitizer, operator, release workflows
```

## Read Next

- [ROADMAP.md](ROADMAP.md) - production plan and open gates.
- [DONE.md](DONE.md) - completed milestones and evidence ledger.
- [docs/getting-started.md](docs/getting-started.md) - local first steps.
- [docs/install.md](docs/install.md) - release archives, package managers, and source build.
- [docs/ai-database-strategy.md](docs/ai-database-strategy.md) - AI database and memory-engine plan.
- [docs/packaging.md](docs/packaging.md) - Docker, npm, Homebrew, AUR, Chocolatey, Debian, RPM.
- [docs/known-limitations.md](docs/known-limitations.md) - current SQL limitations.
- [docs/release-checklist.md](docs/release-checklist.md) - release evidence.
- [BENCHMARKS.md](BENCHMARKS.md) - methodology and artifact policy.

## License

Dual-licensed under Apache-2.0 OR MIT.
