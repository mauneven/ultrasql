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

Fresh run:
`SCALE_SWEEP_ROWS="10000 100000 1000000" ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`.
UltraSQL v0.0.6 was launched as an external `ultrasqld` over TCP on the same
Apple M4 host as installed DuckDB v1.5.2, ClickHouse 26.6.1.208,
SQLite 3.51.0, and PostgreSQL 17. Each row uses 16 measured samples; lower is
better; bold marks the fastest measured engine. Bulk INSERT uses fresh UltraSQL
server processes per measured sample and 10k-row INSERT chunks across engines.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **4.73 ms** | 62.55 ms (1221.5% slower) | 61.79 ms (1205.5% slower) | 18.37 ms (288% slower) | 52.15 ms (1001.8% slower) | UltraSQL |
| INSERT throughput | 100 000 | **40.89 ms** | 402.27 ms (883.8% slower) | 660.22 ms (1514.7% slower) | 64.28 ms (57.2% slower) | 208.14 ms (409% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **432.29 ms** | 3887.89 ms (799.4% slower) | 6523.15 ms (1409% slower) | 623.80 ms (44.3% slower) | 2338.39 ms (440.9% slower) | UltraSQL |
| SELECT scan | 10 000 | **605.58 µs** | 878.25 µs (45% slower) | 1.03 ms (70% slower) | 1.90 ms (213% slower) | 27.76 ms (4483.4% slower) | UltraSQL |
| SELECT scan | 100 000 | **5.97 ms** | 9.35 ms (56.6% slower) | 6.69 ms (12% slower) | 19.51 ms (226.8% slower) | 55.36 ms (827.2% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **58.13 ms** | 98.78 ms (69.9% slower) | 62.37 ms (7.3% slower) | 202.73 ms (248.7% slower) | 205.54 ms (253.6% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **67.29 µs** | 70.33 µs (4.5% slower) | 537.06 µs (698.1% slower) | 140.69 µs (109.1% slower) | 24.39 ms (36139.4% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **63.00 µs** | 105.75 µs (67.9% slower) | 670.77 µs (964.7% slower) | 1.44 ms (2179.4% slower) | 35.97 ms (57002.3% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **57.58 µs** | 187.83 µs (226.2% slower) | 1.72 ms (2895% slower) | 14.25 ms (24653.6% slower) | 38.53 ms (66818% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **59.04 µs** | 94.77 µs (60.5% slower) | 512.40 µs (767.9% slower) | 139.27 µs (135.9% slower) | 24.70 ms (41736.3% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **50.08 µs** | 132.25 µs (164.1% slower) | 730.42 µs (1358.4% slower) | 1.47 ms (2829.2% slower) | 39.02 ms (77808.5% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **50.92 µs** | 263.92 µs (418.3% slower) | 1.68 ms (3207% slower) | 14.00 ms (27399% slower) | 41.84 ms (82076.2% slower) | UltraSQL |
| Filter + SUM | 10 000 | **78.67 µs** | 104.96 µs (33.4% slower) | 595.06 µs (656.4% slower) | 161.52 µs (105.3% slower) | 24.74 ms (31347.7% slower) | UltraSQL |
| Filter + SUM | 100 000 | **55.12 µs** | 139.75 µs (153.5% slower) | 909.33 µs (1549.6% slower) | 1.59 ms (2785.2% slower) | 36.44 ms (66009.9% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **64.42 µs** | 204.98 µs (218.2% slower) | 1.52 ms (2254.7% slower) | 15.79 ms (24417.3% slower) | 41.89 ms (64926.4% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **133.75 µs** | 156.02 µs (16.7% slower) | 4.01 ms (2894.7% slower) | 416.96 µs (211.7% slower) | 42.40 ms (31598.8% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **527.12 µs** | 774.77 µs (47% slower) | 11.75 ms (2129.6% slower) | 4.21 ms (697.9% slower) | 180.00 ms (34047.4% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **1.60 ms** | 2.12 ms (32.6% slower) | 33.43 ms (1989.7% slower) | 43.23 ms (2601.7% slower) | 2025.15 ms (126478.7% slower) | UltraSQL |
| DELETE throughput | 10 000 | **157.08 µs** | 2.09 ms (1227.7% slower) | 5.17 ms (3188.1% slower) | 508.88 µs (224% slower) | 21.65 ms (13681.7% slower) | UltraSQL |
| DELETE throughput | 100 000 | **733.71 µs** | 20.19 ms (2652% slower) | 3.51 ms (378.6% slower) | 5.74 ms (682.1% slower) | 38.88 ms (5199.1% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.35 ms** | 213.66 ms (9005.5% slower) | 2.99 ms (27.5% slower) | 57.83 ms (2364.6% slower) | 168.16 ms (7066.3% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **162.40 µs/op** | 1.25 ms/op (670.3% slower) | 26.97 ms/op (16508.9% slower) | 347.99 µs/op (114.3% slower) | 10.70 ms/op (6486% slower) | UltraSQL |
| Mixed correctness | 100 000 | **153.38 µs** | 312.35 µs (103.7% slower) | 78.74 ms (51239.2% slower) | 2.13 ms (1290.5% slower) | 3.60 ms (2246.9% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.15 ms** | 6.67 ms (60.9% slower) | 5.81 ms (40.2% slower) | 29.27 ms (605.9% slower) | 51.10 ms (1132.5% slower) | UltraSQL |

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
