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
UltraSQL v0.0.9 was launched as an external `ultrasqld` over TCP on the same
Apple M4 host as installed DuckDB v1.5.2, SQLite 3.51.0, and PostgreSQL 17.
ClickHouse was not available in this local run, so its cells are shown as `-`
and are not ranked. Each row uses 16 measured samples; lower is better; bold
marks the fastest measured engine. Bulk INSERT uses fresh UltraSQL server
processes per measured sample and 10k-row INSERT chunks across engines.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **4.83 ms** | 62.26 ms (1188.7% slower) | - | 17.91 ms (270.7% slower) | 46.62 ms (864.9% slower) | UltraSQL |
| INSERT throughput | 100 000 | **43.45 ms** | 402.36 ms (825.9% slower) | - | 60.28 ms (38.7% slower) | 196.40 ms (352% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **458.08 ms** | 3815.34 ms (732.9% slower) | - | 592.37 ms (29.3% slower) | 2075.94 ms (353.2% slower) | UltraSQL |
| SELECT scan | 10 000 | **610.71 µs** | 889.75 µs (45.7% slower) | - | 1.89 ms (210.1% slower) | 28.82 ms (4619.2% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.05 ms** | 8.98 ms (48.6% slower) | - | 19.27 ms (218.7% slower) | 55.71 ms (821.2% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **59.15 ms** | 95.58 ms (61.6% slower) | - | 204.05 ms (245% slower) | 242.29 ms (309.6% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **78.92 µs** | 85.21 µs (8% slower) | - | 138.73 µs (75.8% slower) | 25.31 ms (31975.9% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **69.42 µs** | 106.90 µs (54% slower) | - | 1.41 ms (1925.3% slower) | 36.77 ms (52868.3% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **64.83 µs** | 184.81 µs (185.1% slower) | - | 14.51 ms (22282.8% slower) | 40.34 ms (62125.8% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **79.42 µs** | 90.21 µs (13.6% slower) | - | 136.17 µs (71.5% slower) | 25.11 ms (31514.4% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **70.21 µs** | 133.17 µs (89.7% slower) | - | 1.45 ms (1970.6% slower) | 37.52 ms (53344% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **63.17 µs** | 266.12 µs (321.3% slower) | - | 14.41 ms (22714% slower) | 39.96 ms (63157.9% slower) | UltraSQL |
| Filter + SUM | 10 000 | **62.08 µs** | 97.12 µs (56.4% slower) | - | 161.31 µs (159.8% slower) | 24.95 ms (40084.5% slower) | UltraSQL |
| Filter + SUM | 100 000 | **53.67 µs** | 141.12 µs (163% slower) | - | 1.57 ms (2818.4% slower) | 37.17 ms (69165.7% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **63.87 µs** | 218.33 µs (241.8% slower) | - | 16.04 ms (25007.6% slower) | 39.61 ms (61904.8% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **127.46 µs** | 160.52 µs (25.9% slower) | - | 421.92 µs (231% slower) | 40.44 ms (31625.7% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **519.67 µs** | 768.27 µs (47.8% slower) | - | 4.05 ms (679.7% slower) | 161.15 ms (30910.1% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **1.56 ms** | 2.08 ms (33.7% slower) | - | 42.19 ms (2609% slower) | 2127.37 ms (136488.6% slower) | UltraSQL |
| DELETE throughput | 10 000 | **136.96 µs** | 2.03 ms (1383% slower) | - | 534.21 µs (290.1% slower) | 20.68 ms (14995.9% slower) | UltraSQL |
| DELETE throughput | 100 000 | **494.33 µs** | 20.34 ms (4015.1% slower) | - | 5.83 ms (1079.7% slower) | 36.69 ms (7322.6% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.18 ms** | 210.44 ms (9551.6% slower) | - | 58.85 ms (2599.2% slower) | 182.19 ms (8256.2% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **167.51 µs/op** | 1.23 ms/op (631.6% slower) | - | 322.78 µs/op (92.7% slower) | 10.64 ms/op (6251% slower) | UltraSQL |
| Mixed correctness | 100 000 | **155.75 µs** | 264.00 µs (69.5% slower) | - | 2.26 ms (1349.8% slower) | 3.68 ms (2263.3% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.08 ms** | 6.58 ms (61.5% slower) | - | 29.13 ms (614.3% slower) | 51.84 ms (1171.4% slower) | UltraSQL |

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
