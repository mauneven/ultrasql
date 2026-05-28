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
compatibility, security, coverage, packaging, and operator soak gates must all
close before v1.0.

## Current Shape

- Server, CLI, embedded Node/Bun package, and local runner binaries.
- Parser, binder, optimizer, vectorized executor, MVCC heap, WAL, indexes, COPY,
  JSON/JSONB, text search, vector types, HNSW/IVFFlat, and external scan
  surfaces.
- Driver certification for common application drivers, ORMs, CLI tools, GUI
  tools, and migration tools.
- Reproducible benchmark scripts for reference engines including DuckDB,
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
| INSERT throughput | 10 000 | **6.79 ms** | 66.23 ms (874.9% slower) | 60.54 ms (791.2% slower) | 19.27 ms (183.7% slower) | 50.50 ms (643.4% slower) | UltraSQL |
| INSERT throughput | 100 000 | **59.75 ms** | 409.01 ms (584.5% slower) | 658.31 ms (1001.8% slower) | 62.37 ms (4.4% slower) | 193.88 ms (224.5% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **639.64 ms** | 3929.79 ms (514.4% slower) | 6495.59 ms (915.5% slower) | 642.38 ms (0.4% slower) | 2108.27 ms (229.6% slower) | UltraSQL |
| SELECT scan | 10 000 | **685.38 µs** | 953.21 µs (39.1% slower) | 1.10 ms (60.8% slower) | 1.95 ms (184.4% slower) | 30.66 ms (4372.9% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.87 ms** | 9.20 ms (33.8% slower) | 7.26 ms (5.7% slower) | 19.78 ms (187.8% slower) | 59.29 ms (762.7% slower) | UltraSQL |
| SELECT scan | 1 000 000 | 67.71 ms (1% slower) | 95.34 ms (42.2% slower) | **67.05 ms** | 203.26 ms (203.2% slower) | 210.67 ms (214.2% slower) | ClickHouse |
| SELECT SUM(x) | 10 000 | **70.62 µs** | 93.31 µs (32.1% slower) | 559.14 µs (691.7% slower) | 136.21 µs (92.9% slower) | 25.61 ms (36166.5% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **74.75 µs** | 104.44 µs (39.7% slower) | 870.94 µs (1065.1% slower) | 1.44 ms (1829.5% slower) | 36.69 ms (48981.2% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **63.37 µs** | 174.21 µs (174.9% slower) | 1.94 ms (2959.7% slower) | 13.84 ms (21745.6% slower) | 43.73 ms (68906.2% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **76.67 µs** | 94.19 µs (22.9% slower) | 572.71 µs (647% slower) | 149.25 µs (94.7% slower) | 25.35 ms (32967.1% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **74.75 µs** | 131.54 µs (76% slower) | 790.90 µs (958.1% slower) | 1.48 ms (1882.3% slower) | 38.98 ms (52047.6% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **64.62 µs** | 242.44 µs (275.1% slower) | 2.05 ms (3068.9% slower) | 14.54 ms (22399.9% slower) | 40.82 ms (63064.6% slower) | UltraSQL |
| Filter + SUM | 10 000 | **70.33 µs** | 103.02 µs (46.5% slower) | 702.62 µs (899% slower) | 153.38 µs (118.1% slower) | 26.14 ms (37071.7% slower) | UltraSQL |
| Filter + SUM | 100 000 | **73.38 µs** | 136.62 µs (86.2% slower) | 979.00 µs (1234.2% slower) | 1.60 ms (2077.6% slower) | 37.06 ms (50410% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **63.87 µs** | 186.00 µs (191.2% slower) | 1.59 ms (2384.8% slower) | 16.39 ms (25565.4% slower) | 41.28 ms (64529.7% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **120.67 µs** | 171.35 µs (42% slower) | 4.59 ms (3702.3% slower) | 407.62 µs (237.8% slower) | 44.33 ms (36641.4% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **429.88 µs** | 778.50 µs (81.1% slower) | 11.96 ms (2681.3% slower) | 4.21 ms (878.3% slower) | 172.34 ms (39990.7% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **2.10 ms** | 2.15 ms (2.3% slower) | 32.14 ms (1429.5% slower) | 42.39 ms (1917.5% slower) | 1953.68 ms (92878.7% slower) | UltraSQL |
| DELETE throughput | 10 000 | **167.33 µs** | 2.08 ms (1143.6% slower) | 5.53 ms (3203.3% slower) | 538.62 µs (221.9% slower) | 21.57 ms (12788.1% slower) | UltraSQL |
| DELETE throughput | 100 000 | **724.58 µs** | 19.90 ms (2646.2% slower) | 3.97 ms (447.4% slower) | 5.88 ms (711.9% slower) | 37.02 ms (5008.8% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 6.29 ms (97.3% slower) | 220.82 ms (6821.9% slower) | **3.19 ms** | 59.43 ms (1763% slower) | 186.19 ms (5736.5% slower) | ClickHouse |
| Mixed OLTP | 10 000 | **168.96 µs/op** | 1.26 ms/op (646.4% slower) | 23.38 ms/op (13740.4% slower) | 354.82 µs/op (110% slower) | 11.30 ms/op (6587.9% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.69 ms** | 7.32 ms (56% slower) | 6.18 ms (31.8% slower) | 30.04 ms (540.3% slower) | 53.10 ms (1031.5% slower) | UltraSQL |

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
npm install -g https://github.com/mauneven/ultrasql/releases/download/v0.0.6/ultrasql-0.0.6.tgz
pnpm add -g https://github.com/mauneven/ultrasql/releases/download/v0.0.6/ultrasql-0.0.6.tgz
```

Windows PowerShell or setup EXE:

```powershell
iwr https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.ps1 -UseB | iex
iwr https://github.com/mauneven/ultrasql/releases/download/v0.0.6/ultrasql-v0.0.6-x86_64-pc-windows-msvc-setup.exe -OutFile ultrasql-setup.exe
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
docs/         install, operations, compatibility, packaging, release notes
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
- [docs/known-incompatibilities.md](docs/known-incompatibilities.md) - current SQL compatibility gaps.
- [docs/release-checklist.md](docs/release-checklist.md) - release evidence.
- [BENCHMARKS.md](BENCHMARKS.md) - methodology and artifact policy.

## License

Dual-licensed under Apache-2.0 OR MIT.
