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

Fresh run (2026-06-10):
`CH_BIN="$(command -v clickhouse)" SCALE_SWEEP_ROWS="10000 100000 1000000" ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`.
UltraSQL v0.0.9 was launched as an external `ultrasqld` over TCP on the same
Apple M4 host as installed DuckDB v1.5.2, ClickHouse 26.5.2.39, SQLite 3.51.0,
and PostgreSQL 17. Each row uses 32 measured samples after 8 warmup samples;
lower is better; bold marks the fastest measured engine. Bulk INSERT uses fresh
UltraSQL server processes per measured sample and 10k-row INSERT chunks across
engines.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **5.64 ms** | 67.93 ms (1104.1% slower) | 61.35 ms (987.6% slower) | 21.55 ms (282.1% slower) | 54.43 ms (864.8% slower) | UltraSQL |
| INSERT throughput | 100 000 | **48.50 ms** | 425.40 ms (777% slower) | 652.23 ms (1244.7% slower) | 66.68 ms (37.5% slower) | 205.65 ms (324% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **516.29 ms** | 4068.99 ms (688.1% slower) | 6472.41 ms (1153.6% slower) | 717.06 ms (38.9% slower) | 2196.20 ms (325.4% slower) | UltraSQL |
| SELECT scan | 10 000 | **613.88 µs** | 880.15 µs (43.4% slower) | 1.10 ms (78.7% slower) | 1.95 ms (217.4% slower) | 30.90 ms (4933.7% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.39 ms** | 9.85 ms (54.1% slower) | 7.36 ms (15.2% slower) | 20.26 ms (217.1% slower) | 61.14 ms (857% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **62.05 ms** | 99.28 ms (60% slower) | 68.48 ms (10.4% slower) | 210.37 ms (239% slower) | 220.79 ms (255.8% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **59.88 µs** | 76.02 µs (27% slower) | 457.98 µs (664.9% slower) | 138.17 µs (130.8% slower) | 27.35 ms (45586.2% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **34.83 µs** | 85.02 µs (144.1% slower) | 796.98 µs (2187.9% slower) | 1.46 ms (4094.4% slower) | 39.64 ms (113706.5% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **43.71 µs** | 172.96 µs (295.7% slower) | 1.87 ms (4184% slower) | 14.66 ms (33438.3% slower) | 47.90 ms (109481.1% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **36.46 µs** | 82.71 µs (126.9% slower) | 461.73 µs (1166.5% slower) | 135.56 µs (271.8% slower) | 27.70 ms (75880.8% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **42.21 µs** | 113.75 µs (169.5% slower) | 733.98 µs (1638.9% slower) | 1.47 ms (3376.8% slower) | 40.10 ms (94915.1% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **60.17 µs** | 246.94 µs (310.4% slower) | 1.83 ms (2943.4% slower) | 14.28 ms (23637.2% slower) | 47.55 ms (78930.8% slower) | UltraSQL |
| Filter + SUM | 10 000 | **48.00 µs** | 110.31 µs (129.8% slower) | 557.48 µs (1061.4% slower) | 154.83 µs (222.6% slower) | 27.89 ms (58008.4% slower) | UltraSQL |
| Filter + SUM | 100 000 | **57.08 µs** | 136.29 µs (138.8% slower) | 822.33 µs (1340.6% slower) | 1.62 ms (2742.8% slower) | 40.02 ms (70001.4% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **39.38 µs** | 184.48 µs (368.5% slower) | 1.56 ms (3868.7% slower) | 16.32 ms (41338.8% slower) | 48.48 ms (123018.4% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **94.75 µs** | 160.94 µs (69.9% slower) | 4.31 ms (4450.4% slower) | 425.52 µs (349.1% slower) | 48.41 ms (50989.3% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **563.00 µs** | 797.10 µs (41.6% slower) | 12.29 ms (2082.3% slower) | 4.32 ms (666.9% slower) | 172.95 ms (30619.3% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **1.96 ms** | 2.30 ms (17.3% slower) | 35.54 ms (1713.3% slower) | 44.50 ms (2170.3% slower) | 2101.20 ms (107108.8% slower) | UltraSQL |
| DELETE throughput | 10 000 | **181.75 µs** | 2.19 ms (1104.8% slower) | 4.59 ms (2427.8% slower) | 539.10 µs (196.6% slower) | 24.56 ms (13415.8% slower) | UltraSQL |
| DELETE throughput | 100 000 | **709.21 µs** | 20.39 ms (2774.9% slower) | 3.68 ms (419.4% slower) | 5.87 ms (727.2% slower) | 39.43 ms (5460.1% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.73 ms** | 231.06 ms (8375.4% slower) | 3.14 ms (15.1% slower) | 60.85 ms (2132.2% slower) | 173.38 ms (6259.7% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **211.15 µs/op** | 1.32 ms/op (523.5% slower) | 27.48 ms/op (12915.3% slower) | 386.16 µs/op (82.9% slower) | 12.35 ms/op (5746.9% slower) | UltraSQL |
| Mixed correctness | 100 000 | **159.00 µs** | 284.50 µs (78.9% slower) | 78.83 ms (49476.8% slower) | 2.29 ms (1340.1% slower) | 3.65 ms (2195.5% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.31 ms** | 7.49 ms (73.9% slower) | 6.36 ms (47.5% slower) | 30.18 ms (600.7% slower) | 59.77 ms (1287.6% slower) | UltraSQL |

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
