# UltraSQL

PostgreSQL-compatible OLTP + OLAP database in Rust.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

UltraSQL keeps the PostgreSQL wire protocol and client ecosystem while
rebuilding the engine underneath: storage, MVCC, WAL, query execution,
indexes, vector search, and benchmark tooling are all native Rust.

The project is pre-alpha. It is already fast on the tracked SQL-surface
benchmarks, but release readiness is evidence-based: correctness, driver
compatibility, security, coverage, packaging, and operator soak gates must all
close before v1.0.

## Current Shape

- PostgreSQL wire protocol v3 server plus CLI and local runner binaries.
- Parser, binder, optimizer, vectorized executor, MVCC heap, WAL, indexes, COPY,
  JSON/JSONB, text search, vector types, HNSW/IVFFlat, and external scan
  surfaces.
- Driver certification for libpq, psql, psycopg, SQLAlchemy, Django, Rails,
  node-postgres, Go drivers, JDBC, Npgsql, Prisma, Diesel, and GUI/migration
  tools.
- Reproducible benchmark scripts for PostgreSQL, DuckDB, SQLite, ClickHouse,
  local Firebolt Core, TPC-H, TPC-B, TPC-C, Sysbench-style OLTP, ClickBench,
  ANN/vector, and chaos recovery.
- CI gates for format, clippy, tests, cargo-audit, cargo-deny, docs, coverage,
  fuzz, sanitizers, driver certification, and releases.

## Performance Policy

UltraSQL publishes benchmark claims only from committed scripts and raw
artifacts. The latest release-artifact scale sweep records UltraSQL as the
fastest measured engine on every published row in that table.

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
Apple M4 host as installed DuckDB v1.5.2, SQLite 3.51.0, and PostgreSQL 17.
ClickHouse is a first-class benchmark leg; `-` means the current committed
artifact has no same-run ClickHouse measurement. Each row uses 16 measured
samples; lower is better; bold marks the fastest measured engine. Bulk INSERT
uses fresh UltraSQL server processes per measured sample and 10k-row INSERT
chunks across engines.

| Workload | Rows | UltraSQL | DuckDB | SQLite | PostgreSQL | ClickHouse | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **6.79 ms** | 66.23 ms | 19.27 ms | 50.50 ms | - | UltraSQL |
| INSERT throughput | 100 000 | **59.75 ms** | 409.01 ms | 62.37 ms | 193.88 ms | - | UltraSQL |
| INSERT throughput | 1 000 000 | **639.64 ms** | 3929.79 ms | 642.38 ms | 2108.27 ms | - | UltraSQL |
| SELECT scan | 10 000 | **685.38 µs** | 953.21 µs | 1.95 ms | 30.66 ms | - | UltraSQL |
| SELECT scan | 100 000 | **6.87 ms** | 9.20 ms | 19.78 ms | 59.29 ms | - | UltraSQL |
| SELECT scan | 1 000 000 | **67.71 ms** | 95.34 ms | 203.26 ms | 210.67 ms | - | UltraSQL |
| SELECT SUM(x) | 10 000 | **70.62 µs** | 93.31 µs | 136.21 µs | 25.61 ms | - | UltraSQL |
| SELECT SUM(x) | 100 000 | **74.75 µs** | 104.44 µs | 1.44 ms | 36.69 ms | - | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **63.37 µs** | 174.21 µs | 13.84 ms | 43.73 ms | - | UltraSQL |
| SELECT AVG(x) | 10 000 | **76.67 µs** | 94.19 µs | 149.25 µs | 25.35 ms | - | UltraSQL |
| SELECT AVG(x) | 100 000 | **74.75 µs** | 131.54 µs | 1.48 ms | 38.98 ms | - | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **64.62 µs** | 242.44 µs | 14.54 ms | 40.82 ms | - | UltraSQL |
| Filter + SUM | 10 000 | **70.33 µs** | 103.02 µs | 153.38 µs | 26.14 ms | - | UltraSQL |
| Filter + SUM | 100 000 | **73.38 µs** | 136.62 µs | 1.60 ms | 37.06 ms | - | UltraSQL |
| Filter + SUM | 1 000 000 | **63.87 µs** | 186.00 µs | 16.39 ms | 41.28 ms | - | UltraSQL |
| UPDATE throughput | 10 000 | **120.67 µs** | 171.35 µs | 407.62 µs | 44.33 ms | - | UltraSQL |
| UPDATE throughput | 100 000 | **429.88 µs** | 778.50 µs | 4.21 ms | 172.34 ms | - | UltraSQL |
| UPDATE throughput | 1 000 000 | **2.10 ms** | 2.15 ms | 42.39 ms | 1953.68 ms | - | UltraSQL |
| DELETE throughput | 10 000 | **167.33 µs** | 2.08 ms | 538.62 µs | 21.57 ms | - | UltraSQL |
| DELETE throughput | 100 000 | **724.58 µs** | 19.90 ms | 5.88 ms | 37.02 ms | - | UltraSQL |
| DELETE throughput | 1 000 000 | **6.29 ms** | 220.82 ms | 59.43 ms | 186.19 ms | - | UltraSQL |
| Mixed OLTP | 10 000 | **168.96 µs/op** | 1.26 ms/op | 354.82 µs/op | 11.30 ms/op | - | UltraSQL |
| Window row_number() | 65 536 | **4.69 ms** | 7.32 ms | 30.04 ms | 53.10 ms | - | UltraSQL |

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
- [docs/ai-database-strategy.md](docs/ai-database-strategy.md) - DuckDB/ClickHouse parity map and AI memory engine plan.
- [docs/packaging.md](docs/packaging.md) - Docker, npm, Homebrew, AUR, Chocolatey, Debian, RPM.
- [docs/known-incompatibilities.md](docs/known-incompatibilities.md) - current PostgreSQL gaps.
- [docs/release-checklist.md](docs/release-checklist.md) - release evidence.
- [BENCHMARKS.md](BENCHMARKS.md) - methodology and artifact policy.

## License

Dual-licensed under Apache-2.0 OR MIT.
