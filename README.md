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
artifacts. The tracked SQL-surface matrix records strong wins across scan,
aggregate, filter, mixed OLTP, window, vector, and local Firebolt Core smoke
shapes; stricter release-artifact sweeps also record the places still losing.

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

## Current DB-vs-DB Snapshot

Fresh local run: `benchmarks/run_wire.sh full`, 32 measured samples per row,
Apple M4 / 10 cores / 16 GB RAM, macOS 26.5.

Engines measured in this run: UltraSQL v0.0.6 through `tokio-postgres` against
`ultrasqld`; DuckDB v1.5.2; SQLite 3.51.0; PostgreSQL 14.22 Homebrew. Lower is
better. Bold marks the fastest measured engine for that workload.

| Workload | Rows | UltraSQL | DuckDB | SQLite | PostgreSQL |
|---|---:|---:|---:|---:|---:|
| INSERT throughput | 10 000 | **6.30 ms** | 63.02 ms | 19.09 ms | 46.65 ms |
| SELECT scan | 10 000 | **699.67 µs** | 859.12 µs | 1.86 ms | 29.63 ms |
| SELECT SUM(x) | 65 536 | **63.67 µs** | 76.85 µs | 920.48 µs | 32.09 ms |
| SELECT AVG(x) | 1 000 000 | **63.92 µs** | 249.08 µs | 14.34 ms | 43.73 ms |
| Filter + SUM | 1 000 000 | **63.67 µs** | 167.62 µs | 15.93 ms | 42.78 ms |
| UPDATE throughput | 10 000 | **107.92 µs** | 159.65 µs | 404.42 µs | 44.44 ms |
| DELETE throughput | 10 000 | **161.25 µs** | 1.99 ms | 524.71 µs | 21.74 ms |
| Mixed OLTP | 10 000 | **160.89 µs/op** | 1.24 ms/op | 341.87 µs/op | 10.70 ms/op |
| Window row_number() | 65 536 | **4.78 ms** | 7.15 ms | 29.28 ms | 53.32 ms |

This is a same-host SQL-surface snapshot, not a universal performance claim.
ClickHouse and Firebolt were not measured in this run because their local
binaries/services were unavailable.

## Release-Artifact Scale Sweep

Fresh release-artifact run:
`SCALE_SWEEP_ROWS="10000 100000 1000000" benchmarks/run_scale_sweep.sh full`,
16 measured samples per row, UltraSQL v0.0.6 installed through
`scripts/install.sh latest` and launched as an external `ultrasqld` over TCP on
the same Apple M4 host. Competitors use installed local clients. Lower is
better. A dash means no benchmark claim for that row.

| Workload | Rows | UltraSQL | DuckDB | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **6.62 ms** | 62.64 ms | 18.75 ms | 50.57 ms | UltraSQL |
| INSERT throughput | 100 000 | 69.44 ms | 402.55 ms | **65.86 ms** | 179.95 ms | SQLite |
| INSERT throughput | 1 000 000 | - | 3814.02 ms | **790.31 ms** | 2932.01 ms | SQLite |
| SELECT scan | 10 000 | **714.54 µs** | 866.00 µs | 1.84 ms | 28.22 ms | UltraSQL |
| SELECT scan | 100 000 | **7.07 ms** | 9.90 ms | 19.18 ms | 56.00 ms | UltraSQL |
| SELECT scan | 1 000 000 | **68.79 ms** | 94.88 ms | 202.34 ms | 204.61 ms | UltraSQL |
| SELECT SUM(x) | 10 000 | 74.58 µs | **68.62 µs** | 138.31 µs | 24.18 ms | DuckDB |
| SELECT SUM(x) | 100 000 | **59.62 µs** | 104.58 µs | 1.42 ms | 33.31 ms | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **63.00 µs** | 164.08 µs | 14.04 ms | 40.51 ms | UltraSQL |
| Filter + SUM | 10 000 | **62.38 µs** | 108.48 µs | 155.83 µs | 26.39 ms | UltraSQL |
| Filter + SUM | 100 000 | **71.50 µs** | 141.19 µs | 1.57 ms | 36.04 ms | UltraSQL |
| Filter + SUM | 1 000 000 | **64.50 µs** | 180.56 µs | 15.80 ms | 40.15 ms | UltraSQL |
| UPDATE throughput | 10 000 | **109.75 µs** | 167.58 µs | 418.17 µs | 42.24 ms | UltraSQL |
| UPDATE throughput | 100 000 | **434.83 µs** | 773.10 µs | 4.16 ms | 159.75 ms | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.94 ms | **2.21 ms** | 45.39 ms | 1923.45 ms | DuckDB |

Open scale-sweep gaps: v0.0.6 hits buffer-pool exhaustion on the 1M bulk
INSERT release-artifact row, SQLite narrowly leads the 100k INSERT row, DuckDB
leads the 10k SUM row, and DuckDB leads the 1M UPDATE row.

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
- [docs/packaging.md](docs/packaging.md) - Docker, npm, Homebrew, AUR, Chocolatey, Debian, RPM.
- [docs/known-incompatibilities.md](docs/known-incompatibilities.md) - current PostgreSQL gaps.
- [docs/release-checklist.md](docs/release-checklist.md) - release evidence.
- [BENCHMARKS.md](BENCHMARKS.md) - methodology and artifact policy.

## License

Dual-licensed under Apache-2.0 OR MIT.
