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
artifacts. The current tracked SQL-surface matrix records UltraSQL leading the
published low-tier workloads, including aggregate, scan, insert/update/delete,
mixed OLTP, window, vector, and local Firebolt Core smoke shapes.

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
benchmarked commit `46b47ab9`, Apple M4 / 10 cores / 16 GB RAM, macOS 26.5.

Engines measured in this run: UltraSQL v0.0.6 through `tokio-postgres` against
`ultrasqld`; DuckDB v1.5.2; SQLite 3.51.0; PostgreSQL 14.22 Homebrew. Lower is
better. Bold marks the fastest measured engine for that workload.

| Workload | Rows | UltraSQL | DuckDB | SQLite | PostgreSQL |
|---|---:|---:|---:|---:|---:|
| INSERT throughput | 10 000 | **6.05 ms** | 63.42 ms | 19.05 ms | 48.05 ms |
| SELECT scan | 10 000 | **704.29 µs** | 878.14 µs | 1.90 ms | 28.80 ms |
| SELECT SUM(x) | 65 536 | **63.67 µs** | 93.62 µs | 956.25 µs | 32.15 ms |
| SELECT AVG(x) | 1 000 000 | **61.79 µs** | 258.81 µs | 14.43 ms | 41.54 ms |
| Filter + SUM | 1 000 000 | **63.17 µs** | 191.10 µs | 15.99 ms | 44.27 ms |
| UPDATE throughput | 10 000 | **114.33 µs** | 154.46 µs | 409.42 µs | 47.15 ms |
| DELETE throughput | 10 000 | **146.04 µs** | 2.03 ms | 535.88 µs | 22.08 ms |
| Mixed OLTP | 10 000 | **166.33 µs/op** | 1.25 ms/op | 351.76 µs/op | 10.64 ms/op |
| Window row_number() | 65 536 | **4.62 ms** | 7.05 ms | 29.19 ms | 53.51 ms |

This is a same-host SQL-surface snapshot, not a universal performance claim.
ClickHouse and Firebolt were not measured in this run because their local
binaries/services were unavailable.

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
