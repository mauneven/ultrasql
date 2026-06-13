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

Fresh run (2026-06-12):
`CH_BIN="$(command -v clickhouse)" SCALE_SWEEP_ROWS="10000 100000 1000000" ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`.
UltraSQL v0.0.9 was launched as an external `ultrasqld` over TCP on the same
Apple M4 host as installed DuckDB v1.5.2, ClickHouse 26.5.2.39, SQLite 3.51.0,
and PostgreSQL 14.22 (Homebrew). Each row uses 32 measured samples after 8 warmup samples;
lower is better; bold marks the fastest measured engine. Bulk INSERT uses fresh
UltraSQL server processes per measured sample and 10k-row INSERT chunks across
engines.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **5.66 ms** | 68.14 ms (1103.2% slower) | 60.73 ms (972.3% slower) | 21.13 ms (273.1% slower) | 54.37 ms (860% slower) | UltraSQL |
| INSERT throughput | 100 000 | **47.88 ms** | 425.07 ms (787.8% slower) | 648.81 ms (1255.1% slower) | 66.59 ms (39.1% slower) | 203.99 ms (326.1% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **518.82 ms** | 4073.24 ms (685.1% slower) | 6454.41 ms (1144.1% slower) | 721.66 ms (39.1% slower) | 2193.44 ms (322.8% slower) | UltraSQL |
| SELECT scan | 10 000 | **560.62 µs** | 875.15 µs (56.1% slower) | 1.05 ms (87.1% slower) | 1.93 ms (244.8% slower) | 30.71 ms (5378% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.29 ms** | 9.93 ms (57.9% slower) | 7.43 ms (18% slower) | 20.26 ms (221.9% slower) | 60.71 ms (864.7% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **60.76 ms** | 99.41 ms (63.6% slower) | 66.80 ms (9.9% slower) | 209.42 ms (244.6% slower) | 220.72 ms (263.2% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **59.33 µs** | 90.90 µs (53.2% slower) | 502.81 µs (747.4% slower) | 139.98 µs (135.9% slower) | 27.34 ms (45979.6% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **46.38 µs** | 107.02 µs (130.8% slower) | 712.88 µs (1437.2% slower) | 1.49 ms (3116.7% slower) | 38.73 ms (83414.9% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **45.71 µs** | 158.42 µs (246.6% slower) | 1.76 ms (3750.9% slower) | 14.59 ms (31824.2% slower) | 47.38 ms (103548.8% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **57.79 µs** | 92.08 µs (59.3% slower) | 531.29 µs (819.3% slower) | 140.21 µs (142.6% slower) | 27.52 ms (47511% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **64.29 µs** | 127.02 µs (97.6% slower) | 752.86 µs (1071% slower) | 1.49 ms (2224.7% slower) | 39.82 ms (61842.7% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **65.08 µs** | 250.31 µs (284.6% slower) | 1.78 ms (2630.7% slower) | 14.55 ms (22259.4% slower) | 46.66 ms (71598.6% slower) | UltraSQL |
| Filter + SUM | 10 000 | **46.42 µs** | 105.25 µs (126.8% slower) | 578.81 µs (1147% slower) | 152.00 µs (227.5% slower) | 27.47 ms (59082.9% slower) | UltraSQL |
| Filter + SUM | 100 000 | **44.38 µs** | 133.90 µs (201.7% slower) | 805.38 µs (1714.9% slower) | 1.68 ms (3686.7% slower) | 39.45 ms (88798.1% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **61.75 µs** | 183.56 µs (197.3% slower) | 1.72 ms (2679.6% slower) | 16.28 ms (26272.1% slower) | 48.73 ms (78818.2% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **115.92 µs** | 171.75 µs (48.2% slower) | 3.80 ms (3177.9% slower) | 422.02 µs (264.1% slower) | 48.22 ms (41496.7% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **543.83 µs** | 822.44 µs (51.2% slower) | 12.41 ms (2181.6% slower) | 4.25 ms (682% slower) | 172.81 ms (31675.7% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **2.03 ms** | 2.29 ms (13% slower) | 36.44 ms (1697.7% slower) | 44.63 ms (2101.8% slower) | 2103.99 ms (103704.6% slower) | UltraSQL |
| DELETE throughput | 10 000 | **161.12 µs** | 2.09 ms (1198.7% slower) | 3.75 ms (2229% slower) | 528.85 µs (228.2% slower) | 23.73 ms (14626.1% slower) | UltraSQL |
| DELETE throughput | 100 000 | **599.54 µs** | 20.36 ms (3296.5% slower) | 3.74 ms (524.6% slower) | 5.91 ms (886.3% slower) | 38.75 ms (6363.8% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.68 ms** | 230.93 ms (8524.4% slower) | 3.26 ms (21.6% slower) | 60.58 ms (2162.3% slower) | 176.08 ms (6476% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **211.96 µs/op** | 1.32 ms/op (521.1% slower) | 22.87 ms/op (10691% slower) | 385.31 µs/op (81.8% slower) | 12.35 ms/op (5724.5% slower) | UltraSQL |
| Mixed correctness | 100 000 | **148.38 µs** | 275.90 µs (85.9% slower) | 78.63 ms (52896.1% slower) | 2.25 ms (1418.3% slower) | 3.69 ms (2386.8% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.27 ms** | 7.61 ms (78.4% slower) | 6.29 ms (47.5% slower) | 31.61 ms (641% slower) | 60.23 ms (1311.7% slower) | UltraSQL |

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
