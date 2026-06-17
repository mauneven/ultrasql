# UltraSQL

Fast SQL database in Rust.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

UltraSQL is a native Rust SQL database with durable storage, MVCC, WAL, query
execution, indexes, vector search, embedded Node/Bun support, and
release-grade benchmark tooling.

The project is alpha: the engine is broad enough for serious evaluation,
compatibility testing, and reproducible benchmarking, but release readiness is
evidence-based. Correctness, driver certification, security, coverage,
packaging, external audits, incident drills, and operator soak gates must all
close before v1.0 or production use.

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
python3 scripts/run-benchmark-certification.py --mode full
```

Raw benchmark data lives under
[`benchmarks/results/latest/`](benchmarks/results/latest/). Methodology lives in
[`BENCHMARKS.md`](BENCHMARKS.md).

## Release-Artifact DB-vs-DB Benchmark

Fresh data-dir (WAL-backed) run (2026-06-16):
`PGHOST=127.0.0.1 PGPORT=55417 PGUSER=$(id -un) PGDATABASE=ultrasql_bench CH_BIN="$(command -v clickhouse)" SCALE_SWEEP_ROWS="10000 100000 1000000" SCALE_SWEEP_STORAGE=data-dir ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`,
with a tuned PostgreSQL 17 cluster from `benchmarks/scripts/pg17_bench_server.sh start`.
UltraSQL v0.0.9 (external `ultrasqld` over TCP) was measured on the same Apple
M4 host as installed DuckDB v1.5.2, ClickHouse 26.5.2.39, SQLite 3.51.0, and
**PostgreSQL 17.10** (Homebrew). Every engine is measured over a single
persistent connection/session with prepared statements; no timed region spawns
a client process per query (see the Methodology & Fairness note in
[BENCHMARKS.md](BENCHMARKS.md)). Each row uses 32 measured samples after 8
warmup samples; lower is better; bold marks the fastest *measured* engine.

This is an honest same-host scoreboard, not a clean sweep. UltraSQL's
vectorized executor leads every aggregate (SUM/AVG/Filter+SUM), the windowed
scan, small-batch updates/deletes, and large sequential scans — typically by
~2x over DuckDB rather than the inflated margins an earlier, unfair
`psql -c`-per-query harness produced. It is **not** fastest everywhere: under
fair measurement PostgreSQL 17 wins point-mixed OLTP and the small single-shot
INSERT, DuckDB wins the 1M update and 100k delete, and ClickHouse wins the
100k scan and 1M delete. UltraSQL currently **fails the 1M-row INSERT in
durable mode** (the 8 MiB WAL buffer rejects instead of applying backpressure —
tracked in [ROADMAP.md](ROADMAP.md)), so that row has no UltraSQL measurement.
Because UltraSQL is not fastest on every comparable row, the committed
`benchmark_certification_status.json` is honestly `not_ready`; the schema/
envelope validation that previously failed now passes.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | 9.58 ms (25.7% slower) | 65.48 ms (759.1% slower) | 65.33 ms (757.1% slower) | 21.91 ms (187.4% slower) | **7.62 ms** | PostgreSQL |
| INSERT throughput | 100 000 | **38.26 ms** | 407.03 ms (963.9% slower) | 639.92 ms (1572.7% slower) | 48.82 ms (27.6% slower) | 48.63 ms (27.1% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | - | 3849.00 ms (1211.6% slower) | 6486.97 ms (2110.5% slower) | **293.47 ms** | 349.97 ms (19.3% slower) | SQLite |
| SELECT scan | 10 000 | **507.31 µs** | 869.02 µs (71.3% slower) | 984.98 µs (94.2% slower) | 1.85 ms (264.3% slower) | 1.48 ms (192% slower) | UltraSQL |
| SELECT scan | 100 000 | 6.39 ms (3% slower) | 9.31 ms (50% slower) | **6.20 ms** | 19.50 ms (214.5% slower) | 15.47 ms (149.4% slower) | ClickHouse |
| SELECT scan | 1 000 000 | **50.07 ms** | 98.71 ms (97.1% slower) | 59.62 ms (19.1% slower) | 206.30 ms (312% slower) | 159.09 ms (217.7% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **32.75 µs** | 70.40 µs (114.9% slower) | 385.38 µs (1076.7% slower) | 143.63 µs (338.6% slower) | 281.98 µs (761% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **32.42 µs** | 94.23 µs (190.7% slower) | 701.79 µs (2064.9% slower) | 1.43 ms (4313.3% slower) | 2.40 ms (7296.4% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **60.23 µs** | 159.62 µs (165% slower) | 1.56 ms (2497.3% slower) | 15.42 ms (25504.1% slower) | 10.68 ms (17625.2% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **32.67 µs** | 76.75 µs (135% slower) | 415.85 µs (1173% slower) | 142.90 µs (337.4% slower) | 298.71 µs (814.4% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **33.65 µs** | 130.44 µs (287.7% slower) | 717.02 µs (2031.1% slower) | 1.46 ms (4252.6% slower) | 2.71 ms (7940.5% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **37.62 µs** | 222.40 µs (491.1% slower) | 1.57 ms (4074.4% slower) | 15.58 ms (41318.1% slower) | 11.73 ms (31063.2% slower) | UltraSQL |
| Filter + SUM | 10 000 | **35.98 µs** | 79.25 µs (120.3% slower) | 561.02 µs (1459.3% slower) | 156.17 µs (334% slower) | 297.75 µs (727.6% slower) | UltraSQL |
| Filter + SUM | 100 000 | **31.96 µs** | 119.31 µs (273.3% slower) | 927.69 µs (2802.8% slower) | 1.58 ms (4846.3% slower) | 2.58 ms (7974.4% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **37.52 µs** | 169.35 µs (351.4% slower) | 1.53 ms (3974.4% slower) | 17.66 ms (46967.5% slower) | 11.18 ms (29687.2% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **115.94 µs** | 162.79 µs (40.4% slower) | 3.55 ms (2960.8% slower) | 480.77 µs (314.7% slower) | 5.32 ms (4489.7% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **712.98 µs** | 765.31 µs (7.3% slower) | 18.43 ms (2485.3% slower) | 5.71 ms (700.6% slower) | 126.99 ms (17711.1% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 6.56 ms (213.9% slower) | **2.09 ms** | 53.33 ms (2452.4% slower) | 63.35 ms (2932.1% slower) | 2102.10 ms (100513% slower) | DuckDB |
| DELETE throughput | 10 000 | **87.69 µs** | 101.56 µs (15.8% slower) | 4.65 ms (5204.2% slower) | 584.67 µs (566.8% slower) | 1.65 ms (1777.6% slower) | UltraSQL |
| DELETE throughput | 100 000 | 545.83 µs (31% slower) | **416.69 µs** | 3.21 ms (669.2% slower) | 6.95 ms (1569.1% slower) | 14.12 ms (3288.7% slower) | DuckDB |
| DELETE throughput | 1 000 000 | 4.36 ms (31.7% slower) | 4.41 ms (33% slower) | **3.31 ms** | 78.51 ms (2270.7% slower) | 643.58 ms (19333.8% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 471.76 µs/op (1416.4% slower) | 151.27 µs/op (386.2% slower) | 28.96 ms/op (92986.4% slower) | 38.31 µs/op (23.1% slower) | **31.11 µs/op** | PostgreSQL |
| Mixed correctness | 100 000 | **217.02 µs** | 265.27 µs (22.2% slower) | 72.78 ms (33436.3% slower) | 2.25 ms (938.7% slower) | 3.23 ms (1388.9% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.52 ms** | 7.53 ms (66.5% slower) | 6.59 ms (45.6% slower) | 28.49 ms (529.7% slower) | 16.84 ms (272.1% slower) | UltraSQL |

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

- [docs/production-readiness.md](docs/production-readiness.md) - current audited readiness verdict.
- [docs/documentation-status-audit.md](docs/documentation-status-audit.md) - current docs truth audit.
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
