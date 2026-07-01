# UltraSQL

An embeddable, PostgreSQL-compatible, ACID SQL database in Rust that keeps your
relational data, JSON metadata, full-text, and vector embeddings in one engine —
and ranks them together in one query.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](TODO.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

UltraSQL is a native Rust SQL database with durable storage, MVCC, WAL,
vectorized query execution, B-tree/hash/HNSW/IVFFlat indexes, JSON/JSONB,
full-text search, the `vector` type with `<->`/`<=>`/`<#>` operators, embedded
Node/Bun support, and release-grade benchmark tooling.

## Why UltraSQL for RAG and agent memory

RAG and agent applications usually stitch together Postgres + a vector DB +
a search index + a cache, then reconcile them in application code. UltraSQL
collapses that stack: source text, its embedding, and its JSON metadata are
columns of **one ACID table**, so a single SQL statement fuses vector
similarity, BM25 lexical relevance, and SQL/JSON metadata filters into one
ranked top-k — inside one transaction, over one consistent MVCC snapshot.

```sql
SELECT id, body
FROM memories
WHERE metadata @> '{"tenant":"acme"}'
ORDER BY hybrid_search(body, 'failed invoice payment', embedding,
                       VECTOR '[...]', 'rrf') DESC
LIMIT 10;
```

The moat is transactional consistency: updating a row's text, embedding, and
metadata is one transaction, so the retrieval surfaces can never drift the way
a separate vector store, search index, and SQL database can. Run the whole story
end-to-end — ingest, hybrid retrieval, and survival across a process restart —
as a zero-dependency Node script in [examples/node-rag/](examples/node-rag/).
See [docs/hybrid-search.md](docs/hybrid-search.md) for the worked example,
[docs/vector-benchmarks.md](docs/vector-benchmarks.md) for honest
recall-vs-latency versus pgvector / Qdrant / LanceDB, and
[TODO.md](TODO.md) for what is shipped versus open (selectivity-aware
filtered ANN and competitive recall benchmarks are tracked there with measurable
exit conditions).

This is not a "fastest at everything" claim. UltraSQL's measured strengths are
reads, scans, aggregations, and unified retrieval; heavy single-row OLTP write
throughput is a documented weak spot. The same-host scoreboard below reports
wins and losses honestly.

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
- A driver-certification harness that exercises common application drivers,
  ORMs, CLI tools, GUI tools, and migration tools (current pass status is
  tracked in [docs/driver-certification.md](docs/driver-certification.md), not
  a blanket "certified" guarantee).
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
Beta scope, quickstart, and honest limitations: [`BETA_READINESS.md`](BETA_READINESS.md).

## Release-Artifact DB-vs-DB Benchmark

Fresh data-dir (WAL-backed) run (2026-07-01), pinned to commit
`1faf525d`:
`PGHOST=127.0.0.1 PGPORT=55417 PGUSER=$(id -un) PGDATABASE=ultrasql_bench CH_BIN="$(command -v clickhouse)" SCALE_SWEEP_ROWS="10000 100000 1000000" SCALE_SWEEP_STORAGE=data-dir ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`,
with a tuned PostgreSQL 17 cluster from `benchmarks/scripts/pg17_bench_server.sh start`.
UltraSQL v0.0.9 (external `ultrasqld` over TCP) was measured on the same Apple
M4 host as installed DuckDB v1.5.2, ClickHouse 26.5.2.39, SQLite 3.51.0, and
**PostgreSQL 17.10** (Homebrew). Every engine is measured over a single
persistent connection/session with prepared statements; no timed region spawns
a client process per query (see the Methodology & Fairness note in
[BENCHMARKS.md](BENCHMARKS.md)). Each row uses 32 measured samples after 8
warmup samples; lower is better; bold marks the fastest *measured* engine.

**UltraSQL is the fastest measured engine in 21 of 24 workloads on this host; every loss is reported below.** This is an honest scoreboard, not a clean
sweep. UltraSQL leads every INSERT row (including the 1M-row durable bulk load
that an earlier artifact recorded `not_available` before the WAL backpressure
fix), every aggregate (SUM/AVG/Filter+SUM), all three sequential scans, the
windowed scan, mixed correctness, and every 10k/100k UPDATE and DELETE. The
three losses are reported, not hidden: the 1M-row bulk UPDATE (DuckDB) and
1M-row bulk DELETE (ClickHouse), where those engines parallelize a single bulk
mutation across cores while UltraSQL's fused mutation path is single-threaded,
and point-op Mixed OLTP, where in-process SQLite pays no wire round trip
(UltraSQL and the tuned PostgreSQL 17 server — the same-architecture peer —
measure within a few percent of each other on that row). The certification
gate certifies *fair methodology* and reports per-row wins and losses as a
scoreboard rather than demanding an impossible clean sweep, so
`benchmark_certification_status.json` is `ready`.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.53 ms** | 66.29 ms (4239.3% slower) | 64.70 ms (4134.8% slower) | 22.37 ms (1364.2% slower) | 3.54 ms (131.6% slower) | UltraSQL |
| INSERT throughput | 100 000 | **9.83 ms** | 401.54 ms (3984.1% slower) | 630.05 ms (6308.3% slower) | 44.43 ms (351.9% slower) | 21.46 ms (118.3% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **114.43 ms** | 3818.04 ms (3236.7% slower) | 6171.47 ms (5293.5% slower) | 271.30 ms (137.1% slower) | 267.50 ms (133.8% slower) | UltraSQL |
| SELECT scan | 10 000 | **579.71 µs** | 870.27 µs (50.1% slower) | 975.63 µs (68.3% slower) | 1.88 ms (224.4% slower) | 1.46 ms (152.3% slower) | UltraSQL |
| SELECT scan | 100 000 | **5.64 ms** | 9.26 ms (64.2% slower) | 6.60 ms (17% slower) | 19.51 ms (246% slower) | 15.29 ms (171.1% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **57.97 ms** | 91.29 ms (57.5% slower) | 58.77 ms (1.4% slower) | 205.37 ms (254.3% slower) | 158.62 ms (173.6% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **39.29 µs** | 68.50 µs (74.3% slower) | 491.02 µs (1149.7% slower) | 143.29 µs (264.7% slower) | 298.38 µs (659.4% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **34.98 µs** | 92.35 µs (164% slower) | 669.21 µs (1813.2% slower) | 1.44 ms (4018% slower) | 2.39 ms (6722.9% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **48.58 µs** | 167.33 µs (244.4% slower) | 1.60 ms (3198.4% slower) | 15.50 ms (31808.8% slower) | 10.93 ms (22403% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **28.58 µs** | 73.50 µs (157.1% slower) | 486.52 µs (1602.1% slower) | 143.02 µs (400.4% slower) | 319.37 µs (1017.3% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **39.19 µs** | 114.21 µs (191.4% slower) | 688.12 µs (1656% slower) | 1.44 ms (3579.3% slower) | 2.61 ms (6556.9% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **63.33 µs** | 237.00 µs (274.2% slower) | 1.57 ms (2372.4% slower) | 15.46 ms (24317.5% slower) | 11.75 ms (18455.1% slower) | UltraSQL |
| Filter + SUM | 10 000 | **37.52 µs** | 84.19 µs (124.4% slower) | 578.77 µs (1442.5% slower) | 151.85 µs (304.7% slower) | 321.31 µs (756.4% slower) | UltraSQL |
| Filter + SUM | 100 000 | **36.54 µs** | 123.12 µs (236.9% slower) | 821.33 µs (2147.6% slower) | 1.59 ms (4262.2% slower) | 2.60 ms (7007% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **58.67 µs** | 166.25 µs (183.4% slower) | 1.57 ms (2577.6% slower) | 17.42 ms (29586.8% slower) | 11.76 ms (19937% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **116.40 µs** | 160.19 µs (37.6% slower) | 3.70 ms (3077% slower) | 488.96 µs (320.1% slower) | 5.24 ms (4402.8% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **711.65 µs** | 764.79 µs (7.5% slower) | 11.51 ms (1517.3% slower) | 5.53 ms (677.1% slower) | 107.26 ms (14971.5% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 6.63 ms (202% slower) | **2.20 ms** | 34.28 ms (1461.1% slower) | 59.09 ms (2591.3% slower) | 1806.95 ms (82193.2% slower) | DuckDB |
| DELETE throughput | 10 000 | **97.90 µs** | 103.96 µs (6.2% slower) | 4.18 ms (4166.2% slower) | 586.40 µs (499% slower) | 1.63 ms (1561.4% slower) | UltraSQL |
| DELETE throughput | 100 000 | **313.98 µs** | 422.10 µs (34.4% slower) | 3.30 ms (951.6% slower) | 7.04 ms (2140.9% slower) | 13.80 ms (4296% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 5.40 ms (59.1% slower) | 4.28 ms (26% slower) | **3.40 ms** | 71.88 ms (2015.8% slower) | 431.39 ms (12598.8% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 30.62 µs/op (104.5% slower) | 155.06 µs/op (935.4% slower) | 24.06 ms/op (160526.2% slower) | **14.98 µs/op** | 29.33 µs/op (95.8% slower) | SQLite |
| Mixed correctness | 100 000 | **49.38 µs** | 261.54 µs (429.7% slower) | 76.27 ms (154374.8% slower) | 2.23 ms (4423.1% slower) | 3.18 ms (6347.9% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.57 ms** | 6.80 ms (48.7% slower) | 5.69 ms (24.6% slower) | 27.46 ms (500.9% slower) | 15.73 ms (244.3% slower) | UltraSQL |

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
examples/     runnable demos (e.g. examples/node-rag, the embedded RAG walkthrough)
tests/        workspace integration and driver certification
fuzz/         parser, wire, WAL, and planner fuzz targets
.github/      CI, docs, coverage, fuzz, sanitizer, operator, release workflows
```

## Read Next

- [docs/production-readiness.md](docs/production-readiness.md) - current audited readiness verdict.
- [docs/documentation-status-audit.md](docs/documentation-status-audit.md) - current docs truth audit.
- [TODO.md](TODO.md) - open work and release gates.
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
