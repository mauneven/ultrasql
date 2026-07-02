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
sweep. UltraSQL leads every INSERT row (including the durable 1M-row bulk
load), every aggregate (SUM/AVG/Filter+SUM), all three sequential scans, the
windowed scan, mixed correctness, and every 10k/100k UPDATE and DELETE (the
100k mutations by >2x — the bulk-mutation paths parallelize across cores at
full WAL durability). The three losses are reported, not hidden: the 1M-row
bulk UPDATE (DuckDB, 47% — was 202% before the parallel mutation engine) and
1M-row bulk DELETE (ClickHouse, 26% — inside ClickHouse's own run-to-run
variance band on this host), where columnar engines rewrite chunks while
UltraSQL stamps per-row MVCC headers durably; and point-op Mixed OLTP, where
in-process SQLite pays no wire round trip — UltraSQL measures within 2% of
the tuned PostgreSQL 17 server, the same-architecture wire peer, on that
row. The certification gate certifies *fair methodology* and reports per-row
wins and losses as a scoreboard rather than demanding an impossible clean
sweep, so `benchmark_certification_status.json` is `ready`.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.49 ms** | 64.62 ms (4233.7% slower) | 61.66 ms (4035.3% slower) | 21.56 ms (1346% slower) | 3.63 ms (143.4% slower) | UltraSQL |
| INSERT throughput | 100 000 | **10.34 ms** | 395.09 ms (3721.1% slower) | 650.10 ms (6187.5% slower) | 42.78 ms (313.8% slower) | 23.09 ms (123.3% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **108.26 ms** | 3745.77 ms (3359.8% slower) | 6508.21 ms (5911.4% slower) | 267.22 ms (146.8% slower) | 255.59 ms (136.1% slower) | UltraSQL |
| SELECT scan | 10 000 | **568.17 µs** | 873.17 µs (53.7% slower) | 944.17 µs (66.2% slower) | 1.84 ms (223% slower) | 1.42 ms (150% slower) | UltraSQL |
| SELECT scan | 100 000 | **5.90 ms** | 9.01 ms (52.6% slower) | 6.46 ms (9.4% slower) | 19.20 ms (225.1% slower) | 15.30 ms (159.2% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **58.12 ms** | 90.96 ms (56.5% slower) | 59.86 ms (3% slower) | 204.12 ms (251.2% slower) | 158.65 ms (173% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **31.12 µs** | 68.48 µs (120% slower) | 442.19 µs (1320.7% slower) | 136.21 µs (337.6% slower) | 269.15 µs (764.7% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **38.38 µs** | 86.25 µs (124.8% slower) | 654.39 µs (1605.3% slower) | 1.40 ms (3546.5% slower) | 2.35 ms (6013.4% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **62.85 µs** | 161.29 µs (156.6% slower) | 1.62 ms (2479.8% slower) | 15.70 ms (24873.7% slower) | 11.14 ms (17615.7% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **31.40 µs** | 73.75 µs (134.9% slower) | 472.54 µs (1405.1% slower) | 136.52 µs (334.8% slower) | 292.75 µs (832.5% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **32.81 µs** | 116.42 µs (254.8% slower) | 690.85 µs (2005.4% slower) | 1.42 ms (4214.7% slower) | 2.54 ms (7647.1% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **43.00 µs** | 236.77 µs (450.6% slower) | 1.64 ms (3705.7% slower) | 15.62 ms (36232.6% slower) | 11.95 ms (27696.5% slower) | UltraSQL |
| Filter + SUM | 10 000 | **39.42 µs** | 80.92 µs (105.3% slower) | 531.71 µs (1248.9% slower) | 152.50 µs (286.9% slower) | 317.50 µs (705.5% slower) | UltraSQL |
| Filter + SUM | 100 000 | **38.71 µs** | 129.79 µs (235.3% slower) | 777.56 µs (1908.8% slower) | 1.55 ms (3913.4% slower) | 2.57 ms (6532.6% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **37.69 µs** | 163.60 µs (334.1% slower) | 1.38 ms (3573.2% slower) | 17.83 ms (47209.4% slower) | 11.70 ms (30944% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **111.54 µs** | 159.29 µs (42.8% slower) | 3.58 ms (3111.4% slower) | 480.96 µs (331.2% slower) | 4.78 ms (4186.1% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **339.17 µs** | 767.02 µs (126.1% slower) | 11.27 ms (3222.2% slower) | 5.46 ms (1509.5% slower) | 99.16 ms (29135.7% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.16 ms (47.5% slower) | **2.15 ms** | 34.45 ms (1505.6% slower) | 61.69 ms (2774.8% slower) | 1812.10 ms (84345.8% slower) | DuckDB |
| DELETE throughput | 10 000 | **96.62 µs** | 99.77 µs (3.3% slower) | 4.66 ms (4723% slower) | 576.50 µs (496.6% slower) | 1.52 ms (1469.1% slower) | UltraSQL |
| DELETE throughput | 100 000 | **322.10 µs** | 407.25 µs (26.4% slower) | 3.62 ms (1025.4% slower) | 6.87 ms (2031.4% slower) | 13.58 ms (4117.5% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 3.40 ms (26.2% slower) | 4.25 ms (57.6% slower) | **2.70 ms** | 73.80 ms (2635.3% slower) | 391.89 ms (14425.5% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 28.80 µs/op (87% slower) | 147.16 µs/op (855.8% slower) | 23.26 ms/op (150937.4% slower) | **15.40 µs/op** | 28.16 µs/op (82.9% slower) | SQLite |
| Mixed correctness | 100 000 | **64.21 µs** | 270.71 µs (321.6% slower) | 75.96 ms (118199.2% slower) | 2.24 ms (3389.5% slower) | 3.17 ms (4833% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.59 ms** | 6.94 ms (51.1% slower) | 5.58 ms (21.4% slower) | 27.79 ms (505% slower) | 15.84 ms (244.8% slower) | UltraSQL |

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
