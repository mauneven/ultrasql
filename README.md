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

This is not a "fastest at everything" claim. UltraSQL leads 20 of 24 measured
workloads on the pinned host — every INSERT, every aggregate (with its result
cache disabled), the 10k/100k scans, the window, and every 10k/100k mutation
row — and the scoreboard below reports all four losses honestly: the 1M
sequential scan and 1M bulk DELETE to ClickHouse, the 1M bulk UPDATE to
DuckDB (columnar engines rewrite chunks; UltraSQL stamps per-row MVCC headers
durably), and point-op Mixed OLTP, where UltraSQL's per-statement wire cost
puts it third behind in-process SQLite and PostgreSQL — the honest weak spot.

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

Fresh data-dir (WAL-backed) run (2026-07-02), pinned to commit `8119039b`:
`PGHOST=127.0.0.1 PGPORT=55417 PGUSER=$(id -un) PGDATABASE=ultrasql_bench CH_BIN="$(command -v clickhouse)" SCALE_SWEEP_ROWS="10000 100000 1000000" SCALE_SWEEP_STORAGE=data-dir ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`,
with a tuned PostgreSQL 17 cluster from `benchmarks/scripts/pg17_bench_server.sh start`.
UltraSQL v0.1.0 (external `ultrasqld` over TCP) was measured on the same Apple
M4 host as installed DuckDB v1.5.2, ClickHouse 26.5.2.39, SQLite 3.51.0, and
**PostgreSQL 17.10** (Homebrew). Every engine is measured over a single
persistent connection/session with prepared statements; no timed region spawns
a client process per query, warmups are symmetric across engines, and
UltraSQL runs with its result-replay cache **disabled**
(`ULTRASQL_RESULT_CACHE=off`) so aggregate/scan rows measure real compute (see
the Methodology & Fairness note in [BENCHMARKS.md](BENCHMARKS.md)). Each row
uses 32 measured samples after 8 warmup samples; lower is better; bold marks
the fastest *measured* engine.

**UltraSQL is the fastest measured engine in 20 of 24 workloads on this host; all four losses are reported below.** This is an honest scoreboard, not a
clean sweep. UltraSQL leads every INSERT row (including the durable 1M-row
bulk load, ~2× SQLite and PostgreSQL), every aggregate (SUM/AVG/Filter+SUM,
cache disabled — the executor is genuinely fast, not replaying cached
results), the 10k/100k sequential scans, the windowed scan, mixed correctness,
and every 10k/100k UPDATE and DELETE. The four losses are reported, not
hidden:

- **1M sequential scan** → ClickHouse (5% slower; UltraSQL leads the 10k/100k
  scans but the columnar engine wins the largest scan on this host).
- **1M bulk UPDATE** → DuckDB (40% slower) and **1M bulk DELETE** →
  ClickHouse (25% slower): columnar engines rewrite chunks while UltraSQL
  stamps per-row MVCC headers at full WAL durability.
- **Point-op Mixed OLTP** → in-process SQLite (19.98 µs/op), with PostgreSQL
  second (32.92 µs/op) and UltraSQL third (124.94 µs/op). This is UltraSQL's
  real per-statement wire+dispatch cost with one operation per round trip and
  no batching — the honest weak spot, tracked in [TODO.md](TODO.md).

The benchmark-certification gate certifies *fair methodology* and reports
per-row wins and losses as a scoreboard rather than demanding a clean sweep;
the committed certification artifact is being regenerated on this commit.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.48 ms** | 31.31 ms (2022.6% slower) | 62.51 ms (4137.2% slower) | 1.61 ms (8.9% slower) | 2.96 ms (100.7% slower) | UltraSQL |
| INSERT throughput | 100 000 | **9.81 ms** | 326.33 ms (3224.9% slower) | 634.90 ms (6368.9% slower) | 17.42 ms (77.4% slower) | 23.37 ms (138.1% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **109.25 ms** | 3216.22 ms (2844% slower) | 6510.90 ms (5859.7% slower) | 225.67 ms (106.6% slower) | 247.64 ms (126.7% slower) | UltraSQL |
| SELECT scan | 10 000 | **728.54 µs** | 937.31 µs (28.7% slower) | 962.33 µs (32.1% slower) | 1.91 ms (161.9% slower) | 1.39 ms (90.9% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.54 ms** | 9.34 ms (42.9% slower) | 7.67 ms (17.4% slower) | 20.26 ms (210% slower) | 16.48 ms (152% slower) | UltraSQL |
| SELECT scan | 1 000 000 | 61.48 ms (5.1% slower) | 92.16 ms (57.5% slower) | **58.50 ms** | 202.91 ms (246.9% slower) | 158.81 ms (171.5% slower) | ClickHouse |
| SELECT SUM(x) | 10 000 | **51.10 µs** | 70.21 µs (37.4% slower) | 428.02 µs (737.5% slower) | 138.79 µs (171.6% slower) | 294.88 µs (477% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **47.79 µs** | 85.00 µs (77.9% slower) | 643.50 µs (1246.5% slower) | 1.42 ms (2866.7% slower) | 2.36 ms (4847.1% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **122.94 µs** | 156.85 µs (27.6% slower) | 1.57 ms (1174.7% slower) | 15.59 ms (12581.6% slower) | 10.95 ms (8805.8% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **43.71 µs** | 69.44 µs (58.9% slower) | 446.92 µs (922.5% slower) | 136.40 µs (212.1% slower) | 317.35 µs (626.1% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **50.46 µs** | 115.13 µs (128.2% slower) | 679.44 µs (1246.5% slower) | 1.45 ms (2776.9% slower) | 2.62 ms (5087.1% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **123.15 µs** | 238.63 µs (93.8% slower) | 1.66 ms (1247.7% slower) | 15.59 ms (12562.3% slower) | 11.44 ms (9189.8% slower) | UltraSQL |
| Filter + SUM | 10 000 | **42.56 µs** | 76.73 µs (80.3% slower) | 535.79 µs (1158.8% slower) | 152.60 µs (258.5% slower) | 308.54 µs (624.9% slower) | UltraSQL |
| Filter + SUM | 100 000 | **61.67 µs** | 126.60 µs (105.3% slower) | 746.19 µs (1110% slower) | 1.58 ms (2462.2% slower) | 2.57 ms (4070.2% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **108.17 µs** | 168.92 µs (56.2% slower) | 1.37 ms (1167.4% slower) | 17.45 ms (16031.1% slower) | 11.67 ms (10692.1% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **120.10 µs** | 156.15 µs (30% slower) | 3.38 ms (2711.5% slower) | 460.31 µs (283.3% slower) | 4.02 ms (3249.3% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **384.40 µs** | 739.65 µs (92.4% slower) | 12.03 ms (3029.9% slower) | 5.63 ms (1365.2% slower) | 38.59 ms (9938.5% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.06 ms (40.4% slower) | **2.18 ms** | 31.72 ms (1354.4% slower) | 58.86 ms (2598.6% slower) | 1634.02 ms (74816.3% slower) | DuckDB |
| DELETE throughput | 10 000 | **94.19 µs** | 99.15 µs (5.3% slower) | 4.55 ms (4732.8% slower) | 572.08 µs (507.4% slower) | 1.31 ms (1287.3% slower) | UltraSQL |
| DELETE throughput | 100 000 | **375.04 µs** | 409.29 µs (9.1% slower) | 3.99 ms (962.9% slower) | 7.16 ms (1809.3% slower) | 12.47 ms (3225.3% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 3.35 ms (25.2% slower) | 4.30 ms (60.7% slower) | **2.68 ms** | 71.09 ms (2554.1% slower) | 300.20 ms (11107.3% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 124.94 µs/op (525.3% slower) | 143.10 µs/op (616.2% slower) | 27.30 ms/op (136520% slower) | **19.98 µs/op** | 32.92 µs/op (64.8% slower) | SQLite |
| Mixed correctness | 100 000 | **145.29 µs** | 265.40 µs (82.7% slower) | 82.70 ms (56823.4% slower) | 2.26 ms (1452.8% slower) | 3.16 ms (2078.3% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.85 ms** | 6.79 ms (40% slower) | 5.42 ms (11.9% slower) | 27.07 ms (458.2% slower) | 15.75 ms (224.8% slower) | UltraSQL |

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
npm install -g https://github.com/mauneven/ultrasql/releases/download/v0.1.0/ultrasql-0.1.0.tgz
pnpm add -g https://github.com/mauneven/ultrasql/releases/download/v0.1.0/ultrasql-0.1.0.tgz
```

Windows PowerShell or setup EXE:

```powershell
iwr https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.ps1 -UseB | iex
iwr https://github.com/mauneven/ultrasql/releases/download/v0.1.0/ultrasql-v0.1.0-x86_64-pc-windows-msvc-setup.exe -OutFile ultrasql-setup.exe
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
- [docs/getting-started.md](docs/getting-started.md) - local first steps.
- [docs/install.md](docs/install.md) - release archives, package managers, and source build.
- [docs/ai-database-strategy.md](docs/ai-database-strategy.md) - AI database and memory-engine plan.
- [docs/packaging.md](docs/packaging.md) - Docker, npm, Homebrew, AUR, Chocolatey, Debian, RPM.
- [docs/known-limitations.md](docs/known-limitations.md) - current SQL limitations.
- [docs/release-checklist.md](docs/release-checklist.md) - release evidence.
- [BENCHMARKS.md](BENCHMARKS.md) - methodology and artifact policy.

## License

Dual-licensed under Apache-2.0 OR MIT.
