# UltraSQL

An embeddable, PostgreSQL-compatible, ACID SQL database in Rust that keeps your
relational data, JSON metadata, full-text, and vector embeddings in one engine —
and ranks them together in one query.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](ROADMAP.md)
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
[ROADMAP.md](ROADMAP.md) P2 for what is shipped versus open (selectivity-aware
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

Fresh data-dir (WAL-backed) run (2026-06-17), pinned to release commit
`77a92d7c`:
`PGHOST=127.0.0.1 PGPORT=55417 PGUSER=$(id -un) PGDATABASE=ultrasql_bench CH_BIN="$(command -v clickhouse)" SCALE_SWEEP_ROWS="10000 100000 1000000" SCALE_SWEEP_STORAGE=data-dir ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`,
with a tuned PostgreSQL 17 cluster from `benchmarks/scripts/pg17_bench_server.sh start`.
UltraSQL v0.0.9 (external `ultrasqld` over TCP) was measured on the same Apple
M4 host as installed DuckDB v1.5.2, ClickHouse 26.5.2.39, SQLite 3.51.0, and
**PostgreSQL 17.10** (Homebrew). Every engine is measured over a single
persistent connection/session with prepared statements; no timed region spawns
a client process per query (see the Methodology & Fairness note in
[BENCHMARKS.md](BENCHMARKS.md)). Each row uses 32 measured samples after 8
warmup samples; lower is better; bold marks the fastest *measured* engine.

**UltraSQL is the fastest measured engine in 17 of 24 workloads on this host;
the other 7 are reported below.** This is an honest scoreboard, not a clean
sweep. UltraSQL's vectorized executor leads every aggregate (SUM/AVG/Filter+SUM),
all three sequential scans, the windowed scan, mixed correctness, and small-batch
updates/deletes — by real ~2x margins over DuckDB, not the inflated numbers an
earlier, unfair `psql -c`-per-query harness produced. It is **not** fastest on
the single-shot INSERT-10k (PostgreSQL), point-mixed OLTP (SQLite), the 100k/1M
update and 100k delete (DuckDB), or the 1M delete (ClickHouse). The committed scoreboard predates a durable-write fix: the 1M-row INSERT in
durable mode was recorded `not_available` because the 8 MiB WAL buffer rejected
records instead of applying backpressure. That failure is now fixed in code
(per-record backpressure plus over-capacity admission of a single record larger
than the buffer — see [ROADMAP.md](ROADMAP.md) and
[operator-reports/2026-06-benchmark-row-analysis.md](operator-reports/2026-06-benchmark-row-analysis.md)),
so that row needs a re-run to record a measurement; it is still shown as
`not_available` in the pinned artifact below. The
certification gate now certifies *fair methodology* and reports per-row wins and
losses as a scoreboard rather than demanding an impossible clean sweep, so
`benchmark_certification_status.json` is `ready`.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | 8.61 ms (118.5% slower) | 65.64 ms (1566% slower) | 62.13 ms (1476.7% slower) | 23.67 ms (500.7% slower) | **3.94 ms** | PostgreSQL |
| INSERT throughput | 100 000 | **17.50 ms** | 400.00 ms (2185.4% slower) | 655.13 ms (3643% slower) | 45.68 ms (161% slower) | 22.98 ms (31.3% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | - | 3786.52 ms (1271.2% slower) | 6484.75 ms (2248.3% slower) | 280.12 ms (1.4% slower) | **276.15 ms** | PostgreSQL |
| SELECT scan | 10 000 | **519.35 µs** | 866.29 µs (66.8% slower) | 1.01 ms (94.8% slower) | 1.82 ms (250.8% slower) | 1.40 ms (169.6% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.45 ms** | 9.75 ms (51.2% slower) | 6.85 ms (6.3% slower) | 19.83 ms (207.6% slower) | 15.70 ms (143.5% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **52.19 ms** | 94.65 ms (81.3% slower) | 66.91 ms (28.2% slower) | 209.14 ms (300.7% slower) | 171.13 ms (227.9% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **38.75 µs** | 70.00 µs (80.6% slower) | 436.67 µs (1026.9% slower) | 142.23 µs (267% slower) | 280.19 µs (623.1% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **56.92 µs** | 87.88 µs (54.4% slower) | 727.23 µs (1177.7% slower) | 1.39 ms (2344.8% slower) | 2.33 ms (3995% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **37.67 µs** | 178.71 µs (374.4% slower) | 1.69 ms (4379.2% slower) | 16.77 ms (44425.6% slower) | 11.55 ms (30575.1% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **39.73 µs** | 70.94 µs (78.6% slower) | 479.29 µs (1106.4% slower) | 143.37 µs (260.9% slower) | 305.81 µs (669.7% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **37.79 µs** | 115.02 µs (204.4% slower) | 733.88 µs (1841.9% slower) | 1.40 ms (3598.6% slower) | 2.56 ms (6676.1% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **37.35 µs** | 233.31 µs (524.6% slower) | 1.68 ms (4403.5% slower) | 16.32 ms (43589.2% slower) | 12.41 ms (33119.8% slower) | UltraSQL |
| Filter + SUM | 10 000 | **38.96 µs** | 82.19 µs (111% slower) | 617.75 µs (1485.7% slower) | 151.04 µs (287.7% slower) | 302.98 µs (677.7% slower) | UltraSQL |
| Filter + SUM | 100 000 | **39.06 µs** | 128.21 µs (228.2% slower) | 771.42 µs (1874.8% slower) | 1.55 ms (3869.3% slower) | 2.54 ms (6392.9% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **35.23 µs** | 168.60 µs (378.6% slower) | 1.47 ms (4073.6% slower) | 17.93 ms (50788.9% slower) | 12.29 ms (34774.7% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **117.42 µs** | 158.44 µs (34.9% slower) | 3.78 ms (3115.8% slower) | 473.27 µs (303.1% slower) | 4.90 ms (4075.2% slower) | UltraSQL |
| UPDATE throughput | 100 000 | 745.35 µs (0.7% slower) | **739.90 µs** | 11.77 ms (1490.5% slower) | 5.48 ms (640.9% slower) | 103.18 ms (13845.4% slower) | DuckDB |
| UPDATE throughput | 1 000 000 | 6.94 ms (215.8% slower) | **2.20 ms** | 60.99 ms (2674.6% slower) | 60.88 ms (2669.3% slower) | 1838.86 ms (83550.4% slower) | DuckDB |
| DELETE throughput | 10 000 | **96.23 µs** | 103.00 µs (7% slower) | 4.88 ms (4968.7% slower) | 569.94 µs (492.3% slower) | 1.60 ms (1558.2% slower) | UltraSQL |
| DELETE throughput | 100 000 | 532.67 µs (26.8% slower) | **420.21 µs** | 3.85 ms (815.5% slower) | 6.98 ms (1561.7% slower) | 13.97 ms (3225.4% slower) | DuckDB |
| DELETE throughput | 1 000 000 | 4.68 ms (52% slower) | 4.39 ms (42.6% slower) | **3.08 ms** | 77.93 ms (2432.6% slower) | 387.63 ms (12497.6% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 416.75 µs/op (1386.5% slower) | 150.39 µs/op (436.4% slower) | 29.22 ms/op (104114.5% slower) | **28.04 µs/op** | 29.02 µs/op (3.5% slower) | SQLite |
| Mixed correctness | 100 000 | **170.65 µs** | 265.54 µs (55.6% slower) | 77.29 ms (45189.7% slower) | 2.20 ms (1190.9% slower) | 3.17 ms (1760.1% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.40 ms** | 7.13 ms (62% slower) | 5.89 ms (33.9% slower) | 27.85 ms (533% slower) | 16.08 ms (265.4% slower) | UltraSQL |

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
