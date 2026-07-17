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

This is not a universal performance claim. In the one committed scale sweep
pinned to `a6a97af1`, UltraSQL has the lowest median in 21 of 24 rows — every
INSERT, every aggregate (with its result cache disabled), every scan, the
window, and every 10k/100k mutation row. The same artifact reports all three
losses: 1M bulk DELETE to ClickHouse, 1M bulk UPDATE to DuckDB, and point-op
Mixed OLTP to in-process SQLite (with PostgreSQL second). The 1M sequential
scan differs from ClickHouse by about 2% in that sweep; one run does not
establish repeat-run parity.

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
the same host, raw measurements, the per-row lowest median, and the slower
percentage for every other measured engine.

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
Alpha scope, quickstart, and the public-beta readiness assessment:
[`BETA_READINESS.md`](BETA_READINESS.md).

## Release-Artifact End-to-End Benchmark

One committed data-dir (WAL-backed) full sweep (2026-07-02), pinned to artifact
commit `a6a97af1`:
`PGHOST=127.0.0.1 PGPORT=55417 PGUSER=$(id -un) PGDATABASE=ultrasql_bench CH_BIN="$(command -v clickhouse)" SCALE_SWEEP_ROWS="10000 100000 1000000" SCALE_SWEEP_STORAGE=data-dir ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/run_scale_sweep.sh full`,
with a tuned PostgreSQL 17 cluster from `benchmarks/scripts/pg17_bench_server.sh start`.
UltraSQL v0.1.0 (external `ultrasqld` over TCP) was measured on the same Apple
M4 host as installed DuckDB v1.5.2, ClickHouse 26.5.2.39, SQLite 3.51.0, and
**PostgreSQL 17.10** (Homebrew). UltraSQL and PostgreSQL use external wire
servers, SQLite and DuckDB use embedded Python drivers, and ClickHouse uses its
native TCP driver. Connection/process reuse, physical schemas, bulk APIs,
result draining, and effective warmup differ by workload; ClickHouse also runs
with `fsync_after_insert=0`. UltraSQL runs with its result-replay cache
**disabled**
(`ULTRASQL_RESULT_CACHE=off`) so aggregate/scan rows measure real compute (see
the disclosed implementation differences in [BENCHMARKS.md](BENCHMARKS.md)).
The sweep requests 8 warmups and records 32 measured samples, but a manifest
warmup value does not prove that every competitor runner consumed it. Lower is
better; bold marks the lowest median among available implementations for that
exact row and host. The table and raw samples under
`benchmarks/results/latest/scale-sweep/raw/` describe this one committed
contention-free sweep; no median-across-sweeps or two-of-three stability claim
is made.

The artifact records UltraSQL as the lowest-median implementation in 21 of 24
rows on this host. It records the lowest median for every INSERT, aggregate
(SUM/AVG/Filter+SUM, **cache disabled**), sequential scan, the windowed scan,
mixed correctness, and every 10k/100k UPDATE and DELETE row. The three losses
are reported in the same table:

- **1M bulk UPDATE** → DuckDB (19% slower) and **1M bulk DELETE** →
  ClickHouse (32% slower): the recorded columnar implementations rewrite
  chunks while UltraSQL's row-store path stamps per-row MVCC headers. For these
  mutation rows, the timer covers the UPDATE or DELETE statement inside a
  transaction; the outer `ROLLBACK` is outside the timer, so the values are not
  commit/fsync latency measurements. Durability settings are also not
  identical; see [BENCHMARKS.md](BENCHMARKS.md).
- **Point-op Mixed OLTP** → in-process SQLite (16.30 µs/op), with PostgreSQL
  second (34.20 µs/op) and UltraSQL third (130.19 µs/op). This is UltraSQL's
  real per-statement wire+dispatch cost with one operation per round trip and
  no batching. SQLite's number is an embedded driver call with no network
  round trip, so this row is an end-to-end implementation comparison, not an
  isolated engine-operator ranking; it is tracked in [TODO.md](TODO.md).

The 1M sequential-scan medians in this sweep are UltraSQL 59.92 ms and
ClickHouse 61.15 ms, a 2.1% difference; repeat-run stability has not been
established. The benchmark-certification gate certifies artifact integrity,
raw/rendered consistency, and provenance—not symmetric or fair methodology—and
reports per-row wins and losses as a scoreboard rather than demanding a clean
sweep. `benchmark_certification_status.json` is `ready` for pinned artifact
commit `a6a97af1`; that does not make the aggregate release gate ready.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.55 ms** | 32.75 ms (2015% slower) | 60.19 ms (3786.6% slower) | 1.68 ms (8.6% slower) | 3.07 ms (98.3% slower) | UltraSQL |
| INSERT throughput | 100 000 | **10.30 ms** | 319.68 ms (3003.3% slower) | 610.19 ms (5823.5% slower) | 17.28 ms (67.8% slower) | 20.68 ms (100.8% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **110.11 ms** | 3391.86 ms (2980.4% slower) | 6143.87 ms (5479.7% slower) | 240.89 ms (118.8% slower) | 253.81 ms (130.5% slower) | UltraSQL |
| SELECT scan | 10 000 | **692.56 µs** | 885.94 µs (27.9% slower) | 992.85 µs (43.4% slower) | 1.88 ms (170.8% slower) | 1.46 ms (110.6% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.23 ms** | 9.20 ms (47.8% slower) | 6.73 ms (8.1% slower) | 19.71 ms (216.6% slower) | 15.74 ms (152.8% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **59.92 ms** | 96.02 ms (60.2% slower) | 61.15 ms (2.1% slower) | 207.69 ms (246.6% slower) | 162.73 ms (171.6% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **41.35 µs** | 67.02 µs (62.1% slower) | 467.25 µs (1029.9% slower) | 136.79 µs (230.8% slower) | 282.65 µs (583.5% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **53.19 µs** | 87.31 µs (64.2% slower) | 655.77 µs (1132.9% slower) | 1.42 ms (2577.2% slower) | 2.36 ms (4343.4% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **127.02 µs** | 158.08 µs (24.5% slower) | 1.59 ms (1152.8% slower) | 16.12 ms (12594.8% slower) | 11.18 ms (8699.8% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **52.27 µs** | 70.15 µs (34.2% slower) | 442.71 µs (746.9% slower) | 137.04 µs (162.2% slower) | 313.06 µs (498.9% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **62.08 µs** | 113.81 µs (83.3% slower) | 682.67 µs (999.6% slower) | 1.41 ms (2179.1% slower) | 2.58 ms (4060.2% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **126.81 µs** | 224.10 µs (76.7% slower) | 1.58 ms (1143.8% slower) | 15.72 ms (12297.6% slower) | 11.84 ms (9235% slower) | UltraSQL |
| Filter + SUM | 10 000 | **42.69 µs** | 74.71 µs (75% slower) | 523.15 µs (1125.5% slower) | 152.69 µs (257.7% slower) | 312.04 µs (631% slower) | UltraSQL |
| Filter + SUM | 100 000 | **56.48 µs** | 121.25 µs (114.7% slower) | 764.21 µs (1253.1% slower) | 1.56 ms (2658.4% slower) | 2.56 ms (4430% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **137.02 µs** | 168.19 µs (22.7% slower) | 1.38 ms (905.3% slower) | 17.58 ms (12731.8% slower) | 11.78 ms (8500.7% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **120.83 µs** | 157.50 µs (30.3% slower) | 3.99 ms (3204.7% slower) | 483.17 µs (299.9% slower) | 4.02 ms (3229% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **369.54 µs** | 751.60 µs (103.4% slower) | 11.43 ms (2994.3% slower) | 5.50 ms (1388.2% slower) | 38.33 ms (10272.3% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.13 ms (18.9% slower) | **2.63 ms** | 33.68 ms (1179.8% slower) | 59.80 ms (2172.3% slower) | 1643.89 ms (62366.6% slower) | DuckDB |
| DELETE throughput | 10 000 | **97.10 µs** | 113.85 µs (17.3% slower) | 3.21 ms (3208.4% slower) | 592.19 µs (509.8% slower) | 1.36 ms (1297.2% slower) | UltraSQL |
| DELETE throughput | 100 000 | **369.71 µs** | 409.29 µs (10.7% slower) | 3.28 ms (788.1% slower) | 7.13 ms (1827.5% slower) | 12.28 ms (3222.6% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 3.64 ms (32.2% slower) | 4.41 ms (60% slower) | **2.75 ms** | 71.75 ms (2505.3% slower) | 355.62 ms (12813.4% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 130.19 µs/op (698.8% slower) | 145.94 µs/op (795.4% slower) | 26.62 ms/op (163252.6% slower) | **16.30 µs/op** | 34.20 µs/op (109.9% slower) | SQLite |
| Mixed correctness | 100 000 | **138.81 µs** | 266.29 µs (91.8% slower) | 74.05 ms (53243.3% slower) | 2.23 ms (1507.4% slower) | 3.20 ms (2207.4% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.88 ms** | 6.88 ms (41% slower) | 5.74 ms (17.6% slower) | 27.70 ms (467.4% slower) | 16.23 ms (232.4% slower) | UltraSQL |

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
