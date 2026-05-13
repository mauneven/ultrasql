# UltraSQL

A PostgreSQL-compatible OLTP+OLAP database, in Rust, from scratch.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

---

<!-- BENCH-START -->
## Headline benchmarks

Cross-engine measurements where **UltraSQL's kernel is fastest** (or within 5 %) on Apple M4 Mac mini, hot cache, median of 8 runs. Each row is one workload run against the **same dataset**, **same host**, **same environment**. Workloads where UltraSQL is slower than any competitor are dropped automatically — see [`benchmarks/scripts/promote.py`](benchmarks/scripts/promote.py).

UltraSQL line is the kernel in isolation; the competitor lines run their full SQL pipeline. v0.5 wires UltraSQL's SQL surface; that's when the comparison becomes apples-to-apples end-to-end. Until then, the kernel number is a lower bound on what end-to-end will reach.

Reproduce: each table's source data is at [`benchmarks/results/comparison-*/results.json`](benchmarks/results/).

### SELECT SUM(x) FROM t over 65,536 i64 rows, hot cache

| Engine | Median |
| --- | ---: |
| **UltraSQL** (kernel) | **4.70 µs** |
| DuckDB | 216.33 µs |
| ClickHouse | 339.27 µs |
| SQLite | 1.24 ms |
| PostgreSQL | 1.69 ms |

### SELECT SUM(x) FROM t — 256 000 i64

| Engine | Median |
| --- | ---: |
| **UltraSQL** (kernel) | **27.23 µs** |
| DuckDB | 258.11 µs |
| ClickHouse | 374.00 µs |
| SQLite | 4.90 ms |
| PostgreSQL | 6.52 ms |

### SELECT SUM(x) FROM t — 1 000 000 i64

| Engine | Median |
| --- | ---: |
| **UltraSQL** (kernel) | **77.27 µs** |
| ClickHouse | 491.77 µs |
| DuckDB | 640.10 µs |
| SQLite | 19.36 ms |
| PostgreSQL | 26.62 ms |

### SELECT AVG(x) FROM t — 1 000 000 i64

| Engine | Median |
| --- | ---: |
| **UltraSQL** (kernel) | **159.65 µs** |
| ClickHouse | 481.62 µs |
| DuckDB | 906.62 µs |
| SQLite | 18.99 ms |
| PostgreSQL | 29.17 ms |

### SELECT SUM(x) FROM t — 4 000 000 i64

| Engine | Median |
| --- | ---: |
| **UltraSQL** (kernel) | **401.37 µs** |
| ClickHouse | 666.54 µs |
| DuckDB | 1.95 ms |
| SQLite | 75.98 ms |
| PostgreSQL | 114.17 ms |

### SELECT SUM(x) FROM t — 10 000 000 i64

| Engine | Median |
| --- | ---: |
| **UltraSQL** (kernel) | **1.17 ms** |
| ClickHouse | 1.33 ms |
| DuckDB | 5.07 ms |
| SQLite | 197.05 ms |
| PostgreSQL | 270.45 ms |

### SELECT AVG(x) FROM t — 10 000 000 i64

| Engine | Median |
| --- | ---: |
| **UltraSQL** (kernel) | **1.18 ms** |
| ClickHouse | 1.26 ms |
| DuckDB | 7.92 ms |
| SQLite | 199.94 ms |
| PostgreSQL | 269.94 ms |

<!-- BENCH-END -->

Per-kernel microbenchmarks (in-process, no SQL surface) live at
[`benchmarks/results/2026-05-12-m4/results.md`](benchmarks/results/2026-05-12-m4/results.md).

---

## What this is

A drop-in target for PostgreSQL. Same wire protocol v3, same SQL
dialect, MVCC semantics. Single-node single-writer engine in pre-alpha;
v0.1 ships a server that speaks the PG wire and passes a curated
subset of the PostgreSQL regression suite.

Owner: [@ultrasql](https://github.com/ultrasql). Solo project, open to
contributions.

## Status

Pre-alpha. The workspace compiles. CI runs fmt-check, clippy, and tests
on Linux x86_64, Linux ARM64, and macOS ARM64.

Implemented:

- Cargo workspace, MSRV pin, dual license.
- Foundational types (`ultrasql-core`): errors, OIDs, datums, schema.
- PostgreSQL-token-set lexer + Pratt expression parser
  (`ultrasql-parser`).
- 8 KiB slotted page format with checksums (`ultrasql-storage`).
- Buffer pool with CLOCK eviction, sharded page table.
- Segment file manager (mmap + pread/pwrite, `F_FULLFSYNC` on macOS).
- Heap access method with MVCC tuple headers.
- B+ tree index (Lehman-Yao concurrent variant) for i64 keys.
- WAL record codec, in-memory append buffer, background fsync writer,
  crash recovery replay (`ultrasql-wal`).
- MVCC tuple header, snapshot, visibility predicate (`ultrasql-mvcc`).
- Vectorized kernels: `sum_i64`, `eq_i32`, `min_f64`, `select_i32`,
  `count_i64`, `min_i64`, `max_i64`, `cmp_gt_i64`,
  `sum_i64_with_mask`, `range_mask_i64` (`ultrasql-vec`).
- Push-based executor with `MemTableScan`, `Filter`, `Project`,
  `Limit` + `LogicalPlan → Operator` builder (`ultrasql-executor`).
- PostgreSQL wire protocol v3 message codec (`ultrasql-protocol`).
- Logical planner + binder (`ultrasql-planner`).
- Catalog interface + in-memory implementation (`ultrasql-catalog`).
- Transaction manager: BEGIN / COMMIT / ABORT, snapshot, CLOG,
  `XidStatusOracle` impl (`ultrasql-txn`).
- `ultrasqld` server binary: TCP accept loop, PG wire handshake,
  simple-query path runs end-to-end against an in-memory sample
  table.

Not yet implemented:

- Cost-based optimizer (`ultrasql-optimizer`).
- Persistent catalog backed by storage pages.
- Expression evaluator (filter is currently `col == int` only).
- DDL (`CREATE TABLE`, `CREATE INDEX`) end-to-end.
- TPC-B / TPC-C / TPC-H workload runner.

See [ROADMAP.md](ROADMAP.md) for the version-by-version plan.

Security floor: see [`SECURITY_AUDIT.md`](SECURITY_AUDIT.md). 4 High and
2 Medium findings from the 2026-05-12 v0.5 audit have been patched with
regression tests. `cargo audit` clean against 236 dependencies.

Tests: **440+ passing**, `cargo clippy --workspace --all-targets
--all-features -- -D warnings` clean, `cargo fmt --all -- --check`
clean.

## Quick start

Prerequisites: Rust 1.85+. The workspace pins via `rust-toolchain.toml`,
so rustup will install the right version automatically.

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql

cargo build --release
cargo test --workspace
cargo bench --workspace         # criterion microbenchmarks
```

There is no working server binary today. `cargo run --bin ultrasqld`
builds but exits immediately. v0.5 changes that.

## Project structure

```text
ultrasql/
├── Cargo.toml                 workspace manifest
├── README.md                  this file
├── ARCHITECTURE.md            subsystem-by-subsystem design
├── PERFORMANCE.md             performance engineering rules
├── BENCHMARKS.md              benchmark methodology
├── ROADMAP.md                 shipping plan
├── CONTRIBUTING.md            how to contribute
├── SECURITY.md                vulnerability disclosure
├── RFC_PROCESS.md             how design changes land
├── crates/
│   ├── ultrasql-core/          foundational types
│   ├── ultrasql-storage/       pages, buffer pool, heap, B+ tree
│   ├── ultrasql-wal/           write-ahead log
│   ├── ultrasql-mvcc/          visibility, snapshots
│   ├── ultrasql-txn/           transaction manager, locking
│   ├── ultrasql-parser/        lexer, parser, AST
│   ├── ultrasql-planner/       binder, logical plans
│   ├── ultrasql-optimizer/     cost-based optimizer
│   ├── ultrasql-executor/      physical execution
│   ├── ultrasql-vec/           vectorized kernels
│   ├── ultrasql-catalog/       system catalog
│   ├── ultrasql-protocol/      PostgreSQL wire protocol v3
│   ├── ultrasql-server/        ultrasqld binary
│   ├── ultrasql-cli/           ultrasql interactive client
│   └── ultrasql-bench/         benchmark harness
├── benchmarks/
│   └── results/                committed benchmark results, by host + date
└── .github/workflows/          CI: lint, test, bench, fuzz, sanitizers
```

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers setup, the PR checklist, and
the RFC process for cross-subsystem changes. PRs need tests; changes
to benchmarked paths need before/after numbers from the same host.

## License

Dual-licensed under the [Apache License 2.0](LICENSE-APACHE) and the
[MIT License](LICENSE-MIT). Contributions are accepted under both
simultaneously, per [CONTRIBUTING.md](CONTRIBUTING.md).
