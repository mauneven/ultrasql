<div align="center">

# UltraSQL

**A hardware-aware, PostgreSQL-compatible SQL engine. Written in Rust.**

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

[Architecture](ARCHITECTURE.md) ·
[Performance](PERFORMANCE.md) ·
[Benchmarks](BENCHMARKS.md) ·
[Roadmap](ROADMAP.md) ·
[Contributing](CONTRIBUTING.md) ·
[RFC Process](RFC_PROCESS.md)

</div>

---

## What is UltraSQL?

UltraSQL is a from-scratch open-source SQL database engine designed for the
hardware reality of 2026 and beyond: many cores, deep cache hierarchies,
wide SIMD units, NVMe storage with microsecond latencies, and predominantly
ARM64 server fleets.

The engineering thesis is simple. PostgreSQL is excellent. Its on-disk
formats, MVCC semantics, wire protocol, and SQL dialect are battle-tested
across three decades of production. But its execution model — process per
connection, tuple-at-a-time iterators, B-link-tree-only indexing, single
backend per query — predates the architectural shifts that define modern
servers. UltraSQL keeps PostgreSQL's contracts (wire protocol, SQL surface,
MVCC semantics, ACID guarantees) and rewrites the engine underneath them
with first-principles attention to:

- **Concurrency.** A thread-per-core async runtime, lock-free shared
  structures where they pay for themselves, fine-grained latches everywhere
  else, and an explicit cost model for synchronization.
- **Memory.** Column-oriented in-memory format for OLAP paths, row-oriented
  for OLTP paths, with a planner that chooses between them per pipeline
  segment. Buffer pool sized for the unified memory hierarchy on Apple
  Silicon and the NUMA hierarchy on commodity servers.
- **SIMD.** NEON on ARM64, AVX2 / AVX-512 on x86_64. Kernels are written
  once against a portable vector abstraction and lowered per target.
- **I/O.** `io_uring` on Linux, kqueue on macOS, with batched group commit
  for WAL. Direct I/O when the host supports it; mmap-with-prefetch when
  it does not.
- **Hybrid workloads.** A single engine for OLTP point queries and OLAP
  scans, not two engines glued together. The planner picks pipelines and
  formats; the executor handles both.

## Status

UltraSQL is in pre-alpha. Layered subsystems land in order: foundational
types → storage primitives → WAL → MVCC → parser → planner → executor →
wire protocol. The first milestone (`v0.1`) is a single-node, single-writer
engine that speaks the PostgreSQL wire protocol and passes a curated subset
of the PostgreSQL regression suite.

See [ROADMAP.md](ROADMAP.md) for the full plan. See
[ARCHITECTURE.md](ARCHITECTURE.md) for how the pieces fit together.

## Quick start

Prerequisites: Rust 1.85+ (the workspace pins a stable channel via
`rust-toolchain.toml`).

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql

cargo build --release
cargo test --workspace
cargo run --release --bin ultrasqld -- --help
```

On Apple Silicon the build uses `target-cpu=apple-m1`, which enables NEON,
fp16, dotprod, and the LSE atomics that UltraSQL relies on for lock-free
fast paths.

## Comparison

UltraSQL competes with — and learns from — every major SQL engine. The
honest summary:

|                        | UltraSQL          | PostgreSQL | MySQL  | SQLite | DuckDB | ClickHouse |
| ---------------------- | ----------------- | ---------- | ------ | ------ | ------ | ---------- |
| OLTP transactions      | Target            | Strong     | Strong | Single-writer | No  | No        |
| OLAP scans             | Target            | Adequate   | Weak   | Weak   | Strong | Strong     |
| MVCC                   | Yes               | Yes        | Yes (InnoDB) | No | No  | No         |
| PostgreSQL wire        | Yes               | Native     | No     | No     | No     | No         |
| Vectorized execution   | Yes               | Partial    | No     | No     | Yes    | Yes        |
| SIMD kernels (NEON+x86)| Yes               | No         | No     | No     | Yes    | Yes        |
| Async runtime          | Tokio             | Process    | Thread | Thread | Thread | Thread     |
| In-memory format       | Hybrid row/col    | Row        | Row    | Row    | Column | Column     |
| Implementation lang    | Rust              | C          | C++    | C      | C++    | C++        |

This table is forward-looking. The "Target" cells are honest commitments
captured in [ROADMAP.md](ROADMAP.md); UltraSQL has not shipped them yet.

## Benchmarks

UltraSQL takes performance claims seriously. The repository contains an
automated benchmark harness ([`crates/ultrasql-bench`](crates/ultrasql-bench))
and a reproducibility methodology ([BENCHMARKS.md](BENCHMARKS.md)).

Three rules:

1. **No claim without commit.** Every published number traces to a commit
   SHA, a host description, and a JSON result file in
   `benchmarks/results/`.
2. **Equal-effort comparisons.** When we benchmark against PostgreSQL,
   MySQL, SQLite, DuckDB, or ClickHouse, we tune the comparison subjects to
   their published best practices. We are not the only experts in the room.
3. **No marketing prose.** We report p50 / p95 / p99 latencies, throughput,
   and resource use. We do not say "blazing fast." We say "47k tx/s at p99
   < 4 ms on M4 Mac mini, 32 connections, TPC-B 1× scale."

## Project structure

```text
ultrasql/
├── Cargo.toml              workspace manifest
├── README.md               this file
├── AGENTS.md               operating manual for development tools and humans
├── ARCHITECTURE.md         subsystem-by-subsystem design
├── PERFORMANCE.md          performance engineering rulebook
├── BENCHMARKS.md           benchmark methodology and reproducibility
├── ROADMAP.md              shipping plan
├── CONTRIBUTING.md         how to contribute
├── SECURITY.md             vulnerability disclosure
├── RFC_PROCESS.md          how design changes land
├── GOVERNANCE.md           project governance
├── crates/
│   ├── ultrasql-core/       foundational types
│   ├── ultrasql-storage/    pages, buffer pool, heap, B+ tree
│   ├── ultrasql-wal/        write-ahead log
│   ├── ultrasql-mvcc/       visibility, snapshots
│   ├── ultrasql-txn/        transaction manager, locking
│   ├── ultrasql-parser/     lexer, parser, AST
│   ├── ultrasql-planner/    binder, logical plans
│   ├── ultrasql-optimizer/  cost-based optimizer
│   ├── ultrasql-executor/   physical execution
│   ├── ultrasql-vec/        vectorized kernels
│   ├── ultrasql-catalog/    system catalog
│   ├── ultrasql-protocol/   PostgreSQL wire protocol v3
│   ├── ultrasql-server/     ultrasqld binary
│   ├── ultrasql-cli/        ultrasql interactive client
│   └── ultrasql-bench/      benchmark harness
├── benchmarks/             reproducible benchmark assets and results
├── docs/                   user-facing documentation
└── .github/workflows/      CI for lint, test, bench, fuzz, sanitizers
```

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first. UltraSQL is an engineering-
serious project. Pull requests must come with tests; performance-sensitive
changes must come with benchmarks. Larger changes go through the
[RFC process](RFC_PROCESS.md).

## License

UltraSQL is dual-licensed under the [Apache License 2.0](LICENSE-APACHE) and
the [MIT License](LICENSE-MIT). Contributions are accepted under both
licenses simultaneously, per [CONTRIBUTING.md](CONTRIBUTING.md).
