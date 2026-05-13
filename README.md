# UltraSQL

PostgreSQL-compatible SQL engine in Rust. Pre-alpha.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

[Architecture](ARCHITECTURE.md) ·
[Performance](PERFORMANCE.md) ·
[Benchmarks](BENCHMARKS.md) ·
[Roadmap](ROADMAP.md) ·
[Contributing](CONTRIBUTING.md) ·
[RFC Process](RFC_PROCESS.md)

---

## Microbenchmarks (Apple M4, 2026-05-12)

Numbers come straight from
[`benchmarks/results/2026-05-12-m4/results.md`](benchmarks/results/2026-05-12-m4/results.md).
Commit `373bb69e3264bca51ed0b1c57c8784109a815d90`, criterion 0.5, median of
100 samples per row.

| Bench                                  | Input        | Median  | Throughput        |
| -------------------------------------- | ------------ | ------: | ----------------- |
| `vec/sum_i64`                          | 65 536 i64s  | 4.70 µs | 13.94 Gelem/s     |
| `vec/eq_i32`                           | 65 536 i32s  | 58.4 µs | 1.12 Gelem/s      |
| `storage/page/insert` (1 024 B tuples) | fill 1 page  |  203 ns | 4.69 GiB/s        |
| `storage/buffer_pool/hot_pin`          | 1 page       | 12.7 ns | 79.0 Mops/s       |
| `wal/encode` (4 096 B payload)         | 1 record     |  107 ns | 35.6 GiB/s        |
| `wal/buffer_append` (1 024 B record)   | 1 record     | 96.1 ns | 9.92 GiB/s        |

Single-thread microbenchmarks on Apple M4 Mac mini, release+LTO. End-to-end
query-engine benchmarks (TPC-B, TPC-C, TPC-H) blocked on v0.5 (see
[ROADMAP.md](ROADMAP.md)).

---

## What this is

UltraSQL is a from-scratch PostgreSQL-compatible OLTP+OLAP engine in pure
Rust. Drop-in replacement target for PostgreSQL: same wire protocol v3, same
SQL dialect, MVCC semantics. Single-node single-writer engine in pre-alpha;
v0.1 milestone is a server that speaks the PG wire and passes a curated
subset of the PostgreSQL regression suite.

## Status

Pre-alpha. The workspace compiles. CI runs fmt-check, clippy, and tests on
Linux x86_64, Linux ARM64, and macOS ARM64.

Shipped:

- Cargo workspace, MSRV pin, dual-license, contributor docs.
- Crate skeletons for every planned subsystem (see [Project structure](#project-structure)).
- Foundational types in `ultrasql-core`: errors, OIDs, datums, schema.
- PostgreSQL-token-set lexer in `ultrasql-parser`.
- 8 KiB slotted page format with checksums.
- Buffer pool (CLOCK-Pro) with sharded page table — hot-pin and cycle
  benches running.
- WAL record encode/decode and in-memory `WalBuffer::append`.
- Vectorized kernels: `sum_i64`, `eq_i32`, `min_f64`, `select_i32`.

Scaffolded but not implemented:

- Parser beyond the lexer.
- Binder, planner, optimizer.
- Executor (no `next_batch()` calls anywhere).
- MVCC, transaction manager, lock manager.
- Wire protocol v3 — only the crate exists.
- Catalog, server binary, CLI.
- Segment-file I/O and WAL fsync path.

See [ROADMAP.md](ROADMAP.md) for the version-by-version plan.

## Quick start

Prerequisites: Rust 1.85+ (the workspace pins via `rust-toolchain.toml`).

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql

cargo build --release
cargo test --workspace
cargo bench --workspace        # criterion microbenchmarks
```

There is no working server binary yet. `cargo run --bin ultrasqld` will
build but exits immediately.

On Apple Silicon the build uses `target-cpu=apple-m1`, which enables ARM64
SIMD, fp16, dotprod, and the LSE atomics the buffer pool uses for its
lock-free fast paths.

## Comparison matrix

Forward-looking. "Target" cells are roadmap commitments; UltraSQL does not
ship them today. The non-Target cells describe the named engines.

|                              | UltraSQL          | PostgreSQL | MySQL  | SQLite | DuckDB | ClickHouse |
| ---------------------------- | ----------------- | ---------- | ------ | ------ | ------ | ---------- |
| OLTP transactions            | Target            | Strong     | Strong | Single-writer | No  | No        |
| OLAP scans                   | Target            | Adequate   | Weak   | Weak   | Strong | Strong     |
| MVCC                         | Yes               | Yes        | Yes (InnoDB) | No | No  | No         |
| PostgreSQL wire              | Target            | Native     | No     | No     | No     | No         |
| Vectorized execution         | Target            | Partial    | No     | No     | Yes    | Yes        |
| SIMD kernels (ARM64+x86)     | Partial (vec)     | No         | No     | No     | Yes    | Yes        |
| Async runtime                | Tokio             | Process    | Thread | Thread | Thread | Thread     |
| In-memory format             | Hybrid row/col    | Row        | Row    | Row    | Column | Column     |
| Implementation language      | Rust              | C          | C++    | C      | C++    | C++        |

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

## Links

- Architecture: [ARCHITECTURE.md](ARCHITECTURE.md)
- Performance rules: [PERFORMANCE.md](PERFORMANCE.md)
- Benchmark methodology: [BENCHMARKS.md](BENCHMARKS.md)
- Latest microbench results: [benchmarks/results/2026-05-12-m4/results.md](benchmarks/results/2026-05-12-m4/results.md)
- Roadmap: [ROADMAP.md](ROADMAP.md)
- RFC process: [RFC_PROCESS.md](RFC_PROCESS.md)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first. PRs must come with tests;
changes to benchmarked paths come with before/after numbers in the PR
description. Larger changes go through the [RFC process](RFC_PROCESS.md).

## License

UltraSQL is dual-licensed under the [Apache License 2.0](LICENSE-APACHE) and
the [MIT License](LICENSE-MIT). Contributions are accepted under both
licenses simultaneously, per [CONTRIBUTING.md](CONTRIBUTING.md).
