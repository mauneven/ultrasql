# UltraSQL

A PostgreSQL-compatible OLTP+OLAP database, in Rust, from scratch.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

---

## `SELECT SUM(x) FROM t` over 65,536 i64 rows — Apple M4 Mac mini

| Engine                | Version                  |   Median |
| --------------------- | ------------------------ | -------: |
| **UltraSQL (kernel)** | 0.0.1                    | **4.70 µs** |
| DuckDB                | 1.5.2                    | 216 µs   |
| ClickHouse            | 26.5.1 (official build)  | 339 µs   |
| SQLite                | 3.51.0                   | 1 237 µs |
| PostgreSQL            | 14.22 (Homebrew)         | 1 689 µs |

Same M4 host, same in-memory dataset (SHA-256 `579af385…`), same query,
hot cache, median of 8 runs (criterion 100 samples for UltraSQL).

The UltraSQL row is the `vec/sum_i64` kernel in isolation. The others
include their full SQL pipeline (parse, plan, execute). UltraSQL has no
SQL surface yet — that lands at v0.5. The kernel number is a lower
bound on end-to-end performance, not an end-to-end comparison.

Reproduce: `benchmarks/results/comparison-2026-05-12-m4/run.sh`.
Detail: [`benchmarks/results/comparison-2026-05-12-m4/results.md`](benchmarks/results/comparison-2026-05-12-m4/results.md).

## More microbenchmarks (Apple M4, in-process kernels)

| Bench                                  | Input        | Median  | Throughput     |
| -------------------------------------- | ------------ | ------: | -------------- |
| `vec/sum_i64`                          | 65 536 i64s  | 4.70 µs | 13.94 Gelem/s  |
| `vec/eq_i32`                           | 65 536 i32s  | 58.4 µs | 1.12 Gelem/s   |
| `storage/page/insert` (1 024 B tuples) | fill 1 page  |  203 ns | 4.69 GiB/s     |
| `storage/buffer_pool/hot_pin`          | 1 page       | 12.7 ns | 79.0 Mops/s    |
| `wal/encode` (4 096 B payload)         | 1 record     |  107 ns | 35.6 GiB/s     |
| `wal/buffer_append` (1 024 B record)   | 1 record     | 96.1 ns | 9.92 GiB/s     |

Full table and host descriptor:
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
- Buffer pool, CLOCK eviction, sharded page table.
- WAL record codec + in-memory append buffer (`ultrasql-wal`).
- MVCC tuple header, snapshot, visibility predicate (`ultrasql-mvcc`).
- Vectorized kernels: `sum_i64`, `eq_i32`, `min_f64`, `select_i32`
  (`ultrasql-vec`).
- Push-based executor with `MemTableScan`, `Filter`, `Project`,
  `Limit` (`ultrasql-executor`).
- PostgreSQL wire protocol v3 message codec (`ultrasql-protocol`).
- Logical planner + binder against in-memory catalog
  (`ultrasql-planner`).

Not yet implemented:

- Transaction manager, lock manager (`ultrasql-txn`).
- Cost-based optimizer (`ultrasql-optimizer`).
- Persistent catalog (`ultrasql-catalog` is a stub).
- Server binary and CLI: `ultrasqld` and `ultrasql` exit immediately.
- Segment-file I/O and the WAL fsync path.
- TCP accept loop wiring protocol → planner → executor → storage.

See [ROADMAP.md](ROADMAP.md) for the version-by-version plan.

Tests: **269 passing**, `cargo clippy --workspace --all-targets
--all-features -- -D warnings` clean,
`cargo fmt --all -- --check` clean.

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
