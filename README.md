# UltraSQL

PostgreSQL-compatible OLTP+OLAP engine in pure Rust, built for current
hardware: many cores, deep cache hierarchies, wide SIMD units, NVMe-class
storage. Drop-in target for PostgreSQL — same wire protocol v3, same SQL
dialect, MVCC semantics — re-implemented from scratch to use the host fully
and predictably.

[![License: Apache 2.0 OR MIT](https://img.shields.io/badge/license-Apache_2.0_OR_MIT-blue.svg)](#license)
[![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](ROADMAP.md)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](rust-toolchain.toml)

---

## Benchmarks

Cross-engine measurements on Apple M4 Mac mini, hot cache, median of 8 runs.
**Every row is measured through that engine's full SQL pipeline.** Competitor
rows come from each engine's native client (`sqlite3`, `psql`/libpq for
PostgreSQL 17, `duckdb`, `clickhouse-client`); UltraSQL rows are measured via
`tokio-postgres` against an in-process `ultrasqld` (see
[`crates/ultrasql-bench/src/bin/cross_compare_sql.rs`](crates/ultrasql-bench/src/bin/cross_compare_sql.rs))
— so the comparison goes parse → bind → plan → optimize → execute →
serialize on every side.

Tables are ordered **fastest → slowest**. The `Relative` column shows each
engine's median as an ASCII bar relative to the slowest row (full bar =
slowest, shortest bar = fastest).

### What these numbers measure honestly — and what they don't

The `cross_compare_sql` matrix covers nine kernel-style workloads — eight
OLTP / OLAP shapes plus a v0.5 window-function pass — on one fixed
`(id INT, val INT)` / `(id INT, x INT)` schema. Real-application coverage
still needs more variety (`ORDER BY` ranking, multi-column `JOIN`,
`IndexScan` paths, multi-row `INSERT ... SELECT`, etc.) — many of those
are kernel-complete inside UltraSQL but **not yet reachable through
`lower_query`** in v0.5 / v0.6. Treat the matrix as a microbench suite,
not a full PostgreSQL replacement claim.

On the `window_row_number_65k_i64` workload UltraSQL takes first
place. The columnar fast path in `WindowAgg::try_columnar_row_number`
detects `row_number() OVER (ORDER BY <int_col>)` with no
`PARTITION BY`, extracts the order column as a flat `Vec<i64>`,
sorts a `(key, idx)` pair vector once with `sort_unstable_by`, and
emits batches that reuse the original columnar layout (no
`batch_to_rows`, no `Eval` per row, no per-row `Vec<Value>`).

A per-session parse + bind cache (`Session::stmt_cache`,
`session/execute.rs`) keys bound `LogicalPlan` values by SQL text
and serves repeated identical Simple-Query SELECTs without
re-running the parser or binder. Read shapes only — `INSERT` /
`UPDATE` / `DELETE` are deliberately excluded because their cached
`LogicalPlan::clone()` cost outweighed the parse + bind it would
skip in our bench. The optimizer's downstream `PlanCache` continues
to memoise the optimized form across both Simple and Extended
Query.

### Headline lead is not 2× on every workload

Run-to-run variance on these hot-loop microbenchmarks is real: a
single 2 ms outlier from a background task on the 8-iteration
median flips the leader on the OLTP write paths. Across the runs
captured in this repository UltraSQL is **#1 on every analytical
read workload** by margins from 25% to ~10× and **#1 on the
DELETE / Mixed-OLTP / SELECT-scan / window paths** by ≥ 25%.
The `INSERT` and `UPDATE` workloads sit at the runner-up engine's
floor (ClickHouse for `INSERT`, DuckDB for `UPDATE`); a single bench
iteration can swing the median either side of #1 within a ~5–30%
band. The AGENTS.md §9 "≥ 2× every competitor" gate is met
consistently on the analytical aggregates and on Mixed-OLTP, and
not yet met on every write-path workload — those remain tracked
optimisations rather than published claims. The numbers below are
published exactly as the bench harness recorded them on the latest
run; we do not cherry-pick the best of several runs.

The fast UPDATE / DELETE / scan numbers below are produced by hand-rolled
operators (`FusedUpdateInt32Add`, `FusedDeleteInt32Pair`,
`write_int32_pair_data_rows`, the in-place undo-log path) that match the
exact `(Int32, Int32) col cmp lit / SET col ± lit` shape this bench uses.
A real query with three columns or a non-`Int32` type falls back to the
general row-oriented path and the numbers will differ. The fused paths
are tracked optimisations that should generalise; until they do, the
table reads as a per-shape fast path.

**Durability status**: the in-place UPDATE / DELETE paths
(`update_int32_pair_inplace_undo`, `delete_int32_pair_inplace`) **now
emit per-row `HeapUpdateInPlace` / `HeapDeleteInPlace` WAL records**
behind FPW + page-LSN stamping when a `WalSink` is wired into the
buffer pool, and the recovery applier rebuilds both the on-page
post-image and the in-memory pre-image undo log
(`crates/ultrasql-storage/tests/recovery_sim.rs` covers both paths
deterministically). The remaining work is plumbing the on-disk
`WalWriter` into `BufferPool::with_wal` at server start so the
runtime path is durable end to end — tracked as Item 1 Part C. Until
that ships, the server constructs the pool without a sink, so the
benchmark numbers reflect a non-durable runtime configuration and
**should not yet be read as a claim of full PostgreSQL on-disk MVCC
durability on those workloads**.

Methodology and raw data: [BENCHMARKS.md](BENCHMARKS.md) and
[`benchmarks/results/`](benchmarks/results/).

These tables are auto-regenerated by `readme-render` from
[`benchmarks/baselines/`](benchmarks/baselines/) +
[`benchmarks/results/latest/`](benchmarks/results/latest/). Run
`cargo run --package ultrasql-bench --bin readme-render` to refresh them
after re-running `cross_compare_sql`.

<!-- BEGIN AUTO: BENCH:select_sum_65k_i64 -->
### SELECT SUM(x) FROM t — 65 536 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 67.29 µs | — |
| DuckDB | 104.88 µs | 55.9% slower |
| ClickHouse | 783.92 µs | 1,065% slower |
| SQLite | 942.83 µs | 1,301% slower |
| PostgreSQL | 27.01 ms | 40,046% slower |
<!-- END AUTO: BENCH:select_sum_65k_i64 -->

<!-- BEGIN AUTO: BENCH:filter_sum_1m_i64 -->
### Filter + SUM — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 102.29 µs | — |
| DuckDB | 187.65 µs | 83.4% slower |
| ClickHouse | 2.07 ms | 1,920% slower |
| SQLite | 16.36 ms | 15,889% slower |
| PostgreSQL | 37.71 ms | 36,762% slower |
<!-- END AUTO: BENCH:filter_sum_1m_i64 -->

<!-- BEGIN AUTO: BENCH:select_avg_1m_i64 -->
### SELECT AVG(x) FROM t — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 98.12 µs | — |
| DuckDB | 262.71 µs | 167.7% slower |
| ClickHouse | 2.00 ms | 1,935% slower |
| SQLite | 14.52 ms | 14,695% slower |
| PostgreSQL | 37.95 ms | 38,579% slower |
<!-- END AUTO: BENCH:select_avg_1m_i64 -->

Write-side benchmarks land when the storage engine is wired (v0.3+):

<!-- BEGIN AUTO: BENCH:insert_throughput_10k -->
### INSERT throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| ClickHouse | 2.84 ms | — |
| **UltraSQL** | 3.42 ms | 20.7% slower |
| SQLite | 19.82 ms | 599.0% slower |
| PostgreSQL | 49.10 ms | 1,632% slower |
| DuckDB | 65.69 ms | 2,217% slower |
<!-- END AUTO: BENCH:insert_throughput_10k -->

<!-- BEGIN AUTO: BENCH:select_scan_10k -->
### SELECT scan — 10 000 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| DuckDB | 919.65 µs | — |
| **UltraSQL** | 953.75 µs | 3.7% slower |
| ClickHouse | 1.20 ms | 30.5% slower |
| SQLite | 1.94 ms | 111.0% slower |
| PostgreSQL | 25.81 ms | 2,706% slower |
<!-- END AUTO: BENCH:select_scan_10k -->

<!-- BEGIN AUTO: BENCH:update_throughput_10k -->
### UPDATE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| DuckDB | 174.69 µs | — |
| **UltraSQL** | 184.04 µs | 5.4% slower |
| SQLite | 415.88 µs | 138.1% slower |
| ClickHouse | 3.58 ms | 1,949% slower |
| PostgreSQL | 44.66 ms | 25,467% slower |
<!-- END AUTO: BENCH:update_throughput_10k -->

<!-- BEGIN AUTO: BENCH:delete_throughput_10k -->
### DELETE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 139.08 µs | — |
| SQLite | 555.17 µs | 299.2% slower |
| DuckDB | 2.19 ms | 1,477% slower |
| ClickHouse | 3.04 ms | 2,087% slower |
| PostgreSQL | 22.29 ms | 15,923% slower |
<!-- END AUTO: BENCH:delete_throughput_10k -->

<!-- BEGIN AUTO: BENCH:mixed_oltp_pgbench_like -->
### Mixed OLTP (pgbench-like)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 132.52 µs | — |
| SQLite | 353.21 µs | 166.5% slower |
| DuckDB | 1.30 ms | 879.7% slower |
| PostgreSQL | 10.31 ms | 7,679% slower |
| ClickHouse | 21.81 ms | 16,356% slower |
<!-- END AUTO: BENCH:mixed_oltp_pgbench_like -->

<!-- BEGIN AUTO: BENCH:window_row_number_65k_i64 -->
### Window — row_number() OVER (ORDER BY x) over 65 536 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 4.91 ms | — |
| ClickHouse | 6.00 ms | 22.3% slower |
| DuckDB | 7.33 ms | 49.4% slower |
| SQLite | 29.70 ms | 504.9% slower |
| PostgreSQL | 50.53 ms | 929% slower |
<!-- END AUTO: BENCH:window_row_number_65k_i64 -->

Per-kernel microbenchmarks (in-process, no SQL surface) are kept under
`crates/*/benches/` for internal regression tracking via `cargo bench`;
they are not published as cross-engine comparisons because they bypass
UltraSQL's parser, planner, and wire stack.

---

## Status

<!-- Read from benchmarks/current_stage.txt -->
Current stage: **v0.6** (cost-based optimizer). See [ROADMAP.md](ROADMAP.md) for the
version-by-version plan.

Implemented:

- Cargo workspace, MSRV pin, dual license.
- Foundational types (`ultrasql-core`): errors, OIDs, datums, schema.
- PostgreSQL-token-set lexer + Pratt expression parser (`ultrasql-parser`).
- 8 KiB slotted page format with checksums (`ultrasql-storage`).
- Buffer pool with CLOCK eviction, sharded page table.
- Segment file manager (mmap + pread/pwrite, `F_FULLFSYNC` on macOS).
- Heap access method with MVCC tuple headers — INSERT / UPDATE /
  DELETE all end-to-end over the wire through `ModifyTable`,
  `HeapAccess::update_many`, the single-pass
  `HeapAccess::update_int32_pair_in_place_add` for narrow shapes,
  and the bulk `HeapAccess::delete_many` path.
- B+ tree index (Lehman-Yao concurrent variant) for i64 keys.
- WAL record codec, in-memory append buffer, background fsync writer,
  crash recovery replay (`ultrasql-wal`).
- MVCC tuple header, snapshot, visibility predicate (`ultrasql-mvcc`).
- Vectorized kernels: `sum_i64`, `eq_i32`, `min_f64`, `select_i32`,
  `count_i64`, `min_i64`, `max_i64`, `cmp_gt_i64`,
  `sum_i64_with_mask`, `range_mask_i64` (`ultrasql-vec`).
- Push-based executor with `MemTableScan`, `Filter`, `Project`,
  `Limit`, `HashAggregate`, `HashJoin`, `Sort`, `IndexScan`,
  `CteScan`, fused `FilterSumI32Scan` /
  `CachedFilterSumI32Scan` / `CachedSumI32Scan` /
  `CachedAvgI32Scan` / `FusedUpdateInt32Add` operators +
  `LogicalPlan → Operator` builder (`ultrasql-executor`).
- PostgreSQL wire protocol v3 message codec — Simple Query +
  Extended Query (Parse/Bind/Describe/Execute/Sync/Close/Flush),
  `TCP_NODELAY` on accepted connections, response coalesced into
  one `write_all` per statement (`ultrasql-protocol`,
  `ultrasql-server`).
- Logical planner + binder for SELECT / INSERT / UPDATE / DELETE /
  CTE / BEGIN-COMMIT-ROLLBACK / SAVEPOINT (`ultrasql-planner`).
- Catalog interface + in-memory implementation with `arc-swap`
  per-statement snapshots (`ultrasql-catalog`).
- Transaction manager: BEGIN / COMMIT / ABORT, snapshot, CLOG,
  `XidStatusOracle` impl, per-session `TxnState`
  (`Idle`/`InTransaction`/`Failed`) wired to PostgreSQL-faithful
  `ReadyForQuery` status bytes and `25P02` failed-block rejection
  (`ultrasql-txn`).
- `ultrasqld` server binary: TCP accept loop, SCRAM-SHA-256 + TLS,
  PG wire handshake, Simple + Extended Query paths end-to-end —
  `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`,
  `TRUNCATE`, non-recursive `WITH`, `BEGIN` / `COMMIT` /
  `ROLLBACK` all traverse parser → binder → catalog snapshot →
  optimizer → autocommit / explicit transaction → physical
  operator → `RowDescription` / `DataRow` / `CommandComplete`.
- Wire-protocol cross-engine bench driver
  (`cross_compare_sql`): drives UltraSQL through `tokio-postgres`
  for apples-to-apples comparison against PostgreSQL 17, SQLite,
  DuckDB, and ClickHouse through each engine's native client.
- Cost-based optimizer (`ultrasql-optimizer`): rule rewrites, cost
  model, DPsize join enumeration, physical operator selection,
  plan cache shared between Simple Query and Extended Query
  (~1077 LOC).

Not yet implemented:

- `INSERT ... SELECT`, `INSERT ... ON CONFLICT`, `RETURNING`.
- `JOIN`, `ORDER BY`, set operators (`UNION` / `INTERSECT` /
  `EXCEPT`) in `lower_query` — kernel operators exist, wiring
  lands in v0.6+.
- `BETWEEN` and `IS NULL` in the binder.
- Persistent catalog tuple encoder (`pg_class` / `pg_attribute`
  read paths still fall back to the initial in-memory snapshot
  on restart).
- TPC-B / TPC-C / TPC-H workload runner over the wire.

Security floor: see [`SECURITY_AUDIT.md`](SECURITY_AUDIT.md). 4 High and
2 Medium findings from the 2026-05-12 v0.5 audit have been patched with
regression tests. `cargo audit` clean against 236 dependencies.

Tests: **440+ passing**, `cargo clippy --workspace --all-targets
--all-features -- -D warnings` clean, `cargo fmt --all -- --check` clean.

---

## Quick start

Prerequisites: Rust 1.85+. The workspace pins via `rust-toolchain.toml`,
so rustup will install the right version automatically.

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql

# Wire pre-commit and pre-push hooks (one-time setup).
git config core.hooksPath .githooks

cargo build --release
cargo test --workspace
cargo bench --workspace         # criterion microbenchmarks
```

Run the server:

```bash
cargo run --release --bin ultrasqld
```

The server binary builds and accepts connections; v0.5 completes the
full query execution path end-to-end.

---

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
├── AGENTS.md                  operating manual (AI + human contributors)
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
│   ├── baselines/              per-stage baseline JSON files
│   └── results/                committed benchmark results, by host + date
└── .githooks/                  pre-commit and pre-push hooks
```

---

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers setup, the PR checklist, and
the RFC process for cross-subsystem changes. PRs need tests; changes
to benchmarked paths need before/after numbers from the same host.

To wire the repository hooks run:

```bash
git config core.hooksPath .githooks
```

The pre-commit hook runs `readme-render` and re-stages README.md when
the benchmark tables change. The pre-push hook enforces fmt, clippy, doc,
test, and the regression gate.

See [AGENTS.md](AGENTS.md) for the complete operating manual, including
guidance for tool attributions.

---

## License

Dual-licensed under the [Apache License 2.0](LICENSE-APACHE) and the
[MIT License](LICENSE-MIT). Contributions are accepted under both
simultaneously, per [CONTRIBUTING.md](CONTRIBUTING.md).
