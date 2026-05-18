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

Cross-engine measurements on Apple M4 Mac mini, hot cache, median of 32 runs.
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

Four foundational engine changes landed across parallel agents in
isolated worktrees:

1. **`DirectScalarAggScan` operator** (`crates/ultrasql-executor/src/direct_scalar_agg.rs`)
   — hand-rolled single-pass operator for `SUM(col)`/`AVG(col)`/
   `COUNT(*)` with no `GROUP BY` and a single int column. Bypasses
   the `HashAggregate` state machine and feeds `SeqScan` output
   directly into the SIMD kernels `sum_i32_widening` / `sum_i64`.
   Recognized in `pipeline::agg_fuse::try_lower_direct_scalar_aggregate`
   and short-circuited via `is_scalar_aggregate_shape` in
   `session/execute.rs`.
2. **Parallel sort + pre-sorted shortcut** in
   `WindowAgg::try_columnar_row_number` — when the input is large
   enough (≥ 16 384 rows) the kernel splits the `(i64, u32)` pair
   vector into N scoped-thread chunks (capped at 8), sorts each in
   parallel with `sort_unstable_by`, then runs a 2-way merge tree.
   The new `is_non_decreasing` shortcut skips both the sort and the
   parallel path entirely when the input is already ordered.
3. **Wire-emit fast paths** — per-thread `BytesMut` pool in
   `result_encoder`, identity-projection elision in
   `lower_project_columns`, child-row-count hint propagated via
   `Project::estimated_row_count`, `SeqScan::build` skips the
   builder + heap-walker allocation on column-cache hits, and
   `write_int32_pair_data_rows` (Int32+Int32) / `write_int32_int64_pair_data_rows`
   (Int32+Int64, used for window output) emit DataRow records via
   raw pointer writes against a reserved spare region using a
   200-byte two-digit `DIGIT_PAIRS` lookup table — no per-cell
   `is_null()` branch, no `BytesMut::reserve` re-grow mid-loop.
4. **Per-session parse + bind cache** (`Session::stmt_cache`,
   `session/execute.rs`) keys `Arc<LogicalPlan>` by SQL text so a
   hot Simple-Query loop pays parse + bind once. DML and SELECT
   shapes only; txn-control / DDL / `PREPARE` are deliberately
   excluded.

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
| **UltraSQL** | 70.50 µs | — |
| DuckDB | 97.33 µs | 38.1% slower |
| SQLite | 889.58 µs | 1,162% slower |
| ClickHouse | 2.32 ms | 3,192% slower |
| PostgreSQL | 30.98 ms | 43,842% slower |
<!-- END AUTO: BENCH:select_sum_65k_i64 -->

<!-- BEGIN AUTO: BENCH:filter_sum_1m_i64 -->
### Filter + SUM — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 101.67 µs | — |
| DuckDB | 235.48 µs | 131.6% slower |
| ClickHouse | 3.15 ms | 2,999% slower |
| SQLite | 15.74 ms | 15,385% slower |
| PostgreSQL | 36.90 ms | 36,199% slower |
<!-- END AUTO: BENCH:filter_sum_1m_i64 -->

<!-- BEGIN AUTO: BENCH:select_avg_1m_i64 -->
### SELECT AVG(x) FROM t — 1 000 000 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 87.71 µs | — |
| DuckDB | 280.38 µs | 219.7% slower |
| ClickHouse | 3.93 ms | 4,379% slower |
| SQLite | 13.99 ms | 15,851% slower |
| PostgreSQL | 37.94 ms | 43,154% slower |
<!-- END AUTO: BENCH:select_avg_1m_i64 -->

Write-side benchmarks land when the storage engine is wired (v0.3+):

<!-- BEGIN AUTO: BENCH:insert_throughput_10k -->
### INSERT throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 4.53 ms | — |
| SQLite | 19.66 ms | 334.0% slower |
| PostgreSQL | 55.08 ms | 1,116% slower |
| DuckDB | 63.20 ms | 1,295% slower |
| ClickHouse | 67.01 ms | 1,379% slower |
<!-- END AUTO: BENCH:insert_throughput_10k -->

<!-- BEGIN AUTO: BENCH:select_scan_10k -->
### SELECT scan — 10 000 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 631.50 µs | — |
| DuckDB | 850.00 µs | 34.6% slower |
| SQLite | 1.92 ms | 204.3% slower |
| ClickHouse | 2.89 ms | 357.8% slower |
| PostgreSQL | 28.89 ms | 4,475% slower |
<!-- END AUTO: BENCH:select_scan_10k -->

<!-- BEGIN AUTO: BENCH:update_throughput_10k -->
### UPDATE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 144.75 µs | — |
| DuckDB | 186.29 µs | 28.7% slower |
| SQLite | 465.92 µs | 221.9% slower |
| ClickHouse | 7.60 ms | 5,148% slower |
| PostgreSQL | 45.92 ms | 31,624% slower |
<!-- END AUTO: BENCH:update_throughput_10k -->

<!-- BEGIN AUTO: BENCH:delete_throughput_10k -->
### DELETE throughput — 10 000 rows

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 151.54 µs | — |
| SQLite | 545.23 µs | 259.8% slower |
| DuckDB | 1.98 ms | 1,208% slower |
| ClickHouse | 9.60 ms | 6,235% slower |
| PostgreSQL | 24.45 ms | 16,032% slower |
<!-- END AUTO: BENCH:delete_throughput_10k -->

<!-- BEGIN AUTO: BENCH:mixed_oltp_pgbench_like -->
### Mixed OLTP (pgbench-like)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 122.49 µs | — |
| SQLite | 347.37 µs | 183.6% slower |
| DuckDB | 1.25 ms | 918% slower |
| PostgreSQL | 11.98 ms | 9,677% slower |
| ClickHouse | 25.00 ms | 20,306% slower |
<!-- END AUTO: BENCH:mixed_oltp_pgbench_like -->

<!-- BEGIN AUTO: BENCH:window_row_number_65k_i64 -->
### Window — row_number() OVER (ORDER BY x) over 65 536 i32 rows (full wire round-trip)

| Engine | Median | vs fastest |
| --- | ---: | ---: |
| **UltraSQL** | 4.59 ms | — |
| DuckDB | 7.22 ms | 57.3% slower |
| ClickHouse | 12.39 ms | 170.0% slower |
| SQLite | 29.86 ms | 550.6% slower |
| PostgreSQL | 46.30 ms | 909% slower |
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
