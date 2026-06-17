# Scale-sweep row analysis — June 2026

Profiling notes for the rows where UltraSQL is not the fastest measured engine
on the fair, same-host data-dir scale sweep (Apple M4, PostgreSQL 17.10, DuckDB
1.5.2, ClickHouse 26.5.2, SQLite 3.51). Every number here comes from the
committed harness. The goal is an honest root-cause per row: what was measured,
why UltraSQL is behind, and whether a correctness-preserving win is available
now. Rows that cannot be won honestly are left as honest losses with a
measurable exit condition in [ROADMAP.md](../ROADMAP.md).

## select_scan 100k → ClickHouse (real ~6% loss)

UltraSQL wins `select_scan` at 10k (507 µs vs 985) and 1M (50.1 ms vs 59.6 ms)
but loses at 100k. Re-measured with four fresh-server runs to rule out noise:

| run | UltraSQL median (µs) | min (µs) |
|---|---:|---:|
| 1 | 6585.2 | 6353.2 |
| 2 | 6646.3 | 6317.0 |
| 3 | 6580.3 | 6309.2 |
| 4 | 6622.0 | 6326.1 |

ClickHouse measured 6201.9 µs. UltraSQL is **consistently ~6 % slower**, even at
its min — this is a real loss, not run-to-run noise.

Root cause is **not the column scan**: `SELECT SUM(x)` over the same 100k column
is 32 µs, so reading the data is cheap. The cost is materializing and
wire-encoding 100k `(id, val)` rows. Per-row cost is non-monotonic:

| rows | UltraSQL scan (µs) | ns/row |
|---|---:|---:|
| 10 000 | 507 | ~50 |
| 100 000 | ~6600 | ~66 |
| 1 000 000 | 50 071 | ~50 |

The ~30 % per-row penalty specific to 100k points to a working-set / buffer
growth discontinuity in the row-materialization or `DataRow` wire-encode path
that is amortized at 1M but bites at 100k. A clean win likely exists (e.g.
reserving the result/wire buffer to the projected output size, or a
chunk-boundary fix), but it requires confirming the exact discontinuity in the
encode path and re-validating a ~6 % delta against ClickHouse — work that was
not landed here to avoid shipping an unvalidated change. **Exit condition:** a
committed sweep where `select_scan_100k-ultrasql` median ≤ ClickHouse, with the
scan path's per-row encode cost monotonic across 10k/100k/1M.

## insert_throughput 1M → SQLite (UltraSQL not_available)

UltraSQL fails the 1M-row durable INSERT: `wal buffer full: have 8383400 bytes
used of 8388608`. The bulk insert (one transaction, 10k-row chunks) appends WAL
records faster than the background writer drains the 8 MiB buffer, and the
append path **rejects** instead of applying backpressure.

The proper fix is backpressure (append waits for the writer to free space), but
it is **not a safe, contained change**: `WalBufferSink::appends_without_blocking_io()`
returns `true`, so heap callers may hold page latches across an append. Making
the append block would require flipping that contract to `false` and changing
the latch-release protocol on every write path — a hot-path change with real
deadlock risk that must keep all recovery/isolation tests green. Increasing the
buffer size is only a band-aid (a larger bulk insert still overflows) and is not
a real algorithmic win. **Exit condition:** a 1M-row durable INSERT completes
via WAL backpressure (writer-driven drain wait, callers release latches first),
all `recovery_sim`/`hermitage`/`isolation` tests green, and the sweep records a
measured `insert_throughput_1m-ultrasql` artifact.

## insert_throughput 10k & mixed_oltp 10k → PostgreSQL / SQLite (OLTP commit path)

These are the genuine transactional-write weakness. On the certified sweep
PostgreSQL 17 wins single-shot INSERT-10k and PostgreSQL/SQLite win point-mixed
OLTP (SQLite 28 µs/op, PostgreSQL 29 µs/op vs UltraSQL ~417 µs/op). Every op is
its own transaction, so the per-commit WAL durability cost dominates.

**Profiled, attempted, no win (durable-wait micro-optimization).** The commit
barrier `wait_for_wal_durable` polled `flushed_lsn` with `notify(); sleep(50µs)`,
so each commit appeared to pay a ~50 µs poll-quantum floor. I implemented an
adaptive replacement: the WAL writer signals a condvar the instant it publishes
a new durable LSN, so the committer wakes precisely instead of sleeping
(durability contract unchanged — it still returns only once
`flushed_lsn >= commit_lsn`). All `recovery_sim`/`hermitage`/`isolation`/WAL
tests stayed green. But a clean same-host A/B showed **no improvement**:
mixed_oltp was ~354 µs/op (old sleep-poll) vs ~384 µs/op (condvar) — neutral to
slightly worse. The change was reverted.

**Real root cause.** The per-commit cost is dominated by `full_fsync`
(F_FULLFSYNC, `crates/ultrasql-wal/src/writer.rs::flush_current`), which forces a
true power-loss flush of the drive cache and costs hundreds of µs on this host —
far more than the 50 µs poll, so removing the poll cannot help. PostgreSQL on
macOS uses plain `fsync` by default, which does **not** force a drive-cache
flush, so UltraSQL is providing a *stronger* durability guarantee at a higher
per-commit cost. The single-connection benchmark also gives group commit nothing
to amortize across (commits are serial). A correctness-preserving win is
therefore not available without either (a) weakening durability to PG's macOS
`fsync` level (dishonest vs the current guarantee) or (b) async/deferred commit
(changes durability semantics). Both are off the table under the integrity
rules, so these rows are formally accepted as honest losses. **Exit conditions
(ROADMAP P0):** either a multi-client OLTP benchmark where group commit amortizes
F_FULLFSYNC across concurrent committers and UltraSQL's tx/s meets or beats the
winner, or an apples-to-apples same-durability comparison (both engines at
F_FULLFSYNC, or both at `fsync`) where UltraSQL's per-commit latency is no higher.

## update_throughput 1M → DuckDB, delete_throughput 100k → DuckDB, delete_throughput 1M → ClickHouse

Large bulk mutations against columnar engines. DuckDB/ClickHouse apply
set-oriented column rewrites; UltraSQL's row-store + MVCC + per-row WAL pays
more per row at these cardinalities. These are honest losses to engines
architecturally suited to bulk column mutation. **Exit condition:** only claim a
flip if a committed sweep shows UltraSQL's measured median is lowest with no
harness regression; otherwise these remain documented honest losses.

## Summary

On the fair data-dir sweep UltraSQL leads 17 of 24 workloads. Of the 7 it does
not lead: one is a hard durable-insert failure (clear root cause, risky fix),
two are the OLTP per-commit weakness, three are bulk column mutations against
columnar engines, and one (`select_scan_100k`) is a real but small wire-encode
anomaly. None were "won" by mismeasuring a competitor; the honest scoreboard
stands, and the certification gate now treats these as reported data rather than
a failure.
