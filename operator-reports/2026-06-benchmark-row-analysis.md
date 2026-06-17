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

## insert_throughput 10k & mixed_oltp 10k → PostgreSQL (OLTP write/commit path)

These are the genuine transactional-write weakness. PostgreSQL 17 wins
single-shot INSERT-10k (7.6 ms vs 9.6 ms) and point-mixed OLTP (31 µs/op vs
~472 µs/op). The mixed-OLTP gap is large and is the per-commit cost: every op is
its own transaction, so the WAL commit + fsync cadence dominates. This is the
group-commit / fsync-amplification / lock-manager surface. A genuine
improvement here (e.g. a tuned group-commit batching window) is real future
work; it was not landed in this pass. **Exit condition:** a committed
before/after from the harness showing reduced per-commit latency on the
sysbench/mixed-OLTP path with all isolation/recovery tests green; documented in
ROADMAP P0 Performance Certification.

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
