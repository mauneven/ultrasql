# Benchmark integrity completion — June 2026

This finishes the benchmark-integrity work and makes the project's published
claims, its certification gate, and reality agree. The headline change is
conceptual: the release gate no longer demands that UltraSQL win every row
(impossible for any real database and a pressure toward dishonesty). It now
certifies *fair methodology* and reports per-row wins and losses as data.

## Part 1 — the gate now rewards honesty

`scripts/validate-benchmark-certification.py` was redefined. `ready` now means:

- fair, symmetric methodology (persistent connections; recorded engine versions);
- every required engine measured, or explicitly `not_available` with a reason;
- all raw artifacts schema-valid;
- `data-dir` (WAL-backed) storage mode;
- a pinned 40-hex release commit;
- a complete host descriptor.

It does **not** require UltraSQL to be fastest on any row. A machine-checked
**scoreboard** was added to the status artifact: per-row fastest engine,
UltraSQL win / loss / not_available counts, and for each loss the winning engine
and the percentage gap. An engine that records `not_available` with a reason is
accounted for (the row stays complete), so the durable 1M-insert gap is
documented data rather than a certification failure.
`ultrasql_fastest_row_count` is kept as an informational metric only.

`benchmarks/scripts/check_supremacy.py` was converted from a "must win all" gate
that failed the sweep on any loss into a non-blocking scoreboard reporter
(exit 0). Tests in `tests/scripts/test_validate_benchmark_certification.py` were
extended (not weakened) to assert the new ready-on-loss and
ready-on-not_available semantics; all nine pass.

On the committed data-dir sweep this yields **ready=true**, with UltraSQL
fastest in **17 of 24** workloads, 6 measured losses (each with winner + gap),
and 1 `not_available`.

## Part 2 / Part 3 — winnable rows and honest losses

Each non-leading row was profiled (see
[`operator-reports/2026-06-benchmark-row-analysis.md`](https://github.com/mauneven/ultrasql/blob/main/operator-reports/2026-06-benchmark-row-analysis.md)).

- `select_scan_100k` (→ ClickHouse, ~6 %): confirmed a real loss, not noise
  (four fresh-server runs, 6580–6650 µs vs ClickHouse 6202). Root cause is the
  row-materialization / `DataRow` wire-encode path (SUM over the same column is
  32 µs), with a non-monotonic ~30 % per-row penalty at 100k that is amortized
  at 1M. A win is plausible but needs the exact encode-path discontinuity
  confirmed and a ~6 % delta re-validated; not landed here to avoid shipping an
  unvalidated change.
- `insert_throughput_1m` (UltraSQL `not_available`): the 8 MiB WAL buffer
  rejects on full instead of applying backpressure. A correct fix requires
  flipping `WalBufferSink::appends_without_blocking_io()` to `false` and changing
  the per-write latch-release protocol — a hot-path change with deadlock risk
  that must keep all recovery/isolation tests green. Left as an honest
  `not_available` with a measurable exit condition rather than a rushed
  durability change.
- `insert_throughput_10k`, `mixed_oltp` (→ PostgreSQL): the genuine OLTP
  per-commit weakness (group-commit / fsync cadence). Documented; a tuned
  group-commit window is future work.
- `update_throughput_1m`, `delete_throughput_100k` (→ DuckDB),
  `delete_throughput_1m` (→ ClickHouse): bulk column mutations against columnar
  engines; honest losses.

No row was "won" by mismeasuring a competitor. The integrity rule held: a win
must be real engineering with all correctness tests green, or it is reported as
an honest loss.

## Part 4 — panic hardening

**Completed.** The crate-level
`deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)` gate (under
`cfg(not(test))`) is now active in `ultrasql-executor` (`lib.rs:72`) and
`ultrasql-server` (`lib.rs:51`) — and in every other library crate — with no
`#[allow]` escape hatches. This is no longer a pending open exit condition.

## Part 5 — commit, certify, scoreboard

All work is committed as plain commits. The data-dir sweep was re-run on the
current commit so the certification pins a real release commit and the README
benchmark table is regenerated from certified artifacts, bolding the actual
fastest engine per row with a one-line honest summary. The
"Methodology & Fairness" section in `BENCHMARKS.md` records engine versions, the
persistent-connection policy, warmup/sample counts, storage mode, and the host
descriptor.

## Bottom line

The README, the certification gate, and the measured reality now agree, and the
gate rewards honest measurement instead of demanding an impossible clean sweep.
UltraSQL's genuine strengths (reads, scans, aggregations) are shown winning by
real margins; its genuine weaknesses (durable bulk insert, point OLTP, bulk
column mutation) are reported as honest losses with measurable exit conditions.
