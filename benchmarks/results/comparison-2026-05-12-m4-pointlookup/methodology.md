# Methodology — Point-lookup cross-engine comparison (2026-05-12, M4)

## Scope

A single workload, measured five ways:

| Tag                  | Query                                | Dataset                                                    |
| -------------------- | ------------------------------------ | ---------------------------------------------------------- |
| `point-10m-probes`   | `SELECT x FROM t WHERE id = ?`        | 10,000,000-row `(id BIGINT PRIMARY KEY, x BIGINT)` table   |

We run 100,000 probes per measurement run, three runs per engine, and
report the **median nanoseconds-per-probe across runs** plus the
**total wall time of the median run**.

## Why this comparison exists

The prior comparison directory at
`benchmarks/results/comparison-2026-05-12-m4-extended/results.json`
includes a `point-10m` row that — for UltraSQL only — reports the
per-iteration wall time of a 10,000-probe batch as the row's
`median_us`. In prose summaries the same number was read as "median
microseconds **per probe**". That was off by a factor of 10,000 (the
batch size). When you divide the prior `median_us = 6781.15 µs` by
`10,000` you recover the real per-probe number, `≈ 0.68 µs`, which is
in the same ballpark as the SQL engines after their own per-statement
costs are accounted for.

This directory rewrites the point-lookup measurement under a
methodology where the **per-probe** number is the reported number for
every engine, the timed region is the probe loop only, and the units
are explicit (`*_ns_per_probe`, `total_wall_ns_*`).

The original 10-workload `comparison-2026-05-12-m4-extended` is
unaffected; it remains the canonical source for the other nine
workloads.

## The fairness rule

For every engine:

1. **Setup** — load 10,000,000 rows into a fresh table, declare
   `id` as the primary key, ANALYZE if applicable. This is **not
   timed**.
2. **Prepare statement** — bind the parameterised statement so the
   parser/planner cache is hot. **Not timed.**
3. **Warmup** — first run 10,000 throwaway probes (cycling through
   the head of the probe array) to prime the parser/planner cache and
   the branch predictor, then run one full throwaway pass through all
   100,000 probe keys so every B-tree / index page the measurement
   run will hit is resident in the engine's buffer cache (or the OS
   page cache, for engines that delegate). **Not timed.**
4. **Measure** — run **100,000 probes**. Wall-clock the start and end
   of the loop with the highest-resolution monotonic clock the
   driver exposes (`std::time::Instant` in Rust,
   `time.perf_counter_ns` in Python). **This is the timed region.**
5. Repeat step (4) three times in the same session. Report the median
   per-probe time across the three runs.

This is the same fairness rule the existing extended-workload
comparison uses for non-batched queries (see
`comparison-2026-05-12-m4-extended/methodology.md` § "Per-engine
procedure"). The contribution of this directory is **applying it to
the point lookup**, where the prior comparison was timing batches
rather than probes.

## Datasets

A single deterministic generation step produces two files in
`/tmp/ultracmp`:

| File                | Rows       | Seed                       |
| ------------------- | ---------- | -------------------------- |
| `data_id_10m.csv`   | 10,000,000 | `0xDEADBEEF` for `x`, `0xC0FFEE` for the `id` permutation |
| `probes_100k.txt`   | 100,000    | xorshift seed `0xCAFE_BABE_F00D_1234`, reduced into `[0, 10,000,000)` |

The xorshift used to generate probes matches the Rust binary's
`probe_seed`, so the UltraSQL run and the SQL-engine runs sample the
**same probe ids in the same order**.

The data CSV is the same `data_id_10m.csv` used by
`comparison-2026-05-12-m4-extended`. The probe text file is unique to
this directory.

`run.sh` records the SHA-256 of both files in
`raw/dataset_sha256.txt` and embeds them in `results.json`.

## Per-engine procedure

`run.sh` invokes one Python generation block + five engine blocks +
one parser block. Each engine block writes its raw JSON (one record)
to `raw/<engine>.json`. UltraSQL writes one JSONL line to
`raw/ultrasql.jsonl` to keep the cross_compare style.

### UltraSQL — native Rust binary `point_lookup`

`crates/ultrasql-bench/src/bin/point_lookup.rs` builds a `BTree<i64>`
over the in-memory buffer pool, inserts 10,000,000 deterministic
`(id, TupleId)` pairs in xorshifted order, then runs three timed loops
of 100,000 `BTree::lookup` calls against the same probe array the
SQL engines consume.

Buffer-pool sizing. The v0.5 buffer pool refuses to evict dirty
pages (the storage manager owns flushing, which is not yet wired up).
With `MAX_LEAF_ENTRIES = 32` and random-key insertion, every split
leaves both halves at ≈ 16 entries, so leaves average ≈ 24 entries
and the steady-state leaf count is ≈ ⌈10M / 24⌉ ≈ 416,667. We budget
`n / 12 + 32,768` frames (≈ 866k frames, ≈ 6.6 GiB resident) so
repeated splits and momentarily-pinned root-to-leaf descent paths
never collide with the dirty-page count. Below the host's 16 GiB.

The build itself is slow at 10M keys (~6.5 minutes on M4) — the
v0.5 `acquire_frame_for` does a linear walk over the frame array to
find an empty slot, and with 866k frames and 666k allocations the
walk amortizes to O(N²). The build is **not** in the timed region;
it is documented in `build_ns` in the JSON output and noted in
`results.md`. The probe phase, which **is** timed, runs against a
stable hot pool and is unaffected by the build's quadratic cost.

Per-iteration body:

```rust
let mut hits = 0_i64;
for &k in &keys {
    if let Some(t) = tree.lookup::<i64>(k).unwrap_or(None) {
        hits = hits.wrapping_add(i64::from(t.page.block.raw()));
    }
}
```

The `hits` accumulator is `black_box`-ed at the end of each run so
the optimizer cannot elide the result.

The Rust binary measures the **data plane only** — there is no client
driver, no IPC, no network. This is the lowest-possible-overhead
measurement for UltraSQL, and we call it out in `results.md` as such.
Once the v0.5 SQL pipeline is online, this row will be re-measured
end-to-end and the number will go up by whatever the pipeline costs.

### SQLite — `python3 sqlite3 :memory:`

- Database is `:memory:`, with `journal_mode=MEMORY`,
  `synchronous=OFF`, `temp_store=MEMORY`.
- Table `t (id INTEGER PRIMARY KEY, x INTEGER)`. SQLite's
  `INTEGER PRIMARY KEY` makes the table itself the index (rowid).
- 10,000,000 rows inserted via a single `executemany`.
- `ANALYZE` after load.
- Prepared statement: `SELECT x FROM t WHERE id = ?`. The cursor
  caches the parsed form across calls.
- Warmup: 10,000 cycling probes (predictor warmup) + one full pass
  through all 100,000 probe ids (page-cache warmup).
- 3 measurement runs of 100,000 probes; each timed with
  `time.perf_counter_ns()` outside the inner loop.

The reported number includes the Python loop overhead (≈ a few
hundred nanoseconds per call: the cursor dispatch, the parameter
marshalling, and the `fetchone` allocation). We note this in the
table.

### DuckDB — `python3 duckdb` (in-process)

- Connection is `:memory:`, `PRAGMA threads=1`.
- Table `t (id BIGINT PRIMARY KEY, x BIGINT)`.
- 10,000,000 rows loaded via `COPY t FROM '<csv>' (HEADER, DELIMITER ',')`.
- `ANALYZE`.
- Prepared statement: `SELECT x FROM t WHERE id = ?`. DuckDB's Python
  binding routes repeat calls of the same SQL string through the
  prepared-statement cache.
- Warmup: 10,000 cycling probes + one full pass through all probe ids.
- 3 measurement runs of 100,000 probes.

DuckDB is a bulk-OLAP engine; its per-query setup cost is non-trivial
for tiny single-row queries. The reported number reflects that.

### PostgreSQL — `python3 psycopg` (3.x) over the Unix socket

- A fresh database `ultracmp_point` is created and populated via
  `\COPY`.
- Table `t (id BIGINT PRIMARY KEY, x BIGINT)`; the PK auto-creates a
  B-tree index.
- `SET max_parallel_workers_per_gather = 0` for the session, so
  PostgreSQL executes single-threaded.
- `prepare_threshold=0` on the connection so every parameterised
  statement is server-side-prepared from the first call.
- The same parameterised SQL is reused across all calls in the same
  session; psycopg routes via the named prepared statement.
- Warmup: 10,000 cycling probes + one full pass through all probe ids.
  The full pass matters here: it pulls every index page into
  PostgreSQL's `shared_buffers` (or the kernel page cache), without
  which run 1 measures cold-cache latency and overstates per-probe
  cost by ~2×.
- 3 measurement runs of 100,000 probes.

The reported number includes the Unix-socket round-trip per probe.
Wire-protocol cost is part of "what PostgreSQL costs to use" and we
keep it in.

### ClickHouse — `clickhouse local` MergeTree

- Storage engine is `MergeTree() ORDER BY id PRIMARY KEY id`. Memory
  engine has no primary-key index, which we already documented as a
  loss in the prior comparison; `MergeTree` is the correct OLTP-ish
  comparison shape.
- 10,000,000 rows inserted from CSV, then `OPTIMIZE TABLE t FINAL` to
  fold all parts into a single sorted part so each probe touches one
  granule.
- The probe loop is encoded as a 100,000-line script of literal
  `SELECT x FROM t WHERE id = <k> FORMAT Null;` statements (the
  literal substitution is necessary because the `clickhouse local`
  CLI does not expose `PREPARE`/`EXECUTE` with bound parameters from
  a script).
- `clickhouse local --path <dir> --queries-file <script>` runs the
  whole 100,000-statement script in a single invocation; we wrap the
  invocation in `time.perf_counter_ns()` for the wall clock.
- 10,000 warmup probes via a separate invocation. We deliberately
  skip the "full-pass warmup" that the other engines do (running
  100,000 throwaway probes after the cycling warmup), because that
  would itself cost ~10 minutes on `clickhouse local`. The 10,000
  cycling warmup is enough to make the relevant MergeTree parts
  resident.
- **1 measurement run** (capped). A single 100k-probe pass through
  `clickhouse local` takes ≈ 10 minutes on the reference host — each
  probe pays the engine's per-query startup overhead (parse, plan,
  open MergeTree parts). Doing 3 such runs would consume ~30 minutes
  and blow past the 20-minute total-script budget, so we cap at 1.
  The run is overlay-tagged in the JSON with the engine note "capped
  at 1 run (per-engine target overage; OLTP unfit)". Bypass the cap
  by running `CH_RUNS=3 bash run.sh` if you have an hour to spare.

ClickHouse is not designed for OLTP point lookups; each statement
pays the engine's parse + plan + execute overhead per probe, which
for `clickhouse local` includes opening the parts on disk. The result
is **a loss for ClickHouse on this workload**. We keep the row in the
table because the engine *did* execute the queries — the comparison
is honest about the access pattern's fit.

## What `results.json` looks like

Each engine entry under `results["point-10m-probes"]` has:

```json
{
  "median_ns_per_probe":      <number>,
  "min_ns_per_probe":         <number>,
  "max_ns_per_probe":         <number>,
  "total_wall_ns_median_run": <int>,
  "probes":                   100000,
  "runs":                     3,
  "note":                     "...",
  "raw":                      { ...the engine's full record... }
}
```

`raw` carries each per-run total (`run_ns`) and each per-run rate
(`per_probe_ns`) so consumers can re-aggregate if desired.

Skipped engines emit `{ "skipped": true, "reason": "..." }` in the
same slot, matching the prior comparison's shape.

## Caps and timeouts

| Cap                          | Target / Actual     | Notes                                  |
| ---------------------------- | ------------------- | -------------------------------------- |
| Per-engine wall clock        | 5 min target        | UltraSQL build overshoots (~6.5 min)   |
|                              |                     | ClickHouse 1-run overshoots (~10 min)  |
| Total script wall clock      | ~20 min budget      | observed on reference host             |
| Per-probe cost reasonability | < 1 ms              | a probe ≥ 1 ms means a setup/state bug |

Two engines exceed the per-engine 5-minute target:

- **UltraSQL** — the v0.5 buffer pool's `acquire_frame_for` is O(N)
  in capacity (linear walk for the next empty frame) and the pool
  refuses to evict dirty pages, so the 10M-key build is O(N²) and
  takes ~6.5 minutes. The build is **setup**, not probe time; the
  probe loop itself runs in tens to hundreds of milliseconds.
- **ClickHouse** — a single 100k-probe pass through `clickhouse
  local` takes ≈ 10 minutes because each statement re-pays the
  parse/plan/open-parts cost. We deliberately cap at 1 measurement
  run for ClickHouse (everywhere else: 3 runs) so the total script
  fits the 20-minute budget. ClickHouse is not designed for OLTP
  point-lookup workloads; the table records the engine's number
  with a "1-run, capped" note.

If a run truly fails (process exit, missing engine, etc.) the JSON
records `{"skipped": true, "reason": ...}` and the `results.md` row
sorts to the bottom of the rank table.

## Hot-cache assumption

All engines are warmed before the measured iterations, so that:

- on-disk pages backing the table are resident in the engine's
  page/buffer cache (PostgreSQL's shared_buffers, DuckDB's
  buffer-manager, ClickHouse's part cache, or `:memory:` for SQLite);
- the prepared statement's plan is cached;
- any JIT is fully compiled (PostgreSQL JIT defaults to off for
  queries this small);
- the executor's per-operator state is allocated.

## What is *not* claimed

- This is not a TPC-H/TPC-DS/HammerDB workload. The query is one
  primary-key lookup at a time over a 10M-row table.
- The UltraSQL line measures the v0.5 `BTree<i64>` plus the in-memory
  buffer pool through the native Rust API. **There is no SQL
  pipeline yet** (parser/planner/executor lands at v0.5, see
  `ROADMAP.md`); the eventual end-to-end UltraSQL query will pay for
  the parse/plan/execute machinery on top. Treat the UltraSQL row as
  a **data-plane lower bound**, not a like-for-like end-to-end
  comparison.
- The SQL engines pay for their Python-binding call cost in this
  comparison. A C-language harness would shave a few hundred
  nanoseconds per engine, identically across SQLite/DuckDB/PG.
- macOS scheduler jitter, `cron`/Spotlight background activity, and
  thermal throttling are not controlled for beyond running on a
  plugged-in Mac mini with no other heavy workload. Three runs of
  100,000 probes each smooth most of this out, but for a publishable
  claim a longer run on a dedicated host would be required.

## Reproduction

```sh
cd benchmarks/results/comparison-2026-05-12-m4-pointlookup
bash run.sh
```

Pre-reqs:

- `duckdb`, `sqlite3`, `psql` on `PATH`.
- PostgreSQL running on `localhost:5432` with the current user able
  to create databases.
- The official ClickHouse binary at `/tmp/ultracmp/clickhouse`, or
  point `CH_BIN` at one.
- Python 3 with `duckdb` and `psycopg` installed
  (`pip3 install --user duckdb psycopg`).
- The UltraSQL workspace builds in release.

The script is idempotent: it drops and re-creates per-engine state
each invocation. Raw stdout/JSON per engine lands in `raw/`. The
Python parsing block at the end writes `results.json` and
`results.md`, then a console summary.
