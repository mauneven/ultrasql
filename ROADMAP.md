# UltraSQL Roadmap

Living file for open gates only. Completed/addressed work lives in
[DONE.md](DONE.md). When a gate closes, move the evidence there and keep this
file focused on what still blocks production.

## P0 - Release Blockers

### CI, Coverage, Release Evidence

- Keep `main` green before and after every slice: format, clippy, workspace
  tests, docs, cargo-audit, cargo-deny, driver certification, release build.
- Final release needs `operator soak reports`, `latest green CI workflow run id`,
  `release workflow run id`, `GitHub release notes`, and
  `operator_soak_status.json`, `external_audit_status.json`,
  `incident_drill_status.json`, and `driver_compatibility_status.json`
  recorded in the release checklist.
- Release candidate must rerun benchmark, fuzz, sanitizer, Miri, package, and
  smoke-install gates on the tagged commit.

### Operator Soak And Safety

- Three independent `operator-reports/*.json` files must pass strict validation
  for 30 continuous days through `.github/workflows/operator-soak.yml`.
  Operators can generate schema v2 reports with `scripts/run-operator-soak.py`.
- Two independent `external-audits/*.json` files must pass strict validation
  through `scripts/validate-external-audits.py --strict`; required audit types
  are `security` and `correctness`.
- Required `incident-drills/*.json` files must pass strict validation through
  `scripts/validate-incident-drills.py --strict`; required drill types are
  `backup_restore`, `wal_recovery`, and `disk_full`. Local smoke reports can
  be generated with `scripts/run-incident-drills.py`, but production sign-off
  requires `mode: "production"` reports.
- Driver compatibility must pass strict validation through
  `scripts/run-driver-release-evidence.py` and
  `scripts/validate-driver-compatibility.py --strict`; the status artifact
  records `required_driver_count`, `passing_required_driver_count`, and
  `missing_required_drivers` for the release commit.
- Zero open critical or high-severity correctness bugs.
- Fuzz testing: one clean week for parser, protocol, WAL decoder, and planner.
- Re-run chaos recovery on release candidates; completed random-kill,
  WAL-truncation, and disk-full runner evidence lives in `DONE.md`.

### Correctness Debt

- Serializable isolation has column-range SSI for supported scalar comparisons
  and fully supported multi-column `AND` / `OR` predicate trees plus
  relation-level fallback, but is `not fully predicate-precise` SSI. Implement
  page/tuple/gap precision before broad serializable claims.
- Full public regression breadth still open for broader upstream parser, type,
  catalog-sanity beyond the active shard, and isolation schedules. The curated
  regression subset is active without local skip debt; evidence lives in
  `DONE.md`.

### Performance Certification

- TPC-B: latest committed certification artifact is `target_not_met`; keep the
  correctness work, but do not claim throughput leadership or p99 < 5 ms until
  `benchmarks/results/latest/tpcb_certification.json` passes.
- TPC-C: latest committed certification artifact is `target_not_met`; all five
  transaction paths need a passing same-host PostgreSQL 17 comparison before a
  throughput-leadership claim.
- Sysbench OLTP read/write: latest committed certification artifact is
  `target_not_met`; keep the UltraSQL smoke path, but do not claim same-host
  PostgreSQL 17 leadership until `sysbench_certification.json` passes.
- ClickBench: dataset-backed same-host PostgreSQL 17 comparison, plus
  ClickHouse and Firebolt legs when available.
- Firebolt sparse primary-index pruning: pass
  `target_ratio_ultrasql_vs_firebolt <= 1.0`, require
  `Firebolt primary-index pruning evidence`, and keep the current local
  Firebolt Core state honest: `local Firebolt Core smoke measured`, but
  `Firebolt is not_available` when Core EXPLAIN does not expose pruning.
- Rerun TPC-H SF1/SF10 on the release host before publishing release
  performance claims; completed local evidence lives in `DONE.md`.
- Broaden release-artifact scale-sweep coverage beyond the current same-host
  table: larger row counts, repeatable ClickHouse artifacts, and Firebolt legs
  when local services are available.
- Durable bulk INSERT: the `wal buffer full` failure on a 1,000,000-row INSERT
  in `--data-dir` mode is now fixed in code. Per-record backpressure is wired —
  the WAL sink parks an appender on a transiently-full `WalBuffer` instead of
  rejecting (`crates/ultrasql-wal/src/buffer.rs` `WalBuffer::reserve`,
  `WalWriter::open` `set_drainer`) — and a single record larger than the 8 MiB
  buffer is now admitted over-capacity (bounded by `MAX_RECORD_BYTES`) instead
  of rejected. Exit condition: re-run the `--data-dir` scale sweep and record a
  measured `insert_throughput_1m-ultrasql` artifact (currently `not_available`,
  predating the fix), lifting the certification to 24 comparable measured rows.
- WAL retention / segment recycling: the WAL is never truncated — segments
  accumulate for the life of the database, so disk usage grows unbounded and
  every restart replays all history (the vector index rebuilds from `Lsn::ZERO`
  on each start). Checkpoint-driven truncation is blocked by four coupled
  prerequisites, each a silent-data-loss risk if done wrong: (1) DONE — the WAL
  LSN is an absolute byte offset from the start of history; a crc32c-checksummed
  `wal.manifest` now records the recovery floor (first surviving segment index +
  its absolute start LSN), `recover_with_target` seeds its byte cursor from the
  floor and ignores below-floor segments, and the server's redo and vector
  replay scanners seed from the same floor — so removing head segments no longer
  shifts reconstructed LSNs (behavior-neutral until truncation writes a
  non-origin floor); (2) DONE — commit/abort status was rebuilt by scanning all
  `Commit`/`Abort` records on every startup (no persistent CLOG). A durable,
  crc32c-checksummed `clog.snapshot` of every terminal `(xid, status)` is now
  written at checkpoint (`TransactionManager::export_clog`) and loaded at restart
  before the WAL scan (`import_clog`), used only when it decodes cleanly and its
  LSN is within the durable WAL end, so the `Commit`/`Abort` records below a
  checkpoint can be recycled without losing a committed transaction's status
  (behavior-neutral until truncation drops those records); (3) the checkpoint
  flush is a buffered `write_page` with no fsync, so
  a checkpoint must be made durable before its LSN can bound truncation
  (DONE: `perform_checkpoint` now fsyncs the data segments via
  `SegmentFileManager::fsync_all` before recording `last_checkpoint_lsn`); (4)
  vector indexes need a durable snapshot so their records can be removed without
  losing the only copy of historical index entries (DONE for HNSW: the
  `HnswPersistentPage` arena now has a versioned, crc32c-checksummed on-disk page
  format — `encode_snapshot`/`from_snapshot_bytes` — written per index at
  checkpoint and loaded at restart, with LSN-bounded replay
  (`apply_wal_record_at` + `redo_covered`) applying only the WAL above the
  snapshot's `meta.lsn`; a snapshot is trusted only if its `meta.lsn` is within
  the durable WAL end, else it falls back to full replay. IVFFlat still rebuilds
  from full WAL replay — its snapshot is the remaining piece of (4)). Exit
  condition: a long-running instance recycles WAL segments below a durable
  checkpoint, restart time is bounded by un-checkpointed work (not total
  history), and a disk-full + crash-recovery drill passes after truncation.
- OLTP commit-path losses (`insert_throughput_10k` → PostgreSQL,
  `mixed_oltp_pgbench_like` → SQLite/PostgreSQL): the per-commit cost is
  dominated by `full_fsync` (F_FULLFSYNC, true power-loss durability) in
  `crates/ultrasql-wal/src/writer.rs::flush_current`, which is a *stronger*
  guarantee than PostgreSQL's default macOS `fsync`. A durable-wait
  micro-optimization (condvar wakeup instead of a 50 µs sleep-poll) was
  profiled, implemented, and reverted after a same-host A/B showed no
  improvement (~354 µs/op old vs ~384 µs/op new), because the fsync, not the
  poll, dominates; see
  [`operator-reports/2026-06-benchmark-row-analysis.md`](operator-reports/2026-06-benchmark-row-analysis.md).
  No correctness-preserving win is available without weakening durability or
  changing commit semantics, so these are formally accepted as honest losses.
  Exit condition: either a committed multi-client OLTP artifact where group
  commit amortizes F_FULLFSYNC across concurrent committers and UltraSQL's tx/s
  is ≥ the winner's, or a same-durability-level comparison (both engines at
  F_FULLFSYNC, or both at `fsync`) where UltraSQL's per-commit p50 is ≤ the
  winner's. No OLTP leadership is claimed until then.
- Fair-measurement scoreboard honesty: on the same-host data-dir sweep with a
  tuned PostgreSQL 17, UltraSQL is fastest on 17 of 24 workloads. The
  certification gate (`scripts/validate-benchmark-certification.py`) now treats
  per-row losses as reported scoreboard data rather than a failure, so
  `benchmark_certification_status.json` can be `ready` on fair methodology while
  still recording the rows UltraSQL does not lead. Per-row root causes and exit
  conditions are in
  [`operator-reports/2026-06-benchmark-row-analysis.md`](operator-reports/2026-06-benchmark-row-analysis.md):
  `select_scan_100k` (→ ClickHouse, real ~6 % wire-encode anomaly that is
  monotonic at 10k/1M), `insert_throughput_10k` and `mixed_oltp` (→ PostgreSQL,
  per-commit OLTP cost), `update_throughput_1m`/`delete_throughput_100k` (→
  DuckDB) and `delete_throughput_1m` (→ ClickHouse) bulk column mutations. Exit
  condition for a leadership claim on any row: a committed sweep where
  UltraSQL's measured median is the lowest, with no harness regression.
- AI/vector competitor claims require same-host DuckDB, ClickHouse, and
  PostgreSQL+pgvector artifacts for exact top-k, ANN recall/latency, hybrid
  search, JSON-filtered retrieval, and RAG quality, with answer or recall gates
  before any README row is published.

## P1 - SQL Surface

### Type Surface

- `NUMERIC(p,s)` / `DECIMAL(p,s)`: arbitrary-precision runtime arithmetic
  remains open beyond the scaled-`i64` surface.
- `MONEY`: broader host/ICU monetary locale catalog beyond the built-in
  deterministic templates remains open; do not claim full system-locale parity.
- Date/time remaining: full timezone edge parity beyond the completed
  timestamp/timestamptz `AT TIME ZONE`, fixed-offset `TIMETZ AT TIME ZONE`,
  deterministic abbreviation parser, IANA timestamptz parser/session display,
  DateStyle output, and date-prefixed `TIMETZ` named-zone parser.
- Arrays: broader coercion breadth and every supported element family beyond
  the completed metadata/scalar/mutation subset.
- JSON/JSONB: full SQL/JSON path parity beyond the supported subset, including
  full `datetime(template)` grammar beyond the supported ISO second, minute,
  and fractional-second templates and non-ISO date/time coercions.
- Full-text search remaining: native lexeme/query storage beyond the current
  text-backed representation, dictionaries, full ranking/headline parity, and
  GIN planner integration.
- XML remaining surface: XPath axes/functions beyond the bounded secure subset
  recorded in `DONE.md`, plus full `XMLTABLE` beyond the completed typed
  scalar/default projection subset.
- Locale/collation remaining: ICU-backed collations, `CREATE COLLATION`,
  index/expression collation catalog deparse, and non-bytewise sort/search
  behavior beyond the supported built-in `default`/`C`/`POSIX` subset.

### Catalog, Roles, Privileges

- Replace restart-persisted role, membership, privilege, default-privilege,
  schema, sequence-owner, operator, and RLS runtime sidecars with typed catalog
  rows and migrations before v1.0; current sidecar evidence lives in
  `DONE.md`.
- Broaden remaining dependency tracking for every object kind and every
  `DROP ... CASCADE` / `RESTRICT` path.
- Keep security gates ethical: no proprietary tests, no closed-source
  code, no fake benchmark claims.

### Drivers, ORMs, Tools

- Keep certification green for `libpq`, `psql meta-commands`, `psycopg2`,
  `psycopg3`, `SQLAlchemy`, `Django ORM`, `Rails ActiveRecord`, `Hibernate ORM`,
  `GORM`, `Prisma`, `Diesel`, `node-postgres`, `pgx`, `lib/pq`,
  `JDBC`, and `Npgsql`.
- Keep stock psql meta-command coverage green: `\d`, `\dt`, `\di`, `\df`,
  `\dv`, `\du`, `\l`, `\dn`.
- GUI probes exist for `GUI introspection probes`, `pgAdmin`, `DBeaver`, and
  `DataGrip`; desktop launch/click smoke remains open.
- Migration tools must stay green: `Flyway`, `Liquibase`, and `Alembic`.

## P2 - Vector, Analytics, Lakehouse

### AI-Native Retrieval (hybrid search, filtered ANN, embedding MVCC)

Shipped: single-query hybrid retrieval fusing vector similarity, BM25 lexical
relevance, and SQL/JSON metadata filters via
`ORDER BY hybrid_search(text, query, vector, probe[, fusion]) DESC LIMIT k`,
with Reciprocal Rank Fusion (`'rrf'`) and weighted-linear fusion, reference-
checked tests, and `docs/hybrid-search.md`. Also shipped: transactional
embedding consistency — text+vector+metadata in one transaction, online
vector-index MVCC with crash + WAL-replay recovery, embedding versioning, and
bring-your-own-vectors (see DONE.md "Transactional Embedding Consistency" and
`docs/transactional-embeddings.md`). Open items below carry the
measured baseline taken on 2026-06-16 (unfiltered HNSW recall@10 = 0.998 at
p50 ≈ 257 µs on 2k×16d via `benchmarks/vector_ann_hnsw.sh`):

- Filtered-ANN remaining work (the core shipped; see DONE.md "Persistent
  approximate HNSW + filtered ANN"): (a) selective metadata filters fall back to
  an exact O(N) scan because there is no persistent metadata index to pre-filter
  cheaply — exit condition: a metadata-index pre-filter path so selective
  filtered-ANN p50 is sub-millisecond at 1M rows, recall 1.0; (b) IVFFlat-indexed
  vector columns still use the exact filter+sort path — exit condition: a
  probes-based IVFFlat over-fetch with a committed recall artifact.
- HNSW index build scales O(N²) (PART 3): every inserted vector scans all live
  nodes to pick neighbors, so build time grows quadratically — measured ~424 s
  for 50k×128d on Apple M4 vs pgvector's ~5 s in the same SIFT comparison
  (`benchmarks/vector_ann_sift.sh`), which makes SIFT1M-scale builds impractical.
  Query recall and latency are already competitive (recall@10 0.986 at ef=64,
  1.000 at ef=200; see `docs/vector-benchmarks.md`); the gap is build, not search.
  Exit condition: graph-search-based candidate selection at insert (find the
  `ef_construction` nearest via the partially-built graph instead of a full scan)
  so a 1M×128d build completes in minutes with recall@10 ≥ 0.95 at ef ≤ 128, plus
  a committed SIFT1M artifact from the server wire path. The same O(N²) cost is
  paid again on crash recovery, which replays the inserts — `vector_soak.sh full`
  measured ~40 s to recover a 20k-node graph; the same insert-time fix bounds it.
- Hierarchical HNSW layers (PART 3): the persistent graph is a single navigable
  layer. Recall holds through 50k with the diversity heuristic, but a multi-layer
  graph would lower the ef needed for a given recall at large N. Exit condition:
  per-node levels with per-level neighbor lists, crash/WAL-replay recovery tests,
  and a recall/latency artifact showing lower ef-for-recall at ≥ 100k vs the
  single-layer baseline.
- Agent memory primitives (PART 4): exit condition: tenant/namespace isolation
  enforced and tested under concurrency (no cross-tenant leakage), deterministic
  time-decay ranking (`relevance × decay(age)`), and TTL/decay eviction as
  tested SQL, with a documented agent-memory example.
- Retrieval observability (PART 5): exit condition: `EXPLAIN ANALYZE` on a
  hybrid query reports index choice, candidates examined/pruned, filter
  selectivity, per-component scores, and a recall estimate, with tests asserting
  the explain reflects the executed path.
- Killer demo + competitive benchmarks (PART 6): shipped. Fair
  `recall@k`-with-latency benchmarks versus PostgreSQL 17 + pgvector, LanceDB,
  and Qdrant in their recommended configs — same-host SIFT, computed exact ground
  truth, recall always paired with latency (`benchmarks/vector_ann_sift.sh`,
  `docs/vector-benchmarks.md`). And a runnable Node RAG demo (`examples/node-rag`)
  that ingests text + embedding + metadata in one transaction, retrieves with one
  fused vector + BM25 + metadata query, and returns the identical answer from a
  fresh, WAL-recovered process. See DONE.md "Honest vector benchmark suite" and
  "Embedded Node RAG demo".

### ANN And pgvector

- Production ANN certification: `Page-backed HNSW` and `Page-backed IVFFlat`
  need `large-scale recovery certification`, page-level torn-write handling,
  deeper VACUUM/rebuild stress, `CREATE INDEX CONCURRENTLY`, filtered-query
  fallback policy, and `larger recall/latency artifacts`.
- Keep ANN WAL coverage expanding: crash/restart DML rebuild, corrupt-WAL
  unavailable fallback, and `WAL replay fuzz/property tests`.
- pgvector parity: larger exact top-k profiles, filtered exact search,
  SQL-level HNSW/restart correctness, IVFFlat recall/latency, vector
  arithmetic beyond dense `sum`/`avg`, and broader cast/function cert.

### AI Gauntlet

- AI memory engine: implement the strategy in
  `docs/ai-database-strategy.md` as a SQL table surface that combines MVCC
  state, columnar shadow scans, BM25, vector ANN, JSON metadata, tenant/time
  projections, and auditable `EXPLAIN ANALYZE` path evidence.
- Keep `AI gauntlet measured artifacts` expanding across: `exact top-k`,
  `HNSW ANN recall/latency`, `hybrid search latency`,
  `filtered vector search`, `RAG retrieval quality`,
  `memory per million vectors`, `ingestion throughput`, and
  `cold-start index load`.
- Broaden the completed UltraSQL AI gauntlet into competitor comparisons:
  DuckDB VSS/HNSW, ClickHouse vector similarity/HNSW, PostgreSQL+pgvector
  HNSW/IVFFlat, Firebolt Core vector search, and optional local vector-service
  adapters when reproducible local setup exists.
- Scale AI artifacts beyond the current full profile: larger rows, larger
  dimensions, more probes, metadata-filtered recall/latency, SQL-level HNSW
  restart correctness, and publishable p50/p95/p99 tables.
- For every new published AI/vector claim, commit dataset or fetch instructions,
  host descriptors, raw artifacts, answer checksums, recall target, failure
  reasons, and same-host competitor configuration.

### Columnar And External Data

- Broaden `Columnar scan path` certification beyond the completed contract that
  heap rows remain the OLTP/MVCC source of truth while `HeapAccess::column_cache`
  provides the OLAP shadow path with committed DML invalidation.
- CSV scans: certify larger cross-engine runs and keep parser-buffer reuse
  evidence current for local and object-store inputs.
- Parquet/object-store scans: certify predicate/projection pushdown, object
  range reads, and lakehouse workloads against external engines.
- Iceberg: deletes, time travel, catalog integration, and certification.
- Arrow: Flight endpoint and wider type coverage.

## P3 - Packaging, Distribution, Operations

- Promote package publication evidence from the `release workflow`:
  `docs.ultrasql.org`, `ghcr.io/mauneven/ultrasql`, `packages/npm`,
  `npm publish`, `Windows setup EXE`, `Chocolatey`, `AUR`,
  `yay -S ultrasql-bin`, `Homebrew tap`, `clean GHCR platform list`,
  `Homebrew`, `Debian`, and `RPM`.
- Homebrew core path: use the source-built formula as the base for upstream
  review once UltraSQL is stable/notable enough for plain `brew install
  ultrasql` without tapping `mauneven/tap`.
- Add or verify required secrets: `NPM_TOKEN`, `HOMEBREW_TAP_TOKEN`,
  `AUR_SSH_PRIVATE_KEY`, `CHOCOLATEY_API_KEY`, package signing, and Windows
  code-signing material.
- Harden VACUUM/autovacuum scheduling, freeze policy, deeper bloat metrics,
  and production docs; completed maintenance counters and split
  vacuum/analyze trigger evidence are tracked in `DONE.md`.
- Finish streaming replication, synchronous replication modes, backup/PITR
  restore drills, logical replication, and `pgoutput`.
- Broaden remaining `pg_stat_*` operator views, lock/io wait-event population,
  deeper lock/query timing precision, and production dashboards; completed
  activity lifecycle states, client-read waits, and xact/query start timing are
  tracked in `DONE.md`.

## P4 - Later SQL Surface

- Views and materialized views: updatable views, `WITH CHECK OPTION`,
  dependency-safe view replacement, refresh, and materialized-view indexes.
- PL/pgSQL and procedures: variables, control flow, dynamic SQL, exceptions,
  cursors, `%TYPE`, `%ROWTYPE`, `SETOF`, OUT/INOUT, and `CALL`.
- Triggers: row/statement triggers, `INSTEAD OF`, `NEW`/`OLD`, `WHEN`,
  constraint triggers, and ordering.
- Table partitioning: RANGE/LIST/HASH, attach/detach, pruning, partition-wise
  joins/aggregates, insert routing, cross-partition updates, `Append`, and
  `MergeAppend`.
- Remaining indexes: SP-GiST, bloom filters, and complete GIN/GiST opclasses.
- Hardware performance: x86 AVX-512 CI/bench certification.
- Distributed/lakehouse future: coordinator, Raft catalog, sharding, distributed
  query execution, Arrow Flight, result cache, FDW API, extension loading,
  background workers, event triggers, `pg_stat_statements`, and trigram search.

## Roadmap Hygiene

- New entries need a measurable exit condition, not intent prose.
- Closed entries move to `DONE.md` with artifact paths, commands, or tests.
- No benchmark claim lands unless it is reproducible from committed scripts and
  raw artifacts.
