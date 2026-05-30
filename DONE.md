# DONE

Completed/addressed work moved out of [ROADMAP.md](ROADMAP.md). Keep this file
as a concise evidence ledger; roadmap stays for open gates only.

## Release And Packaging Automation

- Release workflow builds and attaches release archives for Linux, macOS, and
  Windows, including normal `Windows setup EXE` installer assets.
- Docker image path publishes `ghcr.io/mauneven/ultrasql:<tag>` and keeps GHCR
  attestations disabled so package UI shows a `clean GHCR platform list`.
- npm / pnpm installer support exists under `packages/npm`; package metadata uses
  the clean `ultrasql` name and supports `npm publish --access public
  --provenance` when credentials are configured.
- External npm registry cutover is complete: public package `ultrasql` latest
  version `0.0.6` was verified from `https://registry.npmjs.org/ultrasql/latest`
  with repository directory `packages/npm`.
- Homebrew support exists through a source-built `packaging/homebrew/ultrasql.rb.in`
  formula and `scripts/render-homebrew-formula.sh`.
- AUR support exists through `packaging/aur/PKGBUILD.in`,
  `packaging/aur/.SRCINFO.in`, and `scripts/render-aur-package.sh`, rendering
  `ultrasql-bin` for `yay -S ultrasql-bin`.
- Chocolatey support exists through `packaging/chocolatey/` and checksum-pinned
  `.nupkg` generation.
- Debian and RPM support exists through `packaging/nfpm.yaml.in` plus hardened
  systemd packaging files.
- Docs site path exists for `docs.ultrasql.org`; `docs/CNAME` and
  `.github/workflows/docs.yml` build with `mkdocs build --strict`.
- Release notes automation exists: `.github/workflows/release.yml`,
  `docs/release-notes-template.md`, `.github/release.yml`, and
  `scripts/render-release-notes.sh`.

## Baseline, CI, Security

- Baseline audit covered CI, coverage, benchmark, release, and roadmap drift.
- Statement timeout plus cancel propagation for long queries landed and is
  tested.
- Idle session slow-loris timeout is configurable and tested.
- Data-dir ownership and mmap-aliasing threat model fixes landed.
- `cargo audit` and `cargo deny` CI gates are wired.
- Coverage workflow proof exists; runtime/shipped crate per-crate 80% line
  enforcement is wired in `.github/workflows/coverage.yml`.
- Coverage audit refreshed on 2026-05-28. Full workspace `cargo llvm-cov`
  passed test execution, `scripts/coverage_gate.py --min-lines 80` produced
  `docs/testing/coverage-evidence-2026-05-28.md`, and `ultrasql-node` now
  clears the per-crate gate at 84.00%.
- Focused coverage tests now exercise Arrow import/export edge paths,
  object-store URI/range/list/signing paths, and Iceberg metadata planning
  edge paths. Package-scoped `cargo llvm-cov` plus
  `scripts/coverage_gate.py --min-lines 80` clears `ultrasql-arrow` at
  87.24%, `ultrasql-iceberg` at 87.32%, and `ultrasql-objectstore` at
  88.28%. The later 2026-05-29 full workspace artifact supersedes the earlier
  local `errno=28 (No space left on device)` attempt.
- Focused `ultrasql-core` coverage now exercises bit-string binary/type
  contracts, money parsing and binary payloads, network wire/bitwise paths,
  custom type storage helpers, XML/XPath security edges, range/geometry
  predicates, array scalar coercions, and `TIMETZ` parsing/packing. Package
  `cargo llvm-cov` plus `scripts/coverage_gate.py --min-lines 80` clears
  `ultrasql-core` at 80.46%. Evidence:
  `docs/testing/coverage-evidence-2026-05-29-core.md`.
- Focused `ultrasql-sqllogictest-runner` coverage now exercises parser
  directives, skip filters, malformed-record errors, reference-engine
  selection, benchmark artifact JSON/Markdown rendering, hash expectation
  checks, and file collection. This also fixed a skip-filter parsing bug where
  whole-line trimming made empty-pattern validation unreachable. Package
  `cargo llvm-cov` plus `scripts/coverage_gate.py --min-lines 80` clears
  `ultrasql-sqllogictest-runner` at 85.46%. Evidence:
  `docs/testing/coverage-evidence-2026-05-29-sqllogictest-runner.md`.
- Focused `ultrasql-cli` coverage now exercises connection resolution,
  pgpass parsing, meta-command dispatch, local result rendering, ops HTTP
  readiness, WAL dump/archive/restore helpers, `pg_ctl`-style signal writers,
  basebackup, dump/restore, WAL receiver cascade, validation output, and
  `ultrasql-local` file/query helpers. Package `cargo llvm-cov` plus
  `scripts/coverage_gate.py --min-lines 80` clears `ultrasql-cli` at 84.96%.
  Evidence: `docs/testing/coverage-evidence-2026-05-29-cli.md`.
- Focused `ultrasql-planner` coverage now exercises table-reference binding,
  local CSV/JSON inference, Arrow type mapping, expression literals/coercions,
  builtin validation, window binding, catalog OID resolution, expression
  display/accessors, logical plan display/schema/pipeline paths, and privilege
  binding matrices. This also hardened window negative-literal extraction with
  checked negation for integer, decimal, and money defaults. Package
  `cargo llvm-cov` plus `scripts/coverage_gate.py --min-lines 80` clears
  `ultrasql-planner` at 80.19%. Evidence:
  `docs/testing/coverage-evidence-2026-05-29-planner.md`.
- Focused `ultrasql-server` coverage now exercises COPY text/binary edge cases,
  result encoding, transaction state transitions, EXPLAIN rendering, metadata
  statements, privilege collection/enforcement, JSON_TABLE lowering, recursive
  CTE set helpers, TPC-H sidecar caches, Q1 columnar summaries, ops HTTP paths,
  and WAL archive/restore edges. This also fixed binary COPY UUID encoding,
  text COPY `bytea` hex validation, and recursive CTE DISTINCT NULL-bitmap
  preservation. Package `cargo llvm-cov` plus
  `scripts/coverage_gate.py --min-lines 80` clears `ultrasql-server` at
  80.06%. Evidence: `docs/testing/coverage-evidence-2026-05-29-server.md`.
- Focused `ultrasql-executor` coverage now exercises scalar compatibility
  functions, physical lowering edge families, row encoding/decoding,
  projection/filter/sort/unique/set/window/hash aggregate behavior, modify-table
  constraints/index maintenance, and executor profile paths. This also fixed
  non-contiguous window partition grouping, BOOL builder NULL preservation, and
  INTERVAL row-codec coverage. Package `cargo llvm-cov` plus
  `scripts/coverage_gate.py --min-lines 80` clears `ultrasql-executor` at
  80.24%. Evidence: `docs/testing/coverage-evidence-2026-05-29-executor.md`.
- Full workspace coverage refreshed on 2026-05-29. `cargo llvm-cov --workspace
  --all-features` passed test execution, `cargo llvm-cov report --lcov` wrote
  `target/llvm-cov/lcov.info`, and `scripts/coverage_gate.py --min-lines 80
  --exclude-crate ultrasql-bench` cleared 19 checked crates with 0 below
  threshold. Evidence:
  `docs/testing/coverage-evidence-2026-05-29-workspace.md`. `ultrasql-bench`
  is excluded from the line gate because it is a non-published benchmark
  harness with external-engine driver paths; the raw unexcluded value is
  recorded in the evidence file at 46.98% and remains covered by benchmark
  profile, artifact-schema, release-hardening, and smoke certification tests.
- Driver-certification CI was repaired, action runtimes refreshed, and release
  workflows validated on `main`.
- Chaos testing: random kill, WAL truncation, disk full recovery is implemented
  through `benchmarks/chaos_recovery.sh` and writes
  `benchmarks/results/latest/chaos_recovery_manifest.json`.
- Backup/restore smoke runner covers `ultrasql --basebackup`,
  `ultrasql --pg-dump`, `ultrasql --pg-restore`, row counts, and indexed lookup.
- Backup/restore dump-format certification now covers custom, directory, and
  tar dump output. The 2026-05-30 smoke artifact
  `benchmarks/results/latest/backup_restore_smoke_manifest.json` records
  `status: measured`, `dump_formats_verified: ["custom", "directory", "tar"]`,
  matching source/restored row counts, and indexed point-query result `bravo`
  for every format.
- Persistent typed-catalog bootstrap now reloads user tables, indexes,
  `pg_statistic`, and `pg_statistic_ext` from heap storage and hydrates the
  optimizer stats catalog after WAL commit-status recovery. Evidence:
  `crates/ultrasql-server/tests/analyze_round_trip.rs` covers `ANALYZE`
  statistics surviving restart as both `pg_statistic` rows and cost-model
  `lookup_relation_stats` entries.
- Durable table-runtime expression and constraint bootstrap is certified for
  restart: defaults, generated stored expressions, CHECK constraints, foreign
  keys, exclusion constraints, identity defaults, domain checks, and
  `TRUNCATE ... CASCADE` FK dependency walks all have restart coverage under
  `crates/ultrasql-server/tests/*_round_trip.rs`.
- Catalog upgrade story is documented and enforced with `catalog.version = 1`.
- Security/ethics audit docs cover no proprietary tests, no closed-source
  code, and no fake benchmark claims.
- Constant `SELECT` result execution now propagates scalar evaluation failures
  instead of silently returning NULL. Evidence:
  `cargo test -p ultrasql-executor result_propagates_constant_eval_errors`.
- `ORDER BY` full-sort and bounded top-k paths now propagate sort-key
  evaluation failures instead of silently treating them as NULL. Evidence:
  `cargo test -p ultrasql-executor sort` and
  `cargo test -p ultrasql-executor top_k`.
- `HashAggregate` and `SortAggregate` now propagate group-key,
  aggregate-argument, and ordered-set percentile expression failures instead of
  silently treating them as NULL. Evidence:
  `cargo test -p ultrasql-executor hash_aggregate` and
  `cargo test -p ultrasql-executor sort_aggregate`.
- `HashJoin` and `MergeJoin` now propagate join-key evaluation failures instead
  of silently treating them as NULL non-matches. Evidence:
  `cargo test -p ultrasql-executor hash_join` and
  `cargo test -p ultrasql-executor merge_join`.
- `GatherMerge` now propagates merge-key evaluation failures instead of
  silently comparing them as NULL. Evidence:
  `cargo test -p ultrasql-executor gather`.
- `WindowAgg` now propagates partition-key, order-key, and window value
  expression failures instead of silently returning NULL/default values.
  Evidence: `cargo test -p ultrasql-executor window_agg`.
- `TidBitmap` now returns checked `ExecError` values for out-of-range row
  setting and capacity-mismatch merges instead of panicking. Evidence:
  `cargo test -p ultrasql-executor bitmap_heap_scan`.
- Planner production binder paths no longer use non-test `unwrap`/`expect`:
  `CREATE TABLE`, `CREATE DOMAIN`, and date-interval month conversion now
  surface typed `PlanError`s. Evidence:
  `cargo test -p ultrasql-planner binder::tests`,
  `cargo test -p ultrasql-planner expr_bind`, and the non-test panic audit.
- Constant folding now rewrites window `PARTITION BY`, window `ORDER BY`, and
  value expressions inside `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, and
  `NTH_VALUE` even when the window input is already a fixed point. Evidence:
  `cargo test -p ultrasql-optimizer constant_fold` and
  `cargo test -p ultrasql-optimizer`.
- Decimal sort comparison no longer treats scale-normalization overflow as
  equality; high-scale `NUMERIC` ordering uses overflow-safe digit alignment
  shared by full sort and top-k. Evidence:
  `cargo test -p ultrasql-executor sort` and
  `cargo test -p ultrasql-executor top_k`.
- Optimizer statistics now order decimal `NUMERIC` values by exact scaled
  magnitude and canonicalize equal decimal keys before MCV/histogram grouping.
  Evidence: `cargo test -p ultrasql-optimizer value_ord`.
- SQL predicate evaluation now compares high-scale decimal `NUMERIC` values
  without rescale overflow, preserving exact ordering for mixed-scale
  comparisons. Evidence:
  `cargo test -p ultrasql-executor decimal_compare_handles_large_scale_gap_without_overflow`
  and `cargo test -p ultrasql-executor eval`.
- Hash join keys now canonicalize decimal `NUMERIC` values before equality and
  hashing, so values such as `1.0` and `1` match without scale-sensitive false
  negatives. Evidence:
  `cargo test -p ultrasql-executor hash_join_matches_decimal_keys_across_scales`
  and `cargo test -p ultrasql-executor hash_join`.
- DISTINCT and set-operation row keys now share the same decimal `NUMERIC`
  canonicalization as hash join keys, covering hash DISTINCT, sort DISTINCT row
  equality, UNION, INTERSECT, and EXCEPT key semantics. Evidence:
  `cargo test -p ultrasql-executor unique` and
  `cargo test -p ultrasql-executor set_op`.
- Hash aggregate group keys and aggregate `DISTINCT` keys now use shared
  decimal `NUMERIC` canonicalization, so grouped and distinct aggregate paths
  no longer split equal values by display scale. Evidence:
  `cargo test -p ultrasql-executor hash_aggregate`.
- Sort aggregate group-key equality now uses shared decimal `NUMERIC`
  canonicalization, keeping sorted GROUP BY boundaries aligned with hash GROUP
  BY semantics for mixed-scale equal values. Evidence:
  `cargo test -p ultrasql-executor sort_aggregate`.
- Sort and top-k output cursors now return typed internal executor errors
  instead of panicking on missing iterator state; external sort run selection
  also avoids `expect` on stale heads. Evidence:
  `cargo test -p ultrasql-executor sort` and
  `cargo test -p ultrasql-executor top_k`.
- Work-memory reservation release no longer uses a production `expect` in the
  RAII drop path; the saturating atomic release remains behavior-compatible.
  Evidence: `cargo test -p ultrasql-executor work_mem`.
- Vectorized pipeline builder now resolves terminal schema with a typed
  `ExecError::Internal` instead of a production `expect`. Evidence:
  `cargo test -p ultrasql-executor push_pipeline`.
- Row codec builder decode now rejects invalid UTF-8 with a borrowed
  `Utf8Error` variant instead of constructing the error through
  `expect_err`, preserving the no-allocation validation path. Evidence:
  `cargo test -p ultrasql-executor row_codec`.
- Row codec fixed-width `Int64` builder fast paths now use the shared
  `read_fixed` typed truncation helper instead of production `try_into`
  `expect` calls. Evidence: `cargo test -p ultrasql-executor row_codec`.
- SELECT wire streaming now returns a typed server error and rolls back the
  partial row buffer when a logical typed cell cannot be rendered, covering
  corrupt `TIMETZ` physical payloads without a panic. Evidence:
  `cargo test -p ultrasql-server
  write_data_row_typed_rejects_invalid_timetz_payload_without_partial_row`.
- SELECT wire streaming now validates schema and batch column arity before
  touching the output buffer, returning a typed server error instead of
  panicking through `Schema::field_at` on malformed operator output. Evidence:
  `cargo test -p ultrasql-server
  write_data_row_typed_rejects_schema_column_mismatch_without_partial_row`.
- SELECT wire streaming now validates row indexes against every physical
  column before touching the output buffer, returning a typed server error
  instead of panicking on malformed batch row counts. Evidence:
  `cargo test -p ultrasql-server
  write_data_row_typed_rejects_row_index_out_of_bounds_without_partial_row`.
- SELECT wire physical cell fallback and integer text formatting no longer
  depend on production `expect` paths; existing byte-equivalence tests keep
  the hot writer output unchanged. Evidence:
  `cargo test -p ultrasql-server wire_writer` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Recursive CTE DISTINCT key encoding and unseen-row filtering now return
  typed server errors for oversized text keys or impossible filtered-column
  length mismatches instead of relying on production `expect` paths. Evidence:
  `cargo test -p ultrasql-server cte_helpers` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Fused UPDATE/DELETE lowering now treats out-of-range hot-path column indexes
  as a non-match for the fused path instead of relying on `expect` after
  `usize -> u8` conversions. Evidence:
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Listener session draining now handles a drained `JoinSet` with an explicit
  match instead of relying on a production `expect` in the accept loop.
  Evidence:
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- CSV header sniffing now reports an explicit parse error if the CSV parser
  ever returns no record after a single-record length check, removing
  production `expect` from `read_csv` setup. Evidence:
  `cargo test -p ultrasql-server csv_scan` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Swapped hash-join lowering now propagates schema-construction failures as
  `ServerError::Execute` instead of panicking on `Schema::new`. Evidence:
  `cargo test -p ultrasql-server join` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Cached aggregate summary null sentinels now return `None` and fall back when
  nullable column construction fails, instead of panicking in the fast-path
  cache helper. Evidence:
  `cargo test -p ultrasql-server projection_summary` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Sample database bootstrap now logs and returns an empty sample registry if
  static fixture schema or batch construction fails, preserving API
  compatibility while removing production `expect` paths. Evidence:
  `cargo test -p ultrasql-server pipeline::tests` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- TPC-H Q1 static schema helper is now compiled only for tests, keeping
  test-only schema assertions out of production panic audits. Evidence:
  `cargo test -p ultrasql-server pipeline::tests::tpch_sidecars` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Time-partition INSERT affected-row schema construction now logs and falls
  back to an empty schema instead of panicking on an impossible static schema
  failure. Evidence:
  `cargo test -p ultrasql-server time_partition` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Virtual catalog schema construction and signed built-in OID conversion now
  log and return safe fallback values instead of panicking on static catalog
  mistakes. Evidence:
  `cargo test -p ultrasql-server catalog_views` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- SCRAM password hashing and proof verification now propagate HMAC/PBKDF2
  initialization failures through `AuthError`/`ServerError` instead of using
  production `expect` in auth crypto paths. Evidence:
  `cargo test -p ultrasql-server scram` and
  `cargo clippy -p ultrasql-server --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Executor `ModifyTable` affected-row schema construction now logs and falls
  back to an empty schema instead of panicking on an impossible static schema
  failure. Evidence:
  `cargo test -p ultrasql-executor modify` and
  `cargo clippy -p ultrasql-executor --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.

## Core SQL And Wire Protocol

- Simple Query and Extended Query dispatch are wired for parse, bind, describe,
  execute, sync, close, flush, and prepared-statement round trips.
- Explicit transactions work through Simple and Extended Query:
  `BEGIN`, `COMMIT`, `ROLLBACK`, failed-block SQLSTATE `25P02`, and
  `ReadyForQuery` status bytes.
- `ORDER BY`, joins, set operations, `BETWEEN`, index scans, transaction blocks,
  plan cache, and optimizer routing are wired through server execution.
- B-tree handles now share per-relation block allocation and operation latches,
  preventing reopened index handles from reusing leaf blocks during concurrent
  splits.
- Key-stable indexed UPDATE paths keep indexes anchored through HOT/classic
  `ctid` chains. Point probes, range scans, late materialization, and
  `ON CONFLICT DO UPDATE` now resolve the live tuple behind old indexed TIDs.
- SCRAM-SHA-256, optional MD5 auth, TLS, CancelRequest, COPY text/CSV, and
  LISTEN/NOTIFY base surfaces exist.
- Parser, binder, optimizer, executor, storage, MVCC, WAL, catalog, protocol,
  server, CLI, and benchmark crates have working public surfaces and regression
  tests.
- Row-value `IN` over tuple lists now binds through row constructors, evaluates
  record equality with SQL three-valued field semantics, passes wire coverage,
  and removes the public select-regression skip.
- Direct scalar aggregate fast path for `SUM` / `AVG` now handles nullable
  `INT` / `BIGINT` inputs by skipping NULL rows while keeping dense batches on
  the SIMD kernel path.

## Type And Function Surface

- Exact `NUMERIC` / `DECIMAL` base-10000 storage, row/COPY/wire
  payloads, exact scaled arithmetic, scale rounding, text casts, and OID
  coverage exist. Declared `NUMERIC(p,s)` precision is enforced on heap writes
  with SQLSTATE `22003` for overflow, and invalid zero precision typmods are
  rejected at bind time with SQLSTATE `42804`; arbitrary precision remains open.
- `MONEY` type surface, signed-cent storage, OID 790, wire, COPY, catalog
  persistence, checked unary signs, checked addition/subtraction, checked
  integer division, rounded floating-point division, checked scalar
  multiplication, money ratio division, runtime money/numeric/text casts, and
  deterministic `lc_monetary` GUC round trips, and behavior tests exist.
- `CHAR(n)` / `bpchar` parser, binder, row codec, executor, OID 1042, COPY,
  catalog persistence, blank padding, assignment/cast truncation, and
  trailing-space comparison semantics exist.
- `VARCHAR(n)` now enforces character-length bounds through heap row encoding,
  returns SQLSTATE `22001` on overlength wire inserts, preserves the bound in
  durable table metadata, and removes the parser/type regression skip for
  overlength `INSERT`.
- `DATE`, `TIME`, `TIMETZ`, `TIMESTAMP`, `TIMESTAMPTZ`, and `INTERVAL` runtime
  types exist. `TIMETZ` has parser, binder, row codec, executor, COPY, catalog
  persistence, OID 1266, ISO display, casts, coercions, and offset comparison.
- `BIT(n)` / `BIT VARYING(n)` storage, row codec, operators, wire OIDs, COPY,
  and end-to-end tests exist.
- `INET`, `CIDR`, `MACADDR`, and `MACADDR8` storage, operators, wire OIDs,
  COPY, and end-to-end tests exist.
- JSON and JSONB have distinct runtime/catalog/wire identity, JSON validation
  with text preservation, JSONB normalization, COPY, extended params, operator
  evaluation, and regression tests.
- JSON functions landed for `json_build_object`, `jsonb_set`, `json_each`,
  `jsonb_path_query`, `jsonb_path_exists`, JSON_TABLE subset paths, and
  whole-row `row_to_json`.
- Native arrays support multi-dimensional rectangular text/runtime round trips,
  GIN-facing operators, array subscripts/slices, `array_agg`, `array_length`,
  `array_cat`, `array_to_string`, `string_to_array`, and wire-visible `unnest`.
- `CREATE TYPE ... AS ENUM`, `CREATE TYPE ... AS (composite)`, and
  `CREATE DOMAIN` have durable catalog storage, restart round trips, and wire
  type OID coverage.
- `OID`, `REGCLASS`, `REGTYPE`, and `PG_LSN` parser/binder/runtime/storage/wire
  support exists.
- Basic XML storage exists with validated text storage, OID 142 wire rendering,
  COPY, and restart round trip.
- XML scalar functions now cover local-only secure well-formed checks
  (`xml_is_well_formed`, `xml_is_well_formed_content`,
  `xml_is_well_formed_document`) plus a deterministic `xpath` /
  `xpath_exists` subset for absolute element paths with optional attribute
  equality filters. DTD declarations, external entity expansion, unknown entity
  references, and pre-root junk are rejected.
- XML syntax now covers `XMLPARSE(DOCUMENT|CONTENT ...)` and
  `XMLSERIALIZE(DOCUMENT|CONTENT ... AS TEXT)`, with malformed `DOCUMENT`
  inputs rejected through the wire path. Evidence:
  `cargo test -p ultrasql-server --test xml_round_trip xml_functions_validate_securely_and_extract_simple_xpath`.
- Ordered-set aggregates `PERCENTILE_CONT` and `PERCENTILE_DISC` have plan shape
  and wire coverage.
- Portable scalar helpers now cover `COALESCE`, `IFNULL` / `NVL`,
  `NULLIF`, `LEAST`, `GREATEST`, and SQLite-style multi-argument scalar
  `MIN` / `MAX` through wire round-trip tests.

## Security And Client Certification

- `CREATE ROLE / USER`, `ALTER ROLE`, and `DROP ROLE` work through the role
  catalog and `pg_roles` / `pg_user` visibility.
- `GRANT / REVOKE` on tables, schemas, databases, sequences, and functions work
  through privilege catalog checks.
- Column-level privileges enforce `SELECT`, `INSERT`, and `UPDATE` target
  access.
- Role inheritance and `SET ROLE` support transitive membership, cycle
  rejection, `INHERIT` / `NOINHERIT`, `RESET ROLE`, `current_user`, and
  `session_user`.
- Default privileges apply matching templates for future tables and sequences.
- Persistent RLS policies cover owner, superuser, `BYPASSRLS`, and restart
  semantics for the documented RAG tenant policy shape.
- Driver certification covers `libpq`, `psycopg2`, `psycopg3`,
  `node-postgres`, `pgx`, `lib/pq`, `JDBC`, `Npgsql`, and
  `tokio-postgres`.
- ORM certification covers `SQLAlchemy`, `Django ORM`, `Rails ActiveRecord`,
  `Hibernate ORM`, `GORM`, `Prisma`, and `Diesel`.
- psql meta-command coverage exists for `\d`, `\dt`, `\di`, `\df`, `\dv`,
  `\du`, `\l`, and `\dn`.
- `GUI introspection probes` exist for `pgAdmin`, `DBeaver`, and `DataGrip`.
- Migration tool certification covers `Flyway`, `Liquibase`, and `Alembic`.

## Benchmarks And Performance Evidence

- Benchmark policy: published claims must trace to committed scripts, raw
  artifacts, and recorded host descriptions.
- SQL-surface benchmark work made UltraSQL lead the tracked low-tier workloads
  in the committed matrix; no blanket claim is allowed beyond recorded
  artifacts.
- Release-artifact scale sweep exists through `benchmarks/run_scale_sweep.sh`
  and artifacts under `benchmarks/results/latest/scale-sweep/`; the latest
  v0.0.6 same-host run builds the harness with `release-ship`, launches
  external `ultrasqld`, and records UltraSQL as the fastest engine on every
  published DuckDB/ClickHouse/SQLite/PostgreSQL row.
- Mixed correctness benchmark coverage exists in the release-artifact scale
  sweep: each measured engine runs write + write + aggregate inside a rolled-back
  transaction, emits an `answer_sha256`, and
  `benchmarks/scripts/render_scale_sweep.py` refuses to rank the row unless all
  measured answers match. Latest 100k-row artifact records UltraSQL fastest at
  153.38 us with matching answer hash
  `a4bb5c94eb7ea1c1d2c927b57b7da3ae26d2c455d5e60f54b7b57b4ede93f06b`.
- ClickHouse is now a first-class release-artifact scale-sweep leg through
  `benchmarks/scripts/run_clickhouse_writes.sh`; missing local ClickHouse setup
  records `not_available` instead of dropping the measured engine from rendered
  benchmark tables.
- TPC-B certification evidence was refreshed after indexed-update row-lock
  contention fixes: `benchmarks/tpcb_certify.sh` now writes the kernel smoke as
  explicit JSON, `benchmarks/results/latest/raw/tpcb_32conn-ultrasql.json`
  records 8,404.68 tx/s with correctness passing, and
  `benchmarks/results/latest/tpcb_certification.json` remains honestly failed
  until the 32-client p99 and PostgreSQL 17 throughput gates close.
- Sysbench indexed-update smoke was hardened on 2026-05-28. A 30-run local
  repeat of `ultrasql-bench sysbench --engine ultrasql --rows 1000 --duration 1
  --warmup 0 --connections 4` passed. Latest 2026-05-29
  `benchmarks/certify.sh smoke` passed regression-gate, HNSW ANN
  (`median_us=199.7495`, `recall_at_k=1.0`), and UltraSQL sysbench smoke
  (`10178.8167 ops/s`). Smoke artifacts live at
  `benchmarks/results/latest/benchmark_certification_manifest.json`,
  `benchmarks/results/latest/sysbench_smoke.json`, and
  `benchmarks/results/latest/raw/sysbench_oltp_read_write_smoke-ultrasql.json`.
  This is correctness/perf smoke evidence only; PostgreSQL comparison remains
  open without `POSTGRES_DSN`.
- TPC-H SF1 local PostgreSQL 17 certification passed with all q1..q22 complete
  for both engines.
- TPC-H scale 10 (all 22 queries) is complete: latest local artifact
  `benchmarks/results/latest/tpch_sf10_certification.json` has `status passed`
  and `22/22 DuckDB and UltraSQL query timings`.
- Columnar scan path landed: heap rows remain the OLTP/MVCC source of truth,
  `HeapAccess::column_cache` supplies the OLAP shadow path, and committed DML
  invalidation/rebuild/update/delete/insert visibility tests exist.
- Exact vector top-k avoids full physical sort on fallback and reports exact
  kernel/fallback in `EXPLAIN ANALYZE`.
- Exact vector cross-engine artifacts exist for the 10k-row, 8-dimension, k=10
  shape. `benchmarks/results/latest/raw/vector_topk_exact_10k_8d_k10-ultrasql.json`,
  `benchmarks/results/latest/raw/vector_topk_exact_10k_8d_k10-duckdb_list.json`,
  and
  `benchmarks/results/latest/raw/vector_topk_exact_10k_8d_k10-postgres17_pgvector.json`
  are measured with matching answer checksums. The ClickHouse artifact
  `benchmarks/results/latest/raw/vector_topk_exact_10k_8d_k10-clickhouse_vector.json`
  records `status=not_available` with `reason=clickhouse_not_found`.
- Page-backed HNSW and IVFFlat SQL paths exist, survive restart, and have
  crash/corrupt/torn-WAL, rebuild, EXPLAIN, insert/update/delete/VACUUM, and WAL
  payload fuzz/property tests.
- AI benchmark gauntlet full profile passed in
  `benchmarks/results/latest/ai_benchmark_gauntlet_manifest.json` with measured
  UltraSQL artifacts for exact top-k, HNSW recall/latency, hybrid search,
  filtered vector search, RAG quality, memory per million vectors, ingestion
  throughput, and cold-start index load.
- local Firebolt Core smoke measured for aggregating-index, wide
  filter/projection, and HNSW vector shapes; sparse primary-index pruning
  remains open because Core EXPLAIN did not expose pruning evidence.

## Regression Baselines

- Curated PostgreSQL parser/type baseline imports public SQLLogicTest cases from
  `char.sql`, `varchar.sql`, `numeric.sql`, and `type_sanity.sql`.
- Transaction isolation baseline covers `acid.sql`, Hermitage G1a/PMP/G2, and
  manager-level Hermitage matrix.
- Index regression baseline covers `CREATE INDEX`, `CREATE UNIQUE INDEX`,
  indexed equality/range reads, and unique violations.
- Constraint regression baseline covers primary key, check, not-null, foreign
  key, duplicate-key rejection, FK rejection, and check-on-update.
- Operator regression baseline covers comparison lexer/evaluator surfaces,
  `BETWEEN`, and `LIKE`.
- Type-specific regression baseline covers numeric, text, date/time/timetz,
  timestamp, JSON/JSONB, and arrays.
