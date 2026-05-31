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
- Heap WAL append failures after page mutation now return typed
  `HeapError::Wal` and poison the buffer pool instead of panicking or letting
  later page access/flush continue with unlogged dirty bytes. Evidence:
  `cargo test -p ultrasql-storage --lib` and
  `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D warnings`.
- B-tree WAL append failures for leaf insert, split, and delete now use the
  same typed fatal path: `BTreeError::Wal` plus buffer-pool poisoning, with
  later page access rejected. Evidence: `cargo test -p ultrasql-storage --lib`
  and
  `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D warnings`.
- Page-backed IVFFlat metadata initialization now propagates
  `AccessMethodError` instead of panicking in the constructor. Evidence:
  `cargo test -p ultrasql-storage --lib page_backed_ivfflat -- --nocapture`
  and
  `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D warnings`.
- Heap scan no longer uses a production `expect` for the held page guard; an
  impossible missing-guard state now returns a typed `HeapError` instead of
  panicking. Evidence: `cargo test -p ultrasql-storage --lib heap -- --nocapture`
  and
  `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D warnings`.
- Storage strict production panic audit now passes
  `cargo clippy -p ultrasql-storage --lib --all-features -- -D clippy::unwrap_used -D clippy::expect_used`;
  remaining infallible `PageRead`/`PageWrite`/`Page::header` invariants have
  narrow documented lint allows because those APIs cannot return `Result`.
- Catalog binary decoder fixed-width reads now use the checked `Reader::fixed`
  helper instead of `try_into().unwrap()`, preserving typed `DecodeError` on
  truncated catalog heap bytes. Evidence:
  `cargo test -p ultrasql-catalog encoding::tests::truncated_payload_is_caught -- --nocapture`
  and `cargo clippy -p ultrasql-catalog --all-targets --all-features -- -D warnings`.
- Catalog binary decoder length fields now use `Reader::usize_len`, so decoded
  `u32` lengths return typed `DecodeError::LengthOverflow` on unsupported
  targets instead of `expect`. Evidence:
  `cargo test -p ultrasql-catalog encoding::tests::truncated_payload_is_caught -- --nocapture`
  and `cargo clippy -p ultrasql-catalog --all-targets --all-features -- -D warnings`.
- Catalog bootstrap schema invariants now use one documented `static_schema`
  helper instead of repeated production `expect` calls at every system-table
  schema site. Evidence:
  `cargo test -p ultrasql-catalog --lib bootstrap -- --nocapture` and
  `cargo clippy -p ultrasql-catalog --all-targets --all-features -- -D warnings`.
- Catalog row encoders now return typed `EncodeError::LengthOverflow` instead
  of panicking when variable-length strings, option lists, index vectors,
  constraint keys, or statistic-extension keys exceed the `u32` row-format
  prefix. Snapshot installation and composite/table attribute persistence now
  reject `pg_attribute.attnum` overflow with `CatalogError::SchemaConflict`
  instead of panicking or clamping to `i16::MAX`. Evidence:
  `cargo test -p ultrasql-catalog --lib`,
  `cargo clippy -p ultrasql-catalog --lib --all-features -- -D clippy::unwrap_used -D clippy::expect_used`,
  and `cargo clippy -p ultrasql-catalog --all-targets --all-features -- -D warnings`.
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
- Planner binder strict panic audit now also covers `FROM` list binding,
  local file expansion for `read_parquet`/`read_arrow`/CSV header fallback,
  window offset conversion, and `SHOW` schema construction. These paths now
  return typed `PlanError`s instead of relying on non-test `expect`. Evidence:
  `cargo test -p ultrasql-planner --lib binder::from -- --nocapture`,
  `cargo test -p ultrasql-planner --lib binder::tests -- --nocapture`,
  `cargo clippy -p ultrasql-planner --lib --all-features -- -D clippy::unwrap_used -D clippy::expect_used`,
  and `cargo clippy -p ultrasql-planner --all-targets --all-features -- -D warnings`.
- Constant folding now rewrites window `PARTITION BY`, window `ORDER BY`, and
  value expressions inside `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, and
  `NTH_VALUE` even when the window input is already a fixed point. Evidence:
  `cargo test -p ultrasql-optimizer constant_fold` and
  `cargo test -p ultrasql-optimizer`.
- Optimizer strict panic audit now covers common-subexpression elimination,
  constant-fold numeric extraction/comparison, IN-list OR rebuilds, and
  subquery-decorrelation correlation/schema helpers. These paths now no-op or
  return `OptimizeError::RuleFailed` instead of relying on non-test
  `unwrap`/`expect`. Evidence:
  `cargo test -p ultrasql-optimizer --lib -- --nocapture`,
  `cargo clippy -p ultrasql-optimizer --lib --all-features -- -D clippy::unwrap_used -D clippy::expect_used`,
  `cargo clippy -p ultrasql-optimizer --all-targets --all-features -- -D warnings`,
  and
  `cargo clippy -p ultrasql-server --lib --all-features -- -D clippy::unwrap_used -D clippy::expect_used`.
- Workspace lib strict panic audit now reaches the benchmark harness too.
  Benchmark setup failures are centralized through `require_bench_ok`, so
  release evidence aborts loudly instead of using raw `expect`, while TPC-H
  formatting/query helpers return typed `anyhow` errors where possible.
  Evidence:
  `cargo clippy --workspace --lib --all-features -- -D clippy::unwrap_used -D clippy::expect_used`,
  `cargo test -p ultrasql-bench --lib`, and
  `cargo clippy -p ultrasql-bench --all-targets --all-features -- -D warnings`.
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
- Executor SRF and fused-update result schema constructors now log and return
  an empty schema instead of panicking on impossible static schema failures.
  Evidence:
  `cargo test -p ultrasql-executor function_scan` and
  `cargo test -p ultrasql-executor fused_update`.
- `GatherMerge` now reports typed internal executor errors when child head-row
  state is inconsistent instead of panicking on selected children without a
  buffered row. Evidence:
  `cargo test -p ultrasql-executor gather` and
  `cargo clippy -p ultrasql-executor --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Direct scalar aggregate output-schema and NULL-row construction now avoid
  production `expect`, returning typed executor errors for impossible nullable
  column mismatches. Evidence:
  `cargo test -p ultrasql-executor direct_scalar_agg` and
  `cargo clippy -p ultrasql-executor --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Sequential scan builder setup, TID-prefixed schema construction, and
  column-cache slicing now avoid production `expect`/`unreachable` paths,
  surfacing typed executor errors for unsupported builder types and cache
  slice invariants. Evidence:
  `cargo test -p ultrasql-executor seq_scan` and
  `cargo clippy -p ultrasql-executor --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Scalar parameter evaluation now rejects `$0` and out-of-range bind indexes
  with `EvalError::ParameterIndex` instead of depending on saturating
  subtraction plus a production `expect`. Evidence:
  `cargo test -p ultrasql-executor parameter_` and
  `cargo clippy -p ultrasql-executor --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Fused filter/SUM and cached SUM/AVG operators now build output schemas
  infallibly and return typed executor errors for single-row NULL construction
  invariants instead of panicking in scalar aggregate fast paths. Evidence:
  `cargo test -p ultrasql-executor filter_sum` and
  `cargo clippy -p ultrasql-executor --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`.
- Row-codec varlena length prefixes, vector dimension lengths, and decimal
  base-10000 grouping now use typed conversion helpers instead of production
  `expect` calls for `u32 -> usize` and small-constant conversions. Evidence:
  `cargo test -p ultrasql-executor row_codec`,
  `cargo clippy -p ultrasql-executor --all-targets --all-features -- -D
  warnings`, and `rg "u32 fits in usize|small const"
  crates/ultrasql-executor/src/row_codec.rs` returning no matches.
- Row-codec builder finalization now propagates builder null-bitmap, text
  offset, UTF-8, and final batch invariant errors through `RowCodecError`
  instead of panicking while finishing decoded batches. Evidence:
  `cargo test -p ultrasql-executor row_codec`,
  `cargo test -p ultrasql-executor seq_scan`, and
  `cargo clippy -p ultrasql-executor --all-targets --all-features -- -D
  warnings`.
- Row-codec vector element decoding now maps impossible chunk-width failures
  to a typed `RowCodecError::Type`, clearing the executor source
  `expect`/`unwrap` audit. Evidence:
  `cargo test -p ultrasql-executor row_codec`,
  `cargo clippy -p ultrasql-executor --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`, and
  `cargo clippy -p ultrasql-executor --all-targets --all-features -- -D
  warnings`.
- Core bit-string display/counting, CSV header discovery, money formatting,
  network bitwise operations, and bit-vector display now avoid production
  `expect` paths and return deterministic fallbacks or formatting errors for
  impossible conversion failures. Evidence:
  `cargo test -p ultrasql-core --lib` and
  `cargo clippy -p ultrasql-core --all-targets --all-features -- -D warnings`.
- WAL typed payload encoders now reject oversized heap tuple lengths and
  malformed full-page-write page images with `PayloadError` instead of
  panicking; storage FPW emission propagates the typed encode error. Evidence:
  `cargo test -p ultrasql-wal payload`,
  `cargo test -p ultrasql-wal applier`,
  `cargo test -p ultrasql-storage wal_emit`,
  `cargo clippy -p ultrasql-wal --all-targets --all-features -- -D warnings`,
  and `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D
  warnings`.
- WAL writer record-length peeking now compares against an explicit on-disk
  header-size constant instead of a runtime `try_from(...).expect(...)`
  conversion. Evidence:
  `cargo test -p ultrasql-wal writer`,
  `cargo test -p ultrasql-wal record`, and
  `cargo clippy -p ultrasql-wal --all-targets --all-features -- -D warnings`.
- Object-store AWS v4 signing now propagates HMAC initialization failures as
  `ObjectStoreError::Signing` instead of using a production `expect`. Evidence:
  `cargo test -p ultrasql-objectstore --lib` and
  `cargo clippy -p ultrasql-objectstore --all-targets --all-features -- -D
  warnings`.
- Storage page allocation and in-page item-id decoding now avoid heap-vector
  conversion and fixed-slice `expect` paths; segment page-size arithmetic uses
  an explicit compile-time checked `u64` constant. Evidence:
  `cargo test -p ultrasql-storage page`,
  `cargo test -p ultrasql-storage segment`, and
  `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D
  warnings`.
- WAL record construction is now fallible and rejects oversized payloads with a
  typed error instead of panicking. Storage, server, CLI, and benchmark callers
  now propagate record-encoding failures before append. Evidence:
  `cargo test -p ultrasql-wal record`,
  `cargo test -p ultrasql-storage wal`,
  `cargo check --workspace --all-targets`, and clippy for the touched WAL,
  storage, server, CLI, and bench crates.
- Vector parallel filter+sum fan-out now joins all workers and falls back to
  the serial kernel if a worker panics instead of panicking in the caller.
  Evidence: `cargo test -p ultrasql-vec par_above_threshold_matches_serial`,
  `cargo test -p ultrasql-vec prop_par_matches_serial`,
  `cargo test -p ultrasql-vec prop_dict_kernel_matches_dense`, and
  `cargo clippy -p ultrasql-vec --all-targets --all-features -- -D warnings`.
- Vector string dictionary construction now returns typed
  `DictionaryError`s for code-width and generated-bitmap failures; callers use
  explicit fallbacks instead of constructor `expect` paths. Evidence:
  `cargo test -p ultrasql-vec from_strings`,
  `cargo test -p ultrasql-vec group_by_dict`,
  `cargo test -p ultrasql-vec auto_encoding`, `cargo check --workspace
  --all-targets`, and `cargo clippy -p ultrasql-vec --all-targets
  --all-features -- -D warnings`.
- Vector filter/compare/min chunk packing no longer uses fixed-slice
  `expect` conversions on `chunks_exact`; impossible conversion failures now
  skip the word instead of panicking. Evidence:
  `cargo test -p ultrasql-vec filter_eq`,
  `cargo test -p ultrasql-vec eq_i32`,
  `cargo test -p ultrasql-vec cmp_i32`,
  `cargo test -p ultrasql-vec cmp_gt_i64`,
  `cargo test -p ultrasql-vec min_f64`,
  `cargo test -p ultrasql-vec range_mask_i64`, and
  `cargo clippy -p ultrasql-vec --all-targets --all-features -- -D warnings`.
- Vector text kernels no longer use production `expect` paths while rebuilding
  ASCII case-folded string columns; offset/UTF-8 validation now flows through
  `StringColumn::from_parts` and fails closed on impossible invariant breaks.
  Evidence: `cargo test -p ultrasql-vec len_text`,
  `cargo test -p ultrasql-vec lower_text`,
  `cargo test -p ultrasql-vec upper_text`, `cargo check --workspace
  --all-targets`, and `cargo clippy -p ultrasql-vec --all-targets
  --all-features -- -D warnings`.
- Vector boolean NOT kernels now fail closed on mismatched validity bitmaps
  instead of panicking before result construction. Evidence:
  `cargo test -p ultrasql-vec not_bool_mismatched_validity_fails_closed`,
  `cargo test -p ultrasql-vec not_bool_scalar_mismatched_validity_fails_closed`,
  `cargo test -p ultrasql-vec not_bool`, and
  `cargo clippy -p ultrasql-vec --all-targets --all-features -- -D warnings`.
- Vector filter-sum scalar hot loop now uses fixed-width slice patterns instead
  of production `expect` conversions on `chunks_exact(8)` lanes, preserving
  branchless wrapping semantics while removing a panic path from the benchmarked
  kernel. Evidence: `cargo test -p ultrasql-vec filter_sum` and
  `cargo clippy -p ultrasql-vec --all-targets --all-features -- -D warnings`.
- Vector i64 dictionary predicate masks now clamp oversized public dictionary
  inputs to their fixed mask capacity instead of indexing past the end, and
  dictionary filter kernels no longer use production `expect` conversions for
  8-lane chunk packing. Evidence:
  `cargo test -p ultrasql-vec predicate_mask_256_ignores_extra_dict_entries`,
  `cargo test -p ultrasql-vec predicate_mask_65536_ignores_extra_dict_entries`,
  `cargo test -p ultrasql-vec dict`, and
  `cargo clippy -p ultrasql-vec --all-targets --all-features -- -D warnings`.
- Vector string columns now use checked UTF-8 buffer construction and checked
  row access, removing production `expect` paths and returning `None` for
  out-of-bounds text access through the generic `Column` API. Evidence:
  `cargo test -p ultrasql-vec column_text_value_utf8_out_of_bounds_returns_none`,
  `cargo test -p ultrasql-vec column_text_value_dictionary_out_of_bounds_returns_none`,
  `cargo test -p ultrasql-vec column`, and
  `cargo clippy -p ultrasql-vec --all-targets --all-features -- -D warnings`.
- Parser lexer, lookahead, table-function, `OVERLAPS`, and vector typed-literal
  paths now return typed lexer/parser errors instead of relying on production
  `expect` invariants. Evidence: `cargo test -p ultrasql-parser --lib`,
  `cargo clippy -p ultrasql-parser --lib --all-features -- -W
  clippy::expect_used -W clippy::unwrap_used`, and
  `cargo clippy -p ultrasql-parser --all-targets --all-features -- -D
  warnings`.
- Storage heap fast-path header reads now avoid fixed-slice `expect`
  conversions and unchecked integer-width casts in the touched scan/vacuum
  item-id decode paths. Evidence: `cargo test -p ultrasql-storage heap`,
  `cargo test -p ultrasql-storage
  heap::tests::wal_emission::vacuum_heap_reclaims_committed_dead_tuples`, and
  `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D
  warnings`.
- Transaction lock manager fastpath refcount overflow now returns a typed
  `LockError`, detector-thread spawn failure no longer panics during manager
  construction, drop tolerates an already-missing detector handle, and deadlock
  DFS cycle extraction avoids invariant `expect`s. Evidence:
  `cargo test -p ultrasql-txn lock -- --nocapture`, `cargo clippy -p
  ultrasql-txn --lib --all-features -- -W clippy::expect_used -W
  clippy::unwrap_used`, and `cargo clippy -p ultrasql-txn --all-targets
  --all-features -- -D warnings`.
- Storage checkpointer spawn failure now returns an inert handle instead of
  panicking, and shutdown returns `Ok(0)` when no background handle exists.
  Evidence: `cargo test -p ultrasql-storage spawn_and_shutdown_clean --
  --ignored --nocapture`, `cargo test -p ultrasql-storage
  checkpointer_flushes_dirty_pages -- --ignored --nocapture`, and
  `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D
  warnings`.
- WAL applier block-counter maintenance now rejects unrepresentable
  `u32::MAX` page blocks with a typed replay error instead of panicking during
  recovery. Evidence: `cargo test -p ultrasql-storage
  apply_insert_rejects_unrepresentable_block_count_without_panic --
  --nocapture`, `cargo test -p ultrasql-storage wal_applier -- --nocapture`,
  and `cargo clippy -p ultrasql-storage --all-targets --all-features -- -D
  warnings`.

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
- `DROP TABLE` dependency tracking now treats append-only materialized views as
  dependents: `RESTRICT` blocks source-table drops, `CASCADE` drops dependent
  materialized views, and direct materialized-view drops clear runtime
  maintenance state. Evidence:
  `cargo test -p ultrasql-server --test materialized_view_round_trip`.
- Dropped materialized views are removed from durable
  `pg_materialized_views.meta`, preventing stale restart sidecar records after
  direct view drops or source-table cascades. Evidence:
  `cargo test -p ultrasql-server --test materialized_view_round_trip`.
- Dropped tables are removed from durable `pg_table_runtime.meta`, preventing
  stale default, identity, generated-column, check, FK, exclusion, and index
  sidecar records after `DROP TABLE`. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip`.
- Dropped tables are removed from durable `pg_row_security.meta`, preventing
  stale row-level-security policies from surviving restart after `DROP TABLE`.
  Evidence: `cargo test -p ultrasql-server --test rls_round_trip`.
- Dropped tables remove table and column privilege grants from memory and
  durable `pg_privileges.meta`, so a recreated table cannot inherit stale ACLs
  by name. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip`.
- Dropped tables clear runtime optimizer statistics, modification counters, and
  pending auto-analyze work, so recreated names cannot inherit stale planner
  evidence. Evidence: `cargo test -p ultrasql-server --test analyze_round_trip`.
- Dropped table `pg_statistic` / `pg_statistic_ext` rows are removed from the
  live catalog snapshot and ignored during restart bootstrap unless their
  relation still exists. Evidence:
  `cargo test -p ultrasql-server --test analyze_round_trip`.
- `DROP TABLE` now removes live `pg_statistic_ext` rows immediately, so
  extended statistics for dropped relations disappear before restart. Evidence:
  `cargo test -p ultrasql-server --test create_statistics_round_trip`.
- Dropped sequences remove sequence privilege grants from memory and durable
  `pg_privileges.meta`, so a recreated sequence cannot inherit stale ACLs by
  name. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip`.
- Explicit `DROP SEQUENCE` survives restart and permits same-name recreation
  with fresh state. Evidence:
  `cargo test -p ultrasql-server --test sequence_round_trip`.
- Dropping a table now emits WAL drops and clears privilege grants for owned
  SERIAL/identity sequences, so restart/recreate cannot resurrect a stale
  sequence or stale sequence ACL. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip`.
- `DROP SEQUENCE` dependency tracking now treats SERIAL/identity column defaults
  as dependents: `RESTRICT` blocks sequence drops while defaults reference the
  sequence, and `CASCADE` detaches those defaults before removing the sequence.
  Evidence: `cargo test -p ultrasql-server --test sequence_round_trip`.
- `DROP ROLE` now rejects roles that still own live tables or still appear in
  object/default privilege grants, avoiding stale ownership and ACL references.
  Evidence: `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- `DROP ROLE` now rejects roles that still appear in row-level-security policy
  role lists, avoiding stale policy-role references after role deletion.
  Evidence: `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- `DROP ROLE` now rejects roles that still appear as role-membership grantors,
  avoiding stale membership grantor references after role deletion. Evidence:
  `cargo test -p ultrasql-server --test role_ddl_round_trip`.

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
- Role-scoped RLS policies parse `CREATE POLICY ... TO role`, enforce inherited
  role membership, persist across restart, and fail closed when no scoped policy
  applies. Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_policy_roles_scope_visibility_and_restart -- --nocapture`.
- RLS `INSERT ... SELECT` now enforces target-table `WITH CHECK` predicates on
  every produced row through the mutation operator, skips unchecked fused insert
  paths when a row-security check exists, and rejects mixed-tenant source rows
  atomically. Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_insert_select_checks_source_rows_atomically -- --nocapture`.
- RLS `UPDATE` now enforces new-row `WITH CHECK` predicates through the same
  mutation check path, skips unchecked fused update paths when row-security
  checks exist, supports nested RLS/user filters for TID scans, and preserves
  old rows on rejected tenant changes. Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_update_checks_new_rows_and_preserves_old_row_on_failure -- --nocapture`.
- RLS tenant certification is part of `benchmarks/certify.sh smoke` through
  `benchmarks/rls_tenant_certify.sh`, which writes
  `benchmarks/results/latest/rls_tenant_certification.json` after the wire-level
  RLS suite verifies read filtering, mutation checks, role scoping,
  owner/bypass semantics, restrictive policies, and restart persistence.
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
- Tooling panic-path audit tightened benchmark and SQLLogicTest binaries:
  benchmark server/mock setup now returns context errors instead of panics,
  CLI reference engines reject non-CLI targets with typed errors, hex encoding
  avoids fallible formatting, empty point-lookup warmup is guarded, and
  PostgreSQL regression provenance now asserts active row-value `IN` coverage
  instead of stale skip debt.
- Production panic audit now passes for workspace libs and bins under
  `cargo clippy --workspace --lib --bins --all-features -- -D clippy::panic
  -D clippy::todo -D clippy::unimplemented`; benchmark setup failures now emit
  contextual stderr and exit with status 2 instead of unwinding through the
  regression gate.
- Role catalog restart persistence is covered by `pg_auth.meta`: `CREATE ROLE`,
  `ALTER ROLE`, `DROP ROLE`, `GRANT role`, and `REVOKE role` snapshot roles and
  memberships to the data directory with rollback on metadata-write failure.
  `role_catalog_survives_restart` verifies role attributes and `SET ROLE`
  membership after `Server::init` restart, and
  `role_catalog_rolls_back_when_metadata_slot_is_unsafe` verifies unsafe
  metadata slots do not leave failed role DDL in memory.
- Privilege catalog restart persistence is covered by `pg_privileges.meta`:
  `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES`, and default-privilege
  application on future tables, materialized views, and sequences snapshot ACLs
  to the data directory with rollback on metadata-write failure. Evidence:
  `privilege_catalog_survives_restart` and
  `privilege_catalog_rolls_back_when_metadata_slot_is_unsafe`.
