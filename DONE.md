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
- Serializable isolation comments avoid full-SSI overclaims: parser, planner,
  and transaction-manager docs describe it as a client-requested isolation
  level with column-range SSI plus relation-level fallback until full
  predicate-precise SSI lands.
- Serializable SSI now records column-range predicate locks for supported
  scalar comparisons plus relation-level fallback. This keeps Hermitage G2
  write-skew abort coverage while allowing disjoint indexed row updates to
  commit when their bounded predicates do not overlap. Evidence:
  `cargo test -p ultrasql-txn ssi::tests:: --lib -- --nocapture`,
  `cargo test -p ultrasql-server --test isolation_suite_round_trip hermitage_g2_serializable_write_skew_aborts_one_wire -- --nocapture`,
  and
  `cargo test -p ultrasql-server --test isolation_suite_round_trip serializable_indexed_disjoint_row_updates_both_commit_wire -- --nocapture`.
- Constant `SELECT` result execution now propagates scalar evaluation failures
  instead of silently returning NULL. Evidence:
  `cargo test -p ultrasql-executor result_propagates_constant_eval_errors`.
- Planner binder tests are green again after tightening decimal and typed-literal
  behavior: numeric typmod coercion honors the requested target scale, bare
  `numeric` text casts preserve literal scale in the result type, `tsvector`
  typed literals bind as `TsVector`, and schema DDL variants are covered in the
  window-function plan walker. Evidence:
  `cargo test -p ultrasql-planner --lib -- --nocapture`.
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
- SQL/JSON path now supports bounded `decimal([precision[,scale]])` method
  parsing and evaluation with existing exact decimal rounding, precision
  overflow mapped to JSON null, and wire-level `jsonb_path_query` coverage.
  Evidence:
  `cargo test -p ultrasql-executor json_path::tests::path_supports_decimal_method_with_precision_and_scale --lib -- --nocapture`
  and
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_decimal_method -- --nocapture`.
- SQL/JSON path now supports ISO `date()`, `time([precision])`,
  `time_tz([precision])`, `timestamp([precision])`,
  `timestamp_tz([precision])`, auto `datetime()`, and
  `datetime("HH24:MI")` parsing/evaluation with bounded fractional precision.
  Common ISO `datetime(template)` forms now cover `YYYY-MM-DD`, `YYYYMMDD`,
  `YYYY-MM-DD HH24:MI:SS`, `YYYY-MM-DD"T"HH24:MI:SS`, and
  `YYYYMMDDHH24MISS`, with wire-level `jsonb_path_query` coverage. Evidence:
  `cargo test -p ultrasql-core iso_date_and_timestamp_text_helpers_round_trip --lib -- --nocapture`,
  `cargo test -p ultrasql-executor json_path::tests::path_supports_iso_datetime_methods --lib -- --nocapture`,
  and
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_iso_datetime_methods -- --nocapture`.
- XML `xpath` and `xpath_exists` now accept PostgreSQL-style namespace mapping
  arrays for the supported secure XPath subset. The XML walker carries
  inherited namespace context, resolves alias-to-URI matches for elements and
  attributes, and preserves raw-name behavior when no mapping is supplied.
  Evidence:
  `cargo test -p ultrasql-core xml_xpath --lib -- --nocapture`,
  `cargo test -p ultrasql-planner binds_xml_scalar_functions_with_precise_return_types --lib -- --nocapture`,
  and
  `cargo test -p ultrasql-server --test xml_round_trip xml_functions_validate_securely_and_extract_simple_xpath -- --nocapture`.
- XML XPath now supports the descendant `//` abbreviation for bounded element
  paths, including attribute filters and nested terminal selections, without
  expanding the DTD/entity surface. Evidence:
  `cargo test -p ultrasql-core xml_xpath_subset_filters_children_without_entity_resolution --lib -- --nocapture`
  and
  `cargo test -p ultrasql-server --test xml_round_trip xml_functions_validate_securely_and_extract_simple_xpath -- --nocapture`.
- XML XPath now supports element and terminal attribute wildcards (`*`, `@*`)
  in the secure local subset. Attribute wildcards skip namespace declaration
  attributes instead of exposing namespace bindings as normal values.
  Evidence:
  `cargo test -p ultrasql-core xml_xpath --lib -- --nocapture`
  and
  `cargo test -p ultrasql-server --test xml_round_trip -- --nocapture`.
- XML XPath now supports the bounded scalar `count(/supported/path)` function
  by counting matches from the existing secure selector without adding entity
  resolution or external access. Evidence:
  `cargo test -p ultrasql-core xml_xpath_subset_filters_children_without_entity_resolution --lib -- --nocapture`
  and
  `cargo test -p ultrasql-server --test xml_round_trip xml_functions_validate_securely_and_extract_simple_xpath -- --nocapture`.
- XML XPath now supports explicit `child::`, `attribute::`, `descendant::`,
  terminal `.`, and terminal `self::node()` steps in the secure local subset,
  reusing the existing element walker without entity expansion or external
  access. Evidence:
  `cargo test -p ultrasql-server --test xml_round_trip xml_functions_validate_securely_and_extract_simple_xpath -- --nocapture`.
- `pg_catalog.pg_proc` now advertises the supported XML function surface:
  `xml_is_well_formed`, `xml_is_well_formed_content`,
  `xml_is_well_formed_document`, `xpath`, and `xpath_exists`, including the
  namespace-array overloads. `format_type(143)` now renders `xml[]`.
  Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_proc_advertises_supported_xml_functions -- --nocapture`.
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
- `TIMETZ` / `TIMESTAMPTZ` literals now accept deterministic fixed-offset time
  zone abbreviations such as `EST`, `EDT`, `PST`, `PDT`, `CET`, and `EET`
  without claiming broad timezone-conversion parity. `TIMESTAMPTZ` literals,
  COPY text input, and extended text parameters resolve IANA named zones through
  the bundled time-zone database. `TIMETZ` literals resolve date-prefixed named
  zones such as `2000-07-01 04:05 America/New_York`, making DST-sensitive
  offsets deterministic. `TIMESTAMPTZ` result text now honors the session
  `TimeZone` setting for fixed offsets and IANA zones across Simple Query,
  Extended Query text results, DML `RETURNING`, and text/CSV `COPY TO`.
  Evidence:
  `cargo test -p ultrasql-core time_text_parser_and_timetz_pack_reject_bad_edges --lib -- --nocapture`;
  `cargo test -p ultrasql-planner time_and_timetz_literals_parse_postgres_shapes --lib -- --nocapture`
  and
  `cargo test -p ultrasql-server --test timetz_round_trip timetz_and_temporal_display_round_trip -- --nocapture`.
- `DateStyle` now accepts and round-trips PostgreSQL-style `ISO`, `SQL`,
  `Postgres`, `German` plus `MDY` / `DMY` / `YMD` session settings through
  `SET`, `SHOW`, and `RESET`; `DATE`, `TIMESTAMP`, and `TIMESTAMPTZ` text
  output now honors `SQL`, `German`, and `Postgres` date styles for result
  rows and COPY text/CSV paths. Locale variants remain tracked in
  `ROADMAP.md`. Evidence:
  `cargo test -p ultrasql-server --test timetz_round_trip timetz_and_temporal_display_round_trip -- --nocapture`;
  `cargo test -p ultrasql-server --test system_functions_round_trip orm_startup_runtime_parameters_round_trip`.
- `TimeZone` session GUC values are now validated through the same fixed-offset
  and IANA resolver used by timestamp display, so invalid zones are rejected
  before they can silently fall back to UTC. Evidence:
  `cargo test -p ultrasql-server session::execute::tests::session_variable_surface_sets_shows_and_resets_supported_gucs --lib -- --nocapture`.
- `SHOW transaction isolation level` now reflects the active transaction
  isolation (`read committed`, `repeatable read`, or `serializable`) instead
  of always returning the idle default. Evidence:
  `cargo test -p ultrasql-server --test txn_round_trip set_transaction_isolation_level_round_trip -- --nocapture`.
- `pg_catalog.pg_settings` now exposes `transaction_isolation` from the active
  statement context, so catalog introspection matches `SHOW` inside explicit
  transactions. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_settings_reflects_active_transaction_isolation -- --nocapture`.
- `pg_catalog.pg_settings.search_path` now reflects the current session value
  and default used by `SHOW search_path` instead of a hardcoded `public`.
  Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_settings_reflects_session_search_path -- --nocapture`.
- `pg_catalog.pg_settings` now exposes supported runtime GUCs with the same
  values as `SHOW`, including application name, client message level, date,
  interval, float, monetary, and time-zone settings. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_settings_reflects_runtime_gucs -- --nocapture`.
- `pg_catalog.pg_settings.statement_timeout` now reflects the session timeout
  setting and reset state while preserving the `ms` unit, so the enforced
  timeout is visible through catalog introspection. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_settings_reflects_statement_timeout -- --nocapture`.
- `pg_catalog.pg_settings` now exposes static driver defaults for
  `server_version_num` and `max_identifier_length`, matching the `SHOW` surface
  many clients probe during startup. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_settings_exposes_static_driver_defaults -- --nocapture`.
- `pg_catalog.pg_collation` now exposes base `default`, `C`, and `POSIX`
  catalog rows instead of an empty relation, unblocking ORM and GUI
  introspection probes that join collation metadata. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip active_record_column_definitions_probe_uses_catalog_helpers`.
- `COLLATE` now parses and binds in expression and `ORDER BY` positions for
  built-in `default`, `C`, `POSIX`, and `pg_catalog`-qualified names. The
  runtime validates unknown collations and non-text inputs instead of silently
  ignoring them, while preserving the current bytewise ordering semantics.
  Evidence:
  `cargo test -p ultrasql-parser parser::tests::postfix::collate --lib -- --nocapture`
  and
  `cargo test -p ultrasql-server --test order_by_round_trip order_by_builtin_collate_uses_bytewise_order -- --nocapture`.
- `pg_attribute.attcollation` and `pg_type.typcollation` now report the default
  collation OID for textlike columns and textlike domains instead of hardcoded
  zero, while non-collatable types remain zero. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip active_record_column_definitions_probe_uses_catalog_helpers -- --nocapture`.
- Column definitions now parse, validate, persist, and expose built-in
  `COLLATE default`, `COLLATE "C"`, and `COLLATE "POSIX"` metadata for
  textlike columns, reject non-text column collations, and preserve explicit
  built-in collation OIDs across restart. Evidence:
  `cargo test -p ultrasql-parser statements::create_table::tests::create_table_parses_column_collation --lib -- --nocapture`
  and
  `cargo test -p ultrasql-server --test catalog_views_round_trip create_table_column_collate_default_is_validated_and_visible -- --nocapture`;
  `cargo test -p ultrasql-server --test catalog_views_round_trip explicit_column_collation_survives_restart -- --nocapture`.
- `BIT(n)` / `BIT VARYING(n)` storage, row codec, operators, wire OIDs, COPY,
  and end-to-end tests exist.
- `INET`, `CIDR`, `MACADDR`, and `MACADDR8` storage, operators, wire OIDs,
  COPY, network inspector functions (`host`, `family`, `masklen`), and
  end-to-end tests exist. Evidence:
  `cargo test -p ultrasql-server --test network_types_round_trip network_types_storage_ops_and_wire_round_trip -- --nocapture`.
- Built-in range types (`int4range`, `int8range`, `numrange`, `daterange`,
  `tsrange`, `tstzrange`) now appear in `pg_catalog.pg_type` and
  `pg_catalog.pg_range` with PostgreSQL OIDs and subtype OIDs, and range
  columns report range type OIDs instead of falling back to text. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_range_lists_builtin_range_type_metadata -- --nocapture`.
- JSON and JSONB have distinct runtime/catalog/wire identity, JSON validation
  with text preservation, JSONB normalization, COPY, extended params, operator
  evaluation, and regression tests.
- JSON functions landed for `json_build_object`, `jsonb_set`, `json_each`,
  `jsonb_path_query`, `jsonb_path_exists`, JSON_TABLE subset paths, and
  whole-row `row_to_json`; SQL/JSON path prefixes `lax` and `strict` are
  accepted; strict mode reports structural errors for the supported selection
  subset while lax mode suppresses them. `jsonb_path_exists` plus
  `jsonb_path_query` resolve predicate literal variables from a JSON/JSONB
  `vars` argument. Basic `.size()`, `.type()`, `.keyvalue()`, `.boolean()`,
  `.string()`, `.double()`, `.number()`, `.integer()`, `.bigint()`, `.abs()`,
  `.floor()`, and `.ceiling()` SQL/JSON path methods now work in the shared
  path engine, and filter predicates support `&&`, `||`, `!`, nested predicate
  parentheses, `exists(path)`, string `starts with`, and `like_regex` with
  validated `i` / `m` / `s` flags. Evidence:
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_accepts_strict_and_lax_prefixes -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_strict_mode_reports_structural_errors -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_exists_supports_variable_literals -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_variable_literals -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_basic_methods -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_keyvalue_method -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_numeric_methods -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_conversion_methods -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_predicate_boolean_algebra -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_exists_predicates -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_starts_with_predicates -- --nocapture`;
  `cargo test -p ultrasql-server --test jsonb_path_query_round_trip jsonb_path_query_supports_like_regex_predicates -- --nocapture`.
- Native arrays support multi-dimensional rectangular text/runtime round trips,
  GIN-facing operators, array subscripts/slices, `array_agg`, `array_length`,
  `array_ndims`, `array_lower`, `array_upper`, `array_dims`, `cardinality`,
  `array_cat`, `array_append`, `array_prepend`, `array_remove`,
  `array_replace`, `array_position` including the start argument,
  `array_positions`, `trim_array`, `array_to_string`, `string_to_array`, and
  wire-visible `unnest`.
- `CREATE TYPE ... AS ENUM`, `CREATE TYPE ... AS (composite)`, and
  `CREATE DOMAIN` have durable catalog storage, restart round trips, and wire
  type OID coverage.
- Domain runtime metadata reload rejects duplicate domain OIDs or schema/name
  keys before applying restart constraints. Evidence:
  `cargo test -p ultrasql-server --test domain_type_round_trip domain_metadata_rejects_duplicate_domain_rows_on_rebuild -- --nocapture`.
- Domain runtime metadata reload rejects duplicate `CHECK` constraint names for
  the same domain before applying restart constraints. Evidence:
  `cargo test -p ultrasql-server --test domain_type_round_trip domain_metadata_rejects_duplicate_check_rows_on_rebuild -- --nocapture`.
- Domain runtime metadata reload rejects orphan `CHECK` rows whose domain OID is
  not applied by a matching domain metadata row. Evidence:
  `cargo test -p ultrasql-server --test domain_type_round_trip domain_metadata_rejects_orphan_check_rows_on_rebuild -- --nocapture`.
- `OID`, `REGCLASS`, `REGTYPE`, and `PG_LSN` parser/binder/runtime/storage/wire
  support exists.
- Basic XML storage exists with validated text storage, OID 142 wire rendering,
  COPY, and restart round trip.
- XML scalar functions now cover local-only secure well-formed checks
  (`xml_is_well_formed`, `xml_is_well_formed_content`,
  `xml_is_well_formed_document`) plus a deterministic `xpath` /
  `xpath_exists` subset for absolute element paths with optional attribute
  equality filters, element wildcards, terminal `@attr` / `@*` selection,
  terminal `text()` selection, namespace URI mapping arrays, the descendant
  `//` abbreviation, and explicit `child::`, `attribute::`, `descendant::`, and
  terminal self-node steps. DTD declarations, external entity expansion,
  unknown entity references, and pre-root junk are rejected.
- `XMLTABLE` now has a first secure table-function subset: constant XML input,
  element row XPath, scalar column `PATH`, temporal/numeric/money scalar
  projections, string-literal `DEFAULT` values for missing scalar paths, and
  `FOR ORDINALITY`, lowered through the same local XML validator and XPath
  engine as `xpath`. Evidence:
  `cargo test -p ultrasql-parser select_xmltable_in_from_parses_columns_clause --lib -- --nocapture`;
  `cargo test -p ultrasql-planner bind_from_covers_table_ref_families_and_join_scope_shapes --lib -- --nocapture`;
  `cargo test -p ultrasql-server --test function_scan_round_trip xmltable_projects_declared_columns_from_xml_literal -- --nocapture`;
  `cargo test -p ultrasql-server --test function_scan_round_trip xmltable_projects_temporal_numeric_and_money_columns -- --nocapture`;
  `cargo test -p ultrasql-server --test function_scan_round_trip xmltable_uses_defaults_for_missing_scalar_paths -- --nocapture`.
- XML syntax now covers `XMLPARSE(DOCUMENT|CONTENT ...)` and
  `XMLSERIALIZE(DOCUMENT|CONTENT ... AS TEXT)`, with malformed `DOCUMENT`
  inputs rejected through the wire path. Evidence:
  `cargo test -p ultrasql-server --test xml_round_trip xml_functions_validate_securely_and_extract_simple_xpath`.
- `XMLSERIALIZE` now returns a typed executor error if its parser contract is
  ever violated instead of relying on a production `unreachable!`. Evidence:
  `cargo test -p ultrasql-executor null_helpers_extrema_xml_and_unknown_function_paths --lib -- --nocapture`;
  `cargo clippy -p ultrasql-executor --lib --all-features -- -D warnings`.
- Ordered-set aggregates `PERCENTILE_CONT` and `PERCENTILE_DISC` have plan shape
  and wire coverage.
- Portable scalar helpers now cover `COALESCE`, `IFNULL` / `NVL`,
  `NULLIF`, `LEAST`, `GREATEST`, and SQLite-style multi-argument scalar
  `MIN` / `MAX` through wire round-trip tests.
- Text-backed full-text search now covers `to_tsvector`,
  `to_tsquery`, `plainto_tsquery`, `websearch_to_tsquery`, `phraseto_tsquery`,
  `@@`, deterministic `ts_rank`, `ts_rank_cd`, and `ts_headline` subsets, plus
  `numnode` / `querytree` query-inspection helpers through binder, executor,
  and wire tests.
  `TSVECTOR` and `TSQUERY` now have dedicated logical types, PostgreSQL OIDs
  `3614` / `3615`, array OIDs `3643` / `3645`, `pg_type` rows, and
  RowDescription coverage while retaining the current text-backed value
  representation. `pg_catalog.pg_proc` advertises the supported full-text
  function signatures for introspection, and rank helpers reject unsupported
  extra arguments instead of silently ignoring them. Evidence:
  `cargo test -p ultrasql-server --test full_text_round_trip`;
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_proc_advertises_supported_full_text_functions -- --nocapture`.
- `DROP TABLE` dependency tracking now treats append-only materialized views as
  dependents: `RESTRICT` blocks source-table drops, `CASCADE` drops dependent
  materialized views, and direct materialized-view drops clear runtime
  maintenance state. Evidence:
  `cargo test -p ultrasql-server --test materialized_view_round_trip`.
- Dropped materialized views are removed from durable
  `pg_materialized_views.meta`, preventing stale restart sidecar records after
  direct view drops or source-table cascades. Evidence:
  `cargo test -p ultrasql-server --test materialized_view_round_trip`.
- Materialized-view metadata reload rejects duplicate view names or OIDs,
  avoiding ambiguous restart maintenance state from tampered sidecar rows.
  Evidence:
  `cargo test -p ultrasql-server --test materialized_view_round_trip materialized_view_metadata_rejects_duplicate_views_on_rebuild -- --nocapture`.
- Materialized-view metadata reload rejects missing or mismatched view/source
  table references, avoiding silent restart loss of append maintenance from
  tampered sidecar rows. Evidence:
  `cargo test -p ultrasql-server --test materialized_view_round_trip materialized_view_metadata_rejects_mismatched_source_on_rebuild -- --nocapture`.
- Append-only materialized views now report `pg_class.relkind = 'm'`, appear in
  `pg_catalog.pg_matviews`, stay out of `pg_catalog.pg_tables`, and keep that
  catalog shape after restart. Evidence:
  `cargo test -p ultrasql-server --test materialized_view_round_trip`.
- Dropped tables are removed from durable `pg_table_runtime.meta`, preventing
  stale default, identity, generated-column, check, FK, exclusion, and index
  sidecar records after `DROP TABLE`. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip`.
- Table runtime metadata reload rejects duplicate table OIDs or table names,
  avoiding ambiguous restart constraints from tampered sidecar rows. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_duplicate_table_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects table rows whose OID no longer exists in
  the catalog snapshot, avoiding silent restart skips for tampered sidecars.
  Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_unknown_table_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects orphan constraint rows whose table OID is
  not applied by a matching table metadata row. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_orphan_constraint_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects duplicate column default rows for the
  same table and column, avoiding last-row-wins restart defaults. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_duplicate_default_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects duplicate sequence-default rows for the
  same table and column, avoiding ambiguous restarted serial/identity defaults.
  Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_duplicate_sequence_default_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects duplicate identity flag rows for the
  same table and column, avoiding ambiguous restarted `GENERATED ALWAYS`
  enforcement. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_duplicate_identity_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects duplicate generated-column rows for the
  same table and column, avoiding last-row-wins restart expressions. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_duplicate_generated_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects duplicate `CHECK` constraint names for
  the same table, avoiding repeated restart constraint state from tampered
  sidecar rows. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_duplicate_check_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects duplicate `FOREIGN KEY` constraint names
  for the same table, avoiding repeated restart referential actions from
  tampered sidecar rows. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_duplicate_foreign_key_rows_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects `FOREIGN KEY` target name/OID mismatches,
  avoiding silent restart loss of referential checks from tampered sidecars.
  Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_mismatched_foreign_key_target_on_rebuild -- --nocapture`.
- Table runtime metadata reload rejects duplicate `EXCLUDE` constraint names for
  the same table, avoiding repeated restart exclusion checks from tampered
  sidecar rows. Evidence:
  `cargo test -p ultrasql-server --test drop_restart_round_trip table_runtime_metadata_rejects_duplicate_exclusion_rows_on_rebuild -- --nocapture`.
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
- `DROP TABLE` now clears live table, column, and dependent-index
  `pg_description` rows immediately, so catalog views cannot expose comments
  for dropped objects before restart. Evidence:
  `cargo test -p ultrasql-server --test comment_restart_round_trip`.
- `DROP INDEX` is now bound, dispatched, persisted with a durable catalog
  tombstone, clears live index comments, and stays dropped after restart.
  Evidence: `cargo test -p ultrasql-server --test drop_index_round_trip` and
  `cargo test -p ultrasql-planner drop_index`.
- `DROP INDEX` now rejects primary-key and catalog-recorded
  constraint-backed indexes, preventing direct index drops from weakening
  declared constraints. Evidence:
  `cargo test -p ultrasql-server --test drop_index_round_trip`.
- Explicitly qualified `DROP TABLE schema.name` now checks the catalog table's
  stored schema before planning the drop, preventing a wrong-qualified statement
  from dropping a same-name table in another namespace. Evidence:
  `cargo test -p ultrasql-server --test drop_table_round_trip
  drop_table_respects_schema_qualifier -- --nocapture`.
- Explicitly qualified `FROM schema.name` now checks the catalog table's stored
  schema before binding scans, preventing a wrong-qualified `SELECT` from
  reading a same-name table in another namespace. Evidence:
  `cargo test -p ultrasql-server --test schema_ddl_round_trip
  select_respects_schema_qualifier -- --nocapture`.
- Explicitly qualified `INSERT`, `UPDATE`, and `DELETE` targets now check the
  catalog table's stored schema before binding DML, preventing wrong-qualified
  writes from mutating a same-name table in another namespace. Evidence:
  `cargo test -p ultrasql-server --test schema_ddl_round_trip
  dml_respects_schema_qualifier -- --nocapture`.
- Explicitly qualified `TRUNCATE schema.name` now shares the same stored-schema
  check as `DROP TABLE`, preventing wrong-qualified truncates from clearing a
  same-name table in another namespace. Evidence:
  `cargo test -p ultrasql-server --test schema_ddl_round_trip
  truncate_respects_schema_qualifier -- --nocapture`.
- Committed `pg_constraint` rows are now installed into the live catalog map
  before restart, so same-session `DROP INDEX` also rejects inline and
  `ALTER TABLE ADD UNIQUE` constraint indexes. Evidence:
  `cargo test -p ultrasql-server --test drop_index_round_trip`.
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
- Sequence ownership is now tracked, restart-persisted, and surfaced through
  `pg_catalog.pg_sequences`; `DROP ROLE` rejects roles that still own live
  sequences until those sequences are dropped. Evidence:
  `cargo test -p ultrasql-server --test sequence_round_trip` and
  `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- `ALTER SEQUENCE` and `DROP SEQUENCE` now enforce sequence owner or superuser
  authorization before mutating sequence state, dependencies, WAL, owner
  metadata, or ACL metadata; SERIAL/identity-created sequences are recorded and
  cleaned up through the same owner/namespace metadata path. Evidence:
  `cargo test -p ultrasql-server --test sequence_round_trip -- --nocapture`.
- `CREATE SCHEMA`, `CREATE SCHEMA IF NOT EXISTS`, `DROP SCHEMA`, and
  `DROP SCHEMA IF EXISTS` now execute through the wire path, survive restart
  through runtime schema metadata, surface in `pg_namespace` /
  `information_schema.schemata`, and block `DROP ROLE` while a role owns a
  live schema. Evidence:
  `cargo test -p ultrasql-server --test schema_ddl_round_trip`,
  `cargo test -p ultrasql-server --test catalog_views_round_trip`, and
  `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- Qualified object DDL now rejects missing schemas with SQLSTATE `3F000`
  before creating tables, sequences, enum/composite types, domains, or
  materialized views, while allowing those objects inside runtime-created
  schemas. Evidence: `cargo test -p ultrasql-server --test
  schema_ddl_round_trip` and `cargo test -p ultrasql-server
  undefined_schema_is_query_scoped_invalid_schema_name`.
- Qualified sequences now keep their schema in runtime metadata and catalog
  views across restart; `DROP SCHEMA ... RESTRICT` now sees sequence and enum
  dependencies instead of leaving orphaned namespace references. Evidence:
  `cargo test -p ultrasql-server --test schema_ddl_round_trip
  qualified_sequence_schema_survives_restart`.
- `DROP SCHEMA ... CASCADE` now removes qualified sequences in the dropped
  runtime schema, emits sequence-drop WAL, clears sequence owner/namespace
  metadata, removes sequence ACLs, and rejects unsupported table/type/operator
  dependents before mutating state. Evidence:
  `cargo test -p ultrasql-server --test schema_ddl_round_trip
  drop_schema_cascade_removes_qualified_sequences -- --nocapture`.
- Qualified table, materialized-view, enum, and domain schema names now survive
  restart by storing stable runtime namespace OIDs in catalog rows and remapping
  them after schema metadata loads. Evidence: `cargo test -p ultrasql-server
  --test schema_ddl_round_trip
  qualified_relation_and_type_schemas_survive_restart`.
- `DROP SCHEMA` now clears schema-scoped default privilege templates, so a
  recreated schema cannot inherit stale future-object grants from the dropped
  namespace. Evidence: `cargo test -p ultrasql-server --test
  privilege_catalog_round_trip
  drop_schema_removes_schema_scoped_default_privileges`.
- `DROP SCHEMA` now enforces schema owner or superuser authorization before
  dependency checks and metadata removal, so non-owner roles cannot remove a
  namespace they do not own. Evidence: `cargo test -p ultrasql-server --test
  schema_ddl_round_trip non_owner_cannot_drop_schema -- --nocapture`.
- Qualified object creation in runtime schemas now requires schema ownership,
  superuser, or an explicit `CREATE` privilege on the schema for tables,
  materialized views, sequences, enum/composite types, domains, and operators.
  Evidence: `cargo test -p ultrasql-server --test schema_ddl_round_trip
  schema_create_privilege_gates_qualified_object_ddl -- --nocapture`.
- Runtime schema owners can now `GRANT` and `REVOKE` schema privileges without
  requiring superuser, while non-owners remain blocked. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip
  schema_owner_can_grant_and_revoke_schema_create_privilege -- --nocapture`.
- Sequence owners can now `GRANT` and `REVOKE` sequence privileges without
  requiring superuser, while non-owners remain blocked. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip
  sequence_owner_can_grant_and_revoke_sequence_privileges -- --nocapture`.
- Sequence SQL functions now enforce ACLs: `nextval` requires `USAGE` or
  `UPDATE`, `currval`/`lastval` require `USAGE` or `SELECT`, and `setval`
  requires `UPDATE`, with owner/superuser bypass. Evidence:
  `cargo test -p ultrasql-server --test sequence_round_trip
  sequence_functions_enforce_usage_and_update_privileges -- --nocapture`.
- Sequence SQL functions now require schema `USAGE` for sequences in runtime
  schemas before accepting sequence-level ACLs, matching the table-access
  schema gate and keeping private-schema sequences hidden from granted sequence
  users until the namespace is granted. Evidence:
  `cargo test -p ultrasql-server --test sequence_round_trip
  sequence_functions_require_schema_usage_privilege -- --nocapture`.
- `DELETE` now requires table-level `DELETE` privilege for non-owner roles,
  closing the column-only privilege bypass for `DELETE FROM table` without a
  predicate or `RETURNING` list. Evidence: `cargo test -p ultrasql-server
  --test privilege_catalog_round_trip delete_requires_table_delete_privilege
  -- --nocapture`.
- `TRUNCATE` now accepts explicit table-level `TRUNCATE` privilege for
  non-owner roles while preserving owner/superuser bypass. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip
  truncate_accepts_table_truncate_privilege -- --nocapture`.
- Table access inside runtime schemas now requires schema `USAGE` in addition
  to table privileges unless the role owns the schema or is superuser. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip
  schema_usage_is_required_for_table_access -- --nocapture`.
- `DROP ROLE` now rejects roles that still own live tables or still appear in
  object/default privilege grants, avoiding stale ownership and ACL references.
  Evidence: `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- `DROP ROLE` now rejects roles that still appear in row-level-security policy
  role lists, avoiding stale policy-role references after role deletion.
  Evidence: `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- `DROP ROLE` now rejects roles that still appear as role-membership grantors,
  avoiding stale membership grantor references after role deletion. Evidence:
  `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- `DROP ROLE` now rejects roles that still appear as granted-role or member-role
  endpoints in role memberships, avoiding implicit membership deletion during
  role removal. Evidence:
  `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- `REVOKE role FROM role` now rejects unknown granted/member role references
  with SQLSTATE `42704` instead of silently succeeding. Evidence:
  `cargo test -p ultrasql-server --test role_ddl_round_trip`.
- Privilege DDL now rejects unknown grantee/default-owner roles with SQLSTATE
  `42704` instead of generic DDL failure. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip`.

## Security And Client Certification

- `CREATE ROLE / USER`, `ALTER ROLE`, and `DROP ROLE` work through the role
  catalog and `pg_roles` / `pg_user` visibility. `DROP ROLE` rejects the
  bootstrap `ultrasql` role before mutating catalog state, preserving the auth
  restart invariant. `ALTER ROLE ultrasql` rejects privilege/login/validity
  demotion while still allowing password rotation. Catalogued non-`CREATEROLE`
  roles cannot create, alter, drop, grant, or revoke roles. Known `NOLOGIN`
  and expired `VALID UNTIL` roles are rejected during startup authentication.
  `CONNECTION LIMIT` parses through role DDL and is enforced at startup with
  per-role live-session accounting that releases slots on disconnect.
  Catalogued `CREATEROLE` roles cannot grant `SUPERUSER`, `REPLICATION`, or
  `BYPASSRLS`; those privilege-bearing role attributes require superuser, and
  altering existing privileged roles also requires superuser. Privileged role
  memberships, `SET ROLE` into privileged roles, and default-privilege
  administration for privileged owner roles require superuser.
- `GRANT / REVOKE` on tables, schemas, databases, sequences, and functions work
  through privilege catalog checks; table privilege DDL now requires table
  ownership or superuser and records the actual grantor.
- Table-mutating DDL now reuses a shared owner/superuser guard: non-owners
  cannot `CREATE INDEX`, `DROP INDEX`, `COMMENT ON` table/index/column,
  `ALTER TABLE`, `TRUNCATE`, `DROP TABLE`, or `CREATE POLICY` against another
  role's table. Evidence:
  `cargo test -p ultrasql-server --test table_ownership_round_trip -- --nocapture`.
- Column-level privileges enforce `SELECT`, `INSERT`, and `UPDATE` target
  access.
- Role inheritance and `SET ROLE` support transitive membership, cycle
  rejection, `INHERIT` / `NOINHERIT`, `RESET ROLE`, `current_user`, and
  `session_user`.
- `pg_catalog.pg_auth_members` exposes durable role-membership edges with
  role, member, grantor, and admin-option OIDs. Evidence:
  `cargo test -p ultrasql-server --test role_ddl_round_trip role_catalog_survives_restart -- --nocapture`.
- Default privileges apply matching templates for future tables and sequences.
- Persistent RLS policies cover owner, superuser, `BYPASSRLS`, and restart
  semantics for the documented RAG tenant policy shape. `CREATE POLICY` now
  requires table ownership or superuser before mutating policy metadata.
- Row-security metadata reload rejects duplicate table rows and duplicate policy
  names for a table, avoiding ambiguous restart state from tampered sidecar
  rows. Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_metadata_rejects_duplicate_table_rows_on_rebuild -- --nocapture` and
  `cargo test -p ultrasql-server --test rls_round_trip rls_metadata_rejects_duplicate_policy_names_on_rebuild -- --nocapture`.
- Row-security metadata reload rejects table rows whose OID no longer exists in
  the catalog snapshot, avoiding silent restart skips for tampered sidecars.
  Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_metadata_rejects_unknown_table_rows_on_rebuild -- --nocapture`.
- Row-security metadata reload rejects policy role lists that reference unknown
  roles, avoiding ghost-role policies after restart. Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_metadata_rejects_unknown_policy_roles_on_rebuild -- --nocapture`.
- Row-security metadata reload validates persisted `USING` and `WITH CHECK`
  column indexes/names against the target table schema before install,
  avoiding stale or tampered predicate bindings after restart. Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_metadata_rejects_invalid_policy_columns_on_rebuild -- --nocapture`.
- Role-scoped RLS policies parse `CREATE POLICY ... TO role`, enforce inherited
  role membership, persist across restart, and fail closed when no scoped policy
  applies. Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_policy_roles_scope_visibility_and_restart -- --nocapture`.
- `pg_catalog.pg_policy` exposes live row-security policy metadata, including
  command, permissiveness, role OIDs, `USING`, and `WITH CHECK` expressions.
  Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip rls_tenant_policy_filters_reads_and_checks_inserts -- --nocapture`.
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
- `pg_catalog.pg_proc` exposes stable rows for core builtin introspection
  functions while stock `\df` still filters system routines out. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip psql_list_functions_probe_filters_builtin_pg_proc -- --nocapture`.
- `pg_catalog.pg_proc` also carries common typed routine metadata columns
  (`pronargs`, `prorettype`, `proargtypes`, volatility, owner, language, ACL),
  reducing driver/ORM catalog probe drift. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip psql_list_functions_probe_filters_builtin_pg_proc -- --nocapture`.
- `information_schema.routines` is backed by the same builtin routine surface,
  giving SQL-standard introspection a non-empty system function view. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_catalog_and_information_schema_reflect_runtime_objects -- --nocapture`.
- `pg_catalog.pg_publication_rel` now exposes relation links for existing
  publications using stable publication OIDs plus catalog table OIDs, so
  replication and psql introspection joins no longer see an empty link table.
  Evidence:
  `cargo test -p ultrasql-server --test logical_replication_round_trip create_publication_records_committed_dml_stream -- --nocapture`.
- `pg_catalog.pg_locks` now exposes central lock-table grants and waiters,
  including advisory lock `classid` / `objid`, mode, granted state, and owner
  pid when it can be derived from a session-level advisory lock. Evidence:
  `cargo test -p ultrasql-server --test advisory_lock_round_trip try_advisory_lock_conflicts_across_sessions_and_unlocks -- --nocapture`.
- Startup `application_name` is now retained in session settings, sent back in
  `ParameterStatus`, visible through `SHOW application_name`, and reflected in
  `pg_catalog.pg_stat_activity` along with the current session user. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_activity_reflects_session_identity -- --nocapture`.
- Operator statistics catalog views have tested base coverage for
  `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_stat_user_indexes`,
  `pg_stat_progress_create_index`, `pg_stat_database`, `pg_stat_bgwriter`,
  `pg_stat_progress_vacuum`, `pg_stat_progress_analyze`, `pg_stat_wal`, and
  `pg_stat_replication`. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip`;
  `cargo test -p ultrasql-server --test wal_stats_round_trip`;
  `cargo test -p ultrasql-server --test replication_stats_round_trip`.
- `pg_catalog.pg_stat_activity` now lists all open sessions from a live
  process-local registry, updates startup and `SET application_name` values,
  and removes rows when sessions close. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_activity_lists_open_sessions -- --nocapture`.
- `pg_catalog.pg_stat_activity` now tracks coarse `active` / `idle` state and
  exposes current query text while simple or extended statements execute,
  clearing query text when the session returns to idle. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_activity_lists_open_sessions -- --nocapture`.
- `pg_catalog.pg_stat_activity` now exposes backend timing and wait columns:
  `backend_start`, `xact_start`, `query_start`, `state_change`,
  `wait_event_type`, and `wait_event`, with active sessions showing a current
  `query_start` and idle sessions clearing it. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_activity_lists_open_sessions -- --nocapture`.
- `pg_catalog.pg_stat_activity.xact_start` now tracks explicit transaction
  lifecycle: null while idle, non-null after `BEGIN`, and cleared after
  `COMMIT` or `ROLLBACK`. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_activity_lists_open_sessions -- --nocapture`.
- `pg_catalog.pg_stat_activity.xact_start` now also appears while an active
  autocommit statement is running and clears when that session returns to idle,
  while explicit transaction timestamps persist across idle-in-transaction.
  Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_activity_lists_open_sessions -- --nocapture`.
- `pg_catalog.pg_stat_activity.state` now reports `idle in transaction` for
  sessions parked inside an explicit transaction and returns to `idle` after
  `COMMIT` or `ROLLBACK`. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_activity_lists_open_sessions -- --nocapture`.
- Idle `pg_catalog.pg_stat_activity` sessions now expose a basic client wait
  event (`Client` / `ClientRead`) and clear wait-event columns while actively
  executing a statement. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_activity_lists_open_sessions -- --nocapture`.
- `pg_catalog.pg_stat_user_tables` now exposes table maintenance counters:
  `last_vacuum`, `last_autovacuum`, `vacuum_count`, `autovacuum_count`,
  `last_analyze`, `last_autoanalyze`, `analyze_count`, and
  `autoanalyze_count`, with manual `VACUUM` / `ANALYZE` and background
  autovacuum / autoanalyze updating timestamp/count evidence. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_catalog_and_information_schema_reflect_runtime_objects -- --nocapture`;
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_user_tables_tracks_background_maintenance -- --nocapture`.
- Autovacuum and autoanalyze scheduling now use separate modification
  counters, so scheduling autoanalyze no longer clears the VACUUM trigger
  evidence for the same committed row changes. Evidence:
  `cargo test -p ultrasql-server --test catalog_views_round_trip pg_stat_user_tables_tracks_background_maintenance -- --nocapture`.
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
- Public parser/type baseline now keeps `SELECT 'int4'::regtype::text`
  active and returns canonical `integer` text instead of leaking raw OID `23`.
- Transaction isolation baseline covers `acid.sql`, Hermitage G1a/PMP/G2, and
  manager-level Hermitage matrix.
- Index regression baseline covers `CREATE INDEX`, `CREATE UNIQUE INDEX`,
  indexed equality/range reads, and unique violations.
- Constraint regression baseline covers primary key, check, not-null, foreign
  key, duplicate-key rejection, FK rejection, and check-on-update.
- Operator regression baseline covers comparison lexer/evaluator surfaces,
  `BETWEEN`, and `LIKE`.
- `CREATE OPERATOR === (...)` now parses, binds, records a runtime
  `pg_operator` row backed by built-in `bool_eq`, and removes the final local
  skip from the curated index/constraint/operator regression shard. Evidence:
  `cargo run -p ultrasql-sqllogictest-runner -- --mode in-process tests/slt/sql_regression/regression_subset/index_constraint_operator_baseline.slt`.
- `CREATE OPERATOR` runtime catalog metadata now persists to
  `pg_operator_runtime.meta` and reloads on `Server::init`, so custom operator
  `pg_operator` rows survive restart until typed catalog rows replace the
  sidecar before v1.0. The loader rejects tampered rows whose procedure/type
  signature is outside the DDL-supported subset, plus duplicate OIDs or
  duplicate operator signatures. Evidence:
  `cargo test -p ultrasql-server --test operator_catalog_round_trip create_operator_catalog_survives_restart -- --nocapture`
  and
  `cargo test -p ultrasql-server --test operator_catalog_round_trip -- --nocapture`.
- Type-specific regression baseline covers numeric, text, date/time/timetz,
  timestamp, JSON/JSONB, and arrays.
- `pg_type` now exposes built-in array type rows such as `_int4`, with
  `typelem` pointing back to the element type and public type-specific
  regression coverage active.
- Aggregate/window regression baseline covers grouped `COUNT` / `SUM` /
  `AVG` / `MIN` / `MAX`, `HAVING`, and core window functions:
  `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`,
  `last_value`, `nth_value`, and `ntile`.
- Type-coercion regression baseline covers explicit casts, assignment-compatible
  inserts, NULL casts, `COALESCE`, `CASE`, text casts, boolean casts, and
  overlength `VARCHAR` rejection.
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
  metadata slots do not leave failed role DDL in memory. The auth metadata
  loader rejects duplicate role names, role OIDs, and role-membership keys
  instead of silently applying last-row-wins state; it also rejects dangling
  membership role/member/grantor references, empty role names/refs, and zero
  role OIDs. It requires the bootstrap `ultrasql` role to remain present with
  its fixed OID and critical admin/login attributes before installing a restart
  snapshot. Evidence:
  `cargo test -p ultrasql-server --test role_ddl_round_trip -- --nocapture`.
- Privilege catalog restart persistence is covered by `pg_privileges.meta`:
  `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES`, and default-privilege
  application on future tables, materialized views, and sequences snapshot ACLs
  to the data directory with rollback on metadata-write failure. Evidence:
  `privilege_catalog_survives_restart` and
  `privilege_catalog_rolls_back_when_metadata_slot_is_unsafe`. The privilege
  metadata loader rejects duplicate grant/default-grant keys and unknown
  role references instead of silently applying last-row-wins or ghost-role
  ACL state; table column-level grants are validated against known table
  schemas before restart install, preventing stale column ACL resurrection.
  Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip privilege_metadata_rejects -- --nocapture`.
- Schema-qualified table DDL/DML hardening now covers `SELECT`, `INSERT`,
  `UPDATE`, `DELETE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, and
  `CREATE INDEX`, preventing wrong-qualified statements from mutating same-name
  tables in another schema.
  Evidence:
  `cargo test -p ultrasql-server --test schema_ddl_round_trip -- --nocapture`
  and `cargo test -p ultrasql-planner --lib -- --nocapture`.
- Schema-qualified `COPY TO/FROM` now rejects wrong-qualified table targets
  instead of exporting from or importing into a same-name table in another
  schema. Evidence:
  `cargo test -p ultrasql-server --test copy_round_trip -- --nocapture`.
- Schema-qualified `CREATE POLICY` now rejects wrong-qualified table targets
  instead of attaching RLS metadata to a same-name table in another schema.
  Evidence:
  `cargo test -p ultrasql-server --test rls_round_trip -- --nocapture`.
- Schema-qualified `COMMENT ON TABLE/COLUMN` now rejects wrong-qualified table
  targets instead of writing `pg_description` metadata for a same-name table in
  another schema. Evidence:
  `cargo test -p ultrasql-server --test comment_restart_round_trip -- --nocapture`.
- Schema-qualified `COMMENT ON INDEX` now carries explicit namespaces through
  binding and validates them against the indexed table schema before writing
  `pg_description`, preventing wrong-qualified index comments on public
  same-name indexes. Evidence:
  `cargo test -p ultrasql-server --test comment_restart_round_trip -- --nocapture`.
- Schema-qualified foreign-key targets now retain the parsed target object
  through binding, so `REFERENCES schema.table(...)` cannot silently bind a
  same-name table in another schema. Evidence:
  `cargo test -p ultrasql-server --test constraint_round_trip -- --nocapture`.
- Schema-qualified `ALTER SEQUENCE` and `DROP SEQUENCE` now carry explicit
  namespaces through the logical plan and reject wrong-qualified sequence
  targets instead of altering or dropping a same-name sequence in another
  schema. Evidence:
  `cargo test -p ultrasql-server --test sequence_round_trip -- --nocapture`.
- Schema-qualified `DROP INDEX` now preserves explicit index namespaces through
  the logical plan and validates them against the indexed table schema before
  privilege/dependency work, preventing wrong-qualified drops from deleting a
  same-name public index. Evidence:
  `cargo test -p ultrasql-server --test drop_index_round_trip -- --nocapture`.
- Table privilege DDL now validates table object existence and explicit schema
  qualifiers before the superuser administration bypass, preventing ghost
  grants such as `GRANT ... ON TABLE missing_schema.same_name`. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip -- --nocapture`.
- Sequence privilege DDL now validates explicit schema qualifiers before the
  superuser administration bypass and stores valid qualified sequence grants
  under the canonical sequence key. Evidence:
  `cargo test -p ultrasql-server --test sequence_round_trip -- --nocapture`.
- Schema privilege DDL now validates schema existence before the superuser
  administration bypass while preserving built-in schema grants, preventing
  ghost grants on missing schemas. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip -- --nocapture`.
- Schema-scoped default privilege DDL now validates every explicit `IN SCHEMA`
  target before mutating ACL metadata, preventing missing-schema default grants
  from applying to future schemas/tables. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip -- --nocapture`.
- Database and function privilege DDL now validates object existence before the
  superuser administration bypass, preventing ghost grants on missing databases
  and missing functions while preserving valid `ultrasql` database and
  `pg_proc` built-in function grants. Evidence:
  `cargo test -p ultrasql-server --test privilege_catalog_round_trip -- --nocapture`.
