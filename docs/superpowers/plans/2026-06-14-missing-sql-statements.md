# Missing SQL Statements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add production SQL support for `DESCRIBE`, `ALTER VIEW`, `MERGE INTO`, `CHECKPOINT`, `EXPORT DATABASE`, `IMPORT DATABASE`, `SET VARIABLE`, `PIVOT`, `UNPIVOT`, and `SUMMARIZE`.

**Architecture:** Implement one statement family at a time. Each family starts with parser and binder tests that fail, then adds AST, logical plan, execution, integration coverage, and docs only after working behavior exists. Existing support is reused: `SET`/`SHOW`/`RESET` already have binder/executor paths; checkpointing has WAL/storage primitives; `COPY` provides portable file I/O patterns; aggregate and join plans provide most analytics rewrite machinery.

**Tech Stack:** Rust 2024, `ultrasql-parser`, `ultrasql-planner`, `ultrasql-server`, `ultrasql-catalog`, sqllogictest, PostgreSQL wire result encoding, MVCC heap/WAL.

---

## Starting Evidence

- `SET` / `SHOW` / `RESET` already parse as `Statement::SetVar`, bind to `LogicalPlan::SetVariable`, and execute in `crates/ultrasql-server/src/session/execute.rs`.
- PostgreSQL protocol `Describe` exists, but SQL `DESCRIBE` does not.
- `CHECKPOINT` WAL/storage pieces exist: `CheckpointPayload`, `RecordType::Checkpoint`, storage `Checkpointer`, server `flush_dirty_heap_pages`, and WAL writer durability wait. SQL dispatch does not exist.
- Normal `CREATE VIEW` is not user-facing yet; parser only has `CREATE MATERIALIZED VIEW`. `ALTER VIEW` needs normal view catalog semantics or explicit scope limited to materialized/catalog views.
- `MERGE INTO`, `EXPORT DATABASE`, `IMPORT DATABASE`, `PIVOT`, `UNPIVOT`, and `SUMMARIZE` have no parser/planner/executor statement surface.

## Commit Policy

Each family gets atomic commits in this shape:

1. `parser(<scope>): add <statement> syntax`
2. `planner(<scope>): bind <statement> semantics`
3. `executor(<scope>): execute <statement>`
4. `test(<scope>): cover <statement> round trips`
5. `docs(<scope>): document <statement> support`

Docs commits happen only after passing implementation tests for that family.

## Baseline

- [ ] Run `cargo build --workspace --all-features`.
- [ ] Run `cargo test -p ultrasql-parser`.
- [ ] Run `cargo test -p ultrasql-planner`.
- [ ] Run `cargo test -p ultrasql-server --tests`.
- [ ] Run `tests/slt/run_sql_regression.sh` if baseline unit tests pass.
- [ ] Record any pre-existing failures before editing.

## Phase 1: DESCRIBE

**Files:**
- Modify: `crates/ultrasql-parser/src/ast.rs`
- Modify: `crates/ultrasql-parser/src/parser/mod.rs`
- Create: `crates/ultrasql-parser/src/statements/describe.rs`
- Modify: `crates/ultrasql-parser/src/statements/mod.rs`
- Modify: `crates/ultrasql-planner/src/plan.rs`
- Modify: `crates/ultrasql-planner/src/binder/mod.rs`
- Modify: `crates/ultrasql-planner/src/binder/tests.rs`
- Modify: `crates/ultrasql-server/src/session/execute.rs`
- Create: `crates/ultrasql-server/tests/describe_round_trip.rs`
- Create: `docs/sql/describe.md` if docs use per-statement pages; otherwise add to nearest SQL reference page.

- [x] Add parser red tests for `DESCRIBE users`, `DESCRIBE TABLE users`, `DESCRIBE SELECT 1 AS x`, and malformed `DESCRIBE`.
- [x] Add AST:
  - `Statement::Describe(DescribeStmt)`
  - `DescribeTarget::{Object { kind, name }, Query(Box<SelectStmt>)}`
  - `DescribeObjectKind::{Any, Table, View}`
- [x] Parse `DESCRIBE [TABLE|VIEW] object_name` and `DESCRIBE SELECT ...`.
- [x] Add binder red tests for table schema, query expression schema, missing object, and wrong kind.
- [x] Bind to `LogicalPlan::Describe { target, schema }` with fixed output columns:
  - `column_name text`
  - `data_type text`
  - `nullable bool`
  - `source_schema text`
  - `source_object text`
  - `source_kind text`
- [x] Execute table describes from catalog snapshot and query describes from the bound plan schema.
- [x] Add integration tests for table, query, missing object, and view-kind unsupported error until normal views exist.
- [x] Add docs after tests pass with syntax, examples, limitations, and links to test/source paths.
- [x] Commit parser/planner/executor/tests/docs separately.

## Phase 2: ALTER VIEW

**Files:**
- Modify parser AST and `parser/mod.rs` alter dispatch.
- Add `crates/ultrasql-parser/src/statements/alter_view.rs`.
- Modify planner DDL binding and `LogicalPlan`.
- Extend catalog mutation APIs for view rename and schema move only after view entries are real relations.
- Add `crates/ultrasql-server/tests/alter_view_round_trip.rs`.

- [ ] First add normal `CREATE VIEW` if catalog cannot create user views yet; without that, `ALTER VIEW` cannot be production-complete.
- [ ] Implement `ALTER VIEW name RENAME TO new_name`.
- [ ] Implement `ALTER VIEW name SET SCHEMA schema_name` only against real schema catalog support.
- [ ] Implement `ALTER VIEW name AS SELECT ...` only if stored view definitions and dependency validation exist; otherwise parse and reject with structured `NotSupported`.
- [ ] Add rollback tests if DDL transactions become supported; current server rejects DDL in explicit transactions, so test the existing rejection path if unchanged.

## Phase 3: MERGE INTO

**Files:**
- Create parser statement module for merge.
- Extend AST with `MergeStmt`, `MergeSource`, `MergeAction`.
- Extend planner DML binding and `LogicalPlan`.
- Extend `crates/ultrasql-server/src/pipeline/modify.rs` or session DML dispatch.
- Add `crates/ultrasql-server/tests/merge_round_trip.rs`.

- [x] Support `MERGE INTO target USING source ON condition`.
- [x] Support `WHEN MATCHED THEN UPDATE SET ...`.
- [x] Support `WHEN MATCHED THEN DELETE`.
- [x] Support `WHEN NOT MATCHED THEN INSERT [(cols)] VALUES (...)`.
- [x] Reject multiple source rows matching one target row with deterministic SQLSTATE/error text before mutating target.
- [x] Use one transaction boundary for all branches; rollback all branch effects on error.
- [x] Test update, delete, insert, no-op, duplicate match, `NULL` match semantics, rollback, indexes, and constraints.

## Phase 4: CHECKPOINT

**Files:**
- Add parser statement and logical plan.
- Add server execution in `session/execute.rs`.
- Add checkpoint helper on `Server` near `flush_dirty_heap_pages`.
- Add tests in `crates/ultrasql-server/tests/checkpoint_round_trip.rs` and storage/WAL unit tests if new helpers are added.

- [x] Parse bare `CHECKPOINT`; reject options until supported.
- [x] Bind to empty-schema `LogicalPlan::Checkpoint`.
- [x] Execute by notifying WAL writer, waiting for durable LSN, flushing dirty pages whose page LSN is durable, appending checkpoint WAL record, waiting for it durable, and publishing `last_checkpoint_lsn`.
- [x] For in-memory server without WAL writer, return successful no-op after dirty-page flush.
- [ ] Add concurrent read/write safety tests using existing server test harness.

## Phase 5: EXPORT DATABASE / IMPORT DATABASE

**Files:**
- Add parser/admin statement module.
- Add logical plans for export/import.
- Add deterministic dump format module under `crates/ultrasql-server/src/export_import.rs`.
- Add `crates/ultrasql-server/tests/export_import_round_trip.rs`.
- Add docs page for dump format.

- [ ] Format: directory with `manifest.json`, `schema.sql`, one data file per table in deterministic order, and `checksums.json`.
- [ ] Export only after a checkpoint or equivalent read-consistent snapshot fence.
- [ ] Include tables, indexes, materialized views, sequences, schemas, comments, and supported metadata.
- [ ] Import into empty database only in first implementation; reject non-empty targets.
- [ ] Validate manifest version and checksums.
- [ ] Round-trip schema/data/query results.

## Phase 6: SET VARIABLE

**Files:**
- Modify `crates/ultrasql-parser/src/statements/set_stmt.rs`.
- Add parser/planner/server tests for exact spelling.
- Update configuration docs after passing tests.

- [x] Parse `SET VARIABLE name = value` and `SET VARIABLE name TO value` as the same AST as existing session `SET`.
- [x] Keep scope session-local.
- [x] Preserve existing type validation in `apply_session_variable`.
- [x] Reject `SET LOCAL VARIABLE` unless deliberately supported with tests.
- [x] Add tests for invalid names, invalid types, session visibility, transaction behavior, and prepared usage.

## Phase 7: PIVOT / UNPIVOT

**Files:**
- Extend `TableRef` AST.
- Extend select parser table-factor grammar.
- Add binder rewrites in a new focused module, likely `crates/ultrasql-planner/src/binder/pivot.rs`.
- Reuse aggregate/project/filter/values plans; add executor code only if rewrite cannot express behavior.
- Add `crates/ultrasql-server/tests/pivot_unpivot_round_trip.rs`.

- [ ] Use explicit documented syntax, not silent DuckDB/Oracle superset parsing.
- [ ] Lower `PIVOT` to grouped aggregates with conditional expressions.
- [ ] Lower `UNPIVOT` to a union/values rewrite preserving types and null policy.
- [ ] Reject duplicate pivot values and mixed incompatible output types.
- [ ] Test grouping, nulls, aliases, empty input, duplicates, and mixed types.

## Phase 8: SUMMARIZE

**Files:**
- Add parser statement.
- Add logical plan.
- Execute through an internally generated aggregate plan or direct table scan summary operator.
- Add `crates/ultrasql-server/tests/summarize_round_trip.rs`.

- [ ] Support `SUMMARIZE table_name`.
- [ ] Return per-column rows with `column_name`, `data_type`, `count`, `null_count`, `min`, `max`, `approx_unique`, `avg`, and `stddev` where meaningful.
- [ ] Use exact `COUNT(DISTINCT)` if available for small test correctness; document cost.
- [ ] Return `NULL` for unsupported stats by type instead of fake values.
- [ ] Test numeric, text, bool, date/time, null-heavy, and empty tables.

## Final Verification

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] `tests/slt/run_sql_regression.sh`
- [ ] Docs build/check command if present.
- [ ] Final report: implemented statements, unsupported subclauses, tests, docs, known limitations, commands and results.
