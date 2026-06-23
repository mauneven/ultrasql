# TODO — UltraSQL open work to a production-grade, PostgreSQL-faithful database

This file is the single, comprehensive, **honest** list of everything still needed to reach a
production-grade, PostgreSQL-faithful "perfect database". It **replaces ROADMAP.md** as the
authoritative open-work tracker.

**Status: pre-1.0, NOT production-ready.** UltraSQL is a fast alpha. Do not claim production
readiness, "best in every aspect", or write/OLTP leadership. Do not expose the server on an
untrusted network without a soak-tested deployment.

- For **current shipped limitations** (what is true of the code today), see
  [`docs/known-limitations.md`](docs/known-limitations.md).
- For **completed work with evidence**, see [`DONE.md`](DONE.md).

Every item below is traceable to concrete evidence (file:line or committed artifact). Items use
`- [ ] <title> — <description> (criticality; evidence)`. Reverted work (e.g. the SAVEPOINT
own-write visibility attempt) is listed here as TODO, not done.

---

## Most critical next

The top items by criticality and blast radius. These are the things that block any honest
"production-grade, PostgreSQL-faithful" claim.

1. **#1 — SAVEPOINT subtransaction visibility — ✅ LANDED (safe increment, adversarially gated).**
   The data-integrity defect is fixed for single-phase transactions: a transaction sees its own
   writes under an active SAVEPOINT; `ROLLBACK TO` hides a savepoint's inserts and restores its
   deletes / in-place-update pre-images (verified from a second connection after `COMMIT`); `RELEASE`
   keeps writes without leaking them before the parent commits; and the parent's commit/abort folds
   the whole subxid family atomically **and durably** (the commit WAL record carries the committed
   subxid list, so single-phase `COMMIT` recovers correctly). The index access method now follows
   PostgreSQL's lossy-index + heap-recheck model (no per-subxid index undo). Implemented per
   [`docs/savepoint-subtransactions-design.md`](docs/savepoint-subtransactions-design.md) and gated on
   the §5 adversarial battery plus two independent adversarial re-reviews — which caught and fixed a
   unique-index liveness bug (aborted-deleter double-live-key) and a recovery data-loss bug
   (released/open subxid) **before any push**.
   **Remaining follow-ups** (tracked in `docs/known-limitations.md`): two-phase commit does not yet
   carry the subxid family, so `COMMIT PREPARED` recovery can lose a savepoint row (pre-existing); the
   fused fast-path `DELETE` stays gated onto the general MVCC path under an open savepoint (perf, not
   correctness); `COPY FROM STDIN` and `ALTER TABLE` heap-rewrite DDL under a savepoint stamp the
   parent xid.
2. **Predicate-precise SSI / serializable correctness** — column-range SSI degrades to relation-wide
   locks for most types and is not page/tuple/gap-precise; blocks any serializable-correctness claim
   (high).
3. **EvalPlanQual / READ COMMITTED re-check on concurrent UPDATE/DELETE** — concurrent-writer
   conflicts surface as errors instead of waiting + re-evaluating; lost-update breadth unproven (high).
4. **Continuous networked + synchronous physical replication** — replication is offline WAL-file
   copying only; no walsender/walreceiver wire protocol, no streaming hot-standby apply, no sync
   commit, no failover/promotion. Blocks HA/DR (critical).
5. **CLOG persistence + transactional DDL** — in-memory DashMap commit log and DDL hard-rejected
   inside explicit transaction blocks; both are atomicity/durability correctness gaps and block
   ORM/migration certification beyond autocommit (high).

> **Most critical next: predicate-precise SSI / serializable correctness (#2).** SAVEPOINT (#1)
> landed as a gated safe increment; the remaining 2PC/perf/DDL follow-ups are tracked in
> `docs/known-limitations.md`.

---

## Correctness & MVCC

- [x] SAVEPOINT own-write visibility — **DONE** (safe increment, adversarially gated). `Snapshot` now carries own-subxid sets (live + rolled-back) and `is_current_xid` recognises descendant subxids; every DML write path stamps the active subtransaction id via `Transaction::write_xid()` (debug-assert guarded). Adopted PostgreSQL's lossy-index + heap-recheck model instead of per-subxid index undo (the approach that sank the first attempt). The committed subxid family is folded atomically and durably (commit WAL record carries the list). See `docs/savepoint-subtransactions-design.md` and the `savepoint_subtxn_battery_round_trip` battery.
- [x] Rolled-back-savepoint hiding — **DONE**, but via a different design than this item proposed: the dead `is_visible_ext` / `SubxactOracle` / `InfoMask::SUBXACT` path was **deleted**; rolled-back rows are now hidden by `own_subxid_rolled_back(xmin/xmax)` guards in the single `is_visible` predicate driven by the snapshot's own-subxid sets (no on-disk SUBXACT bit needed). Covered by battery tests B/C/E on both shapes.
- [ ] Predicate-precise serializable isolation (SSI) — implement page/tuple/gap precision; today column-range SSI exists only for i64-mappable types (Bool/Int16/32/64/Timestamp/TimestampTz) and AND/OR scalar trees, everything else (text/numeric/float/date/uuid, joins, function predicates) degrades to relation-wide tags, so the engine is `not fully predicate-precise` SSI, causing spurious 40001 aborts and blocking broad serializable claims (high; `crates/ultrasql-txn/src/ssi.rs:10-12,416-417`, `crates/ultrasql-server/src/serializable.rs:428-438`, `docs/known-limitations.md:24-28`).
- [ ] Real SSI engine + full isolation suite — serializable reuses the fixed RR snapshot + dangerous-structure check, not the full PostgreSQL SSI engine; executable coverage is only acid.sql + Hermitage G1a/PMP/G2. Import the upstream `src/test/isolation` isolationtester schedules and match expected outputs (high; `crates/ultrasql-txn/src/manager.rs:339-345`, `docs/testing/isolation-suite.md:14,32-43`).
- [ ] EvalPlanQual / READ COMMITTED re-check on concurrent UPDATE/DELETE — on a concurrently-committed row, wait on the row lock then re-evaluate the qual against the new version instead of erroring. Today `HeapError::WriteConflict` propagates; the only committed-conflict detection is the int32-pair fast path, so lost-update breadth across arbitrary DML is unproven (high; `crates/ultrasql-storage/src/heap/update_inplace.rs:674-685`, `crates/ultrasql-executor/src/fused_update.rs:321-344`).
- [ ] SERIALIZABLE READ ONLY DEFERRABLE + safe-snapshot deferral — no read-only-transaction safe-snapshot detection, no deferrable mode; every serializable txn is fully rw-conflict-tracked regardless of read-only status. `SET TRANSACTION` does not parse/honor READ ONLY / DEFERRABLE (medium; `crates/ultrasql-txn/src/ssi.rs:640-655`, `crates/ultrasql-server/src/session/txn.rs:321-349`).
- [ ] Deadlock detection + lock-timeout GUCs — wait-for graph is built only from the central LockManager (no SSI edges, no orphaned prepared-txn holds), runs on a fixed 1s interval (not on-demand-after-`deadlock_timeout`), victim is always the youngest XID. Add `lock_timeout`, `deadlock_timeout`, `idle_in_transaction_session_timeout`, cost-based victim selection, and a real `acquire()` timeout path (medium; `crates/ultrasql-txn/src/lock.rs:746-773,854-855`, `crates/ultrasql-server/src/session/mod.rs:137-138`).
- [ ] Full advisory-lock family — only session `pg_advisory_lock`/`try`/`unlock`/`unlock_all` + `pg_try_advisory_xact_lock` are wired, exclusive-only, on the simple-query literal fast path. Add all `_shared` variants, blocking `pg_advisory_xact_lock`(`_shared`), `pg_try_advisory_xact_lock_shared`, the two-int32 overloads, and call from any expression context / with params (medium; `crates/ultrasql-server/src/session_state.rs:100-160`, `crates/ultrasql-server/src/session/advisory.rs:46-84`).
- [ ] Persistent page-backed CLOG + freeze/wraparound machinery — commit log is an in-memory DashMap rebuilt from checkpoint + WAL scan. Add a persistent page-backed CLOG, and PostgreSQL-compatible freeze policy: `relfrozenxid`/`datfrozenxid`, anti-wraparound autovacuum, `age()`, vacuum freeze (Xid is u64 so no 32-bit wrap risk, but no freeze machinery exists) (high; `crates/ultrasql-txn/src/lib.rs:34-39`, `crates/ultrasql-core/src/id.rs:132`).
- [ ] MVCC long-tail: multixact, key-share/no-key-update semantics, hint-bit advancement — visibility deliberately omits multixact members, key-share locks, and infomask hint-bit advancement; a row cannot record multiple distinct lockers with mixed modes, and ForNoKeyUpdate/ForKeyShare strength is enforced only via the generic conflict matrix, not tied to whether the UPDATE changes key columns (medium; `crates/ultrasql-mvcc/src/visibility.rs:1-7`, `crates/ultrasql-txn/src/row_lock.rs:58-70`).
- [ ] Long-running-txn / bloat bounds — no `idle_in_transaction_session_timeout` to bound a txn pinning the xmin horizon; harden VACUUM/autovacuum scheduling, freeze policy, and deeper bloat metrics (high; `crates/ultrasql-txn/src/lib.rs:36-39`).

## SQL surface & types

- [ ] Transactional DDL inside explicit transaction blocks — all DDL inside `BEGIN…COMMIT` is hard-rejected (SQLSTATE 0A000, txn→Failed 25P02); covers CREATE/DROP/ALTER TABLE, views/matviews, types/domains/operators, indexes, roles/privileges/schemas/sequences, COMMENT, CHECKPOINT, TRUNCATE, EXPORT/IMPORT. Blocks atomic multi-statement migrations and limits ORM cert to autocommit/nontransactional mode (high; `crates/ultrasql-server/src/session/execute/query.rs:302-305`, `docs/known-limitations.md:137-139`).
- [ ] DDL via Extended Query (Parse/Bind/Execute) — DDL through the extended protocol is rejected ("DDL via Extended Query is not yet wired; use Simple Query"); drivers that prepare DDL must fall back to Simple Query (medium; `crates/ultrasql-server/src/extended/execute.rs:86-90`).
- [ ] FOREIGN KEY / EXCLUDE via ALTER TABLE ADD CONSTRAINT — ADD CONSTRAINT supports only PK/UNIQUE/CHECK; FK and EXCLUDE can only be declared in CREATE TABLE (high; `crates/ultrasql-planner/src/binder/ddl/alter_table.rs:159-168`).
- [ ] ALTER TABLE action breadth — no ALTER COLUMN … TYPE, SET/DROP NOT NULL, SET/DROP DEFAULT, SET STATISTICS, OWNER TO, SET SCHEMA; ADD COLUMN accepts only NULL/NOT NULL/DEFAULT; SET supports autovacuum reloptions only (high; `crates/ultrasql-parser/src/ast/schema.rs:290-353`, `crates/ultrasql-planner/src/binder/ddl/alter_table.rs:205,316`).
- [ ] Updatable views (INSERT/UPDATE/DELETE through a view) — simple-updatable-view DML not supported; also unblocks INSTEAD OF triggers / complex updatable views (high; `known-limitations.md:63-65`).
- [ ] WITH CHECK OPTION on views — no WITH [LOCAL|CASCADED] CHECK OPTION; depends on updatable views (medium; `known-limitations.md:63-65`).
- [ ] Dependency-safe CREATE OR REPLACE VIEW / ALTER VIEW … AS — both rejected pending dependency-safe in-place view replacement; today must DROP then CREATE (medium; `crates/ultrasql-server/src/session/ddl/views.rs:205-208`).
- [ ] Materialized view REFRESH + index parity — no REFRESH grammar at all; matviews populate only at CREATE time, cannot REFRESH [CONCURRENTLY] or carry their own indexes (high; no REFRESH keyword in `crates/ultrasql-parser`, `known-limitations.md:63-65`).
- [ ] General RANGE/LIST/HASH declarative partitioning — parser only accepts PARTITION BY RANGE, binder only time-chunks a single TIMESTAMP/TIMESTAMPTZ column. No LIST/HASH, multi-column RANGE, bounds, ATTACH/DETACH, pruning, partition-wise joins/aggregates, cross-partition update routing, or Append/MergeAppend plans (high; `crates/ultrasql-parser/src/statements/create_table.rs:92`, `crates/ultrasql-planner/src/binder/ddl/create_table.rs:295-316`).
- [ ] PL/pgSQL, stored procedures, CALL — no CREATE FUNCTION/PROCEDURE/CALL grammar; PL/pgSQL entirely absent (variables, control flow, dynamic EXECUTE, exceptions, cursors, %TYPE/%ROWTYPE, SETOF, OUT/INOUT) (high; no function/procedure/call keywords in `crates/ultrasql-parser/src/keywords.rs`).
- [ ] Triggers (row/statement/INSTEAD OF/constraint/WHEN/NEW-OLD) — no CREATE TRIGGER grammar; INSTEAD OF also blocks complex updatable views (high; no `trigger` keyword in `crates/ultrasql-parser/src/keywords.rs`).
- [ ] Event triggers — ddl_command_start/end, table_rewrite, sql_drop not implemented (low; `known-limitations.md:47-48`).
- [ ] Extension loading (CREATE EXTENSION / LOAD) — no grammar; pgcrypto/uuid-ossp ecosystem unavailable (medium; no extension/load DDL keyword in `crates/ultrasql-parser/src/keywords.rs`).
- [ ] Full locale/collation (ICU, CREATE COLLATION, non-bytewise sort) — only `default`/`C`/`POSIX` recognized, all byte-wise; no ICU/libc linguistic ordering, no CREATE COLLATION, no collation catalog deparse (high; `crates/ultrasql-planner/src/binder/expr_bind/coerce_common.rs:20-44`).
- [ ] Domain type breadth — no domain-over-domain (rejected "not implemented"), no DEFAULT on domains (DomainConstraint has only NotNull/Null/Check) (low; `crates/ultrasql-planner/src/binder/ddl/types.rs:118-150`).
- [ ] Composite type breadth — nested composites, full row-value/record operations, composite arrays open beyond the metadata/scalar subset (low; `crates/ultrasql-planner/src/binder/ddl/types.rs:78-93`).
- [ ] Full XML / XMLTABLE coverage — bounded/secure XPath subset; unsupported row paths error "unsupported XPath". Full namespaces, broader axes/functions, full typed projection open (low; `crates/ultrasql-server/src/pipeline/xml_table_scan.rs:3,56-58`).
- [ ] Hypothetical-set / DISTINCT ordered-set / more statistical aggregates — only PERCENTILE_CONT/DISC via WITHIN GROUP; add rank/dense_rank/percent_rank/cume_dist WITHIN GROUP, mode(), regr_*/covar_*; allow DISTINCT on ordered-set forms (medium; `crates/ultrasql-planner/src/binder/aggregate.rs:376,547`).
- [ ] Aggregate/window FILTER clause — `agg(x) FILTER (WHERE p)` is a parse error; `KwFilter` is tokenized but never consumed and `Expr::Call` has no filter field (medium; `crates/ultrasql-parser/src/token.rs:230`, `crates/ultrasql-parser/src/ast/expr.rs:51-67`).
- [ ] avg() over integer → numeric (not double precision) — AVG over integers resolves to Float64 (DuckDB/SQLite oracle), not PostgreSQL arbitrary-precision numeric (medium; `crates/ultrasql-planner/src/binder/aggregate.rs:143-151`).
- [ ] RANGE offset PRECEDING/FOLLOWING on date/time/interval keys — value-offset RANGE frames require one numeric ORDER BY column; date/time/interval keys explicitly deferred (medium; `crates/ultrasql-planner/src/binder/window.rs:763-786`).
- [ ] NUMERIC/DECIMAL true arbitrary precision — runtime decimal is a scaled i64, overflows ("decimal overflow") beyond ~18 significant digits; wire encoding emits base-10000 but runtime is range-bounded vs PostgreSQL's true arbitrary precision (high; `crates/ultrasql-core/src/decimal.rs:3,113`).
- [ ] CREATE INDEX CONCURRENTLY truly online — the flag flows into the plan but `create_index` ignores it (blocking build); aggregating-index CONCURRENTLY is rejected. Implement lock-free/online build (medium; `crates/ultrasql-planner/src/plan/logical_plan.rs:585-586`, `crates/ultrasql-server/src/session/ddl/create_index.rs`).
- [ ] Broader CAST / type matrix — non-literal CAST expressions, some CAST targets, and some CREATE TABLE column types are NotSupported ("not implemented in v0.5") (low; `crates/ultrasql-planner/src/binder`).
- [ ] Extended-protocol LIMIT/OFFSET + parameter edge cases — non-literal LIMIT/OFFSET expressions NotSupported; param counts/indices beyond protocol limits rejected (low; `crates/ultrasql-server/src/extended/handlers.rs:128,265-274`).
- [ ] Full-text search parity — text-backed representation, not native tsvector/tsquery; add native lexeme/query storage, dictionaries, ts_rank/ts_headline parity, and GIN planner integration (medium; ).
- [ ] SQL/JSON path parity — documented subset; unsupported path syntax/regex flags/escapes/methods error; datetime(template) supports only ISO second/minute/fractional templates. Implement full path language + non-ISO coercions (low; `crates/ultrasql-executor/src/json_path.rs:159,1071,1373-1442,1614`).
- [ ] Array + date/time type breadth — broader array element-family coercion and remaining timezone edge gaps beyond the completed AT TIME ZONE / IANA / DateStyle subset (low; ).

## Durability, Recovery, Replication & Backup

- [ ] Continuous networked physical replication (walsender/walreceiver wire protocol) — replication is offline WAL-file copying between dirs; no START_REPLICATION / IDENTIFY_SYSTEM / CREATE_REPLICATION_SLOT / BASE_BACKUP / primary_conninfo. A real standby cannot stream WAL over libpq (critical; `crates/ultrasql-server/src/replication.rs:897-995`, `docs/known-limitations.md:100-102`).
- [ ] Streaming hot-standby apply (online replay loop) — a standby only replays WAL at startup; running standby never sees primary changes until restarted. Add an online apply loop, replication-lag tracking, and apply feedback (critical; `crates/ultrasql-server/src/session/execute/query.rs:50-52`, `crates/ultrasql-cli/src/cli_support/wal_ship.rs:65-102`).
- [ ] Synchronous replication modes — `synchronous_commit` is accepted but inert; no `synchronous_standby_names`, no quorum/priority set, commit never waits for standby ack. No cross-node RPO=0 (high; `crates/ultrasql-server/src/session/execute/describe.rs:344-346,423`).
- [ ] Replication failover / promotion / timelines / slot WAL retention — no `pg_promote`/trigger-file promotion, no timeline-ID or `.history` files; replication slots don't pin WAL retention on a live primary, so the checkpoint truncation floor can recycle segments a lagging standby still needs. No automated HA failover (high; `crates/ultrasql-server/src/replication.rs:802-895`, `crates/ultrasql-wal/src/truncate.rs:122-145`).
- [ ] True cascading replication — `receive_once_cascading` is file copying with the same restart-required, non-streaming limits; no chained streaming standby, no timeline/promotion handling (medium; `crates/ultrasql-server/src/replication.rs:974-994`).
- [ ] Logical decoding + pgoutput — CDC records only statement-level `LogicalChange{table, kind, rows_affected}`; no row images, REPLICA IDENTITY, TOAST handling, pgoutput binary protocol, or in-progress-xact streaming. Subscriptions store conninfo but never connect, so Debezium/pgoutput consumers can't use it (high; `crates/ultrasql-server/src/replication.rs:380-425`, `docs/known-limitations.md:103`).
- [ ] PREPARE TRANSACTION WAL-logging + lock re-hold on recovery — 2PC writes a hand-rolled JSON state file per gid (not WAL), CLOG is in-memory, and recovery restores InProgress status but does NOT re-acquire the prepared txn's row/relation locks, so a recovered in-doubt txn's rows are unprotected until COMMIT/ROLLBACK PREPARED. State-file parser is a bespoke non-serde single-layout reader (high; `crates/ultrasql-txn/src/two_phase.rs:360-401`, `crates/ultrasql-server/src/server_wal_recovery.rs:438-451`).
- [ ] Non-blocking online backup window — `/backup/start` flips the whole server read-only ("hot standby is read-only") instead of allowing concurrent writes with full-page-image torn-page safety + a backup_label LSN range bounding WAL replay (high; `crates/ultrasql-server/src/main_support/ops.rs:31-55`).
- [ ] pg_dump/restore completeness + per-workload round-trip validation — `--pg-dump`/`--pg-restore` is an UltraSQL-native data-dir archive (not pg_dump-compatible), smoke-verified only on a 3-row single-table fixture. Validate broad schema/type/constraint round-trip across realistic workloads (high; `docs/backup-restore.md:18-47`, `docs/known-limitations.md:104`).
- [ ] WAL recycling robustness — recycling is disabled outright when a required vector-index snapshot isn't durable (unbounded WAL growth + full replay); the floor excludes physical replication slots. Add fuzzing/soak for the recycle+restart path (the prior block-count-undercount bug shows fragility) (medium; `crates/ultrasql-wal/src/truncate.rs:142-206`).
- [ ] Group commit to amortize F_FULLFSYNC — every fsync uses F_FULLFSYNC; per-commit cost dominates and insert_throughput_10k / mixed_oltp lose to PostgreSQL/SQLite ("honest losses"). Exit: a group-commit artifact amortizing fsync across concurrent committers, or a same-durability comparison (medium; `crates/ultrasql-wal/src/writer.rs`).

## Security & Admin

- [ ] Typed catalog rows for roles/privileges/default-privileges/RLS (replace runtime sidecars) — role/membership/privilege/default-privilege/schema/sequence-owner/operator/RLS state is escaped text in sidecar files rebuilt at startup, not MVCC-versioned `pg_authid`/`pg_auth_members`/`relacl`/`pg_default_acl`/`pg_policy` rows with migrations. v1.0 blocker (no transactional DDL on grants, no per-row MVCC, ad hoc format) (high; `docs/known-limitations.md:90-91`, `crates/ultrasql-server/src/metadata_io.rs:1-3`, `crates/ultrasql-server/src/server_meta_role_priv.rs:1-40`).
- [ ] pg_hba md5 / password auth methods — both parse but always return Ok(false) at auth time because credentials are stored only as SCRAM verifiers; only Trust/Reject/scram-sha-256 function per-role. Store per-role secrets enabling md5 digest / cleartext verification (medium; `crates/ultrasql-server/src/session/startup.rs:523-531`, `crates/ultrasql-server/src/auth/hba.rs:347-353`).
- [ ] Client-certificate auth (`cert`) + pg_ident user mapping — TLS works (with the CVE-2021-23214 buffered-plaintext guard) but the `cert` pg_hba method and `pg_ident.conf` system-user→role maps are not wired (medium; `docs/known-limitations.md:85-88`, `crates/ultrasql-server/src/auth/hba.rs:21`).
- [ ] GSSAPI / Kerberos (GSSENCRequest) — GSSENCRequest is decoded but declined; pg_hba `gssapi` is a parse error. No GSSAPI transport encryption or Kerberos auth (low; `crates/ultrasql-server/src/session/startup.rs:89`, `crates/ultrasql-server/src/auth/hba.rs:506`).
- [ ] RLS policy breadth — only the documented tenant policy shape is certified; add per-command FOR SELECT/INSERT/UPDATE/DELETE shapes, USING vs WITH CHECK divergence, PERMISSIVE/RESTRICTIVE composition, arbitrary policy expressions, full FORCE ROW LEVEL SECURITY (medium; `docs/known-limitations.md:94-96`, `benchmarks/results/latest/rls_tenant_certification.json`).
- [ ] Admin-tool mutation workflows + desktop GUI launch/click smoke — pgAdmin/DBeaver/DataGrip certified only for read-only introspection query families; GUI create/alter/drop, role/permission and data editors, every migration-CLI flag, and launch/click smoke remain open (medium; `docs/known-limitations.md:92-93,140-141`).
- [ ] Observability breadth — broaden remaining `pg_stat_*` operator views, lock/io wait-event population, deeper lock/query timing precision, production dashboards; add `pg_stat_statements`; harden VACUUM/autovacuum scheduling + freeze policy + bloat metrics with docs (medium; ).

## Performance & Benchmark release gates

- [ ] Record a durable bulk-INSERT 1M-row measurement — committed `insert_throughput_1m-ultrasql.json` is `not_available` ("wal buffer full: 8386518 of 8388608 bytes"); the per-record backpressure + over-capacity admission fix is in code but the `--data-dir` scale sweep must be re-run to record a measurement (currently 23, not 24, comparable measured rows) (critical; `benchmarks/results/latest/scale-sweep/raw/insert_throughput_1m-ultrasql.json`).
- [ ] Fresh data-dir scale-sweep certification on the release commit — aggregate benchmark gate is not_ready: "expected release commit is required", "ultrasql_storage_mode expected data-dir, got None", many raw artifacts fail strict schema ("schema_version must be 1", "status must be measured or not_available"). The README sweep is the stale same-host fastest-table run pinned to 77a92d7c, not a fresh WAL-backed data-dir cert (high; `benchmarks/results/latest/release_gate_status.json`, `docs/known-limitations.md:113-115`).
- [ ] Close per-row scale-sweep losses (or formally accept) — UltraSQL leads 17/24; loses select_scan_100k→ClickHouse, insert_throughput_10k + mixed_oltp_pgbench_like→PostgreSQL/SQLite (F_FULLFSYNC OLTP cost, accepted honest losses), update_throughput_1m + delete_throughput_100k→DuckDB, delete_throughput_1m→ClickHouse. No OLTP/write leadership claim until a committed sweep shows lowest median (medium; `docs/documentation-status-audit.md:47-53`).
- [ ] TPC-B / TPC-C / Sysbench / ClickBench gates passing — tpcb/tpcc = target_not_met, sysbench = failed/target_not_met, clickbench = partial/missing_required_engine_results. No throughput-leadership or p99<5ms claim until these pass same-host PostgreSQL 17 (+ ClickHouse/Firebolt where applicable). TPC-H SF1/SF10 and pgvector pass (high; `benchmarks/results/latest/{tpcb,tpcc,sysbench,clickbench}_certification.json`).
- [ ] Firebolt sparse primary-index pruning gate — pass `target_ratio_ultrasql_vs_firebolt <= 1.0` and require `Firebolt primary-index pruning evidence`. Today the honest state is `local Firebolt Core smoke measured`, but `Firebolt is not_available` when Core EXPLAIN does not expose pruning; Firebolt comparisons use local Firebolt Core only (medium; `benchmarks/results/latest/clickbench_certification.json`, `docs/known-limitations.md`).
- [ ] SIFT1M ANN recall/latency gate — no committed SIFT1M server-wire artifact giving absolute recall@10 + latency at 1M scale (build ~45 min extrapolated, not pgvector-competitive "minutes"); the in-memory fallback HnswIndex is still single-layer O(N) and not persisted (high; `docs/known-limitations.md:56-61`).
- [ ] Long fuzz: one clean week (parser/protocol/WAL-decoder/planner) — current evidence is short-window nightly/manual runs (`-max_total_time=900`), not a clean continuous week; corrupt-WAL replay torn-write handling is unit-tested, not fuzz-hardened (medium; `docs/release-checklist.md:106-108`).
- [ ] Aggregate release gate closes (`release_gate_status.json = ready`) — `validate-release-evidence.py --strict` must pass with committed evidence for the release commit across all sub-gates; no stronger-than-narrow claim until then (critical; `benchmarks/results/latest/release_gate_status.json`, `docs/release-checklist.md:150`).

## Operations & Soak gates

- [ ] Three independent 30-day operator soaks (0 of 3) — `operator_soak_status.json` not_ready: 0 valid release reports, 0 independent operators, 0 valid release commits. Hard v1.0 gate via `.github/workflows/operator-soak.yml`. The final release needs the operator soak reports plus the `latest green CI workflow run id`, the `release workflow run id`, and the GitHub release notes recorded in the release checklist (critical; `benchmarks/results/latest/operator_soak_status.json`, `DONE.md`).
- [ ] Two independent external security + correctness audits (0 of 2) — `external_audit_status.json` not_ready: 0 valid reports, 0 independent auditors, missing both required audit types; valid reports must cover the expected release commit and pass `scripts/validate-external-audits.py` --strict (critical; `benchmarks/results/latest/external_audit_status.json`).
- [ ] Incident drills in production mode: backup_restore, wal_recovery, disk_full (0 of 3) — `incident_drill_status.json` not_ready: 0 valid drill reports, all three required types missing, 0 valid release commits. Needs `mode:production` reports with RTO/RPO, postmortem, monitoring-alerted, zero unresolved sev0/sev1, generated/validated via `scripts/run-incident-drills.py` + `scripts/validate-incident-drills.py` --strict (critical; `benchmarks/results/latest/incident_drill_status.json`, `docs/incident-drills.md:6-35`).
- [ ] PITR production drill end-to-end — PITR primitives exist (up_to_lsn/xid/time, recovery.targets, restore_command) but no committed production drill proving base-backup + WAL-archive + restore_command + recovery-target replay on a real workload (high; `crates/ultrasql-wal/src/recovery.rs:37-88`, `crates/ultrasql-server/src/snapshots.rs:231-419`).
- [ ] Re-run chaos/crash/disk-full recovery on each release candidate — chaos manifest is `measured`/passed for random-kill, WAL-truncation, per-process disk-full, but P0 requires re-running on release candidates; current disk-full is a per-process `ulimit -f` cap, not a true host-level ENOSPC across all fsync sites (medium; `benchmarks/results/latest/chaos_recovery_manifest.json`).
- [ ] Driver compatibility certification on the release commit (20 required drivers) — `release_gate_status.json` blocks on driver_compatibility ("report missing: target/driver-certification.json") with all 20 required drivers missing. Keep certification green for libpq, psycopg2, psycopg3, SQLAlchemy, Django ORM, Rails ActiveRecord, Hibernate ORM, GORM, Prisma, Diesel, node-postgres, pgx, lib/pq, JDBC, Npgsql, Flyway, Liquibase, Alembic, the stock psql meta-commands (`\d`, `\dt`, `\di`, `\df`, `\dv`, `\du`, `\l`, `\dn`), and GUI introspection probes (pgAdmin, DBeaver, DataGrip). Generate a fresh strict cert via `scripts/run-driver-release-evidence.py` + `scripts/validate-driver-compatibility.py --strict`; the `driver_compatibility_status.json` artifact records `required_driver_count`, `passing_required_driver_count`, and `missing_required_drivers` for the release commit (high; `benchmarks/results/latest/{release_gate,driver_compatibility}_status.json`).

## Licensing & Release plumbing

- [ ] Provision release-signing / publication material — NPM_TOKEN, HOMEBREW_TAP_TOKEN, AUR_SSH_PRIVATE_KEY, CHOCOLATEY_API_KEY, package signing, Windows code-signing. (NOTE: the older "currently unlicensed / unsuitable for production" gap is STALE — repo is dual-licensed Apache-2.0 OR MIT with LICENSE-APACHE/LICENSE-MIT/NOTICE present; only signing/publication remains.) (low; `Cargo.toml:34`, `DONE.md`).
- [ ] Promote package publication evidence from the release workflow — `docs.ultrasql.org`, `ghcr.io/mauneven/ultrasql` (a clean GHCR platform list), `packages/npm` + `npm publish`, the Windows setup EXE, Chocolatey, AUR (`yay -S ultrasql-bin`), the Homebrew tap, plus Debian/RPM. Open until each channel publishes from a tagged release (low; `.github/workflows/release.yml`, `docs/packaging.md`).

## AI / Strategic surface

- [ ] Persistent, multi-layer vector index from the server wire path — the in-memory fallback `HnswIndex` is single-layer O(N) and not persisted; HNSW build-scaling and hierarchical layers are DONE in code but the SIFT1M 1M-scale server-wire artifact (recall@10 + latency, competitive build time) is the open release-blocking deliverable for any published 1M-scale ANN claim (high; `DONE.md`, `docs/known-limitations.md:56-61`).
- [ ] Production ANN certification for Page-backed HNSW and Page-backed IVFFlat — both need large-scale recovery certification, page-level torn-write handling, deeper VACUUM/rebuild stress, `CREATE INDEX CONCURRENTLY`, filtered-query fallback policy, larger recall/latency artifacts, and WAL replay fuzz/property tests before any production ANN claim (high; `DONE.md`, `docs/known-limitations.md:56-61`).
- [ ] Broaden AI gauntlet measured artifacts into competitor comparisons — keep the AI gauntlet measured artifacts expanding across exact top-k, HNSW ANN recall/latency, hybrid search latency, filtered vector search, RAG retrieval quality, memory per million vectors, ingestion throughput, and cold-start index load, then add same-host DuckDB/ClickHouse/PostgreSQL+pgvector legs with answer/recall gates before publishing (medium; `docs/vector-benchmarks.md`).

---

## Release verdict (do not remove)

UltraSQL is **not production ready for v1.0**. Allowed claims are narrow (fastest measured engine on
17 of 24 workloads on the pinned same-host Apple M4 run). "Production ready", "best in every aspect",
and "fastest writes / OLTP leadership" are forbidden until the gates above close with committed
evidence for the release commit, validated via `scripts/validate-release-evidence.py --strict`.
