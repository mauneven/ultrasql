> **Design for TODO.md item #5** (transactional DDL — allowing `CREATE` / `ALTER` /
> `DROP` inside an explicit `BEGIN…COMMIT` block with correct `ROLLBACK`). **Increment B
> — the rejection SQLSTATE — landed this session**: DDL inside an explicit transaction now
> returns SQLSTATE `0A000` (`feature_not_supported`) with an autocommit `HINT` instead of a
> generic "unsupported" string, so ORM / migration tooling gets a deterministic, classifiable
> failure. This does **not** make transactional DDL work; it only makes the rejection honest.
> The real feature is a **SAVEPOINT-class dedicated effort** gated on the §6 adversarial
> battery. **Do NOT lift the `is_ddl && InTransaction` guard** (`query.rs:302`, `ext.rs:464`)
> without the per-transaction catalog-overlay / versioning work: the catalog is mutated
> globally-in-place and committed durably mid-statement under a private `ddl_txn` with no
> per-transaction overlay, so a rolled-back transaction's schema change cannot be undone —
> textbook silent schema corruption, the exact class that got SAVEPOINT reverted once. This
> document is code-verified against `main` (every anchor below was re-read on disk before being
> written as fact).

---

# UltraSQL Transactional DDL — Implementation-Ready Design

## Verified current-state map (live `main`, re-read on disk, not from memos)

I re-read every anchor before designing. Confirmed on `main`:

### The two (and only) DDL-in-transaction rejection sites

Both gates key on the **same** condition — an explicit transaction is open
(`TxnState::InTransaction(_)`). Autocommit (`TxnState::Idle`) is the only state that runs DDL.

- **Simple-Query path** — `crates/ultrasql-server/src/session/execute/query.rs:302`:

  ```rust
  if is_ddl && matches!(self.txn_state, TxnState::InTransaction(_)) {
      return Err(self.fail_if_in_transaction(ServerError::DdlInTransaction));
  }
  ```

  `is_ddl` is a 30-arm `matches!` over every DDL `LogicalPlan` variant
  (`query.rs:268-301`).

- **Extended-Query path** — `crates/ultrasql-server/src/session/ext.rs:464`: same condition,
  gated by `Self::is_ddl_plan(plan)` (the identical list, `query.rs:458-492`) — and routed
  to the same `ServerError::DdlInTransaction`.

`fail_if_in_transaction` (`effects.rs:214`) transitions the block to `Failed`, so subsequent
statements get SQLSTATE `25P02` (`in_failed_sql_transaction`) until `COMMIT`/`ROLLBACK`.

**Increment B (landed):** `ServerError::DdlInTransaction` (`crates/ultrasql-server/src/error.rs`)
maps to SQLSTATE `0A000` (`feature_not_supported`) in `ServerError::sqlstate()` and carries a
message naming the construct plus an embedded `HINT:` telling the caller to run the statement
in autocommit. `0A000` is the PG-faithful code: PostgreSQL *implements* transactional DDL, so
"UltraSQL has not implemented it yet" is `feature_not_supported`, not `25001`. The codebase
already maps `ServerError::Unsupported` to `0A000` (`error.rs`), and existing round-trip tests
already expect `0A000` for DDL-in-txn (`alter_table_round_trip.rs:161`, `constraint_round_trip.rs:1204`,
`view_round_trip.rs`), so the dedicated variant is internally consistent. The hint travels in
the message text because the server's error-reporting path is uniform `(message, sqlstate)` with
no separate wire `HINT` field; threading a third field through every `send_error`/`encode_error_response`
call site would be disproportionate to the win.

### The catalog is mutated globally-in-place, with no MVCC and no per-txn overlay

`PersistentCatalog` (`crates/ultrasql-catalog/src/persistent/core.rs:25-66`, `mod.rs:105-138`)
holds `DashMap`s (`pg_class`, `pg_attribute`, `tables_by_name`/`tables_by_oid`, indexes, …)
plus a single `snapshot: ArcSwap<CatalogSnapshot>` read cache and one process-global
`write_lock: Mutex<()>`. Reads are wait-free: `snapshot.load_full()` (`core.rs:64-66`); the
session obtains it per-statement via `catalog_snapshot()`. **`CatalogSnapshot` carries no
xmin/xmax** — it is a flat `HashMap<String, TableEntry>`; there is no transaction id, no command
id, and no session-scoped overlay on the DashMaps.

`create_table` / `drop_table` / `alter_*` (`crates/ultrasql-catalog/src/persistent/traits_impl.rs:110-150`,
`:222-367`) take `write_lock`, mutate the DashMaps **immediately**, and call `rebuild_snapshot()`,
whose last line is `self.snapshot.store(snap)` (`mutations.rs:113`) — a single atomic swap that
makes the change visible to **every** session the instant the handler runs. There is no
copy-on-write per-txn version and nothing to roll back to.

### Durable catalog rows are committed mid-statement under a private, self-committing `ddl_txn`

Each DDL handler opens its **own** short-lived transaction and commits it durably **mid-statement**,
under a different xid than the user's `BEGIN`. For `CREATE TABLE`
(`crates/ultrasql-server/src/session/ddl/create_table.rs:342-404`):

```rust
let ddl_txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);   // :342
let ddl_xid = ddl_txn.xid;
// … persist_table_rows_with_defaults / persist_index_rows / persist_constraint_row,
//    each heap.insert stamped with ddl_xid …
self.state.commit_transaction(ddl_txn, true, "CREATE TABLE catalog-write transaction")?;  // :404
```

`commit_transaction(.., true, ..)` (`server_lifecycle.rs:544-563`) appends a Commit WAL record
and `wait_for_wal_durable` — fsync-durable **before the statement returns**. The durable catalog
rows *are* MVCC-stamped (`persist_tables.rs:374`, `InsertOptions{ xmin, command_id }`; DROP is an
append-only `RelKind::Dropped` tombstone, `persist_tables.rs:236`), but they ride the **`ddl_xid`,
not the user xid**. `CREATE INDEX` is worse: it commits **twice** — a build txn
(`create_index_build_btree.rs:175`) and a catalog txn (`:218`) — so there is no single atomic unit
to roll back.

### `execute_commit` / `execute_rollback` never touch the catalog

`execute_rollback` (`crates/ultrasql-server/src/session/txn.rs:474-509`) does exactly three things:
`heap.rollback_in_place_updates(xid)`, `abort_transaction(txn, false, …)`, and
`clear_pending_dml_effects()` — **none** reference `persistent_catalog`. `execute_commit`
(`txn.rs:365-471`) likewise never touches it. The shared `commit_transaction` /
`abort_transaction` helpers (`server_lifecycle.rs:544-581`) only flip CLOG status and append WAL
markers. `clear_pending_dml_effects` (`effects.rs`) clears only DML staging maps. **Conclusion:
a user `ROLLBACK` cannot undo any catalog change today** — the change was never owned by the
user's transaction.

### Side effects: what is cheap/MVCC-revertible vs. the non-MVCC hazards

| Side effect | Revertibility today | Anchor |
| --- | --- | --- |
| `CREATE TABLE` segment file | **Cheap** — no file until first insert (lazy `allocate_block`) | `segment.rs` lazy open |
| `DROP TABLE`/`DROP INDEX` file unlink | **Cheap** — never unlinks ("segment manager has not yet landed") | `drop.rs:162-172` |
| `TRUNCATE`, `ALTER ADD/DROP COLUMN` rewrite | **MVCC** — `delete`/`update`, reverted by `rollback_in_place_updates` | `alter.rs`, heap MVCC |
| `CREATE INDEX` btree build | **Hazard** — two separate commits (build + catalog); must collapse onto one user xid | `create_index_build_btree.rs:175,:218` |
| `next_oid` allocation | **Cosmetic leak** — `AtomicU32::fetch_add`, non-transactional; PG tolerates OID leaks | `core.rs:55-56` |
| roles / privileges / RLS / sequence-owner / operator | **Non-MVCC sidecars** — escaped-text files in `metadata_io.rs`, rebuilt at startup; cannot be transactionally rolled back | `metadata_io.rs`, `server_meta_role_priv.rs` |

### Locking: DDL takes no relation lock

The `LockMode`/`AccessExclusive` conflict matrix exists (`crates/ultrasql-txn/src/lock.rs:43-106`)
but is **unused by DDL** — there are zero acquisition sites in the DDL paths. Concurrent DML can
therefore observe a half-applied schema.

---

## 2. Why there is no safe increment that lifts the gate

Deleting the `is_ddl && InTransaction` guard is exactly the silent-schema-corruption scenario.
A rolled-back transaction whose `CREATE`/`DROP`/`ALTER` ran would leave:

1. the **durably-committed catalog rows** in place (their `ddl_txn` already committed, not the
   user xid), and
2. the **globally-swapped in-memory `ArcSwap` snapshot** in place, visible to **every** session.

It also violates PostgreSQL isolation *before* commit: the in-memory mutation is published to
other concurrent sessions immediately (a dirty schema read). This is the precise class that got
SAVEPOINT reverted once. **The genuinely useful feature — DDL visible-to-self, invisible-to-others,
reverted on `ROLLBACK` — requires the catalog-overlay/versioning prerequisite and is a dedicated
multi-day effort.**

---

## 3. THE LATENT BOOTSTRAP CORRUPTION VECTOR (Increment A)

> This is an **independent correctness bug that exists today even for autocommit DDL** — it is
> not specific to transactional DDL — and it is the **recommended FIRST step** of the dedicated
> effort because the overlay feature is unsound without it.

### The bug

`bootstrap_from_heap` rebuilds the catalog on restart with a **raw, non-visibility scan**:

```rust
// crates/ultrasql-catalog/src/persistent/bootstrap_heap.rs:276
let class_scan = heap.scan(pg_class_rel, class_blocks);
// … keep latest_class_by_oid per OID, regardless of commit fate …
```

`heap.scan` (`crates/ultrasql-storage/src/heap/update.rs:519`) yields every normal slot and
applies **no CLOG/commit-status filter** — its own doc comment says "a future revision will accept
a snapshot + oracle". The catalog heap is append-only, so bootstrap keeps the newest `pg_class`
row per OID **regardless of whether the writing xid ever committed**.

**The corruption:** an autocommit DDL that crashes *between* its catalog rows becoming durable
(`persist_table_rows_with_defaults` → `heap.insert`, WAL-logged) and its `ddl_txn`'s commit marker
becoming durable (`commit_transaction(.., true, ..)`) leaves uncommitted catalog rows on disk with
no commit record. On restart, the raw scan **resurrects the uncommitted table as live schema**.
This is adversarial test #4 below — the one the current bootstrap fails today.

### Why the fix is BLOCKED on recovery re-ordering

The naive fix — switch `bootstrap_heap.rs` scans to the visibility-filtered `scan_visible`
(`heap/update.rs:538`, backed by an `XidStatusOracle`) — **cannot be applied as-is**, because of
the recovery step ordering in `crates/ultrasql-server/src/server_wal_recovery.rs`:

| Step | Recovery action | Anchor |
| --- | --- | --- |
| 2 | `persistent_catalog.bootstrap_from_heap(heap)` | `server_wal_recovery.rs:421` |
| 3 | `txn_manager = TransactionManager::new_with_ssi(ssi)` (bare) | `:428` |
| 4–5 | `import_clog` (if snapshot present) / `recover_commit_status_from_wal()` (full CLOG rebuild) | `:519`, `:536` |

Bootstrap runs at step 2, **before** the CLOG is rebuilt at steps 4–5. Forcing a visibility
filter against the empty CLOG at step 2 would resolve **every** catalog xid as not-committed and
hide **all** committed catalog rows — losing the entire user schema on every restart.

### The prerequisite fix (the recommended first step)

1. **Re-order recovery so commit status is known before bootstrap.** Extract
   `recover_commit_status_from_wal` (`server_wal_recovery.rs:536`) into a step that runs against
   a bare `txn_manager` (the `import_clog` snapshot path at `:519` plus the WAL commit-status
   rebuild) **before** `bootstrap_from_heap` at `:421`. The WAL data already exists at that point
   — only the call order changes.
2. **Define a bootstrap snapshot** (an all-committed-up-to-`next_xid` snapshot, or the recovered
   CLOG itself as the `XidStatusOracle`) for the catalog scan to consult.
3. **Switch the catalog scans** in `bootstrap_heap.rs` (the `pg_class` scan at `:276` and the
   ~10 sibling scan sites for `pg_attribute`/`pg_constraint`/`pg_index`/`pg_sequence`/…) from
   `heap.scan` to `scan_visible(.., &bootstrap_snapshot, &oracle)`.

After this, an aborted or crashed DDL's catalog rows are correctly hidden on restart — closing the
autocommit-crash vector **and** unblocking the durable side of transactional DDL (item §4.4 below).
Land it behind test-#4-style crash-recovery assertions. **Effort: ~1–2 focused days**,
self-contained in `ultrasql-catalog` bootstrap + an `XidStatusOracle` wire-up.

---

## 4. The dedicated-effort design (the SAVEPOINT-class project)

Flip the model from **immediate-global-publish** to **stage-until-commit**: the user's transaction
must **own** both the in-memory and the durable catalog changes.

### 4.1 Per-transaction catalog overlay (the core build)

Add a session/txn-local pending-DDL layer over the committed `ArcSwap<CatalogSnapshot>`. Catalog
reads **for the issuing session** resolve **overlay-first, then committed snapshot**; reads for
**other** sessions see **only the committed snapshot**. On `COMMIT`, atomically merge the overlay
into the global snapshot (one `rebuild_snapshot()` / `snapshot.store`); on `ROLLBACK`, **discard**
the overlay (no global mutation ever happened). Concretely: introduce `MutableCatalog` methods that
take a txn context and route to the overlay instead of the DashMaps when a user txn is open
(replacing the immediate `traits_impl.rs:110-150` mutation for the in-txn path). This is the
in-memory half of self-yes / others-no visibility.

### 4.2 Retarget durable catalog rows onto the USER xid; stop self-committing

Stop opening a private `ddl_txn` per handler. Stamp `pg_class`/`pg_attribute`/`pg_constraint`/
`pg_sequence` heap rows with the **outer** transaction's xid and command_id, and do **not** commit
mid-statement — let the user's `COMMIT`/`ROLLBACK` decide. The append-only catalog heap
(`persist_tables.rs`) plus DROP-as-tombstone already make catalog rows MVCC-stamped, so this is
wiring, not new storage. Collapse `CREATE INDEX`'s two commits
(`create_index_build_btree.rs:175,:218`) onto the single user xid as one unit.

### 4.3 Visibility (self-yes / others-no)

The **durable** side is handled by MVCC once rows ride the user xid (runtime reads honor
xmin/xmax via the snapshot). The **in-memory** side is handled by the overlay (§4.1). Both must
agree.

### 4.4 Durability + recovery

`bootstrap_from_heap` **must** become commit-aware (the §3 Increment A prerequisite). After
switching to a visibility-filtered scan with an `XidStatusOracle`, an aborted/crashed DDL's
catalog rows are correctly hidden on restart. Without this, §4.2 alone would resurrect aborted
DDL on crash.

### 4.5 Publish / discard drive points

Hook `execute_commit` (`txn.rs:365-471`) to (a) durably commit the user xid — already done for
DML — and (b) merge the catalog overlay into the global snapshot. Hook `execute_rollback`
(`txn.rs:474-509`) to discard the overlay. Recovery's CLOG-driven visibility then makes the heap
side consistent automatically (an aborted user xid → its catalog rows are invisible).

### 4.6 Locking

Transactional DDL must take **`AccessExclusive`** on the target relation
(`lock.rs:43-106` matrix, currently unused by DDL) so concurrent DML/DDL cannot observe a
half-applied schema. This is required for **isolation correctness**, not just rollback.

### 4.7 Scope: milestone 1 = TABLE / INDEX / CONSTRAINT / TYPE DDL only

The non-MVCC sidecars in `metadata_io.rs` (roles/privileges/default-privileges/RLS/sequence-owner/
operator — escaped-text files, `TODO.md:129`) cannot be transactionally rolled back. **Keep
`GRANT`/`REVOKE`/`CREATE ROLE`/`ALTER ROLE`/`COMMENT`/`CHECKPOINT`/`EXPORT`/`IMPORT` rejected-in-txn**
(SQLSTATE `0A000` per Increment B) until those sidecars become typed MVCC catalog rows. This split
is natural and lets milestone 1 be coherent.

---

## 5. Blast radius

Cross-crate, invariant-bearing — comparable to the SAVEPOINT effort.

- **`ultrasql-catalog`**: `traits_impl.rs` (create/drop/alter route to a txn overlay instead of
  immediate DashMap+rebuild), `mutations.rs` (`rebuild_snapshot`/`store` becomes the COMMIT-time
  merge), `core.rs` (`snapshot()`/lookup resolve overlay-first for the issuing session),
  `bootstrap_heap.rs` (raw `heap.scan` → `scan_visible` + `XidStatusOracle`, `:276` and the
  ~10 sibling scan sites), `persist_tables.rs` (rows stamped with caller-supplied xid — mostly
  already parameterized).
- **`ultrasql-server`**: `session/ddl/*` (`create_table.rs:342-404`, `drop.rs`, `alter.rs`,
  `create_index_build_btree.rs:175/218` — stop the private self-committing `ddl_txn`; ride the
  user xid; collapse CREATE INDEX), `session/txn.rs` (`execute_commit:365-471` merges overlay;
  `execute_rollback:474-509` discards overlay), `session/execute/query.rs:302` + `session/ext.rs:464`
  (relax the gate for the scoped subset **only**), `effects.rs` (pending-DDL effects alongside
  pending DML), `server_wal_recovery.rs` (recovery re-order, §3), `server_lifecycle.rs` helpers.
- **`ultrasql-txn`**: `lock.rs` `AccessExclusive` acquisition newly wired into DDL paths
  (currently zero acquisition sites).
- **`ultrasql-storage` / `ultrasql-wal`**: recovery path must agree with commit-aware catalog
  bootstrap; the WAL applier already rebuilds CLOG, so the data exists, but the catalog read path
  on restart changes.

Every `Catalog`/`MutableCatalog` read call site across planner, binder, and executor is potentially
affected because catalog reads must become snapshot/overlay-aware for the issuing session.
**Explicitly out of scope for milestone 1** (keep rejected-in-txn):
`GRANT`/`REVOKE`/`CREATE ROLE`/`ALTER ROLE`/`COMMENT`/`CHECKPOINT`/`EXPORT`/`IMPORT`.

---

## 6. Adversarial battery (the hard gate)

Mirrors the SAVEPOINT precedent — it was reverted once for corruption and re-landed only behind an
adversarial battery. **Each test must pass for BOTH the simple-query and the extended/portal path.**

1. **ROLLBACK undoes the DDL — in-memory.** `BEGIN; CREATE TABLE t; …; ROLLBACK;` then on the
   **same** session `\d t` / `lookup_table(t)` returns not-found and the global `ArcSwap` snapshot
   has no entry for `t`. Symmetric: `BEGIN; DROP TABLE existing; ROLLBACK;` → `existing` still
   present and queryable.
2. **ROLLBACK undoes the DDL — second connection.** While txn A holds
   `BEGIN; CREATE TABLE t` (uncommitted), a separate connection B must **not** see `t`
   (others-no isolation, the dirty-schema-read case). After A `ROLLBACK`, B still does not see `t`;
   after A `COMMIT` instead, B sees `t`.
3. **Self-visible before commit.** `BEGIN; CREATE TABLE t; INSERT INTO t …; SELECT FROM t` — all
   succeed within the same txn before `COMMIT` (self-yes). DROP variant:
   `BEGIN; DROP TABLE existing; SELECT FROM existing` → error within the same txn.
4. **Crash mid-transaction-DDL recovers clean.** Kill the process after the in-txn `CREATE`/`DROP`/
   `ALTER` ran but **before** `COMMIT` (catalog heap rows may be durably on disk; the user xid has
   no commit record). On restart, `bootstrap_from_heap` must **not** resurrect the table. **This is
   the test the current raw-scan bootstrap (`bootstrap_heap.rs:276`) fails today — the reason
   §3 / Increment A is a prerequisite.** Symmetric: crash *after* the COMMIT WAL record is durable
   → the table **is** present on restart.
5. **Concurrent DML/DDL isolation.** Session A `BEGIN; ALTER TABLE t ADD COLUMN …` (holding
   `AccessExclusive`); session B's concurrent `INSERT`/`SELECT` on `t` must block or serialize,
   never observe a half-applied schema (torn column set). Verify no `AccessExclusive`-vs-`AccessShare`
   conflict is silently skipped.
6. **No orphaned files / no leaked live state.** After `ROLLBACK` of `CREATE INDEX`, no btree
   pages are reachable as a live index (collapsed-commit unit fully reverted); after `ROLLBACK` of
   `CREATE TABLE`, no segment file under `base/<oid>/` (lazy creation means none should exist —
   assert it); after `ROLLBACK` of `DROP TABLE`, the relation's segment files are intact. Assert
   the OID leak is bounded/cosmetic only (PG-tolerable).
7. **Mixed DDL+DML txn.** `BEGIN; CREATE TABLE t; INSERT INTO t; CREATE INDEX ix ON t; COMMIT` →
   all present and consistent on a fresh connection **and** after crash-restart. Same sequence with
   `ROLLBACK` → none present, before and after restart.
8. **Regression guard.** The out-of-scope subset
   (`GRANT`/`CREATE ROLE`/`COMMENT`/`CHECKPOINT`/`EXPORT`/`IMPORT`) must **still** be rejected-in-txn
   with SQLSTATE `0A000`, transitioning to `Failed` (`25P02`) — so the relaxed gate did not
   accidentally open the non-MVCC-sidecar DDL.

**Gate rule: no push if any rollback / crash / isolation case (1–5, 7) is red.**

---

## 7. Effort + risk

- **Increment B (rejection SQLSTATE) — landed this session.** Hours. No semantics change; the gate
  still rejects DDL-in-txn, but now with a deterministic `0A000` + autocommit `HINT`. See
  `crates/ultrasql-server/src/error.rs` (`DdlInTransaction`), `query.rs:302`, `ext.rs:464`, and
  the `ddl_in_explicit_transaction_is_feature_not_supported_with_hint` test in `txn_round_trip.rs`.
- **Increment A (recovery re-order + commit-aware bootstrap) — recommended FIRST step.** ~1–2
  focused days. A **prerequisite** for the feature **and** an **independent corruption fix** for
  the autocommit-crash vector (§3). Self-contained in `ultrasql-catalog` bootstrap + an
  `XidStatusOracle` wire-up + the `server_wal_recovery.rs` step re-order.
- **The overlay feature — multi-day, SAVEPOINT-class.** Per-transaction catalog overlay (§4.1) +
  durable rows retargeted onto the user xid (§4.2) + commit-merge / rollback-discard hooks (§4.5) +
  `AccessExclusive` locking (§4.6), scoped to TABLE/INDEX/CONSTRAINT/TYPE DDL (§4.7). Gated behind
  the full §6 battery, exactly as SAVEPOINT was.

> **Do NOT ship Increment C** (treat `BEGIN; <one DDL>; COMMIT` as autocommit-equivalent). It
> covers only the trivial single-statement migration; real ORM migrations (Rails/Django/Alembic)
> bundle multiple DDLs + DML, so it does not unblock them — it gives false confidence.
