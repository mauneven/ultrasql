> **Design for TODO.md item #1** (correct SAVEPOINT subtransaction visibility). A first
> implementation was reverted for data corruption + B-tree incoherence on `ROLLBACK TO`
> (see the SAVEPOINT entry in `docs/known-limitations.md`). This design resolves the crux —
> the per-subxid index-undo problem that sank the first attempt — by adopting PostgreSQL's
> lossy-index + heap-recheck model (the index read paths already recheck heap visibility,
> so deleted entries can simply be left for VACUUM). It is code-verified against `main`
> post-revert. The feature is deeply coupled (subxid stamping ↔ own-write visibility ↔
> commit/abort fold ↔ index model) with **no safe partial increment** — a stamping fix
> alone trades one correctness bug for another. It is a dedicated multi-day effort, to be
> implemented with the §5 adversarial battery as a **hard gate before any push**.

---

# UltraSQL SAVEPOINT Subtransaction Visibility — Implementation-Ready Design

## Verification summary (live `main`, verified against disk, not the audit memos)

I re-read every anchor before designing. Confirmed on `main` (post-revert at `99c13ca0`):

- `crates/ultrasql-mvcc/src/snapshot.rs:95` — `is_current_xid` is parent-only (`xid == self.current_xid`). No subxid sets. **Reverted.**
- `crates/ultrasql-mvcc/src/visibility.rs:142` — `is_visible_ext(...,&NoSubxacts)` exists; the `SubxactOracle` trait + `InfoMask::SUBXACT` branch survived but is **dead** (no caller passes a real oracle; the on-disk SUBXACT bit is never set by any write path I can find). The `own_subxid_rolled_back(xmax)` revert checks are **absent**.
- `crates/ultrasql-server/src/session/execute/bound_plan.rs:482` — fast INSERT stamps `xmin: txn.xid` (**PARENT — BUG, live**).
- `crates/ultrasql-server/src/session/execute/mvcc_maint.rs:163` — explicit-txn fused int32-pair DELETE stamps `xid: txn.xid` (**PARENT — BUG, live, the corruption root**).
- `crates/ultrasql-txn/src/manager.rs:621` — `release_savepoint` flips the subxid CLOG to **Committed** (**the cross-txn leak bug, live**).
- `crates/ultrasql-txn/src/manager.rs:589` — `rollback_to_savepoint` is CLOG-only (`InProgress → Aborted` + `record_rolled_back`). No subxid_parent oracle, no atomic family fold, no merged_up.
- `crates/ultrasql-server/src/session/txn.rs:556` — `execute_rollback_to_savepoint` does **not** call `rollback_in_place_updates`, `rollback_delete_stamps`, or any column-cache invalidation. It just flips state back to `InTransaction`.
- `crates/ultrasql-server/src/pipeline/index_scan/btree_probe.rs:316,326,336` — index read path uses plain `is_visible` (heap recheck — the load-bearing PG property is **already present**) and maps `VisiblePreImage → Ok(None)`. Same at `late_materialize.rs:350,360`.
- `crates/ultrasql-executor/src/modify/index_ops.rs:347` — `apply_delete_index_changes` calls `delete_key → tree.delete_logged` (physical leaf removal, no undo).
- `crates/ultrasql-executor/src/modify/index_maintainer.rs:64` — `contains_key/lookup_tid` is **index-only**, no heap recheck (the uniqueness gap).
- `crates/ultrasql-storage/src/btree/vacuum.rs:47` — `vacuum(is_dead)` physical reclamation exists.
- `crates/ultrasql-storage/src/heap/delete.rs:619` — `rollback_delete_stamps(xid)` exists, **never called** by ROLLBACK TO.
- `crates/ultrasql-server/src/pipeline/modify/{insert,delete,update,merge}.rs` — all stamp `ctx.xid`; `ctx.xid = txn.current_xid()` (`txn_exec.rs:204,422`). **All pipeline DML correct.**
- `alter.rs` DDL rewrite sites stamp `txn.xid` (secondary scope).
- Column-cache coherence gate (`b121812a`, already merged) gates publish+read on a quiescent, writer-visible snapshot.

The single most important finding, verified: **both index read paths already recheck heap visibility.** This is exactly PG's "lossy index + heap recheck" model and it is the entire justification for the index-model decision below.

---

## 1. THE INDEX MODEL DECISION — Adopt PostgreSQL's no-index-undo model (Option A)

**Decision: stop physically removing B-tree leaf entries on MVCC DELETE and on key-changing UPDATE. Let the heap-visibility recheck filter stale entries, and let VACUUM reclaim them. Do NOT build a per-subxid index-undo log.**

### Justification from the investigation evidence

1. **The read side is already authoritative-heap.** `fetch_visible_index_payload` (btree_probe.rs:316–342) and `LateMaterializeScan::fetch_visible_payload` (late_materialize.rs:340–367) both `heap.fetch(tid)` then `is_visible(&tuple.header, …)` and drop the candidate on `Invisible`. The index entry's presence is *necessary but never sufficient*. This is the precondition PG's no-undo model requires, and it is already met. The doc contract at btree_probe.rs:18–33 ("the user observes the same row set whether or not the index is consulted") is the invariant we are preserving.

2. **ROLLBACK TO becomes index-coherent for free.** With no physical removal, there is nothing to restore. A rolled-back DELETE leaves its (still-present) index entry pointing at a heap tuple whose `xmax` is the aborted subxid; the heap recheck sees the delete "did not count" and the row reappears through *both* seq scan and index scan — automatically, identically to how the heap self-heals. This is the exact PG mechanism (PG REFERENCE §3).

3. **VACUUM reclamation already exists.** `BTree::vacuum(is_dead)` (vacuum.rs:30–50) physically removes leaf entries whose heap tuple is dead per a caller predicate. No new reclamation machinery is needed; we only must ensure the VACUUM scheduler drives index leaves with a correct `is_dead`.

4. **Option B (per-subxid index undo) is rejected** precisely because it re-introduces the class of subtlety that sank the first attempt (risk memo): the reverse-insert must reproduce Lehman-Yao leaf placement and duplicate ordering, interleave with concurrent splits and with a VACUUM that may already have reclaimed the slot, and be WAL-logged for crash recovery of partial subxact rollback. Large blast radius, fragile, and **unnecessary** given the read side already rechecks. Its only advantage (lean leaves) is exactly PG's known, accepted bloat cost.

5. **Option C (hybrid)** collapses to "do A now, defer any undo log until profiling justifies it." That is the right posture, but the in-memory removal-suppression set is not needed for correctness, so the concrete plan is Option A.

### Exact code changes

**(A1) Stop physical index removal on MVCC delete.** In `apply_delete_index_changes` (index_ops.rs:339–358), do **not** call `delete_key`. The heap tuple's `xmax` stamp is the sole authority; the leaf entry stays for VACUUM.

```rust
// index_ops.rs:339  apply_delete_index_changes — REMOVE the delete_key call.
// Old leaf entry is retained; VACUUM reclaims it once the tuple is dead to all snapshots.
pub(crate) fn apply_delete_index_changes(&mut self, _changes: &[DeleteIndexChange])
    -> Result<(), ExecError> { Ok(()) }
```
Keep `extract_delete_tids_and_index_changes` building `DeleteIndexChange` (it is also used for the FK-cascade and MERGE-delete code paths and for vector indexes); only the *apply* becomes a no-op for the B-tree heap-delete case.

**(A2) Stop physical index removal on the old-key arm of key-changing UPDATE.** In `apply_update_index_changes` (index_ops.rs:151–158), drop the `delete_key(old_key, old_tid, …)` call but **keep** the `insert_key(new_key, new_tid, …)`. The old TID's tuple now carries `xmax = subxid/xid` and is filtered by heap recheck; the new entry is the live one. `updated_ctid_target` (btree_probe.rs:344) already chases the UPDATE redirect so an index hit on the stale old entry resolves to the right version (or is dropped).

**(A3 — the single must-fix new work) Uniqueness probe must recheck heap visibility.** Once dead entries linger, `contains_key`/`lookup_tid` (index_maintainer.rs:64–72) over-reports conflicts: a UNIQUE index would falsely reject re-insertion of a key whose old tuple is dead (rolled-back, or deleted-and-committed). Replace the index-only conflict test with a heap-visibility-gated one (PG does this through the heap):

```rust
// index_maintainer.rs — new conflict test used by insert/update unique enforcement.
// Returns true only if SOME index hit for `key` points at a LIVE (visible-to-an-up-to-date
// snapshot) heap tuple. A hit whose heap tuple is dead/aborted is NOT a conflict.
pub(crate) fn has_live_conflict(
    &self, key: i64, heap: &HeapAccess, snapshot: &Snapshot, oracle: &dyn XidStatusOracle,
) -> Result<bool, ExecError> {
    // For each TID indexed under `key` (unique => at most the lookup TID;
    // non-unique uniqueness is N/A), fetch the heap tuple and test visibility.
    let Some(tid) = self.lookup_tid(key)? else { return Ok(false) };
    let tuple = heap.fetch(tid)?;
    Ok(matches!(is_visible(&tuple.header, snapshot, oracle),
                Visibility::Visible | Visibility::VisiblePreImage | Visibility::DeletedByOwn))
}
```
Route the two unique checks — `apply_update_index_changes` (index_ops.rs:160) and `precheck_update_index_changes` (index_ops.rs:186) — and the INSERT-side `reject_duplicate_insert_keys`/`contains_key` precheck through `has_live_conflict`. For correctness of the uniqueness *guarantee* this recheck must use an up-to-date snapshot (PG uses a fresh "dirty" snapshot for uniqueness, not the statement MVCC snapshot, so it also blocks against in-progress inserters). Concretely: probe with the writer's own `current_xid` snapshot extended so that an in-progress *other* writer's pending insert still counts as a conflict (treat `Invisible`-because-in-progress as a conflict, `Invisible`-because-aborted/deleted as not). This is the one genuinely new piece of logic and must have its own test (see §5, test U).

> Note: `BTree::insert` itself also raises `DuplicateKey` (insert.rs / index_maintainer.rs:91). For a true unique index the physical tree still holds the stale key, so `tree.insert` would reject the reuse at the storage layer regardless of the executor check. **Therefore A3 also requires** the unique-index insert path to tolerate a stale duplicate physical key whose heap tuple is dead — i.e. unique inserts must use a "insert-or-replace-if-dead" against the leaf, or the executor must `delete_logged` the *specific dead TID's* entry immediately before inserting the new one (a targeted, same-key physical replace, which is safe because it is not a rollback-undo and is fully WAL-logged in the forward direction). The targeted-replace approach is simpler and keeps unique leaves from accumulating dead duplicates. Use it for **unique** indexes only; non-unique indexes take the pure leave-entries path.

### Blast radius

**Small and localized.** Changes touch only:
- `crates/ultrasql-executor/src/modify/index_ops.rs` (apply_delete: no-op; apply_update old-key arm: drop delete; both unique checks: route through heap recheck).
- `crates/ultrasql-executor/src/modify/index_maintainer.rs` (add `has_live_conflict`; for unique indexes add targeted-dead-replace).
- VACUUM driver: ensure the btree `is_dead` predicate is fed by the heap dead-tuple horizon (verify `vacuum.rs:30–50` is actually scheduled for index leaves — see §4 step 9).
- **Read side: zero changes** (already rechecks).
- **WAL: fewer records emitted** (no more `BTreeOpKind::Delete` for the MVCC-delete case); DELETE/UPDATE get cheaper.

No changes to fused fast paths for index coherence: the fused/fast DELETE bypass is gated to **non-indexed** tables (mvcc_maint.rs:139–146 returns `None` when indexes exist), so it never touches an index.

---

## 2. 100% SUBXID STAMPING — exhaustive coverage + a guard that fails on any parent stamp

**Stamping rule (uniform, no exceptions): every heap `xmin`/`xmax` written by user DML stamps `txn.current_xid()`** (manager.rs:173 — already returns the active subxid when a savepoint is open, else the parent). The B-tree leaf carries no MVCC stamp (node.rs:197 — `{key, value}` only); its `xid` argument is WAL-chaining-only and already sourced from `ctx.xid`, so the index needs no stamping change.

### Every write/stamp site, classified (verified against disk)

| # | Path | File:line | Source today | Action |
|---|------|-----------|--------------|--------|
| 1 | General INSERT operator | executor `operator.rs:395/506` ← `insert.rs:165` | `ctx.xid` = current_xid | OK |
| 2 | Fused INSERT int32 (executor) | `fused_insert.rs:87` ← `insert.rs:408` | `ctx.xid` | OK |
| 3 | **FAST INSERT int32 (cached bound-plan bypass)** | **`bound_plan.rs:482`** | **`txn.xid` (PARENT)** | **FIX → `txn.current_xid()`** |
| 4 | COPY FROM stdin | `copy/stdio.rs:534` | `txn.current_xid()` | OK |
| 5 | COPY FROM file | `file_ops.rs:476` (pipeline) | `ctx.xid` | OK |
| 6 | TimePartitionInsert | `time_partition.rs:485` ← `insert.rs:92` | `ctx.xid` | OK |
| 7 | Classical UPDATE new version | `heap/update.rs:113/447` ← `UpdateOptions.xid` ← `ctx.xid` | current_xid | OK |
| 8 | General DELETE / UPDATE old-version | executor `operator.rs:100/449/481` ← `delete.rs:144`/`update.rs:371` | `ctx.xid` | OK |
| 9 | FK CASCADE delete/update | `referential.rs:256/267/302/333/357` ← `constraints.rs:56` | `ctx.xid` | OK |
| 10 | Fused UPDATE int32 (executor) | `fused_update.rs:313/337/359` ← `update.rs:193` | `ctx.xid` | OK |
| 11 | Fused DELETE int32 via pipeline | `fused_delete.rs:174` ← `delete.rs:90` | `ctx.xid` | OK |
| 12 | **Fused DELETE int32 EXPLICIT-TXN bypass** | **`mvcc_maint.rs:163`** | **`txn.xid` (PARENT)** | **FIX → `txn.current_xid()`** |
| 13 | In-place UPDATE | `update_inplace.rs` ← `UpdateOptions.xid` = `ctx.xid` | current_xid | OK |
| 14 | MERGE (insert/update/delete) | `merge.rs:304` | `ctx.xid` | OK |
| 15 | B-tree leaf | `node.rs:197` (no MVCC stamp; WAL xid = `ctx.xid`) | — | OK (no stamp) |
| 16 | Vector index tombstones | `index_maintainer.rs:141…` (WAL xid = `ctx.xid`) | — | OK (no stamp) |
| 17 | WAL applier replay | `wal_applier.rs:393/475/582/727/856` | re-stamps recorded payload xid verbatim | OK by construction once #3,#12 fixed |
| 18 | **ALTER TABLE rewrite (DDL)** | `alter.rs:740/765/1234/1250/1267/1595` | `txn.xid` | **Secondary** — see decision below |

### The two production fixes

**Fix #3** — `bound_plan.rs:482`:
```rust
xmin: txn.current_xid(),   // was: txn.xid
```
**Fix #12** — `mvcc_maint.rs:163`:
```rust
let stamp = DeleteInt32PairStamp {
    xid: txn.current_xid(),   // was: txn.xid
    command_id: txn.current_command,
};
```
Because `DeleteInt32PairStamp.xid` feeds *both* the heap header (`stamp_delete_int32_pair_header`, delete.rs:532) **and** the WAL payload, and the applier re-stamps the recorded xid (wal_applier.rs), fixing the call site fixes recovery for free. **Verify** the WAL payload's xid is the same `stamp.xid` value, not a separately-passed `txn.xid` (it is, by construction of `DeleteInt32PairStamp`, but assert in a recovery test — §5 test R).

### DDL scope decision (#18)

**Recommended: leave `alter.rs` parent-stamped and document the limitation.** Treat an ALTER-TABLE rewrite as a parent-bound command (PG runs DDL as its own command; the rewrite produces a fresh heap bound to the (sub)transaction). SAVEPOINT-around-DDL exact own-write rollback is out of scope for the #1 DML-integrity item. Add a one-line known-limitation note. If a future feature needs exact DDL-under-savepoint rollback, convert those six sites to `current_xid()` then — it is mechanical and isolated.

### The regression guard (this is what would have caught the fused-DELETE miss)

The implicit invariant "every DML stamp routes through `current_xid()`" is load-bearing and undocumented — that is *why* the revert happened. Add **defense-in-depth chokepoint + debug assertion**, not just the two edits:

1. **Encapsulate the stamp source.** Add `Transaction::write_xid()` as the *only* sanctioned stamp accessor (an alias of `current_xid()` with a doc-comment forbidding `txn.xid` at write sites), and make all DML stamp sites use it. Optionally make `Transaction::xid` `pub(crate)`-readable only for *termination/lock/SSI* identity, not stamping.

2. **A `debug_assert` in the heap stamping helpers** that the stamped xid is either the parent top xid or a live descendant subxid of the current backend's open savepoint stack. This needs the subxid→parent oracle (§3) wired into the storage stamp path; gate it behind `cfg(debug_assertions)` so release builds pay nothing. It fires the instant any fast path stamps a foreign/parent xid while a savepoint is open.

3. **An invariant test harness** (§5 test G) that, for **each** write path, performs a write under an active SAVEPOINT then asserts the stamped header xid equals `current_xid()` (read back via a heap fetch / debug hook). This is the matrix that must include the exact int32-pair-fused-DELETE shape (2× Int32, no index, no referenced-by checks — the gate at mvcc_maint.rs:131–146).

---

## 3. REUSE — what to re-apply from the reverted commits, and what to change

Re-land the MVCC/txn layer that was reviewed correct, with the three defect fixes folded in.

### (R1) Snapshot subxid sets + widened `is_current_xid` — re-apply verbatim (from `35b7e09b`)
`crates/ultrasql-mvcc/src/snapshot.rs`: two private sorted `SmallVec`s — `own_live_subxids` (live + released-but-parent-open = "self") and `own_rolled_back_subxids` (forced invisible) — plus a derived `own_subxid_lo` range bound. Widen:
```rust
pub fn is_current_xid(&self, xid: Xid) -> bool {
    xid == self.current_xid
      || (!self.own_live_subxids.is_empty()
          && xid >= self.own_subxid_lo
          && self.own_live_subxids.binary_search(&xid).is_ok())
}
pub fn own_subxid_rolled_back(&self, xid: Xid) -> bool {
    !self.own_rolled_back_subxids.is_empty()
      && xid >= self.own_subxid_lo
      && self.own_rolled_back_subxids.binary_search(&xid).is_ok()
}
```
`set_own_subxids` patches **only** the two vecs + the bound in place, leaving `xmin/xmax/xip/current_xid/current_command` untouched — this is how a frozen RR/SSI snapshot stays coherent across SAVEPOINT/RELEASE/ROLLBACK TO without breaking snapshot stability. Empty sets collapse the hot path to the single-Xid compare. Re-land the four snapshot tests (`is_current_xid_includes_own_live_subxids`, `rolled_back_subxid_is_not_current_but_is_flagged`, `empty_subxid_sets_short_circuit`, `set_own_subxids_patches_only_subxid_sets`).

### (R2) Single visibility predicate — re-apply, **delete the dead `SubxactOracle` path**, and fix DEFECT 3
`crates/ultrasql-mvcc/src/visibility.rs`: collapse to one predicate (drop `SubxactOracle`/`NoSubxacts`/`is_visible_ext` and the `InfoMask::SUBXACT` branch — there is no on-disk SUBXACT bit; the snapshot set lookup is the sole authority, keeping savepoint-ness off disk). Add guards:

- **Top guard:** `if snapshot.own_subxid_rolled_back(header.xmin) { return Invisible }` — a rolled-back insert beats any CLOG "committed" hint at any isolation level.
- **DEFECT 3 FIX (the masked self-inserter gap):** inside the `is_current_xid(header.xmin)` branch (visibility.rs:171–204), **before** the live-xmax `UPDATED_IN_PLACE` handling at :183, add:
  ```rust
  if !header.xmax.is_invalid() && snapshot.own_subxid_rolled_back(header.xmax) {
      // Our own row was deleted / in-place-updated by a subxid we rolled back.
      // The delete/update "did not count" — revert independently of physical undo.
      return if header.infomask.contains(InfoMask::UPDATED_IN_PLACE) {
          Visibility::VisiblePreImage
      } else {
          Visibility::Visible
      };
  }
  ```
  This makes visibility correct **independent of whether physical undo ran**, closing the unsound masking the reverted code relied on.
- **Foreign-deleter revert (the later guard, now reachable for the non-current-xmin path):** after the committed-before-snapshot `xmin` gate, mirror the same `own_subxid_rolled_back(header.xmax)` check before treating `xmax` as a committed delete.

### (R3) subxid→parent oracle (pg_subtrans analog) + atomic fold — re-apply verbatim (from `79fe951a`)
`crates/ultrasql-txn/src/manager.rs`: `DashMap<Xid,Xid> subxid_parent`, recorded in `begin_savepoint`. `XidStatusOracle::status`: if the subxid's own CLOG entry is `InProgress` **and** it has a parent link, return the **parent's** status (one level of indirection; savepoints always fold to the top parent). This keeps a RELEASEd-but-parent-open subxid invisible to foreign backends and makes the parent's single commit/abort the only observable boundary.

- **`release_savepoint` MUST stop flipping to Committed** (the current live bug at manager.rs:630). Change it to **keep the subxid `InProgress`** and add it to `merged_up` (see R5). The parent's commit is the only thing that makes it durable/visible — this kills the cross-txn dirty-read + permanent-leak-on-parent-abort.
- Add `terminate_with_subxids`: at top commit/abort, flip the **parent CLOG first** (preserves commit-at-most-once), then flip each still-InProgress subxid, then remove parent + all folded subxids from the `in_progress` mirror **under one lock** so a concurrent `build_snapshot` (taking the same lock) sees the whole family in-progress or none of it — no torn read. Abort re-aborts released subxids. The SSI serialization-failure path (`commit`, manager.rs:399–409) force-aborts the folded subxids too.

### (R4) `build_snapshot` own-subxid exclusion — re-apply (from `b608e578`)
Thread `OwnSubxids{live, rolled_back}` into `build_snapshot` (manager.rs:511). Own live (+merged-up) subxids are **excluded** from `xip`/`xmin` (they are self, not concurrent foreign writers) and the two sets are emitted into the snapshot via `set_own_subxids`. `begin` uses `OwnSubxids::empty()`; RC `refresh_snapshot` (manager.rs:332) rebuilds with `OwnSubxids::from_subtxn`; `statement_snapshot` (manager.rs:357) carries the prior sets over (constant within a statement).

### (R5) `merged_up` set + `rollback_to` pruning — re-apply (from `b608e578`/`79fe951a`)
`crates/ultrasql-txn/src/savepoint.rs`: add `merged_up: Mutex<HashSet<Xid>>` (released-but-parent-open subxids = "self" until top commit). `rollback_to` computes `cutoff = target savepoint's own subxid` (subxids strictly increasing), drains the stack from the target, **and prunes `merged_up` of every subxid ≥ cutoff, moving them to `rolled_back`** — so ROLLBACK TO an outer savepoint correctly discards an already-RELEASEd inner one instead of folding it Committed at top commit. The existing `rolled_back`/`record_rolled_back`/`is_rolled_back` (savepoint.rs:93,229,238) stay.

### (R6) Index-path `VisiblePreImage` agreement — re-apply (from `82b4da6f`)
`HeapAccess::fetch_visible_pre_image` wraps `lookup_undo_pre_image` (scan.rs:230). Change **both** index read paths so `Visibility::VisiblePreImage` returns the pre-image instead of `Ok(None)`:
- btree_probe.rs:336 — replace `return Ok(None)` with a `fetch_visible_pre_image(current)` call.
- late_materialize.rs:360 — same.

This makes an index scan surface the same pre-image row a seq scan does. Note: this fixes **visibility-level** agreement; with the Option-A index model it is now also the mechanism that handles the rolled-back-in-place-update-of-own-row case end-to-end through the index.

### (R7) Fast-INSERT subxid stamp — covered in §2 (#3) and extended to #12.

### (R8) Column-cache coherence — already merged (`b121812a`), plus add ROLLBACK-TO invalidation
The publish/read gate on a quiescent, writer-visible snapshot is **already on main**. One addition: `execute_rollback_to_savepoint` (txn.rs:556) must **invalidate the column cache for touched relations** (call the existing `invalidate_column_cache_for_tables` for relations modified under the rolled-back subxids), because the shared cache is keyed on relation mutation-version, not snapshot, and a rolled-back savepoint's writes must not survive in a published projection.

---

## 4. Ordered implementation steps (with file:line anchors)

Sequencing follows the revert memo's safe-staging guidance: land the *visibility-correct, non-corrupting* milestone first, gated so fast paths cannot corrupt, then the index model, then re-enable.

**Phase 0 — Stamping + guard (closes DEFECT 1; cheap, highest urgency)**
1. `bound_plan.rs:482` `xmin: txn.xid → txn.current_xid()`.
2. `mvcc_maint.rs:163` `xid: txn.xid → txn.current_xid()`.
3. Add `Transaction::write_xid()` chokepoint (manager.rs near :173) and route both fixed sites + a doc-comment; add the `cfg(debug_assertions)` stamp assertion in `heap/delete.rs:532` and `heap/helpers.rs:347` (xmax stamp) + insert stamp helper.
4. Add the §5 test-G stamping matrix; confirm each FAILS pre-fix, passes post-fix.

**Phase 1 — MVCC/txn layer (R1–R5; visibility-correct)**
5. `snapshot.rs` — add subxid sets + widen `is_current_xid` + `own_subxid_rolled_back` + `set_own_subxids` (R1).
6. `visibility.rs` — collapse to one predicate, delete `SubxactOracle`, add the two rolled-back guards **including the DEFECT-3 in-branch xmax check** (R2).
7. `manager.rs` — `subxid_parent` map + oracle indirection (R3); `release_savepoint` keep-InProgress + merged_up; `terminate_with_subxids` atomic fold under the `in_progress` lock; `build_snapshot`/`refresh_snapshot`/`statement_snapshot` thread `OwnSubxids` (R3/R4); wire `commit`/`abort` (manager.rs:382,424) and the SSI force-abort path to the family fold.
8. `savepoint.rs` — `merged_up` + `rollback_to` cutoff pruning (R5).

**Phase 2 — ROLLBACK TO physical + cache (heap side; gated)**
9. `execute_rollback_to_savepoint` (txn.rs:556) — for each aborted subxid call `heap.rollback_in_place_updates(sub_xid)` **and** `heap.rollback_delete_stamps(sub_xid)` (delete.rs:619, currently never called), then `invalidate_column_cache_for_tables` for touched relations. (With the Phase-1 predicate these are now belt-and-suspenders for own-visibility, but they reclaim heap bytes and keep seq-scan and the undo log consistent.)
10. **Gate the fast/fused in-place DELETE on no-savepoint:** in `mvcc_maint.rs` dispatch (the gate at :131–146) and `dml_txn.rs:185`, fall through to the general MVCC delete path when `txn.subtxn_stack.depth() > 0`. This guarantees no corruption even if a fast path is later mis-stamped, and is the "safe milestone" gate.

**Phase 3 — Index model (Option A; §1)**
11. `index_ops.rs:339` — `apply_delete_index_changes` → no-op for B-tree heap-delete.
12. `index_ops.rs:151` — old-key arm of `apply_update_index_changes` → drop `delete_key`, keep `insert_key`.
13. `index_maintainer.rs` — add `has_live_conflict` (heap-recheck uniqueness) + targeted-dead-replace for unique inserts; route `index_ops.rs:160,186` and the INSERT precheck through it (A3).
14. btree_probe.rs:336 + late_materialize.rs:360 — `VisiblePreImage` → `fetch_visible_pre_image` (R6).
15. Verify VACUUM scheduler drives `BTree::vacuum(is_dead)` (vacuum.rs:30–50) with the heap dead-tuple horizon; add a regression test that dead leaf entries are reclaimed.

**Phase 4 — Re-enable + harden**
16. Remove the Phase-2 no-savepoint gate on the fused DELETE (step 10) once Phases 1–3 are green and the full §5 battery passes — the predicate + stamping now make it safe.
17. Document the ALTER-TABLE-under-savepoint limitation (#18).

---

## 5. Adversarial test plan (each must FAIL on current pre-fix code)

All end-to-end tests run two connections (S1 writer, S2 concurrent reader) against the real server so heap truth is verified from a second connection, and run on **both** table shapes:
- **Shape N (no-index, column-cache-eligible):** `t_pair(id int4, val int4)` — triggers the fast/fused int32-pair paths and the column cache.
- **Shape I (indexed/multi-column):** `t_idx(id int4 primary key, val int4, name text)` with a secondary index on `val` — forces the general operator + index paths.

**A — Own-write visible.** `BEGIN; INSERT(1); SAVEPOINT s1; INSERT(2); SELECT` → `{1,2}` via seq scan **and** index scan. *Fails now:* `is_current_xid` is parent-only, so row 2 (xmin=subxid) is invisible to the same txn.

**B — ROLLBACK TO hides insert.** Continue A: `ROLLBACK TO s1; SELECT` → `{1}` via seq **and** index. From S2 mid-txn → `{}`. After `COMMIT`, S2 → `{1}`. *Fails now:* no rolled-back-subxid guard.

**C — ROLLBACK TO restores deleted row** (the corruption test). `t={1}`; `BEGIN; SAVEPOINT s1; DELETE WHERE id=1; SELECT → {}; ROLLBACK TO s1; SELECT → {1}; COMMIT`. Then **S2 → {1}** and `sum(val)` correct. Run on Shape N (hits the fused int32-pair DELETE bypass at mvcc_maint.rs:163) **and** Shape I (general indexed delete). *Fails now on Shape N:* DELETE stamps parent xmax, ROLLBACK TO keys on subxid → row stays dead → COMMIT makes it durably gone (the exact reverted corruption). *Fails now on Shape I via index scan:* even if heap restored, the physically-removed leaf entry is gone (DEFECT 2) — this test must pass only after Phase 3.

**D — Nested RELEASE-inner-then-ROLLBACK-outer.** `BEGIN; INSERT(10); SAVEPOINT a; INSERT(20); SAVEPOINT b; INSERT(30); ROLLBACK TO b → {10,20}; ` then a fresh variant `SAVEPOINT a; INSERT(20); RELEASE a; ROLLBACK TO <outer>` asserts the released inner subxid is **discarded**, not folded Committed at top commit. Also `… RELEASE a; COMMIT → row 20 persists`. *Fails now:* no merged_up pruning; release flips Committed → leaked row.

**E — UPDATE pre-image.** `t={(1,100)}; BEGIN; SAVEPOINT s1; UPDATE val=200 WHERE id=1; SELECT → 200; ROLLBACK TO s1; SELECT → 100` via seq **and** index. Plus the **self-inserter** DEFECT-3 shape: parent inserts row, a subxid in-place-UPDATEs it, ROLLBACK TO → must revert to pre-image *without* relying on physical undo (run with physical undo stubbed off in a unit test of `is_visible`). *Fails now:* the in-branch `own_subxid_rolled_back(xmax)` check is absent; index path returns `Ok(None)` on `VisiblePreImage`.

**F — Cross-txn isolation, gated on top commit/abort.** S1 `BEGIN; SAVEPOINT s1; INSERT(99)`; S2 (any time pre-commit) → no 99. S1 `RELEASE s1` (still pre-commit) → S2 still no 99 (no dirty read after RELEASE — the leak test). S1 `COMMIT` → S2 new snapshot sees 99. Variant: S1 `ROLLBACK TO s1; COMMIT` → S2 never sees 99. *Fails now:* release flips Committed → S2 dirty-reads 99 before S1 commits.

**G — No-parent-stamp guard / stamping matrix.** For each write path {general/fast/fused INSERT, general/fused/explicit-txn DELETE, classical/in-place/fused UPDATE, COPY stdin+file, MERGE, TimePartitionInsert, FK cascade}: write under an active SAVEPOINT, read the heap header back, assert `header.xmin/xmax == current_xid()` (the subxid), not the parent. Explicitly include the int32-pair fused-DELETE shape (2× Int32, no index, no referenced-by checks). *Fails now:* sites #3 and #12 stamp parent.

**H — COMMIT atomicity under racing readers.** S1 builds a 3-deep savepoint family with writes at each level + one RELEASE; a spawned reader thread takes snapshots in a tight loop across S1's `COMMIT`. Assert every observed snapshot is **all-or-nothing** for the family (never parent-committed-while-subxid-in-progress, never a torn subset). *Fails/races now:* no atomic `terminate_with_subxids` under the `in_progress` lock.

**I — Isolation matrix RC / RR / Serializable.** Repeat A–F under each isolation level. RR/SSI must keep the frozen snapshot stable while `set_own_subxids` patches only the subxid sets across SAVEPOINT/RELEASE/ROLLBACK TO (assert `xmin/xmax/xip` unchanged). Serializable: a savepoint write that participates in a dangerous structure still triggers serialization failure, and the failure path force-aborts the folded subxids.

**R — Crash recovery of subxact rollback.** Replay-after-restart variant of C and B: write under savepoint, ROLLBACK TO, COMMIT, crash, recover from WAL; assert the rolled-back row's invisibility survives replay (the applier re-stamps the *subxid*, not the parent). *Fails now:* parent-stamped delete replays durably wrong.

**U — Unique-index dead-entry reuse (Option-A must-fix).** UNIQUE index on `val`. Insert `val=5`; delete it and COMMIT (dead entry lingers); re-insert `val=5` → must succeed (no false `UniqueViolation`). Variant: insert `val=5` under a savepoint, ROLLBACK TO, then re-insert `val=5` → succeeds. Variant: a *live* `val=5` still present → correctly rejected. Concurrent variant: in-progress other writer holding `val=5` uncommitted → rejected (dirty-snapshot conflict). *Fails after Phase 3 without A3:* `contains_key` over-reports the dead entry as a conflict.

**Z — Index/seq agreement fuzz.** Randomized DML under random savepoint nesting + ROLLBACK TO/RELEASE, then assert seq-scan rowset == index-scan rowset == a second-connection rowset for every committed state. This is the catch-all for the btree_probe.rs:18–33 contract.

---

## 6. Effort + risk assessment, and the recommended safe increment

### Bounded / safe (do all, low risk)
- **Stamping fixes #3, #12 + chokepoint + guard (Phase 0):** ~1 day. Two one-line edits, one accessor, one debug-assert, one test matrix. Closes the actual data-corruption vector. Highest value-to-effort.
- **Re-land MVCC snapshot/visibility/oracle/fold (Phase 1, R1–R5):** moderate, but these commits were already reviewed correct; this is largely a re-apply with the DEFECT-3 in-branch xmax check added. Risk is concentrated in the atomic fold (test H) and the SSI interaction (test I) — both have dedicated tests.
- **Index model Option A apply-path edits (Phase 3, steps 11–14):** small, localized, read side untouched. R6 pre-image re-land is two two-line edits.

### Larger / needs care (still in scope; gated)
- **A3 uniqueness heap-recheck + unique-index dead-replace (step 13):** the *one genuinely new* algorithm. Getting the dirty-snapshot semantics right (in-progress inserter = conflict; dead/aborted = not) is subtle. Mitigated by test U's four variants. Medium risk.
- **VACUUM-drives-index reclamation (step 15):** must confirm the scheduler actually visits index leaves with a correct `is_dead`, else dead entries accumulate unbounded. Verification + one test; low code, real diligence.
- **Atomic `terminate_with_subxids` under the `in_progress` lock (step 7):** the concurrency-correctness crux. A future refactor that splits these locks re-introduces torn reads — call this out in the doc-comment and guard with test H.

### Recommended SAFE INCREMENT if one pass is too big
Land **Phase 0 + Phase 1 + Phase 2 with the step-10 no-savepoint gate on the fused DELETE**, *without* Phase 3. This yields the revert memo's "visibility-correct, physically-incomplete-but-non-corrupting" milestone:
- DEFECT 1 closed (stamping + chokepoint).
- DEFECT 3 closed (predicate self-sufficient).
- Own-write visibility, ROLLBACK-TO hide/restore, nested RELEASE, cross-txn isolation, COMMIT atomicity all correct (tests A,B,D,E,F,H,I,R pass).
- Index coherence after ROLLBACK TO on **indexed** tables (test C Shape-I via index scan) is deferred — covered by the gate falling back to the general MVCC delete (which leaves the heap authoritative) **plus** the fact that the physical-removal incoherence only bites a *rolled-back indexed delete*; if Phase 3 is not yet in, gate indexed DELETE under an open savepoint onto a path that does not physically remove (i.e. land step 11–12 even in the safe increment — they are tiny and make the index model coherent regardless).

**My recommendation: do the complete implementation (Phases 0–4).** This is the #1 data-integrity item; the index decision (Option A) is low-infra precisely because the read side already rechecks, and the only new algorithm (A3) is small and well-bounded by tests. Splitting risks shipping the visibility layer that *looks* done while indexed ROLLBACK TO silently disagrees between access paths — the exact disagreement that forced the revert. Gate the whole thing behind the §5 battery as a mandatory adversarial review before any push; every test there must fail on pre-fix `main` and pass after.

### Key file anchors for the implementer
- `crates/ultrasql-mvcc/src/snapshot.rs:95` · `crates/ultrasql-mvcc/src/visibility.rs:142,171,183`
- `crates/ultrasql-txn/src/manager.rs:173,332,357,382,424,511,567,589,621` · `crates/ultrasql-txn/src/savepoint.rs:93,166,229`
- `crates/ultrasql-server/src/session/execute/bound_plan.rs:482` · `crates/ultrasql-server/src/session/execute/mvcc_maint.rs:163` · `crates/ultrasql-server/src/session/execute/dml_txn.rs:185`
- `crates/ultrasql-server/src/session/txn.rs:556` · `crates/ultrasql-server/src/txn_exec.rs:204,422`
- `crates/ultrasql-executor/src/modify/index_ops.rs:151,160,186,339` · `crates/ultrasql-executor/src/modify/index_maintainer.rs:64,103`
- `crates/ultrasql-server/src/pipeline/index_scan/btree_probe.rs:316,336` · `late_materialize.rs:350,360`
- `crates/ultrasql-storage/src/heap/delete.rs:532,619` · `update_inplace.rs:294` · `scan.rs:230` · `btree/vacuum.rs:47` · `btree/lookup.rs:265`