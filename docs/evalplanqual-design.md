> **Design for TODO.md item #3** (EvalPlanQual / READ COMMITTED concurrent-update
> re-check). **PIECE (1) — error classification (`HeapError::WriteConflict` →
> `ExecError::SerializationFailure` → SQLSTATE 40001) — is ALREADY LANDED** at commit
> `626f8d8f` (`feat(executor,server): classify concurrent-update write conflicts as
> retryable SQLSTATE 40001`). This document captures the **remaining behavior change**:
> generalizing the proven wait+refresh+re-evaluate loop, plus the corruption-prone work
> that is explicitly **deferred** to a dedicated, SAVEPOINT-class effort. It is
> code-verified against `main` (every anchor below was re-read on disk before being
> written as fact). Following this repo's own precedent — the SAVEPOINT feature was
> **reverted once for fused-delete corruption** and re-landed only behind an A–Z
> adversarial battery (`docs/savepoint-subtransactions-design.md`) — the behavior change
> is to be implemented behind a **hard adversarial gate**: every lost-update / skip /
> deadlock case must fail on pre-change `main` and pass after, with **no push if any case
> is red**.

---

# UltraSQL EvalPlanQual / READ COMMITTED Concurrent-Update Re-check — Implementation-Ready Design

## Verified current-state map (live `main`, re-read on disk, not from memos)

I re-read every anchor before designing. Confirmed on `main`:

### The three (and only) `HeapError::WriteConflict` raise sites — all in `update_inplace.rs`

There are exactly **three** places in the whole engine that raise `HeapError::WriteConflict`,
all in the fused `(Int32, Int32)` in-place UPDATE primitives, all gated on the **same**
predicate: `is_visible()` returns `Visibility::VisiblePreImage` **and** the pre-image bytes
still match the WHERE clause. The arm is identical at every site:

```rust
Visibility::VisiblePreImage => {
    if predicate(id, val) {            // pre-image bytes still match the WHERE clause
        return Err(HeapError::WriteConflict("in-place tuple has an unresolved writer"));
    }
    continue;                          // pre-image no longer matches -> silently skip
}
```

- `crates/ultrasql-storage/src/heap/update_inplace.rs:676-685` — `update_int32_pair_inplace_undo` (WAL scan form) → **seq-scan UPDATE**.
- `crates/ultrasql-storage/src/heap/update_inplace.rs:1081-1089` — `update_int32_pair_inplace_undo_parallel_no_wal` (no-WAL scan form) → **seq-scan UPDATE**.
- `crates/ultrasql-storage/src/heap/update_inplace.rs:1215-1226` — `update_int32_pair_tid_inplace_undo` (indexed point form) → **point UPDATE**.

`Visibility::VisiblePreImage` (`crates/ultrasql-mvcc/src/visibility.rs:197-211`) is produced
when a slot carries `InfoMask::UPDATED_IN_PLACE` **and** its `xmax` is **not**
committed-before-our-snapshot — i.e. one of five states: in-progress, aborted,
committed-after-our-snapshot, our own future-command write, or our own rolled-back subxid.
The predicate fires for an **in-progress** writer too, not only a committed one — pinned by
`crates/ultrasql-storage/src/heap/tests/update.rs:148-167`, where writer `20` is in
writer-`30`'s `xip` set (in-progress) yet still triggers `WriteConflict` rather than waiting.

At each raise site the full decoded `TupleHeader` is in hand (so `header.xmax` — the
conflicting writer's xid — is recoverable), plus the live `snapshot`, the `oracle`
(CLOG / `XidStatusOracle`), the page write guard, and the decoded `(id, val)`. The
conflicting xid being recoverable is what makes a wait-on-writer feasible **from the
executor** (never from under the page guard — see §"Load-bearing invariant").

### DELETE has ZERO conflict detection — a real lost-delete

DELETE never raises any conflict. The three batch/range delete loops decode the header,
call `is_visible`, then skip every non-`Visible` outcome:

- `crates/ultrasql-storage/src/heap/delete.rs:1068`, `:1570`, `:1755` — `if !matches!(visibility, Visibility::Visible) { continue; }`.

The row-level primitive `delete_in_place`
(`crates/ultrasql-storage/src/heap/delete.rs:1810-1834`) does
`header.mark_deleted(xmax, cmax)` with **no MVCC check at all** — it blind-stamps `xmax`
over whatever is there. Because plain DELETE never sets `UPDATED_IN_PLACE`, `is_visible`
**never** returns `VisiblePreImage` for a deleted row: a concurrent **in-progress** deleter
yields `Visibility::Visible` (`crates/ultrasql-mvcc/src/visibility.rs:226`), so a second
in-progress deleter sees the row as live and stamps over it — **two concurrent deleters
double-stamp `xmax` = a real lost-delete**. This is net-new behavior to fix, not a refactor,
and it lives on the hottest RC code path; it is **deferred** (below).

### The existing PARTIAL EvalPlanQual seam (indexed int32-pair UPDATE only)

A genuine wait → re-fetch → re-evaluate → apply-or-skip loop already exists, confined to the
fused indexed-point UPDATE. Wiring:

- `crates/ultrasql-server/src/pipeline/modify/update.rs:196-208` — for index-probe target
  TIDs the operator gets `.with_target_tid_lock(move |tid| acquire_indexed_update_row_lock(...), refresh_after_lock)`
  where **`refresh_after_lock = ctx.isolation == IsolationLevel::ReadCommitted`** (the RC-only
  gate).
- `crates/ultrasql-server/src/pipeline/modify/update.rs:279-304` — `acquire_indexed_update_row_lock`
  takes `LockTag::Tuple(tid)` **Exclusive**: `try_acquire` first, and on conflict blocks in
  `lock_manager.acquire(req)` (wrapped in `tokio::task::block_in_place` on the multi-thread
  runtime), returning `waited = true`.
- `crates/ultrasql-executor/src/fused_update.rs:292-348` — the loop: for each TID call the
  lock callback; if it waited **and** `refresh_snapshot_after_lock`, rebuild
  `oracle.statement_snapshot(xid, command_id)` **before** the heap write; run
  `update_int32_pair_tid_inplace_undo` against the refreshed snapshot (it re-runs `is_visible`
  + the predicate against the latest slot bytes); on a residual `WriteConflict` with a lock
  held and not-yet-refreshed, refresh **once** and retry exactly once
  (`fused_update.rs:321-344`, bounded by `!refreshed_after_lock`).

This is a **one-shot, fused-only, predicate-as-closure** EvalPlanQual analog. Because the
in-place model keeps the new version at the **same TID**, "re-fetch latest" is literally
re-reading the same slot under the refreshed snapshot — no ctid walk. The delta composes
commutatively (`col += delta`) onto the committed post-image, so it is **lost-update-safe**
for this path (pinned: the existing `v=2` test).

### Supporting infrastructure (verified)

- `is_visible` — `crates/ultrasql-mvcc/src/visibility.rs:84`. The single MVCC predicate read
  by **every** heap/index/stats reader; the `UPDATED_IN_PLACE` → `Visible`/`VisiblePreImage`
  decision is at `:197-211`; the concurrent-plain-deleter → `Visible` fall-through is at `:226`.
- `LockManager` — `crates/ultrasql-txn/src/lock.rs`. `LockTag::Tuple(TupleId)` is a first-class
  tag (`:128-129`); `acquire` enqueues a waiter and parks on the per-entry `waiters_changed`
  `parking_lot::Condvar` (`:201`, wait at `:487`); the **only** wakeups are `release`/`release_all`
  `notify_all` (`:562`, `:598`); `commit`/`abort` call `release_all(xid)`
  (`crates/ultrasql-txn/src/manager.rs:531`, `:584`). There is **no** separate XactLock — a txn
  waits for another by blocking on the **tuple** lock the other holds until its commit/abort.
  The deadlock detector sweeps the central table every `DEFAULT_DEADLOCK_INTERVAL = 1s`
  (`lock.rs:316`, loop at `:728`), builds a wait-for graph over all tags incl. `Tuple`, picks
  the youngest-xid victim, and returns `LockError::Deadlock { victim }` (`:483`, `:855`). The
  lock layer does **not** abort the txn — the caller must translate `Deadlock` to a full abort.
- `statement_snapshot` — `crates/ultrasql-txn/src/manager.rs:476` (and
  `statement_snapshot_with_subxids` at `:484`). `refresh_snapshot` (`:436`) rebuilds the
  snapshot wholesale **only** for RC; RR/Serializable keep the frozen snapshot.
- Fused DELETE already **bails to the general MVCC path under an open savepoint**:
  `crates/ultrasql-server/src/session/execute/mvcc_maint.rs:133` — `if txn.subtxn_stack.depth() > 0 { ... }`.
  This is the precedent piece (2) mirrors.
- Two duplicate ctid chasers exist: `updated_ctid_target` at
  `crates/ultrasql-server/src/pipeline/index_scan/btree_probe.rs:484` and at
  `crates/ultrasql-executor/src/modify/helpers.rs:131` (64-hop cap). The version-creating
  `heap.update` path errors `MalformedHeader("update on deleted tuple")` on a concurrently-dead
  old slot (`crates/ultrasql-storage/src/heap/helpers.rs:110`, `:217`) with no chain walk.

---

## Gap vs PostgreSQL

PostgreSQL `heap_update`/`heap_delete` on a row a concurrent txn already touched return
`TM_Updated`. At READ COMMITTED the executor (`ExecUpdate`/`ExecDelete` → `EvalPlanQual`)
does **not abort**: it `XactLockTableWait`s on the row's `xmax` until that writer ends; if the
writer committed it `EvalPlanQualFetch`es the latest version (following the `t_ctid` chain),
re-evaluates the statement qual against it, and either applies to the new version or skips the
row; if the writer aborted it proceeds on the original.

UltraSQL diverges on **five axes** (from the map):

1. **It errors at the storage layer** (`HeapError::WriteConflict`) instead of returning a
   `TM_Updated`-style status the executor can act on. (PIECE (1) made this error *honest* —
   SQLSTATE 40001 — but it is still an abort, not a wait.)
2. **It fires on the PRE-image qual**, never re-evaluating against the latest committed
   POST-image — there is no `EvalPlanQualFetch` equivalent at the raise site (except inside the
   one indexed seam).
3. **It fires for in-progress / aborted / committed-after writers alike**, where PG would wait
   (in-progress), proceed (aborted), or re-fetch (committed) — there is no
   `XactLockTableWait`-keyed-on-`xmax` at the raise site.
4. **The partial seam waits on `LockTag::Tuple(tid)`, not on the writer's xact**, does a
   **one-shot** retry, and exists **only** for the indexed-point branch — the seq-scan UPDATE
   branch and **all** of DELETE have no wait/refetch/retry.
5. **DELETE is worse than "no EPQ": it has no conflict detection at all** — two in-progress
   deleters both see `Visible` and double-stamp (lost-delete), and `delete_in_place` skips MVCC
   entirely.

---

## The PIECE split (the core of this document)

The honest conclusion from the synthesis: **do not attempt a "general EvalPlanQual for all
UPDATE/DELETE" in one session** — that is the dedicated, corruption-prone effort. The work
decomposes into separable pieces, each independently shippable behind its own adversarial gate.

### PIECE (1) — error classification → SQLSTATE 40001. ✅ DONE (`626f8d8f`).

Added `ExecError::SerializationFailure(String)` (`crates/ultrasql-executor/src/lib.rs:328`;
none existed before) and routed `HeapError::WriteConflict` to it through
`heap_update_error_to_exec_error` (`crates/ultrasql-executor/src/fused_update.rs:389-396`,
formerly the catch-all `other => ExecError::TypeMismatch`) and the direct modify call sites,
then wired it to SQLSTATE **40001** (`serialization_failure`) in the server. This is the
SAVEPOINT-style "make the failure honest before changing the behavior" move: it touches no
MVCC, no page guards, no locks — it cannot corrupt anything — and it converts every
statement-abort into a retryable 40001 the client/driver can loop. It is the prerequisite that
lets clients survive the wider rollout.

### PIECE (2) — generalize the proven wait+refresh+re-evaluate loop to the SCAN int32-pair UPDATE (still int32-pair only). **THIS is the remaining behavior change.**

The indexed branch (`fused_update.rs:292-348`) already does the correct PG-like dance. The
**scan** branch (`fused_update.rs:349-378` → `update_inplace.rs:674-689` / `:1081-1089`)
instead **raises** `WriteConflict` on `VisiblePreImage`+match and never waits. PIECE (2):
when `refresh_after_lock` (RC), make the scan path, upon hitting a `VisiblePreImage`-matching
slot, **collect that TID**, finish the page (**drop the page write guard first**), then run
those TIDs through the **same** indexed wait+refresh+re-evaluate primitive that already works.
This reuses a proven, tested loop on a proven storage primitive; it does **not** push conflict
detection into the heap stamp primitives and does **not** touch the general ModifyTable path.

The design rests on these verified facts and constraints:

- **The loop lives in the EXECUTOR (`FusedUpdateInt32Add::next_batch`), never in the heap
  under a page guard.** This is the *load-bearing invariant* — the existing code already
  respects it. The wait (a blocking condvar park inside `LockManager::acquire`, `lock.rs:487`,
  wrapped in `block_in_place` on the multi-thread runtime, `update.rs:295-302`) happens in the
  lock callback **before** the heap call; `update_int32_pair_tid_inplace_undo` takes the page
  write guard **only after** the lock is held. **Blocking on a row lock while holding a heap
  page write guard is a textbook buffer-pool / cluster-wide deadlock and is forbidden.** The
  scan path therefore must surface conflicting TIDs out from under its page guard and re-drive
  them through the executor loop.

- **The in-place model means "re-fetch latest" is re-reading the same TID under a refreshed
  snapshot** — no ctid walk. The new version lives at the **same** TID
  (`visibility.rs:183-211`), and `is_visible` flips `VisiblePreImage → Visible` once the
  writer's `xmax` is committed-before-the-refreshed-snapshot (`:207-208`). This is exactly why
  the int32-pair path is the safe place to generalize and the version-creating path is deferred.

- **Reuse the existing predicate closure** (`fused_update.rs:276-284`) over `(id, val)` — it
  **is** the re-evaluation, run against the current slot bytes inside
  `update_int32_pair_tid_inplace_undo` (`update_inplace.rs:1228`). No new qual machinery for
  piece (2).

- **Lost-update prevention** rests on two facts that hold for piece (2) and **must be asserted
  by tests**: (i) `statement_snapshot` (`manager.rs:476`) rebuilds the active set **after** the
  prior writer committed, so the writer's `xmax` reads committed-before-snapshot →
  `is_visible` returns `Visible` (post-image); (ii) the edit composes commutatively
  (`col += delta`) onto the **current** bytes, so two serialized `+1`s yield `+2` (pinned by the
  existing `v=2` test).

- **The RC-only snapshot-refresh gate MUST be preserved.** `refresh_after_lock` is
  `isolation == ReadCommitted` (`update.rs:197`). For RR/Serializable the snapshot is **not**
  refreshed — refreshing mid-statement there would break RR/SSI isolation. Piece (2) must keep
  this gate intact.

- **`VisiblePreImage` own-vs-foreign disambiguation** (the subtle correctness crux).
  `VisiblePreImage` conflates **five** `xmax` states: in-progress, aborted,
  committed-after-snapshot, own-future-command, own-rolled-back-subxid
  (`visibility.rs:197-210`). The wait→refresh→re-eval loop is correct for the **foreign**
  in-progress / committed-after cases (wait → on the writer's commit it becomes `Visible` and
  we apply; on the writer's **abort**, `release_all` wakes us and the refreshed snapshot sees
  the slot's pre-image still matching → we apply on the rolled-back-to bytes, correct). But for
  **own**-write cases (`snapshot.is_current_xid(header.xmax)`, `visibility.rs:198-205`) the loop
  must **not** take a row lock keyed on our own xid and must **not** treat own-future-command as
  a foreign conflict. **Piece (2) branches: only a foreign `VisiblePreImage`
  (`xmax != current_xid` and `xmax` not an own subxid) triggers the wait; own cases fall
  through to the existing own-write handling.**

- **Savepoint-depth bail.** `statement_snapshot` carries **no** own-subxid sets
  (`manager.rs:469-477`), so re-evaluation under an open savepoint would lose savepoint-aware
  visibility. Piece (2) **must bail to the non-EPQ path (or refuse the fast path) when
  `txn.subtxn_stack.depth() > 0`** — exactly as fused DELETE already does
  (`mvcc_maint.rs:133`).

- **Deadlock → SQLSTATE 40P01.** All waits funnel through the one
  `LockManager::acquire` on `LockTag::Tuple` (`lock.rs:452`), which has the wait-for-graph
  detector and returns `LockError::Deadlock { victim }` (youngest xid, `:855`). The executor
  must translate that to a full transaction abort surfaced as SQLSTATE **40P01**
  (`deadlock_detected`) — the lock layer does **not** abort the txn. The scan generalization
  acquires tuple locks in **page/scan order**, which differs from the index-probe order of the
  existing indexed path; two statements taking the same locks in different orders **can**
  deadlock. This is acceptable (the detector resolves it) but **must be a tested case** (§ test
  battery).

**PIECE (2) blast radius — deliberately contained.** Changed code:
`FusedUpdateInt32Add::next_batch` scan branch (`fused_update.rs:349-378`) gains a
collect-conflicting-TIDs-then-replay-through-the-indexed-primitive step; the heap scan
primitives (`update_int32_pair_inplace_undo` / `_parallel_no_wal`, `update_inplace.rs:513`/`841`)
change from "raise `WriteConflict` on `VisiblePreImage`+match" to "record the TID and continue"
**when invoked in an EPQ mode** — a **mode flag**, **not** a change to the default raise.
Preserve the raise for non-RC and for any caller that does not opt in.

- **Do NOT modify `is_visible` or the `VisiblePreImage` arm.** That predicate is read by every
  scan/index/update/delete/stats reader (`scan.rs`, `walker.rs`, `late_materialize.rs`,
  `btree_probe.rs`, `stats.rs`); piece (2) **consumes** the existing `VisiblePreImage` signal,
  it does not redefine it. **No new `Visibility` variant.**
- The general ModifyTable path, the version-creating `heap.update` path, and **all** of DELETE
  are untouched — that is the deliberate boundary that keeps this increment non-corrupting.

The only behavior change is for rows that **today error**: they now wait + re-evaluate. The
common case (no concurrent writer) never produces `VisiblePreImage`, never enters the new path,
and never takes a tuple lock — the page-major fast scan is unchanged for the overwhelming
majority of rows.

### DEFERRED to a dedicated, SAVEPOINT-class effort (where the corruption / lost-update risk lives)

Each of these is explicitly **out of this increment** and carries its own A–Z battery when
taken up. The reasoning is the repo's own precedent: net-new locking, qual threading, and
chain-following on the highest-traffic RC code is exactly the lost-update/corruption class that
forced the SAVEPOINT revert; that work lands only as a captured design executed behind a hard
gate, never rushed.

- **(a) DELETE EvalPlanQual.** DELETE today takes **no** lock and does **no** conflict check
  (`delete.rs:1755` skips all non-`Visible`; `delete_in_place` at `:1810-1834` blind-stamps with
  zero MVCC check). Two concurrent in-progress deleters both see `Visible`
  (`visibility.rs:226`) and double-stamp `xmax` — **a real lost-delete**. Fixing it is
  **net-new tuple-lock acquisition on the hottest RC path**, changing deadlock/throughput
  characteristics fleet-wide. **Why deferred:** it is new behavior (not a refactor), it is the
  most-trafficked path, and a half-correct change reintroduces the lost-write class.

- **(b) General arbitrary-qual / arbitrary-width ModifyTable EvalPlanQual.** The WHERE clause is
  **not** in ModifyTable — it is a `Filter` in the **child** tree
  (`crates/ultrasql-server/src/pipeline/modify/lowering.rs:26-53`, predicate shifted `+2`). To
  re-evaluate it against a fresh tuple version, the `ScalarExpr` (and its column-index shift)
  must be threaded **out of the child Filter** into a per-row lock/refetch/re-bind/re-evaluate
  loop. That fights the **bulk drain → `update_many`/`delete_many`** architecture
  (`crates/ultrasql-executor/src/modify/operator.rs:62-65`, `:472`, `:493`), which coalesces a
  fully-drained batch into one page-grouped call — an EPQ loop is fundamentally per-row and
  defeats the coalescing. It must also feed the **new** row's column values back into arbitrary
  `SET` assignment expressions (the fused path only re-adds a constant delta). **Why deferred:**
  threading the qual + per-row loop + arbitrary-SET re-binding against the bulk design is large,
  novel, and on the general DML path — exactly the blast radius to keep gated.

- **(c) ctid-chain following for the version-creating `heap.update` path.** HOT / non-HOT
  `heap.update` (`crates/ultrasql-storage/src/heap/update.rs:49`) creates a **new TID** with a
  `ctid` redirect; on a concurrently-dead old slot it errors
  `MalformedHeader("update on deleted tuple")` (`helpers.rs:110`, `:217`) with **no** chain
  walk to the new version. A correct EPQ here must chase `ctid` to the live version, **re-lock
  the chased TID** (not the original), refresh, and re-evaluate — unifying the two duplicated
  `updated_ctid_target` chasers (`btree_probe.rs:484`, `modify/helpers.rs:131`) into a
  heap-level `fetch_latest_for_update(tid) -> (latest_tid, header, data)` primitive. **Why
  deferred:** two update storage models with opposite chain semantics (in-place same-TID vs
  version-creating ctid-redirect) must be reconciled; getting the lock-on-the-chased-version and
  VACUUM-trims-the-chain-end races wrong silently overwrites a committed version (lost update).

---

## Ordered implementation steps — PIECE (2) (with anchors)

Land behind the § adversarial battery as a hard gate. If any lost-update / skip / deadlock case
goes red, **do not push** — ship nothing beyond the already-landed PIECE (1).

1. **Add an EPQ-mode flag to the scan heap primitives.** In `update_int32_pair_inplace_undo`
   (`crates/ultrasql-storage/src/heap/update_inplace.rs:676-685`) and
   `update_int32_pair_inplace_undo_parallel_no_wal` (`:1081-1089`), thread a mode that, on
   `VisiblePreImage` + predicate match, **records the conflicting TID and continues** instead of
   raising `WriteConflict`. **Preserve the existing raise as the default** for non-RC callers and
   any caller that does not opt in. Return the collected TID list to the executor. Do **not**
   touch the page-guard discipline — the TID is recorded under the guard, the wait happens after.

2. **Wire the scan branch in the executor to replay collected TIDs through the indexed
   primitive.** In `FusedUpdateInt32Add::next_batch` scan branch
   (`crates/ultrasql-executor/src/fused_update.rs:349-378`), when `refresh_snapshot_after_lock`
   is set, after the page-major scan returns its collected conflicting TIDs, run **each** through
   the existing indexed loop body (`fused_update.rs:292-348`): take `LockTag::Tuple(tid)`
   Exclusive via the lock callback, refresh `statement_snapshot` if it waited, re-run
   `update_int32_pair_tid_inplace_undo` with the same `predicate_fn`, apply-or-skip. Drop the
   page write guard **before** the first lock acquire (non-negotiable).

3. **Wire the RC-only lock callback into the scan-path operator.** In
   `crates/ultrasql-server/src/pipeline/modify/update.rs:196-208`, extend the wiring so the
   scan-path `FusedUpdateInt32Add` also receives `with_target_tid_lock(acquire_indexed_update_row_lock, refresh_after_lock)`
   with `refresh_after_lock = ctx.isolation == ReadCommitted`. Reuse
   `acquire_indexed_update_row_lock` (`:279-304`) unchanged (try then `block_in_place` blocking
   acquire).

4. **Branch own-vs-foreign at the collection point.** Only record a TID for replay when the
   conflicting `header.xmax` is **foreign** (`!snapshot.is_current_xid(header.xmax)` and not an
   own subxid, per `visibility.rs:198-205`). Own-future-command / own-rolled-back cases must
   fall through to the existing own-write handling — never take a self-keyed row lock.

5. **Bail to the non-EPQ path under an open savepoint.** Before opting the scan path into EPQ
   mode, check `txn.subtxn_stack.depth() > 0` and fall back to the non-EPQ path (mirroring fused
   DELETE at `crates/ultrasql-server/src/session/execute/mvcc_maint.rs:133`), because
   `statement_snapshot` carries no own-subxid sets (`manager.rs:469-477`).

6. **Translate `LockError::Deadlock` to SQLSTATE 40P01.** Ensure the executor surfaces a
   `Deadlock { victim }` from `acquire` (`lock.rs:483`, `:855`) as a full transaction abort
   mapped to `deadlock_detected` (40P01) — distinct from the 40001 a genuine write conflict maps
   to. The lock layer does not abort the txn; the executor/server must.

7. **Migrate the heap test pin.** `crates/ultrasql-storage/src/heap/tests/update.rs:148-167`
   currently asserts an in-progress in-place writer yields `WriteConflict` on the scan path.
   Under RC + EPQ mode that scan path must now **wait** (not immediately conflict). Migrate the
   assertion (do not delete it): pin the non-EPQ default to still raise, and add the EPQ-mode
   wait behavior as a new case.

---

## § Adversarial test battery (the hard gate)

This mirrors the SAVEPOINT precedent: the feature was reverted once for fused-delete corruption
and re-landed only behind an A–Z battery. Today there is exactly **one** concurrency test for
this whole area — `crates/ultrasql-server/tests/update_concurrency_round_trip.rs`
(`concurrent_indexed_updates_wait_and_apply_latest_row`: two connections, A holds → B waits → A
commits → B applies → `v=2`). The battery must extend it to a matrix. **No push without all
green.** Every test must FAIL on pre-change `main` and PASS after. Multi-thread two-connection
tests **must be timeout-bounded** (as the existing test is, via `tokio::time::timeout`) so a
missed wakeup or deadlock surfaces as a **failure, not a hang**.

### Lost-update matrix (two-connection, the core)

1. **Existing indexed `v=2`** — the landed case, kept as the regression anchor.
2. **The SAME on the SCAN int32-pair UPDATE path** (no index) — this **errors today**; after
   piece (2) it must **wait and yield `v=2`**.
3. **N=8 concurrent connections each `+1`** on the same row → assert final = `start + 8` (no lost
   increments, no double-apply) on **both** the indexed and scan paths.
4. **int32-pair-vs-general AGREEMENT** — run the identical concurrent workload on a 2-col int32
   table (fused) and on a 3-col / non-int32 table (general path); assert the general path is
   **either** correct **or** cleanly errors with 40001 — it must **never silently lose /
   under-count**. (Today the general scan path silently skips concurrently-modified rows; this
   test documents and locks that gap and prevents a false claim of RC compliance.)

### Skip-on-no-longer-matches (EvalPlanQual negative)

A holds `UPDATE ... WHERE v=0`; B (waiting) runs `UPDATE ... SET v=v+1 WHERE v=0`; after A
commits `v` to a value that fails the predicate, B must re-evaluate the **fresh** version, find
it no longer matches, and **SKIP** (apply 0 rows, `Ok(0)`) — **not** error and **not** apply.
Pin both the indexed and scan paths.

### Hermitage / PG RC isolation slices

Add RC slices to `crates/ultrasql-txn/tests/hermitage.rs` (today P4 lost-update is only asserted
at RR+, the test at `:246-277`; the header table at `:16` says "Prevented at RR+"):

- **P4 lost-update at READ COMMITTED behaves as PG RC** — the wait + re-read makes the second
  writer compose on the committed value; it must **not** lose the update.
- **G0 dirty-write still prevented.**
- **No-spurious-serialization-error** — an RC concurrent UPDATE that can wait-and-apply must
  **not** raise a spurious 40001; it should 40001 only when it genuinely cannot make progress
  (e.g. a deadlock victim).

### Deadlock

Two connections that lock tuple T1 then T2 vs T2 then T1 (forceable on the scan path because
piece (2) takes tuple locks in **scan order**). Shorten the detector interval in the test
(don't wait the default 1s — `lock.rs:316`). Assert: the detector fires, **one** txn aborts with
SQLSTATE **40P01** (`deadlock_detected`, translated from `LockError::Deadlock`), the other
commits, and there is **no corruption and no hang** (timeout-bounded).

### Migration of the existing heap test pin

`crates/ultrasql-storage/src/heap/tests/update.rs:148-167` encodes the OLD scan-path behavior
(in-progress in-place writer → immediate `WriteConflict`). Under RC + EPQ it must now **WAIT**,
not immediately conflict. This test must be **migrated, not deleted** — keep the non-EPQ default
raising, add the EPQ wait case.

### Gate rule

`cargo test` across **ultrasql-txn** (hermitage, isolation), **ultrasql-server**
(update_concurrency battery, plus the savepoint battery to prove no regression), and
**ultrasql-storage** (heap update/delete) all green; multi-thread two-connection tests
timeout-bounded. **No push if any lost-update, skip, or deadlock case is red.** If the battery
cannot go fully green in a session, split and ship nothing beyond the already-landed PIECE (1).

---

## Effort + risk

- **PIECE (1) — error classification + 40001 wiring: DONE (`626f8d8f`).** ~half a session, low
  risk, fully testable. It touched no MVCC / page / lock / visibility code — it cannot corrupt;
  it only re-labeled an error that already aborted the statement, making it retryable.

- **PIECE (2) — generalize wait+refresh+re-evaluate from the indexed to the SCAN int32-pair
  UPDATE path:** **~1 focused, gated session.** It adds the EPQ-mode flag on the scan heap
  primitives, the `VisiblePreImage` own-vs-foreign disambiguation, the savepoint-depth bail, the
  40P01 deadlock translation, and the full adversarial battery. It reuses a **proven, tested**
  loop on a **proven** storage primitive, with **no change** to the shared `is_visible`
  predicate and **no new** heap stamp logic — a defensible safe increment. Risk is low but
  non-zero (the new path runs only for rows that today error); it is **only** safe behind the
  battery. If any lost-update / skip / deadlock case is red, ship nothing beyond PIECE (1).

- **The deferred general / DELETE work — (a) DELETE EpQ, (b) general arbitrary-qual /
  arbitrary-width ModifyTable EPQ, (c) ctid-chain following for the version-creating path:** a
  **multi-day dedicated effort**, with its own design doc and its own A–Z battery, **comparable
  in scope and risk to the SAVEPOINT feature** (a multi-step land–revert–reland). It requires
  net-new locking, qual threading out of the child Filter, per-row lock/refetch/re-bind against
  the bulk drain design, and chain-following — on the highest-traffic RC code, where a
  half-correct change risks exactly the lost-update / corruption class that forced the SAVEPOINT
  revert. Per this repo's precedent, it lands only as a captured design executed behind a hard
  adversarial gate, never rushed into a single session.

### Key file anchors for the implementer

- `crates/ultrasql-storage/src/heap/update_inplace.rs:676-685` · `:1081-1089` · `:1215-1226`
  (the three `WriteConflict` sites) · `:513`/`:841` (scan primitives) · `:1228` (post-wait
  predicate re-check)
- `crates/ultrasql-storage/src/heap/delete.rs:1068` · `:1570` · `:1755` (skip non-Visible) ·
  `:1810-1834` (`delete_in_place` blind stamp)
- `crates/ultrasql-mvcc/src/visibility.rs:84` (`is_visible`) · `:197-211` (UPDATED_IN_PLACE
  arm) · `:226` (concurrent plain-deleter → Visible)
- `crates/ultrasql-executor/src/fused_update.rs:292-348` (indexed EPQ loop) · `:349-378`
  (scan branch) · `:389-396` (`heap_update_error_to_exec_error` → 40001, **landed**)
- `crates/ultrasql-executor/src/lib.rs:328` (`ExecError::SerializationFailure`, **landed**)
- `crates/ultrasql-server/src/pipeline/modify/update.rs:196-208` (RC gate + lock wiring) ·
  `:279-304` (`acquire_indexed_update_row_lock`)
- `crates/ultrasql-server/src/session/execute/mvcc_maint.rs:133` (savepoint-depth bail
  precedent)
- `crates/ultrasql-txn/src/manager.rs:436` (`refresh_snapshot`, RC-only) · `:476`
  (`statement_snapshot`) · `:484` (`_with_subxids`) · `:531`/`:584` (`release_all`)
- `crates/ultrasql-txn/src/lock.rs:128-129` (`LockTag::Tuple`) · `:452`/`:487` (acquire/wait) ·
  `:483`/`:855` (`Deadlock`/victim) · `:316`/`:728` (detector interval/loop) · `:562`/`:598`
  (notify on release)
- `crates/ultrasql-server/src/pipeline/index_scan/btree_probe.rs:484` ·
  `crates/ultrasql-executor/src/modify/helpers.rs:131` (the two `updated_ctid_target` chasers
  to unify in deferred work (c)) · `crates/ultrasql-storage/src/heap/helpers.rs:110`/`:217`
  (`MalformedHeader("update on deleted tuple")`)
- `crates/ultrasql-server/src/pipeline/modify/lowering.rs:26-53` (WHERE → child Filter, for
  deferred work (b)) · `crates/ultrasql-executor/src/modify/operator.rs:62-65`/`:472`/`:493`
  (bulk drain → `update_many`/`delete_many`)
- Tests: `crates/ultrasql-server/tests/update_concurrency_round_trip.rs` (the lone existing
  concurrency test) · `crates/ultrasql-storage/src/heap/tests/update.rs:148-167` (the pin to
  migrate) · `crates/ultrasql-txn/tests/hermitage.rs:16`,`:246-277` (P4 to extend to RC)
