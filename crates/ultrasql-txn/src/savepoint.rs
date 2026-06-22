//! Subtransaction / savepoint manager.
//!
//! Implements `SAVEPOINT name`, `ROLLBACK TO SAVEPOINT name`, and
//! `RELEASE SAVEPOINT name` as described in the PostgreSQL manual §13.3.
//!
//! # Model
//!
//! Each [`SubtxnManager`] is owned by a single top-level transaction.  It
//! maintains a stack of [`Subtxn`] handles, one per active savepoint.
//! Savepoints are named; names need not be unique — a `RELEASE` or
//! `ROLLBACK TO` targets the *most recent* savepoint with the matching name,
//! matching PostgreSQL behavior.
//!
//! ## Visibility semantics
//!
//! A subtransaction's writes are identified by their XID in the tuple header
//! (`xmin`).  They become visible to siblings only after the subtransaction is
//! released (i.e. its XID is merged into the parent chain).  Until release,
//! visibility follows normal MVCC rules: a concurrent reader whose snapshot
//! pre-dates the subxid will not see the writes.
//!
//! Aborting a subtransaction via `ROLLBACK TO` marks its XID as aborted in
//! the CLOG; visibility rules then hide those tuples automatically.
//!
//! # Concurrency
//!
//! [`SubtxnManager`] is owned by one connection and is accessed exclusively
//! from that connection's task.  The `Mutex<Vec<Subtxn>>` is therefore
//! uncontended in practice; it exists only so the type can be held inside an
//! `Arc<SubtxnManager>` without `RefCell`.

use std::collections::HashSet;

use parking_lot::Mutex;
use ultrasql_core::{CommandId, Xid};

/// A savepoint (subtransaction) record.
///
/// The record is returned by [`SubtxnManager::savepoint`] and stored in the
/// stack until the savepoint is released or rolled back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subtxn {
    /// XID of the parent (top-level) transaction.
    pub parent_xid: Xid,
    /// XID allocated for this subtransaction.  Writes performed after the
    /// savepoint is set carry this XID in their tuple header.
    pub xid: Xid,
    /// User-visible savepoint name.
    pub name: String,
    /// [`CommandId`] at the moment the savepoint was set.  Used to restore
    /// the command counter on rollback.
    pub command_id_at_save: CommandId,
}

/// Errors returned by savepoint operations.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SavepointError {
    /// No savepoint with the given name exists in the current stack.
    #[error("savepoint \"{name}\" does not exist")]
    NotFound {
        /// The savepoint name that was not found.
        name: String,
    },
}

/// Subtransaction / savepoint manager for one top-level transaction.
///
/// Maintains a stack of [`Subtxn`] entries.  The stack grows on
/// [`Self::savepoint`] and shrinks on [`Self::rollback_to`] (which may
/// pop multiple entries) or [`Self::release`] (which pops exactly one).
///
/// Also maintains a per-parent set of rolled-back subtransaction XIDs so
/// that visibility code can detect tuples written by rolled-back savepoints
/// even when the parent transaction is still in progress (i.e. when the
/// [`crate::manager::TransactionManager`] CLOG already marks those subxids
/// `Aborted` but callers want a fast local check).
///
/// # Send + Sync
///
/// [`SubtxnManager`] is `Send + Sync` because `parking_lot::Mutex` is
/// `Send + Sync` and all inner collections contain only `Send + Sync` types.
#[derive(Debug)]
pub struct SubtxnManager {
    /// Parent (top-level) XID.  Informational; not mutated after construction.
    parent_xid: Xid,
    /// Stack of active savepoints, LIFO order.
    stack: Mutex<Vec<Subtxn>>,
    /// XIDs of subtransactions that have been rolled back within this
    /// top-level transaction.  Entries are added by
    /// [`Self::record_rolled_back`] (typically called by the transaction
    /// manager after updating the CLOG) and never removed — aborted state
    /// is permanent.
    rolled_back: Mutex<HashSet<Xid>>,
    /// XIDs of subtransactions that have been **released** (merged up into
    /// the parent) while the parent transaction is still open.  Their
    /// writes remain visible to the parent — and behave like *self* for
    /// own-write visibility — until the top-level transaction commits or
    /// aborts.  Populated by [`Self::record_merged_up`] on `RELEASE`;
    /// folded into the parent's CLOG status at top-level commit/abort.
    merged_up: Mutex<HashSet<Xid>>,
}

impl Clone for SubtxnManager {
    /// Clone the manager, producing a snapshot of the current stack and
    /// rolled-back set.
    ///
    /// The cloned instance shares no lock with the original; subsequent
    /// mutations to either are independent.  This matches the semantics
    /// needed for cloning a [`crate::manager::Transaction`] handle.
    fn clone(&self) -> Self {
        let stack_clone = self.stack.lock().clone();
        let rolled_back_clone = self.rolled_back.lock().clone();
        let merged_up_clone = self.merged_up.lock().clone();
        Self {
            parent_xid: self.parent_xid,
            stack: Mutex::new(stack_clone),
            rolled_back: Mutex::new(rolled_back_clone),
            merged_up: Mutex::new(merged_up_clone),
        }
    }
}

impl SubtxnManager {
    /// Create a new [`SubtxnManager`] for a transaction with the given parent
    /// XID.
    #[must_use]
    pub fn new(parent: Xid) -> Self {
        Self {
            parent_xid: parent,
            stack: Mutex::new(Vec::new()),
            rolled_back: Mutex::new(HashSet::new()),
            merged_up: Mutex::new(HashSet::new()),
        }
    }

    /// Set a savepoint with the given name.
    ///
    /// Allocates a new subtransaction XID via `alloc_xid` and pushes a
    /// [`Subtxn`] record onto the stack.  Returns the newly created record.
    ///
    /// Multiple savepoints may share the same name; [`Self::rollback_to`] and
    /// [`Self::release`] target the most recent matching entry.
    ///
    /// # Parameters
    ///
    /// - `name` — the savepoint name (arbitrary UTF-8, matches SQL identifier).
    /// - `alloc_xid` — a closure that allocates and returns a fresh XID.  The
    ///   manager calls this exactly once per invocation.
    /// - `current_cid` — the current [`CommandId`] at the time of the savepoint.
    pub fn savepoint(
        &self,
        name: &str,
        alloc_xid: impl FnOnce() -> Xid,
        current_cid: CommandId,
    ) -> Subtxn {
        let xid = alloc_xid();
        let entry = Subtxn {
            parent_xid: self.parent_xid,
            xid,
            name: name.to_owned(),
            command_id_at_save: current_cid,
        };
        self.stack.lock().push(entry.clone());
        entry
    }

    /// Roll back to the savepoint named `name`.
    ///
    /// Finds the most recent savepoint with the given name, removes it and
    /// all savepoints set after it, and returns the XIDs of all removed
    /// subtransactions (in stack order, most-recent-first).  The caller is
    /// responsible for marking those XIDs as aborted in the CLOG.
    ///
    /// Returns [`SavepointError::NotFound`] if no savepoint with that name
    /// exists.
    pub fn rollback_to(&self, name: &str) -> Result<Vec<Xid>, SavepointError> {
        let mut stack = self.stack.lock();
        // Find the most recent entry with the matching name (scan from the top).
        let pos =
            stack
                .iter()
                .rposition(|s| s.name == name)
                .ok_or_else(|| SavepointError::NotFound {
                    name: name.to_owned(),
                })?;

        // The target savepoint's own subxid is the low-water mark for
        // "opened at or after this savepoint": subxids are allocated in
        // strictly increasing order, so every subtransaction started at or
        // after the target — whether still live on the stack or already
        // RELEASEd into `merged_up` — has a subxid `>=` this value.
        let cutoff = stack[pos].xid;

        // Drain from `pos` to the end.  The entry at `pos` itself is also
        // removed — after rollback the savepoint no longer exists and must be
        // re-established via another `SAVEPOINT name` if needed.
        let mut removed: Vec<Xid> = stack.drain(pos..).map(|s| s.xid).collect();
        drop(stack);

        // Prune `merged_up`: a subtransaction that was RELEASEd *after* the
        // target savepoint must still be discarded by ROLLBACK TO (PostgreSQL
        // discards every subxact started after the named savepoint, released
        // or not). `merged_up` grows monotonically on RELEASE and is never
        // otherwise pruned, so without this an inner released savepoint's
        // writes would survive a rollback to an outer savepoint — and be
        // folded `Committed` at top-level commit. Move every merged-up subxid
        // `>= cutoff` into the rolled-back set so it becomes invisible and is
        // never folded up.
        {
            let mut merged = self.merged_up.lock();
            let pruned: Vec<Xid> = merged.iter().copied().filter(|x| *x >= cutoff).collect();
            for x in &pruned {
                merged.remove(x);
            }
            drop(merged);
            removed.extend(pruned);
        }
        Ok(removed)
    }

    /// Release the savepoint named `name`.
    ///
    /// Finds the most recent savepoint with the given name and removes it from
    /// the stack, returning its XID.  The caller is responsible for folding the
    /// subtransaction's writes into the parent (e.g. by making the XID visible
    /// as committed in the CLOG).
    ///
    /// Unlike [`Self::rollback_to`], release removes only the target savepoint
    /// and leaves deeper savepoints intact.
    ///
    /// Returns [`SavepointError::NotFound`] if no savepoint with that name
    /// exists.
    pub fn release(&self, name: &str) -> Result<Xid, SavepointError> {
        let mut stack = self.stack.lock();
        let pos =
            stack
                .iter()
                .rposition(|s| s.name == name)
                .ok_or_else(|| SavepointError::NotFound {
                    name: name.to_owned(),
                })?;
        let xid = stack.remove(pos).xid;
        drop(stack);
        Ok(xid)
    }

    /// Return a snapshot of the current savepoint stack (bottom to top).
    ///
    /// Useful for diagnostics and tests.
    pub fn stack_snapshot(&self) -> Vec<Subtxn> {
        self.stack.lock().clone()
    }

    /// Return the depth of the savepoint stack (number of active savepoints).
    pub fn depth(&self) -> usize {
        self.stack.lock().len()
    }

    /// Record `subxid` as having been rolled back within this transaction.
    ///
    /// Called by the transaction manager after marking the subtransaction
    /// `Aborted` in the CLOG.  Visibility code consults
    /// [`Self::is_rolled_back`] when it encounters a tuple with the
    /// `SUBXACT` infomask bit set.
    pub fn record_rolled_back(&self, subxid: Xid) {
        self.rolled_back.lock().insert(subxid);
    }

    /// Return `true` if `subxid` was rolled back within this transaction.
    ///
    /// This is a local, lock-free-when-uncontended alternative to querying
    /// the CLOG.  The set only grows; entries are never removed.
    #[must_use]
    pub fn is_rolled_back(&self, subxid: Xid) -> bool {
        self.rolled_back.lock().contains(&subxid)
    }

    /// Record `subxid` as having been **released** (merged up into the
    /// parent) while the parent is still open.
    ///
    /// Called by the transaction manager after marking the subtransaction
    /// `Committed` in the CLOG on `RELEASE SAVEPOINT`.  The subxid is then
    /// treated as *self* for own-write visibility until the top-level
    /// transaction resolves, matching PostgreSQL's
    /// `TransactionIdIsCurrentTransactionId` semantics.
    pub fn record_merged_up(&self, subxid: Xid) {
        self.merged_up.lock().insert(subxid);
    }

    /// This transaction's own *live* (still on the stack) plus
    /// *merged-up* (released, parent still open) subtransaction XIDs,
    /// sorted ascending and de-duplicated.
    ///
    /// This is the set treated as *self* by the visibility predicate.
    /// Empty when no savepoint is or was active in this transaction.
    #[must_use]
    pub fn own_live_subxids_sorted(&self) -> Vec<Xid> {
        let mut out: Vec<Xid> = {
            let stack = self.stack.lock();
            let merged = self.merged_up.lock();
            stack
                .iter()
                .map(|s| s.xid)
                .chain(merged.iter().copied())
                .collect()
        };
        out.sort_unstable();
        out.dedup();
        out
    }

    /// This transaction's rolled-back subtransaction XIDs, sorted
    /// ascending.  Empty when nothing was rolled back.
    #[must_use]
    pub fn rolled_back_sorted(&self) -> Vec<Xid> {
        let mut out: Vec<Xid> = self.rolled_back.lock().iter().copied().collect();
        out.sort_unstable();
        out
    }

    /// This transaction's merged-up (released, parent still open)
    /// subtransaction XIDs, sorted ascending.  Empty when nothing was
    /// released.
    #[must_use]
    pub fn merged_up_sorted(&self) -> Vec<Xid> {
        let mut out: Vec<Xid> = self.merged_up.lock().iter().copied().collect();
        out.sort_unstable();
        out
    }
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use ultrasql_core::{CommandId, Xid};

    use super::*;

    fn xid(n: u64) -> Xid {
        Xid::new(n)
    }

    fn cid(n: u32) -> CommandId {
        CommandId::new(n)
    }

    /// XID allocator for tests: increments a counter each call.
    fn make_alloc(start: u64) -> impl FnMut() -> Xid {
        let mut counter = start;
        move || {
            let x = xid(counter);
            counter += 1;
            x
        }
    }

    // ── savepoint is pushed onto the stack ──────────────────────────────────

    #[test]
    fn savepoint_creates_entry_on_stack() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(100);
        let sp = mgr.savepoint("sp1", &mut alloc, cid(0));

        assert_eq!(sp.name, "sp1");
        assert_eq!(sp.parent_xid, xid(1));
        assert_eq!(sp.xid, xid(100));
        assert_eq!(sp.command_id_at_save, cid(0));
        assert_eq!(mgr.depth(), 1);
    }

    // ── nested savepoints ────────────────────────────────────────────────────

    #[test]
    fn nested_savepoints_stack_correctly() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(10);

        mgr.savepoint("a", &mut alloc, cid(0));
        mgr.savepoint("b", &mut alloc, cid(1));
        mgr.savepoint("c", &mut alloc, cid(2));

        assert_eq!(mgr.depth(), 3);
        let snap = mgr.stack_snapshot();
        assert_eq!(snap[0].name, "a");
        assert_eq!(snap[1].name, "b");
        assert_eq!(snap[2].name, "c");
    }

    // ── rollback_to unwinds to target ────────────────────────────────────────

    #[test]
    fn rollback_to_unwinds_stack() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(20);

        mgr.savepoint("a", &mut alloc, cid(0)); // xid 20
        mgr.savepoint("b", &mut alloc, cid(1)); // xid 21
        mgr.savepoint("c", &mut alloc, cid(2)); // xid 22

        // Rollback to "b" should remove "b" and "c".
        let aborted = mgr.rollback_to("b").unwrap();
        // Returns XIDs in drain order: index 1 ("b") first, then index 2 ("c").
        assert_eq!(aborted.len(), 2);
        assert!(aborted.contains(&xid(21)));
        assert!(aborted.contains(&xid(22)));

        // Only "a" remains.
        assert_eq!(mgr.depth(), 1);
        let snap = mgr.stack_snapshot();
        assert_eq!(snap[0].name, "a");
    }

    // ── rollback_to unknown name returns error ───────────────────────────────

    #[test]
    fn rollback_to_unknown_name_returns_error() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(30);
        mgr.savepoint("sp1", &mut alloc, cid(0));

        let err = mgr.rollback_to("nonexistent").unwrap_err();
        assert_eq!(
            err,
            SavepointError::NotFound {
                name: "nonexistent".to_owned(),
            }
        );
        // Stack remains intact.
        assert_eq!(mgr.depth(), 1);
    }

    // ── release removes only the target ─────────────────────────────────────

    #[test]
    fn release_removes_target_leaves_others() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(40);

        mgr.savepoint("a", &mut alloc, cid(0)); // xid 40
        mgr.savepoint("b", &mut alloc, cid(1)); // xid 41
        mgr.savepoint("c", &mut alloc, cid(2)); // xid 42

        let released = mgr.release("b").unwrap();
        assert_eq!(released, xid(41));

        // "a" and "c" remain.
        assert_eq!(mgr.depth(), 2);
        let snap = mgr.stack_snapshot();
        assert_eq!(snap[0].name, "a");
        assert_eq!(snap[1].name, "c");
    }

    // ── release commits to parent ────────────────────────────────────────────

    /// After release the caller should merge the subtxn's writes into the
    /// parent.  This test verifies that release returns the correct XID so the
    /// caller can mark it committed.
    #[test]
    fn release_returns_xid_for_parent_commit() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(50);

        let sp = mgr.savepoint("work", &mut alloc, cid(0));
        let xid_back = mgr.release("work").unwrap();

        assert_eq!(xid_back, sp.xid, "released XID must match the one assigned");
        assert_eq!(
            mgr.depth(),
            0,
            "stack should be empty after releasing the only savepoint"
        );
    }

    // ── release unknown name returns error ───────────────────────────────────

    #[test]
    fn release_unknown_name_returns_error() {
        let mgr = SubtxnManager::new(xid(1));
        let err = mgr.release("ghost").unwrap_err();
        assert_eq!(
            err,
            SavepointError::NotFound {
                name: "ghost".to_owned(),
            }
        );
    }

    // ── duplicate names target most recent ──────────────────────────────────

    #[test]
    fn duplicate_savepoint_name_targets_most_recent_on_release() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(60);

        mgr.savepoint("dup", &mut alloc, cid(0)); // xid 60 — first
        mgr.savepoint("dup", &mut alloc, cid(1)); // xid 61 — second (most recent)

        let released = mgr.release("dup").unwrap();
        // Most recent "dup" (xid 61) should be released.
        assert_eq!(released, xid(61));
        assert_eq!(mgr.depth(), 1);

        // First "dup" still exists.
        let snap = mgr.stack_snapshot();
        assert_eq!(snap[0].xid, xid(60));
    }

    // ── rollback_to prunes released (merged-up) inner subxids ───────────────

    #[test]
    fn rollback_to_prunes_released_inner_subxids() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(80);

        let outer = mgr.savepoint("outer", &mut alloc, cid(0)).xid; // xid 80
        let inner = mgr.savepoint("inner", &mut alloc, cid(1)).xid; // xid 81
        // Release inner: it leaves the stack and joins merged_up. (The
        // transaction manager records the merged-up xid; mirror that here.)
        let released = mgr.release("inner").unwrap();
        assert_eq!(released, inner);
        mgr.record_merged_up(inner);
        assert!(mgr.merged_up_sorted().contains(&inner));

        // Rolling back to outer must discard outer AND the released inner.
        let removed = mgr.rollback_to("outer").unwrap();
        assert!(removed.contains(&outer));
        assert!(
            removed.contains(&inner),
            "released inner subxid (>= outer's xid) must be discarded by ROLLBACK TO outer",
        );
        assert!(
            !mgr.merged_up_sorted().contains(&inner),
            "inner must be pruned from merged_up",
        );
        assert!(
            !mgr.own_live_subxids_sorted().contains(&inner),
            "inner must no longer be treated as self",
        );
    }

    #[test]
    fn rollback_to_keeps_released_subxids_older_than_target() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(90);

        // Release an *earlier* savepoint, then set a later one and roll back
        // to it: the earlier released subxid (xid < target) must survive.
        let early = mgr.savepoint("early", &mut alloc, cid(0)).xid; // xid 90
        mgr.release("early").unwrap();
        mgr.record_merged_up(early);
        let target = mgr.savepoint("target", &mut alloc, cid(1)).xid; // xid 91
        let removed = mgr.rollback_to("target").unwrap();
        assert!(removed.contains(&target));
        assert!(
            !removed.contains(&early),
            "a released subxid older than the rollback target must survive",
        );
        assert!(mgr.merged_up_sorted().contains(&early));
    }

    #[test]
    fn duplicate_savepoint_name_rollback_targets_most_recent() {
        let mgr = SubtxnManager::new(xid(1));
        let mut alloc = make_alloc(70);

        mgr.savepoint("x", &mut alloc, cid(0)); // xid 70
        mgr.savepoint("x", &mut alloc, cid(1)); // xid 71 — most recent
        mgr.savepoint("y", &mut alloc, cid(2)); // xid 72

        // Rollback to most-recent "x" should remove "x" (xid 71) and "y" (xid 72).
        let aborted = mgr.rollback_to("x").unwrap();
        assert_eq!(aborted.len(), 2);
        assert!(aborted.contains(&xid(71)));
        assert!(aborted.contains(&xid(72)));

        // First "x" (xid 70) remains.
        assert_eq!(mgr.depth(), 1);
        assert_eq!(mgr.stack_snapshot()[0].xid, xid(70));
    }
}
