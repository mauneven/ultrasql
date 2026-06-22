//! Transaction manager.
//!
//! The manager hands out [`Transaction`] handles, tracks their lifecycle
//! in an in-memory commit log (CLOG), and serves visibility queries via
//! the [`ultrasql_mvcc::XidStatusOracle`] trait.
//!
//! Lifecycle
//! ---------
//!
//! ```text
//!                begin()
//!                   │
//!                   ▼
//!            ┌──────────────┐
//!            │  InProgress  │
//!            └──────┬───────┘
//!         commit() │ │ abort()
//!                  ▼ ▼
//!         ┌────────┐ ┌─────────┐
//!         │Committed│ │ Aborted │
//!         └────────┘ └─────────┘
//! ```
//!
//! Snapshots are built by scanning the CLOG for entries still in
//! `InProgress` at the moment the snapshot is requested. The XID counter
//! is an `AtomicU64`; `xmax` for a fresh snapshot is the value the
//! counter would hand to the next caller (i.e. the current load of the
//! counter).
//!
//! Concurrency
//! -----------
//!
//! - `next_xid: AtomicU64` — wait-free XID allocation.
//! - `clog: DashMap<Xid, XidStatus>` — shard-locked map keyed by XID.
//!   The keys are the XIDs we have ever begun; values transition
//!   monotonically (`InProgress -> Committed | Aborted`).
//! - Snapshot construction is read-only against the CLOG and the
//!   counter. It is not strictly atomic — a transaction whose begin is
//!   in flight may be observed as `InProgress` either at snapshot time
//!   or just after. The semantics match PostgreSQL's procarray: a
//!   concurrent begin is either visible as in-progress in the snapshot
//!   or contributes to a future xmax, never both.
//!
//! No `unwrap` or `expect` is used in non-test code in this module.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use ultrasql_core::{CommandId, Lsn, Xid};
use ultrasql_mvcc::{Snapshot, XidStatus, XidStatusOracle};

use crate::lock::LockManager;
use crate::savepoint::{SavepointError, Subtxn, SubtxnManager};
use crate::ssi::{PredicateLockTag, SsiError, SsiManager};

/// A transaction's own subtransaction (savepoint) context, captured for
/// snapshot construction.
///
/// `live` holds this backend's live (still on the stack) plus merged-up
/// (released, parent still open) subxids — the set treated as *self*.
/// `rolled_back` holds the subxids forced invisible. Both are sorted
/// ascending; both are empty for a transaction with no savepoints (the
/// common case), making [`OwnSubxids::is_own`] a cheap empty-set check.
struct OwnSubxids {
    live: Vec<Xid>,
    rolled_back: Vec<Xid>,
}

impl OwnSubxids {
    /// The empty context — no savepoints. Used by [`TransactionManager::begin`]
    /// and any snapshot built for a transaction with no subtransactions.
    fn empty() -> Self {
        Self {
            live: Vec::new(),
            rolled_back: Vec::new(),
        }
    }

    /// Capture the live/merged-up and rolled-back subxid sets from a
    /// transaction's subtransaction stack.
    fn from_subtxn(stack: &SubtxnManager) -> Self {
        Self {
            live: stack.own_live_subxids_sorted(),
            rolled_back: stack.rolled_back_sorted(),
        }
    }

    /// Whether `xid` is one of this backend's own live (+merged-up)
    /// subxids. Binary search over the (tiny, usually empty) live set.
    fn is_own(&self, xid: Xid) -> bool {
        !self.live.is_empty() && self.live.binary_search(&xid).is_ok()
    }
}

/// Isolation level applied to a [`Transaction`].
///
/// v0.5 implements snapshot semantics for [`Self::ReadCommitted`] and
/// [`Self::RepeatableRead`]. [`Self::Serializable`] currently uses the
/// same snapshot strategy as [`Self::RepeatableRead`]; the server records
/// column-range predicate tags for supported scalar comparisons and relation
/// fallback tags for unsupported predicates. The enum value still carries
/// through so callers and tests can branch on the requested level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    /// Each statement sees a fresh snapshot taken at statement start.
    /// `refresh_snapshot` reinstalls the snapshot.
    ReadCommitted,
    /// The transaction's snapshot is fixed at begin and reused for the
    /// life of the transaction. `refresh_snapshot` only bumps the
    /// command id.
    RepeatableRead,
    /// Serializable Snapshot Isolation (SSI) request.
    ///
    /// Uses the same fixed snapshot strategy as [`Self::RepeatableRead`]
    /// for reads, and additionally registers the transaction with
    /// [`SsiManager`] to track rw-anti-dependency edges. The current
    /// server integration records column-range predicate tags for supported
    /// scalar comparisons and relation-level fallback tags, not full
    /// PostgreSQL predicate precision. On commit, the SSI manager
    /// checks for dangerous structures (T1 → T2 → T3 cycles); if a
    /// cycle is found, the commit fails with
    /// [`TxnError::SerializationFailure`].
    Serializable,
}

/// Errors raised by the transaction manager.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TxnError {
    /// The transaction has already terminated. Commit / abort were
    /// invoked on a CLOG entry that is no longer `InProgress`.
    #[error("transaction {xid} already terminated as {status:?}")]
    AlreadyTerminated {
        /// The XID whose status was unexpected.
        xid: Xid,
        /// The terminal status observed.
        status: XidStatus,
    },
    /// The CLOG had no entry for this transaction. This is a programmer
    /// error: it means a [`Transaction`] handle outlived its
    /// [`TransactionManager`] entry (typically a bug in test fixtures
    /// that share state across managers).
    #[error("transaction {xid} unknown to the manager")]
    Unknown {
        /// The XID that was not found.
        xid: Xid,
    },
    /// A serialization anomaly was detected by the SSI manager during
    /// commit. The transaction identified by `victim` must abort and
    /// retry.
    ///
    /// This error is only returned for transactions begun with
    /// [`IsolationLevel::Serializable`] when a [`SsiManager`] is
    /// installed in the [`TransactionManager`].
    #[error("serialization failure: transaction {victim:?} is the pivot; {detail}")]
    SerializationFailure {
        /// The XID that was chosen as the pivot victim.
        victim: Xid,
        /// Human-readable description of the conflict structure.
        detail: String,
    },
}

/// A handle to an in-flight transaction.
///
/// The handle is cheap to clone — the snapshot's in-progress list is a
/// `SmallVec<[Xid; 8]>` that inlines the common case. Cloning is
/// nevertheless rare; the typical lifecycle is `begin -> commit | abort`
/// with no copies in between.
///
/// Each handle commits or aborts at most once. The terminating methods
/// take the handle by value; the type system prevents reuse.
#[derive(Clone, Debug)]
pub struct Transaction {
    /// Globally-unique transaction identifier.
    pub xid: Xid,
    /// Snapshot taken at the time the handle was constructed (or at the
    /// last [`TransactionManager::refresh_snapshot`] for
    /// [`IsolationLevel::ReadCommitted`]).
    pub snapshot: Snapshot,
    /// Isolation level chosen at begin.
    pub isolation: IsolationLevel,
    /// WAL LSN observed at begin. v0.5 records the manager-local
    /// monotonic counter; once the WAL crate exposes the durable LSN at
    /// begin, this field will reflect that value instead. The type is
    /// stable.
    pub start_lsn: Lsn,
    /// Current statement (command) within the transaction. Advances on
    /// every [`TransactionManager::refresh_snapshot`].
    pub current_command: CommandId,
    /// Subtransaction / savepoint stack for this transaction.
    ///
    /// A freshly begun transaction has an empty stack.  `SAVEPOINT name`
    /// pushes an entry via [`TransactionManager::begin_savepoint`];
    /// `ROLLBACK TO SAVEPOINT` and `RELEASE SAVEPOINT` pop via
    /// [`TransactionManager::rollback_to_savepoint`] and
    /// [`TransactionManager::release_savepoint`].
    pub subtxn_stack: SubtxnManager,
}

impl Transaction {
    /// Return the XID writes performed *right now* should carry in
    /// their tuple header `xmin`/`xmax`.
    ///
    /// When no savepoint is active this is the parent
    /// [`Self::xid`]. When a savepoint is active the top of the
    /// subtxn stack returns the inner subtxn's XID, so a subsequent
    /// `ROLLBACK TO` that aborts only that subxid hides exactly the
    /// rows written under that savepoint via the standard MVCC
    /// visibility rules.
    #[must_use]
    pub fn current_xid(&self) -> Xid {
        self.subtxn_stack
            .stack_snapshot()
            .last()
            .map_or(self.xid, |sp| sp.xid)
    }
}

/// The transaction manager.
///
/// Owns the XID counter and the in-memory CLOG. One instance per server;
/// the manager is `Send + Sync` and intended to be shared via `Arc`.
///
/// When an [`SsiManager`] is installed (via [`TransactionManager::new_with_ssi`]),
/// transactions begun at [`IsolationLevel::Serializable`] are registered
/// with the SSI manager and their commits are checked for dangerous structures.
#[derive(Debug)]
pub struct TransactionManager {
    /// The XID allocator. Stores the *next* XID to hand out.
    next_xid: AtomicU64,
    /// In-memory commit log. Keyed by XID, valued by `XidStatus`.
    /// Entries are inserted at begin and transitioned (in place) at
    /// commit / abort. Vacuum may later promote entries to `Frozen`;
    /// that transition is owned by the vacuum subsystem and is not
    /// performed here.
    clog: DashMap<Xid, XidStatus>,
    /// Hot-path mirror of every XID currently in
    /// [`XidStatus::InProgress`]. Updated atomically with `clog`: insert
    /// on `begin`, remove on `terminate`. Holds a `BTreeSet` so
    /// [`Self::build_snapshot`] reads `xmin` in O(log n) (first key) and
    /// emits `xip` in O(n_in_progress) — without walking every
    /// historically-committed CLOG entry. The full `clog` is still the
    /// source of truth for visibility lookups (`XidStatusOracle`) and
    /// recovery.
    in_progress: parking_lot::Mutex<std::collections::BTreeSet<Xid>>,
    /// Subtransaction → top-level (parent) XID map, PostgreSQL's
    /// `pg_subtrans`. Populated on `begin_savepoint` and consulted by the
    /// [`XidStatusOracle`] so a *foreign* backend resolves a subtransaction
    /// to its parent's terminal status rather than to the subxid's own CLOG
    /// entry. This is what keeps a `RELEASE`d-but-parent-still-open subxid
    /// invisible to other backends (no cross-transaction dirty read) and
    /// makes the parent's single commit/abort transition the only boundary a
    /// concurrent reader can observe (no torn read). Entries are removed when
    /// the subxid resolves (rolled back, or folded at parent commit/abort).
    subxid_parent: DashMap<Xid, Xid>,
    /// Optional SSI conflict tracker. Present only when the server is
    /// configured to support [`IsolationLevel::Serializable`] isolation.
    /// `None` causes Serializable to alias `RepeatableRead` (the pre-v0.4
    /// behaviour).
    ssi: Option<Arc<SsiManager>>,
    /// Lock manager for row-level and relation-level locks.
    ///
    /// Owned here so commit/abort can release all locks held by the
    /// terminating transaction.
    pub lock_manager: Arc<LockManager>,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    /// Construct a fresh manager with no SSI support.
    ///
    /// The XID counter starts at [`Xid::FIRST_USER`]. The CLOG is empty.
    /// Transactions begun with [`IsolationLevel::Serializable`] will
    /// alias [`IsolationLevel::RepeatableRead`] until an [`SsiManager`]
    /// is installed via [`Self::new_with_ssi`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_xid: AtomicU64::new(Xid::FIRST_USER.raw()),
            clog: DashMap::new(),
            in_progress: parking_lot::Mutex::new(std::collections::BTreeSet::new()),
            subxid_parent: DashMap::new(),
            ssi: None,
            lock_manager: Arc::new(LockManager::new()),
        }
    }

    /// Construct a manager with SSI conflict-graph support.
    ///
    /// Transactions begun with [`IsolationLevel::Serializable`] will be
    /// registered in `ssi` and their commits will undergo the
    /// dangerous-structure check.  Returns [`TxnError::SerializationFailure`]
    /// when a cycle is detected.
    #[must_use]
    pub fn new_with_ssi(ssi: Arc<SsiManager>) -> Self {
        Self {
            next_xid: AtomicU64::new(Xid::FIRST_USER.raw()),
            clog: DashMap::new(),
            in_progress: parking_lot::Mutex::new(std::collections::BTreeSet::new()),
            subxid_parent: DashMap::new(),
            ssi: Some(ssi),
            lock_manager: Arc::new(LockManager::new()),
        }
    }

    /// Begin a new transaction with the given isolation level.
    ///
    /// Returns a [`Transaction`] handle. The handle owns a snapshot
    /// taken at begin; for [`IsolationLevel::ReadCommitted`] callers
    /// will replace this snapshot at every statement boundary via
    /// [`Self::refresh_snapshot`].
    pub fn begin(&self, isolation: IsolationLevel) -> Transaction {
        // 1. Allocate the XID. The increment is wait-free and the
        //    returned value is unique across threads.
        let raw = self.next_xid.fetch_add(1, Ordering::AcqRel);
        let xid = Xid::new(raw);

        // 2. Publish the XID as in-progress in the CLOG *and* in the
        //    hot-path `in_progress` mirror *before* sampling the
        //    active set. Ordering matters: any snapshot taken
        //    concurrently after these inserts will observe our XID
        //    either in `xip` or via `xmax`; both cases are correct.
        //    The clog stays the source of truth for visibility
        //    queries; `in_progress` is the read-only set
        //    `build_snapshot` walks.
        self.clog.insert(xid, XidStatus::InProgress);
        self.in_progress.lock().insert(xid);

        // 3. Sample the active transactions and the high-water XID. A
        //    freshly begun transaction has no savepoints yet, so the
        //    own-subxid context is empty.
        let snapshot = self.build_snapshot(xid, CommandId::FIRST, &OwnSubxids::empty());

        // 4. Register with SSI if this is a serializable transaction and an
        //    SSI manager is installed.
        if isolation == IsolationLevel::Serializable {
            if let Some(ssi) = &self.ssi {
                ssi.register_xid(xid);
            }
        }

        Transaction {
            xid,
            snapshot,
            isolation,
            start_lsn: Lsn::ZERO,
            current_command: CommandId::FIRST,
            subtxn_stack: SubtxnManager::new(xid),
        }
    }

    /// Register an existing in-flight transaction as serializable.
    ///
    /// Invoked by `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` on a
    /// transaction that began at a weaker level. No-op when no
    /// [`SsiManager`] is installed; otherwise inserts `xid` into the
    /// SSI conflict graph so subsequent rw-conflicts are tracked.
    /// Idempotent — re-registering a known xid is a no-op inside the
    /// SSI manager.
    pub fn register_serializable(&self, xid: Xid) {
        if let Some(ssi) = &self.ssi {
            ssi.register_xid(xid);
        }
    }

    /// Refresh `txn`'s snapshot.
    ///
    /// The behaviour depends on `txn.isolation`:
    ///
    /// - [`IsolationLevel::ReadCommitted`] replaces the snapshot with a
    ///   fresh one taken now. The previous snapshot is discarded.
    /// - [`IsolationLevel::RepeatableRead`] and
    ///   [`IsolationLevel::Serializable`] keep the existing snapshot;
    ///   only `current_command` advances.
    ///
    /// In every case `current_command` advances by one so the
    /// visibility predicate can distinguish writes performed by earlier
    /// statements from writes performed by the current statement.
    pub fn refresh_snapshot(&self, txn: &mut Transaction) {
        txn.current_command = txn.current_command.next();

        match txn.isolation {
            IsolationLevel::ReadCommitted => {
                // Rebuild with the current own-subxid context. Excluding
                // own live subxids from `xip` is what makes the backend
                // see its own savepoint writes under READ COMMITTED.
                let own = OwnSubxids::from_subtxn(&txn.subtxn_stack);
                txn.snapshot = self.build_snapshot(txn.xid, txn.current_command, &own);
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Snapshot stays. Keep `current_xid` / `current_command`
                // coherent inside the existing snapshot so own-write
                // visibility advances with the statement counter. The
                // own-subxid sets are kept current by
                // `begin_savepoint` / `release_savepoint` /
                // `rollback_to_savepoint` (which patch the frozen
                // snapshot in place), so they are not touched here.
                txn.snapshot.current_command = txn.current_command;
            }
        }
    }

    /// Build a fresh statement snapshot for `current_xid` at
    /// `current_command`, preserving the own-subtransaction context of
    /// `prev`.
    ///
    /// Callers use this after blocking on a row lock in READ COMMITTED
    /// mode. The lock wait may let an earlier writer commit after the
    /// statement began; the update then needs to re-check the latest
    /// committed row instead of treating the stale snapshot's `xip`
    /// entry as a permanent write conflict.
    ///
    /// The own-subxid sets are constant within a statement, so they are
    /// carried over verbatim from `prev` (the operator's existing
    /// snapshot, itself built subxid-aware): own live subxids stay
    /// excluded from the refreshed `xip` and rolled-back subxids stay
    /// rejected. For a transaction with no savepoints `prev`'s sets are
    /// empty and this reduces to the prior behaviour.
    #[must_use]
    pub fn statement_snapshot(
        &self,
        current_xid: Xid,
        current_command: CommandId,
        prev: &Snapshot,
    ) -> Snapshot {
        let own = OwnSubxids {
            live: prev.own_live_subxids().to_vec(),
            rolled_back: prev.own_rolled_back_subxids().to_vec(),
        };
        self.build_snapshot(current_xid, current_command, &own)
    }

    /// Commit `txn`. Marks the XID `Committed` in the CLOG.
    ///
    /// For [`IsolationLevel::Serializable`] transactions with an installed
    /// [`SsiManager`], the SSI manager's dangerous-structure check is run
    /// after the CLOG entry is flipped. If a serialization anomaly is
    /// detected, the commit fails with [`TxnError::SerializationFailure`]
    /// and the caller must call [`Self::abort`] to roll back.
    ///
    /// Returns [`TxnError::AlreadyTerminated`] if the XID has already
    /// been committed or aborted, [`TxnError::Unknown`] if the XID is
    /// not in the CLOG. Both indicate misuse; callers are expected to
    /// drive the lifecycle linearly.
    ///
    /// Takes the handle by value: a transaction commits at most once.
    /// The by-value parameter is intentional even though the body only
    /// reads `txn.xid` — moving the handle in lets the type system
    /// enforce the "commit at most once" invariant at call sites.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "by-value enforces the at-most-once lifecycle invariant"
    )]
    pub fn commit(&self, txn: Transaction) -> Result<(), TxnError> {
        let xid = txn.xid;
        let isolation = txn.isolation;

        // Fold the parent and all its still-open subtransactions (live on
        // the stack + released-but-not-yet-folded) into `Committed` as one
        // atomic step. Removing the parent and every subxid from the
        // `in_progress` mirror under a single lock means a concurrent
        // `build_snapshot` (which takes the same lock) samples either the
        // whole transaction family as in-progress or none of it — never a
        // partial state. This is what makes the commit appear atomic to
        // other backends (no torn read where the parent looks committed but
        // a subxid still reads in-progress). Rolled-back subxids are already
        // `Aborted` and absent from the fold set, so they stay aborted.
        self.terminate_with_subxids(xid, &txn.subtxn_stack, XidStatus::Committed)?;

        // Release all row-level and relation-level locks.
        self.lock_manager.release_all(xid);

        // SSI check: only for serializable transactions with an installed manager.
        if isolation == IsolationLevel::Serializable {
            if let Some(ssi) = &self.ssi {
                // Capture the current XID high-water mark as this commit's
                // GC horizon: every transaction able to be concurrent with us
                // has, by definition, already begun and so carries a smaller
                // XID. `terminate` above already removed `xid` from the active
                // set, so a later `oldest_in_progress()` reaching this value
                // proves no concurrent transaction survives.
                if let Err(SsiError::Serialization { victim, detail }) =
                    ssi.commit(xid, self.next_xid())
                {
                    // The SSI manager marked us committed before detecting the
                    // cycle; we must immediately abort to restore consistency.
                    // Flip the CLOG entry back to Aborted using the force path
                    // since the entry is now Committed, not InProgress. Force
                    // the folded subxids back to Aborted too so a savepoint
                    // write does not survive a serialization-failure rollback.
                    self.force_abort(xid);
                    for sub_xid in txn.subtxn_stack.own_live_subxids_sorted() {
                        self.force_abort(sub_xid);
                        self.subxid_parent.remove(&sub_xid);
                    }
                    ssi.abort(xid);
                    return Err(TxnError::SerializationFailure { victim, detail });
                }
            }
        }

        Ok(())
    }

    /// Abort `txn`. Marks the XID `Aborted` in the CLOG.
    ///
    /// See [`Self::commit`] for the error contract and the rationale
    /// for taking the handle by value.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "by-value enforces the at-most-once lifecycle invariant"
    )]
    pub fn abort(&self, txn: Transaction) -> Result<(), TxnError> {
        let xid = txn.xid;
        let isolation = txn.isolation;

        // Fold the parent and all its still-open subtransactions into
        // `Aborted` atomically (see [`Self::commit`] for the atomicity
        // rationale). This also re-aborts any subxid that was RELEASEd but
        // never folded — which, now that RELEASE keeps the subxid
        // `InProgress`, correctly makes a released-then-parent-aborted
        // subtransaction's writes vanish for everyone.
        self.terminate_with_subxids(xid, &txn.subtxn_stack, XidStatus::Aborted)?;

        // Release all row-level and relation-level locks.
        self.lock_manager.release_all(xid);

        // Notify SSI of the abort so the entry can be removed.
        if isolation == IsolationLevel::Serializable {
            if let Some(ssi) = &self.ssi {
                ssi.abort(xid);
            }
        }

        Ok(())
    }

    /// Terminate the parent `xid` together with every still-open
    /// subtransaction of `stack`, transitioning them all to `status`
    /// (`Committed` at top-level commit, `Aborted` at top-level abort) as
    /// one atomic step with respect to snapshot observers.
    ///
    /// "Still open" subtransactions are those live on the savepoint stack or
    /// merged-up (released, parent still open). Rolled-back subxids are
    /// already terminal and absent from the fold set, so they are untouched.
    ///
    /// Atomicity: the CLOG entries are flipped first (the parent must be
    /// `InProgress`; idempotency is enforced exactly as in [`Self::terminate`]),
    /// then the parent and all folded subxids are removed from the
    /// `in_progress` mirror under a **single** lock acquisition. Because
    /// [`Self::build_snapshot`] takes the same lock to sample `xip`, a
    /// concurrent reader observes the whole transaction family as in-progress
    /// or none of it — never the parent committed while a subxid still reads
    /// in-progress. The `subxid_parent` links are dropped last, after the
    /// subxids carry their own terminal CLOG status.
    fn terminate_with_subxids(
        &self,
        xid: Xid,
        stack: &SubtxnManager,
        status: XidStatus,
    ) -> Result<(), TxnError> {
        // Validate + flip the parent first. This preserves the
        // commit-at-most-once invariant: exactly one caller observes the
        // parent `InProgress` and wins the transition.
        {
            let Some(mut entry) = self.clog.get_mut(&xid) else {
                return Err(TxnError::Unknown { xid });
            };
            match *entry.value() {
                XidStatus::InProgress => *entry.value_mut() = status,
                other => return Err(TxnError::AlreadyTerminated { xid, status: other }),
            }
        }

        // `own_live_subxids_sorted` is the union of the live stack and the
        // merged-up set, sorted and de-duplicated. Flip each subxid's CLOG
        // entry that is still `InProgress`, collecting the ones we folded so
        // we can remove them from the mirror atomically with the parent.
        let mut folded: Vec<Xid> = Vec::new();
        for sub_xid in stack.own_live_subxids_sorted() {
            if let Some(mut entry) = self.clog.get_mut(&sub_xid) {
                if matches!(*entry.value(), XidStatus::InProgress) {
                    *entry.value_mut() = status;
                    folded.push(sub_xid);
                }
            }
        }

        // Remove the parent + every folded subxid from the in-progress
        // mirror in one critical section: snapshots see all or nothing.
        {
            let mut active = self.in_progress.lock();
            active.remove(&xid);
            for sub_xid in &folded {
                active.remove(sub_xid);
            }
        }

        // The subxids now carry their own terminal status; drop their parent
        // links so the oracle reads the terminal status directly.
        for sub_xid in &folded {
            self.subxid_parent.remove(sub_xid);
        }
        Ok(())
    }

    /// Current oldest in-progress XID.
    ///
    /// Used by vacuum to decide which dead tuples are no longer visible
    /// to any running snapshot. When no transactions are in progress,
    /// returns the value the next [`Self::begin`] will hand out — that
    /// is, the high-water XID itself (equivalent to PostgreSQL's
    /// `latestCompletedXid + 1`).
    pub fn oldest_in_progress(&self) -> Xid {
        let mut oldest: Option<Xid> = None;
        for entry in &self.clog {
            if matches!(*entry.value(), XidStatus::InProgress) {
                let xid = *entry.key();
                oldest = Some(match oldest {
                    Some(cur) if cur <= xid => cur,
                    _ => xid,
                });
            }
        }
        oldest.unwrap_or_else(|| Xid::new(self.next_xid.load(Ordering::Acquire)))
    }

    /// Whether `xid` is currently recorded as in progress.
    ///
    /// A meaningful answer requires `xid` to be a real, allocated transaction
    /// (the CLOG default for an unknown XID is `InProgress`). The WAL-truncation
    /// checkpoint only calls this for XIDs that have actually written WAL
    /// records, which are by definition allocated, so the result is accurate.
    #[must_use]
    pub fn is_in_progress(&self, xid: Xid) -> bool {
        matches!(self.status(xid), XidStatus::InProgress)
    }

    /// Retire committed SSI entries that no longer overlap any running
    /// transaction.
    ///
    /// `horizon` is the oldest in-progress XID (callers pass
    /// [`Self::oldest_in_progress`]); a committed serializable transaction
    /// whose commit horizon has been reached cannot be concurrent with any
    /// live transaction, so its conflict-graph entry and predicate locks are
    /// dropped. Without this sweep every serializable `COMMIT` would leak an
    /// [`SsiManager`] entry for the life of the process and ancient committed
    /// transactions could fabricate spurious serialization failures against
    /// much later, non-overlapping transactions.
    ///
    /// No-op (returns `0`) when no [`SsiManager`] is installed. Returns the
    /// number of entries retired.
    pub fn collect_ssi_garbage(&self, horizon: Xid) -> usize {
        self.ssi
            .as_ref()
            .map_or(0, |ssi| ssi.collect_garbage(horizon))
    }

    /// XID that the next top-level transaction or savepoint allocation will
    /// hand out.
    #[must_use]
    pub fn next_xid(&self) -> Xid {
        Xid::new(self.next_xid.load(Ordering::Acquire))
    }

    // ---- internal helpers -------------------------------------------------

    /// Build a snapshot at this instant for `current_xid` and
    /// `current_command`, carrying this backend's own subtransaction
    /// context.
    ///
    /// `xmax` is the current value of the XID counter — one past the
    /// largest XID handed out so far. `xmin` is the smallest in-progress
    /// XID; in the absence of any in-progress transaction it equals
    /// `xmax`, which renders [`Snapshot::xid_in_progress`] correct: every
    /// XID strictly less than `xmin` is fully resolved.
    ///
    /// `own_subxids` carries the owning transaction's live (+merged-up)
    /// and rolled-back subtransaction XIDs. All own live (+merged-up)
    /// subxids are **excluded** from `xip`/`xmin` — they are *self*, not
    /// concurrent foreign writers — and the two subxid sets are embedded
    /// in the snapshot so the visibility predicate can treat own
    /// savepoint writes as self / rejected. `own_subxids` is empty for a
    /// transaction with no savepoints (the common case), reducing this to
    /// the pre-subtransaction behaviour.
    fn build_snapshot(
        &self,
        current_xid: Xid,
        current_command: CommandId,
        own_subxids: &OwnSubxids,
    ) -> Snapshot {
        // Sample xmax first. Any XID assigned strictly before this load
        // is observable in the CLOG; any XID assigned after is part of
        // [xmax, ..).
        let xmax_raw = self.next_xid.load(Ordering::Acquire);
        let xmax = Xid::new(xmax_raw);

        // Walk the hot-path `in_progress` mirror instead of the full
        // CLOG. `in_progress` only ever holds InProgress XIDs (begin
        // inserts; terminate removes), so this is O(in-progress) per
        // snapshot rather than O(total committed history). For an
        // autocommit workload with no concurrent writers the set is
        // typically empty or single-entry; the prior CLOG-walk path
        // re-visited every historically-committed entry on every
        // statement.
        //
        // `BTreeSet` gives us `xmin` (smallest element) in O(log n)
        // and an ordered `xip` Vec via in-order iteration without an
        // extra sort. Holding the lock briefly is fine: writers only
        // contend on begin/commit/abort which are already serialised
        // through the CLOG's per-shard locks.
        let active = self.in_progress.lock();
        let mut xip: Vec<Xid> = Vec::with_capacity(active.len());
        let mut min_xid: Option<Xid> = None;
        for &xid in active.iter() {
            // Exclude our own XID — the snapshot's `current_xid`
            // slot identifies us; the visibility predicate treats
            // `current_xid` specially.
            if xid == current_xid {
                continue;
            }
            // Exclude this backend's own live (+merged-up) subxids:
            // they are *self*, carried in the snapshot's
            // `own_live_subxids` set. Leaving them in `xip` would make
            // the backend's own savepoint writes look like a concurrent
            // foreign transaction and hide them from itself (the RC
            // manifestation of the bug). `is_own` is a binary search
            // over the tiny, usually-empty live set.
            if own_subxids.is_own(xid) {
                continue;
            }
            // Defensive: ignore any XID at or above the xmax we
            // observed. Such an XID was inserted after our `xmax`
            // load and falls into the implicit-future region.
            if xid >= xmax {
                continue;
            }
            xip.push(xid);
            min_xid = Some(min_xid.map_or(xid, |cur| if cur <= xid { cur } else { xid }));
        }
        drop(active);

        let xmin = min_xid.unwrap_or(xmax);
        Snapshot::new_with_subxids(
            xmin,
            xmax,
            current_xid,
            current_command,
            xip,
            own_subxids.live.iter().copied(),
            own_subxids.rolled_back.iter().copied(),
        )
    }

    // ---- savepoint helpers -------------------------------------------------

    /// Set a savepoint named `name` within `txn`.
    ///
    /// Allocates a new subtransaction XID and records it on `txn`'s
    /// subtransaction stack.  The returned [`Subtxn`] carries the XID and
    /// command-id context that the executor needs to mark subsequent writes.
    ///
    /// The new subxid is recorded in the CLOG as `InProgress` immediately so
    /// that visibility rules can apply to subtransaction writes.
    pub fn begin_savepoint(&self, txn: &mut Transaction, name: &str) -> Subtxn {
        let parent_xid = txn.xid;
        let subtxn = txn.subtxn_stack.savepoint(
            name,
            || {
                let raw = self.next_xid.fetch_add(1, Ordering::AcqRel);
                let sub_xid = Xid::new(raw);
                self.clog.insert(sub_xid, XidStatus::InProgress);
                self.in_progress.lock().insert(sub_xid);
                // Record the subxid → parent link so a foreign reader
                // resolves this subtransaction to the parent's fate.
                self.subxid_parent.insert(sub_xid, parent_xid);
                sub_xid
            },
            txn.current_command,
        );
        // Keep the snapshot's own-subxid sets current. For READ COMMITTED
        // this only bridges until the next statement's rebuild; for the
        // frozen REPEATABLE READ / SERIALIZABLE snapshot it is the sole
        // mechanism that makes the new subxid visible as *self*.
        self.sync_snapshot_subxids(txn);
        subtxn
    }

    /// Patch `txn`'s snapshot so its own-subtransaction sets match the
    /// current subtransaction stack.
    ///
    /// Mutates only the snapshot's two subxid `SmallVec`s (via
    /// [`Snapshot::set_own_subxids`]); `xmin` / `xmax` / `xip` /
    /// `current_xid` / `current_command` are untouched, so REPEATABLE
    /// READ / SERIALIZABLE snapshot stability is preserved. Called after
    /// every savepoint-control mutation so own savepoint-write visibility
    /// is correct under every isolation level without rebuilding the
    /// snapshot (a rebuild would violate RR/SSI).
    fn sync_snapshot_subxids(&self, txn: &mut Transaction) {
        let live = txn.subtxn_stack.own_live_subxids_sorted();
        let rolled_back = txn.subtxn_stack.rolled_back_sorted();
        txn.snapshot.set_own_subxids(live, rolled_back);
    }

    /// Roll back `txn` to the savepoint named `name`.
    ///
    /// Pops all subtransactions set after `name` (inclusive) from the stack
    /// and marks each of their XIDs as `Aborted` in the CLOG.  Returns the
    /// aborted XIDs so the executor can undo their heap writes.
    ///
    /// Returns [`SavepointError::NotFound`] if no savepoint with that name
    /// exists on `txn`'s stack.
    pub fn rollback_to_savepoint(
        &self,
        txn: &mut Transaction,
        name: &str,
    ) -> Result<Vec<Xid>, SavepointError> {
        let aborted_xids = txn.subtxn_stack.rollback_to(name)?;
        for &sub_xid in &aborted_xids {
            // Transition InProgress → Aborted.  If the entry is missing
            // (programming error) the transition is a no-op. A subxid that
            // was previously RELEASEd is still `InProgress` (RELEASE no
            // longer flips it), so a ROLLBACK TO an outer savepoint can
            // correctly abort it here.
            if let Some(mut entry) = self.clog.get_mut(&sub_xid) {
                if matches!(*entry.value(), XidStatus::InProgress) {
                    *entry.value_mut() = XidStatus::Aborted;
                    drop(entry);
                    self.in_progress.lock().remove(&sub_xid);
                }
            }
            // The subxid is now terminal (Aborted) on its own; drop its
            // parent link so the oracle reads the terminal status directly.
            self.subxid_parent.remove(&sub_xid);
            // Track the rollback locally so the snapshot's visibility
            // predicate forces tuples written by this savepoint invisible
            // even before the CLOG abort is observed.
            txn.subtxn_stack.record_rolled_back(sub_xid);
        }
        // The aborted subxids left the stack and joined the rolled-back
        // set: refresh the snapshot's own-subxid sets so the owning
        // backend immediately stops seeing their writes (insert vanishes,
        // delete reverts) under every isolation level.
        self.sync_snapshot_subxids(txn);
        Ok(aborted_xids)
    }

    /// Release the savepoint named `name` within `txn`.
    ///
    /// Removes the savepoint from the stack and marks its XID as `Committed`
    /// in the CLOG, making the subtransaction's writes permanently visible to
    /// the parent transaction (and to other transactions under normal MVCC
    /// rules once the parent commits).
    ///
    /// Returns the committed subxid on success, or [`SavepointError::NotFound`]
    /// if no savepoint with that name exists.
    pub fn release_savepoint(
        &self,
        txn: &mut Transaction,
        name: &str,
    ) -> Result<Xid, SavepointError> {
        let sub_xid = txn.subtxn_stack.release(name)?;
        // RELEASE must NOT publish the subtransaction as independently
        // committed. PostgreSQL merges a released subxact into its parent;
        // its effective commit is gated on the parent. We therefore leave
        // the subxid `InProgress` in the CLOG and in the `in_progress`
        // mirror, so a *foreign* backend either samples it into its `xip`
        // (invisible) or resolves it through the `subxid_parent` map to the
        // still-in-progress parent (also invisible). Flipping it `Committed`
        // here was a cross-transaction dirty read: another backend saw the
        // released-but-uncommitted row, and if the parent later aborted the
        // subxid stayed `Committed` forever.
        //
        // The owning backend keeps treating the released subxid as *self*
        // via `merged_up` → `own_live_subxids`, so its own reads still see
        // the row. At top-level commit/abort the subxid is folded with the
        // parent.
        txn.subtxn_stack.record_merged_up(sub_xid);
        self.sync_snapshot_subxids(txn);
        Ok(sub_xid)
    }

    // ---- 2PC helper --------------------------------------------------------

    /// Consume `txn` into the two-phase-commit coordinator.
    ///
    /// Records the XID under `gid` in `coordinator`, leaving the XID in the
    /// CLOG as `InProgress` until the coordinator resolves it with
    /// `commit_prepared` or `rollback_prepared`.
    ///
    /// The `Transaction` handle is consumed so it cannot be double-committed
    /// via the normal path.
    ///
    /// Returns [`crate::two_phase::TwoPhaseError`] if the GID is a duplicate
    /// or if state-file I/O fails.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "by-value enforces the at-most-once lifecycle invariant: prepare consumes the Transaction handle"
    )]
    pub fn prepare_transaction(
        &self,
        gid: &str,
        txn: Transaction,
        coordinator: &crate::two_phase::TwoPhaseCoordinator,
    ) -> Result<(), crate::two_phase::TwoPhaseError> {
        coordinator.prepare(gid, txn.xid)
        // `txn` is dropped here; the CLOG entry remains `InProgress` until
        // the coordinator resolves via `commit_prepared` / `rollback_prepared`.
    }

    fn terminate(&self, xid: Xid, new_status: XidStatus) -> Result<(), TxnError> {
        // Use a single shard-locked mutation: look up the entry mutably
        // and validate that it is still `InProgress` before flipping
        // it. This makes commit / abort idempotent under contention —
        // exactly one caller observes `InProgress` and wins the
        // transition.
        let Some(mut entry) = self.clog.get_mut(&xid) else {
            return Err(TxnError::Unknown { xid });
        };
        match *entry.value() {
            XidStatus::InProgress => {
                *entry.value_mut() = new_status;
                // Drop the shard lock before touching `in_progress`
                // to keep lock order: clog → in_progress.
                drop(entry);
                self.in_progress.lock().remove(&xid);
                Ok(())
            }
            other => Err(TxnError::AlreadyTerminated { xid, status: other }),
        }
    }

    /// Force-set the CLOG entry for `xid` to `Aborted`, regardless of the
    /// current status.
    ///
    /// Used exclusively by the SSI commit path to roll back a transaction
    /// that was optimistically flipped to `Committed` but subsequently
    /// found to be the pivot of a dangerous structure.  Callers must hold
    /// the SSI manager's entry for `xid` while invoking this to prevent
    /// concurrent observers from seeing a partially-committed state.
    fn force_abort(&self, xid: Xid) {
        if let Some(mut entry) = self.clog.get_mut(&xid) {
            *entry.value_mut() = XidStatus::Aborted;
            drop(entry);
            self.in_progress.lock().remove(&xid);
        }
    }

    /// Finalise a previously-prepared transaction by stamping its
    /// CLOG entry with `final_status` (must be
    /// [`XidStatus::Committed`] or [`XidStatus::Aborted`]).
    ///
    /// Used by the 2PC phase-2 path
    /// (`COMMIT PREPARED 'gid'` / `ROLLBACK PREPARED 'gid'`) after
    /// the [`crate::two_phase::TwoPhaseCoordinator`] has already
    /// removed the on-disk state file and returned the prepared
    /// `xid`. The transaction has remained `InProgress` in the
    /// CLOG since [`Self::prepare_transaction`] consumed the
    /// `Transaction` handle without flipping status; this method
    /// closes the loop.
    pub fn finalise_prepared(&self, xid: Xid, final_status: XidStatus) -> Result<(), TxnError> {
        debug_assert!(matches!(
            final_status,
            XidStatus::Committed | XidStatus::Aborted
        ));
        self.terminate(xid, final_status)?;
        self.lock_manager.release_all(xid);
        Ok(())
    }

    /// Validate that a prepared XID is still open for phase-2 resolution.
    pub fn validate_prepared(&self, xid: Xid) -> Result<(), TxnError> {
        let Some(entry) = self.clog.get(&xid) else {
            return Err(TxnError::Unknown { xid });
        };
        match *entry.value() {
            XidStatus::InProgress => Ok(()),
            status => Err(TxnError::AlreadyTerminated { xid, status }),
        }
    }

    /// Restore a committed XID observed during WAL recovery.
    pub fn recover_committed(&self, xid: Xid) {
        if xid == Xid::INVALID || xid == Xid::FROZEN || xid == Xid::BOOTSTRAP {
            return;
        }
        self.clog.insert(xid, XidStatus::Committed);
        self.in_progress.lock().remove(&xid);
        self.recover_observed_xid(xid);
    }

    /// Restore an aborted XID observed during WAL recovery.
    pub fn recover_aborted(&self, xid: Xid) {
        if xid == Xid::INVALID || xid == Xid::FROZEN || xid == Xid::BOOTSTRAP {
            return;
        }
        self.clog.insert(xid, XidStatus::Aborted);
        self.in_progress.lock().remove(&xid);
        self.recover_observed_xid(xid);
    }

    /// Mark a WAL-observed XID without a terminal record as aborted.
    ///
    /// Crash recovery reaches this after replaying the WAL and restoring any
    /// prepared transaction state. An XID that already has a CLOG entry may be
    /// committed, aborted, or prepared-in-progress and is left unchanged.
    pub fn recover_uncommitted_as_aborted(&self, xid: Xid) {
        if xid == Xid::INVALID || xid == Xid::FROZEN || xid == Xid::BOOTSTRAP {
            return;
        }
        if self.clog.get(&xid).is_none() {
            self.clog.insert(xid, XidStatus::Aborted);
            self.in_progress.lock().remove(&xid);
        }
        self.recover_observed_xid(xid);
    }

    /// Restore an in-progress prepared XID observed in 2PC state.
    ///
    /// WAL recovery runs before 2PC state recovery. If WAL has already marked
    /// the XID terminal, the state file is inconsistent and startup must fail
    /// rather than re-open a resolved transaction.
    pub fn recover_prepared(&self, xid: Xid) -> Result<(), TxnError> {
        if xid == Xid::INVALID || xid == Xid::FROZEN || xid == Xid::BOOTSTRAP {
            return Ok(());
        }

        if let Some(entry) = self.clog.get(&xid) {
            match *entry.value() {
                XidStatus::InProgress => {}
                status => return Err(TxnError::AlreadyTerminated { xid, status }),
            }
        } else {
            self.clog.insert(xid, XidStatus::InProgress);
        }
        self.in_progress.lock().insert(xid);
        self.recover_observed_xid(xid);
        Ok(())
    }

    /// Advance the allocator past an XID observed during WAL recovery.
    pub fn recover_observed_xid(&self, xid: Xid) {
        if xid == Xid::INVALID || xid == Xid::FROZEN || xid == Xid::BOOTSTRAP {
            return;
        }
        let next = xid.raw().saturating_add(1);
        let _ = self
            .next_xid
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < next).then_some(next)
            });
    }

    /// Export the commit log for a durable checkpoint snapshot.
    ///
    /// Returns the allocator's `next_xid` and every **terminal**
    /// `(xid, status)` entry (`Committed` / `Aborted` / `Frozen`). In-progress
    /// (including prepared 2PC) transactions are intentionally excluded: their
    /// status is restored from their WAL and 2PC state, whose records are
    /// retained until the transaction resolves. Persisting this snapshot lets
    /// the `Commit`/`Abort` WAL records below a checkpoint be recycled without
    /// losing the status of transactions that resolved before it.
    #[must_use]
    pub fn export_clog(&self) -> (u64, Vec<(Xid, XidStatus)>) {
        let next_xid = self.next_xid.load(Ordering::Acquire);
        let entries: Vec<(Xid, XidStatus)> = self
            .clog
            .iter()
            .filter_map(|entry| match *entry.value() {
                status @ (XidStatus::Committed | XidStatus::Aborted | XidStatus::Frozen) => {
                    Some((*entry.key(), status))
                }
                XidStatus::InProgress => None,
            })
            .collect();
        (next_xid, entries)
    }

    /// Seed the commit log from a snapshot produced by [`Self::export_clog`].
    ///
    /// Used at restart to restore the status of transactions whose `Commit` /
    /// `Abort` WAL records were recycled. An XID that already has a CLOG entry
    /// (e.g. re-derived from retained WAL) is left unchanged, mirroring the
    /// `recover_*` idempotency; the allocator is advanced past `next_xid` and
    /// every imported XID.
    pub fn import_clog(&self, next_xid: u64, entries: &[(Xid, XidStatus)]) {
        for (xid, status) in entries {
            if *xid == Xid::INVALID || *xid == Xid::FROZEN || *xid == Xid::BOOTSTRAP {
                continue;
            }
            // Seed only terminal statuses, and only when the XID is not already
            // resolved (retained WAL is authoritative — `import_clog` runs before
            // the WAL commit-status scan, so this seeds the truncated tail only).
            if matches!(
                status,
                XidStatus::Committed | XidStatus::Aborted | XidStatus::Frozen
            ) && self.clog.get(xid).is_none()
            {
                self.clog.insert(*xid, *status);
                self.in_progress.lock().remove(xid);
            }
            self.recover_observed_xid(*xid);
        }
        let _ = self
            .next_xid
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < next_xid).then_some(next_xid)
            });
    }

    // ── SSI pass-through methods ─────────────────────────────────────────────

    /// Record a predicate lock for a serializable transaction.
    ///
    /// Pass-through to [`SsiManager::add_predicate_lock`]. No-ops when no
    /// [`SsiManager`] is installed or when `xid` was not begun at
    /// [`IsolationLevel::Serializable`].
    pub fn record_predicate_lock(&self, xid: Xid, tag: PredicateLockTag) {
        if let Some(ssi) = &self.ssi {
            ssi.add_predicate_lock(xid, tag);
        }
    }

    /// Record an rw-anti-dependency edge from `reader` to `writer`.
    ///
    /// Pass-through to [`SsiManager::record_rw_conflict`]. No-ops when no
    /// [`SsiManager`] is installed.
    pub fn record_rw_conflict(&self, reader: Xid, writer: Xid) {
        if let Some(ssi) = &self.ssi {
            ssi.record_rw_conflict(reader, writer);
        }
    }

    /// Record rw-anti-dependencies caused by `writer` modifying `tag`.
    ///
    /// Pass-through to [`SsiManager::record_write_conflicts`]. No-ops when no
    /// [`SsiManager`] is installed. The returned XIDs are serializable readers
    /// whose predicate locks covered the write target.
    pub fn record_write_conflicts(&self, writer: Xid, tag: &PredicateLockTag) -> Vec<Xid> {
        self.ssi
            .as_ref()
            .map_or_else(Vec::new, |ssi| ssi.record_write_conflicts(writer, tag))
    }
}

impl XidStatusOracle for TransactionManager {
    fn status(&self, xid: Xid) -> XidStatus {
        // Sentinels first — they never appear in the CLOG.
        if xid == Xid::FROZEN {
            return XidStatus::Frozen;
        }
        // Bootstrap is treated as committed: tuples written during
        // catalog bootstrap are always visible.
        if xid == Xid::BOOTSTRAP {
            return XidStatus::Committed;
        }
        let own = self
            .clog
            .get(&xid)
            .map_or(XidStatus::InProgress, |entry| *entry.value());
        // Subtransaction resolution (PostgreSQL's
        // `SubTransGetTopmostTransaction` + `TransactionIdDidCommit`): while a
        // subxid's own CLOG entry is still `InProgress`, its effective fate is
        // its parent's. This keeps a RELEASEd-but-parent-still-open subxid
        // invisible to other backends (the parent is in progress) and makes
        // the parent's single commit/abort transition the only observable
        // boundary. Once the subxid is folded at parent resolution its own
        // entry is terminal and is returned directly (no map lookup).
        if matches!(own, XidStatus::InProgress)
            && let Some(parent) = self.subxid_parent.get(&xid)
        {
            let parent = *parent.value();
            // One level of indirection suffices: savepoints always map
            // directly to the top-level parent (`begin_savepoint` records
            // `txn.xid`), never to another subxid.
            return self
                .clog
                .get(&parent)
                .map_or(XidStatus::InProgress, |entry| *entry.value());
        }
        own
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::thread;

    use super::*;

    #[test]
    fn begin_returns_first_user_xid() {
        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(t.xid, Xid::FIRST_USER);
        assert_eq!(t.isolation, IsolationLevel::ReadCommitted);
        assert_eq!(t.current_command, CommandId::FIRST);
        assert_eq!(t.start_lsn, Lsn::ZERO);
    }

    #[test]
    fn subsequent_begins_return_increasing_xids() {
        let mgr = TransactionManager::new();
        let a = mgr.begin(IsolationLevel::ReadCommitted);
        let b = mgr.begin(IsolationLevel::ReadCommitted);
        let c = mgr.begin(IsolationLevel::ReadCommitted);
        assert!(a.xid < b.xid);
        assert!(b.xid < c.xid);
        assert_eq!(b.xid.raw(), a.xid.raw() + 1);
        assert_eq!(c.xid.raw(), b.xid.raw() + 1);
    }

    #[test]
    fn snapshot_under_third_sees_first_two_in_xip() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::RepeatableRead);
        let t2 = mgr.begin(IsolationLevel::RepeatableRead);
        let t3 = mgr.begin(IsolationLevel::RepeatableRead);

        // t3's snapshot should list t1 and t2 as active.
        let xips: Vec<Xid> = t3.snapshot.xip().to_vec();
        assert_eq!(xips, vec![t1.xid, t2.xid]);

        // xmin is the oldest in-progress, which is t1.
        assert_eq!(t3.snapshot.xmin, t1.xid);
        // xmax is one past the highest XID handed out — i.e. the next
        // XID the counter would assign, which is t3.xid + 1.
        assert_eq!(t3.snapshot.xmax.raw(), t3.xid.raw() + 1);
        // The transaction's own XID is not in its own xip.
        assert!(!t3.snapshot.xip().contains(&t3.xid));
    }

    #[test]
    fn snapshot_after_commit_omits_committed_xid_and_oracle_reports_committed() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::ReadCommitted);
        let t1_xid = t1.xid;
        mgr.commit(t1).unwrap();

        let t4 = mgr.begin(IsolationLevel::ReadCommitted);
        assert!(!t4.snapshot.xip().contains(&t1_xid));

        // Oracle should now report t1 as Committed.
        assert_eq!(mgr.status(t1_xid), XidStatus::Committed);
        assert!(mgr.is_committed(t1_xid));
    }

    #[test]
    fn abort_marks_aborted_in_oracle() {
        let mgr = TransactionManager::new();
        let t2 = mgr.begin(IsolationLevel::ReadCommitted);
        let t2_xid = t2.xid;
        mgr.abort(t2).unwrap();

        assert_eq!(mgr.status(t2_xid), XidStatus::Aborted);
        assert!(mgr.is_aborted(t2_xid));
        assert!(!mgr.is_committed(t2_xid));
    }

    #[test]
    fn recover_aborted_restores_terminal_status_and_advances_allocator() {
        let mgr = TransactionManager::new();
        let xid = Xid::new(10);

        mgr.recover_aborted(xid);
        let next = mgr.begin(IsolationLevel::ReadCommitted);

        assert_eq!(mgr.status(xid), XidStatus::Aborted);
        assert_eq!(next.xid, Xid::new(11));
        assert!(!next.snapshot.xip().contains(&xid));
    }

    #[test]
    fn clog_export_import_round_trips_terminal_status() {
        let src = TransactionManager::new();
        src.recover_committed(Xid::new(10));
        src.recover_committed(Xid::new(11));
        src.recover_aborted(Xid::new(12));
        // An in-progress transaction must not be exported.
        let live = src.begin(IsolationLevel::ReadCommitted);
        let live_xid = live.xid;

        let (next_xid, entries) = src.export_clog();
        assert_eq!(entries.len(), 3, "only terminal entries are exported");
        assert!(
            !entries.iter().any(|(x, _)| *x == live_xid),
            "in-progress xid must not be exported"
        );

        // Import into a fresh manager restores statuses and the allocator.
        let dst = TransactionManager::new();
        dst.import_clog(next_xid, &entries);
        assert_eq!(dst.status(Xid::new(10)), XidStatus::Committed);
        assert_eq!(dst.status(Xid::new(11)), XidStatus::Committed);
        assert_eq!(dst.status(Xid::new(12)), XidStatus::Aborted);
        assert!(
            dst.begin(IsolationLevel::ReadCommitted).xid.raw() >= next_xid,
            "allocator must never reissue an imported xid"
        );

        // Retained WAL is authoritative: an entry already resolved before import
        // is not overwritten by the snapshot.
        let dst2 = TransactionManager::new();
        dst2.recover_aborted(Xid::new(10));
        dst2.import_clog(next_xid, &entries);
        assert_eq!(
            dst2.status(Xid::new(10)),
            XidStatus::Aborted,
            "import must not override a status already re-derived from WAL"
        );
    }

    #[test]
    fn read_committed_refresh_replaces_snapshot() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::ReadCommitted);
        let t1_xid = t1.xid;

        let mut reader = mgr.begin(IsolationLevel::ReadCommitted);
        let snap_before = reader.snapshot.clone();
        // Before refresh, t1 is in reader's xip.
        assert!(reader.snapshot.xip().contains(&t1_xid));
        let cmd_before = reader.current_command;

        // Commit t1 in between.
        mgr.commit(t1).unwrap();

        mgr.refresh_snapshot(&mut reader);

        // Command id advanced.
        assert_eq!(reader.current_command, cmd_before.next());
        // Snapshot changed — t1 is no longer in xip.
        assert!(!reader.snapshot.xip().contains(&t1_xid));
        assert_ne!(reader.snapshot, snap_before);
    }

    #[test]
    fn repeatable_read_refresh_keeps_snapshot() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::ReadCommitted);
        let t1_xid = t1.xid;

        let mut reader = mgr.begin(IsolationLevel::RepeatableRead);
        let snap_xip_before: Vec<Xid> = reader.snapshot.xip().to_vec();
        let xmin_before = reader.snapshot.xmin;
        let xmax_before = reader.snapshot.xmax;

        mgr.commit(t1).unwrap();
        mgr.refresh_snapshot(&mut reader);

        // The xip / xmin / xmax must not have changed under RR.
        let snap_xip_after: Vec<Xid> = reader.snapshot.xip().to_vec();
        assert_eq!(snap_xip_after, snap_xip_before);
        assert_eq!(reader.snapshot.xmin, xmin_before);
        assert_eq!(reader.snapshot.xmax, xmax_before);
        // t1 still considered active by reader's frozen snapshot.
        assert!(reader.snapshot.xip().contains(&t1_xid));
        // current_command still advances.
        assert_eq!(reader.current_command, CommandId::FIRST.next());
        // And so does the snapshot's view of it.
        assert_eq!(reader.snapshot.current_command, reader.current_command);
    }

    #[test]
    fn serializable_refresh_keeps_snapshot_like_rr() {
        // v0.4: Serializable uses a fixed snapshot (same as RepeatableRead)
        // combined with SSI conflict tracking via SsiManager.
        let mgr = TransactionManager::new();
        let _t1 = mgr.begin(IsolationLevel::ReadCommitted);

        let mut reader = mgr.begin(IsolationLevel::Serializable);
        let snap_before = reader.snapshot.clone();
        mgr.refresh_snapshot(&mut reader);
        // xip / xmin / xmax unchanged.
        assert_eq!(reader.snapshot.xip(), snap_before.xip());
        assert_eq!(reader.snapshot.xmin, snap_before.xmin);
        assert_eq!(reader.snapshot.xmax, snap_before.xmax);
        // command id bumped on the snapshot too.
        assert_eq!(
            reader.snapshot.current_command,
            snap_before.current_command.next()
        );
    }

    #[test]
    fn oldest_in_progress_advances_when_oldest_commits() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::ReadCommitted);
        let t2 = mgr.begin(IsolationLevel::ReadCommitted);
        let _t3 = mgr.begin(IsolationLevel::ReadCommitted);
        let _t4 = mgr.begin(IsolationLevel::ReadCommitted);
        let _t5 = mgr.begin(IsolationLevel::ReadCommitted);

        // Five in progress: oldest is t1.
        assert_eq!(mgr.oldest_in_progress(), t1.xid);

        // Commit t2. t1 is still oldest in progress.
        let t2_xid = t2.xid;
        mgr.commit(t2).unwrap();
        assert_eq!(mgr.oldest_in_progress(), t1.xid);
        // Sanity: t2 itself is now committed in the oracle.
        assert_eq!(mgr.status(t2_xid), XidStatus::Committed);
    }

    #[test]
    fn is_in_progress_tracks_begin_and_commit() {
        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let xid = t.xid;
        assert!(
            mgr.is_in_progress(xid),
            "a freshly begun xid is in progress"
        );
        mgr.commit(t).unwrap();
        assert!(
            !mgr.is_in_progress(xid),
            "a committed xid is no longer in progress"
        );
    }

    #[test]
    fn oldest_in_progress_is_next_xid_when_idle() {
        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let t_xid = t.xid;
        mgr.commit(t).unwrap();
        // No one is in progress; the value reported should be the next
        // XID the allocator would hand out — one past the highest XID
        // assigned so far.
        let oldest = mgr.oldest_in_progress();
        assert_eq!(oldest.raw(), t_xid.raw() + 1);
    }

    #[test]
    fn next_xid_reports_allocator_high_water() {
        let mgr = TransactionManager::new();
        assert_eq!(mgr.next_xid(), Xid::FIRST_USER);

        let t1 = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(mgr.next_xid().raw(), t1.xid.raw() + 1);

        let t2 = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(mgr.next_xid().raw(), t2.xid.raw() + 1);
    }

    #[test]
    fn oracle_reports_inprogress_for_unknown_xids_and_handles_sentinels() {
        let mgr = TransactionManager::new();
        // Frozen sentinel:
        assert_eq!(mgr.status(Xid::FROZEN), XidStatus::Frozen);
        // Bootstrap is treated as committed:
        assert_eq!(mgr.status(Xid::BOOTSTRAP), XidStatus::Committed);
        // An XID we never allocated falls back to InProgress per the
        // oracle's contract.
        assert_eq!(mgr.status(Xid::new(99_999)), XidStatus::InProgress);
    }

    #[test]
    fn oracle_is_consistent_with_lifecycle_for_a_live_transaction() {
        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let xid = t.xid;
        // Begin: in progress.
        assert_eq!(mgr.status(xid), XidStatus::InProgress);
        assert!(mgr.is_in_progress(xid));
        // Commit: committed.
        mgr.commit(t).unwrap();
        assert_eq!(mgr.status(xid), XidStatus::Committed);
        assert!(mgr.is_committed(xid));
        assert!(!mgr.is_aborted(xid));
    }

    #[test]
    fn double_commit_is_rejected_as_already_terminated() {
        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let xid = t.xid;
        let dup = t.clone();
        mgr.commit(t).unwrap();
        let err = mgr.commit(dup).unwrap_err();
        let TxnError::AlreadyTerminated { xid: e_xid, status } = err else {
            panic!("unexpected error: {err:?}");
        };
        assert_eq!(e_xid, xid);
        assert_eq!(status, XidStatus::Committed);
    }

    #[test]
    fn commit_then_abort_is_rejected() {
        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let dup = t.clone();
        mgr.commit(t).unwrap();
        assert!(matches!(
            mgr.abort(dup),
            Err(TxnError::AlreadyTerminated { .. })
        ));
    }

    #[test]
    fn concurrent_begin_produces_distinct_xids() {
        const N_THREADS: usize = 16;
        const PER_THREAD: usize = 64;

        let mgr = Arc::new(TransactionManager::new());
        let started = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..N_THREADS)
            .map(|_| {
                let mgr = Arc::clone(&mgr);
                let started = Arc::clone(&started);
                thread::spawn(move || {
                    // Spin briefly so threads start their begin loops
                    // close together and maximise contention.
                    started.fetch_add(1, AtomicOrdering::Relaxed);
                    while started.load(AtomicOrdering::Relaxed) < N_THREADS {
                        std::hint::spin_loop();
                    }
                    let mut local = Vec::with_capacity(PER_THREAD);
                    for _ in 0..PER_THREAD {
                        let t = mgr.begin(IsolationLevel::ReadCommitted);
                        local.push(t.xid);
                    }
                    local
                })
            })
            .collect();

        let mut all = Vec::with_capacity(N_THREADS * PER_THREAD);
        for h in handles {
            let xids = h.join().expect("thread panicked");
            all.extend(xids);
        }

        // All XIDs are unique.
        let mut sorted = all.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), N_THREADS * PER_THREAD);

        // And they form a contiguous range starting at FIRST_USER.
        let first = Xid::FIRST_USER.raw();
        let generated = u64::try_from(N_THREADS * PER_THREAD).expect("xid count fits u64");
        let last = first + generated - 1;
        let min = sorted.first().copied().expect("non-empty");
        let max = sorted.last().copied().expect("non-empty");
        assert_eq!(min.raw(), first);
        assert_eq!(max.raw(), last);
    }

    // ── SSI integration ────────────────────────────────────────────────────

    #[test]
    fn serializable_with_ssi_no_conflict_commits_cleanly() {
        let ssi = Arc::new(crate::ssi::SsiManager::new());
        let mgr = TransactionManager::new_with_ssi(Arc::clone(&ssi));

        let t1 = mgr.begin(IsolationLevel::Serializable);
        let t2 = mgr.begin(IsolationLevel::Serializable);

        // No rw-conflict edges → both must commit.
        mgr.commit(t1).unwrap();
        mgr.commit(t2).unwrap();
    }

    #[test]
    fn serializable_with_ssi_pivot_fails_with_serialization_failure() {
        let ssi = Arc::new(crate::ssi::SsiManager::new());
        let mgr = TransactionManager::new_with_ssi(Arc::clone(&ssi));

        let t1 = mgr.begin(IsolationLevel::Serializable);
        let t2 = mgr.begin(IsolationLevel::Serializable);
        let t3 = mgr.begin(IsolationLevel::Serializable);
        let t2_xid = t2.xid;
        let t3_xid = t3.xid;

        // Build T1 --rw--> T2 --rw--> T3 (T2 is pivot).
        mgr.record_rw_conflict(t1.xid, t2.xid);
        mgr.record_rw_conflict(t2.xid, t3.xid);

        // T1 commits first — marks one leg as committed.
        mgr.commit(t1).unwrap();

        // T2 is the pivot → must fail.
        let err = mgr.commit(t2).expect_err("T2 (pivot) must fail");
        assert!(
            matches!(err, TxnError::SerializationFailure { .. }),
            "expected SerializationFailure, got {err:?}"
        );

        // After T2's commit fails, its CLOG entry must be Aborted.
        assert_eq!(mgr.status(t2_xid), XidStatus::Aborted);

        // T3 has no conflict-in so it commits cleanly.
        mgr.commit(t3).unwrap();
        assert_eq!(mgr.status(t3_xid), XidStatus::Committed);
    }

    #[test]
    fn commit_releases_all_locks_on_commit() {
        use crate::lock::{LockMode, LockRequest, LockTag};
        use ultrasql_core::RelationId;

        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let xid = t.xid;

        let tag = LockTag::Relation(RelationId::new(1));
        mgr.lock_manager
            .acquire(LockRequest {
                xid,
                tag,
                mode: LockMode::Exclusive,
            })
            .unwrap();

        // Lock must be held.
        let snap = mgr.lock_manager.inspect(tag).expect("entry must exist");
        assert!(snap.grants.iter().any(|(x, _)| *x == xid));

        // Commit — must release all locks.
        mgr.commit(t).unwrap();

        // Entry pruned (no grants, no waiters).
        assert!(
            mgr.lock_manager.inspect(tag).is_none(),
            "lock must be released on commit"
        );
    }

    #[test]
    fn abort_releases_all_locks_on_abort() {
        use crate::lock::{LockMode, LockRequest, LockTag};
        use ultrasql_core::RelationId;

        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let xid = t.xid;

        let tag = LockTag::Relation(RelationId::new(2));
        mgr.lock_manager
            .acquire(LockRequest {
                xid,
                tag,
                mode: LockMode::Share,
            })
            .unwrap();

        // Abort — must release all locks.
        mgr.abort(t).unwrap();

        assert!(
            mgr.lock_manager.inspect(tag).is_none(),
            "lock must be released on abort"
        );
    }

    #[test]
    fn savepoint_rollback_records_rolled_back_subxid() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);

        let sp = mgr.begin_savepoint(&mut t, "sp1");
        let sub_xid = sp.xid;

        // Roll back to "sp1" — sub_xid should be marked rolled back.
        let aborted = mgr.rollback_to_savepoint(&mut t, "sp1").unwrap();
        assert!(aborted.contains(&sub_xid));
        assert!(
            t.subtxn_stack.is_rolled_back(sub_xid),
            "sub_xid must be in the rolled-back set after rollback_to_savepoint"
        );

        mgr.commit(t).unwrap();
    }

    // ── subtransaction snapshot / fold-up integration (§7 B9–B11) ─────────────

    /// B9: `build_snapshot` (via begin_savepoint, RC) excludes own live
    /// subxids from `xip` and does not drag `xmin` down to a subxid; the
    /// snapshot carries the subxid as *self*.
    #[test]
    fn build_snapshot_excludes_own_live_subxids_from_xip() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);

        let sp = mgr.begin_savepoint(&mut t, "sp1");
        let sub_xid = sp.xid;
        // Advance to a new statement so the RC snapshot is rebuilt with
        // the savepoint context.
        mgr.refresh_snapshot(&mut t);

        // The own subxid must NOT be in xip (it is self, not foreign).
        assert!(
            !t.snapshot.xip().contains(&sub_xid),
            "own live subxid must be excluded from xip"
        );
        // xmin must not have been dragged down to the subxid.
        assert!(
            t.snapshot.xmin > sub_xid || t.snapshot.xmin == t.snapshot.xmax,
            "xmin must not be the own subxid"
        );
        // The subxid is carried as self.
        assert!(t.snapshot.is_current_xid(sub_xid));
        assert!(t.snapshot.own_live_subxids().contains(&sub_xid));

        mgr.commit(t).unwrap();
    }

    /// B10: after `rollback_to_savepoint` the subxid is `Aborted`, removed
    /// from `in_progress`, in the rolled-back set, and the snapshot flags
    /// it rolled-back.
    #[test]
    fn rollback_to_savepoint_marks_aborted_and_patches_snapshot() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);

        let sp = mgr.begin_savepoint(&mut t, "sp1");
        let sub_xid = sp.xid;
        mgr.rollback_to_savepoint(&mut t, "sp1").unwrap();

        assert_eq!(mgr.status(sub_xid), XidStatus::Aborted);
        assert!(!mgr.is_in_progress(sub_xid));
        assert!(t.subtxn_stack.is_rolled_back(sub_xid));
        // The frozen-snapshot patch (also applied under RC) flags it.
        assert!(
            t.snapshot.own_subxid_rolled_back(sub_xid),
            "snapshot must flag the rolled-back subxid"
        );
        assert!(!t.snapshot.is_current_xid(sub_xid));

        mgr.commit(t).unwrap();
    }

    /// B11: `commit` folds live + merged-up subxids to `Committed`;
    /// `abort` folds them to `Aborted`. A rolled-back subxid stays
    /// `Aborted` through a parent commit.
    #[test]
    fn commit_and_abort_fold_subxids() {
        // Commit path: one live subxid + one released (merged-up) subxid
        // both become Committed; a rolled-back one stays Aborted.
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);
        let parent = t.xid;
        let live = mgr.begin_savepoint(&mut t, "live").xid;
        let released = mgr.begin_savepoint(&mut t, "rel").xid;
        mgr.release_savepoint(&mut t, "rel").unwrap();
        let rolled = mgr.begin_savepoint(&mut t, "rb").xid;
        mgr.rollback_to_savepoint(&mut t, "rb").unwrap();

        // Pre-commit, parent still open: live and released both resolve via
        // the subxid → parent map to the parent's InProgress status. RELEASE
        // must NOT publish the subxid as independently committed (that was a
        // cross-transaction dirty read). The rolled-back subxid is Aborted.
        assert_eq!(mgr.status(live), XidStatus::InProgress);
        assert_eq!(
            mgr.status(released),
            XidStatus::InProgress,
            "a released-but-parent-open subxid resolves to the parent (still in progress)",
        );
        assert_eq!(mgr.status(parent), XidStatus::InProgress);
        assert_eq!(mgr.status(rolled), XidStatus::Aborted);

        mgr.commit(t).unwrap();
        assert_eq!(mgr.status(live), XidStatus::Committed, "live folded up");
        assert_eq!(
            mgr.status(released),
            XidStatus::Committed,
            "released subxid becomes committed once the parent commits",
        );
        assert_eq!(
            mgr.status(rolled),
            XidStatus::Aborted,
            "rolled-back subxid stays aborted across parent commit"
        );
        assert!(!mgr.is_in_progress(live));
        assert!(!mgr.is_in_progress(released));

        // Abort path: a live subxid AND a released-but-parent-open subxid are
        // both folded to Aborted, so a released-then-parent-aborted
        // subtransaction's writes vanish for everyone.
        let mgr2 = TransactionManager::new();
        let mut t2 = mgr2.begin(IsolationLevel::ReadCommitted);
        let live2 = mgr2.begin_savepoint(&mut t2, "live2").xid;
        let released2 = mgr2.begin_savepoint(&mut t2, "rel2").xid;
        mgr2.release_savepoint(&mut t2, "rel2").unwrap();
        assert_eq!(mgr2.status(live2), XidStatus::InProgress);
        assert_eq!(mgr2.status(released2), XidStatus::InProgress);
        mgr2.abort(t2).unwrap();
        assert_eq!(mgr2.status(live2), XidStatus::Aborted, "live folded down");
        assert_eq!(
            mgr2.status(released2),
            XidStatus::Aborted,
            "released subxid is aborted when the parent aborts (no leak)",
        );
        assert!(!mgr2.is_in_progress(live2));
        assert!(!mgr2.is_in_progress(released2));
    }

    /// A released-but-parent-open subxid resolves to its parent's status
    /// for a *foreign* observer (the oracle), so RELEASE cannot leak its
    /// writes across transactions. Once the parent aborts, the released
    /// subxid reads Aborted (it never independently committed).
    #[test]
    fn release_does_not_publish_subxid_until_parent_resolves() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);
        let released = mgr.begin_savepoint(&mut t, "s1").xid;
        mgr.release_savepoint(&mut t, "s1").unwrap();

        // Foreign observer: the subxid is still in progress (parent open).
        assert_eq!(mgr.status(released), XidStatus::InProgress);
        assert!(
            mgr.is_in_progress(released),
            "released subxid stays in the in-progress mirror while the parent is open",
        );

        // Parent aborts → the released subxid is folded to Aborted, never
        // having been independently committed.
        mgr.abort(t).unwrap();
        assert_eq!(
            mgr.status(released),
            XidStatus::Aborted,
            "released subxid follows the parent to Aborted (no cross-txn leak)",
        );
    }

    /// ROLLBACK TO an outer savepoint must discard subtransactions started
    /// after it even if they were already RELEASEd — `merged_up` is pruned.
    #[test]
    fn rollback_to_outer_discards_released_inner_subxid() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);
        let outer = mgr.begin_savepoint(&mut t, "outer").xid;
        let inner = mgr.begin_savepoint(&mut t, "inner").xid;
        mgr.release_savepoint(&mut t, "inner").unwrap();

        // ROLLBACK TO outer drains the stack down to (and including) outer
        // and prunes the merged-up inner subxid, marking both aborted.
        let aborted = mgr.rollback_to_savepoint(&mut t, "outer").unwrap();
        assert!(aborted.contains(&outer));
        assert!(
            aborted.contains(&inner),
            "released inner subxid must be discarded by ROLLBACK TO outer",
        );
        assert_eq!(mgr.status(inner), XidStatus::Aborted);
        assert_eq!(mgr.status(outer), XidStatus::Aborted);
        assert!(
            !t.subtxn_stack.merged_up_sorted().contains(&inner),
            "inner must be pruned from merged_up so it is never folded committed",
        );

        // After commit the discarded inner subxid stays aborted.
        mgr.commit(t).unwrap();
        assert_eq!(mgr.status(inner), XidStatus::Aborted);
    }

    /// RR/SSI: savepoint control patches the frozen snapshot's subxid sets
    /// in place without disturbing `xmin` / `xmax` / `xip`.
    #[test]
    fn rr_savepoint_patches_frozen_snapshot_without_disturbing_it() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::RepeatableRead);
        let xmin = t.snapshot.xmin;
        let xmax = t.snapshot.xmax;
        let xip = t.snapshot.xip().to_vec();

        let sub = mgr.begin_savepoint(&mut t, "sp1").xid;
        // Frozen fields untouched.
        assert_eq!(t.snapshot.xmin, xmin);
        assert_eq!(t.snapshot.xmax, xmax);
        assert_eq!(t.snapshot.xip().to_vec(), xip);
        // Subxid carried as self.
        assert!(t.snapshot.is_current_xid(sub));

        // Rolling back patches the rolled-back set, still no xip change.
        mgr.rollback_to_savepoint(&mut t, "sp1").unwrap();
        assert_eq!(t.snapshot.xmin, xmin);
        assert_eq!(t.snapshot.xmax, xmax);
        assert_eq!(t.snapshot.xip().to_vec(), xip);
        assert!(t.snapshot.own_subxid_rolled_back(sub));
        assert!(!t.snapshot.is_current_xid(sub));

        mgr.commit(t).unwrap();
    }
}
