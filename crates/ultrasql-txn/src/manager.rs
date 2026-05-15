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

/// Isolation level applied to a [`Transaction`].
///
/// v0.5 implements snapshot semantics for [`Self::ReadCommitted`] and
/// [`Self::RepeatableRead`]. [`Self::Serializable`] currently uses the
/// same snapshot strategy as [`Self::RepeatableRead`]; full predicate
/// locking is tracked as an RFC follow-up. The enum value still carries
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
    /// True Serializable Snapshot Isolation (SSI).
    ///
    /// Uses the same fixed snapshot strategy as [`Self::RepeatableRead`]
    /// for reads, and additionally registers the transaction with
    /// [`SsiManager`] to track rw-anti-dependency edges. On commit,
    /// the SSI manager checks for dangerous structures (T1 → T2 → T3
    /// cycles); if a cycle is found, the commit fails with
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
            ssi: None,
            lock_manager: Arc::new(LockManager::new()),
        }
    }

    /// Construct a manager with full SSI support.
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

        // 3. Sample the active transactions and the high-water XID.
        let snapshot = self.build_snapshot(xid, CommandId::FIRST);

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
                txn.snapshot = self.build_snapshot(txn.xid, txn.current_command);
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Snapshot stays. Keep `current_xid` / `current_command`
                // coherent inside the existing snapshot so own-write
                // visibility advances with the statement counter.
                txn.snapshot.current_command = txn.current_command;
            }
        }
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
        self.terminate(xid, XidStatus::Committed)?;

        // Release all row-level and relation-level locks.
        self.lock_manager.release_all(xid);

        // SSI check: only for serializable transactions with an installed manager.
        if isolation == IsolationLevel::Serializable {
            if let Some(ssi) = &self.ssi {
                if let Err(SsiError::Serialization { victim, detail }) = ssi.commit(xid) {
                    // The SSI manager marked us committed before detecting the
                    // cycle; we must immediately abort to restore consistency.
                    // Flip the CLOG entry back to Aborted using the force path
                    // since the entry is now Committed, not InProgress.
                    self.force_abort(xid);
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
        self.terminate(xid, XidStatus::Aborted)?;

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

    // ---- internal helpers -------------------------------------------------

    /// Build a snapshot at this instant for `current_xid` and
    /// `current_command`.
    ///
    /// `xmax` is the current value of the XID counter — one past the
    /// largest XID handed out so far. `xmin` is the smallest in-progress
    /// XID; in the absence of any in-progress transaction it equals
    /// `xmax`, which renders [`Snapshot::xid_in_progress`] correct: every
    /// XID strictly less than `xmin` is fully resolved.
    fn build_snapshot(&self, current_xid: Xid, current_command: CommandId) -> Snapshot {
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
        Snapshot::new(xmin, xmax, current_xid, current_command, xip)
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
        txn.subtxn_stack.savepoint(
            name,
            || {
                let raw = self.next_xid.fetch_add(1, Ordering::AcqRel);
                let sub_xid = Xid::new(raw);
                self.clog.insert(sub_xid, XidStatus::InProgress);
                self.in_progress.lock().insert(sub_xid);
                sub_xid
            },
            txn.current_command,
        )
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
            // (programming error) the transition is a no-op.
            if let Some(mut entry) = self.clog.get_mut(&sub_xid) {
                if matches!(*entry.value(), XidStatus::InProgress) {
                    *entry.value_mut() = XidStatus::Aborted;
                    drop(entry);
                    self.in_progress.lock().remove(&sub_xid);
                }
            }
            // Track the rollback locally so visibility code can detect
            // SUBXACT-flagged tuples written by this savepoint.
            txn.subtxn_stack.record_rolled_back(sub_xid);
        }
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
        // Mark the subtransaction as committed so MVCC visibility picks it up.
        if let Some(mut entry) = self.clog.get_mut(&sub_xid) {
            if matches!(*entry.value(), XidStatus::InProgress) {
                *entry.value_mut() = XidStatus::Committed;
                drop(entry);
                self.in_progress.lock().remove(&sub_xid);
            }
        }
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
        self.clog
            .get(&xid)
            .map_or(XidStatus::InProgress, |entry| *entry.value())
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
        let xips: Vec<Xid> = t3.snapshot.xip.iter().copied().collect();
        assert_eq!(xips, vec![t1.xid, t2.xid]);

        // xmin is the oldest in-progress, which is t1.
        assert_eq!(t3.snapshot.xmin, t1.xid);
        // xmax is one past the highest XID handed out — i.e. the next
        // XID the counter would assign, which is t3.xid + 1.
        assert_eq!(t3.snapshot.xmax.raw(), t3.xid.raw() + 1);
        // The transaction's own XID is not in its own xip.
        assert!(!t3.snapshot.xip.contains(&t3.xid));
    }

    #[test]
    fn snapshot_after_commit_omits_committed_xid_and_oracle_reports_committed() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::ReadCommitted);
        let t1_xid = t1.xid;
        mgr.commit(t1).unwrap();

        let t4 = mgr.begin(IsolationLevel::ReadCommitted);
        assert!(!t4.snapshot.xip.contains(&t1_xid));

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
    fn read_committed_refresh_replaces_snapshot() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::ReadCommitted);
        let t1_xid = t1.xid;

        let mut reader = mgr.begin(IsolationLevel::ReadCommitted);
        let snap_before = reader.snapshot.clone();
        // Before refresh, t1 is in reader's xip.
        assert!(reader.snapshot.xip.contains(&t1_xid));
        let cmd_before = reader.current_command;

        // Commit t1 in between.
        mgr.commit(t1).unwrap();

        mgr.refresh_snapshot(&mut reader);

        // Command id advanced.
        assert_eq!(reader.current_command, cmd_before.next());
        // Snapshot changed — t1 is no longer in xip.
        assert!(!reader.snapshot.xip.contains(&t1_xid));
        assert_ne!(reader.snapshot, snap_before);
    }

    #[test]
    fn repeatable_read_refresh_keeps_snapshot() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::ReadCommitted);
        let t1_xid = t1.xid;

        let mut reader = mgr.begin(IsolationLevel::RepeatableRead);
        let snap_xip_before: Vec<Xid> = reader.snapshot.xip.iter().copied().collect();
        let xmin_before = reader.snapshot.xmin;
        let xmax_before = reader.snapshot.xmax;

        mgr.commit(t1).unwrap();
        mgr.refresh_snapshot(&mut reader);

        // The xip / xmin / xmax must not have changed under RR.
        let snap_xip_after: Vec<Xid> = reader.snapshot.xip.iter().copied().collect();
        assert_eq!(snap_xip_after, snap_xip_before);
        assert_eq!(reader.snapshot.xmin, xmin_before);
        assert_eq!(reader.snapshot.xmax, xmax_before);
        // t1 still considered active by reader's frozen snapshot.
        assert!(reader.snapshot.xip.contains(&t1_xid));
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
        assert_eq!(reader.snapshot.xip, snap_before.xip);
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
        let last = first + (N_THREADS * PER_THREAD) as u64 - 1;
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
}
