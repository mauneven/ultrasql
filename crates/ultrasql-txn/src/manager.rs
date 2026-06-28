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

/// The transaction's own subtransaction sets, threaded into the
/// snapshot-building path so the resulting snapshot can tell *its own*
/// savepoint writes apart from concurrent foreign writers.
///
/// - `live` — subxids that count as **self**: still-open savepoints plus
///   any `RELEASE`d-while-parent-open ("merged up") subxids. These are
///   excluded from the snapshot's `xip`/`xmin` (they are not concurrent
///   foreign writers) and emitted into the snapshot's own-live set.
/// - `rolled_back` — subxids discarded via `ROLLBACK TO`. Emitted into the
///   snapshot's own-rolled-back set so visibility reverts their writes
///   independently of physical heap undo.
#[derive(Clone, Debug, Default)]
pub struct OwnSubxids {
    /// Own subxids that count as self (live + merged-up).
    pub live: Vec<Xid>,
    /// Own subxids rolled back via `ROLLBACK TO`.
    pub rolled_back: Vec<Xid>,
}

impl OwnSubxids {
    /// The empty sets — used at `begin` (no savepoints yet) and anywhere a
    /// fresh top-level snapshot is built.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build the sets from a transaction's subtransaction manager.
    #[must_use]
    pub fn from_subtxn(subtxn: &crate::savepoint::SubtxnManager) -> Self {
        Self {
            live: subtxn.self_subxids(),
            rolled_back: subtxn.rolled_back_subxids(),
        }
    }

    /// `true` when there are no own subxids at all (the common, no-
    /// savepoint case) — lets the snapshot keep the single-`Xid` fast path.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.live.is_empty() && self.rolled_back.is_empty()
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
    /// Whether this transaction is read-only (`BEGIN READ ONLY` / `SET
    /// TRANSACTION READ ONLY`). The server rejects data-modifying statements
    /// with SQLSTATE `25006` while this is set. Sequence advancement
    /// (`nextval`) is intentionally still permitted, matching PostgreSQL.
    /// `begin()` defaults this to `false`; the server sets it from the
    /// requested access mode.
    pub read_only: bool,
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

    /// The **only** sanctioned XID accessor for stamping a heap tuple
    /// header (`xmin`/`xmax`) on a user-DML write path.
    ///
    /// This is an alias of [`Self::current_xid`]: it returns the active
    /// subtransaction XID when a `SAVEPOINT` is open, else the parent
    /// [`Self::xid`]. The dedicated name exists as a regression guard.
    /// Stamping a heap header with the raw parent [`Self::xid`] while a
    /// savepoint is open writes a row that `ROLLBACK TO` cannot hide and
    /// that the own-write visibility predicate cannot revert — the exact
    /// data-corruption class that forced the first subtransaction-
    /// visibility attempt to be reverted (`99c13ca0`).
    ///
    /// **Write sites must call `write_xid()`, never `txn.xid`.** The raw
    /// `xid` field stays public only for *termination / lock / SSI /
    /// recovery identity*, which is always the top-level transaction.
    #[must_use]
    pub fn write_xid(&self) -> Xid {
        self.current_xid()
    }

    /// Debug-only invariant check for a heap stamp site.
    ///
    /// Asserts that `stamped` is the XID `write_xid()` would have
    /// returned — i.e. the active subtransaction XID while a savepoint is
    /// open, or the parent XID otherwise. Fast / fused write paths that
    /// stamp the raw parent `xid` while a savepoint is open trip this
    /// assertion immediately in debug builds, before any corrupting row
    /// reaches disk. Compiles to nothing in release builds.
    ///
    /// This is the defense-in-depth chokepoint described in the
    /// subtransaction-visibility design §2: the implicit "every DML stamp
    /// routes through `write_xid()`" invariant is load-bearing and was
    /// previously undocumented — which is *why* the fused-DELETE miss
    /// shipped and forced a revert.
    #[inline]
    pub fn debug_assert_stamp(&self, stamped: Xid) {
        debug_assert_eq!(
            stamped,
            self.write_xid(),
            "DML stamp xid {stamped:?} must equal write_xid() {:?} \
             (open savepoint depth {}); stamping the parent while a \
             savepoint is open is the reverted corruption class",
            self.write_xid(),
            self.subtxn_stack.depth(),
        );
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
    /// Subtransaction → parent-(top-level)-XID map (a `pg_subtrans`
    /// analog). Recorded in [`Self::begin_savepoint`] when a subxid is
    /// allocated; consulted by [`XidStatusOracle::status`] so a foreign
    /// backend resolves a still-`InProgress` subxid to the **parent's**
    /// status. Savepoints always fold to the top parent, so one level of
    /// indirection suffices: a `RELEASE`d-but-parent-open subxid stays
    /// invisible to other backends, and the parent's single commit/abort is
    /// the only observable boundary.
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
        //    fresh transaction has no savepoints yet, so the own-subxid
        //    sets are empty.
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
            read_only: false,
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
                // Rebuild against the current active set, carrying the
                // transaction's own savepoint sets so a fresh RC statement
                // still sees its own savepoint writes and reverts its own
                // rolled-back ones.
                let own = OwnSubxids::from_subtxn(&txn.subtxn_stack);
                txn.snapshot = self.build_snapshot(txn.xid, txn.current_command, &own);
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Snapshot stays frozen. Keep `current_command` coherent so
                // own-write visibility advances with the statement counter,
                // and re-emit the own-subxid sets (savepoint state may have
                // changed since the snapshot froze) — patching only the
                // subxid sets, never `xmin`/`xmax`/`xip`.
                txn.snapshot.current_command = txn.current_command;
                self.apply_own_subxids(txn);
            }
        }
    }

    /// Build a fresh statement snapshot for `current_xid` at
    /// `current_command`.
    ///
    /// Callers use this after blocking on a row lock in READ COMMITTED
    /// mode. The lock wait may let an earlier writer commit after the
    /// statement began; the update then needs to re-check the latest
    /// committed row instead of treating the stale snapshot's `xip`
    /// entry as a permanent write conflict.
    ///
    /// Carries no own-subxid sets: these recheck paths (lock-wait re-probe,
    /// fused-update conflict recheck) operate from the top-level
    /// transaction's perspective and are not invoked mid-savepoint with
    /// own writes that need savepoint-aware visibility. Use
    /// [`Self::statement_snapshot_with_subxids`] when subxid awareness is
    /// required.
    #[must_use]
    pub fn statement_snapshot(&self, current_xid: Xid, current_command: CommandId) -> Snapshot {
        self.build_snapshot(current_xid, current_command, &OwnSubxids::empty())
    }

    /// Like [`Self::statement_snapshot`] but carrying the transaction's own
    /// savepoint sets so its own savepoint writes stay visible (and its
    /// rolled-back ones reverted) in the fresh snapshot.
    #[must_use]
    pub fn statement_snapshot_with_subxids(
        &self,
        current_xid: Xid,
        current_command: CommandId,
        own: &OwnSubxids,
    ) -> Snapshot {
        self.build_snapshot(current_xid, current_command, own)
    }

    /// Commit `txn` in a single shot. Marks the XID `Committed` in the CLOG.
    ///
    /// This is the non-durable convenience path: it runs the serializable check
    /// and immediately makes the txn visible, with no WAL fsync in between.
    /// **Durable** commit paths (every txn that changed persistent heap/index
    /// state) must instead use the two-phase decomposition so the commit is made
    /// durable BEFORE it is visible:
    ///
    /// 1. [`Self::commit_check_serializable`] — run the SSI check, get the family
    /// 2. append the Commit WAL record for the family and fsync it
    /// 3. [`Self::commit_finalize_visible`] — flip the CLOG + release locks
    ///
    /// For [`IsolationLevel::Serializable`] transactions with an installed
    /// [`SsiManager`], the dangerous-structure check runs first; if a
    /// serialization anomaly is detected the commit fails with
    /// [`TxnError::SerializationFailure`] (the CLOG is left untouched) and the
    /// caller must call [`Self::abort`] to roll back.
    ///
    /// Returns the **committed subtransaction family** — every subxid flipped
    /// to `Committed` alongside the parent (released + implicitly-released
    /// open-savepoint subxids; `ROLLBACK TO`-aborted ones are excluded). The
    /// caller embeds this list in the durable Commit WAL record so recovery can
    /// mark the whole family committed atomically with the parent.
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
    pub fn commit(&self, txn: Transaction) -> Result<Vec<Xid>, TxnError> {
        // Single-shot commit: run the serialization check (computing the
        // committed-subxid family), then immediately make the txn visible.
        // The durable-commit path splits these two phases — running the WAL
        // fsync between them so the commit is durable BEFORE it is visible —
        // via [`Self::commit_check_serializable`] and
        // [`Self::commit_finalize_visible`].
        match self.commit_check_serializable(&txn) {
            Ok(folded) => {
                // A double-commit surfaces here as `AlreadyTerminated`; propagate
                // it unchanged (the txn is already terminated, so there is
                // nothing to roll back). The normal path makes the txn visible.
                self.commit_finalize_visible(&txn, &folded)?;
                Ok(folded)
            }
            Err(e) => {
                // Backward-compatible single-shot contract: a serialization
                // failure leaves the txn fully rolled back (CLOG `Aborted`,
                // locks released), so direct `commit` callers that consume the
                // handle do not leak locks. The durable server path uses the
                // decomposed methods and aborts explicitly instead.
                let _ = self.abort(txn);
                Err(e)
            }
        }
    }

    /// Phase 1 of a durable commit: run the serializable-isolation check and
    /// return the committed-subtransaction family, **without** flipping the CLOG
    /// or releasing locks.
    ///
    /// The returned family — every released (merged-up) and implicitly-released
    /// open-savepoint subxid, excluding `ROLLBACK TO`-aborted ones — is the set
    /// that must ride inside the durable Commit WAL record so recovery marks the
    /// whole family `Committed` together. Callers append+fsync that record, then
    /// call [`Self::commit_finalize_visible`] with this family to make the txn
    /// visible *only after* it is durable.
    ///
    /// For a [`IsolationLevel::Serializable`] txn with an installed
    /// [`SsiManager`] this runs the dangerous-structure check. On a serialization
    /// anomaly the SSI entry is removed and [`TxnError::SerializationFailure`] is
    /// returned; the caller must then abort the txn (no Commit record is written,
    /// so the txn is never made visible and never durably committed). The CLOG is
    /// untouched, so the SSI optimistic "mark committed" no longer needs the
    /// later `force_abort` retraction the single-shot path used.
    pub fn commit_check_serializable(&self, txn: &Transaction) -> Result<Vec<Xid>, TxnError> {
        let folded = self.merged_up_family(txn);

        // SSI check: only for serializable transactions with an installed
        // manager. This runs BEFORE the CLOG flip, so a failure leaves the txn
        // still `InProgress` (MVCC-invisible) and the caller aborts it cleanly.
        if txn.isolation == IsolationLevel::Serializable {
            if let Some(ssi) = &self.ssi {
                // Capture the current XID high-water mark as this commit's GC
                // horizon: every transaction able to be concurrent with us has,
                // by definition, already begun and so carries a smaller XID. The
                // txn is still in the active set here (the CLOG flip happens in
                // phase 2), but the horizon is `next_xid()` — strictly greater
                // than `txn.xid` — so a later `oldest_in_progress()` reaching it
                // still proves no concurrent transaction survives.
                if let Err(SsiError::Serialization { victim, detail }) =
                    ssi.commit(txn.xid, self.next_xid())
                {
                    // The SSI manager optimistically marked us committed before
                    // detecting the cycle. The CLOG was never flipped, so there
                    // is nothing to retract there; just drop the SSI entry and
                    // report the failure. The caller aborts the txn.
                    ssi.abort(txn.xid);
                    return Err(TxnError::SerializationFailure { victim, detail });
                }
            }
        }

        Ok(folded)
    }

    /// Phase 2 of a durable commit: make the txn visible.
    ///
    /// Flips the parent CLOG entry and the whole `folded` subxid family to
    /// `Committed` in one atomic active-set transition (so a concurrent
    /// `build_snapshot` sees the whole family committed or none of it), then
    /// releases all of the txn's row- and relation-level locks and forgets the
    /// folded subxid→parent links.
    ///
    /// This is the step that makes the commit observable to every other backend.
    /// The durable-commit caller invokes it **only after** the Commit WAL record
    /// is fsynced, so no other snapshot can observe a commit that a subsequent
    /// crash would roll back.
    ///
    /// `folded` must be the family returned by the matching
    /// [`Self::commit_check_serializable`] call for `txn`.
    ///
    /// Returns [`TxnError::AlreadyTerminated`] / [`TxnError::Unknown`] if the
    /// parent CLOG entry is no longer `InProgress` (a mis-driven double-commit);
    /// the CLOG and locks are left untouched in that case.
    pub fn commit_finalize_visible(
        &self,
        txn: &Transaction,
        folded: &[Xid],
    ) -> Result<(), TxnError> {
        let xid = txn.xid;
        // Atomic family fold: the parent and every still-`InProgress` merged-up
        // subxid flip to Committed together, under the `in_progress` lock, so a
        // concurrent `build_snapshot` sees the whole family committed or none of
        // it (test H). The parent must still be `InProgress` here in the normal
        // linearly-driven lifecycle; a double-commit surfaces as
        // `AlreadyTerminated` and leaves visibility/locks untouched.
        self.terminate_with_subxids(xid, folded, XidStatus::Committed, false)?;

        // Release all row-level and relation-level locks.
        self.lock_manager.release_all(xid);
        self.forget_subxid_parents(folded);
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
        // On abort the *whole* family dies: the parent, every live (still
        // on the stack) subxid, and every merged-up subxid all fold to
        // Aborted under the `in_progress` lock. Rolled-back subxids are
        // already Aborted; re-aborting them is a no-op.
        let folded = self.abort_family(&txn);
        self.terminate_with_subxids(xid, &folded, XidStatus::Aborted, false)?;

        // Release all row-level and relation-level locks.
        self.lock_manager.release_all(xid);
        self.forget_subxid_parents(&folded);

        // Notify SSI of the abort so the entry can be removed.
        if isolation == IsolationLevel::Serializable {
            if let Some(ssi) = &self.ssi {
                ssi.abort(xid);
            }
        }

        Ok(())
    }

    /// Best-effort, idempotent abort of an autocommit transaction identified by
    /// `xid` alone, used by the server's per-statement unwind/scope guards when
    /// the panic has already moved the owning [`Transaction`] handle out of
    /// reach.
    ///
    /// Returns `true` iff this call performed the abort (the XID was still
    /// `InProgress` and is now `Aborted`); `false` if the XID was already
    /// terminated (committed or aborted) or never existed. Being a no-op on an
    /// already-terminated XID makes it **safe to call on every unwind path even
    /// when the normal commit/abort already ran** — there is no double-abort.
    ///
    /// This is for the *autocommit* path only: such transactions never open a
    /// savepoint, so the savepoint family is just the parent `xid`. It marks
    /// the CLOG entry `Aborted` and releases every row/relation lock the XID
    /// held (the leak this exists to prevent), then notifies SSI. It does **not**
    /// append a durable abort marker — an autocommit statement that unwound
    /// mid-flight has no committed effects, and recovery's default-abort sweep
    /// treats a never-committed XID as aborted regardless.
    ///
    /// Explicit `BEGIN` blocks must NOT route through here: their handle lives
    /// in the session state and is aborted with its full savepoint family by
    /// the client's `ROLLBACK`/`COMMIT`.
    pub fn abort_in_progress_by_xid(&self, xid: Xid) -> bool {
        // Flip only if still InProgress. `terminate_with_subxids` enforces the
        // same precondition and returns `Err(AlreadyTerminated)` otherwise; we
        // map that to "did nothing" so the guard's Drop is a clean no-op after a
        // normal commit/abort.
        if self
            .terminate_with_subxids(xid, &[xid], XidStatus::Aborted, false)
            .is_err()
        {
            return false;
        }
        // Release all row-level and relation-level locks held by the XID — the
        // permanent leak this method exists to close.
        self.lock_manager.release_all(xid);
        // Mirror `abort`'s SSI notification. The XID's isolation level is not
        // available here, so notify unconditionally when an SSI manager is
        // installed; `SsiManager::abort` is a no-op for an unregistered XID.
        if let Some(ssi) = &self.ssi {
            ssi.abort(xid);
        }
        true
    }

    /// The merged-up subxid family folded into a parent **commit**: every
    /// `RELEASE`d-while-parent-open subxid that is still `InProgress`
    /// (rolled-back ones are already Aborted and excluded by
    /// [`Subtxn`]'s bookkeeping). Live (un-released) stack subxids are *not*
    /// folded on commit — releasing is what merges a subxid into the
    /// parent; an un-released open savepoint at COMMIT is implicitly
    /// released by PostgreSQL, which we model here by treating the live
    /// stack as merged-up too.
    fn merged_up_family(&self, txn: &Transaction) -> Vec<Xid> {
        let mut family = txn.subtxn_stack.merged_up_subxids();
        family.extend(txn.subtxn_stack.live_stack_subxids());
        family.sort_unstable();
        family.dedup();
        family
    }

    /// The subxid family folded into a parent **abort**: live stack +
    /// merged-up. Identical membership to [`Self::merged_up_family`] today
    /// (both implicitly resolve every open/released subxid), kept as a
    /// distinct name so the commit/abort intent stays explicit at the call
    /// sites.
    fn abort_family(&self, txn: &Transaction) -> Vec<Xid> {
        self.merged_up_family(txn)
    }

    /// Drop the subxid → parent links for a folded family. Called after
    /// the family terminates so the `subxid_parent` map does not grow
    /// without bound.
    fn forget_subxid_parents(&self, folded: &[Xid]) {
        for sub in folded {
            self.subxid_parent.remove(sub);
        }
    }

    /// Atomically transition `parent` and every subxid in `folded` to
    /// `new_status`, removing the whole family from the `in_progress`
    /// mirror **under one lock**.
    ///
    /// Atomicity contract (the test-H crux): a concurrent
    /// [`Self::build_snapshot`] takes the same `in_progress` lock to sample
    /// the active set. By holding that lock across the entire family's CLOG
    /// flip + mirror removal, any snapshot observes either the complete
    /// family still in-progress (lock acquired before we start) or the
    /// complete family terminated (lock acquired after we finish) — never a
    /// torn subset where the parent looks committed while a subxid is still
    /// in-progress, or vice versa.
    ///
    /// The parent flips first (preserving commit-at-most-once via the
    /// `InProgress` precondition); subxids that are already terminal (e.g.
    /// rolled back) are skipped. Returns the parent's terminate result.
    ///
    /// `force_family` controls how a non-`InProgress` subxid is handled. With
    /// `false` (single-phase `commit`/`abort`) a subxid is upgraded only if it
    /// is still `InProgress`, so a `ROLLBACK TO`-aborted subxid keeps its
    /// `Aborted` status. With `true` (2PC phase-2 `finalise_prepared`) every
    /// subxid in `folded` is **force-set** to `new_status` regardless of its
    /// current value: after an in-doubt restart recovery's default-abort sweep
    /// marks the still-uncommitted savepoint subxids `Aborted`, and the
    /// authoritative 2PC decision (`COMMIT`/`ROLLBACK PREPARED`) must override
    /// that to match the durable Commit/Abort WAL record for the family.
    fn terminate_with_subxids(
        &self,
        parent: Xid,
        folded: &[Xid],
        new_status: XidStatus,
        force_family: bool,
    ) -> Result<(), TxnError> {
        // Hold the in_progress lock for the whole family so the active-set
        // transition is observed atomically by snapshot construction.
        let mut active = self.in_progress.lock();

        // Flip the parent CLOG entry first, validating the at-most-once
        // precondition. If it is not InProgress, fail without touching the
        // family — the caller drove the lifecycle incorrectly.
        {
            let Some(mut entry) = self.clog.get_mut(&parent) else {
                return Err(TxnError::Unknown { xid: parent });
            };
            match *entry.value() {
                XidStatus::InProgress => {
                    *entry.value_mut() = new_status;
                }
                other => {
                    return Err(TxnError::AlreadyTerminated {
                        xid: parent,
                        status: other,
                    });
                }
            }
        }
        active.remove(&parent);

        // Fold each subxid. In the single-phase path (`force_family == false`)
        // only a still-`InProgress` subxid is upgraded — a subxid already
        // Aborted (rolled back via ROLLBACK TO) or otherwise terminal keeps its
        // status. In the 2PC phase-2 path (`force_family == true`) the family's
        // outcome is fixed by the durable COMMIT/ROLLBACK PREPARED decision, so
        // each subxid is force-set to `new_status` even if recovery's
        // default-abort sweep had marked it Aborted in memory.
        for &sub in folded {
            if sub == parent {
                continue;
            }
            if let Some(mut entry) = self.clog.get_mut(&sub) {
                if force_family || matches!(*entry.value(), XidStatus::InProgress) {
                    *entry.value_mut() = new_status;
                }
            } else if force_family {
                // The subxid had no CLOG entry at all (e.g. recovery never
                // observed a record for it). Seed it directly to the
                // authoritative terminal status.
                self.clog.insert(sub, new_status);
            }
            active.remove(&sub);
        }
        drop(active);
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
    /// `current_command`.
    ///
    /// `xmax` is the current value of the XID counter — one past the
    /// largest XID handed out so far. `xmin` is the smallest in-progress
    /// XID; in the absence of any in-progress transaction it equals
    /// `xmax`, which renders [`Snapshot::xid_in_progress`] correct: every
    /// XID strictly less than `xmin` is fully resolved.
    fn build_snapshot(
        &self,
        current_xid: Xid,
        current_command: CommandId,
        own: &OwnSubxids,
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
            // Exclude our own live / merged-up subtransaction XIDs: they
            // are *self*, not concurrent foreign writers. Including them in
            // `xip` (or letting them lower `xmin`) would make our own
            // savepoint writes look in-progress-to-someone-else and hide
            // them from ourselves, and would also pin the vacuum horizon on
            // our own subxids. They are surfaced instead via the snapshot's
            // own-live set below.
            if own.live.contains(&xid) {
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
        let mut snapshot = Snapshot::new(xmin, xmax, current_xid, current_command, xip);
        // Emit the own-subxid sets so visibility recognises our own
        // savepoint writes as self (live) and reverts rolled-back ones.
        if !own.is_empty() {
            snapshot.set_own_subxids(own.live.iter().copied(), own.rolled_back.iter().copied());
        }
        snapshot
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
        let parent = txn.xid;
        let sp = txn.subtxn_stack.savepoint(
            name,
            || {
                let raw = self.next_xid.fetch_add(1, Ordering::AcqRel);
                let sub_xid = Xid::new(raw);
                self.clog.insert(sub_xid, XidStatus::InProgress);
                self.in_progress.lock().insert(sub_xid);
                sub_xid
            },
            txn.current_command,
        );
        // Record the subxid → top-parent link (pg_subtrans analog). Foreign
        // backends resolve a still-`InProgress` subxid to the parent's
        // status via this map, so a subxid is never independently visible.
        self.subxid_parent.insert(sp.xid, parent);
        // Refresh the transaction's own snapshot so its own writes under
        // this new savepoint are immediately visible to itself. For RR/SSI
        // this patches only the subxid sets (snapshot stays frozen); for RC
        // the next statement rebuilds anyway, but patching now keeps the
        // in-flight statement coherent.
        self.apply_own_subxids(txn);
        sp
    }

    /// Patch `txn`'s snapshot so its own-subxid sets reflect the current
    /// savepoint stack. Leaves `xmin`/`xmax`/`xip`/`current_xid`/
    /// `current_command` untouched — snapshot stability across SAVEPOINT /
    /// RELEASE / ROLLBACK TO is the whole point.
    ///
    /// `current_xid` on the snapshot still names the **top-level** XID; the
    /// live-subxid set carries the open/merged savepoint XIDs so
    /// [`Snapshot::is_current_xid`] recognises them as self.
    fn apply_own_subxids(&self, txn: &mut Transaction) {
        let own = OwnSubxids::from_subtxn(&txn.subtxn_stack);
        txn.snapshot.set_own_subxids(own.live, own.rolled_back);
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
        let parent = txn.xid;
        // PG keeps the savepoint after ROLLBACK TO, re-stamping it with a
        // fresh subxid. Allocate it the same way `begin_savepoint` does so
        // subsequent writes carry a live, registered subxid.
        let outcome = txn.subtxn_stack.rollback_to(name, || {
            let raw = self.next_xid.fetch_add(1, Ordering::AcqRel);
            let sub_xid = Xid::new(raw);
            self.clog.insert(sub_xid, XidStatus::InProgress);
            self.in_progress.lock().insert(sub_xid);
            sub_xid
        })?;
        // Register the surviving savepoint's fresh subxid → top-parent link
        // (pg_subtrans analog), mirroring `begin_savepoint`.
        self.subxid_parent.insert(outcome.new_target_xid, parent);
        let aborted_xids = outcome.aborted;
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
            // Track the rollback locally so visibility code reverts this
            // savepoint's writes via the snapshot's own-rolled-back set.
            txn.subtxn_stack.record_rolled_back(sub_xid);
        }
        // `rollback_to` may also have pruned `merged_up` (already-RELEASEd
        // inner savepoints at/above the cutoff) into the rolled-back set;
        // re-aborting those subxids in the CLOG keeps the oracle coherent.
        for sub_xid in txn.subtxn_stack.rolled_back_subxids() {
            if let Some(mut entry) = self.clog.get_mut(&sub_xid) {
                if matches!(*entry.value(), XidStatus::InProgress) {
                    *entry.value_mut() = XidStatus::Aborted;
                    drop(entry);
                    self.in_progress.lock().remove(&sub_xid);
                }
            }
        }
        // Release every row lock taken since the rolled-back savepoint(s).
        // A row lock acquired under a savepoint is held by the stable
        // top-level xid (so a re-lock later in the transaction is a no-op),
        // but carries the acquiring subxid as its *owner*. Releasing by the
        // rolled-back subxid set frees exactly those locks — matching
        // PostgreSQL, where a second session can lock the row again right
        // after the `ROLLBACK TO` — while locks taken before the savepoint
        // (owned by an outer subxid or the top-level xid) stay held. The full
        // rolled-back set is used (not just `aborted_xids`) so a lock taken
        // under a now-pruned merged-up subxid is released too; re-releasing an
        // already-freed subxid is a no-op.
        self.lock_manager
            .release_subxact_locks(&txn.subtxn_stack.rolled_back_subxids());
        // Re-emit the own-subxid sets into the (possibly frozen) snapshot so
        // the rolled-back writes become invisible to this transaction
        // immediately, without disturbing snapshot stability.
        self.apply_own_subxids(txn);
        Ok(aborted_xids)
    }

    /// Release the savepoint named `name` within `txn`.
    ///
    /// Removes the savepoint from the stack and records its XID as
    /// **merged up** — the subtransaction's writes count as part of the
    /// parent from now on, but the subxid CLOG entry is **kept
    /// `InProgress`**. The parent's eventual commit/abort is the *only*
    /// thing that makes those writes durable/visible to other backends
    /// (see [`Self::commit`] / [`Self::abort`] which atomically fold the
    /// merged-up subxids with the parent under the `in_progress` lock).
    ///
    /// Flipping the subxid to `Committed` here — as the reverted first
    /// attempt did — is a cross-transaction dirty-read leak: another
    /// backend would see the released savepoint's rows *before the parent
    /// commits*, and a later parent abort would have no way to retract
    /// them. Keeping it `InProgress` + folding on parent termination fixes
    /// both.
    ///
    /// Returns the released subxid on success, or [`SavepointError::NotFound`]
    /// if no savepoint with that name exists.
    pub fn release_savepoint(
        &self,
        txn: &mut Transaction,
        name: &str,
    ) -> Result<Xid, SavepointError> {
        let sub_xid = txn.subtxn_stack.release(name)?;
        // CLOG entry intentionally left InProgress; `merged_up` (recorded
        // inside `SubtxnManager::release`) keeps the writes as self until
        // the parent terminates. Re-emit own-subxid sets so the released
        // subxid stays in the snapshot's live (self) set.
        self.apply_own_subxids(txn);
        Ok(sub_xid)
    }

    // ---- 2PC helper --------------------------------------------------------

    /// Consume `txn` into the two-phase-commit coordinator.
    ///
    /// Records the XID under `gid` in `coordinator`, leaving the XID in the
    /// CLOG as `InProgress` until the coordinator resolves it with
    /// `commit_prepared` or `rollback_prepared`.
    ///
    /// The committed-subxid **family** — every released (merged-up) savepoint
    /// subxid plus every still-open-savepoint subxid that implicitly releases
    /// at commit, excluding `ROLLBACK TO`-aborted ones — is captured here,
    /// **before** the `txn` handle (and its savepoint stack) is dropped, and
    /// handed to the coordinator so it is persisted durably in the prepared
    /// state file. Phase-2 `COMMIT PREPARED` re-reads that family and embeds it
    /// in the durable Commit WAL record (exactly as single-phase commit does via
    /// [`Self::commit`]), so a row written under a released/open savepoint inside
    /// a prepared transaction survives a crash that follows `COMMIT PREPARED`.
    /// This reuses `merged_up_family` — the same set single-phase commit
    /// folds.
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
        // Capture the committed-subxid family BEFORE `txn` (and its savepoint
        // stack) is dropped — the same family single-phase COMMIT folds.
        let committed_subxids = self.merged_up_family(&txn);
        coordinator.prepare(gid, txn.xid, &committed_subxids)
        // `txn` is dropped here; the CLOG entry remains `InProgress` until
        // the coordinator resolves via `commit_prepared` / `rollback_prepared`.
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
    ///
    /// `family` is the prepared transaction's savepoint family (carried durably
    /// from `PREPARE` in the 2PC state file). It is flipped to `final_status`
    /// **atomically with the parent** under the `in_progress` lock — mirroring
    /// single-phase [`Self::commit`] — so the in-memory CLOG agrees with the
    /// durable Commit/Abort WAL record for the family.
    ///
    /// Crucially the family is **force-set** to `final_status` (not upgraded
    /// only when `InProgress`): after a restart in the in-doubt window
    /// (PREPARE done, phase-2 not yet) recovery re-seeds only the *parent* as
    /// `InProgress` and its default-abort sweep marks the still-uncommitted
    /// savepoint subxids `Aborted`. The authoritative 2PC decision must
    /// override that stale in-memory status so a `COMMIT PREPARED`'d savepoint
    /// row is visible in the same live process, without waiting for a further
    /// restart to re-apply the durable Commit record. On a `ROLLBACK PREPARED`
    /// force-setting the family to `Aborted` is idempotent with the recovered
    /// default but also correct in the same-process happy path where the
    /// subxids were still `InProgress`.
    ///
    /// The parent flip retains its `InProgress`-only precondition (commit-at-
    /// most-once); only the family — whose outcome is fixed by the durable 2PC
    /// record — is force-set.
    pub fn finalise_prepared(
        &self,
        xid: Xid,
        family: &[Xid],
        final_status: XidStatus,
    ) -> Result<(), TxnError> {
        debug_assert!(matches!(
            final_status,
            XidStatus::Committed | XidStatus::Aborted
        ));
        // Flip the parent and the whole savepoint family in one atomic
        // active-set transition, exactly like single-phase commit, force-setting
        // the family to the durably-decided terminal status.
        self.terminate_with_subxids(xid, family, final_status, true)?;
        self.lock_manager.release_all(xid);
        self.forget_subxid_parents(family);
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

        // pg_subtrans indirection: a subtransaction's own status only
        // becomes terminal at its parent's commit/abort (a `RELEASE`d-but-
        // parent-open subxid is kept `InProgress` on purpose). While the
        // subxid is still `InProgress`, resolve it to the **parent's**
        // status so a *foreign* backend never observes a released savepoint
        // as committed before the parent commits — and observes the whole
        // family abort atomically when the parent aborts. A subxid that has
        // itself been rolled back is already `Aborted` here and returns
        // directly without indirection.
        if matches!(own, XidStatus::InProgress) {
            if let Some(parent) = self.subxid_parent.get(&xid) {
                let parent = *parent.value();
                if parent != xid {
                    return self
                        .clog
                        .get(&parent)
                        .map_or(XidStatus::InProgress, |entry| *entry.value());
                }
            }
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
    fn commit_check_serializable_does_not_make_txn_visible_or_release_locks() {
        use crate::lock::{LockMode, LockRequest, LockTag};
        use ultrasql_core::RelationId;

        // FIX #5: the durable-commit phase 1 must NOT flip the CLOG visible nor
        // release locks — those happen only in phase 2, after the WAL fsync.
        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let xid = t.xid;

        let tag = LockTag::Relation(RelationId::new(7));
        mgr.lock_manager
            .acquire(LockRequest {
                xid,
                tag,
                mode: LockMode::Exclusive,
            })
            .unwrap();

        // Phase 1: serialization check + family computation. Visibility/locks
        // must be untouched.
        let folded = mgr.commit_check_serializable(&t).expect("phase 1 ok");
        assert!(folded.is_empty(), "no savepoints → empty family");
        assert_eq!(
            mgr.status(xid),
            XidStatus::InProgress,
            "txn must stay InProgress (invisible) until phase 2"
        );
        assert!(
            mgr.lock_manager
                .inspect(tag)
                .is_some_and(|s| s.grants.iter().any(|(x, _)| *x == xid)),
            "lock must still be held after phase 1"
        );

        // Phase 2: make visible + release locks.
        mgr.commit_finalize_visible(&t, &folded)
            .expect("phase 2 ok");
        assert_eq!(mgr.status(xid), XidStatus::Committed);
        assert!(
            mgr.lock_manager.inspect(tag).is_none(),
            "lock must be released only in phase 2"
        );
    }

    #[test]
    fn commit_check_serializable_failure_leaves_txn_in_progress_for_caller_abort() {
        // FIX #5: an SSI pivot must fail in phase 1 WITHOUT flipping the CLOG, so
        // the caller can abort cleanly (and never writes a Commit WAL record).
        let ssi = Arc::new(crate::ssi::SsiManager::new());
        let mgr = TransactionManager::new_with_ssi(Arc::clone(&ssi));

        let t1 = mgr.begin(IsolationLevel::Serializable);
        let t2 = mgr.begin(IsolationLevel::Serializable);
        let t3 = mgr.begin(IsolationLevel::Serializable);
        let t2_xid = t2.xid;

        mgr.record_rw_conflict(t1.xid, t2.xid);
        mgr.record_rw_conflict(t2.xid, t3.xid);

        mgr.commit(t1).unwrap();
        mgr.commit(t3).unwrap();

        // Phase-1 check for the pivot must report SerializationFailure and leave
        // the CLOG InProgress (NOT Committed, NOT yet Aborted — the caller aborts).
        let err = mgr
            .commit_check_serializable(&t2)
            .expect_err("pivot must fail phase 1");
        assert!(matches!(err, TxnError::SerializationFailure { .. }));
        assert_eq!(
            mgr.status(t2_xid),
            XidStatus::InProgress,
            "phase-1 failure must not flip the CLOG; the caller aborts"
        );

        // The caller's abort path then marks it Aborted.
        mgr.abort(t2).unwrap();
        assert_eq!(mgr.status(t2_xid), XidStatus::Aborted);
    }

    #[test]
    fn single_shot_commit_still_serializable_aborts_pivot() {
        // Regression: the single-shot `commit` (rebuilt on the new decomposition)
        // must still abort an SSI pivot and report SerializationFailure.
        let ssi = Arc::new(crate::ssi::SsiManager::new());
        let mgr = TransactionManager::new_with_ssi(Arc::clone(&ssi));

        let t1 = mgr.begin(IsolationLevel::Serializable);
        let t2 = mgr.begin(IsolationLevel::Serializable);
        let t3 = mgr.begin(IsolationLevel::Serializable);
        let t2_xid = t2.xid;

        mgr.record_rw_conflict(t1.xid, t2.xid);
        mgr.record_rw_conflict(t2.xid, t3.xid);

        mgr.commit(t1).unwrap();
        let err = mgr.commit(t2).expect_err("pivot must fail");
        assert!(matches!(err, TxnError::SerializationFailure { .. }));
        // Single-shot `commit` preserves its original contract: a failed commit
        // leaves the txn fully aborted (CLOG Aborted, locks released).
        assert_eq!(mgr.status(t2_xid), XidStatus::Aborted);
        mgr.commit(t3).unwrap();
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
    fn abort_in_progress_by_xid_releases_locks_and_is_idempotent() {
        use crate::lock::{LockMode, LockRequest, LockTag};
        use ultrasql_core::RelationId;

        let mgr = TransactionManager::new();
        let t = mgr.begin(IsolationLevel::ReadCommitted);
        let xid = t.xid;

        let tag = LockTag::Relation(RelationId::new(3));
        mgr.lock_manager
            .acquire(LockRequest {
                xid,
                tag,
                mode: LockMode::Exclusive,
            })
            .unwrap();

        // Simulate the unwind path: the autocommit guard aborts by xid alone
        // (the `Transaction` handle was dropped during the panic).
        assert!(
            mgr.abort_in_progress_by_xid(xid),
            "first by-xid abort of an in-progress txn performs the abort"
        );
        assert_eq!(
            XidStatusOracle::status(&mgr, xid),
            XidStatus::Aborted,
            "the XID is marked Aborted"
        );
        assert!(
            mgr.lock_manager.inspect(tag).is_none(),
            "the leaked lock is released by the by-xid abort"
        );

        // Idempotent: a second call (or a call after the normal commit/abort
        // already ran) is a no-op and returns false — NO double-abort.
        assert!(
            !mgr.abort_in_progress_by_xid(xid),
            "by-xid abort of an already-terminated XID is a no-op"
        );

        // A committed XID is never flipped to Aborted by the guard.
        let t2 = mgr.begin(IsolationLevel::ReadCommitted);
        let xid2 = t2.xid;
        mgr.commit(t2).unwrap();
        assert!(
            !mgr.abort_in_progress_by_xid(xid2),
            "a committed XID is not aborted by the guard (no double-termination)"
        );
        assert_eq!(
            XidStatusOracle::status(&mgr, xid2),
            XidStatus::Committed,
            "the committed XID stays Committed"
        );

        // An XID never registered (no CLOG entry) is a no-op.
        assert!(
            !mgr.abort_in_progress_by_xid(Xid::new(999_999)),
            "an unregistered XID is a no-op"
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

    // ── Phase 1: subtransaction visibility wiring ─────────────────────────────

    #[test]
    fn savepoint_writes_are_self_visible_via_snapshot_subxid_sets() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);
        let parent = t.xid;

        let sp = mgr.begin_savepoint(&mut t, "sp1");
        // The snapshot must recognise both the parent and the live subxid
        // as "current" (self).
        assert!(t.snapshot.is_current_xid(parent));
        assert!(
            t.snapshot.is_current_xid(sp.xid),
            "live savepoint subxid must be self in the snapshot"
        );
        assert!(!t.snapshot.own_subxid_rolled_back(sp.xid));

        mgr.commit(t).unwrap();
    }

    #[test]
    fn rollback_to_flags_subxid_rolled_back_in_snapshot() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);

        let sp = mgr.begin_savepoint(&mut t, "sp1");
        mgr.rollback_to_savepoint(&mut t, "sp1").unwrap();

        // After ROLLBACK TO, the subxid is no longer "self" and IS flagged
        // rolled-back so its writes are reverted by visibility.
        assert!(!t.snapshot.is_current_xid(sp.xid));
        assert!(t.snapshot.own_subxid_rolled_back(sp.xid));
        assert_eq!(mgr.status(sp.xid), XidStatus::Aborted);

        mgr.commit(t).unwrap();
    }

    #[test]
    fn release_keeps_subxid_in_progress_until_parent_commits() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);

        let sp = mgr.begin_savepoint(&mut t, "sp1");
        mgr.release_savepoint(&mut t, "sp1").unwrap();

        // RELEASE must NOT flip the subxid to Committed (the cross-txn
        // dirty-read leak): it stays InProgress, folded only at parent
        // commit. From a *foreign* oracle lookup the subxid resolves to the
        // parent's status (still InProgress).
        assert_eq!(mgr.status(sp.xid), XidStatus::InProgress);
        // The released subxid still counts as self for the owner.
        assert!(t.snapshot.is_current_xid(sp.xid));

        // After parent commit the family folds Committed atomically.
        mgr.commit(t).unwrap();
        assert_eq!(mgr.status(sp.xid), XidStatus::Committed);
    }

    #[test]
    fn release_then_rollback_to_outer_discards_inner_subxid() {
        // D: RELEASE inner, then ROLLBACK TO an outer savepoint must
        // discard the already-released inner subxid instead of folding it
        // Committed at top commit.
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);

        let outer = mgr.begin_savepoint(&mut t, "outer");
        let inner = mgr.begin_savepoint(&mut t, "inner");
        mgr.release_savepoint(&mut t, "inner").unwrap();
        // inner is merged-up (still InProgress, counts as self).
        assert_eq!(mgr.status(inner.xid), XidStatus::InProgress);
        assert!(t.snapshot.is_current_xid(inner.xid));

        // ROLLBACK TO outer: the cutoff is outer.xid; inner.xid >= cutoff,
        // so the merged-up inner must be pruned into rolled-back.
        mgr.rollback_to_savepoint(&mut t, "outer").unwrap();
        assert_eq!(mgr.status(inner.xid), XidStatus::Aborted);
        assert_eq!(mgr.status(outer.xid), XidStatus::Aborted);
        assert!(t.snapshot.own_subxid_rolled_back(inner.xid));
        assert!(!t.snapshot.is_current_xid(inner.xid));

        mgr.commit(t).unwrap();
        // Even after commit, the discarded inner stays Aborted (not folded
        // Committed) — the nested-release leak is closed.
        assert_eq!(mgr.status(inner.xid), XidStatus::Aborted);
    }

    #[test]
    fn foreign_oracle_resolves_inprogress_subxid_to_parent() {
        // R3: a foreign backend resolving a still-InProgress subxid sees
        // the PARENT's status, never the subxid independently.
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);
        let parent = t.xid;
        let sp = mgr.begin_savepoint(&mut t, "sp1");

        // Parent in progress → subxid resolves to InProgress.
        assert_eq!(mgr.status(sp.xid), XidStatus::InProgress);

        mgr.release_savepoint(&mut t, "sp1").unwrap();
        // Still in progress (release does not commit it).
        assert_eq!(mgr.status(sp.xid), XidStatus::InProgress);

        mgr.commit(t).unwrap();
        // Parent committed → subxid now resolves Committed (its own entry
        // was folded Committed, and parent indirection agrees).
        assert_eq!(mgr.status(parent), XidStatus::Committed);
        assert_eq!(mgr.status(sp.xid), XidStatus::Committed);
    }

    #[test]
    fn abort_folds_whole_subxid_family_atomically() {
        // H (unit-level): aborting the parent aborts every live/merged-up
        // subxid; no subxid is left dangling InProgress.
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);

        let a = mgr.begin_savepoint(&mut t, "a");
        let b = mgr.begin_savepoint(&mut t, "b");
        mgr.release_savepoint(&mut t, "b").unwrap(); // b merged-up
        let c = mgr.begin_savepoint(&mut t, "c"); // c live

        mgr.abort(t).unwrap();
        for sub in [a.xid, b.xid, c.xid] {
            assert_eq!(
                mgr.status(sub),
                XidStatus::Aborted,
                "subxid {sub:?} must be Aborted after parent abort"
            );
            assert!(!mgr.is_in_progress(sub));
        }
    }

    #[test]
    fn commit_folds_merged_up_and_live_subxids_committed() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);
        let a = mgr.begin_savepoint(&mut t, "a");
        mgr.release_savepoint(&mut t, "a").unwrap();
        let b = mgr.begin_savepoint(&mut t, "b"); // live at commit

        mgr.commit(t).unwrap();
        // Released and still-open savepoints both fold Committed at top
        // commit (PostgreSQL implicitly releases open savepoints on COMMIT).
        assert_eq!(mgr.status(a.xid), XidStatus::Committed);
        assert_eq!(mgr.status(b.xid), XidStatus::Committed);
    }

    #[test]
    fn rolled_back_subxid_stays_aborted_through_parent_commit() {
        let mgr = TransactionManager::new();
        let mut t = mgr.begin(IsolationLevel::ReadCommitted);
        let sp = mgr.begin_savepoint(&mut t, "sp1");
        mgr.rollback_to_savepoint(&mut t, "sp1").unwrap();
        assert_eq!(mgr.status(sp.xid), XidStatus::Aborted);
        mgr.commit(t).unwrap();
        assert_eq!(
            mgr.status(sp.xid),
            XidStatus::Aborted,
            "a rolled-back subxid must not be revived Committed at parent commit"
        );
    }
}
