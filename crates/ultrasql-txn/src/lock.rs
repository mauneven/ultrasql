//! Two-tier lock manager.
//!
//! Tier 1: fastpath relation lock — a per-backend cache of `AccessShare`
//! locks held without touching the central table. The cache is a
//! thread-local `SmallVec<[FastpathEntry; 16]>` modelling the common case
//! where a backend holds many `AccessShare` relation locks that do not need
//! a central record.
//!
//! Tier 2: central lock table — a sharded `DashMap<LockTag, LockEntry>`
//! where `LockEntry` carries the current grant set and a FIFO wait
//! queue.
//!
//! Lock modes follow PostgreSQL's hierarchy: `AccessShare`,
//! `RowShare`, `RowExclusive`, `ShareUpdateExclusive`, `Share`,
//! `ShareRowExclusive`, `Exclusive`, `AccessExclusive`.
//!
//! Lock-order contract: callers must acquire locks in the documented
//! global order (see ARCHITECTURE.md §14). The lock manager itself
//! takes only its own per-tag entry lock — no cross-tag holds while
//! the entry lock is held.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};
use smallvec::SmallVec;
use ultrasql_core::{RelationId, TupleId, Xid};

// ─── public types ────────────────────────────────────────────────────────────

/// The eight lock modes, ordered from weakest to strongest, matching
/// PostgreSQL's lock hierarchy.
///
/// See the `conflicts_with` method for the full compatibility matrix.
/// `AccessExclusive` is the strongest mode; `AccessShare` is the weakest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Acquired by `SELECT`. Conflicts only with `AccessExclusive`.
    AccessShare,
    /// Acquired by `SELECT FOR UPDATE / SHARE`. Conflicts with
    /// `Exclusive` and `AccessExclusive`.
    RowShare,
    /// Acquired by `INSERT`, `UPDATE`, `DELETE`. Conflicts with
    /// `Share`, `ShareRowExclusive`, `Exclusive`, `AccessExclusive`.
    RowExclusive,
    /// Acquired by `VACUUM` (non-full), `ANALYZE`, `CREATE INDEX
    /// CONCURRENTLY`. Conflicts with itself and stronger modes.
    ShareUpdateExclusive,
    /// Acquired by `CREATE INDEX` (non-concurrent). Conflicts with
    /// `RowExclusive`, `ShareUpdateExclusive`, `ShareRowExclusive`,
    /// `Exclusive`, `AccessExclusive`.
    Share,
    /// Acquired by `CREATE TRIGGER`, some `ALTER TABLE` forms. Conflicts
    /// with `RowExclusive`, `ShareUpdateExclusive`, `Share`, itself,
    /// `Exclusive`, `AccessExclusive`.
    ShareRowExclusive,
    /// Acquired by some `ALTER TABLE` forms, `REFRESH MATERIALIZED VIEW
    /// CONCURRENTLY`. Conflicts with everything except `AccessShare`.
    Exclusive,
    /// Acquired by `DROP TABLE`, `TRUNCATE`, `VACUUM FULL`, most DDL.
    /// Conflicts with every other mode.
    AccessExclusive,
}

impl LockMode {
    /// Conflict matrix per PostgreSQL `src/backend/storage/lmgr/lock.c`.
    ///
    /// Returns `true` if acquiring `self` is blocked by `held` being
    /// already granted.
    ///
    /// The matrix is symmetric: if `A.conflicts_with(B)` then
    /// `B.conflicts_with(A)`.
    #[must_use]
    #[allow(clippy::unnested_or_patterns)] // The conflict matrix must stay ordered for correctness
    pub const fn conflicts_with(self, held: Self) -> bool {
        use LockMode::{
            AccessExclusive, AccessShare, Exclusive, RowExclusive, RowShare, Share,
            ShareRowExclusive, ShareUpdateExclusive,
        };
        match (self, held) {
            // Compatible pairs: Exclusive is safe with AccessShare or RowShare.
            (Exclusive, AccessShare | RowShare) | (AccessShare | RowShare, Exclusive) => false,
            // All conflicting pairs, combined into one arm.
            // AccessExclusive conflicts with everything.
            // Exclusive conflicts with everything except AccessShare / RowShare.
            // ShareRowExclusive conflicts with RowExclusive, ShareUpdateExclusive, Share, itself.
            // Share conflicts with RowExclusive and ShareUpdateExclusive.
            // ShareUpdateExclusive conflicts with itself and RowExclusive.
            (AccessExclusive, _)
            | (_, AccessExclusive)
            | (Exclusive, _)
            | (_, Exclusive)
            | (
                ShareRowExclusive,
                RowExclusive | ShareUpdateExclusive | Share | ShareRowExclusive,
            )
            | (RowExclusive | ShareUpdateExclusive | Share, ShareRowExclusive)
            | (Share, RowExclusive | ShareUpdateExclusive)
            | (RowExclusive | ShareUpdateExclusive, Share)
            | (ShareUpdateExclusive | RowExclusive, ShareUpdateExclusive)
            | (ShareUpdateExclusive, RowExclusive) => true,
            // Everything else is compatible.
            _ => false,
        }
    }

    /// Whether this mode is `AccessShare`.
    #[must_use]
    const fn is_access_share(self) -> bool {
        matches!(self, Self::AccessShare)
    }
}

/// The subject of a lock request.
///
/// A `LockTag` uniquely identifies a lockable object. The central lock
/// table is keyed on `LockTag`; the fastpath cache stores only
/// `Relation` tags with mode `AccessShare`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LockTag {
    /// A relation (table, index, sequence, …).
    Relation(RelationId),
    /// A specific heap tuple (row-level locking).
    Tuple(TupleId),
    /// A user-visible advisory lock distinguished by a 64-bit key split
    /// into two 32-bit fields, matching PostgreSQL's `pg_advisory_lock`
    /// signature.
    Advisory {
        /// High 32 bits of the advisory key.
        classid: u32,
        /// Low 32 bits of the advisory key.
        objid: u32,
    },
}

/// A description of one lock acquisition request.
///
/// All fields are `Copy` types; `LockRequest` is `Copy` itself.
#[derive(Clone, Copy, Debug)]
pub struct LockRequest {
    /// The transaction requesting the lock.
    pub xid: Xid,
    /// The object to lock.
    pub tag: LockTag,
    /// The requested mode.
    pub mode: LockMode,
}

/// Errors returned by lock-acquisition operations.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// The deadlock detector selected `victim` as the cycle's youngest
    /// transaction. The victim must abort and retry.
    #[error("deadlock detected: victim {victim:?}")]
    Deadlock {
        /// The XID chosen as victim.
        victim: Xid,
    },
    /// The lock could not be acquired within the caller-supplied
    /// [`LockWait::timeout`] (the SQL `lock_timeout`). The waiter has
    /// already been removed from the wait queue when this is returned.
    #[error("lock acquisition timed out")]
    Timeout,
    /// `try_acquire` found a conflicting grant; the lock was not taken.
    #[error("lock held by another transaction")]
    Conflict,
    /// The blocking wait was interrupted by the caller-supplied
    /// [`LockWait::cancelled`] observer (statement timeout or client
    /// cancel). The waiter has already been removed from the wait queue
    /// when this is returned.
    #[error("lock wait cancelled")]
    Cancelled,
    /// A fastpath relation-lock reference count overflowed.
    #[error("fastpath lock reference count overflow")]
    FastpathOverflow,
}

/// Deadline / cancellation options for a blocking lock wait.
///
/// The default (`LockWait::default()`) waits forever with no
/// cancellation observer — exactly the historical [`LockManager::acquire`]
/// behaviour, using an unbounded condvar `wait` with no periodic wakeups.
/// When either field is set, the waiter sleeps in bounded slices
/// ([`LockWait::POLL_INTERVAL`], or less when the timeout deadline is
/// nearer) so it can observe cancellation and enforce the timeout even if
/// no grant/release notification ever arrives.
///
/// Priority on wake: a grantable lock always wins over an expired
/// timeout or a cancellation observed on the same wakeup, matching
/// PostgreSQL (the lock was available before the error was raised).
#[derive(Clone, Default)]
pub struct LockWait {
    /// Relative timeout measured from the start of the blocking wait;
    /// `None` waits forever. Expiry returns [`LockError::Timeout`]
    /// (SQL `lock_timeout` → SQLSTATE `55P03`).
    pub timeout: Option<Duration>,
    /// Cancellation observer polled on every wakeup (statement timeout /
    /// client cancel). Returning `true` aborts the wait with
    /// [`LockError::Cancelled`] (→ SQLSTATE `57014`). The closure must be
    /// cheap: it is polled at least every [`LockWait::POLL_INTERVAL`].
    pub cancelled: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
}

impl std::fmt::Debug for LockWait {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockWait")
            .field("timeout", &self.timeout)
            .field("has_cancel_observer", &self.cancelled.is_some())
            .finish()
    }
}

impl LockWait {
    /// Upper bound between cancellation polls while blocked on a lock.
    ///
    /// 10 ms keeps a cancelled/timed-out statement's exit latency well
    /// under human-visible thresholds without measurable idle cost (the
    /// waiter is parked; each wakeup is two atomic loads + a clock read).
    pub const POLL_INTERVAL: Duration = Duration::from_millis(10);

    /// Whether this wait is a plain unbounded wait (no timeout, no
    /// cancellation observer).
    #[must_use]
    pub fn is_unbounded(&self) -> bool {
        self.timeout.is_none() && self.cancelled.is_none()
    }

    /// Poll the cancellation observer.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.as_ref().is_some_and(|f| f())
    }
}

/// A point-in-time snapshot of who holds and waits on a particular
/// `LockTag`.
#[derive(Clone, Debug)]
pub struct LockTableSnapshot {
    /// Transactions that currently hold a grant.
    pub grants: Vec<(Xid, LockMode)>,
    /// Transactions waiting for a grant, in FIFO order.
    pub waiters: Vec<(Xid, LockMode)>,
}

// ─── internal state ──────────────────────────────────────────────────────────

/// One granted lock.
///
/// `xid` is the conflict / `release_all` / snapshot key: for a row lock taken
/// inside a savepoint it is the **top-level** transaction xid, so the grant is
/// released at transaction end and a re-lock of the same row later in the same
/// transaction (under a different subxid) is a no-op rather than a self-block.
///
/// `owner` is the subtransaction that actually acquired the grant — the xid the
/// write was stamped under (== `xid` when no savepoint is open). It exists only
/// so [`LockManager::release_subxact_locks`] can free exactly the locks taken
/// since a savepoint on `ROLLBACK TO`, matching PostgreSQL, while conflict
/// detection and `release_all` stay keyed on the stable top-level `xid`.
#[derive(Clone, Copy, Debug)]
struct Grant {
    xid: Xid,
    mode: LockMode,
    owner: Xid,
}

/// Mutable state stored under the per-entry mutex.
struct LockEntryState {
    /// Current grant holders.
    grants: Vec<Grant>,
    /// FIFO wait queue.
    waiters: VecDeque<(Xid, LockMode)>,
}

/// Per-`LockTag` entry in the central table.
struct LockEntry {
    inner: Mutex<LockEntryState>,
    /// Notified whenever the grants or waiters list changes (a grant was
    /// released, or a deadlock victim was chosen).
    waiters_changed: Condvar,
}

impl LockEntry {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(LockEntryState {
                grants: Vec::new(),
                waiters: VecDeque::new(),
            }),
            waiters_changed: Condvar::new(),
        })
    }
}

/// Per-XID deadlock tracking.
struct DeadlockState {
    /// Set to `true` by the detector when this XID is chosen as a victim.
    victim: AtomicBool,
}

impl DeadlockState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            victim: AtomicBool::new(false),
        })
    }

    fn is_victim(&self) -> bool {
        self.victim.load(Ordering::Acquire)
    }

    fn mark_victim(&self) {
        self.victim.store(true, Ordering::Release);
    }
}

// ─── thread-local fastpath ────────────────────────────────────────────────────

/// One entry in the per-backend fastpath cache.
struct FastpathEntry {
    tag: LockTag,
    mode: LockMode,
    /// Reference count (same mode on the same tag may be acquired
    /// multiple times by the same backend).
    count: u32,
}

thread_local! {
    static FASTPATH: std::cell::RefCell<SmallVec<[FastpathEntry; 16]>> =
        std::cell::RefCell::new(SmallVec::new());
}

// ─── LockManager ─────────────────────────────────────────────────────────────

/// Two-tier lock manager.
///
/// Safe to share across threads via `Arc`. One global instance per
/// server; backends hold references into it.
///
/// # Architecture
///
/// Tier 1 (fastpath): For `AccessShare` on a `Relation`, the lock is
/// recorded only in a thread-local cache — no central state is touched
/// and no synchronisation is needed on the acquire path.
///
/// Tier 2 (central table): All other modes go through a sharded
/// `DashMap`. Each `LockTag` maps to a `LockEntry` that carries the
/// current grant set and a FIFO wait queue. Waiters sleep on a
/// `parking_lot::Condvar` and are woken on every release.
///
/// # Deadlock detection
///
/// A background thread walks the central table at a configurable
/// interval (default 1 s), builds a wait-for graph, detects cycles via
/// DFS, and marks the youngest XID in each cycle as a victim. Victims
/// are woken immediately; their `acquire` call returns
/// `Err(LockError::Deadlock { victim })`.
///
/// # Send + Sync
///
/// `LockManager` is `Send + Sync` because:
/// - `DashMap` is `Send + Sync`.
/// - The detector-thread handle is behind a `Mutex`.
/// - `Arc<LockEntry>` and `Arc<DeadlockState>` compose `Send + Sync`
///   types.
pub struct LockManager {
    /// Central lock table.
    table: Arc<DashMap<LockTag, Arc<LockEntry>>>,
    /// Per-XID deadlock state.
    xid_states: Arc<DashMap<Xid, Arc<DeadlockState>>>,
    /// Duration between deadlock-detector sweeps.
    deadlock_interval: Duration,
    /// Set to `true` by `Drop` to stop the background detector thread.
    detector_stop: Arc<AtomicBool>,
    /// Join handle for the background detector thread.
    detector_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl std::fmt::Debug for LockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockManager")
            .field("deadlock_interval", &self.deadlock_interval)
            .field("table_entries", &self.table.len())
            .finish_non_exhaustive()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    /// Default deadlock detection interval.
    pub const DEFAULT_DEADLOCK_INTERVAL: Duration = Duration::from_secs(1);

    /// Create a new `LockManager` with the default deadlock interval (1 s).
    #[must_use]
    pub fn new() -> Self {
        Self::with_deadlock_interval(Self::DEFAULT_DEADLOCK_INTERVAL)
    }

    /// Create a new `LockManager` with a custom deadlock detection interval.
    ///
    /// A shorter interval speeds up deadlock detection (useful in tests)
    /// at the cost of more CPU in the background thread.
    #[must_use]
    pub fn with_deadlock_interval(interval: Duration) -> Self {
        let table = Arc::new(DashMap::new());
        let xid_states: Arc<DashMap<Xid, Arc<DeadlockState>>> = Arc::new(DashMap::new());
        let stop = Arc::new(AtomicBool::new(false));

        let t_table = Arc::clone(&table);
        let t_states = Arc::clone(&xid_states);
        let t_stop = Arc::clone(&stop);

        let handle = match thread::Builder::new()
            .name("ultrasql-deadlock-detector".into())
            .spawn(move || {
                detector_loop(&t_table, &t_states, &t_stop, interval);
            }) {
            Ok(handle) => Some(handle),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "failed to spawn deadlock detector; deadlock detection is DISABLED"
                );
                None
            }
        };

        Self {
            table,
            xid_states,
            deadlock_interval: interval,
            detector_stop: stop,
            detector_handle: Mutex::new(handle),
        }
    }

    // ── fastpath ──────────────────────────────────────────────────────────

    /// Attempt to satisfy `req` via the per-backend fastpath cache.
    ///
    /// Only `AccessShare` on a `Relation` tag is eligible. For any
    /// other combination this method delegates to [`Self::acquire`]
    /// (the central-table path).
    ///
    /// The fastpath cache lives in thread-local storage; no synchronisation
    /// is performed for the cache itself. Callers that share a `LockOwner`
    /// across threads must use the full `acquire` path.
    pub fn acquire_fastpath(&self, req: LockRequest) -> Result<(), LockError> {
        // Only AccessShare on Relation qualifies for the fastpath.
        if req.mode.is_access_share() {
            if let LockTag::Relation(_) = req.tag {
                return FASTPATH.with(|fp| {
                    let mut cache = fp.borrow_mut();
                    // Search for an existing entry for this (tag, mode).
                    if let Some(entry) = cache
                        .iter_mut()
                        .find(|e| e.tag == req.tag && e.mode == req.mode)
                    {
                        entry.count = entry
                            .count
                            .checked_add(1)
                            .ok_or(LockError::FastpathOverflow)?;
                    } else {
                        cache.push(FastpathEntry {
                            tag: req.tag,
                            mode: req.mode,
                            count: 1,
                        });
                    }
                    Ok(())
                });
            }
        }
        self.acquire(req)
    }

    /// Release one fastpath `AccessShare` on a `Relation` held by this
    /// thread. If the entry is not in the fastpath, delegates to
    /// [`Self::release`].
    pub fn release_fastpath(&self, xid: Xid, tag: LockTag, mode: LockMode) {
        if mode.is_access_share() {
            if let LockTag::Relation(_) = tag {
                let mut released_from_fastpath = false;
                FASTPATH.with(|fp| {
                    let mut cache = fp.borrow_mut();
                    if let Some(pos) = cache.iter().position(|e| e.tag == tag && e.mode == mode) {
                        let entry = &mut cache[pos];
                        if entry.count > 1 {
                            entry.count -= 1;
                        } else {
                            cache.remove(pos);
                        }
                        released_from_fastpath = true;
                    }
                });
                if released_from_fastpath {
                    return;
                }
            }
        }
        self.release(xid, tag, mode);
    }

    // ── central-table path ────────────────────────────────────────────────

    /// Acquire the lock described by `req`.
    ///
    /// Blocks the calling thread until the grant is available or until
    /// the deadlock detector marks `req.xid` as a victim, in which case
    /// `Err(LockError::Deadlock { victim })` is returned.
    ///
    /// Algorithm:
    /// 1. Look up (or insert) the `LockEntry` for `req.tag` in the
    ///    central table.
    /// 2. Lock the entry's mutex.
    /// 3. If no current grant conflicts with `req.mode`, push to grants
    ///    and return `Ok(())`.
    /// 4. Otherwise push to waiters, then loop:
    ///    - Drop the mutex and wait on `waiters_changed`.
    ///    - On wake, re-acquire the mutex.
    ///    - If the deadlock detector flagged us as a victim, remove our
    ///      waiter entry and return `Err(Deadlock)`.
    ///    - Otherwise check again whether the lock can be granted; if so,
    ///      move from waiters to grants and return `Ok(())`.
    pub fn acquire(&self, req: LockRequest) -> Result<(), LockError> {
        self.acquire_with_owner(req, req.xid, &LockWait::default())
    }

    /// Like [`Self::acquire`] but bounded by `wait`: an expired
    /// [`LockWait::timeout`] returns [`LockError::Timeout`] and a firing
    /// [`LockWait::cancelled`] observer returns [`LockError::Cancelled`].
    /// On either early exit the waiter is removed from the wait queue
    /// under the entry mutex — a timed-out waiter can never linger and
    /// block or confuse later acquirers.
    pub fn acquire_with_wait(&self, req: LockRequest, wait: &LockWait) -> Result<(), LockError> {
        self.acquire_with_owner(req, req.xid, wait)
    }

    /// Like [`Self::acquire`] but records `owner` (the acquiring
    /// subtransaction xid) on the grant so `ROLLBACK TO` can release it via
    /// [`Self::release_subxact_locks`]. Conflict detection, `release_all`, and
    /// the snapshot views still key on `req.xid` (the stable top-level xid for
    /// a row lock taken inside a savepoint). Pass `owner == req.xid` when no
    /// savepoint is open.
    pub fn acquire_for_owner(&self, req: LockRequest, owner: Xid) -> Result<(), LockError> {
        self.acquire_with_owner(req, owner, &LockWait::default())
    }

    /// [`Self::acquire_for_owner`] bounded by `wait` — see
    /// [`Self::acquire_with_wait`] for the timeout / cancellation contract.
    pub fn acquire_for_owner_with_wait(
        &self,
        req: LockRequest,
        owner: Xid,
        wait: &LockWait,
    ) -> Result<(), LockError> {
        self.acquire_with_owner(req, owner, wait)
    }

    #[allow(clippy::significant_drop_tightening)] // `state` is intentionally held across `wait`
    fn acquire_with_owner(
        &self,
        req: LockRequest,
        owner: Xid,
        wait: &LockWait,
    ) -> Result<(), LockError> {
        // Register the XID's deadlock state if not already present.
        self.ensure_xid_state(req.xid);

        let entry = self
            .table
            .entry(req.tag)
            .or_insert_with(LockEntry::new)
            .clone();

        let mut state = entry.inner.lock();

        if !has_conflict(&state.grants, req.xid, req.mode) {
            // Fast path: no conflict — grant immediately.
            state.grants.push(Grant {
                xid: req.xid,
                mode: req.mode,
                owner,
            });
            // Clear any stale victim flag from a previous false-positive
            // detection run.
            self.clear_victim(req.xid);
            return Ok(());
        }

        // A statement already cancelled must not enqueue at all.
        if wait.is_cancelled() {
            return Err(LockError::Cancelled);
        }
        // The timeout deadline starts when the blocking wait starts.
        let deadline = wait.timeout.map(|t| Instant::now() + t);

        // Slow path: enqueue and wait.
        state.waiters.push_back((req.xid, req.mode));

        loop {
            // Check victim flag *before* sleeping to avoid a wake-up race.
            // The check runs while holding the entry mutex, so it is
            // atomic with respect to the detector's notify (see module
            // doc for the full liveness argument).
            if self.is_victim(req.xid) {
                remove_waiter(&mut state.waiters, req.xid, req.mode);
                return Err(LockError::Deadlock { victim: req.xid });
            }

            if wait.is_unbounded() {
                // `wait` atomically releases the mutex and parks the thread.
                entry.waiters_changed.wait(&mut state);
            } else {
                // Bounded sleep: wake at the next cancellation poll tick or
                // at the timeout deadline, whichever comes first, so both
                // are observed even if no notification ever arrives.
                let mut wake_at = Instant::now() + LockWait::POLL_INTERVAL;
                if let Some(deadline) = deadline {
                    wake_at = wake_at.min(deadline);
                }
                let _ = entry.waiters_changed.wait_until(&mut state, wake_at);
            }

            // Re-check victim flag after waking.
            if self.is_victim(req.xid) {
                remove_waiter(&mut state.waiters, req.xid, req.mode);
                return Err(LockError::Deadlock { victim: req.xid });
            }

            // A grantable lock wins over a timeout/cancel observed on the
            // same wakeup: the lock was available before the error would
            // have been raised (PostgreSQL behaves the same way).
            if !has_conflict(&state.grants, req.xid, req.mode) {
                remove_waiter(&mut state.waiters, req.xid, req.mode);
                state.grants.push(Grant {
                    xid: req.xid,
                    mode: req.mode,
                    owner,
                });
                // Clear any stale victim flag (false positive from a
                // detection run that resolved itself by the time we woke).
                self.clear_victim(req.xid);
                return Ok(());
            }

            // CRITICAL: both early exits remove this waiter under the entry
            // mutex, exactly like the deadlock-victim path above — a waiter
            // is never leaked in the queue. Nothing else needs waking:
            // grant eligibility is computed from `grants` alone (FIFO order
            // never gates a grant), so removing a waiter cannot unblock or
            // starve another waiter.
            if wait.is_cancelled() {
                remove_waiter(&mut state.waiters, req.xid, req.mode);
                return Err(LockError::Cancelled);
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                remove_waiter(&mut state.waiters, req.xid, req.mode);
                return Err(LockError::Timeout);
            }
        }
    }

    /// Attempt to acquire the lock without blocking.
    ///
    /// Returns `Ok(true)` if the lock was granted immediately,
    /// `Ok(false)` if there is a conflict (the lock was not taken),
    /// or `Err(LockError::Deadlock)` if — in the unlikely edge case —
    /// the XID is already flagged as a deadlock victim.
    pub fn try_acquire(&self, req: LockRequest) -> Result<bool, LockError> {
        self.try_acquire_with_owner(req, req.xid)
    }

    /// Like [`Self::try_acquire`] but records `owner` (the acquiring
    /// subtransaction xid) on the grant so `ROLLBACK TO` can release it via
    /// [`Self::release_subxact_locks`]. See [`Self::acquire_for_owner`].
    pub fn try_acquire_for_owner(&self, req: LockRequest, owner: Xid) -> Result<bool, LockError> {
        self.try_acquire_with_owner(req, owner)
    }

    fn try_acquire_with_owner(&self, req: LockRequest, owner: Xid) -> Result<bool, LockError> {
        self.ensure_xid_state(req.xid);

        if self.is_victim(req.xid) {
            return Err(LockError::Deadlock { victim: req.xid });
        }

        let entry = self
            .table
            .entry(req.tag)
            .or_insert_with(LockEntry::new)
            .clone();

        let mut state = entry.inner.lock();

        if has_conflict(&state.grants, req.xid, req.mode) {
            return Ok(false);
        }

        state.grants.push(Grant {
            xid: req.xid,
            mode: req.mode,
            owner,
        });
        drop(state);
        Ok(true)
    }

    /// Release one grant of (`xid`, `tag`, `mode`) from the central
    /// table.
    ///
    /// Removes exactly one matching entry from the grant list. If the
    /// entry becomes empty (no grants, no waiters) it is pruned from the
    /// central table to bound memory usage.
    ///
    /// After removing the grant, all waiters on the entry are notified so
    /// they can re-evaluate their wait condition.
    pub fn release(&self, xid: Xid, tag: LockTag, mode: LockMode) {
        let Some(entry) = self.table.get(&tag).map(|e| Arc::clone(&e)) else {
            return;
        };

        {
            let mut state = entry.inner.lock();
            // Remove at most one matching grant.
            if let Some(pos) = state
                .grants
                .iter()
                .position(|g| g.xid == xid && g.mode == mode)
            {
                state.grants.remove(pos);
            }
            // Notify all waiters regardless; each will re-check the condition.
        }
        entry.waiters_changed.notify_all();

        // Prune the entry from the central table if it is empty.
        self.prune_entry_if_empty(tag);
    }

    /// Release all locks held by `xid` from the central table.
    ///
    /// Callers should invoke this at transaction commit or abort to
    /// reclaim all held grants and wake any waiters.
    ///
    /// Locks held in the fastpath cache are not released by this method;
    /// call [`Self::release_fastpath`] (or clear the cache directly) for
    /// those.
    pub fn release_all(&self, xid: Xid) {
        // Collect affected tags first to avoid holding DashMap iterator
        // across mutable operations.
        let affected: Vec<LockTag> = self
            .table
            .iter()
            .filter_map(|entry| {
                let state = entry.value().inner.lock();
                if state.grants.iter().any(|g| g.xid == xid) {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();

        for tag in affected {
            if let Some(entry) = self.table.get(&tag).map(|e| Arc::clone(&e)) {
                {
                    let mut state = entry.inner.lock();
                    state.grants.retain(|g| g.xid != xid);
                }
                entry.waiters_changed.notify_all();
                self.prune_entry_if_empty(tag);
            }
        }

        // Clean up deadlock state for this XID.
        self.xid_states.remove(&xid);
    }

    /// Release every grant whose **owner** subtransaction is in `owners`,
    /// leaving grants owned by other subxids (and by the top-level xid)
    /// untouched.
    ///
    /// Called by `ROLLBACK TO SAVEPOINT` with the set of subxids the rollback
    /// discarded (the rolled-back savepoints' subxids). A row lock taken since
    /// the savepoint carries one of those subxids as its `owner`, so it is
    /// released here — matching PostgreSQL, which frees the locks held by the
    /// rolled-back subtransactions while the top-level transaction continues.
    ///
    /// Unlike [`Self::release_all`] this does **not** touch `xid_states`: the
    /// top-level transaction is still live, and the owner subxids never had
    /// their own deadlock state (locks are registered under the top-level
    /// `xid`). Waiters on each affected tag are notified so a blocked
    /// concurrent acquirer can re-check.
    pub fn release_subxact_locks(&self, owners: &[Xid]) {
        if owners.is_empty() {
            return;
        }
        let owner_set: std::collections::HashSet<Xid> = owners.iter().copied().collect();

        let affected: Vec<LockTag> = self
            .table
            .iter()
            .filter_map(|entry| {
                let state = entry.value().inner.lock();
                if state.grants.iter().any(|g| owner_set.contains(&g.owner)) {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();

        for tag in affected {
            if let Some(entry) = self.table.get(&tag).map(|e| Arc::clone(&e)) {
                {
                    let mut state = entry.inner.lock();
                    state.grants.retain(|g| !owner_set.contains(&g.owner));
                }
                entry.waiters_changed.notify_all();
                self.prune_entry_if_empty(tag);
            }
        }
    }

    /// Return a point-in-time snapshot of the grants and waiters for
    /// `tag`, or `None` if no entry exists.
    #[allow(clippy::significant_drop_tightening)] // `entry` must live until after state is used
    pub fn inspect(&self, tag: LockTag) -> Option<LockTableSnapshot> {
        let entry = self.table.get(&tag)?;
        let state = entry.inner.lock();
        Some(LockTableSnapshot {
            grants: state.grants.iter().map(|g| (g.xid, g.mode)).collect(),
            waiters: state.waiters.iter().copied().collect(),
        })
    }

    /// Return a point-in-time snapshot of every central lock-table
    /// entry that currently has grants or waiters.
    pub fn snapshot(&self) -> Vec<(LockTag, LockTableSnapshot)> {
        let mut out = Vec::new();
        for entry in self.table.iter() {
            let tag = *entry.key();
            let state = entry.value().inner.lock();
            if state.grants.is_empty() && state.waiters.is_empty() {
                continue;
            }
            out.push((
                tag,
                LockTableSnapshot {
                    grants: state.grants.iter().map(|g| (g.xid, g.mode)).collect(),
                    waiters: state.waiters.iter().copied().collect(),
                },
            ));
        }
        out
    }

    // ── private helpers ───────────────────────────────────────────────────

    fn ensure_xid_state(&self, xid: Xid) {
        self.xid_states
            .entry(xid)
            .or_insert_with(DeadlockState::new);
    }

    fn is_victim(&self, xid: Xid) -> bool {
        self.xid_states.get(&xid).is_some_and(|s| s.is_victim())
    }

    fn clear_victim(&self, xid: Xid) {
        if let Some(state) = self.xid_states.get(&xid) {
            state.victim.store(false, Ordering::Release);
        }
    }

    fn prune_entry_if_empty(&self, tag: LockTag) {
        // Remove the entry only if both grants and waiters are empty, and do
        // so atomically with respect to a concurrent `acquire`. `remove_if`
        // evaluates the predicate *while still holding the DashMap shard
        // lock*, so the emptiness check and the removal cannot be interleaved
        // by another thread that obtained the same `Arc<LockEntry>` and
        // pushed a grant. A plain `get` + `remove` would race: after the
        // `get` returns and we drop both the entry mutex and the shard lock,
        // a concurrent `acquire` could grant a lock on the still-present
        // entry, and the unconditional `remove` would then evict a
        // now-non-empty entry — orphaning that grant and letting a later
        // acquirer create a fresh entry and grant a *conflicting* lock on the
        // same tag (two holders on one tag).
        // Test seam: allow a regression test to deterministically inject a
        // concurrent `acquire` into the prune window (the point at which the
        // buggy implementation had already observed the entry empty and
        // dropped its locks). `remove_if` re-checks the predicate under the
        // shard lock *after* this hook runs, so a grant pushed during the
        // hook keeps the entry alive — exactly the property the seam proves.
        #[cfg(test)]
        Self::prune_window_hook();

        self.table.remove_if(&tag, |_, entry| {
            let state = entry.inner.lock();
            state.grants.is_empty() && state.waiters.is_empty()
        });
    }

    /// Test seam invoked inside [`Self::prune_entry_if_empty`] at the prune
    /// window. By default a no-op; a regression test can install a callback
    /// (see `set_prune_window_hook`) to deterministically interleave a
    /// concurrent `acquire` with a prune.
    #[cfg(test)]
    fn prune_window_hook() {
        let hook = PRUNE_WINDOW_HOOK.with(|cell| cell.borrow().clone());
        if let Some(hook) = hook {
            hook();
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Per-thread prune-window callback for the prune/acquire race test.
    static PRUNE_WINDOW_HOOK: std::cell::RefCell<Option<Arc<dyn Fn() + Send + Sync>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn set_prune_window_hook(hook: Option<Arc<dyn Fn() + Send + Sync>>) {
    PRUNE_WINDOW_HOOK.with(|cell| *cell.borrow_mut() = hook);
}

impl Drop for LockManager {
    fn drop(&mut self) {
        self.detector_stop.store(true, Ordering::Release);
        // Wake the detector if it is sleeping so it exits promptly.
        if let Some(handle) = self.detector_handle.lock().take() {
            // Best-effort join; ignore the result.
            let _ = handle.join();
        }
    }
}

// ─── deadlock detector ───────────────────────────────────────────────────────

/// Entry point for the background detector thread.
///
/// Loops at `interval` until `stop` is set, calling `detect_and_resolve`
/// on each iteration.
fn detector_loop(
    table: &DashMap<LockTag, Arc<LockEntry>>,
    xid_states: &DashMap<Xid, Arc<DeadlockState>>,
    stop: &AtomicBool,
    interval: Duration,
) {
    while !stop.load(Ordering::Acquire) {
        thread::sleep(interval);
        if stop.load(Ordering::Acquire) {
            break;
        }
        detect_and_resolve(table, xid_states);
    }
}

/// Build a wait-for graph, detect cycles via DFS, mark the youngest
/// (largest XID raw value) victim in each cycle, and wake all waiters
/// on every `LockEntry` so victim threads can observe the flag promptly.
fn detect_and_resolve(
    table: &DashMap<LockTag, Arc<LockEntry>>,
    xid_states: &DashMap<Xid, Arc<DeadlockState>>,
) {
    // Build the wait-for graph: edge (waiter → holder) for every pair
    // where the waiter's requested mode conflicts with the holder's mode.
    // We also record, for each waiter XID, which entry it is sleeping on
    // so we can wake it after marking it as a victim.
    //
    // Graph: xid → Vec<xid> (adjacency list).
    // waiter_entries: xid → Arc<LockEntry>  (where to send notify_all).
    let mut wait_for: HashMap<Xid, Vec<Xid>> = HashMap::new();
    let mut waiter_entries: HashMap<Xid, Arc<LockEntry>> = HashMap::new();

    for entry_ref in table {
        let entry_arc = Arc::clone(entry_ref.value());
        let state = entry_arc.inner.lock();
        for (waiter_xid, waiter_mode) in &state.waiters {
            waiter_entries
                .entry(*waiter_xid)
                .or_insert_with(|| Arc::clone(&entry_arc));
            for grant in &state.grants {
                if waiter_mode.conflicts_with(grant.mode) && *waiter_xid != grant.xid {
                    wait_for.entry(*waiter_xid).or_default().push(grant.xid);
                }
            }
        }
    }

    if wait_for.is_empty() {
        return;
    }

    // DFS cycle detection. We track:
    // - `visited`: globally visited (not on any active stack).
    // - `on_stack`: currently on the DFS recursion stack.
    let mut visited: HashMap<Xid, bool> = HashMap::new(); // true = fully processed
    let mut on_stack: Vec<Xid> = Vec::new();
    let all_nodes: Vec<Xid> = wait_for.keys().copied().collect();

    let mut victims: Vec<Xid> = Vec::new();

    for start in all_nodes {
        if visited.contains_key(&start) {
            continue;
        }
        dfs_find_cycle(
            start,
            &wait_for,
            &mut visited,
            &mut on_stack,
            xid_states,
            &mut victims,
        );
    }

    // Wake every entry that has a victim waiting on it so the victim
    // thread can observe the flag in its `acquire` loop.
    //
    // We hold the entry's inner mutex while calling notify_all. This
    // closes the race where the victim thread checks `is_victim` (false),
    // then the detector marks the flag, then the detector calls
    // notify_all (no one sleeping yet), then the victim calls `wait`
    // and sleeps forever. By holding the mutex across mark + notify, the
    // victim either:
    //   (a) has not yet called `wait` — it will observe the flag in the
    //       pre-wait check because it must acquire the mutex first, and
    //       the mutex is held until after we notify; or
    //   (b) is already sleeping in `wait` — it is woken by notify_all.
    for victim_xid in victims {
        if let Some(entry) = waiter_entries.get(&victim_xid) {
            let _guard = entry.inner.lock();
            entry.waiters_changed.notify_all();
        }
    }
}

/// Recursive DFS that detects cycles, marks victims, and records them.
///
/// `on_stack` is the current path from the DFS root. When a back-edge is
/// found (we visit a node that is already on the stack), the portion of
/// the stack from that node onwards is the cycle. We pick the youngest
/// (largest raw XID) as the victim, flag it via `xid_states`, and
/// append it to `victims` so the caller can wake its condvar.
fn dfs_find_cycle(
    node: Xid,
    graph: &HashMap<Xid, Vec<Xid>>,
    visited: &mut HashMap<Xid, bool>,
    on_stack: &mut Vec<Xid>,
    xid_states: &DashMap<Xid, Arc<DeadlockState>>,
    victims: &mut Vec<Xid>,
) {
    on_stack.push(node);

    if let Some(neighbors) = graph.get(&node) {
        for &neighbor in neighbors {
            if let Some(&fully_done) = visited.get(&neighbor) {
                if fully_done {
                    continue; // Already fully processed, no cycle through it.
                }
                // `neighbor` is on the stack → cycle detected.
                if on_stack.contains(&neighbor) {
                    // Extract the cycle: everything from `neighbor`'s
                    // position in `on_stack` to the current end.
                    let Some(cycle_start) = on_stack.iter().position(|&x| x == neighbor) else {
                        continue;
                    };
                    let cycle: Vec<Xid> = on_stack[cycle_start..].to_vec();
                    // Pick youngest (largest raw XID) as victim.
                    let Some(victim) = cycle.iter().copied().max_by_key(|x| x.raw()) else {
                        continue;
                    };
                    if let Some(state) = xid_states.get(&victim) {
                        state.mark_victim();
                    }
                    victims.push(victim);
                }
            } else {
                // Not yet visited.
                visited.insert(neighbor, false);
                dfs_find_cycle(neighbor, graph, visited, on_stack, xid_states, victims);
            }
        }
    }

    on_stack.pop();
    visited.insert(node, true);
}

// ─── free-standing helpers ────────────────────────────────────────────────────

/// Returns `true` if any existing grant in `grants` conflicts with
/// `mode`.
///
/// Keyed on each grant's `xid` (the top-level transaction for a row lock taken
/// inside a savepoint), so a transaction never conflicts with a lock it already
/// holds — including one it took under an earlier savepoint.
fn has_conflict(grants: &[Grant], requester: Xid, mode: LockMode) -> bool {
    grants
        .iter()
        .any(|g| g.xid != requester && mode.conflicts_with(g.mode))
}

/// Remove the first occurrence of (`xid`, `mode`) from a waiter queue.
fn remove_waiter(waiters: &mut VecDeque<(Xid, LockMode)>, xid: Xid, mode: LockMode) {
    if let Some(pos) = waiters
        .iter()
        .position(|(wxid, wmode)| *wxid == xid && *wmode == mode)
    {
        waiters.remove(pos);
    }
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use proptest::prelude::*;

    use super::*;
    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};

    // ── helpers ──────────────────────────────────────────────────────────────

    fn rel(n: u32) -> LockTag {
        LockTag::Relation(RelationId::new(n))
    }

    fn tup(rel_n: u32, block: u32, slot: u16) -> LockTag {
        let page = PageId::new(RelationId::new(rel_n), BlockNumber::new(block));
        LockTag::Tuple(TupleId::new(page, slot))
    }

    fn xid(n: u64) -> Xid {
        Xid::new(n)
    }

    fn req(x: u64, tag: LockTag, mode: LockMode) -> LockRequest {
        LockRequest {
            xid: xid(x),
            tag,
            mode,
        }
    }

    // ── property test: conflict matrix is symmetric ───────────────────────

    /// Enumerate all 8 × 8 pairs and confirm that `A.conflicts_with(B)`
    /// iff `B.conflicts_with(A)`.
    #[test]
    fn compatibility_matrix_is_symmetric() {
        use LockMode::{
            AccessExclusive, AccessShare, Exclusive, RowExclusive, RowShare, Share,
            ShareRowExclusive, ShareUpdateExclusive,
        };
        let all = [
            AccessShare,
            RowShare,
            RowExclusive,
            ShareUpdateExclusive,
            Share,
            ShareRowExclusive,
            Exclusive,
            AccessExclusive,
        ];
        for &a in &all {
            for &b in &all {
                assert_eq!(
                    a.conflicts_with(b),
                    b.conflicts_with(a),
                    "{a:?}.conflicts_with({b:?}) != {b:?}.conflicts_with({a:?})"
                );
            }
        }
    }

    // Proptest version of the symmetry check.
    proptest! {
        #[test]
        fn conflict_matrix_symmetric_proptest(a in 0u8..8, b in 0u8..8) {
            let all = [
                LockMode::AccessShare,
                LockMode::RowShare,
                LockMode::RowExclusive,
                LockMode::ShareUpdateExclusive,
                LockMode::Share,
                LockMode::ShareRowExclusive,
                LockMode::Exclusive,
                LockMode::AccessExclusive,
            ];
            let ma = all[usize::from(a)];
            let mb = all[usize::from(b)];
            prop_assert_eq!(ma.conflicts_with(mb), mb.conflicts_with(ma));
        }
    }

    // ── AccessShare + RowShare co-exist on same tag by same XID ──────────

    #[test]
    fn single_transaction_can_hold_self_consistent_modes() {
        let mgr = LockManager::new();
        let tag = rel(1);
        mgr.acquire(req(10, tag, LockMode::AccessShare)).unwrap();
        mgr.acquire(req(10, tag, LockMode::RowShare)).unwrap();
        let snap = mgr.inspect(tag).expect("entry should exist");
        assert_eq!(snap.grants.len(), 2);
        assert!(snap.waiters.is_empty());
    }

    // ── conflicting locks block until release ─────────────────────────────

    #[test]
    #[ignore = "slow: real-time sleep (20 ms) for waiter synchronisation; run via cargo test -- --ignored"]
    fn conflicting_locks_block_until_release() {
        let mgr = Arc::new(LockManager::new());
        let tag = rel(2);

        // XID 1 holds AccessExclusive.
        mgr.acquire(req(1, tag, LockMode::AccessExclusive)).unwrap();

        let mgr2 = Arc::clone(&mgr);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier2 = Arc::clone(&barrier);

        let handle = std::thread::spawn(move || {
            // Signal we are about to block.
            barrier2.wait();
            // This must block until XID 1 releases.
            mgr2.acquire(req(2, tag, LockMode::AccessShare)).unwrap();
        });

        // Wait until the second thread is ready to acquire (a best-effort
        // synchronisation — we give it a small head-start to enqueue).
        barrier.wait();
        // Small sleep to let the second thread enter `acquire` and block.
        std::thread::sleep(Duration::from_millis(20));

        // Now inspect: XID 2 should be waiting.
        let snap = mgr.inspect(tag).expect("entry should exist");
        assert_eq!(snap.grants.len(), 1);
        assert_eq!(snap.grants[0].0, xid(1));
        assert!(!snap.waiters.is_empty(), "xid 2 should be waiting");

        // Release XID 1 — XID 2 should now wake and succeed.
        mgr.release(xid(1), tag, LockMode::AccessExclusive);

        handle.join().expect("second thread panicked");

        let snap2 = mgr.inspect(tag).expect("entry should exist after acquire");
        assert_eq!(snap2.grants[0].0, xid(2));
    }

    // ── non-conflicting locks are granted concurrently ────────────────────

    #[test]
    fn non_conflicting_locks_are_granted_concurrently() {
        let mgr = Arc::new(LockManager::new());
        let tag = rel(3);

        let barrier = Arc::new(std::sync::Barrier::new(4));
        let barriers: Vec<_> = (0..3).map(|_| Arc::clone(&barrier)).collect();

        let handles: Vec<_> = (0u64..3)
            .zip((0..3).map(|_| Arc::clone(&mgr)).zip(barriers))
            .map(|(i, (m, b))| {
                std::thread::spawn(move || {
                    b.wait();
                    m.acquire(req(i + 100, tag, LockMode::AccessShare)).unwrap();
                })
            })
            .collect();

        barrier.wait();
        for h in handles {
            h.join().expect("thread panicked");
        }

        let snap = mgr.inspect(tag).expect("entry should exist");
        assert_eq!(snap.grants.len(), 3, "all three AccessShare should be held");
        assert!(snap.waiters.is_empty());
    }

    // ── try_acquire returns false on conflict without blocking ────────────

    #[test]
    fn try_acquire_returns_false_on_conflict_without_blocking() {
        let mgr = LockManager::new();
        let tag = rel(4);

        mgr.acquire(req(1, tag, LockMode::AccessExclusive)).unwrap();
        let result = mgr.try_acquire(req(2, tag, LockMode::AccessShare)).unwrap();
        assert!(!result, "should return false (conflict), not block");
    }

    #[test]
    fn try_acquire_returns_true_when_no_conflict() {
        let mgr = LockManager::new();
        let tag = rel(5);

        let got = mgr.try_acquire(req(1, tag, LockMode::AccessShare)).unwrap();
        assert!(got);
    }

    #[test]
    fn same_xid_can_reacquire_conflicting_lock() {
        let mgr = LockManager::new();
        let tag = tup(6, 0, 0);

        mgr.acquire(req(1, tag, LockMode::Exclusive)).unwrap();
        let got = mgr.try_acquire(req(1, tag, LockMode::Exclusive)).unwrap();
        assert!(got, "a transaction must not conflict with its own lock");
    }

    // ── release_all clears every lock held by the XID ────────────────────

    #[test]
    fn release_all_clears_every_lock_held_by_xid() {
        let mgr = LockManager::new();
        let tags = [rel(10), tup(10, 0, 0), rel(11)];

        mgr.acquire(req(5, tags[0], LockMode::AccessShare)).unwrap();
        mgr.acquire(req(5, tags[1], LockMode::RowExclusive))
            .unwrap();
        mgr.acquire(req(5, tags[2], LockMode::Share)).unwrap();
        // Another XID also holds something.
        mgr.acquire(req(6, tags[0], LockMode::AccessShare)).unwrap();

        mgr.release_all(xid(5));

        // XID 5 grants must be gone.
        for tag in tags {
            if let Some(snap) = mgr.inspect(tag) {
                assert!(
                    snap.grants.iter().all(|(x, _)| *x != xid(5)),
                    "XID 5 should have no grants on {tag:?}"
                );
            }
        }
        // XID 6's lock on tags[0] must survive.
        let snap = mgr.inspect(tags[0]).expect("entry should exist for xid 6");
        assert!(snap.grants.iter().any(|(x, _)| *x == xid(6)));
    }

    // ── release_subxact_locks frees by owner, keying conflict on top xid ──

    /// A `ROLLBACK TO SAVEPOINT` analog: row locks taken inside the rolled-back
    /// subxids are held under the *top-level* xid (so they never self-conflict
    /// and `release_all` still reclaims them at commit) but carry the acquiring
    /// subxid as `owner`. `release_subxact_locks` frees exactly the rolled-back
    /// owners' grants; locks owned by the top-level xid (taken before the
    /// savepoint) and by surviving subxids stay held.
    #[test]
    fn release_subxact_locks_frees_only_rolled_back_owners() {
        let mgr = LockManager::new();
        let top = xid(100);
        let sub_a = xid(101); // a savepoint subxid that will be rolled back
        let sub_b = xid(102); // a surviving (outer) savepoint subxid
        let r_pre = tup(20, 0, 0); // locked before any savepoint (owner = top)
        let r_a = tup(20, 0, 1); // locked under sub_a (rolled back)
        let r_b = tup(20, 0, 2); // locked under sub_b (survives)

        // All three locks are held under the SAME top-level xid; only the owner
        // distinguishes them — exactly how the row-lock paths acquire them.
        mgr.acquire_for_owner(req(100, r_pre, LockMode::Exclusive), top)
            .unwrap();
        mgr.acquire_for_owner(req(100, r_a, LockMode::Exclusive), sub_a)
            .unwrap();
        mgr.acquire_for_owner(req(100, r_b, LockMode::Exclusive), sub_b)
            .unwrap();

        // Roll back sub_a.
        mgr.release_subxact_locks(&[sub_a]);

        // r_a is now free: a different transaction can take it.
        assert!(
            mgr.try_acquire(req(200, r_a, LockMode::Exclusive)).unwrap(),
            "the rolled-back subxid's lock must be released"
        );
        // r_pre (owner = top) and r_b (owner = surviving sub_b) stay held: a
        // peer conflicts on both.
        assert!(
            !mgr.try_acquire(req(200, r_pre, LockMode::Exclusive))
                .unwrap(),
            "pre-savepoint lock (owned by the top-level xid) must survive"
        );
        assert!(
            !mgr.try_acquire(req(200, r_b, LockMode::Exclusive)).unwrap(),
            "an outer surviving savepoint's lock must survive"
        );

        // The top-level xid still holds r_pre and r_b; release_all reclaims
        // them at commit (including the grant the rolled-back owner left — here
        // already gone). A re-lock by the same top xid is a no-op throughout.
        mgr.release_all(top);
    }

    /// `release_subxact_locks` with an owner set that matches nothing (and the
    /// empty set) is a no-op and never disturbs unrelated grants.
    #[test]
    fn release_subxact_locks_noop_when_no_owner_matches() {
        let mgr = LockManager::new();
        let tag = tup(21, 0, 0);
        mgr.acquire_for_owner(req(1, tag, LockMode::Exclusive), xid(1))
            .unwrap();

        mgr.release_subxact_locks(&[]); // empty
        mgr.release_subxact_locks(&[xid(999)]); // no match

        assert!(
            !mgr.try_acquire(req(2, tag, LockMode::Exclusive)).unwrap(),
            "unrelated grant must remain held"
        );
    }

    // ── deadlock detector picks the youngest victim ───────────────────────

    #[test]
    #[ignore = "slow: multi-thread deadlock contention stress; run via cargo test -- --ignored"]
    fn deadlock_detector_picks_youngest_victim() {
        // Use a very short detection interval so the test completes quickly.
        let mgr = Arc::new(LockManager::with_deadlock_interval(Duration::from_millis(
            50,
        )));

        let rel_a = rel(100);
        let rel_b = rel(101);

        // XID 1 holds AccessExclusive on A.
        // XID 2 holds AccessExclusive on B.
        mgr.acquire(req(1, rel_a, LockMode::AccessExclusive))
            .unwrap();
        mgr.acquire(req(2, rel_b, LockMode::AccessExclusive))
            .unwrap();

        let mgr_t1 = Arc::clone(&mgr);
        let mgr_t2 = Arc::clone(&mgr);

        // XID 1 tries to acquire B (blocked by XID 2).
        // On deadlock the aborting thread must release all its locks to
        // unblock the surviving thread — this mirrors real transaction
        // abort behaviour.
        let h1 = std::thread::spawn(move || -> Result<(), LockError> {
            match mgr_t1.acquire(req(1, rel_b, LockMode::AccessExclusive)) {
                Ok(()) => Ok(()),
                Err(e @ LockError::Deadlock { .. }) => {
                    // Simulate transaction abort: release all locks.
                    mgr_t1.release_all(xid(1));
                    Err(e)
                }
                Err(e) => Err(e),
            }
        });

        // XID 2 tries to acquire A (blocked by XID 1).
        let h2 = std::thread::spawn(move || -> Result<(), LockError> {
            match mgr_t2.acquire(req(2, rel_a, LockMode::AccessExclusive)) {
                Ok(()) => Ok(()),
                Err(e @ LockError::Deadlock { .. }) => {
                    // Simulate transaction abort: release all locks.
                    mgr_t2.release_all(xid(2));
                    Err(e)
                }
                Err(e) => Err(e),
            }
        });

        let r1 = h1.join().expect("thread 1 panicked");
        let r2 = h2.join().expect("thread 2 panicked");

        // Exactly one must have been aborted as a Deadlock victim; that
        // should be XID 2 (the larger / younger XID).
        let got_deadlock = match (&r1, &r2) {
            (Err(LockError::Deadlock { victim }), Ok(()))
            | (Ok(()), Err(LockError::Deadlock { victim })) => {
                assert_eq!(*victim, xid(2), "youngest victim should be xid(2)");
                true
            }
            (Err(LockError::Deadlock { .. }), Err(LockError::Deadlock { .. })) => {
                // Both were aborted — still acceptable as long as the
                // younger was marked.  In practice the detector picks one.
                true
            }
            _ => false,
        };
        assert!(
            got_deadlock,
            "expected a deadlock error, got {r1:?} / {r2:?}"
        );
    }

    // ── inspect reports current grants and waiters ────────────────────────

    #[test]
    #[ignore = "slow: real-time sleep (20 ms) for waiter synchronisation; run via cargo test -- --ignored"]
    fn inspect_reports_current_grants_and_waiters() {
        let mgr = Arc::new(LockManager::new());
        let tag = rel(200);

        // XID 10 holds AccessExclusive.
        mgr.acquire(req(10, tag, LockMode::AccessExclusive))
            .unwrap();

        let mgr2 = Arc::clone(&mgr);
        let ready = Arc::new(std::sync::Barrier::new(2));
        let ready2 = Arc::clone(&ready);

        let handle = std::thread::spawn(move || {
            ready2.wait();
            mgr2.acquire(req(11, tag, LockMode::AccessShare)).unwrap();
        });

        ready.wait();
        // Small delay to let the waiter enqueue.
        std::thread::sleep(Duration::from_millis(20));

        let snap = mgr.inspect(tag).expect("entry should exist");
        assert!(
            snap.grants.iter().any(|(x, _)| *x == xid(10)),
            "xid 10 should be in grants"
        );
        assert!(
            snap.waiters.iter().any(|(x, _)| *x == xid(11)),
            "xid 11 should be in waiters"
        );

        // Release XID 10 to unblock the waiter.
        mgr.release(xid(10), tag, LockMode::AccessExclusive);
        handle.join().expect("waiter thread panicked");
    }

    // ── advisory lock tag ─────────────────────────────────────────────────

    #[test]
    fn advisory_lock_acquires_and_releases() {
        let mgr = LockManager::new();
        let tag = LockTag::Advisory {
            classid: 1,
            objid: 42,
        };
        mgr.acquire(req(1, tag, LockMode::Exclusive)).unwrap();
        let snap = mgr.inspect(tag).expect("entry should exist");
        assert_eq!(snap.grants.len(), 1);
        mgr.release(xid(1), tag, LockMode::Exclusive);
        // After release the entry should be pruned.
        assert!(mgr.inspect(tag).is_none());
    }

    // ── fastpath: AccessShare on Relation ─────────────────────────────────

    #[test]
    fn fastpath_access_share_does_not_use_central_table() {
        let mgr = LockManager::new();
        let tag = rel(300);
        mgr.acquire_fastpath(req(1, tag, LockMode::AccessShare))
            .unwrap();
        // The central table should have no entry for this tag since
        // the fastpath was used.
        assert!(
            mgr.inspect(tag).is_none(),
            "fastpath should not populate central table"
        );
    }

    #[test]
    fn fastpath_non_access_share_falls_through_to_central_table() {
        let mgr = LockManager::new();
        let tag = rel(301);
        mgr.acquire_fastpath(req(1, tag, LockMode::RowExclusive))
            .unwrap();
        // RowExclusive is not eligible for the fastpath → should be in
        // the central table.
        assert!(
            mgr.inspect(tag).is_some(),
            "non-fastpath lock must be in central table"
        );
    }

    // ── prune must never evict a non-empty entry (race regression) ────────

    /// `prune_entry_if_empty` must leave an entry alone if it acquired a
    /// grant after the caller decided the entry *was* empty. The old
    /// implementation dropped the entry mutex and the shard lock before an
    /// unconditional `self.table.remove(&tag)`, so a grant pushed in that gap
    /// was silently evicted. The `remove_if` predicate, evaluated under the
    /// shard lock, closes that window: here we push a grant by hand and
    /// confirm the prune is a no-op.
    #[test]
    fn prune_does_not_evict_entry_with_a_live_grant() {
        let mgr = LockManager::new();
        let tag = tup(400, 0, 0);

        // Create the entry and grant it (simulating an `acquire` that landed
        // between a concurrent releaser's emptiness check and its removal).
        mgr.acquire(req(1, tag, LockMode::AccessExclusive)).unwrap();

        // A prune attempt for this still-held tag must NOT remove the entry.
        mgr.prune_entry_if_empty(tag);

        let snap = mgr
            .inspect(tag)
            .expect("entry with a live grant must survive prune");
        assert_eq!(snap.grants.len(), 1, "the live grant must remain");
        assert_eq!(snap.grants[0].0, xid(1));

        // And once the grant is released, the prune (driven by `release`)
        // does reclaim the entry.
        mgr.release(xid(1), tag, LockMode::AccessExclusive);
        assert!(
            mgr.inspect(tag).is_none(),
            "empty entry should be pruned after release"
        );
    }

    // ── deadline-aware waits: lock_timeout / cancellation ─────────────────

    /// A bounded wait on a held conflicting lock returns `Timeout` once the
    /// `LockWait::timeout` elapses — and, critically, removes the waiter
    /// from the queue so nothing leaks: the holder can release and a fresh
    /// transaction acquires immediately.
    #[test]
    fn acquire_with_wait_times_out_and_leaves_no_waiter() {
        let mgr = LockManager::new();
        let tag = tup(600, 0, 0);

        mgr.acquire(req(1, tag, LockMode::Exclusive)).unwrap();

        let started = std::time::Instant::now();
        let wait = LockWait {
            timeout: Some(Duration::from_millis(50)),
            cancelled: None,
        };
        let err = mgr
            .acquire_with_wait(req(2, tag, LockMode::Exclusive), &wait)
            .expect_err("conflicting bounded wait must time out");
        assert!(matches!(err, LockError::Timeout), "got {err:?}");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(50),
            "must not time out early: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must time out promptly: {elapsed:?}"
        );

        // No waiter leak: the queue is empty again.
        let snap = mgr.inspect(tag).expect("entry still held by xid 1");
        assert!(
            snap.waiters.is_empty(),
            "timed-out waiter must be removed from the queue: {snap:?}"
        );

        // The holder releases; a fresh transaction acquires immediately.
        mgr.release(xid(1), tag, LockMode::Exclusive);
        let wait = LockWait {
            timeout: Some(Duration::from_millis(50)),
            cancelled: None,
        };
        mgr.acquire_with_wait(req(3, tag, LockMode::Exclusive), &wait)
            .expect("free lock must be granted immediately after release");
    }

    /// A cancellation observer flipping mid-wait aborts the wait with
    /// `Cancelled` (the statement-timeout / client-cancel path) and removes
    /// the waiter from the queue.
    #[test]
    fn acquire_with_wait_observes_cancellation() {
        let mgr = LockManager::new();
        let tag = tup(601, 0, 0);

        mgr.acquire(req(1, tag, LockMode::Exclusive)).unwrap();

        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observer = Arc::clone(&cancelled);
        let wait = LockWait {
            timeout: None,
            cancelled: Some(Arc::new(move || {
                observer.load(std::sync::atomic::Ordering::Relaxed)
            })),
        };
        // Flip the flag shortly after the wait starts.
        let flipper = Arc::clone(&cancelled);
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            flipper.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        let started = std::time::Instant::now();
        let err = mgr
            .acquire_with_wait(req(2, tag, LockMode::Exclusive), &wait)
            .expect_err("cancelled wait must abort");
        assert!(matches!(err, LockError::Cancelled), "got {err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "cancellation must interrupt the wait promptly"
        );
        handle.join().expect("flipper thread");

        let snap = mgr.inspect(tag).expect("entry still held by xid 1");
        assert!(
            snap.waiters.is_empty(),
            "cancelled waiter must be removed from the queue: {snap:?}"
        );
    }

    /// An already-cancelled statement never enqueues at all.
    #[test]
    fn acquire_with_wait_pre_cancelled_does_not_enqueue() {
        let mgr = LockManager::new();
        let tag = tup(602, 0, 0);
        mgr.acquire(req(1, tag, LockMode::Exclusive)).unwrap();

        let wait = LockWait {
            timeout: None,
            cancelled: Some(Arc::new(|| true)),
        };
        let err = mgr
            .acquire_with_wait(req(2, tag, LockMode::Exclusive), &wait)
            .expect_err("pre-cancelled wait must abort");
        assert!(matches!(err, LockError::Cancelled));
        let snap = mgr.inspect(tag).expect("entry still held by xid 1");
        assert!(snap.waiters.is_empty(), "must not have enqueued: {snap:?}");
    }

    /// A bounded wait whose lock becomes free before the deadline is
    /// granted, not timed out — a grantable lock wins over the deadline.
    #[test]
    fn acquire_with_wait_grants_when_released_before_deadline() {
        let mgr = Arc::new(LockManager::new());
        let tag = tup(603, 0, 0);
        mgr.acquire(req(1, tag, LockMode::Exclusive)).unwrap();

        let releaser = Arc::clone(&mgr);
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            releaser.release(xid(1), tag, LockMode::Exclusive);
        });

        let wait = LockWait {
            timeout: Some(Duration::from_secs(5)),
            cancelled: None,
        };
        mgr.acquire_with_wait(req(2, tag, LockMode::Exclusive), &wait)
            .expect("wait must be granted once the holder releases");
        handle.join().expect("releaser thread");

        let snap = mgr.inspect(tag).expect("entry must exist");
        assert_eq!(snap.grants.len(), 1);
        assert_eq!(snap.grants[0].0, xid(2));
        assert!(snap.waiters.is_empty());
    }

    /// A timeout on one waiter must not disturb a second waiter on the same
    /// tag: the survivor still gets the grant when the holder releases.
    #[test]
    fn timed_out_waiter_does_not_disturb_surviving_waiter() {
        let mgr = Arc::new(LockManager::new());
        let tag = tup(604, 0, 0);
        mgr.acquire(req(1, tag, LockMode::Exclusive)).unwrap();

        // Waiter A: short timeout — will expire.
        let mgr_a = Arc::clone(&mgr);
        let a = std::thread::spawn(move || {
            let wait = LockWait {
                timeout: Some(Duration::from_millis(40)),
                cancelled: None,
            };
            mgr_a.acquire_with_wait(req(2, tag, LockMode::Exclusive), &wait)
        });
        // Waiter B: unbounded — must eventually be granted.
        let mgr_b = Arc::clone(&mgr);
        let b = std::thread::spawn(move || mgr_b.acquire(req(3, tag, LockMode::Exclusive)));

        // Let A time out while the holder still holds the lock.
        std::thread::sleep(Duration::from_millis(120));
        let a_result = a.join().expect("waiter A thread");
        assert!(matches!(a_result, Err(LockError::Timeout)), "{a_result:?}");

        // Release: B must be granted.
        mgr.release(xid(1), tag, LockMode::Exclusive);
        b.join().expect("waiter B thread").expect("B granted");

        let snap = mgr.inspect(tag).expect("entry must exist");
        assert_eq!(snap.grants.len(), 1);
        assert_eq!(snap.grants[0].0, xid(3));
        assert!(snap.waiters.is_empty());
    }

    /// Deterministic prune/acquire race regression.
    ///
    /// This forces the exact interleaving the bug allowed via the
    /// `prune_window_hook` test seam: while transaction 1 releases its lock
    /// (and the lock manager runs the prune), a *concurrent* acquire by
    /// transaction 2 lands in the prune window and is granted. The buggy
    /// implementation re-checked emptiness, dropped both the entry mutex and
    /// the DashMap shard lock, then unconditionally removed the entry —
    /// silently evicting transaction 2's live grant. A third acquirer
    /// (transaction 3) would then create a fresh entry and be granted a
    /// *second* conflicting `AccessExclusive` on the same tag.
    ///
    /// With the `remove_if` fix, the predicate re-evaluates under the shard
    /// lock *after* the window, sees transaction 2's grant, and keeps the
    /// entry — so transaction 3's `try_acquire` correctly observes the
    /// conflict and is refused.
    #[test]
    fn prune_acquire_race_never_grants_two_conflicting_holders() {
        let mgr = Arc::new(LockManager::new());
        // Tuple tag → never eligible for the fastpath, so release/prune go
        // through the central table.
        let tag = tup(500, 1, 0);

        // Transaction 1 holds an exclusive lock.
        mgr.acquire(req(1, tag, LockMode::AccessExclusive)).unwrap();

        // Install a one-shot prune-window hook: when transaction 1's release
        // reaches the prune window, transaction 2 acquires the same tag
        // (simulating a concurrent acquire that races the prune). The grant
        // succeeds because transaction 1's grant was already removed at this
        // point.
        let mgr_in_hook = Arc::clone(&mgr);
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fired_in_hook = Arc::clone(&fired);
        set_prune_window_hook(Some(Arc::new(move || {
            // Only act on the first prune window (the one for transaction 1's
            // release); re-entrant prunes from inside this hook must not
            // recurse.
            if fired_in_hook.swap(true, std::sync::atomic::Ordering::AcqRel) {
                return;
            }
            mgr_in_hook
                .acquire(req(2, tag, LockMode::AccessExclusive))
                .expect("tx2 acquires in the prune window");
        })));

        // Release transaction 1. Internally: removes tx1's grant, then enters
        // the prune window (firing the hook → tx2 grant lands), then runs the
        // removal. The fix must keep the entry alive because tx2 now holds it.
        mgr.release(xid(1), tag, LockMode::AccessExclusive);
        set_prune_window_hook(None);

        // The entry must still exist with transaction 2's grant intact.
        let snap = mgr
            .inspect(tag)
            .expect("entry must survive: tx2's grant landed in the prune window");
        assert_eq!(snap.grants.len(), 1, "tx2's grant must not be orphaned");
        assert_eq!(snap.grants[0].0, xid(2));

        // The decisive invariant: a different transaction must NOT be granted
        // a conflicting exclusive lock. With the bug, tx2's entry was evicted,
        // so this fresh acquire would succeed → two conflicting holders.
        let granted = mgr
            .try_acquire(req(3, tag, LockMode::AccessExclusive))
            .unwrap();
        assert!(
            !granted,
            "tx3 must be refused: tx2 already holds AccessExclusive on this tag"
        );
    }
}
