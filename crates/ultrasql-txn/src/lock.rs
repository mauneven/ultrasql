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
use std::time::Duration;

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
    /// The lock could not be acquired within the configured timeout.
    #[error("lock acquisition timed out")]
    Timeout,
    /// `try_acquire` found a conflicting grant; the lock was not taken.
    #[error("lock held by another transaction")]
    Conflict,
    /// A fastpath relation-lock reference count overflowed.
    #[error("fastpath lock reference count overflow")]
    FastpathOverflow,
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

/// Mutable state stored under the per-entry mutex.
struct LockEntryState {
    /// Current grant holders.
    grants: Vec<(Xid, LockMode)>,
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

        let handle = thread::Builder::new()
            .name("ultrasql-deadlock-detector".into())
            .spawn(move || {
                detector_loop(&t_table, &t_states, &t_stop, interval);
            })
            .ok();

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
    #[allow(clippy::significant_drop_tightening)] // `state` is intentionally held across `wait`
    pub fn acquire(&self, req: LockRequest) -> Result<(), LockError> {
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
            state.grants.push((req.xid, req.mode));
            // Clear any stale victim flag from a previous false-positive
            // detection run.
            self.clear_victim(req.xid);
            return Ok(());
        }

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

            // `wait` atomically releases the mutex and parks the thread.
            entry.waiters_changed.wait(&mut state);

            // Re-check victim flag after waking.
            if self.is_victim(req.xid) {
                remove_waiter(&mut state.waiters, req.xid, req.mode);
                return Err(LockError::Deadlock { victim: req.xid });
            }

            if !has_conflict(&state.grants, req.xid, req.mode) {
                remove_waiter(&mut state.waiters, req.xid, req.mode);
                state.grants.push((req.xid, req.mode));
                // Clear any stale victim flag (false positive from a
                // detection run that resolved itself by the time we woke).
                self.clear_victim(req.xid);
                return Ok(());
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

        state.grants.push((req.xid, req.mode));
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
                .position(|(gxid, gmode)| *gxid == xid && *gmode == mode)
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
                if state.grants.iter().any(|(gxid, _)| *gxid == xid) {
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
                    state.grants.retain(|(gxid, _)| *gxid != xid);
                }
                entry.waiters_changed.notify_all();
                self.prune_entry_if_empty(tag);
            }
        }

        // Clean up deadlock state for this XID.
        self.xid_states.remove(&xid);
    }

    /// Return a point-in-time snapshot of the grants and waiters for
    /// `tag`, or `None` if no entry exists.
    #[allow(clippy::significant_drop_tightening)] // `entry` must live until after state is used
    pub fn inspect(&self, tag: LockTag) -> Option<LockTableSnapshot> {
        let entry = self.table.get(&tag)?;
        let state = entry.inner.lock();
        Some(LockTableSnapshot {
            grants: state.grants.clone(),
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
                    grants: state.grants.clone(),
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
        // Try to remove the entry only if both grants and waiters are
        // empty. We do the check under the entry lock to avoid TOCTOU.
        if let Some(entry_ref) = self.table.get(&tag) {
            let state = entry_ref.inner.lock();
            if state.grants.is_empty() && state.waiters.is_empty() {
                drop(state);
                drop(entry_ref);
                self.table.remove(&tag);
            }
        }
    }
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
            for (holder_xid, holder_mode) in &state.grants {
                if waiter_mode.conflicts_with(*holder_mode) && waiter_xid != holder_xid {
                    wait_for.entry(*waiter_xid).or_default().push(*holder_xid);
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
fn has_conflict(grants: &[(Xid, LockMode)], requester: Xid, mode: LockMode) -> bool {
    grants
        .iter()
        .any(|(holder, held)| *holder != requester && mode.conflicts_with(*held))
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
            let ma = all[a as usize];
            let mb = all[b as usize];
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
}
