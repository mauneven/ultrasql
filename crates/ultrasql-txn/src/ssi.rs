//! Serializable Snapshot Isolation (SSI) manager.
//!
//! Implements the core Serializable Snapshot Isolation (SSI) conflict graph
//! described in Ports et al. (VLDB 2012).
//! The core idea: two rw-anti-dependency edges that form a pivot (T2 has an
//! in-conflict-in from T1 *and* an in-conflict-out to T3) constitute a
//! dangerous structure if one of the three transactions has committed. When
//! a dangerous structure is detected the pivot transaction is aborted.
//!
//! The manager supports tuple, page, relation, and scalar column-range
//! predicate-lock tags. `ultrasql-server` records column-range tags for the
//! supported scalar comparison subset and relation-level fallback tags when a
//! predicate cannot be bounded safely.
//!
//! # Public surface
//!
//! - [`SsiManager`] — shared state for one database instance.
//! - [`PredicateLock`] / [`PredicateLockTag`] — what a reader locked.
//! - [`SsiError`] — serialization failure with victim XID and detail.
//!
//! # Concurrency
//!
//! All shared state lives inside a [`DashMap`] keyed by [`Xid`].  Each map
//! operation acquires at most one shard; no cross-shard locks are ever held
//! simultaneously.  The dangerous-structure check reads only the entry for
//! `xid` and then consults its `in_conflict_in` / `in_conflict_out` sets,
//! re-reading each referenced entry individually to avoid holding two shard
//! locks at once.

use std::collections::HashSet;

use dashmap::DashMap;
use ultrasql_core::{RelationId, TupleId, Xid};

// Re-export PageId so callers can name `PredicateLockTag::Page` without
// reaching into ultrasql-core directly.
pub use ultrasql_core::PageId;

/// Tag identifying the resource range that a predicate lock covers.
///
/// Granularity escalates from fine (tuple) to coarse (relation) as the
/// number of tuple-level locks held by a transaction grows.  The exact
/// escalation policy is not enforced here; callers issue the escalated lock
/// and may drop finer-grained ones.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PredicateLockTag {
    /// A single heap tuple.
    Tuple(TupleId),
    /// All tuples on one 8 KiB page.
    Page(PageId),
    /// Scalar key range for one relation column.
    ///
    /// Bounds are inclusive. `None` means unbounded on that side. This tag is
    /// used for predicate-aware SSI in the supported integer/bool/timestamp
    /// comparison subset; unsupported predicates fall back to [`Self::Relation`].
    ColumnRange {
        /// Relation being read or written.
        relation: RelationId,
        /// Zero-based column index within the relation schema.
        column: u16,
        /// Inclusive lower bound, or unbounded below.
        low: Option<i64>,
        /// Inclusive upper bound, or unbounded above.
        high: Option<i64>,
    },
    /// All tuples in a relation.
    Relation(RelationId),
}

/// A predicate (read) lock acquired by a serializable transaction.
///
/// Predicate locks are recorded so that concurrent writers can detect whether
/// their write conflicts with a reader's range and register an rw-anti-
/// dependency edge in the conflict graph.
#[derive(Clone, Debug)]
pub struct PredicateLock {
    /// What the transaction locked.
    pub tag: PredicateLockTag,
}

/// Per-transaction SSI bookkeeping.
#[derive(Debug)]
struct SsiState {
    /// XIDs that have a read-write conflict *into* this transaction.
    /// An entry `r ∈ in_conflict_in` means `r` has an rw-anti-dependency
    /// edge pointing at `self` (r read something that self wrote).
    in_conflict_in: HashSet<Xid>,
    /// XIDs that have a read-write conflict *out of* this transaction.
    /// An entry `w ∈ in_conflict_out` means `self` has an rw-anti-dependency
    /// edge pointing at `w` (self read something that w wrote).
    in_conflict_out: HashSet<Xid>,
    /// Predicate locks held by this transaction.
    predicate_locks: Vec<PredicateLock>,
    /// Whether this transaction has committed (as opposed to still active or
    /// aborted).  An aborted transaction's entry is removed entirely;
    /// committed entries are retained until the SSI manager can safely GC them
    /// (currently: until all concurrent transactions that overlap with this
    /// one have terminated).
    committed: bool,
}

impl SsiState {
    fn new() -> Self {
        Self {
            in_conflict_in: HashSet::new(),
            in_conflict_out: HashSet::new(),
            predicate_locks: Vec::new(),
            committed: false,
        }
    }
}

/// Error returned by SSI conflict checks.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SsiError {
    /// A serialization anomaly was detected; `victim` must abort and retry.
    #[error("serialization failure: transaction {victim:?} is the pivot; {detail}")]
    Serialization {
        /// The transaction selected as the abort victim (the pivot).
        victim: Xid,
        /// Human-readable description of the conflict structure.
        detail: String,
    },
}

/// Shared SSI conflict-tracking state for one database instance.
///
/// One [`SsiManager`] is shared across all concurrent serializable
/// transactions via `Arc`.  It tracks the rw-anti-dependency graph and
/// predicate locks; when a dangerous structure is detected it returns
/// [`SsiError::Serialization`] from [`Self::commit`] or
/// [`Self::check_for_dangerous_structure`].
///
/// # Send + Sync
///
/// [`SsiManager`] is `Send + Sync` because [`DashMap`] is `Send + Sync`.
/// No thread-local or `Cell`-backed state is used.
#[derive(Debug)]
pub struct SsiManager {
    rw_conflicts: DashMap<Xid, SsiState>,
}

impl Default for SsiManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SsiManager {
    /// Create a fresh [`SsiManager`] with no registered transactions.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rw_conflicts: DashMap::new(),
        }
    }

    /// Register a new serializable transaction.
    ///
    /// Must be called before any [`Self::add_predicate_lock`] or
    /// [`Self::record_rw_conflict`] for `xid`.
    pub fn register_xid(&self, xid: Xid) {
        self.rw_conflicts.entry(xid).or_insert_with(SsiState::new);
    }

    /// Add a predicate lock for `xid` over the range described by `tag`.
    ///
    /// If `xid` is not registered (e.g. the transaction aborted before this
    /// call), the lock is silently dropped — the transaction will not commit
    /// anyway.
    pub fn add_predicate_lock(&self, xid: Xid, tag: PredicateLockTag) {
        if let Some(mut entry) = self.rw_conflicts.get_mut(&xid) {
            entry.predicate_locks.push(PredicateLock { tag });
        }
    }

    /// Record an rw-anti-dependency edge from `reader` to `writer`.
    ///
    /// `reader` read a row version that was subsequently overwritten by
    /// `writer`.  This means `writer` must not commit before `reader` in any
    /// serializable history, i.e. there is an rw-anti-dependency
    /// reader → writer.
    ///
    /// Silently no-ops if either XID is no longer registered (both must be
    /// alive for the edge to matter).
    pub fn record_rw_conflict(&self, reader: Xid, writer: Xid) {
        // Record `writer` in reader's `in_conflict_out`.
        if let Some(mut r) = self.rw_conflicts.get_mut(&reader) {
            r.in_conflict_out.insert(writer);
        }
        // Record `reader` in writer's `in_conflict_in`.
        if let Some(mut w) = self.rw_conflicts.get_mut(&writer) {
            w.in_conflict_in.insert(reader);
        }
    }

    /// Record rw-anti-dependencies caused by `writer` modifying `tag`.
    ///
    /// Finds every registered serializable transaction whose predicate lock
    /// covers `tag`, excludes `writer` itself, and records `reader --rw-->
    /// writer` for each remaining holder. Returns the readers whose locks
    /// matched in deterministic XID order so callers can expose or test the
    /// conflict edge set.
    pub fn record_write_conflicts(&self, writer: Xid, tag: &PredicateLockTag) -> Vec<Xid> {
        if !self.rw_conflicts.contains_key(&writer) {
            return Vec::new();
        }

        let mut readers = self.find_predicate_lock_holders(tag);
        readers.retain(|reader| *reader != writer);
        readers.sort_unstable();
        readers.dedup();

        for reader in readers.iter().copied() {
            self.record_rw_conflict(reader, writer);
        }

        readers
    }

    /// Check whether `xid` is the pivot of a dangerous structure.
    ///
    /// A dangerous structure exists when:
    ///
    /// ```text
    /// T1 --rw--> T2 --rw--> T3
    /// ```
    ///
    /// where T2 is `xid`, T1 ∈ `xid.in_conflict_in`, T3 ∈ `xid.in_conflict_out`,
    /// and at least one of T1, T2, T3 is committed.
    ///
    /// Returns [`SsiError::Serialization`] with `victim = xid` (the pivot) if
    /// a dangerous structure is found.
    pub fn check_for_dangerous_structure(&self, xid: Xid) -> Result<(), SsiError> {
        // Read xid's state under a shared reference.
        let Some(pivot) = self.rw_conflicts.get(&xid) else {
            return Ok(());
        };

        // Collect the in/out sets and the committed flag without holding the
        // shard lock while we inspect the neighboring entries.
        let in_set: Vec<Xid> = pivot.in_conflict_in.iter().copied().collect();
        let out_set: Vec<Xid> = pivot.in_conflict_out.iter().copied().collect();
        let pivot_committed = pivot.committed;
        drop(pivot); // release shard lock before inspecting neighbors

        if in_set.is_empty() || out_set.is_empty() {
            return Ok(());
        }

        // For each (T1, T3) pair check whether the structure is dangerous.
        //
        // T1 and T3 may be the *same* other transaction: a 2-tx write-skew
        // shows up as `pivot.in_set = pivot.out_set = {Tx}`, which the
        // Cahill SSI paper and PostgreSQL's `predicate.c` both treat as a
        // dangerous structure (the cycle is `pivot ↔ Tx`). The pivot
        // itself is excluded because a self-conflict cannot exist (MVCC
        // never sees its own writes).
        for t1 in &in_set {
            for t3 in &out_set {
                if *t1 == xid || *t3 == xid {
                    continue;
                }

                let Some(t1_entry) = self.rw_conflicts.get(t1) else {
                    continue;
                };
                let t1_committed = t1_entry.committed;
                drop(t1_entry);

                let Some(t3_entry) = self.rw_conflicts.get(t3) else {
                    continue;
                };
                let t3_committed = t3_entry.committed;
                drop(t3_entry);

                // The structure is dangerous if any leg has committed.
                if t1_committed || pivot_committed || t3_committed {
                    return Err(SsiError::Serialization {
                        victim: xid,
                        detail: format!(
                            "dangerous structure detected: \
                             T1={t1:?} --rw--> T2(pivot)={xid:?} --rw--> T3={t3:?}; \
                             committed flags: T1={t1_committed}, \
                             pivot={pivot_committed}, T3={t3_committed}"
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// Commit `xid`.
    ///
    /// Marks the transaction as committed, then performs a final
    /// dangerous-structure check.  Returns [`SsiError::Serialization`] if
    /// a dangerous structure is found after the commit mark is set (the
    /// committed flag is required for the "one leg has committed" condition).
    ///
    /// On error the transaction must abort; callers are responsible for
    /// invoking [`Self::abort`] afterwards.
    pub fn commit(&self, xid: Xid) -> Result<(), SsiError> {
        // Mark committed first so the dangerous-structure check can see it.
        if let Some(mut entry) = self.rw_conflicts.get_mut(&xid) {
            entry.committed = true;
        }
        self.check_for_dangerous_structure(xid)
    }

    /// Abort (or clean up) `xid`.
    ///
    /// Removes the entry from the conflict graph.  Any rw-conflict edges that
    /// referenced this XID are not retroactively cleaned from other entries
    /// — the SSI check already ran or was skipped for those.
    pub fn abort(&self, xid: Xid) {
        self.rw_conflicts.remove(&xid);
    }

    /// Returns the predicate locks held by `xid`, or an empty slice.
    ///
    /// Used by tests and by a future executor layer that needs to check
    /// whether a writer's tuple overlaps a reader's predicate range.
    pub fn predicate_locks(&self, xid: Xid) -> Vec<PredicateLock> {
        self.rw_conflicts
            .get(&xid)
            .map(|e| e.predicate_locks.clone())
            .unwrap_or_default()
    }

    /// Resolve a `LockTag` to the coarsest-matching [`PredicateLockTag`]
    /// that a reader might hold, and check whether any registered transaction
    /// holds a predicate lock that covers it.
    ///
    /// Returns the set of XIDs whose predicate lock covers `tag`.  This is a
    /// building block for the writer-calls-this-to-find-readers pattern; the
    /// caller then calls [`Self::record_rw_conflict`] for each returned XID.
    pub fn find_predicate_lock_holders(&self, tag: &PredicateLockTag) -> Vec<Xid> {
        let mut holders = Vec::new();
        for entry in &self.rw_conflicts {
            let xid = *entry.key();
            for pl in &entry.predicate_locks {
                if predicate_lock_covers(&pl.tag, tag) {
                    holders.push(xid);
                    break;
                }
            }
        }
        holders
    }
}

/// Returns `true` if `held` (a predicate lock already held by a reader)
/// covers the resource described by `requested`.
///
/// Coverage is coarser-wins: a Relation lock covers everything in that
/// relation; a Page lock covers all tuples on that page; a Tuple lock covers
/// only the exact tuple. Broad writer requests are also treated as overlapping
/// finer read locks in the same relation so a conservative writer fallback
/// cannot miss a precise reader.
fn predicate_lock_covers(held: &PredicateLockTag, requested: &PredicateLockTag) -> bool {
    match (held, requested) {
        (PredicateLockTag::Relation(hr), PredicateLockTag::Relation(rr)) => hr == rr,
        (PredicateLockTag::Relation(hr), PredicateLockTag::Page(rp)) => *hr == rp.relation,
        (PredicateLockTag::Relation(hr), PredicateLockTag::Tuple(rt)) => *hr == rt.page.relation,
        (
            PredicateLockTag::Relation(hr),
            PredicateLockTag::ColumnRange {
                relation,
                low,
                high,
                ..
            },
        ) => hr == relation && !range_is_empty(*low, *high),
        (PredicateLockTag::Page(hp), PredicateLockTag::Page(rp)) => hp == rp,
        (PredicateLockTag::Page(hp), PredicateLockTag::Relation(rr)) => hp.relation == *rr,
        (PredicateLockTag::Page(hp), PredicateLockTag::Tuple(rt)) => *hp == rt.page,
        (PredicateLockTag::Tuple(ht), PredicateLockTag::Relation(rr)) => ht.page.relation == *rr,
        (PredicateLockTag::Tuple(ht), PredicateLockTag::Page(rp)) => ht.page == *rp,
        (PredicateLockTag::Tuple(ht), PredicateLockTag::Tuple(rt)) => ht == rt,
        (
            PredicateLockTag::ColumnRange {
                relation,
                low,
                high,
                ..
            },
            PredicateLockTag::Relation(rr),
        ) => *relation == *rr && !range_is_empty(*low, *high),
        (
            PredicateLockTag::ColumnRange {
                relation: held_rel,
                column: held_col,
                low: held_low,
                high: held_high,
            },
            PredicateLockTag::ColumnRange {
                relation: requested_rel,
                column: requested_col,
                low: requested_low,
                high: requested_high,
            },
        ) => {
            held_rel == requested_rel
                && held_col == requested_col
                && ranges_overlap(*held_low, *held_high, *requested_low, *requested_high)
        }
        _ => false,
    }
}

fn ranges_overlap(
    left_low: Option<i64>,
    left_high: Option<i64>,
    right_low: Option<i64>,
    right_high: Option<i64>,
) -> bool {
    if range_is_empty(left_low, left_high) || range_is_empty(right_low, right_high) {
        return false;
    }
    if let (Some(left_high), Some(right_low)) = (left_high, right_low)
        && left_high < right_low
    {
        return false;
    }
    if let (Some(right_high), Some(left_low)) = (right_high, left_low)
        && right_high < left_low
    {
        return false;
    }
    true
}

const fn range_is_empty(low: Option<i64>, high: Option<i64>) -> bool {
    matches!((low, high), (Some(low), Some(high)) if low > high)
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};

    use super::*;

    fn xid(n: u64) -> Xid {
        Xid::new(n)
    }

    fn rel_tag(n: u32) -> PredicateLockTag {
        PredicateLockTag::Relation(RelationId::new(n))
    }

    fn page_tag(rel: u32, block: u32) -> PredicateLockTag {
        let page = PageId::new(RelationId::new(rel), BlockNumber::new(block));
        PredicateLockTag::Page(page)
    }

    fn tuple_tag(rel: u32, block: u32, slot: u16) -> PredicateLockTag {
        let page = PageId::new(RelationId::new(rel), BlockNumber::new(block));
        PredicateLockTag::Tuple(TupleId::new(page, slot))
    }

    fn column_range_tag(
        rel: u32,
        column: u16,
        low: Option<i64>,
        high: Option<i64>,
    ) -> PredicateLockTag {
        PredicateLockTag::ColumnRange {
            relation: RelationId::new(rel),
            column,
            low,
            high,
        }
    }

    // ── basic lifecycle ──────────────────────────────────────────────────────

    #[test]
    fn register_and_commit_without_conflicts_is_ok() {
        let mgr = SsiManager::new();
        mgr.register_xid(xid(1));
        mgr.commit(xid(1)).unwrap();
    }

    #[test]
    fn abort_removes_entry() {
        let mgr = SsiManager::new();
        mgr.register_xid(xid(1));
        mgr.abort(xid(1));
        // A second abort is a no-op and must not panic.
        mgr.abort(xid(1));
    }

    // ── 3-transaction serialization anomaly ─────────────────────────────────

    /// Classic write-skew / rw-anti-dependency cycle:
    ///   T1 reads x, T2 reads y, T1 writes y, T2 writes x.
    ///   In SSI this manifests as T1 --rw--> T3 and T2 --rw--> T3 where
    ///   T3 is the transaction that both readers conflict with.
    ///
    /// Here we model the canonical T1 → T2 → T3 dangerous structure:
    ///   - T1 has a predicate lock and T2 wrote into T1's range (T1 in-in of T2).
    ///   - T2 has a predicate lock and T3 wrote into T2's range (T2 in-in of T3).
    ///
    ///   T2 is therefore the pivot with both an in-conflict-in (from T1) and an
    ///   in-conflict-out (to T3).  When T1 commits, the structure becomes
    ///   dangerous.
    #[test]
    fn three_transaction_serialization_anomaly_detected() {
        let mgr = SsiManager::new();
        let t1 = xid(10);
        let t2 = xid(11);
        let t3 = xid(12);

        mgr.register_xid(t1);
        mgr.register_xid(t2);
        mgr.register_xid(t3);

        // Add predicate locks so the structure is credible.
        mgr.add_predicate_lock(t1, rel_tag(1));
        mgr.add_predicate_lock(t2, rel_tag(1));

        // Record rw-conflict edges:
        //   T1 --rw--> T2  (T2 wrote something T1 read)
        //   T2 --rw--> T3  (T3 wrote something T2 read)
        mgr.record_rw_conflict(t1, t2);
        mgr.record_rw_conflict(t2, t3);

        // Commit T1 — now one of the legs is committed, making the structure
        // dangerous for T2 (the pivot).
        mgr.commit(t1).unwrap(); // T1 commits cleanly

        // T2 is the pivot: in-conflict-in = {T1}, in-conflict-out = {T3}.
        // T1 is committed → dangerous structure → T2 must abort.
        let err = mgr
            .check_for_dangerous_structure(t2)
            .expect_err("should detect dangerous structure");
        let SsiError::Serialization { victim, .. } = err;
        assert_eq!(victim, t2, "T2 is the pivot and must be the victim");
    }

    /// No conflict edges means no dangerous structure.
    #[test]
    fn no_conflicts_no_dangerous_structure() {
        let mgr = SsiManager::new();
        mgr.register_xid(xid(1));
        mgr.register_xid(xid(2));
        mgr.register_xid(xid(3));

        // No record_rw_conflict calls.

        mgr.commit(xid(1)).unwrap();
        mgr.check_for_dangerous_structure(xid(2)).unwrap();
    }

    /// Only one edge (T1→T2) without a second (T2→T3) is not dangerous.
    #[test]
    fn single_edge_not_dangerous() {
        let mgr = SsiManager::new();
        mgr.register_xid(xid(1));
        mgr.register_xid(xid(2));
        mgr.register_xid(xid(3));

        mgr.record_rw_conflict(xid(1), xid(2));
        mgr.commit(xid(1)).unwrap();

        // T2 has in-conflict-in = {T1} but in-conflict-out = {} → no pivot.
        mgr.check_for_dangerous_structure(xid(2)).unwrap();
    }

    // ── safe-snapshot optimisation ───────────────────────────────────────────

    /// A transaction that committed before any concurrent serializable
    /// transaction began has no edges and is trivially safe.  Simulated here
    /// by committing T1 before creating T2.
    #[test]
    fn safe_snapshot_no_conflict_after_prior_commit() {
        let mgr = SsiManager::new();
        // T1 commits first with no edges.
        mgr.register_xid(xid(1));
        mgr.commit(xid(1)).unwrap();

        // T2 starts after T1 committed; no rw-conflict edges exist for T2.
        mgr.register_xid(xid(2));
        mgr.commit(xid(2)).unwrap();
    }

    // ── predicate lock granularity ───────────────────────────────────────────

    #[test]
    fn relation_predicate_covers_page_and_tuple() {
        let mgr = SsiManager::new();
        let t1 = xid(20);
        mgr.register_xid(t1);
        mgr.add_predicate_lock(t1, rel_tag(5));

        // A writer on a page within relation 5 should find T1 as a holder.
        let holders_page = mgr.find_predicate_lock_holders(&page_tag(5, 0));
        assert!(
            holders_page.contains(&t1),
            "relation lock should cover page queries"
        );

        // A writer on a tuple within relation 5 should also find T1.
        let holders_tuple = mgr.find_predicate_lock_holders(&tuple_tag(5, 0, 0));
        assert!(
            holders_tuple.contains(&t1),
            "relation lock should cover tuple queries"
        );

        // A query for a different relation should not match.
        let holders_other = mgr.find_predicate_lock_holders(&page_tag(6, 0));
        assert!(
            !holders_other.contains(&t1),
            "relation lock on rel 5 should not cover rel 6"
        );
    }

    #[test]
    fn page_predicate_covers_tuple_but_not_other_page() {
        let mgr = SsiManager::new();
        let t1 = xid(21);
        mgr.register_xid(t1);
        mgr.add_predicate_lock(t1, page_tag(1, 3));

        // A tuple on page (1, 3) should match.
        let holders = mgr.find_predicate_lock_holders(&tuple_tag(1, 3, 0));
        assert!(
            holders.contains(&t1),
            "page lock should cover tuples on that page"
        );

        // A tuple on a different page should not match.
        let holders2 = mgr.find_predicate_lock_holders(&tuple_tag(1, 4, 0));
        assert!(
            !holders2.contains(&t1),
            "page lock on block 3 should not cover block 4"
        );
    }

    #[test]
    fn tuple_predicate_exact_match_only() {
        let mgr = SsiManager::new();
        let t1 = xid(22);
        mgr.register_xid(t1);
        mgr.add_predicate_lock(t1, tuple_tag(1, 0, 7));

        // Exact match.
        let holders = mgr.find_predicate_lock_holders(&tuple_tag(1, 0, 7));
        assert!(holders.contains(&t1), "exact tuple match");

        // Different slot → no match.
        let holders2 = mgr.find_predicate_lock_holders(&tuple_tag(1, 0, 8));
        assert!(!holders2.contains(&t1), "different slot should not match");
    }

    #[test]
    fn column_range_predicate_matches_overlap_only() {
        let mgr = SsiManager::new();
        let t1 = xid(23);
        mgr.register_xid(t1);
        mgr.add_predicate_lock(t1, column_range_tag(7, 0, Some(10), Some(20)));

        let overlapping = mgr.find_predicate_lock_holders(&column_range_tag(7, 0, Some(20), None));
        assert_eq!(overlapping, vec![t1]);

        let disjoint = mgr.find_predicate_lock_holders(&column_range_tag(7, 0, Some(21), None));
        assert!(disjoint.is_empty());

        let other_column =
            mgr.find_predicate_lock_holders(&column_range_tag(7, 1, Some(10), Some(20)));
        assert!(other_column.is_empty());
    }

    #[test]
    fn empty_column_range_predicate_matches_nothing() {
        let mgr = SsiManager::new();
        let t1 = xid(25);
        mgr.register_xid(t1);
        mgr.add_predicate_lock(t1, column_range_tag(7, 0, Some(10), Some(9)));

        assert!(
            mgr.find_predicate_lock_holders(&column_range_tag(7, 0, Some(9), Some(10)))
                .is_empty()
        );
        assert!(mgr.find_predicate_lock_holders(&rel_tag(7)).is_empty());
    }

    #[test]
    fn broad_relation_write_matches_precise_column_reader() {
        let mgr = SsiManager::new();
        let t1 = xid(24);
        mgr.register_xid(t1);
        mgr.add_predicate_lock(t1, column_range_tag(7, 0, Some(10), Some(20)));

        let holders = mgr.find_predicate_lock_holders(&rel_tag(7));
        assert_eq!(holders, vec![t1]);
    }

    // ── commit triggers dangerous-structure check ─────────────────────────────

    #[test]
    fn commit_detects_dangerous_structure_for_pivot() {
        let mgr = SsiManager::new();
        let t1 = xid(30);
        let t2 = xid(31);
        let t3 = xid(32);

        mgr.register_xid(t1);
        mgr.register_xid(t2);
        mgr.register_xid(t3);

        // Build the dangerous structure with t2 as pivot.
        mgr.record_rw_conflict(t1, t2);
        mgr.record_rw_conflict(t2, t3);

        // Commit t3 first (the right-side committed leg).
        mgr.commit(t3).unwrap();

        // Now t2 tries to commit — it is the pivot and t3 is committed.
        let err = mgr.commit(t2).expect_err("pivot commit should fail");
        let SsiError::Serialization { victim, .. } = err;
        assert_eq!(victim, t2);
    }
}
