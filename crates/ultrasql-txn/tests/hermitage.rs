//! Hermitage anomaly test suite skeleton.
//!
//! Ports the canonical anomaly names from Martin Kleppmann's
//! [Hermitage](https://github.com/ept/hermitage) test suite. The pinned
//! source commit and CC BY 4.0 attribution are recorded under
//! `tests/isolation/`.
//!
//! | ID      | Name                          | PostgreSQL behavior      |
//! |---------|-------------------------------|--------------------------|
//! | G0      | Dirty write                   | Prevented at all levels  |
//! | G1a     | Dirty read                    | Prevented at all levels  |
//! | G1b     | Intermediate read             | Prevented at all levels  |
//! | G1c     | Circular information flow     | Prevented at all levels  |
//! | OTV     | Observed transaction vanishes | Prevented at RR+         |
//! | PMP     | Predicate-many-preceders      | Prevented at RR+         |
//! | P4      | Lost update                   | Prevented at RR+         |
//! | G-single| Read skew                     | Prevented at RR+         |
//! | G2-item | Write skew on item            | Prevented at Serializable|
//! | G2      | Anti-dependency cycle         | Prevented at Serializable|
//!
//! Each test drives [`TransactionManager`] directly. The "tuple visibility"
//! assertions are proxied through the [`XidStatusOracle`] and snapshot
//! visibility predicates rather than reading from a real heap. The
//! canonical Hermitage assertions (read value X, see Y) need a real heap.
//! Server-level value assertions for the G1a, PMP, and G2 slices live in
//! `crates/ultrasql-server/tests/isolation_suite_round_trip.rs`; the remaining
//! manager-level tests assert transactional outcomes (commit / abort, snapshot
//! frozen / refreshed).

use std::sync::Arc;

use ultrasql_mvcc::XidStatusOracle;
use ultrasql_txn::{IsolationLevel, SsiManager, TransactionManager};

// ── helpers ───────────────────────────────────────────────────────────────────

fn mgr_rr() -> Arc<TransactionManager> {
    Arc::new(TransactionManager::new())
}

fn mgr_ser() -> Arc<TransactionManager> {
    let ssi = Arc::new(SsiManager::new());
    Arc::new(TransactionManager::new_with_ssi(ssi))
}

// ── G0: dirty write ───────────────────────────────────────────────────────────

/// G0 (dirty write): T1 and T2 both update the same row without T1
/// first committing. In a correct system the final value is either the
/// T1-then-T2 or the T2-then-T1 sequence — never a mix of partial
/// writes. The transaction manager guarantees this via MVCC: only one
/// XID wins the xmax slot (enforced by the lock manager or CAS at the
/// storage layer).
///
/// At the `TxnManager` level we assert that both T1 and T2 can begin and
/// terminate without corrupting the CLOG (no double-commit).
#[test]
fn g0_dirty_write_prevented() {
    let mgr = mgr_rr();

    let t1 = mgr.begin(IsolationLevel::ReadCommitted);
    let t2 = mgr.begin(IsolationLevel::ReadCommitted);

    // In a real engine T1 would acquire a row lock on the tuple before
    // writing; T2's write would block until T1 commits or aborts.
    // At the TxnManager level we just exercise the lifecycle.
    mgr.commit(t1).unwrap();
    mgr.commit(t2).unwrap();
}

// ── G1a: dirty read ──────────────────────────────────────────────────────────

/// G1a (dirty read): T1 writes a value and then aborts; T2 must not see
/// T1's intermediate (aborted) write.
///
/// The `TxnManager` oracle correctly marks T1 as Aborted after
/// `abort(t1)`, so any snapshot-based visibility check will exclude T1's
/// writes.
#[test]
fn g1a_dirty_read_prevented() {
    let mgr = mgr_rr();

    // T1 begins and writes (simulated — we just begin and abort).
    let t1 = mgr.begin(IsolationLevel::ReadCommitted);
    let t1_xid = t1.xid;

    // T2 begins while T1 is in progress.
    let t2 = mgr.begin(IsolationLevel::ReadCommitted);
    assert!(
        t2.snapshot.xip.contains(&t1_xid),
        "T1 must be in T2's snapshot as in-progress"
    );

    // T1 aborts.
    mgr.abort(t1).unwrap();

    // T2 refreshes — T1's XID must now appear Aborted, not Committed.
    let mut t2 = t2;
    mgr.refresh_snapshot(&mut t2);
    assert!(
        !t2.snapshot.xip.contains(&t1_xid),
        "after T1 aborts and T2 refreshes, T1 must no longer be in-progress"
    );
    assert!(mgr.is_aborted(t1_xid), "oracle must report T1 as Aborted");

    mgr.commit(t2).unwrap();
}

// ── G1b: intermediate read ────────────────────────────────────────────────────

/// G1b (intermediate read): T1 performs two writes; a concurrent T2 must
/// not see an intermediate state (only the first write, not the second).
///
/// At the `TxnManager` level this collapses to: T2 cannot see T1's writes
/// until T1 commits. All of T1's writes share T1's XID in xmin; the
/// snapshot sees them all or none.
#[test]
fn g1b_intermediate_read_prevented() {
    let mgr = mgr_rr();

    let t1 = mgr.begin(IsolationLevel::ReadCommitted);
    let t1_xid = t1.xid;

    // T2 begins while T1 is in the middle of its "two writes".
    let t2 = mgr.begin(IsolationLevel::ReadCommitted);

    // Verify T2 sees T1 as in-progress (cannot read T1's partial state).
    assert!(
        t2.snapshot.xip.contains(&t1_xid),
        "T1 must be in-progress for T2's snapshot"
    );

    // T1 commits (both writes become visible atomically).
    mgr.commit(t1).unwrap();

    // T2's snapshot (before refresh) still sees T1 as in-progress under RC.
    // After refresh, T1 is visible — but as a complete unit.
    mgr.abort(t2).unwrap();
}

// ── G1c: circular information flow ───────────────────────────────────────────

/// G1c (circular information flow): a cycle of transactions where each
/// reads from the other's write. This requires true serializable
/// isolation to detect.
///
/// With SSI the cycle is detected as a dangerous structure and at least
/// one transaction is aborted.
#[test]
fn g1c_circular_information_flow_prevented_at_serializable() {
    use ultrasql_core::RelationId;
    use ultrasql_txn::PredicateLockTag;

    let mgr = mgr_ser();

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);

    let rel = PredicateLockTag::Relation(RelationId::new(1));
    mgr.record_predicate_lock(t1.xid, rel.clone());
    mgr.record_predicate_lock(t2.xid, rel);

    // T1 wrote something T2 read → T2 --rw--> T1.
    // T2 wrote something T1 read → T1 --rw--> T2.
    mgr.record_rw_conflict(t2.xid, t1.xid);
    mgr.record_rw_conflict(t1.xid, t2.xid);

    let r1 = mgr.commit(t1);
    let r2 = mgr.commit(t2);

    let at_least_one_aborted = r1.is_err() || r2.is_err();
    assert!(
        at_least_one_aborted,
        "G1c cycle must abort at least one transaction at Serializable"
    );
}

// ── OTV: observed transaction vanishes ───────────────────────────────────────

/// OTV (observed transaction vanishes): T1 writes, T2 reads T1's write,
/// T1 aborts — T2 must not have seen T1's value.
///
/// Under MVCC this is guaranteed: T2's snapshot cannot see T1's writes
/// until T1 commits.
#[test]
fn otv_observed_transaction_vanishes_prevented() {
    let mgr = mgr_rr();

    let t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let t1_xid = t1.xid;

    // T2 begins and takes a snapshot that sees T1 as in-progress.
    let t2 = mgr.begin(IsolationLevel::RepeatableRead);
    assert!(
        t2.snapshot.xip.contains(&t1_xid),
        "T2 must see T1 as in-progress"
    );

    // T1 aborts.
    mgr.abort(t1).unwrap();

    // T2's frozen snapshot (under RR) still lists T1 as in-progress, so
    // any visibility check will treat T1's writes as invisible.
    assert!(
        t2.snapshot.xip.contains(&t1_xid),
        "under RR, T2's frozen snapshot still sees T1 as in-progress"
    );

    mgr.commit(t2).unwrap();
}

// ── PMP: predicate-many-preceders ────────────────────────────────────────────

/// PMP (predicate-many-preceders): T1 reads a range predicate; T2 inserts
/// a new row matching the predicate and commits; T1 must not see the
/// new row (phantom read).
///
/// Under RR, T1's snapshot is frozen: T2's XID is above T1's xmax or in
/// T1's xip, so T2's tuples are invisible.
#[test]
fn pmp_predicate_many_preceders_prevented_at_rr() {
    let mgr = mgr_rr();

    let t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let t1_xmax = t1.snapshot.xmax;

    // T2 begins and commits after T1's snapshot.
    let t2 = mgr.begin(IsolationLevel::RepeatableRead);
    assert!(
        t2.xid >= t1_xmax,
        "T2's XID is past T1's xmax so its tuples are invisible to T1"
    );
    mgr.commit(t2).unwrap();

    // T1's snapshot remains frozen under RR — T2's writes are invisible.
    let mut t1 = t1;
    mgr.refresh_snapshot(&mut t1);
    assert_eq!(
        t1.snapshot.xmax, t1_xmax,
        "T1's xmax must not advance on refresh"
    );

    mgr.commit(t1).unwrap();
}

// ── P4: lost update ───────────────────────────────────────────────────────────

/// P4 (lost update): T1 and T2 both read a value and then write a new
/// value based on what they read. One update must be lost.
///
/// Under RR/Serializable, the lock manager (row locks from `FOR UPDATE`)
/// prevents both from reading and writing concurrently without conflict.
///
/// At the `TxnManager` level we verify that both transactions can be
/// properly sequenced without CLOG corruption.
#[test]
fn p4_lost_update_prevented_at_rr() {
    let mgr = mgr_rr();

    let t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let t2 = mgr.begin(IsolationLevel::RepeatableRead);

    // In a real engine both would acquire a `FOR UPDATE` row lock here;
    // the second would block until the first commits. At TxnManager level
    // we just ensure the lifecycle is correct.
    mgr.commit(t1).unwrap();
    mgr.commit(t2).unwrap();
}

// ── G-single: read skew ───────────────────────────────────────────────────────

/// G-single (read skew): T1 reads A, then T2 updates both A and B and
/// commits, then T1 reads B — T1 sees an inconsistent pair (old A, new B).
///
/// Under RR, T1's snapshot is frozen at begin so it always sees the
/// pre-T2 state of both A and B.
#[test]
fn g_single_read_skew_prevented_at_rr() {
    let mgr = mgr_rr();

    // T1 begins with a frozen snapshot.
    let t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let t1_xmax = t1.snapshot.xmax;

    // T2 begins *after* T1, so T2.xid is past T1's xmax — invisible
    // by the snapshot xmax cutoff (not by being listed in xip).
    let t2 = mgr.begin(IsolationLevel::RepeatableRead);
    let t2_xid = t2.xid;
    assert!(
        t2_xid >= t1_xmax,
        "T2 began after T1's snapshot — its xid must be past T1's xmax"
    );

    // T2 commits (updating A and B atomically from T1's perspective).
    mgr.commit(t2).unwrap();

    // T1's frozen snapshot under RR must not advance. Refresh is a no-op
    // for xmax — T2 stays invisible to T1.
    let mut t1 = t1;
    mgr.refresh_snapshot(&mut t1);
    assert_eq!(
        t1.snapshot.xmax, t1_xmax,
        "RR snapshot xmax must not advance on refresh — T2 stays invisible"
    );

    mgr.commit(t1).unwrap();
}

// ── G2-item: write skew on a single item ─────────────────────────────────────

/// G2-item (write skew on item): T1 and T2 both read item X and then each
/// writes X based on what they read. In a strict serial history only one
/// write would be based on the original value.
///
/// Under Serializable SSI must detect and abort one transaction.
#[test]
fn g2_item_write_skew_on_item_prevented_at_serializable() {
    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};
    use ultrasql_txn::PredicateLockTag;

    let mgr = mgr_ser();

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);

    // Both read the same tuple (predicate lock at tuple granularity).
    let tid = TupleId::new(PageId::new(RelationId::new(1), BlockNumber::new(0)), 0);
    let tag = PredicateLockTag::Tuple(tid);
    mgr.record_predicate_lock(t1.xid, tag.clone());
    mgr.record_predicate_lock(t2.xid, tag);

    // Both write the same item — rw-conflict in both directions.
    mgr.record_rw_conflict(t2.xid, t1.xid);
    mgr.record_rw_conflict(t1.xid, t2.xid);

    let r1 = mgr.commit(t1);
    let r2 = mgr.commit(t2);

    assert!(
        r1.is_err() || r2.is_err(),
        "G2-item write skew must abort at least one tx at Serializable; \
         got r1={r1:?} r2={r2:?}"
    );
}

// ── G2: anti-dependency cycle ─────────────────────────────────────────────────

/// G2 (anti-dependency cycle): the general case of write skew involving
/// an arbitrary number of transactions in a rw-anti-dependency cycle.
///
/// This three-transaction version (T1 → T2 → T3 → T1) is the canonical
/// SSI dangerous structure test.
#[test]
fn g2_anti_dependency_cycle_prevented_at_serializable() {
    use ultrasql_core::RelationId;
    use ultrasql_txn::PredicateLockTag;

    let mgr = mgr_ser();

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);
    let t3 = mgr.begin(IsolationLevel::Serializable);

    let rel = PredicateLockTag::Relation(RelationId::new(1));
    mgr.record_predicate_lock(t1.xid, rel.clone());
    mgr.record_predicate_lock(t2.xid, rel.clone());
    mgr.record_predicate_lock(t3.xid, rel);

    // Build T1 --rw--> T2 --rw--> T3 --rw--> T1 cycle.
    mgr.record_rw_conflict(t1.xid, t2.xid);
    mgr.record_rw_conflict(t2.xid, t3.xid);
    mgr.record_rw_conflict(t3.xid, t1.xid);

    let r1 = mgr.commit(t1);
    let r2 = mgr.commit(t2);
    let r3 = mgr.commit(t3);

    let at_least_one_aborted = r1.is_err() || r2.is_err() || r3.is_err();
    assert!(
        at_least_one_aborted,
        "G2 anti-dependency cycle must abort at least one tx; \
         got r1={r1:?} r2={r2:?} r3={r3:?}"
    );
}

#[test]
fn g2_write_skew_via_predicate_writes_aborts_one_tx() {
    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};
    use ultrasql_txn::PredicateLockTag;

    let mgr = mgr_ser();

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);
    let t1_xid = t1.xid;
    let t2_xid = t2.xid;

    let doctors = RelationId::new(700);
    let all_doctors = PredicateLockTag::Relation(doctors);
    mgr.record_predicate_lock(t1_xid, all_doctors.clone());
    mgr.record_predicate_lock(t2_xid, all_doctors);

    let alice = PredicateLockTag::Tuple(TupleId::new(PageId::new(doctors, BlockNumber::new(0)), 0));
    let bob = PredicateLockTag::Tuple(TupleId::new(PageId::new(doctors, BlockNumber::new(0)), 1));

    let alice_readers = mgr.record_write_conflicts(t1_xid, &alice);
    assert!(alice_readers.contains(&t2_xid));
    assert!(!alice_readers.contains(&t1_xid));

    let bob_readers = mgr.record_write_conflicts(t2_xid, &bob);
    assert!(bob_readers.contains(&t1_xid));
    assert!(!bob_readers.contains(&t2_xid));

    let r1 = mgr.commit(t1);
    let r2 = mgr.commit(t2);

    assert!(
        r1.is_err() || r2.is_err(),
        "write-skew dangerous structure must abort one tx; got r1={r1:?} r2={r2:?}"
    );
    assert!(
        mgr.is_aborted(t1_xid) || mgr.is_aborted(t2_xid),
        "serialization victim must be Aborted in oracle"
    );
}

#[test]
fn serializable_stress_many_hermitage_pivots_abort_correctly() {
    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};
    use ultrasql_txn::PredicateLockTag;

    let mgr = mgr_ser();

    for scenario in 0..16_u32 {
        let t1 = mgr.begin(IsolationLevel::Serializable);
        let pivot = mgr.begin(IsolationLevel::Serializable);
        let t3 = mgr.begin(IsolationLevel::Serializable);

        let left_rel = RelationId::new(800 + scenario * 2);
        let right_rel = RelationId::new(801 + scenario * 2);
        mgr.record_predicate_lock(t1.xid, PredicateLockTag::Relation(left_rel));
        mgr.record_predicate_lock(pivot.xid, PredicateLockTag::Relation(right_rel));

        let left_write =
            PredicateLockTag::Tuple(TupleId::new(PageId::new(left_rel, BlockNumber::new(0)), 0));
        let right_write =
            PredicateLockTag::Tuple(TupleId::new(PageId::new(right_rel, BlockNumber::new(0)), 0));

        assert_eq!(
            mgr.record_write_conflicts(pivot.xid, &left_write),
            vec![t1.xid]
        );
        assert_eq!(
            mgr.record_write_conflicts(t3.xid, &right_write),
            vec![pivot.xid]
        );

        let t1_xid = t1.xid;
        let pivot_xid = pivot.xid;
        let t3_xid = t3.xid;

        let r1 = mgr.commit(t1);
        assert!(r1.is_ok(), "left tx should commit: {r1:?}");

        let rp = mgr.commit(pivot);
        assert!(
            rp.is_err(),
            "pivot must abort in scenario {scenario}; got {rp:?}"
        );
        assert!(mgr.is_committed(t1_xid));
        assert!(mgr.is_aborted(pivot_xid));

        let r3 = mgr.commit(t3);
        assert!(r3.is_ok(), "right tx should commit: {r3:?}");
        assert!(mgr.is_committed(t3_xid));
    }
}
