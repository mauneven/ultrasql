//! Isolation level integration tests.
//!
//! Each test drives two interleaved transactions against a real
//! [`TransactionManager`] and asserts the documented behavior for the
//! three isolation levels.
//!
//! # Scenarios
//!
//! - **Read Committed**: T1 sees T2's commit after acquiring a fresh snapshot
//!   via [`TransactionManager::refresh_snapshot`].
//! - **Repeatable Read**: T1's snapshot is frozen at begin; T2's commit is
//!   invisible to T1 until T1 terminates.
//! - **Serializable**: A write-skew anomaly is detected by the SSI manager and
//!   causes one of the conflicting transactions to fail with
//!   [`TxnError::SerializationFailure`].
//!
//! Tests use `std::thread` for interleaving.  The `loom` crate is a dev-dep
//! on `ultrasql-txn` (see workspace) but is not used here because the
//! isolation-level properties can be observed with deterministic sequencing
//! and barriers — no exhaustive model checking is needed for these contracts.

use std::sync::Arc;

use ultrasql_mvcc::XidStatusOracle;
use ultrasql_txn::{IsolationLevel, SsiManager, TransactionManager};

// ── Read Committed ────────────────────────────────────────────────────────────

/// RC contract: a statement that runs *after* a concurrent transaction
/// commits sees the committed data.
///
/// Scenario:
///   T0: begin(RC)  ← T0 is started first to become part of T1's xip
///   T1: begin(RC)  ← T1 sees T0 as in-progress
///   T0: commit
///   T1: `refresh_snapshot` → T0 no longer in xip
#[test]
fn read_committed_sees_concurrent_commit_after_refresh() {
    let mgr = Arc::new(TransactionManager::new());

    // T0 starts first — it will be in T1's initial xip.
    let t0 = mgr.begin(IsolationLevel::ReadCommitted);
    let t0_xid = t0.xid;

    // T1 begins after T0; T1's snapshot has T0 in xip.
    let t1 = mgr.begin(IsolationLevel::ReadCommitted);

    assert!(
        t1.snapshot.xip.contains(&t0_xid),
        "T0 must be in T1's initial snapshot xip"
    );

    // T0 commits.
    mgr.commit(t0).unwrap();

    // T1 refreshes its snapshot (new statement starts).
    let mut t1 = t1;
    mgr.refresh_snapshot(&mut t1);

    // T1's new snapshot must no longer include T0 (it committed).
    assert!(
        !t1.snapshot.xip.contains(&t0_xid),
        "after refresh, T0 must be absent from T1's refreshed snapshot"
    );
    // The oracle confirms T0 is committed.
    assert!(
        mgr.is_committed(t0_xid),
        "T0 must be committed in the oracle"
    );

    mgr.abort(t1).unwrap();
}

/// RC contract: without a refresh, the old snapshot still sees T0 as
/// in-progress even after T0 commits.
#[test]
fn read_committed_without_refresh_sees_stale_snapshot() {
    let mgr = Arc::new(TransactionManager::new());

    // T0 starts first.
    let t0 = mgr.begin(IsolationLevel::ReadCommitted);
    let t0_xid = t0.xid;

    // T1 starts; its snapshot sees T0 as in-progress.
    let t1 = mgr.begin(IsolationLevel::ReadCommitted);
    assert!(
        t1.snapshot.xip.contains(&t0_xid),
        "T0 must be in T1's initial snapshot"
    );

    // T0 commits.
    mgr.commit(t0).unwrap();

    // T1 has NOT refreshed — its snapshot still sees T0 as in-progress.
    assert!(
        t1.snapshot.xip.contains(&t0_xid),
        "without refresh, T0 still appears in-progress in T1's old snapshot"
    );

    mgr.abort(t1).unwrap();
}

// ── Repeatable Read ───────────────────────────────────────────────────────────

/// RR contract: T1's snapshot is frozen at begin and does not change on
/// refresh. T0 (started before T1) remains in-progress in T1's frozen
/// snapshot even after T0 commits.
///
/// Scenario:
///   T0: begin(RR)
///   T1: begin(RR) — T0 is in T1's initial xip
///   T0: commit
///   T1: refresh — snapshot must not change (RR invariant)
#[test]
fn repeatable_read_snapshot_frozen_after_begin() {
    let mgr = Arc::new(TransactionManager::new());

    // T0 starts first so it lands in T1's xip.
    let t0 = mgr.begin(IsolationLevel::RepeatableRead);
    let t0_xid = t0.xid;

    let t1 = mgr.begin(IsolationLevel::RepeatableRead);

    // T0 must be visible as in-progress at the start.
    assert!(
        t1.snapshot.xip.contains(&t0_xid),
        "T0 must be in T1's initial snapshot"
    );

    // T0 commits.
    mgr.commit(t0).unwrap();

    // Capture snapshot state before refresh.
    let xmin_before = t1.snapshot.xmin;
    let xmax_before = t1.snapshot.xmax;
    let xip_before: Vec<_> = t1.snapshot.xip.iter().copied().collect();

    let mut t1 = t1;
    mgr.refresh_snapshot(&mut t1);

    assert_eq!(
        t1.snapshot.xmin, xmin_before,
        "RR snapshot xmin must not change on refresh"
    );
    assert_eq!(
        t1.snapshot.xmax, xmax_before,
        "RR snapshot xmax must not change on refresh"
    );
    let xip_after: Vec<_> = t1.snapshot.xip.iter().copied().collect();
    assert_eq!(
        xip_after, xip_before,
        "RR snapshot xip must not change on refresh"
    );
    // T0 is still considered in-progress by T1's frozen snapshot.
    assert!(
        t1.snapshot.xip.contains(&t0_xid),
        "under RR, T0 remains in T1's frozen snapshot even after committing"
    );

    mgr.abort(t1).unwrap();
}

/// RR anti-anomaly: phantom reads cannot occur. T1 reads a set of XIDs at
/// begin; no new XID appears in-progress if T3 begins and commits
/// while T1 is running (because T1's xmax was already past T3).
#[test]
fn repeatable_read_no_phantom_from_later_begin() {
    let mgr = Arc::new(TransactionManager::new());

    // T1 begins. Its xmax captures the current high-water mark.
    let t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let t1_xmax = t1.snapshot.xmax;

    // T3 begins and commits *after* T1's begin.
    let t3 = mgr.begin(IsolationLevel::RepeatableRead);
    assert!(
        t3.xid >= t1_xmax,
        "T3's XID must be >= T1's xmax (T3 started after T1's snapshot)"
    );
    mgr.commit(t3).unwrap();

    // T1 refreshes — still no T3 in its snapshot.
    let mut t1 = t1;
    mgr.refresh_snapshot(&mut t1);
    // xmax is unchanged — T1's window is still closed before T3.
    assert_eq!(
        t1.snapshot.xmax, t1_xmax,
        "T1's xmax must not advance on RR refresh"
    );

    mgr.abort(t1).unwrap();
}

// ── Serializable ──────────────────────────────────────────────────────────────

/// SSI contract: a three-transaction rw-anti-dependency cycle is detected
/// and at least one transaction is aborted.
///
/// This is the canonical dangerous-structure scenario from Ports et al.
/// (VLDB 2012):
///
/// ```text
/// T1 --rw--> T2 --rw--> T3
/// ```
///
/// - T1 reads a range; T2 writes into that range → T1 --rw--> T2.
/// - T2 reads a range; T3 writes into that range → T2 --rw--> T3.
///
/// T2 is the pivot. When any of T1, T2, T3 commits, the structure is
/// recognized as dangerous and the pivot (T2) is aborted.
#[test]
fn serializable_write_skew_aborts_pivot() {
    use ultrasql_core::RelationId;
    use ultrasql_txn::PredicateLockTag;

    let ssi = Arc::new(SsiManager::new());
    let mgr = Arc::new(TransactionManager::new_with_ssi(Arc::clone(&ssi)));

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);
    let t3 = mgr.begin(IsolationLevel::Serializable);

    // Predicate locks for T1 and T2.
    let rel_tag = PredicateLockTag::Relation(RelationId::new(1));
    mgr.record_predicate_lock(t1.xid, rel_tag.clone());
    mgr.record_predicate_lock(t2.xid, rel_tag);

    // T2 wrote into T1's range → T1 --rw--> T2.
    mgr.record_rw_conflict(t1.xid, t2.xid);
    // T3 wrote into T2's range → T2 --rw--> T3.
    mgr.record_rw_conflict(t2.xid, t3.xid);

    // T1 commits cleanly (no dangerous structure yet — T3 not committed).
    let t1_result = mgr.commit(t1);

    // T2 is the pivot: in-conflict-in = {T1}, in-conflict-out = {T3}.
    // T1 committed → dangerous structure → T2 commit must fail.
    let t2_result = mgr.commit(t2);

    // T3 commit (may succeed or fail depending on timing).
    let t3_result = mgr.commit(t3);

    // At least T2 (the pivot) must have been aborted.
    let got_failure = t1_result.is_err() || t2_result.is_err() || t3_result.is_err();
    assert!(
        got_failure,
        "dangerous structure must cause a SerializationFailure; \
         got t1={t1_result:?} t2={t2_result:?} t3={t3_result:?}"
    );
    // T2 specifically must fail (it is the pivot).
    assert!(
        t2_result.is_err(),
        "T2 (the pivot) must fail with SerializationFailure; got {t2_result:?}"
    );
}

/// SSI contract: concurrent serializable transactions with no rw-conflicts
/// both commit successfully.
#[test]
fn serializable_no_conflict_both_commit() {
    let ssi = Arc::new(SsiManager::new());
    let mgr = Arc::new(TransactionManager::new_with_ssi(Arc::clone(&ssi)));

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);

    // No predicate locks, no rw-conflicts.

    mgr.commit(t1).unwrap();
    mgr.commit(t2).unwrap();
}

/// SSI contract: the oracle correctly reflects Aborted status for the
/// pivot victim after a serialization failure.
///
/// Uses the canonical T1 → T2(pivot) → T3 structure. T2 must be Aborted
/// in the oracle after its commit is rejected.
#[test]
fn serializable_victim_is_aborted_in_oracle() {
    use ultrasql_core::RelationId;
    use ultrasql_txn::PredicateLockTag;

    let ssi = Arc::new(SsiManager::new());
    let mgr = Arc::new(TransactionManager::new_with_ssi(Arc::clone(&ssi)));

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);
    let t3 = mgr.begin(IsolationLevel::Serializable);

    let t1_xid = t1.xid;
    let t2_xid = t2.xid;
    let t3_xid = t3.xid;

    let rel_tag = PredicateLockTag::Relation(RelationId::new(99));
    mgr.record_predicate_lock(t1_xid, rel_tag.clone());
    mgr.record_predicate_lock(t2_xid, rel_tag);

    // Build T1 --rw--> T2 --rw--> T3 dangerous structure.
    mgr.record_rw_conflict(t1_xid, t2_xid);
    mgr.record_rw_conflict(t2_xid, t3_xid);

    // T1 commits cleanly.
    let r1 = mgr.commit(t1);
    assert!(r1.is_ok(), "T1 must commit cleanly: {r1:?}");
    assert!(
        mgr.is_committed(t1_xid),
        "T1 must be Committed in the oracle"
    );

    // T2 is the pivot — must fail.
    let r2 = mgr.commit(t2);
    assert!(
        r2.is_err(),
        "T2 (the pivot) must fail with SerializationFailure; got {r2:?}"
    );
    assert!(
        mgr.is_aborted(t2_xid),
        "serialization victim T2 must appear Aborted in oracle"
    );

    // T3 was not the pivot; it should commit.
    let r3 = mgr.commit(t3);
    assert!(r3.is_ok(), "T3 must commit cleanly: {r3:?}");
    assert!(
        mgr.is_committed(t3_xid),
        "T3 must be Committed in the oracle"
    );
}

/// Serializable without an installed `SsiManager` aliases `RepeatableRead` —
/// both transactions commit even under write-skew conditions.
#[test]
fn serializable_without_ssi_aliases_repeatable_read() {
    // No SsiManager installed.
    let mgr = Arc::new(TransactionManager::new());

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);

    // Even if we tried to record predicate locks, there's no SSI manager,
    // so record_predicate_lock and record_rw_conflict are no-ops.
    mgr.record_rw_conflict(t1.xid, t2.xid);
    mgr.record_rw_conflict(t2.xid, t1.xid);

    // Both must commit — no SSI check is performed.
    mgr.commit(t1).unwrap();
    mgr.commit(t2).unwrap();
}
