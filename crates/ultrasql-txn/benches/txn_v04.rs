//! v0.4 benchmarks: SSI commit-throughput under contention and
//! `ForUpdate` row-lock acquire/release latency.
//!
//! Run with:
//!   `cargo bench --package ultrasql-txn --bench txn_v04`

#![allow(clippy::missing_docs_in_private_items)]

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
use ultrasql_txn::{
    IsolationLevel, PredicateLockTag, RowLockExt, RowLockMode, RowLockRequest, SsiManager,
    TransactionManager,
};

// ─── SSI commit throughput ────────────────────────────────────────────────────

/// Measure commit throughput for N serializable transactions going through
/// [`TransactionManager::new_with_ssi`].
///
/// Two variants:
///   - `no_contention`: each XID has a unique predicate lock (no conflicts).
///   - `with_contention`: ~10% of XIDs share a rw-conflict edge.
fn bench_ssi_via_txn_manager(c: &mut Criterion) {
    const N: u64 = 500;

    let mut group = c.benchmark_group("ssi_via_txn_manager");

    // Variant 1: no conflicts.
    group.bench_function(BenchmarkId::new("no_contention", N), |b| {
        b.iter(|| {
            let ssi = Arc::new(SsiManager::new());
            let mgr = Arc::new(TransactionManager::new_with_ssi(Arc::clone(&ssi)));

            let mut txns = Vec::with_capacity(N as usize);
            for _ in 0..N {
                let t = mgr.begin(IsolationLevel::Serializable);
                mgr.record_predicate_lock(
                    t.xid,
                    PredicateLockTag::Relation(RelationId::new(t.xid.raw() as u32)),
                );
                txns.push(t);
            }

            for t in txns {
                let _ = mgr.commit(t);
            }

            std::hint::black_box(&mgr);
        });
    });

    // Variant 2: ~10% of XIDs share a rw-conflict edge.
    group.bench_function(BenchmarkId::new("with_contention", N), |b| {
        b.iter(|| {
            let ssi = Arc::new(SsiManager::new());
            let mgr = Arc::new(TransactionManager::new_with_ssi(Arc::clone(&ssi)));

            let mut txns = Vec::with_capacity(N as usize);
            for _ in 0..N {
                let t = mgr.begin(IsolationLevel::Serializable);
                mgr.record_predicate_lock(t.xid, PredicateLockTag::Relation(RelationId::new(1)));
                txns.push(t);
            }

            // Seed a deterministic pseudo-random stream with a simple LCG.
            let mut state: u64 = 0xDEAD_BEEF_CAFE_1337;
            let lcg = |s: &mut u64| -> u64 {
                *s = s
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                *s
            };

            let xids: Vec<Xid> = txns.iter().map(|t| t.xid).collect();
            for i in 0..N as usize {
                if lcg(&mut state) % 10 == 0 {
                    let j = (lcg(&mut state) as usize) % xids.len();
                    if j != i {
                        mgr.record_rw_conflict(xids[i], xids[j]);
                    }
                }
            }

            for t in txns {
                let _ = mgr.commit(t);
            }

            std::hint::black_box(&mgr);
        });
    });

    group.finish();
}

// ─── ForUpdate row-lock acquire/release latency ───────────────────────────────

/// Measure the round-trip latency of acquiring and releasing a
/// `FOR UPDATE` row lock on a single tuple with no contention.
fn bench_for_update_acquire_release(c: &mut Criterion) {
    let mgr = Arc::new(TransactionManager::new());
    let tid = TupleId::new(PageId::new(RelationId::new(1), BlockNumber::new(0)), 0);

    let mut group = c.benchmark_group("row_lock");

    group.bench_function("for_update_acquire_release_no_contention", |b| {
        let mut xid_counter: u64 = 1;
        b.iter(|| {
            let xid = Xid::new(xid_counter);
            xid_counter += 1;

            mgr.lock_manager
                .acquire_row_lock(RowLockRequest {
                    xid,
                    tid,
                    mode: RowLockMode::ForUpdate,
                })
                .expect("acquire must succeed with no contention");

            mgr.lock_manager
                .release_row_lock(xid, tid, RowLockMode::ForUpdate);
            std::hint::black_box(xid);
        });
    });

    // ForShare: multiple concurrent readers, measure single acquire.
    group.bench_function("for_share_acquire_release_no_contention", |b| {
        let mut xid_counter: u64 = 100_000;
        b.iter(|| {
            let xid = Xid::new(xid_counter);
            xid_counter += 1;

            mgr.lock_manager
                .acquire_row_lock(RowLockRequest {
                    xid,
                    tid,
                    mode: RowLockMode::ForShare,
                })
                .expect("acquire must succeed");

            mgr.lock_manager
                .release_row_lock(xid, tid, RowLockMode::ForShare);
            std::hint::black_box(xid);
        });
    });

    group.finish();
}

// ─── harness ─────────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_ssi_via_txn_manager,
    bench_for_update_acquire_release,
);
criterion_main!(benches);
