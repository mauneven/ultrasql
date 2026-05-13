//! Advanced transaction benchmarks: SSI, savepoints, and 2PC.
//!
//! Run with:
//!   `cargo bench --package ultrasql-txn --bench txn_advanced`

#![allow(clippy::missing_docs_in_private_items)]

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use ultrasql_core::{CommandId, RelationId, Xid};
use ultrasql_txn::{PredicateLockTag, SsiManager, SubtxnManager, two_phase::TwoPhaseCoordinator};

// ─── SSI benchmark ───────────────────────────────────────────────────────────

/// Measure commit throughput for N concurrent XIDs that each hold one
/// relation-level predicate lock, with random rw-conflict edges inserted
/// between them.
///
/// Each iteration registers 1 000 XIDs, adds one predicate lock per XID,
/// inserts a random rw-anti-dependency edge for ~10% of XIDs, and then
/// commits every XID.
fn bench_ssi_commit_throughput(c: &mut Criterion) {
    const N: u64 = 1_000;

    let mut group = c.benchmark_group("ssi");
    group.bench_function(BenchmarkId::new("commit_throughput", N), |b| {
        b.iter(|| {
            let mgr = SsiManager::new();

            for i in 0..N {
                mgr.register_xid(Xid::new(i + 1));
            }

            for i in 0..N {
                mgr.add_predicate_lock(
                    Xid::new(i + 1),
                    PredicateLockTag::Relation(RelationId::new(1)),
                );
            }

            // Seed a deterministic pseudo-random stream with a simple LCG.
            let mut state: u64 = 0xDEAD_BEEF_CAFE_1337;
            let lcg = |s: &mut u64| -> u64 {
                *s = s
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                *s
            };

            for i in 0..N {
                if lcg(&mut state) % 10 == 0 {
                    let j = (lcg(&mut state) % N) + 1;
                    if j != i + 1 {
                        mgr.record_rw_conflict(Xid::new(i + 1), Xid::new(j));
                    }
                }
            }

            for i in 0..N {
                // Serialization errors are expected in ~some cases; ignore them.
                let _ = mgr.commit(Xid::new(i + 1));
            }

            std::hint::black_box(&mgr);
        });
    });

    group.finish();
}

// ─── Savepoint benchmark ─────────────────────────────────────────────────────

/// Measure depth-100 savepoint set + release throughput.
fn bench_savepoint_depth_100(c: &mut Criterion) {
    const DEPTH: u64 = 100;

    let mut group = c.benchmark_group("savepoint");
    group.bench_function(BenchmarkId::new("set_and_release_depth", DEPTH), |b| {
        b.iter(|| {
            let parent_xid = Xid::new(1);
            let mgr = SubtxnManager::new(parent_xid);

            let mut next_xid: u64 = 100;

            for i in 0..DEPTH {
                let name = format!("sp{i}");
                let alloc = || {
                    let x = Xid::new(next_xid);
                    next_xid += 1;
                    x
                };
                let cid = CommandId::new(u32::try_from(i).unwrap_or(u32::MAX));
                mgr.savepoint(&name, alloc, cid);
            }

            for i in (0..DEPTH).rev() {
                let name = format!("sp{i}");
                let _ = mgr.release(&name);
            }

            std::hint::black_box(&mgr);
        });
    });

    group.finish();
}

// ─── 2PC benchmark ───────────────────────────────────────────────────────────

/// Measure prepare + `commit_prepared` round-trip latency.
fn bench_2pc_prepare_commit(c: &mut Criterion) {
    let dir = TempDir::new().expect("tempdir");
    let state_dir = dir.path().to_path_buf();
    let coord = Arc::new(TwoPhaseCoordinator::new(state_dir));

    let mut group = c.benchmark_group("two_phase");
    group.bench_function("prepare_and_commit_prepared", |b| {
        let mut counter: u64 = 1;
        b.iter(|| {
            let gid = format!("bench-gid-{counter}");
            let xid = Xid::new(counter);
            counter += 1;

            coord.prepare(&gid, xid).expect("prepare must succeed");
            let resolved = coord.commit_prepared(&gid).expect("commit must succeed");
            std::hint::black_box(resolved);
        });
    });

    group.finish();
}

// ─── harness ─────────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_ssi_commit_throughput,
    bench_savepoint_depth_100,
    bench_2pc_prepare_commit,
);
criterion_main!(benches);
