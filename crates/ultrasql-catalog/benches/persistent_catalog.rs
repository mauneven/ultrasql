//! Benchmarks for [`ultrasql_catalog::PersistentCatalog`].
//!
//! Measures:
//! - `snapshot_latency` — cost of a single `catalog.snapshot()` call
//!   (should be < 50 ns; dominated by `ArcSwap::load_full`).
//! - `bootstrap_latency` — full cold-start bootstrap from a heap that
//!   has no catalog tuples yet (fresh-database path).
//! - `concurrent_snapshot_16t` — 16 concurrent threads each calling
//!   `catalog.snapshot()` in a tight loop; measures scalability of the
//!   wait-free read path.

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use ultrasql_catalog::PersistentCatalog;
use ultrasql_core::{BlockNumber, DataType, Field, Lsn, Oid, PageId, Schema};
use ultrasql_storage::buffer_pool::BufferPool;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::page::Page;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A heap backed by a blank page loader — every page miss returns a fresh
/// empty heap page.  Sufficient for the bootstrap benchmark because the
/// fresh-database path never reads any page data.
fn blank_heap() -> HeapAccess<impl ultrasql_storage::buffer_pool::PageLoader> {
    let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
    HeapAccess::new(pool)
}

/// A catalog pre-seeded with `n` synthetic table entries via
/// `MutableCatalog::create_table`.  Used by the concurrent-snapshot bench.
fn seeded_catalog(n: u32) -> Arc<PersistentCatalog> {
    use ultrasql_catalog::MutableCatalog as _;
    let cat = Arc::new(PersistentCatalog::new());
    let schema = Schema::new([
        Field::required("id", DataType::Int64),
        Field::nullable("name", DataType::Text { max_len: None }),
    ])
    .expect("bench schema is valid");
    for i in 0..n {
        cat.create_table(ultrasql_catalog::TableEntry {
            oid: Oid::new(16_384 + i),
            name: format!("bench_table_{i}"),
            schema_name: "public".to_owned(),
            schema: schema.clone(),
            created_at_lsn: Lsn::ZERO,
            n_blocks: 0,
            root_block: BlockNumber::INVALID,
        })
        .expect("bench create_table");
    }
    cat
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Measures the per-call latency of `catalog.snapshot()`.
///
/// The call is a wait-free `ArcSwap::load_full` under the hood; on a
/// modern machine this should be well below 50 ns.
fn snapshot_latency(c: &mut Criterion) {
    let cat = seeded_catalog(0);
    c.bench_function("snapshot_latency", |b| {
        b.iter(|| {
            let snap = cat.snapshot();
            criterion::black_box(snap.tables.len());
        });
    });
}

/// Measures the full bootstrap from an empty heap (fresh-database path).
///
/// The bootstrap detects an empty heap and installs the hard-coded initial
/// snapshot.  This includes arc-swap construction and the `DashMap` seed.
fn bootstrap_latency(c: &mut Criterion) {
    let heap = blank_heap();
    c.bench_function("bootstrap_latency_empty_heap", |b| {
        b.iter(|| {
            let cat = PersistentCatalog::new();
            let stats = cat
                .bootstrap_from_heap(&heap)
                .expect("bootstrap must not fail");
            criterion::black_box(stats);
        });
    });
}

/// 16 threads each call `catalog.snapshot()` concurrently.
///
/// The snapshot call is wait-free; this bench verifies there is no
/// hidden contention under parallel read pressure.
fn concurrent_snapshot_16t(c: &mut Criterion) {
    let cat = Arc::new(seeded_catalog(100));
    c.bench_function("concurrent_snapshot_16t", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..16_usize)
                .map(|_| {
                    let cat = Arc::clone(&cat);
                    std::thread::spawn(move || {
                        let snap = cat.snapshot();
                        criterion::black_box(snap.tables.len());
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("bench thread panicked");
            }
        });
    });
}

criterion_group!(
    benches,
    snapshot_latency,
    bootstrap_latency,
    concurrent_snapshot_16t
);
criterion_main!(benches);
