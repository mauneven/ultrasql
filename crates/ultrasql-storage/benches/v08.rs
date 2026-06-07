//! v0.8 benchmarks: B-tree multi-column insert/lookup, Hash index lookup,
//! BRIN summary build, and Sequence nextval contention.
//!
//! Run with:
//!   cargo bench --bench v08 -p ultrasql-storage

#![allow(clippy::items_after_statements)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_core::endian::write_i64_le;
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};
use ultrasql_storage::access_method::{AccessMethod, BTreeAccessMethod, BrinIndex, HashIndex};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const fn tid(block: u32, slot: u16) -> TupleId {
    TupleId::new(
        PageId::new(RelationId::new(99), BlockNumber::new(block)),
        slot,
    )
}

fn i64_key(v: i64) -> [u8; 8] {
    let mut buf = [0_u8; 8];
    write_i64_le(&mut buf, v);
    buf
}

// ---------------------------------------------------------------------------
// B-tree multi-column insert + lookup 1M rows
// ---------------------------------------------------------------------------

fn bench_btree_multi_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_multicolumn");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));

    const N: u32 = 100_000; // 100K for CI; bump to 1M for local runs
    group.throughput(Throughput::Elements(u64::from(N)));

    group.bench_function("insert", |b| {
        b.iter(|| {
            let am = BTreeAccessMethod::new(false);
            for i in 0..N {
                let key = i64_key(i64::from(i));
                am.insert(&key, tid(i, 0)).unwrap();
            }
            black_box(am);
        });
    });

    // Pre-build index for lookup benchmark.
    let am = {
        let a = BTreeAccessMethod::new(false);
        for i in 0..N {
            a.insert(&i64_key(i64::from(i)), tid(i, 0)).unwrap();
        }
        a
    };

    group.bench_function("lookup", |b| {
        let mut i = 0_u32;
        b.iter(|| {
            let key = i64_key(i64::from(i % N));
            i = i.wrapping_add(1);
            black_box(am.lookup(&key).unwrap())
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Hash index lookup 1M rows
// ---------------------------------------------------------------------------

fn bench_hash_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_lookup");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    const N: u32 = 100_000;
    group.throughput(Throughput::Elements(u64::from(N)));

    let am = HashIndex::new(1024);
    for i in 0..N {
        am.insert(&i64_key(i64::from(i)), tid(i, 0)).unwrap();
    }

    group.bench_function("lookup_hit", |b| {
        let mut i = 0_u32;
        b.iter(|| {
            let key = i64_key(i64::from(i % N));
            i = i.wrapping_add(1);
            black_box(am.lookup(&key).unwrap())
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// BRIN summary build over 10M rows (simulated)
// ---------------------------------------------------------------------------

fn bench_brin_summarize(c: &mut Criterion) {
    let mut group = c.benchmark_group("brin_summarize");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));

    // 10M rows / 128 pages_per_range = ~78K ranges.
    const TOTAL_BLOCKS: u32 = 78_125;
    const PAGES_PER_RANGE: u32 = 128;
    group.throughput(Throughput::Elements(u64::from(TOTAL_BLOCKS)));

    group.bench_function("build", |b| {
        b.iter(|| {
            let am = BrinIndex::new(PAGES_PER_RANGE);
            let mut range_start = 0_u32;
            while range_start < TOTAL_BLOCKS {
                let range_end = (range_start + PAGES_PER_RANGE - 1).min(TOTAL_BLOCKS - 1);
                // min/max keys for this range.
                let min_key = i64_key(i64::from(range_start) * 128);
                let max_key = i64_key(i64::from(range_end) * 128 + 127);
                am.summarize_range(range_start, range_end, min_key.to_vec(), max_key.to_vec());
                range_start += PAGES_PER_RANGE;
            }
            black_box(am);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Sequence nextval contention with 16 threads
// ---------------------------------------------------------------------------

fn bench_sequence_nextval_contention(c: &mut Criterion) {
    use ultrasql_storage::sequence::{Sequence, SequenceOptions};

    let mut group = c.benchmark_group("sequence_nextval");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));

    for threads in [1, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("contention", threads),
            &threads,
            |b, &threads| {
                let seq = Arc::new(
                    Sequence::new(SequenceOptions {
                        start: 1,
                        increment: 1,
                        min: None,
                        max: None,
                        cache: 1,
                        cycle: false,
                    })
                    .unwrap(),
                );

                b.iter(|| {
                    let counter = Arc::new(AtomicI64::new(0));
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let seq = Arc::clone(&seq);
                            let counter = Arc::clone(&counter);
                            thread::spawn(move || {
                                for _ in 0..100 {
                                    let v = seq.nextval().unwrap();
                                    counter.fetch_add(v, Ordering::Relaxed);
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                    black_box(counter.load(Ordering::Relaxed))
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_btree_multi_column,
    bench_hash_lookup,
    bench_brin_summarize,
    bench_sequence_nextval_contention,
);
criterion_main!(benches);
