//! Criterion microbenchmarks for TOAST, persistent CLOG, free-space map,
//! and visibility map.
//!
//! Run with:
//!   `cargo bench --bench storage_extras -p ultrasql-storage`

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_core::{BlockNumber, PageId, RelationId, Result, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::clog::PersistentClog;
use ultrasql_storage::fsm::FreeSpaceMap;
use ultrasql_storage::page::Page;
use ultrasql_storage::toast::ToastTable;
use ultrasql_storage::vm::VisibilityMap;

// ---------------------------------------------------------------------------
// Blank page loader
// ---------------------------------------------------------------------------

struct BlankLoader;

impl PageLoader for BlankLoader {
    fn load(&self, _page_id: PageId) -> Result<Page> {
        Ok(Page::new_heap())
    }
}

fn make_pool(capacity: usize) -> Arc<BufferPool<BlankLoader>> {
    Arc::new(BufferPool::new(capacity, BlankLoader))
}

const fn rel(n: u32) -> RelationId {
    RelationId::new(n)
}

// ---------------------------------------------------------------------------
// TOAST benchmarks: store/fetch 1 KiB, 100 KiB, 1 MiB
// ---------------------------------------------------------------------------

fn bench_toast(c: &mut Criterion) {
    let mut group = c.benchmark_group("toast");

    for &size in &[1024_usize, 100 * 1024, 1024 * 1024] {
        let data: Vec<u8> = (0u8..=255).cycle().take(size).collect();
        let label = format!("{}KiB", size / 1024);

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("store", &label), &data, |b, data| {
            b.iter_batched(
                || {
                    let pool = make_pool(4096);
                    ToastTable::new(pool, rel(1))
                },
                |table| {
                    let ptr = table.store(black_box(data)).unwrap();
                    black_box(ptr);
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("store_fetch", &label), &data, |b, data| {
            b.iter_batched(
                || {
                    let pool = make_pool(4096);
                    let table = ToastTable::new(pool, rel(2));
                    let ptr = table.store(data).unwrap();
                    (table, ptr)
                },
                |(table, ptr)| {
                    let out = table.fetch(black_box(&ptr)).unwrap();
                    black_box(out);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// CLOG benchmarks: set + get 100 000 XIDs
// ---------------------------------------------------------------------------

const CLOG_N: u64 = 100_000;

fn bench_clog(c: &mut Criterion) {
    let mut group = c.benchmark_group("clog");
    group.throughput(Throughput::Elements(CLOG_N));

    group.bench_function("set_100k", |b| {
        b.iter_batched(
            || {
                let pool = make_pool(4096);
                PersistentClog::new(pool, rel(100))
            },
            |clog| {
                for i in 0..CLOG_N {
                    let xid = Xid::new(i + 3);
                    clog.set_status(xid, ultrasql_mvcc::XidStatus::Committed)
                        .unwrap();
                }
                black_box(&clog);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("get_100k", |b| {
        b.iter_batched(
            || {
                let pool = make_pool(4096);
                let clog = PersistentClog::new(pool, rel(101));
                for i in 0..CLOG_N {
                    let xid = Xid::new(i + 3);
                    clog.set_status(xid, ultrasql_mvcc::XidStatus::Committed)
                        .unwrap();
                }
                clog
            },
            |clog| {
                for i in 0..CLOG_N {
                    let s = clog.status(Xid::new(i + 3)).unwrap();
                    black_box(s);
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// FSM benchmarks: 1 M record_free_space ops
// ---------------------------------------------------------------------------

const FSM_M: u32 = 1_000_000;

fn bench_fsm(c: &mut Criterion) {
    let mut group = c.benchmark_group("fsm");
    group.throughput(Throughput::Elements(u64::from(FSM_M)));

    group.bench_function("record_free_space_1M", |b| {
        b.iter_batched(
            FreeSpaceMap::new,
            |fsm| {
                let r = rel(200);
                for i in 0..FSM_M {
                    fsm.record_free_space(r, BlockNumber::new(i), 4096);
                }
                black_box(fsm);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("find_block_1M_lookups", |b| {
        let fsm = FreeSpaceMap::new();
        let r = rel(201);
        for i in 0..FSM_M {
            fsm.record_free_space(r, BlockNumber::new(i), 4096);
        }
        b.iter(|| {
            for _ in 0..1000 {
                let found = fsm.find_block_with_at_least(black_box(r), 4096);
                black_box(found);
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// VM benchmarks: 1 M is_all_visible queries
// ---------------------------------------------------------------------------

const VM_M: u32 = 1_000_000;

fn bench_vm(c: &mut Criterion) {
    let mut group = c.benchmark_group("vm");
    group.throughput(Throughput::Elements(u64::from(VM_M)));

    group.bench_function("is_all_visible_1M", |b| {
        let vm = VisibilityMap::new();
        let r = rel(300);
        for i in (0..10_000u32).step_by(2) {
            vm.mark_all_visible(r, BlockNumber::new(i));
        }
        b.iter(|| {
            for i in 0..1000u32 {
                let v = vm.is_all_visible(black_box(r), BlockNumber::new(i % 10_000));
                black_box(v);
            }
        });
    });

    group.bench_function("mark_and_clear_1M", |b| {
        b.iter_batched(
            VisibilityMap::new,
            |vm| {
                let r = rel(301);
                for i in 0..VM_M {
                    vm.mark_all_visible(r, BlockNumber::new(i));
                    vm.clear(r, BlockNumber::new(i));
                }
                black_box(vm);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

criterion_group!(benches, bench_toast, bench_clog, bench_fsm, bench_vm,);
criterion_main!(benches);
