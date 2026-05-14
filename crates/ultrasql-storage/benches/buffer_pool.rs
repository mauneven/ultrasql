//! Microbenchmarks for the buffer pool.
//!
//! The loader produces blank heap pages in O(1). The benchmark
//! therefore isolates the buffer-pool overhead — pin / unpin /
//! eviction — rather than I/O latency. Disk-backed segment benchmarks
//! land alongside the segment-file manager.

use std::sync::Arc;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_core::{BlockNumber, PageId, RelationId, Result};
use ultrasql_storage::{BufferPool, Page, PageLoader};

struct BlankLoader;
impl PageLoader for BlankLoader {
    fn load(&self, _: PageId) -> Result<Page> {
        Ok(Page::new_heap())
    }
}

const fn pid(block: u32) -> PageId {
    PageId::new(RelationId::new(1), BlockNumber::new(block))
}

fn bench_hot_pin(c: &mut Criterion) {
    let pool = Arc::new(BufferPool::new(128, BlankLoader));
    // Warm one page; the guard is dropped immediately by the
    // statement-expression form.
    drop(pool.get_page(pid(0)).unwrap());

    let mut group = c.benchmark_group("buffer_pool/hot_pin");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_page_repin", |b| {
        b.iter(|| {
            let g = pool.get_page(black_box(pid(0))).unwrap();
            drop(g);
        });
    });
    group.finish();
}

fn bench_cycle(c: &mut Criterion) {
    // 64-frame pool; the benchmark cycles through 64 pages so every
    // access is a hit on a warm clock; then through 256 pages so the
    // pool churns through eviction.
    let mut group = c.benchmark_group("buffer_pool/cycle");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &n_pages in &[64_u32, 256] {
        let pool = Arc::new(BufferPool::new(64, BlankLoader));
        for i in 0..n_pages {
            drop(pool.get_page(pid(i)).unwrap());
        }
        group.throughput(Throughput::Elements(u64::from(n_pages)));
        group.bench_function(format!("{n_pages}_pages"), |b| {
            b.iter(|| {
                for i in 0..n_pages {
                    let g = pool.get_page(black_box(pid(i))).unwrap();
                    drop(g);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_hot_pin, bench_cycle);
criterion_main!(benches);
