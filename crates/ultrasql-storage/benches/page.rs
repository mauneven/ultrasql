//! Microbenchmarks for the slotted-page format.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_storage::Page;

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("page/insert");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &tuple_size in &[16_usize, 64, 256, 1024] {
        let tuple = vec![0xABu8; tuple_size];
        group.throughput(Throughput::Bytes(tuple_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(tuple_size),
            &tuple_size,
            |bencher, _| {
                bencher.iter(|| {
                    let mut page = Page::new_heap();
                    // Fill the page until insertion fails.
                    while page.insert_tuple(black_box(&tuple)).is_ok() {}
                    black_box(page);
                });
            },
        );
    }
    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("page/read");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    let tuple = vec![0xCDu8; 64];
    let mut page = Page::new_heap();
    let mut slots = Vec::new();
    while let Ok(s) = page.insert_tuple(&tuple) {
        slots.push(s);
    }
    group.throughput(Throughput::Elements(slots.len() as u64));
    group.bench_function("scan_all_slots", |bencher| {
        bencher.iter(|| {
            for &s in &slots {
                black_box(page.read_tuple(s).unwrap());
            }
        });
    });
    group.finish();
}

fn bench_checksum(c: &mut Criterion) {
    let mut page = Page::new_heap();
    let payload = vec![0u8; 64];
    for _ in 0..100 {
        if page.insert_tuple(&payload).is_err() {
            break;
        }
    }
    c.bench_function("page/refresh_checksum_8KiB", |b| {
        b.iter(|| {
            page.refresh_checksum();
            black_box(&page);
        });
    });
}

criterion_group!(benches, bench_insert, bench_read, bench_checksum);
criterion_main!(benches);
