//! Microbenchmarks for the vectorized kernels.

//!
//! These are *micro*-benchmarks: each measures a single kernel over a
//! single batch of data. They are intended as regression watchdogs for
//! per-row CPU cost, not as workload-level claims.
//!
//! Host description and configuration are recorded in
//! `benchmarks/results/host.yaml` (committed alongside results).

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::kernels::{eq_i32, min_f64, select_i32, sum_i64};

const SIZES: &[usize] = &[64, 1_024, 4_096, 65_536];

fn bench_len_i32(n: usize) -> i32 {
    i32::try_from(n).expect("benchmark size fits i32")
}

fn bench_len_i64(n: usize) -> i64 {
    i64::try_from(n).expect("benchmark size fits i64")
}

fn bench_len_u64(n: usize) -> u64 {
    u64::try_from(n).expect("benchmark size fits u64")
}

fn bench_eq_i32(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec/eq_i32");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &n in SIZES {
        let end = bench_len_i32(n);
        let a = NumericColumn::from_data((0..end).collect());
        let b = NumericColumn::from_data((0..end).map(|x| x ^ (x & 1)).collect());
        group.throughput(Throughput::Elements(bench_len_u64(n)));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |bencher, _| {
            bencher.iter(|| {
                let mask = eq_i32(black_box(&a), black_box(&b));
                black_box(mask);
            });
        });
    }
    group.finish();
}

fn bench_sum_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec/sum_i64");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &n in SIZES {
        let col = NumericColumn::from_data((0..bench_len_i64(n)).collect());
        group.throughput(Throughput::Elements(bench_len_u64(n)));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |bencher, _| {
            bencher.iter(|| {
                let s = sum_i64(black_box(&col));
                black_box(s);
            });
        });
    }
    group.finish();
}

fn bench_min_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec/min_f64");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &n in SIZES {
        let col = NumericColumn::from_data(
            (0..bench_len_i32(n))
                .map(|i| f64::from(i) * 0.5_f64)
                .collect(),
        );
        group.throughput(Throughput::Elements(bench_len_u64(n)));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |bencher, _| {
            bencher.iter(|| {
                let m = min_f64(black_box(&col));
                black_box(m);
            });
        });
    }
    group.finish();
}

fn bench_select_i32(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec/select_i32");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &n in SIZES {
        let col = NumericColumn::from_data((0..bench_len_i32(n)).collect());
        // 50% selectivity (every other row).
        let mut sel = Bitmap::new(n, false);
        for i in (0..n).step_by(2) {
            sel.set(i, true);
        }
        group.throughput(Throughput::Elements(bench_len_u64(n)));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |bencher, _| {
            bencher.iter(|| {
                let out = select_i32(black_box(&col), black_box(&sel));
                black_box(out);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_eq_i32,
    bench_sum_i64,
    bench_min_f64,
    bench_select_i32
);
criterion_main!(benches);
