//! Workload-level benchmark for the fused branchless filter+sum kernel.
//!
//! This benchmark mirrors the ClickHouse / DuckDB cross-engine
//! comparison harness: `SELECT SUM(x) FROM t WHERE y > 0` over a
//! deterministic 10 M-row `(i64 x, i64 y)` synthetic dataset, where
//! roughly half the `y` values are non-positive.
//!
//! Single-threaded NEON: on Apple M-series the theoretical
//! memory-bandwidth floor for 160 MB scanned at ~72 GB/s on a single
//! core is ~2.2 ms; the serial kernel hits that floor.
//!
//! Multi-core fan-out: aggregate sustained DRAM bandwidth on M4 is
//! ~110 GB/s shared across all cores, giving a parallel floor of
//! 160 MB / 110 GB/s ≈ 1.45 ms. The `par_*` variants approach that
//! floor at 6–10 threads; below 4 threads the per-core bandwidth has
//! not been amortized enough to beat serial.
//!
//! Reproducibility:
//! ```text
//! cargo bench -p ultrasql-vec --bench filter_sum_10m -- \
//!     --warm-up-time 1 --measurement-time 5
//! ```

#![allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::{
    DictI64U8, DictI64U16, PredicateMask16, PredicateMask256, PredicateMask65536,
    filter_sum_i64_where_dict_predicate, filter_sum_i64_where_dict_predicate_tbl,
    filter_sum_i64_where_dict_predicate_u16, filter_sum_i64_where_gt_zero,
    filter_sum_par_auto_i64_where_dict_predicate, filter_sum_par_auto_i64_where_gt_zero,
    filter_sum_par_i64_where_dict_predicate, filter_sum_par_i64_where_gt_zero,
};

/// Deterministic 10 M-row dataset.
///
/// The LCG seed (`0x9E37_79B9_7F4A_7C15`) and multipliers are fixed so
/// every run produces the same bytes — important for cross-run
/// comparisons against the criterion baseline file.
fn build_dataset(n: usize) -> (NumericColumn<i64>, NumericColumn<i64>) {
    let mut xs = Vec::with_capacity(n);
    let mut ys = Vec::with_capacity(n);
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..n {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        xs.push(i64::from_ne_bytes(s.to_ne_bytes()) >> 32);
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // Sign of `y` is uniform over the upper 32 bits of the state.
        // We shift right by one to keep the magnitude reasonable, then
        // subtract a separate fresh sample to ensure the distribution
        // straddles zero so the predicate selectivity is ~50%.
        let a = i64::from_ne_bytes(s.to_ne_bytes()) >> 1;
        let b = i64::from_ne_bytes(s.to_ne_bytes()) / 2;
        ys.push(a.wrapping_sub(b));
    }
    (NumericColumn::from_data(xs), NumericColumn::from_data(ys))
}

fn bench_filter_sum_10m(c: &mut Criterion) {
    let n = 10_000_000_usize;
    let (x_col, y_col) = build_dataset(n);

    let mut group = c.benchmark_group("filter_sum_10m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("ultrasql_fused", |b| {
        b.iter(|| {
            black_box(filter_sum_i64_where_gt_zero(
                black_box(&x_col),
                black_box(&y_col),
            ))
        });
    });
    // Single-threaded NEON baseline reported under a stable label so
    // the multi-core variants below can be compared apples-to-apples.
    group.bench_function("serial_neon", |b| {
        b.iter(|| {
            black_box(filter_sum_i64_where_gt_zero(
                black_box(&x_col),
                black_box(&y_col),
            ))
        });
    });
    // Multi-core fan-out. The thread counts cover M-series M4's
    // 4 P-cores, 8-thread sweet spot, and a few off-ramps either side
    // so the scaling shape is visible in the criterion report.
    for nt in [2_usize, 3, 4, 5, 6, 8] {
        let label = format!("par_{nt}");
        group.bench_function(&label, |b| {
            b.iter(|| {
                black_box(filter_sum_par_i64_where_gt_zero(
                    black_box(&x_col),
                    black_box(&y_col),
                    nt,
                ))
            });
        });
    }
    group.bench_function("par_auto", |b| {
        b.iter(|| {
            black_box(filter_sum_par_auto_i64_where_gt_zero(
                black_box(&x_col),
                black_box(&y_col),
            ))
        });
    });
    group.finish();
}

// ============================================================================
// Dictionary-encoded predicate dataset
// ============================================================================

/// Build a 10 M-row dataset where `y` has exactly 256 distinct values
/// uniformly distributed around zero (so the `y > 0` predicate has
/// roughly 50% selectivity), encoded as `DictI64U8`. The `x` column is
/// the same shape as the dense bench so the comparison is apples-to-
/// apples on the `x` stream.
fn build_dict_dataset(n: usize) -> (NumericColumn<i64>, DictI64U8, PredicateMask256) {
    let mut xs = Vec::with_capacity(n);
    let mut ys = Vec::with_capacity(n);
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..n {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        xs.push(i64::from_ne_bytes(s.to_ne_bytes()) >> 32);
        // y values in -128..=127 → 256 distinct values centred around
        // zero, ~50% selectivity for `y > 0`.
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let v = (i64::from_ne_bytes(s.to_ne_bytes()).wrapping_rem(256)) - 128;
        ys.push(v);
    }
    let x_col = NumericColumn::from_data(xs);
    let y_col = NumericColumn::from_data(ys);
    let y_dict = DictI64U8::try_from_column(&y_col).expect("y cardinality ≤ 256 by construction");
    let mask = PredicateMask256::from_gt(&y_dict.dict, 0);
    (x_col, y_dict, mask)
}

/// Build a 10 M-row dataset where the dictionary has only 16 distinct
/// values — small enough to fit in a single NEON register for the
/// `vqtbl1q_u8` fast path.
fn build_dict16_dataset(n: usize) -> (NumericColumn<i64>, DictI64U8, PredicateMask16) {
    let mut xs = Vec::with_capacity(n);
    let mut ys = Vec::with_capacity(n);
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..n {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        xs.push(i64::from_ne_bytes(s.to_ne_bytes()) >> 32);
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let v = (i64::from_ne_bytes(s.to_ne_bytes()).wrapping_rem(16)) - 8;
        ys.push(v);
    }
    let x_col = NumericColumn::from_data(xs);
    let y_col = NumericColumn::from_data(ys);
    let y_dict = DictI64U8::try_from_column(&y_col).expect("16 distinct values fit");
    let mask = PredicateMask16::from_gt(&y_dict.dict, 0).expect("≤ 16 entries");
    (x_col, y_dict, mask)
}

/// Build a 10 M-row dataset with 65 536 distinct `y` values so the
/// `u16`-coded gather path is exercised on its target shape (cold-cache
/// 512 KB mask table).
fn build_dict_u16_dataset(n: usize) -> (NumericColumn<i64>, DictI64U16, PredicateMask65536) {
    let mut xs = Vec::with_capacity(n);
    let mut ys = Vec::with_capacity(n);
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..n {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        xs.push(i64::from_ne_bytes(s.to_ne_bytes()) >> 32);
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // y in -32_768..=32_767 → up to 65 536 distinct values.
        let v = (i64::from_ne_bytes(s.to_ne_bytes()).wrapping_rem(65_536)) - 32_768;
        ys.push(v);
    }
    let x_col = NumericColumn::from_data(xs);
    let y_col = NumericColumn::from_data(ys);
    let y_dict = DictI64U16::try_from_column(&y_col).expect("≤ 65 536 distinct values");
    let mask = PredicateMask65536::from_gt(&y_dict.dict, 0);
    (x_col, y_dict, mask)
}

fn bench_dict_path(c: &mut Criterion) {
    let n = 10_000_000_usize;
    let (x_col, y_dict, mask) = build_dict_dataset(n);

    let mut group = c.benchmark_group("filter_sum_dict_10m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("ultrasql_dict_u8", |b| {
        b.iter(|| {
            black_box(filter_sum_i64_where_dict_predicate(
                black_box(&x_col),
                black_box(&y_dict),
                black_box(&mask),
            ))
        });
    });
    // Multi-core dict fan-out: same sweep as the dense kernel.
    for nt in [2_usize, 3, 4, 5, 6, 8] {
        let label = format!("par_dict_{nt}");
        group.bench_function(&label, |b| {
            b.iter(|| {
                black_box(filter_sum_par_i64_where_dict_predicate(
                    black_box(&x_col),
                    black_box(&y_dict),
                    black_box(&mask),
                    nt,
                ))
            });
        });
    }
    group.bench_function("par_dict_auto", |b| {
        b.iter(|| {
            black_box(filter_sum_par_auto_i64_where_dict_predicate(
                black_box(&x_col),
                black_box(&y_dict),
                black_box(&mask),
            ))
        });
    });
    group.finish();
}

fn bench_dict_tbl_path(c: &mut Criterion) {
    let n = 10_000_000_usize;
    let (x_col, y_dict, mask) = build_dict16_dataset(n);

    let mut group = c.benchmark_group("filter_sum_dict_tbl_10m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("ultrasql_dict_u8_tbl16", |b| {
        b.iter(|| {
            black_box(
                filter_sum_i64_where_dict_predicate_tbl(
                    black_box(&x_col),
                    black_box(&y_dict),
                    black_box(&mask),
                )
                .expect("dict fits"),
            )
        });
    });
    group.finish();
}

fn bench_dict_u16_path(c: &mut Criterion) {
    let n = 10_000_000_usize;
    let (x_col, y_dict, mask) = build_dict_u16_dataset(n);

    let mut group = c.benchmark_group("filter_sum_dict_u16_10m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("ultrasql_dict_u16_65k", |b| {
        b.iter(|| {
            black_box(filter_sum_i64_where_dict_predicate_u16(
                black_box(&x_col),
                black_box(&y_dict),
                black_box(&mask),
            ))
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_filter_sum_10m,
    bench_dict_path,
    bench_dict_tbl_path,
    bench_dict_u16_path,
);
criterion_main!(benches);
