//! `filter_sum_10m_i64` benchmark implementation.
//!
//! Exercises the fused branchless filter+sum kernel:
//! `SELECT SUM(x) FROM t WHERE y > 0` over 10 000 000 `i64` rows using
//! [`ultrasql_vec::kernels::filter_sum_par_auto_i64_where_gt_zero`].
//!
//! The two input columns are synthesised via an xorshift64 PRNG seeded
//! at compile time, so the dataset is deterministic across runs.
//!
//! Throughput is reported as `row_count / median_elapsed_seconds` so
//! the metric is directly comparable across engines.

use std::time::Instant;

use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::kernels::filter_sum_par_auto_i64_where_gt_zero;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production row count: 10 000 000 rows = 160 MiB of input data
/// (two i64 columns × 8 bytes × 10 M rows).
#[cfg(not(test))]
const ROW_COUNT: usize = 10_000_000;

/// Reduced row count for fast unit tests.
#[cfg(test)]
const ROW_COUNT: usize = 1_000;

/// Runs the filter+sum kernel over a synthesised 10 M-row dataset.
///
/// Each measured iteration calls
/// [`filter_sum_par_auto_i64_where_gt_zero`] once on the pre-allocated
/// columns. The result is consumed by a `std::hint::black_box` call to
/// prevent the compiler from eliminating the kernel entirely.
pub fn run(ctx: &BenchContext) -> BenchResult {
    // Synthesise columns once, before warmup, so allocation cost is
    // excluded from the measurements.
    let (x_col, y_col) = build_columns(ROW_COUNT);

    let timed_iter = |x: &NumericColumn<i64>, y: &NumericColumn<i64>| -> f64 {
        let t0 = Instant::now();
        let result = filter_sum_par_auto_i64_where_gt_zero(x, y);
        let elapsed = t0.elapsed();
        // Prevent dead-code elimination.
        std::hint::black_box(result);
        elapsed.as_secs_f64() * 1_000_000.0 // microseconds
    };

    // Warmup: run without recording.
    for _ in 0..ctx.warmup_iterations {
        timed_iter(&x_col, &y_col);
    }

    // Measured iterations.
    let iteration_count = usize::try_from(ctx.iterations).unwrap_or(0);
    let mut samples: Vec<f64> = Vec::with_capacity(iteration_count);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&x_col, &y_col));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);

    // Throughput = rows processed per second using the median iteration.
    let rows = crate::runs::count_as_f64(ROW_COUNT);
    let throughput_per_sec = if median_us > 0.0 {
        rows / (median_us / 1_000_000.0)
    } else {
        0.0
    };

    BenchResult {
        throughput_per_sec,
        p50_latency_us: median_us,
        p99_latency_us: p99_us,
        samples,
    }
}

/// Builds the two input columns using an xorshift64 PRNG.
///
/// The PRNG is seeded at `0x9E37_79B9_7F4A_7C15` (a golden-ratio-
/// derived constant) so the dataset is deterministic. Each call
/// produces a new `u64` state and the `i64` is taken by reinterpreting
/// the bytes. Roughly half of `y` values are positive, so approximately
/// half of `x` values are summed.
fn build_columns(n: usize) -> (NumericColumn<i64>, NumericColumn<i64>) {
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut x_data = Vec::with_capacity(n);
    let mut y_data = Vec::with_capacity(n);
    for _ in 0..n {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        x_data.push(i64::from_ne_bytes(s.to_ne_bytes()));
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        y_data.push(i64::from_ne_bytes(s.to_ne_bytes()));
    }
    (
        NumericColumn::from_data(x_data),
        NumericColumn::from_data(y_data),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{BenchContext, HostInfo};

    fn test_ctx() -> BenchContext {
        BenchContext {
            iterations: 2,
            warmup_iterations: 1,
            host: HostInfo {
                cpu: "test".to_string(),
                cores: 1,
                ram_gb: 1,
                os: "test".to_string(),
            },
        }
    }

    #[test]
    fn run_produces_two_samples_with_positive_throughput() {
        let ctx = test_ctx();
        let result = run(&ctx);
        assert_eq!(
            result.samples.len(),
            usize::try_from(ctx.iterations).unwrap_or(0),
            "must produce one sample per measured iteration"
        );
        assert!(
            result.throughput_per_sec > 0.0,
            "throughput must be positive: {}",
            result.throughput_per_sec
        );
    }
}
