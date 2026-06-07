//! `select_sum_65k_i64` benchmark implementation.
//!
//! Exercises `SELECT SUM(x) FROM t` over a 65 536-row `i64` column that
//! fits in L1/L2 cache on Apple M-series and is therefore a latency-
//! bound rather than bandwidth-bound workload. Uses
//! [`ultrasql_vec::kernels::sum_i64`].
//!
//! Throughput = `65_536 / median_elapsed_seconds`.

use std::time::Instant;

use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::kernels::sum_i64;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production row count: 65 536 rows (hot-cache workload).
#[cfg(not(test))]
const ROW_COUNT: usize = 65_536;

/// Reduced row count for fast unit tests.
#[cfg(test)]
const ROW_COUNT: usize = 256;

/// Runs `SUM(x)` over a 65 536-element `i64` column.
pub fn run(ctx: &BenchContext) -> BenchResult {
    let col = build_column(ROW_COUNT);

    let timed_iter = |c: &NumericColumn<i64>| -> f64 {
        let t0 = Instant::now();
        let result = sum_i64(c);
        let elapsed = t0.elapsed();
        std::hint::black_box(result);
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&col);
    }

    let iteration_count = usize::try_from(ctx.iterations).unwrap_or(0);
    let mut samples: Vec<f64> = Vec::with_capacity(iteration_count);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&col));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let rows = ROW_COUNT as f64;
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

/// Builds a deterministic `i64` column using a simple xorshift PRNG.
fn build_column(n: usize) -> NumericColumn<i64> {
    let mut s: u64 = 0xDEAD_BEEF_C0FF_EE01;
    let mut data = Vec::with_capacity(n);
    for _ in 0..n {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        data.push(i64::from_ne_bytes(s.to_ne_bytes()));
    }
    NumericColumn::from_data(data)
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
            usize::try_from(ctx.iterations).unwrap_or(0)
        );
        assert!(result.throughput_per_sec > 0.0);
    }
}
