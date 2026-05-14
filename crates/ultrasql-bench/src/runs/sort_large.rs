//! `sort_large` benchmark implementation.
//!
//! Simulates `SELECT x FROM t ORDER BY x DESC` over 1 000 000 `i64` values.
//! Each measured iteration sorts a fresh copy of the data in descending
//! order using Rust's `sort_unstable` (the same comparison core the
//! [`ultrasql_executor::Sort`] operator uses for numeric keys once it
//! inlines the comparison).
//!
//! Throughput = `ROW_COUNT / median_elapsed_seconds`.

use std::time::Instant;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production row count: 1 000 000 `i64` values.
#[allow(dead_code)] // used only in non-test builds via smoke_row_count
const PROD_ROW_COUNT: usize = 1_000_000;

/// Reduced row count for fast unit tests.
#[cfg(test)]
const TEST_ROW_COUNT: usize = 2_000;

/// Smoke-mode row count (used when `ULTRASQL_BENCH_SMOKE` is set).
#[allow(dead_code)] // used only in non-test builds via smoke_row_count
const SMOKE_ROW_COUNT: usize = 500;

/// Builds a deterministic `i64` dataset via xorshift64.
fn build_data(n: usize) -> Vec<i64> {
    let mut s: u64 = 0xF0F0_F0F0_A5A5_A5A5;
    let mut data = Vec::with_capacity(n);
    for _ in 0..n {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        data.push(i64::from_ne_bytes(s.to_ne_bytes()));
    }
    data
}

/// Runs the sort benchmark.
///
/// Each measured iteration copies the pre-built data (so allocation cost
/// is excluded) and sorts it in descending order.
pub fn run(ctx: &BenchContext) -> BenchResult {
    #[cfg(test)]
    let row_count = TEST_ROW_COUNT;
    #[cfg(not(test))]
    let row_count = crate::runs::smoke_row_count(PROD_ROW_COUNT, SMOKE_ROW_COUNT);

    let data = build_data(row_count);

    let timed_iter = |src: &[i64]| -> f64 {
        // Clone so each sort starts from the same unsorted state.
        let mut buf = src.to_vec();
        let t0 = Instant::now();
        buf.sort_unstable_by(|a, b| b.cmp(a)); // descending
        let elapsed = t0.elapsed();
        std::hint::black_box(&buf);
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&data);
    }

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&data));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let rows = row_count as f64;
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
        assert_eq!(result.samples.len(), ctx.iterations as usize);
        assert!(result.throughput_per_sec > 0.0);
    }
}
