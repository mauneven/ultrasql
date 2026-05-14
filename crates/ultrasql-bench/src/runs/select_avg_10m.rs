//! `select_avg_10m_i64` benchmark implementation.
//!
//! Exercises `SELECT AVG(x) FROM t` over 10 000 000 `i64` rows.
//! Implemented as `sum_i64(col) / len` — the same arithmetic the
//! executor performs for `AVG` over a non-nullable integer column.
//!
//! Throughput = `10_000_000 / median_elapsed_seconds`.

use std::time::Instant;

use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::kernels::sum_i64;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production row count: 10 000 000 rows (80 MiB scanned).
#[cfg(not(test))]
const ROW_COUNT: usize = 10_000_000;

/// Reduced row count for fast unit tests.
#[cfg(test)]
const ROW_COUNT: usize = 1_000;

/// Runs `AVG(x)` over a 10 M-element `i64` column.
pub fn run(ctx: &BenchContext) -> BenchResult {
    let col = build_column(ROW_COUNT);
    let len = col.len();

    let timed_iter = |c: &NumericColumn<i64>| -> f64 {
        let t0 = Instant::now();
        let s = sum_i64(c);
        // Integer division: this is the executor's `AVG(bigint)` result.
        let avg = if len > 0 {
            s.wrapping_div(i64::try_from(len).unwrap_or(i64::MAX))
        } else {
            0
        };
        let elapsed = t0.elapsed();
        std::hint::black_box(avg);
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&col);
    }

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
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
    let mut s: u64 = 0xCAFE_BABE_1234_5678;
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
        assert_eq!(result.samples.len(), ctx.iterations as usize);
        assert!(result.throughput_per_sec > 0.0);
    }
}
