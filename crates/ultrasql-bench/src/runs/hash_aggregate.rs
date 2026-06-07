//! `hash_aggregate` benchmark implementation.
//!
//! Simulates `SELECT COUNT(*), SUM(value) FROM t GROUP BY group_id` over
//! 1 000 000 rows with 1 000 distinct `group_id` values. The implementation
//! uses a `HashMap<i32, (i64, i64)>` (group → `(count, sum)`) which mirrors
//! the kernel-level logic of `ultrasql_executor::HashAggregate` without
//! depending on the executor crate (which is a dev-dependency only).
//!
//! Additionally, the [`ultrasql_vec::kernels::hash_i64`] kernel is called
//! on the group column to exercise the hash path used by the real operator.
//!
//! Throughput = `ROW_COUNT / median_elapsed_seconds`.

use std::collections::HashMap;
use std::time::Instant;

use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::kernels::hash_i64;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production row count: 1 000 000 rows.
#[cfg(not(test))]
const PROD_ROW_COUNT: usize = 1_000_000;

/// Reduced row count for fast unit tests.
#[cfg(test)]
const TEST_ROW_COUNT: usize = 2_000;

/// Smoke-mode row count (used when `ULTRASQL_BENCH_SMOKE` is set).
#[cfg(not(test))]
const SMOKE_ROW_COUNT: usize = 500;

/// Number of distinct `group_id` values.
const GROUP_COUNT: usize = 1_000;

/// Builds two columns: `group_id` (i64, cycling through `GROUP_COUNT`)
/// and `value` (i64, deterministic from the row index).
fn build_columns(n: usize) -> (Vec<i32>, Vec<i64>) {
    let mut group_ids: Vec<i32> = Vec::with_capacity(n);
    let mut values: Vec<i64> = Vec::with_capacity(n);
    for i in 0..n {
        let gid = i32::try_from(i % GROUP_COUNT).unwrap_or(0);
        let val = i64::try_from(i).unwrap_or(0).wrapping_mul(999_983);
        group_ids.push(gid);
        values.push(val);
    }
    (group_ids, values)
}

/// Runs the hash-aggregate benchmark.
///
/// Each measured iteration:
/// 1. Calls `hash_i64` on a `NumericColumn<i64>` built from `group_ids`
///    to exercise the hash kernel path.
/// 2. Accumulates `(count, sum)` per group into a `HashMap<i32, (i64, i64)>`.
/// 3. Discards results via `black_box`.
pub fn run(ctx: &BenchContext) -> BenchResult {
    #[cfg(test)]
    let row_count = TEST_ROW_COUNT;
    #[cfg(not(test))]
    let row_count = crate::runs::smoke_row_count(PROD_ROW_COUNT, SMOKE_ROW_COUNT);

    let (group_ids, values) = build_columns(row_count);

    // Build the i64 version of group_ids for the hash kernel.
    let group_i64: Vec<i64> = group_ids.iter().map(|&g| i64::from(g)).collect();
    let group_col = NumericColumn::from_data(group_i64);

    let timed_iter = |gids: &[i32], vals: &[i64], gc: &NumericColumn<i64>| -> f64 {
        let t0 = Instant::now();

        // Exercise the hash kernel (as the real operator does).
        // The second argument is an optional validity bitmap; None means
        // no nulls in this non-nullable column.
        let hashes = hash_i64(gc, None);
        std::hint::black_box(&hashes);

        // Accumulate aggregates.
        let mut table: HashMap<i32, (i64, i64)> = HashMap::with_capacity(GROUP_COUNT * 2);
        for (i, &gid) in gids.iter().enumerate() {
            let val = vals[i];
            let entry = table.entry(gid).or_insert((0_i64, 0_i64));
            entry.0 = entry.0.wrapping_add(1);
            entry.1 = entry.1.wrapping_add(val);
        }
        std::hint::black_box(&table);

        let elapsed = t0.elapsed();
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&group_ids, &values, &group_col);
    }

    let iteration_count = usize::try_from(ctx.iterations).unwrap_or(0);
    let mut samples: Vec<f64> = Vec::with_capacity(iteration_count);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&group_ids, &values, &group_col));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let rows = crate::runs::count_as_f64(row_count);
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
        assert_eq!(
            result.samples.len(),
            usize::try_from(ctx.iterations).unwrap_or(0)
        );
        assert!(result.throughput_per_sec > 0.0);
    }
}
