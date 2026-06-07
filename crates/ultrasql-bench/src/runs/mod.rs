//! Real benchmark implementations for the registry's benchmark specs.
//!
//! Each sub-module exports a single `pub fn run(ctx: &BenchContext) -> BenchResult`
//! that exercises genuine UltraSQL execution paths. The modules map 1-to-1 onto
//! the `BenchSpec` entries in [`crate::registry`].
//!
//! # Dataset sizes
//!
//! Production-level dataset constants are defined per module. Test builds
//! (`#[cfg(test)]`) use small constants so the test suite completes quickly.
//! The binary-level smoke test (in `regression_gate.rs`) enables the
//! process-local smoke guard; each module calls `smoke_row_count` to select
//! the appropriate size at runtime. The `ULTRASQL_BENCH_SMOKE` environment
//! variable remains supported for shell-driven smoke runs.
//!
//! # Timing contract
//!
//! Each `run` function:
//! 1. Runs `ctx.warmup_iterations` iterations without recording samples.
//! 2. Runs `ctx.iterations` iterations and records one `f64` per iteration
//!    (elapsed microseconds) into `samples`.
//! 3. Returns a `BenchResult` with `throughput_per_sec`, `p50_latency_us`,
//!    `p99_latency_us` computed from `samples`.

use std::sync::atomic::{AtomicUsize, Ordering};

use num_traits::ToPrimitive;

static SMOKE_MODE_GUARDS: AtomicUsize = AtomicUsize::new(0);

/// Process-local smoke-mode guard.
///
/// Keep the returned guard alive for the duration of a smoke benchmark run.
/// Dropping it restores the previous state. A nesting counter is used instead
/// of a boolean so parallel tests cannot accidentally disable another test's
/// active smoke mode.
#[derive(Debug)]
pub struct SmokeModeGuard;

impl Drop for SmokeModeGuard {
    fn drop(&mut self) {
        SMOKE_MODE_GUARDS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Enables smoke mode for the current process until the returned guard drops.
#[must_use]
pub fn enable_smoke_mode_for_process() -> SmokeModeGuard {
    SMOKE_MODE_GUARDS.fetch_add(1, Ordering::SeqCst);
    SmokeModeGuard
}

fn smoke_mode_enabled() -> bool {
    SMOKE_MODE_GUARDS.load(Ordering::SeqCst) > 0
        || std::env::var_os("ULTRASQL_BENCH_SMOKE").is_some()
}

/// Returns `smoke` if the `ULTRASQL_BENCH_SMOKE` environment variable is
/// set (to any value), or if the process-local smoke guard is active;
/// otherwise returns `prod`.
///
/// This lets the binary-level smoke test run each benchmark with a tiny
/// dataset so it completes in milliseconds even in a debug build, while
/// production runs and `--release` regression gates use the full sizes.
#[must_use]
pub(crate) fn smoke_row_count(prod: usize, smoke: usize) -> usize {
    if smoke_mode_enabled() { smoke } else { prod }
}

pub(crate) fn count_as_f64(count: usize) -> f64 {
    count.to_f64().unwrap_or(f64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{count_as_f64, enable_smoke_mode_for_process, smoke_row_count};

    #[test]
    fn smoke_row_count_uses_process_guard() {
        let _guard = enable_smoke_mode_for_process();
        assert_eq!(smoke_row_count(10_000, 512), 512);
    }

    #[test]
    fn count_as_f64_converts_small_counts() {
        assert_eq!(count_as_f64(42), 42.0);
    }
}

pub mod btree_point_lookup;
pub mod csv_gauntlet;
pub mod delete_throughput;
pub mod filter_sum_10m;
pub mod hash_aggregate;
pub mod insert_throughput;
pub mod mixed_oltp;
pub mod point_lookup;
pub mod range_scan;
pub mod select_avg_10m;
pub mod select_sum_65k;
pub mod sort_large;
pub mod tpcb;
pub mod tpcc;
pub mod tpch_q1;
pub mod tpch_q22;
pub mod update_throughput;
