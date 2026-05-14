//! Real benchmark implementations replacing the `stub_run` placeholders.
//!
//! Each sub-module exports a single `pub fn run(ctx: &BenchContext) -> BenchResult`
//! that exercises genuine UltraSQL execution paths. The modules map 1-to-1 onto
//! the `BenchSpec` entries in [`crate::registry`].
//!
//! # Dataset sizes
//!
//! Production-level dataset constants are defined per module. Test builds
//! (`#[cfg(test)]`) use small constants so the test suite completes quickly.
//! The binary-level smoke test (in `regression_gate.rs`) sets the
//! `ULTRASQL_BENCH_SMOKE` environment variable; each module calls
//! `smoke_row_count` to select the appropriate size at runtime.
//!
//! # Timing contract
//!
//! Each `run` function:
//! 1. Runs `ctx.warmup_iterations` iterations without recording samples.
//! 2. Runs `ctx.iterations` iterations and records one `f64` per iteration
//!    (elapsed microseconds) into `samples`.
//! 3. Returns a `BenchResult` with `throughput_per_sec`, `p50_latency_us`,
//!    `p99_latency_us` computed from `samples`.

/// Returns `smoke` if the `ULTRASQL_BENCH_SMOKE` environment variable is
/// set (to any value), otherwise returns `prod`.
///
/// This lets the binary-level smoke test run each benchmark with a tiny
/// dataset so it completes in milliseconds even in a debug build, while
/// production runs and `--release` regression gates use the full sizes.
#[must_use]
#[allow(dead_code)] // called only from #[cfg(not(test))] branches in run modules
pub(crate) fn smoke_row_count(prod: usize, smoke: usize) -> usize {
    if std::env::var("ULTRASQL_BENCH_SMOKE").is_ok() {
        smoke
    } else {
        prod
    }
}

pub mod btree_point_lookup;
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
