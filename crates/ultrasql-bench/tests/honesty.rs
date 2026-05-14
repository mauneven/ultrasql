//! Honesty tests: every registered `BenchSpec` must report numbers that are
//! consistent with the work it actually performed.
//!
//! For each non-stub spec we run with `ULTRASQL_BENCH_SMOKE=1` (tiny dataset)
//! and a single iteration, then assert:
//!
//! - `samples.len() == ctx.iterations` (the right number of samples was recorded)
//! - `p99_us > 0` (real time elapsed — the bench is not a no-op)
//! - No `NaN` or infinite value in `samples`
//! - `throughput_per_sec > 0` (sanity: positive throughput follows from > 0 time)
//!
//! The stub benchmarks (`tpcb_32conn`, `tpcc_5types`) are excluded because they
//! return all-zero placeholders by design while their execution paths are not
//! yet implemented.

use ultrasql_bench::registry::{BenchContext, HostInfo, REGISTRY};

/// Stubs that return all-zero / empty results while their real implementation
/// is not yet wired. Completely excluded from all assertions.
const STUB_IDS: &[&str] = &["tpcb_32conn", "tpcc_5types"];

/// Drops `ULTRASQL_BENCH_SMOKE` on `Drop` so a panicking test cannot leak it.
struct SmokeGuard;
impl SmokeGuard {
    fn new() -> Self {
        // SAFETY: integration tests run in a separate process; no other thread
        // writes `ULTRASQL_BENCH_SMOKE` concurrently in this process.
        unsafe { std::env::set_var("ULTRASQL_BENCH_SMOKE", "1") };
        Self
    }
}
impl Drop for SmokeGuard {
    fn drop(&mut self) {
        // SAFETY: same as above.
        unsafe { std::env::remove_var("ULTRASQL_BENCH_SMOKE") };
    }
}

fn smoke_ctx() -> BenchContext {
    BenchContext {
        iterations: 1,
        warmup_iterations: 0,
        host: HostInfo {
            cpu: "honesty-test".to_string(),
            cores: 1,
            ram_gb: 1,
            os: "test".to_string(),
        },
    }
}

#[test]
fn every_registered_spec_reports_honest_numbers() {
    let _guard = SmokeGuard::new();
    let ctx = smoke_ctx();

    for spec in REGISTRY {
        // Stubs are not expected to do real work; skip them entirely.
        if STUB_IDS.contains(&spec.id) {
            continue;
        }

        let result = (spec.run)(&ctx);

        // Must produce exactly `ctx.iterations` samples.
        assert_eq!(
            result.samples.len(),
            ctx.iterations as usize,
            "spec '{}': samples.len() {} != ctx.iterations {}",
            spec.id,
            result.samples.len(),
            ctx.iterations
        );

        // Every sample must be finite.
        for (i, &s) in result.samples.iter().enumerate() {
            assert!(
                s.is_finite(),
                "spec '{}': sample[{i}] = {s} is NaN or infinite",
                spec.id
            );
        }

        // p99 must be positive — a non-zero amount of real time elapsed.
        assert!(
            result.p99_latency_us > 0.0,
            "spec '{}': p99_latency_us = {} is not positive (bench may be a no-op)",
            spec.id,
            result.p99_latency_us
        );

        // Positive throughput follows from positive elapsed time.
        assert!(
            result.throughput_per_sec > 0.0,
            "spec '{}': throughput_per_sec = {} is not positive",
            spec.id,
            result.throughput_per_sec
        );
    }
}
