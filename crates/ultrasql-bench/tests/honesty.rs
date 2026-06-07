//! Honesty tests: every registered `BenchSpec` must report numbers that are
//! consistent with the work it actually performed.
//!
//! For each spec we run with the process-local smoke guard (tiny dataset)
//! and a single iteration, then assert:
//!
//! - `samples.len() == ctx.iterations` (the right number of samples was recorded)
//! - `p99_us > 0` (real time elapsed — the bench is not a no-op)
//! - No `NaN` or infinite value in `samples`
//! - `throughput_per_sec > 0` (sanity: positive throughput follows from > 0 time)
//!
use ultrasql_bench::registry::{BenchContext, HostInfo, REGISTRY};
use ultrasql_bench::runs::enable_smoke_mode_for_process;

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
    let _guard = enable_smoke_mode_for_process();
    let ctx = smoke_ctx();

    for spec in REGISTRY {
        let result = (spec.run)(&ctx);

        // Must produce exactly `ctx.iterations` samples.
        assert_eq!(
            result.samples.len(),
            usize::try_from(ctx.iterations).unwrap_or(0),
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
