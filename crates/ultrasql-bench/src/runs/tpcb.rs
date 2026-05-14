//! `tpcb_32conn` benchmark stub.
//!
//! TPC-B with 32 concurrent connections requires a complete async connection
//! handler and a multi-writer WAL path, both targeting v0.9. This module
//! returns a zero-throughput [`BenchResult`] so the regression gate can
//! parse the stage baseline without a live implementation.
//!
//! The gate already skips competitor-floor checks when `throughput_per_sec`
//! is `0.0`, so this stub does not falsely fail the floor assertions.
//!
// TODO(v0.9-tpcb): replace with a real implementation once the async
// connection handler and multi-writer WAL path land.

use crate::registry::{BenchContext, BenchResult};

/// Returns a zero-throughput placeholder result.
///
/// Real TPC-B requires 32 concurrent PostgreSQL-wire connections driving
/// a mix of balance reads, branch updates, teller updates, and account
/// updates with durable commit per transaction. Scheduled for v0.9.
// Vec allocation prevents making this const fn.
#[allow(clippy::missing_const_for_fn)]
pub fn run(_ctx: &BenchContext) -> BenchResult {
    BenchResult {
        throughput_per_sec: 0.0,
        p50_latency_us: 0.0,
        p99_latency_us: 0.0,
        samples: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{BenchContext, HostInfo};

    #[test]
    fn stub_returns_zero_throughput_without_panic() {
        let ctx = BenchContext {
            iterations: 2,
            warmup_iterations: 1,
            host: HostInfo {
                cpu: "test".to_string(),
                cores: 1,
                ram_gb: 1,
                os: "test".to_string(),
            },
        };
        let result = run(&ctx);
        // Stub must not panic and must return exactly 0.0 throughput.
        assert!(
            (result.throughput_per_sec - 0.0).abs() < f64::EPSILON,
            "tpcb stub must return 0.0 throughput"
        );
        assert!(
            result.samples.is_empty(),
            "tpcb stub must return no samples"
        );
    }
}
