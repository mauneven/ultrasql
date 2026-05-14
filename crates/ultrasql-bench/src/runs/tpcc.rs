//! `tpcc_5types` benchmark stub.
//!
//! Full TPC-C with 5 transaction types (New-Order, Payment, Order-Status,
//! Delivery, Stock-Level) requires a complete OLTP execution path with
//! durable commits and concurrent sessions, targeting v1.0. This module
//! returns a zero-throughput [`BenchResult`] so the regression gate can
//! parse the stage baseline without a live implementation.
//!
//! The gate already skips competitor-floor checks when `throughput_per_sec`
//! is `0.0`, so this stub does not falsely fail the floor assertions.
//!
// TODO(v1.0-tpcc): replace with a real implementation once TPC-C
// transaction types are fully implemented.

use crate::registry::{BenchContext, BenchResult};

/// Returns a zero-throughput placeholder result.
///
/// Real TPC-C requires 5 transaction types driven concurrently against a
/// WAREHOUSE/DISTRICT/CUSTOMER/ITEM/STOCK/ORDER/ORDER-LINE schema with
/// durable commits. Scheduled for v1.0.
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
        assert!(
            (result.throughput_per_sec - 0.0).abs() < f64::EPSILON,
            "tpcc stub must return 0.0 throughput"
        );
        assert!(
            result.samples.is_empty(),
            "tpcc stub must return no samples"
        );
    }
}
