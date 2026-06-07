//! `btree_point_lookup` benchmark implementation (v0.8 tag).
//!
//! Identical workload to [`crate::runs::point_lookup`] but tagged
//! under v0.8 "indexes and constraints". Serves as a stable reference
//! point as the B-tree evolves (e.g. when duplicate-key support or
//! variable-length keys land).
//!
//! Delegates entirely to the `point_lookup` implementation; the tag
//! difference is recorded in the registry.

use crate::registry::{BenchContext, BenchResult};

/// Runs the B-tree point-lookup benchmark (v0.8 tag).
///
/// See [`crate::runs::point_lookup::run`] for the implementation
/// contract and dataset description.
pub fn run(ctx: &BenchContext) -> BenchResult {
    crate::runs::point_lookup::run(ctx)
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
