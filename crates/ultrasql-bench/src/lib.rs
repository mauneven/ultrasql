//! UltraSQL benchmark harness library.
//!
//! Exposes the [`tpch`] module so the `tpch` binary and any future
//! benchmark binaries can share it without duplicating source paths, and
//! the [`registry`] module which holds the stage-tagged benchmark registry
//! used by the `regression-gate` binary.
//!
//! # Feature flags
//!
//! | Feature | Effect |
//! |---------|--------|
//! | `pg-runner` | Enables the PostgreSQL execution path in [`tpch::runner`] and [`tpch::load`], pulling in `tokio-postgres`. |

// The bench harness uses ad-hoc index arithmetic across synthetic data
// generators, iteration counters, and ASCII-table renderers. Production
// crates enforce the AGENTS.md §3.3 cast rules at `deny`; the bench
// crate carries an explicit allow at the crate root so each callsite
// does not need a per-site `#[allow]` block.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "bench harness: deterministic synthetic data + iteration math; no impact on engine crates"
)]

use std::cmp::Ordering;

pub mod ai_gauntlet;
pub mod ann_vector;
pub mod registry;
pub mod runs;
pub mod tpch;

/// Compare benchmark floating-point samples with NaN sorted after finite values.
///
/// Benchmark artifacts can contain `NaN` when a competitor reports an invalid
/// metric. Sorting those samples as equal hides the bad value inside medians and
/// rendered rankings, so report code uses this deterministic order everywhere.
#[must_use]
pub fn compare_f64_nan_last(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

/// Sort benchmark floating-point samples with invalid values last.
pub fn sort_f64_nan_last(values: &mut [f64]) {
    values.sort_by(|left, right| compare_f64_nan_last(*left, *right));
}
