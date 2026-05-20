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

pub mod ann_vector;
pub mod registry;
pub mod runs;
pub mod tpch;
