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

pub mod registry;
pub mod runs;
pub mod tpch;
