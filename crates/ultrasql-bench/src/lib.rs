//! UltraSQL benchmark harness library.
//!
//! Re-exports the [`tpch`] module so the `tpch` binary and any future
//! benchmark binaries can share it without duplicating source paths.
//!
//! # Feature flags
//!
//! | Feature | Effect |
//! |---------|--------|
//! | `pg-runner` | Enables the PostgreSQL execution path in [`tpch::runner`] and [`tpch::load`], pulling in `tokio-postgres`. |

pub mod tpch;
