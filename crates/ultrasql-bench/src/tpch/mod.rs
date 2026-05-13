//! TPC-H scale-1 benchmark harness.
//!
//! This module provides everything needed to bootstrap the TPC-H schema,
//! generate or ingest data, run all 22 standard queries, record per-query
//! timing baselines, and detect regressions against a previously recorded
//! baseline.
//!
//! ## Sub-modules
//!
//! | Module | Responsibility |
//! |--------|---------------|
//! | [`schema`]   | DDL constants for the 8 TPC-H tables |
//! | [`data_gen`] | `dbgen` wrapper + deterministic synthetic fallback |
//! | [`load`]     | `.tbl` → engine bulk loader |
//! | [`queries`]  | All 22 TPC-H query SQL strings |
//! | [`runner`]   | Timing harness; per-query median + p95 |
//! | [`baseline`] | Baseline JSON read/write + regression detection |

pub mod baseline;
pub mod data_gen;
pub mod load;
pub mod queries;
pub mod runner;
pub mod schema;
