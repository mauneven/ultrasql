//! Cost model — populated in wave 6b.
//!
//! This module will contain selectivity estimation, join cardinality
//! estimation, and the cost formulae for sequential scans, index scans,
//! hash joins, sorts, and aggregates. It is intentionally empty in wave 6a
//! so that wave 6b can populate it without touching `lib.rs`.

#[allow(dead_code)]
/// Placeholder cost context; wave 6b replaces this with real formulas.
struct Cost;
