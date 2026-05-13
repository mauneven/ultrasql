//! Statistics subsystem — populated in wave 6b.
//!
//! This module will contain per-column histograms, most-common-value lists,
//! per-relation row/page counts, and index correlation data sourced from
//! `ANALYZE`. It is intentionally empty in wave 6a so that wave 6b can
//! populate it without touching `lib.rs`.

#[allow(dead_code)]
/// Placeholder statistics context; wave 6b replaces this with real data.
struct Stats;
