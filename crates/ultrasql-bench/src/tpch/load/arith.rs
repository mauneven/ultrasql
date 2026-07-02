//! Numeric conversion helpers for the TPC-H loader.

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use num_traits::ToPrimitive;

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
pub(crate) fn tpch_u64_to_f64(value: u64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}
