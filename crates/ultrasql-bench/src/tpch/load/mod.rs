//! TPC-H `.tbl` file loader.
//!
//! Reads pipe-delimited `.tbl` files produced by `dbgen` (or the synthetic
//! fallback in [`crate::tpch::data_gen`]) and bulk-inserts the rows into the
//! target engine using batched transactions of up to [`BATCH_SIZE`] rows each.
//!
//! The Postgres path is gated behind the `pg-runner` Cargo feature. When the
//! feature is disabled, calling [`load_postgres`] returns an `anyhow` error
//! describing the missing feature gate.
//!
//! ## Sub-modules
//!
//! The loader was split out of a single oversized file into cohesive
//! sub-modules; every previously-reachable path is preserved via the
//! re-exports below.
//!
//! | Module | Responsibility |
//! |--------|---------------|
//! | `arith`       | checked fixed-point arithmetic + numeric conversions |
//! | `encode`      | row encoders, literal formatters, binary heap codecs |
//! | `loader`      | public load entry points + wire-protocol COPY/INSERT loaders |
//! | `direct_load` | direct-load orchestration + per-query sidecar caching |
//! | `direct_table`| per-table direct heap loader + Q1 columnar cache |
//! | `sidecars_*`    | TPC-H Q2-Q21 direct-load sidecar build states |

use std::path::Path;

use anyhow::{Context, Result};

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
mod arith;
#[cfg(feature = "sql-bench")]
mod direct_load;
#[cfg(feature = "sql-bench")]
mod direct_table;
#[cfg(any(test, feature = "sql-bench"))]
mod encode;
mod loader;
#[cfg(feature = "sql-bench")]
mod sidecars_q11_q15;
#[cfg(feature = "sql-bench")]
mod sidecars_q16_q18;
#[cfg(feature = "sql-bench")]
mod sidecars_q19_q21;
#[cfg(feature = "sql-bench")]
mod sidecars_q2_q5;
#[cfg(feature = "sql-bench")]
mod sidecars_q7_q10;

#[cfg(test)]
mod tests;

pub use loader::{load_postgres, load_ultrasql};

#[cfg(feature = "sql-bench")]
pub(crate) use direct_load::load_ultrasql_direct_into_server;
#[cfg(feature = "sql-bench")]
pub(crate) use loader::load_ultrasql_into_client;
#[cfg(feature = "sql-bench")]
pub(crate) use loader::{ultrasql_direct_load_enabled, ultrasql_tpch_pool_frames};

/// Number of rows per INSERT transaction batch.
pub const BATCH_SIZE: usize = 10_000;

#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02: i32 = -486;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q6_SHIPDATE_START_1994_01_01: i32 = -2_191;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q6_SHIPDATE_END_1995_01_01: i32 = -1_826;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q6_DISCOUNT_MIN: i64 = 5;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q6_DISCOUNT_MAX: i64 = 7;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q6_QUANTITY_LIMIT: i64 = 2_400;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q3_DATE_1995_03_15: i32 = -1_753;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q4_ORDERDATE_START_1993_07_01: i32 = -2_375;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q4_ORDERDATE_END_1993_10_01: i32 = -2_283;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q7_SHIPDATE_END_EXCLUSIVE_1997_01_01: i32 = -1_095;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q7_YEAR_1996_START_1996_01_01: i32 = -1_461;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q12_RECEIPTDATE_START_1994_01_01: i32 = -2_191;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q12_RECEIPTDATE_END_1995_01_01: i32 = -1_826;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q14_SHIPDATE_START_1995_09_01: i32 = -1_583;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q14_SHIPDATE_END_1995_10_01: i32 = -1_553;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q15_SHIPDATE_START_1996_01_01: i32 = -1_461;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_Q15_SHIPDATE_END_1996_04_01: i32 = -1_370;

/// Row-count summary returned after a successful load.
#[derive(Debug)]
pub struct LoadStats {
    /// Name of the table that was loaded.
    pub table: String,
    /// Total rows inserted.
    pub row_count: u64,
    /// Load throughput in rows per second.
    pub rows_per_sec: f64,
}

/// Reads a `.tbl` file and returns the rows as a `Vec<Vec<String>>`.
///
/// Each inner `Vec<String>` is one row; fields are split on `|`. The trailing
/// `|` that `dbgen` appends to every row is silently stripped.
pub fn read_tbl(path: &Path) -> Result<Vec<Vec<String>>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut rows = Vec::new();
    for line in raw.lines() {
        if let Some(fields) = parse_tbl_line(line) {
            rows.push(fields);
        }
    }
    Ok(rows)
}

pub(crate) fn parse_tbl_line(line: &str) -> Option<Vec<String>> {
    let line = line.trim_end_matches('|');
    if line.is_empty() {
        return None;
    }
    Some(line.split('|').map(str::to_owned).collect())
}

/// Returns the number of columns in the TPC-H table with the given name.
///
/// These counts mirror the TPC-H schema constants in [`crate::tpch::schema`].
pub fn column_count(table: &str) -> usize {
    match table {
        "region" => 3,
        "nation" => 4,
        "supplier" => 7,
        "customer" => 8,
        "part" | "orders" => 9,
        "partsupp" => 5,
        "lineitem" => 16,
        _ => 0,
    }
}
