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
//! | `arith`       | numeric conversions |
//! | `encode`      | literal formatters + `INSERT ... VALUES` SQL builder |
//! | `loader`      | public load entry points + wire-protocol COPY/INSERT loaders |

use std::path::Path;

use anyhow::{Context, Result};

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
mod arith;
#[cfg(any(test, feature = "sql-bench"))]
mod encode;
mod loader;

#[cfg(test)]
mod tests;

pub use loader::{load_postgres, load_ultrasql};

#[cfg(feature = "sql-bench")]
pub(crate) use loader::load_ultrasql_into_client;
#[cfg(feature = "sql-bench")]
pub(crate) use loader::ultrasql_tpch_pool_frames;

/// Number of rows per INSERT transaction batch.
pub const BATCH_SIZE: usize = 10_000;

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
