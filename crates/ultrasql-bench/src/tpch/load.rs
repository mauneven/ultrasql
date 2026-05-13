//! TPC-H `.tbl` file loader.
//!
//! Reads pipe-delimited `.tbl` files produced by `dbgen` (or the synthetic
//! fallback in [`crate::tpch::data_gen`]) and bulk-inserts the rows into the
//! target engine using batched transactions of up to [`BATCH_SIZE`] rows each.
//!
//! The Postgres path is gated behind the `pg-runner` Cargo feature. When the
//! feature is disabled, calling [`load_postgres`] returns an `anyhow` error
//! describing the missing feature gate.

use std::path::Path;

use anyhow::{Context, Result, bail};

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
        let line = line.trim_end_matches('|');
        if line.is_empty() {
            continue;
        }
        let fields: Vec<String> = line.split('|').map(str::to_owned).collect();
        rows.push(fields);
    }
    Ok(rows)
}

/// Loads one `.tbl` file into PostgreSQL.
///
/// This function is only compiled when the `pg-runner` feature is active.
/// Without the feature it always returns an error.
#[cfg(feature = "pg-runner")]
pub fn load_postgres(
    client: &mut tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
    runtime: &tokio::runtime::Runtime,
) -> Result<LoadStats> {
    let path = data_dir.join(format!("{table}.tbl"));
    let rows = read_tbl(&path)?;
    let total = u64::try_from(rows.len()).context("row count overflow")?;
    let t0 = std::time::Instant::now();

    // Build the parameterised INSERT outside the batch loop to reuse the
    // string allocations.
    let col_count = column_count(table);
    let placeholders: Vec<String> = (1..=col_count).map(|i| format!("${i}")).collect();
    let insert_sql = format!("INSERT INTO {table} VALUES ({})", placeholders.join(", "));

    let mut inserted: u64 = 0;
    for chunk in rows.chunks(BATCH_SIZE) {
        runtime.block_on(async {
            let txn = client.transaction().await.context("begin transaction")?;
            for row in chunk {
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = row
                    .iter()
                    .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                txn.execute(&insert_sql, &params)
                    .await
                    .with_context(|| format!("insert into {table}"))?;
            }
            txn.commit().await.context("commit transaction")?;
            Ok::<(), anyhow::Error>(())
        })?;
        inserted += u64::try_from(chunk.len()).context("chunk len overflow")?;
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        f64::from(u32::try_from(inserted).unwrap_or(u32::MAX)) / elapsed
    } else {
        0.0
    };

    Ok(LoadStats {
        table: table.to_owned(),
        row_count: total,
        rows_per_sec,
    })
}

/// Stub returned when the `pg-runner` feature is not active.
#[cfg(not(feature = "pg-runner"))]
pub fn load_postgres(_table: &str, _data_dir: &Path) -> Result<LoadStats> {
    bail!("NotYetWired: pg-runner feature is not enabled; rebuild with --features pg-runner")
}

/// Loads all TPC-H tables from `data_dir` into UltraSQL.
///
/// Currently returns `Error::NotYetWired` because the executor's
/// `LogicalInsert` / datasource lowering path is not yet available
/// (targeted for v0.6+ executor refactor).
pub fn load_ultrasql(_data_dir: &Path) -> Result<Vec<LoadStats>> {
    bail!("NotYetWired: UltraSQL loader is pending the executor datasource refactor (v0.6+)")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_tbl_strips_trailing_pipe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.tbl");
        std::fs::write(&path, "1|Alice|42|\n2|Bob|7|\n").expect("write");
        let rows = read_tbl(&path).expect("read");
        assert_eq!(rows.len(), 2);
        // Trailing pipe stripped — 3 fields per row.
        assert_eq!(rows[0].len(), 3, "row 0 should have 3 fields");
        assert_eq!(rows[0][0], "1");
        assert_eq!(rows[0][1], "Alice");
        assert_eq!(rows[0][2], "42");
    }

    #[test]
    fn read_tbl_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.tbl");
        std::fs::write(&path, "").expect("write");
        let rows = read_tbl(&path).expect("read empty");
        assert!(rows.is_empty());
    }

    #[test]
    fn column_count_all_tables() {
        assert_eq!(column_count("region"), 3);
        assert_eq!(column_count("nation"), 4);
        assert_eq!(column_count("supplier"), 7);
        assert_eq!(column_count("customer"), 8);
        assert_eq!(column_count("part"), 9);
        assert_eq!(column_count("partsupp"), 5);
        assert_eq!(column_count("orders"), 9);
        assert_eq!(column_count("lineitem"), 16);
        assert_eq!(column_count("unknown"), 0);
    }
}
