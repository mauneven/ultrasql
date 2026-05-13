//! Per-relation statistics.
//!
//! [`RelationStats`] bundles the table-level statistics (row count, page
//! count) with the per-column statistics collected during `ANALYZE`.
//! It is the primary unit exchanged between the statistics catalog and
//! the cost model.

use crate::stats::column::ColumnStats;

/// Statistics for a single relation (table).
///
/// Produced by [`crate::stats::analyze::AnalyzeRunner`] and registered in
/// a [`crate::stats::StatsCatalog`] implementation.
///
/// `row_count` and `page_count` are estimates; they are snapshotted at
/// `ANALYZE` time and may drift from reality. The cost model tolerates
/// moderate inaccuracies.
#[derive(Clone, Debug, PartialEq)]
pub struct RelationStats {
    /// Case-folded table name, matching the name used in
    /// [`ultrasql_planner::LogicalPlan::Scan`].
    pub table: String,
    /// Estimated number of live rows at `ANALYZE` time.
    pub row_count: u64,
    /// Estimated number of 8 KiB heap pages occupied by the relation.
    pub page_count: u64,
    /// Per-column statistics, one entry per column in the relation's
    /// schema. The entry at index `i` corresponds to column index `i`.
    pub columns: Vec<ColumnStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::column::ColumnStats;

    fn make_rel_stats(cols: usize) -> RelationStats {
        let columns = (0..cols)
            .map(|i| ColumnStats {
                column_index: i,
                n_distinct: 100.0,
                null_frac: 0.0,
                avg_width_bytes: 4,
                histogram: None,
                mcv: None,
                correlation: 1.0,
            })
            .collect();
        RelationStats {
            table: "test_table".to_owned(),
            row_count: 10_000,
            page_count: 50,
            columns,
        }
    }

    /// `RelationStats` fields round-trip correctly.
    #[test]
    fn fields_are_accessible() {
        let rs = make_rel_stats(3);
        assert_eq!(rs.table, "test_table");
        assert_eq!(rs.row_count, 10_000);
        assert_eq!(rs.page_count, 50);
        assert_eq!(rs.columns.len(), 3);
    }

    /// Column index within `RelationStats` matches the expected position.
    #[test]
    fn column_indices_match_position() {
        let rs = make_rel_stats(4);
        for (i, col) in rs.columns.iter().enumerate() {
            assert_eq!(
                col.column_index, i,
                "column at position {i} has wrong index"
            );
        }
    }

    /// A relation with zero columns is valid.
    #[test]
    fn zero_column_relation_is_valid() {
        let rs = make_rel_stats(0);
        assert!(rs.columns.is_empty());
    }
}
