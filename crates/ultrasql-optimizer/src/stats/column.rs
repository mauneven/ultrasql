//! Per-column statistics used by the cost model.
//!
//! [`ColumnStats`] captures all planner-relevant statistics for a single
//! column: the estimated number of distinct values, null fraction, average
//! storage width, an optional equi-depth histogram, an optional MCV list,
//! and the physical-vs-logical correlation.
//!
//! The `n_distinct` encoding follows PostgreSQL convention:
//! - A positive value is an absolute distinct-value count.
//! - A negative value is a fraction of rows (e.g., `-0.1` means roughly
//!   10 % of rows are distinct).

use crate::stats::histogram::EquiDepthHistogram;
use crate::stats::mcv::MostCommonValues;

/// Per-column statistics produced by `ANALYZE` and consumed by the cost
/// model.
///
/// All floating-point fields (`null_frac`, `correlation`) are stored as
/// `f64` for precision; adapters that write to the `pg_statistic` catalog
/// may lossy-round to `f32`.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStats {
    /// 0-based column index in the parent relation's schema.
    pub column_index: usize,
    /// Estimated number of distinct non-NULL values.
    ///
    /// Positive: absolute count. Negative: fraction of row count (e.g.
    /// `-0.1` means "about 10% of rows have a distinct value").
    pub n_distinct: f64,
    /// Fraction of rows that are NULL: `0.0` to `1.0`.
    pub null_frac: f64,
    /// Average storage width in bytes across non-NULL values.
    pub avg_width_bytes: u32,
    /// Optional equi-depth histogram over non-NULL, non-MCV values.
    pub histogram: Option<EquiDepthHistogram>,
    /// Optional most-common-values list.
    pub mcv: Option<MostCommonValues>,
    /// Pearson correlation between the physical row order and the sorted
    /// value order. Ranges from `-1.0` (perfect reverse order) to `1.0`
    /// (perfect forward order). Used by the index-scan cost model.
    pub correlation: f64,
}

#[cfg(test)]
mod tests {
    use ultrasql_core::Value;

    use super::*;
    use crate::stats::histogram::EquiDepthHistogram;
    use crate::stats::mcv::MostCommonValues;
    use crate::stats::pg_statistic::PgStatisticRow;

    fn sample_column_stats() -> ColumnStats {
        let samples: Vec<Value> = (1_i32..=20).map(Value::Int32).collect();
        let hist = EquiDepthHistogram::build_from_sorted(&samples, 5);
        let mcv = MostCommonValues::build_from_samples(&samples, 3);
        ColumnStats {
            column_index: 0,
            n_distinct: 20.0,
            null_frac: 0.05,
            avg_width_bytes: 4,
            histogram: Some(hist),
            mcv: Some(mcv),
            correlation: 0.85,
        }
    }

    /// `ColumnStats` can be constructed and all fields are accessible.
    #[test]
    fn column_stats_fields_are_accessible() {
        let cs = sample_column_stats();
        assert_eq!(cs.column_index, 0);
        assert!((cs.n_distinct - 20.0).abs() < 1e-9);
        assert!(cs.histogram.is_some());
        assert!(cs.mcv.is_some());
    }

    /// Round-trip via `PgStatisticRow` preserves significant fields.
    #[test]
    fn round_trip_via_pg_statistic_preserves_fields() {
        let original = sample_column_stats();
        let row = PgStatisticRow::from_column_stats(1, 1, &original);
        let recovered = row.to_column_stats();

        // null_frac and avg_width round-trip through f32 so allow small error.
        assert!(
            (recovered.null_frac - original.null_frac).abs() < 1e-4,
            "null_frac: {} vs {}",
            recovered.null_frac,
            original.null_frac
        );
        assert_eq!(
            recovered.avg_width_bytes, original.avg_width_bytes,
            "avg_width_bytes should be preserved"
        );
        // n_distinct round-trips through f32.
        assert!(
            (recovered.n_distinct - original.n_distinct).abs() < 0.5,
            "n_distinct: {} vs {}",
            recovered.n_distinct,
            original.n_distinct
        );
    }

    /// Negative `n_distinct` encodes a fraction correctly.
    #[test]
    fn negative_n_distinct_encodes_fraction() {
        let cs = ColumnStats {
            column_index: 0,
            n_distinct: -0.1,
            null_frac: 0.0,
            avg_width_bytes: 4,
            histogram: None,
            mcv: None,
            correlation: 0.0,
        };
        assert!(cs.n_distinct < 0.0, "negative means fraction-of-rows");
        let row = PgStatisticRow::from_column_stats(0, 1, &cs);
        let recovered = row.to_column_stats();
        assert!(
            recovered.n_distinct < 0.0,
            "fraction encoding should be preserved"
        );
    }
}
