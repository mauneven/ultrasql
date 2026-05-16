//! In-memory `pg_statistic` catalog row shape.
//!
//! This module mirrors the structure of PostgreSQL's `pg_statistic` system
//! catalog at the logical level. The physical heap-backed read/write path is
//! deferred to wave 8; this module ships the row shape so the persistent
//! adapter is a thin decoder over what is already defined here.
//!
//! ## Slot conventions
//!
//! PostgreSQL allocates five parallel "slots" (`stakind`, `staop`,
//! `stanumbers`, `stavalues`) in `pg_statistic`. We model two:
//!
//! | `stakind` | Meaning |
//! |-----------|---------|
//! | `1`       | MCV list — `stavalues1` contains the values, `stanumbers1` contains their frequencies |
//! | `2`       | Histogram — `stavalues2` contains the bucket boundary values, `stanumbers2` contains `[samples_per_bucket]` |
//!
//! `stakind = 0` means the slot is unused.

use ultrasql_core::Value;

use crate::stats::column::ColumnStats;
use crate::stats::histogram::EquiDepthHistogram;
use crate::stats::mcv::MostCommonValues;

/// `pg_statistic` catalog row (simplified, in-memory).
///
/// Field names mirror the PostgreSQL catalog column names verbatim so
/// a future persistent adapter reading heap tuples can map them 1-to-1.
///
/// The five PostgreSQL slot pairs are reduced to two here; the remaining
/// three are intentionally absent and will be added when the corresponding
/// statistics kinds (correlation slot, distinct-list slot, etc.) are
/// implemented.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug, PartialEq)]
pub struct PgStatisticRow {
    /// OID of the relation this row describes.
    pub starelid: u32,
    /// 1-based attribute (column) number.
    pub staattnum: u16,
    /// Whether the statistics were collected across inherited children.
    pub stainherit: bool,
    /// Fraction of non-null rows sampled (1.0 - `null_frac`).
    ///
    /// Stored as `f32` to match the catalog column type. The precision
    /// loss is intentional and mirrors PostgreSQL.
    pub stanullfrac: f32,
    /// Average storage width in bytes.
    pub stawidth: i32,
    /// Estimated number of distinct values.
    ///
    /// Positive = absolute count. Negative = fraction of row count.
    /// Stored as `f32` — same precision constraint as PostgreSQL.
    pub stadistinct: f32,
    /// Kind of statistics in slot 1. `1` = MCV list, `0` = unused.
    pub stakind1: u16,
    /// Kind of statistics in slot 2. `2` = Histogram, `0` = unused.
    pub stakind2: u16,
    /// Numeric data for slot 1 (MCV frequencies when `stakind1 == 1`).
    pub stanumbers1: Option<Vec<f64>>,
    /// Numeric data for slot 2 (histogram metadata when `stakind2 == 2`).
    pub stanumbers2: Option<Vec<f64>>,
    /// Value data for slot 1 (MCV values when `stakind1 == 1`).
    pub stavalues1: Option<Vec<Value>>,
    /// Value data for slot 2 (histogram boundaries when `stakind2 == 2`).
    pub stavalues2: Option<Vec<Value>>,
}

impl PgStatisticRow {
    /// Build a `PgStatisticRow` from a [`ColumnStats`].
    ///
    /// Slot assignments:
    /// - Slot 1 (`stakind1 = 1`): MCV list if present.
    /// - Slot 2 (`stakind2 = 2`): Histogram if present.
    #[must_use]
    pub fn from_column_stats(starelid: u32, staattnum: u16, stats: &ColumnStats) -> Self {
        // Slot 1 — MCV.
        let (stakind1, stanumbers1, stavalues1) =
            stats.mcv.as_ref().map_or((0, None, None), |mcv| {
                (
                    1_u16,
                    Some(mcv.frequencies.clone()),
                    Some(mcv.values.clone()),
                )
            });

        // Slot 2 — Histogram.
        let (stakind2, stanumbers2, stavalues2) =
            stats.histogram.as_ref().map_or((0, None, None), |hist| {
                (
                    2_u16,
                    Some(vec![hist.samples_per_bucket as f64]),
                    Some(hist.bounds.clone()),
                )
            });

        Self {
            starelid,
            staattnum,
            stainherit: false,
            // Precision-loss casts (f64 → f32) are accepted: pg_statistic
            // stores stanullfrac / stadistinct as `f32` by design.
            #[allow(
                clippy::cast_possible_truncation,
                reason = "pg_statistic stanullfrac is f32 in the catalog row schema"
            )]
            stanullfrac: stats.null_frac as f32,
            stawidth: i32::try_from(stats.avg_width_bytes).unwrap_or(i32::MAX),
            #[allow(
                clippy::cast_possible_truncation,
                reason = "pg_statistic stadistinct is f32 in the catalog row schema"
            )]
            stadistinct: stats.n_distinct as f32,
            stakind1,
            stakind2,
            stanumbers1,
            stanumbers2,
            stavalues1,
            stavalues2,
        }
    }

    /// Reconstruct a [`ColumnStats`] from this row.
    ///
    /// The `column_index` in the returned `ColumnStats` is derived from
    /// `staattnum - 1` (converting from 1-based PG attribute numbering
    /// to 0-based).
    ///
    /// The `correlation` field is not stored in `pg_statistic` rows as
    /// modelled here; it is returned as `0.0`.
    #[must_use]
    pub fn to_column_stats(&self) -> ColumnStats {
        let column_index = usize::from(self.staattnum.saturating_sub(1));

        // Reconstruct MCV from slot 1.
        let mcv = if self.stakind1 == 1 {
            match (&self.stavalues1, &self.stanumbers1) {
                (Some(vals), Some(freqs)) => Some(MostCommonValues {
                    values: vals.clone(),
                    frequencies: freqs.clone(),
                }),
                _ => None,
            }
        } else {
            None
        };

        // Reconstruct histogram from slot 2.
        let histogram = if self.stakind2 == 2 {
            match (&self.stavalues2, &self.stanumbers2) {
                (Some(bounds), Some(numbers)) => {
                    let samples_per_bucket = numbers.first().copied().map_or(0, |n| {
                        // Histogram bucket counts are non-negative and well
                        // within u64; saturate negative or overflowing
                        // inputs rather than panicking.
                        #[allow(
                            clippy::cast_possible_truncation,
                            clippy::cast_sign_loss,
                            reason = "histogram sample counts are non-negative, bounded by row_count"
                        )]
                        {
                            n.max(0.0) as u64
                        }
                    });
                    let bucket_count =
                        u16::try_from(bounds.len().saturating_sub(1)).unwrap_or(u16::MAX);
                    if bucket_count == 0 {
                        None
                    } else {
                        Some(EquiDepthHistogram {
                            bucket_count,
                            bounds: bounds.clone(),
                            samples_per_bucket,
                        })
                    }
                }
                _ => None,
            }
        } else {
            None
        };

        let avg_width_bytes = u32::try_from(self.stawidth.max(0)).unwrap_or(0);

        ColumnStats {
            column_index,
            n_distinct: f64::from(self.stadistinct),
            null_frac: f64::from(self.stanullfrac),
            avg_width_bytes,
            histogram,
            mcv,
            correlation: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::Value;

    use super::*;
    use crate::stats::column::ColumnStats;
    use crate::stats::histogram::EquiDepthHistogram;
    use crate::stats::mcv::MostCommonValues;

    fn make_full_column_stats() -> ColumnStats {
        let samples: Vec<Value> = (1_i32..=20).map(Value::Int32).collect();
        let hist = EquiDepthHistogram::build_from_sorted(&samples, 4);
        let mcv = MostCommonValues::build_from_samples(&samples, 5);
        ColumnStats {
            column_index: 2,
            n_distinct: 20.0,
            null_frac: 0.1,
            avg_width_bytes: 4,
            histogram: Some(hist),
            mcv: Some(mcv),
            correlation: 0.95,
        }
    }

    /// `from_column_stats` + `to_column_stats` preserves `null_frac` within
    /// f32 precision.
    #[test]
    fn round_trip_preserves_null_frac() {
        let original = make_full_column_stats();
        let row = PgStatisticRow::from_column_stats(42, 3, &original);
        let recovered = row.to_column_stats();
        assert!(
            (recovered.null_frac - original.null_frac).abs() < 1e-4,
            "null_frac: {} vs {}",
            recovered.null_frac,
            original.null_frac
        );
    }

    /// `from_column_stats` + `to_column_stats` preserves MCV values.
    #[test]
    fn round_trip_preserves_mcv() {
        let original = make_full_column_stats();
        let row = PgStatisticRow::from_column_stats(1, 3, &original);
        let recovered = row.to_column_stats();
        assert!(recovered.mcv.is_some(), "MCV should survive the round-trip");
        let orig_mcv = original.mcv.as_ref().unwrap();
        let rec_mcv = recovered.mcv.as_ref().unwrap();
        assert_eq!(
            orig_mcv.values.len(),
            rec_mcv.values.len(),
            "MCV value count mismatch"
        );
    }

    /// `from_column_stats` + `to_column_stats` preserves histogram bounds.
    #[test]
    fn round_trip_preserves_histogram_bounds() {
        let original = make_full_column_stats();
        let row = PgStatisticRow::from_column_stats(1, 3, &original);
        let recovered = row.to_column_stats();
        assert!(
            recovered.histogram.is_some(),
            "histogram should survive the round-trip"
        );
        let orig_hist = original.histogram.as_ref().unwrap();
        let rec_hist = recovered.histogram.as_ref().unwrap();
        assert_eq!(
            orig_hist.bounds.len(),
            rec_hist.bounds.len(),
            "histogram bound count mismatch"
        );
    }

    /// `staattnum` converts correctly to 0-based `column_index`.
    #[test]
    fn staattnum_converts_to_zero_based_index() {
        let cs = ColumnStats {
            column_index: 0,
            n_distinct: 5.0,
            null_frac: 0.0,
            avg_width_bytes: 4,
            histogram: None,
            mcv: None,
            correlation: 0.0,
        };
        // staattnum = 3 → column_index = 2.
        let row = PgStatisticRow::from_column_stats(1, 3, &cs);
        let recovered = row.to_column_stats();
        assert_eq!(recovered.column_index, 2);
    }

    /// A row with no MCV or histogram slots recovers correctly.
    #[test]
    fn row_with_no_slots_recovers_empty_stats() {
        let cs = ColumnStats {
            column_index: 0,
            n_distinct: 3.0,
            null_frac: 0.2,
            avg_width_bytes: 8,
            histogram: None,
            mcv: None,
            correlation: 0.5,
        };
        let row = PgStatisticRow::from_column_stats(7, 1, &cs);
        assert_eq!(row.stakind1, 0);
        assert_eq!(row.stakind2, 0);
        let recovered = row.to_column_stats();
        assert!(recovered.mcv.is_none());
        assert!(recovered.histogram.is_none());
    }
}
