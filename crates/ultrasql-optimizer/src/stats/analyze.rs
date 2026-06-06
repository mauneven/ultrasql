//! `ANALYZE` driver — builds [`RelationStats`] from a row sample.
//!
//! [`AnalyzeRunner`] accepts a caller-supplied row iterator and produces a
//! complete [`RelationStats`] ready to be inserted into a
//! [`crate::stats::StatsCatalog`] implementation.
//!
//! ## Design notes
//!
//! - **Sampling**: The caller is responsible for any row sampling before
//!   passing the iterator to `run`. `AnalyzeRunner` consumes every row it
//!   sees without further sub-sampling.
//! - **Memory**: Distinct-value counting uses a `HashSet<Vec<u8>>` of
//!   canonical value keys (see `crate::stats::value_ord::value_key`).
//!   This is O(n) in the sample size, which is acceptable for v0.6 with
//!   `sample_size ≤ 30 000`.
//! - **Correlation**: Pearson's r between the sample index (physical
//!   order) and the rank of the value in sorted order. The implementation
//!   is exact over the sample; it does not attempt to correct for the
//!   sampling fraction.

use ahash::AHashSet;
use ultrasql_core::{DataType, Schema, Value};

use crate::stats::StatsError;
use crate::stats::column::ColumnStats;
use crate::stats::histogram::EquiDepthHistogram;
use crate::stats::mcv::MostCommonValues;
use crate::stats::relation::RelationStats;
use crate::stats::value_ord::{compare_values, value_key};

/// Tuning parameters for [`AnalyzeRunner`].
#[derive(Clone, Debug)]
pub struct AnalyzeOptions {
    /// Number of equi-depth histogram buckets. Defaults to 100.
    pub histogram_buckets: u16,
    /// How many most-common values to track per column. Defaults to 100.
    pub mcv_top_k: u16,
    /// Target sample size. The caller must supply at most this many rows;
    /// `run` accepts all rows it receives. Defaults to 30 000.
    pub sample_size: u64,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            histogram_buckets: 100,
            mcv_top_k: 100,
            sample_size: 30_000,
        }
    }
}

/// Drives `ANALYZE` on a single relation.
///
/// Create with [`AnalyzeRunner::new`] and call [`AnalyzeRunner::run`] once
/// per relation. The runner is stateless and may be reused across multiple
/// `run` calls.
#[derive(Debug)]
pub struct AnalyzeRunner {
    opts: AnalyzeOptions,
}

impl AnalyzeRunner {
    /// Create a new runner with the given options.
    #[must_use]
    pub const fn new(opts: AnalyzeOptions) -> Self {
        Self { opts }
    }

    /// Analyze the rows produced by `rows`, returning a [`RelationStats`].
    ///
    /// Each row must be a `Vec<Value>` whose length equals `schema.len()`.
    /// If any row has a different arity, `run` returns
    /// [`StatsError::Arity`].
    ///
    /// The `table` name is stored verbatim in the returned `RelationStats`.
    ///
    /// # Errors
    ///
    /// - [`StatsError::Arity`] if any row has a different arity from the
    ///   schema.
    /// - [`StatsError::UnsupportedType`] if a column has a type for which
    ///   histogram building is not supported (e.g., `DataType::Record`).
    pub fn run(
        &self,
        table: &str,
        schema: &Schema,
        rows: impl Iterator<Item = Vec<Value>>,
    ) -> Result<RelationStats, StatsError> {
        let ncols = schema.len();

        // Per-column accumulators.
        let mut col_values: Vec<Vec<Value>> = vec![Vec::new(); ncols];
        let mut null_counts: Vec<u64> = vec![0; ncols];
        let mut width_sums: Vec<u64> = vec![0; ncols];
        let mut total_rows: u64 = 0;

        for row in rows {
            if row.len() != ncols {
                return Err(StatsError::Arity {
                    expected: ncols,
                    got: row.len(),
                });
            }
            total_rows = total_rows.saturating_add(1);
            for (col_idx, v) in row.into_iter().enumerate() {
                if v.is_null() {
                    null_counts[col_idx] = null_counts[col_idx].saturating_add(1);
                } else {
                    let w = value_width(&v, &schema.field_at(col_idx).data_type);
                    width_sums[col_idx] = width_sums[col_idx].saturating_add(w);
                    col_values[col_idx].push(v);
                }
            }
        }

        // Validate that column types are supported.
        for (col_idx, field) in schema.fields().iter().enumerate() {
            match &field.data_type {
                DataType::Record(_) | DataType::Array(_) => {
                    return Err(StatsError::UnsupportedType {
                        index: col_idx,
                        ty: field.data_type.clone(),
                    });
                }
                _ => {}
            }
        }

        // Build per-column stats.
        let mut columns = Vec::with_capacity(ncols);
        for col_idx in 0..ncols {
            let non_null_count = col_values[col_idx].len();
            let null_frac = if total_rows == 0 {
                0.0
            } else {
                null_counts[col_idx] as f64 / total_rows as f64
            };
            let avg_width_bytes = if non_null_count == 0 {
                0_u32
            } else {
                u32::try_from(width_sums[col_idx] / non_null_count as u64).unwrap_or(u32::MAX)
            };

            // n_distinct.
            let n_distinct = compute_n_distinct(&col_values[col_idx], total_rows);

            // MCV.
            let mcv = if self.opts.mcv_top_k > 0 && !col_values[col_idx].is_empty() {
                let m =
                    MostCommonValues::build_from_samples(&col_values[col_idx], self.opts.mcv_top_k);
                if m.values.is_empty() { None } else { Some(m) }
            } else {
                None
            };

            // Histogram (sort, then build).
            let histogram = if self.opts.histogram_buckets > 0 && non_null_count >= 2 {
                let mut sorted = col_values[col_idx].clone();
                sorted.sort_unstable_by(compare_values);
                Some(EquiDepthHistogram::build_from_sorted(
                    &sorted,
                    self.opts.histogram_buckets,
                ))
            } else {
                None
            };

            // Correlation: Pearson r between sample index and value rank.
            let correlation = compute_correlation(&col_values[col_idx]);

            columns.push(ColumnStats {
                column_index: col_idx,
                n_distinct,
                null_frac,
                avg_width_bytes,
                histogram,
                mcv,
                correlation,
            });
        }

        // Estimate page count: assume 8 KiB pages, average row ~100 bytes.
        let avg_row_bytes: u64 = 100;
        let page_bytes: u64 = 8192;
        let page_count = (total_rows * avg_row_bytes).div_ceil(page_bytes).max(1);

        Ok(RelationStats {
            table: table.to_owned(),
            row_count: total_rows,
            page_count,
            columns,
        })
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Compute `n_distinct` for a non-null value slice.
///
/// If the distinct count is less than or equal to 10% of the sample, we
/// report it as an absolute count (positive). Otherwise we encode it as a
/// fraction of `total_rows` (negative).
fn compute_n_distinct(values: &[Value], total_rows: u64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut seen: AHashSet<Vec<u8>> = AHashSet::with_capacity(values.len().min(10_000));
    for v in values {
        seen.insert(value_key(v));
    }
    let distinct = seen.len();
    // If distinct is a small fraction of rows, return absolute count;
    // otherwise encode as negative fraction.
    let sample_n = values.len();
    if distinct <= sample_n / 10 {
        distinct as f64
    } else {
        -(distinct as f64 / total_rows.max(1) as f64)
    }
}

/// Compute Pearson's r between sample position (0, 1, 2, …) and the rank
/// of each value in sorted order.
///
/// Returns `0.0` for slices of length < 2.
fn compute_correlation(values: &[Value]) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }

    // Build a rank array: rank[i] is the 0-based sorted rank of values[i].
    let mut index_value: Vec<(usize, &Value)> = values.iter().enumerate().collect();
    index_value.sort_unstable_by(|(_, a), (_, b)| compare_values(a, b));

    let mut ranks: Vec<f64> = vec![0.0; n];
    for (rank, (orig_idx, _)) in index_value.iter().enumerate() {
        ranks[*orig_idx] = rank as f64;
    }

    // Pearson r between positions [0..n) and ranks.
    let n_f = n as f64;
    let pos_mean = (n_f - 1.0) / 2.0; // mean of 0, 1, ..., n-1
    let rank_mean: f64 = ranks.iter().copied().sum::<f64>() / n_f;

    // Ranks is a permutation of 0..n, so rank_mean == pos_mean.
    let mut cov = 0.0_f64;
    let mut var_pos = 0.0_f64;
    let mut var_rank = 0.0_f64;
    for (i, r) in ranks.iter().enumerate() {
        let dp = i as f64 - pos_mean;
        let dr = r - rank_mean;
        cov += dp * dr;
        var_pos += dp * dp;
        var_rank += dr * dr;
    }
    let denom = (var_pos * var_rank).sqrt();
    if denom < f64::EPSILON {
        0.0
    } else {
        (cov / denom).clamp(-1.0, 1.0)
    }
}

/// Estimate the storage width of a value in bytes.
fn value_width(v: &Value, ty: &DataType) -> u64 {
    match v {
        Value::Text(s) => s.len() as u64,
        Value::Bytea(b) => b.len() as u64,
        _ => ty.fixed_size().map_or(8, |s| s as u64),
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};

    use super::*;

    fn two_col_schema() -> Schema {
        Schema::new([
            Field::nullable("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    fn two_col_rows(n: u32) -> Vec<Vec<Value>> {
        (0..n)
            .map(|i| {
                vec![
                    Value::Int32(i32::try_from(i).unwrap_or(i32::MAX)),
                    Value::Text(format!("name_{i}")),
                ]
            })
            .collect()
    }

    /// End-to-end analyze of a synthetic two-column relation.
    #[test]
    fn analyze_two_column_relation_produces_expected_row_count() {
        let opts = AnalyzeOptions::default();
        let runner = AnalyzeRunner::new(opts);
        let schema = two_col_schema();
        let rows = two_col_rows(500);
        let stats = runner
            .run("test", &schema, rows.into_iter())
            .expect("analyze ok");
        assert_eq!(stats.table, "test");
        assert_eq!(stats.row_count, 500);
        assert_eq!(stats.columns.len(), 2);
    }

    /// Per-column stats are present for both columns.
    #[test]
    fn analyze_produces_per_column_stats() {
        let runner = AnalyzeRunner::new(AnalyzeOptions::default());
        let schema = two_col_schema();
        let rows = two_col_rows(200);
        let stats = runner.run("tbl", &schema, rows.into_iter()).expect("ok");
        for col in &stats.columns {
            // No nulls in our synthetic data.
            assert!(
                (col.null_frac - 0.0).abs() < 1e-9,
                "expected zero null_frac for column {}",
                col.column_index
            );
            // Histogram should be built (200 distinct non-null values).
            assert!(
                col.histogram.is_some(),
                "histogram missing for column {}",
                col.column_index
            );
        }
    }

    /// `run` returns `StatsError::Arity` when a row has the wrong column count.
    #[test]
    fn arity_mismatch_returns_error() {
        let runner = AnalyzeRunner::new(AnalyzeOptions::default());
        let schema = two_col_schema();
        // Supply rows with 3 columns.
        let bad_rows = vec![vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]];
        let err = runner.run("t", &schema, bad_rows.into_iter()).unwrap_err();
        assert!(
            matches!(
                err,
                StatsError::Arity {
                    expected: 2,
                    got: 3
                }
            ),
            "got {err:?}"
        );
    }

    /// Null values are counted correctly in `null_frac`.
    #[test]
    fn null_frac_computed_correctly() {
        let runner = AnalyzeRunner::new(AnalyzeOptions::default());
        let schema = Schema::new([Field::nullable("x", DataType::Int32)]).expect("ok");
        // 4 nulls out of 8 rows.
        let stats = runner
            .run(
                "t",
                &schema,
                (0..8).map(|i| {
                    if i % 2 == 0 {
                        vec![Value::Null]
                    } else {
                        vec![Value::Int32(i)]
                    }
                }),
            )
            .expect("ok");
        let nf = stats.columns[0].null_frac;
        assert!((nf - 0.5).abs() < 1e-9, "expected null_frac=0.5, got {nf}");
    }

    /// Correlation is 1.0 when values are in sorted order.
    #[test]
    fn correlation_is_one_for_sorted_input() {
        let runner = AnalyzeRunner::new(AnalyzeOptions {
            histogram_buckets: 10,
            mcv_top_k: 10,
            sample_size: 100,
        });
        let schema = Schema::new([Field::nullable("v", DataType::Int32)]).expect("ok");
        // Strictly ascending.
        let stats = runner
            .run("t", &schema, (0_i32..50).map(|i| vec![Value::Int32(i)]))
            .expect("ok");
        let corr = stats.columns[0].correlation;
        assert!(
            (corr - 1.0).abs() < 1e-9,
            "expected correlation=1.0, got {corr}"
        );
    }
}
