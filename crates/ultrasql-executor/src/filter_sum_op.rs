//! Fused `SELECT SUM(col_sum) FROM t WHERE col_pred op lit`
//! operator that bypasses the per-batch `select_column`
//! materialisation a generic `Filter → HashAggregate` chain pays.
//!
//! At pipeline lowering, when the plan tree matches the exact
//! shape
//!
//! ```text
//! Aggregate { group_by: [], aggregates: [Sum(col_sum)] }
//!   └── Filter { col_pred op lit }
//!         └── Scan { rel }
//! ```
//!
//! and every involved column is `Int32` (the bench's
//! `(id INT, x INT)` shape), the executor lowers to
//! [`FilterSumI32Scan`] instead of the generic
//! `HashAggregate(Filter(SeqScan))` chain. The fused operator
//! drives its child a batch at a time and runs two SIMD passes
//! per batch:
//!
//! 1. `cmp_i32_scalar` builds the predicate bitmap.
//! 2. `sum_i32_widening_with_mask` walks the sum column and
//!    accumulates only the lanes whose mask bit is set.
//!
//! `Filter::select_column` (per-row scalar `push`) is skipped
//! entirely — for a 50%-selectivity 1M-row scan it saves ~500 k
//! pushes per emitted column, which is the dominant cost of the
//! `filter_sum_1m_i64` workload after the column-cache landed.
//!
//! Output schema is `[("sum", Int64)]`, matching PostgreSQL's
//! widening rule for `SUM(INT)` and the binder's
//! `AggregateFunc::Sum` result type.

use std::sync::Arc;

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_storage::column_cache::CachedColumns;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};
use ultrasql_vec::jit::{JitConfig, filter_sum_i32_widening_gt_jit, filter_sum_i64_gt_jit};
use ultrasql_vec::kernels::{
    CmpOp, cmp_i32_scalar, cmp_i64_scalar, filter_sum_i32_widening_gt, filter_sum_i64_gt,
    sum_i32_widening, sum_i32_widening_with_mask, sum_i64_with_mask,
};

use crate::{ExecError, Operator};

/// Fused filter + SUM operator over an `Int32` predicate and
/// `Int32` sum column. See module docs.
pub struct FilterSumI32Scan {
    /// Upstream batch source — typically [`crate::SeqScan`] (the
    /// column-cache fast path is fully transparent to this
    /// operator, since `SeqScan::next_batch` already replays cached
    /// columns when present).
    inner: Box<dyn Operator>,
    /// Index of the predicate column in the inner operator's
    /// output schema.
    predicate_col: usize,
    /// Right-hand-side literal of the predicate.
    predicate_threshold: i32,
    /// Predicate comparison op.
    predicate_op: CmpOp,
    /// Index of the column to sum in the inner operator's output
    /// schema.
    sum_col: usize,
    /// Output schema: `[("sum", Int64)]`.
    output_schema: Schema,
    /// `true` after the operator has emitted its single-row
    /// result batch. Subsequent calls return `Ok(None)`.
    done: bool,
    /// Per-statement JIT policy inherited from the lowerer.
    jit: JitConfig,
}

/// Fused filter + SUM operator over an `Int64` predicate and `Int64`
/// sum column. This is the `BIGINT` sibling of [`FilterSumI32Scan`].
pub struct FilterSumI64Scan {
    inner: Box<dyn Operator>,
    predicate_col: usize,
    predicate_threshold: i64,
    predicate_op: CmpOp,
    sum_col: usize,
    output_schema: Schema,
    done: bool,
    jit: JitConfig,
}

impl std::fmt::Debug for FilterSumI32Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterSumI32Scan")
            .field("predicate_col", &self.predicate_col)
            .field("predicate_threshold", &self.predicate_threshold)
            .field("predicate_op", &self.predicate_op)
            .field("sum_col", &self.sum_col)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl FilterSumI32Scan {
    /// Build the fused operator. Caller is responsible for
    /// validating that `predicate_col` and `sum_col` are valid
    /// indices into `inner.schema()` and both reference `Int32`
    /// columns — the pipeline-lowering caller does that as part of
    /// the pattern match that produces this operator.
    #[must_use]
    pub fn new(
        inner: Box<dyn Operator>,
        predicate_col: usize,
        predicate_threshold: i32,
        predicate_op: CmpOp,
        sum_col: usize,
        output_name: String,
    ) -> Self {
        let output_schema = Schema::new([Field::required(output_name, DataType::Int64)])
            .expect("output schema is trivially well-formed");
        Self {
            inner,
            predicate_col,
            predicate_threshold,
            predicate_op,
            sum_col,
            output_schema,
            done: false,
            jit: JitConfig::OFF,
        }
    }

    /// Enable runtime-compiled kernels for this operator.
    #[must_use]
    pub fn with_jit(mut self, jit: JitConfig) -> Self {
        self.jit = jit;
        self
    }
}

impl std::fmt::Debug for FilterSumI64Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterSumI64Scan")
            .field("predicate_col", &self.predicate_col)
            .field("predicate_threshold", &self.predicate_threshold)
            .field("predicate_op", &self.predicate_op)
            .field("sum_col", &self.sum_col)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl FilterSumI64Scan {
    /// Build the fused `Int64` filter-sum operator.
    #[must_use]
    pub fn new(
        inner: Box<dyn Operator>,
        predicate_col: usize,
        predicate_threshold: i64,
        predicate_op: CmpOp,
        sum_col: usize,
        output_name: String,
    ) -> Self {
        let output_schema = Schema::new([Field::required(output_name, DataType::Int64)])
            .expect("output schema is trivially well-formed");
        Self {
            inner,
            predicate_col,
            predicate_threshold,
            predicate_op,
            sum_col,
            output_schema,
            done: false,
            jit: JitConfig::OFF,
        }
    }

    /// Enable runtime-compiled kernels for this operator.
    #[must_use]
    pub fn with_jit(mut self, jit: JitConfig) -> Self {
        self.jit = jit;
        self
    }
}

/// Direct-from-cache variant of [`FilterSumI32Scan`].
///
/// When the relation already has a live
/// [`ColumnCache`][ultrasql_storage::column_cache::ColumnCache]
/// entry, pipeline lowering wires this operator instead of the
/// `SeqScan(cache) → FilterSumI32Scan` chain. The cache-driving
/// `SeqScan` would copy the entire column out of the cache via
/// `slice_column` (one 4 MB `memcpy` per 1 M-row Int32 column)
/// before passing it to `FilterSumI32Scan`. Reading directly from
/// the `Arc<CachedColumns>` borrow skips that copy and runs the
/// fused SIMD kernel once over the full relation.
pub struct CachedFilterSumI32Scan {
    columns: Arc<CachedColumns>,
    predicate_col: usize,
    predicate_threshold: i32,
    predicate_op: CmpOp,
    sum_col: usize,
    output_schema: Schema,
    done: bool,
    jit: JitConfig,
}

/// Direct-from-cache variant of [`FilterSumI64Scan`].
pub struct CachedFilterSumI64Scan {
    columns: Arc<CachedColumns>,
    predicate_col: usize,
    predicate_threshold: i64,
    predicate_op: CmpOp,
    sum_col: usize,
    output_schema: Schema,
    done: bool,
    jit: JitConfig,
}

impl std::fmt::Debug for CachedFilterSumI32Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFilterSumI32Scan")
            .field("predicate_col", &self.predicate_col)
            .field("predicate_threshold", &self.predicate_threshold)
            .field("predicate_op", &self.predicate_op)
            .field("sum_col", &self.sum_col)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl CachedFilterSumI32Scan {
    /// Build the cached-input fused operator. Caller is responsible
    /// for verifying that `predicate_col` and `sum_col` reference
    /// `Int32` columns inside `columns.columns`.
    #[must_use]
    pub fn new(
        columns: Arc<CachedColumns>,
        predicate_col: usize,
        predicate_threshold: i32,
        predicate_op: CmpOp,
        sum_col: usize,
        output_name: String,
    ) -> Self {
        let output_schema = Schema::new([Field::required(output_name, DataType::Int64)])
            .expect("output schema is trivially well-formed");
        Self {
            columns,
            predicate_col,
            predicate_threshold,
            predicate_op,
            sum_col,
            output_schema,
            done: false,
            jit: JitConfig::OFF,
        }
    }

    /// Enable runtime-compiled kernels for this operator.
    #[must_use]
    pub fn with_jit(mut self, jit: JitConfig) -> Self {
        self.jit = jit;
        self
    }
}

impl std::fmt::Debug for CachedFilterSumI64Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFilterSumI64Scan")
            .field("predicate_col", &self.predicate_col)
            .field("predicate_threshold", &self.predicate_threshold)
            .field("predicate_op", &self.predicate_op)
            .field("sum_col", &self.sum_col)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl CachedFilterSumI64Scan {
    /// Build the cached-input fused `Int64` operator.
    #[must_use]
    pub fn new(
        columns: Arc<CachedColumns>,
        predicate_col: usize,
        predicate_threshold: i64,
        predicate_op: CmpOp,
        sum_col: usize,
        output_name: String,
    ) -> Self {
        let output_schema = Schema::new([Field::required(output_name, DataType::Int64)])
            .expect("output schema is trivially well-formed");
        Self {
            columns,
            predicate_col,
            predicate_threshold,
            predicate_op,
            sum_col,
            output_schema,
            done: false,
            jit: JitConfig::OFF,
        }
    }

    /// Enable runtime-compiled kernels for this operator.
    #[must_use]
    pub fn with_jit(mut self, jit: JitConfig) -> Self {
        self.jit = jit;
        self
    }
}

impl Operator for CachedFilterSumI32Scan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let cols = &self.columns.columns;
        let (pred_col, sum_col) = match (&cols[self.predicate_col], &cols[self.sum_col]) {
            (Column::Int32(p), Column::Int32(s)) => (p, s),
            _ => {
                return Err(ExecError::TypeMismatch(
                    "CachedFilterSumI32Scan: predicate and sum columns must both be Int32"
                        .to_owned(),
                ));
            }
        };
        let n_rows = pred_col.len();
        let total = if self.predicate_col == self.sum_col && matches!(self.predicate_op, CmpOp::Gt)
        {
            if self.jit.should_jit(n_rows) {
                filter_sum_i32_widening_gt_jit(sum_col.data(), self.predicate_threshold)
                    .unwrap_or_else(|| {
                        filter_sum_i32_widening_gt(sum_col.data(), self.predicate_threshold)
                    })
            } else {
                filter_sum_i32_widening_gt(sum_col.data(), self.predicate_threshold)
            }
        } else {
            let mask = cmp_i32_scalar(pred_col, self.predicate_threshold, self.predicate_op);
            sum_i32_widening_with_mask(sum_col, &mask)
        };

        let result_col = if n_rows == 0 {
            let mut nulls = ultrasql_vec::Bitmap::new(1, false);
            nulls.set(0, false);
            Column::Int64(NumericColumn::with_nulls(vec![0_i64], nulls).expect("matching lengths"))
        } else {
            Column::Int64(NumericColumn::from_data(vec![total]))
        };
        let batch = Batch::new([result_col]).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        // Scalar aggregate emits exactly one row; see the matching
        // override on [`CachedSumI32Scan::estimated_row_count`].
        Some(1)
    }
}

impl Operator for CachedFilterSumI64Scan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let cols = &self.columns.columns;
        let (pred_col, sum_col) = match (&cols[self.predicate_col], &cols[self.sum_col]) {
            (Column::Int64(p), Column::Int64(s)) => (p, s),
            _ => {
                return Err(ExecError::TypeMismatch(
                    "CachedFilterSumI64Scan: predicate and sum columns must both be Int64"
                        .to_owned(),
                ));
            }
        };
        let n_rows = pred_col.len();
        let total = if self.predicate_col == self.sum_col && matches!(self.predicate_op, CmpOp::Gt)
        {
            if self.jit.should_jit(n_rows) {
                filter_sum_i64_gt_jit(sum_col.data(), self.predicate_threshold)
                    .unwrap_or_else(|| filter_sum_i64_gt(sum_col.data(), self.predicate_threshold))
            } else {
                filter_sum_i64_gt(sum_col.data(), self.predicate_threshold)
            }
        } else {
            let mask = cmp_i64_scalar(pred_col, self.predicate_threshold, self.predicate_op);
            sum_i64_with_mask(sum_col, &mask)
        };

        let result_col = if n_rows == 0 {
            let mut nulls = ultrasql_vec::Bitmap::new(1, false);
            nulls.set(0, false);
            Column::Int64(NumericColumn::with_nulls(vec![0_i64], nulls).expect("matching lengths"))
        } else {
            Column::Int64(NumericColumn::from_data(vec![total]))
        };
        let batch = Batch::new([result_col]).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        Some(1)
    }
}

/// Direct-from-cache pure SUM operator (no filter).
///
/// Pipeline lowering wires this when the plan is
/// `Aggregate { group_by: [], aggregates: [Sum(Int32 col)] }`
/// over a `Scan` whose relation already has a live column-cache
/// entry. Runs the hand-NEON `sum_i32_widening` kernel once over
/// the full cached column — no batch slicing, no per-batch
/// allocations.
pub struct CachedSumI32Scan {
    columns: Arc<CachedColumns>,
    sum_col: usize,
    output_schema: Schema,
    done: bool,
}

impl std::fmt::Debug for CachedSumI32Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedSumI32Scan")
            .field("sum_col", &self.sum_col)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl CachedSumI32Scan {
    #[must_use]
    pub fn new(columns: Arc<CachedColumns>, sum_col: usize, output_name: String) -> Self {
        let output_schema = Schema::new([Field::required(output_name, DataType::Int64)])
            .expect("output schema is trivially well-formed");
        Self {
            columns,
            sum_col,
            output_schema,
            done: false,
        }
    }
}

impl Operator for CachedSumI32Scan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let col = match &self.columns.columns[self.sum_col] {
            Column::Int32(c) => c,
            _ => {
                return Err(ExecError::TypeMismatch(
                    "CachedSumI32Scan: sum column must be Int32".to_owned(),
                ));
            }
        };
        let n_rows = col.len();
        let total = sum_i32_widening(col);
        let result_col = if n_rows == 0 {
            let mut nulls = ultrasql_vec::Bitmap::new(1, false);
            nulls.set(0, false);
            Column::Int64(NumericColumn::with_nulls(vec![0_i64], nulls).expect("matching lengths"))
        } else {
            Column::Int64(NumericColumn::from_data(vec![total]))
        };
        let batch = Batch::new([result_col]).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        // Scalar aggregate: exactly one row in one batch.
        // The wire encoder uses this to size its output BytesMut
        // tight (~96 bytes for a single Int64 row) instead of the
        // 32 KiB default that pre-touches eight memory pages.
        Some(1)
    }
}

/// Direct-from-cache pure AVG operator (no filter).
///
/// Computes `Float64(sum_i32_widening(col)) / count(non_null)`
/// over the full cached column in a single kernel pass + a
/// scalar divide. Output schema is `[("avg", Float64)]`.
pub struct CachedAvgI32Scan {
    columns: Arc<CachedColumns>,
    sum_col: usize,
    output_schema: Schema,
    done: bool,
}

impl std::fmt::Debug for CachedAvgI32Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedAvgI32Scan")
            .field("sum_col", &self.sum_col)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl CachedAvgI32Scan {
    #[must_use]
    pub fn new(columns: Arc<CachedColumns>, sum_col: usize, output_name: String) -> Self {
        let output_schema = Schema::new([Field::required(output_name, DataType::Float64)])
            .expect("output schema is trivially well-formed");
        Self {
            columns,
            sum_col,
            output_schema,
            done: false,
        }
    }
}

impl Operator for CachedAvgI32Scan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let col = match &self.columns.columns[self.sum_col] {
            Column::Int32(c) => c,
            _ => {
                return Err(ExecError::TypeMismatch(
                    "CachedAvgI32Scan: sum column must be Int32".to_owned(),
                ));
            }
        };
        let n_rows = col.len();
        // Count non-null entries. Our cached columns currently
        // never carry a null bitmap (only non-nullable columns
        // are cached for now), so this is always `n_rows`.
        let non_null = col.nulls().map_or(n_rows, |bm| {
            let mut c = 0_usize;
            for i in 0..bm.len() {
                if bm.get(i) {
                    c += 1;
                }
            }
            c
        });
        let result_col = if non_null == 0 {
            let mut nulls = ultrasql_vec::Bitmap::new(1, false);
            nulls.set(0, false);
            Column::Float64(
                NumericColumn::with_nulls(vec![0.0_f64], nulls).expect("matching lengths"),
            )
        } else {
            let total = sum_i32_widening(col);
            // Cast through f64 once; matches PostgreSQL's
            // `AVG(int) → float8` widening rule under the bench's
            // schema (the binder declares the aggregate's result
            // type as Float64).
            let avg = (total as f64) / (non_null as f64);
            Column::Float64(NumericColumn::from_data(vec![avg]))
        };
        let batch = Batch::new([result_col]).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        // Scalar aggregate emits exactly one row; see the matching
        // override on [`CachedSumI32Scan::estimated_row_count`].
        Some(1)
    }
}

impl Operator for FilterSumI32Scan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        let mut total: i64 = 0;
        let mut saw_any = false;
        // Fast path: when predicate and sum target the same column
        // and the op is `Gt` (the `cross_compare_sql::filter_sum_1m_i64`
        // bench shape), use the hand-NEON `filter_sum_i32_widening_gt`
        // kernel that fuses cmp + and-mask + widen + accumulate into
        // one SIMD loop. Skips the intermediate `Bitmap`
        // materialisation entirely.
        let fused_self =
            self.predicate_col == self.sum_col && matches!(self.predicate_op, CmpOp::Gt);
        while let Some(batch) = self.inner.next_batch()? {
            if batch.rows() == 0 {
                continue;
            }
            let cols = batch.columns();
            let (pred_col, sum_col) = match (&cols[self.predicate_col], &cols[self.sum_col]) {
                (Column::Int32(p), Column::Int32(s)) => (p, s),
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "FilterSumI32Scan: predicate and sum columns must both be Int32".to_owned(),
                    ));
                }
            };
            if fused_self {
                let delta = if self.jit.should_jit(sum_col.len()) {
                    filter_sum_i32_widening_gt_jit(sum_col.data(), self.predicate_threshold)
                        .unwrap_or_else(|| {
                            filter_sum_i32_widening_gt(sum_col.data(), self.predicate_threshold)
                        })
                } else {
                    filter_sum_i32_widening_gt(sum_col.data(), self.predicate_threshold)
                };
                total = total.wrapping_add(delta);
            } else {
                let mask = cmp_i32_scalar(pred_col, self.predicate_threshold, self.predicate_op);
                total = total.wrapping_add(sum_i32_widening_with_mask(sum_col, &mask));
            }
            saw_any |= true;
        }
        self.done = true;

        // PostgreSQL semantics for `SUM(INT) WHERE …`:
        // - Empty input ⇒ NULL.
        // - All-non-empty even with zero matching rows ⇒ 0
        //   (the binder/aggregate normally tracks `saw_non_null`
        //   per match; for the fused path we have only the
        //   `saw_any_batch` proxy because `sum_i32_widening_with_mask`
        //   contributes zero whether the mask is empty or not).
        // The `cross_compare_sql` workloads always preload a
        // non-empty relation so this branch primarily exists for
        // correctness on the empty-relation edge case.
        let result_col = if saw_any {
            Column::Int64(NumericColumn::from_data(vec![total]))
        } else {
            // Emit a single NULL row.
            let mut nulls = ultrasql_vec::Bitmap::new(1, false);
            nulls.set(0, false);
            Column::Int64(NumericColumn::with_nulls(vec![0_i64], nulls).expect("matching lengths"))
        };
        let batch = Batch::new([result_col]).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        // Filtered scalar aggregate emits exactly one row; see the
        // matching override on [`CachedSumI32Scan::estimated_row_count`].
        Some(1)
    }
}

impl Operator for FilterSumI64Scan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        let mut total: i64 = 0;
        let mut saw_any = false;
        let fused_self =
            self.predicate_col == self.sum_col && matches!(self.predicate_op, CmpOp::Gt);
        while let Some(batch) = self.inner.next_batch()? {
            if batch.rows() == 0 {
                continue;
            }
            let cols = batch.columns();
            let (pred_col, sum_col) = match (&cols[self.predicate_col], &cols[self.sum_col]) {
                (Column::Int64(p), Column::Int64(s)) => (p, s),
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "FilterSumI64Scan: predicate and sum columns must both be Int64".to_owned(),
                    ));
                }
            };
            if fused_self {
                let delta = if self.jit.should_jit(sum_col.len()) {
                    filter_sum_i64_gt_jit(sum_col.data(), self.predicate_threshold).unwrap_or_else(
                        || filter_sum_i64_gt(sum_col.data(), self.predicate_threshold),
                    )
                } else {
                    filter_sum_i64_gt(sum_col.data(), self.predicate_threshold)
                };
                total = total.wrapping_add(delta);
            } else {
                let mask = cmp_i64_scalar(pred_col, self.predicate_threshold, self.predicate_op);
                total = total.wrapping_add(sum_i64_with_mask(sum_col, &mask));
            }
            saw_any |= true;
        }
        self.done = true;

        let result_col = if saw_any {
            Column::Int64(NumericColumn::from_data(vec![total]))
        } else {
            let mut nulls = ultrasql_vec::Bitmap::new(1, false);
            nulls.set(0, false);
            Column::Int64(NumericColumn::with_nulls(vec![0_i64], nulls).expect("matching lengths"))
        };
        let batch = Batch::new([result_col]).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        Some(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{DataType, Field};
    use ultrasql_vec::Bitmap;

    use crate::mem_table_scan::MemTableScan;

    fn output_i64(batch: &Batch) -> i64 {
        let Column::Int64(col) = &batch.columns()[0] else {
            panic!("expected int64 output")
        };
        col.data()[0]
    }

    fn output_f64(batch: &Batch) -> f64 {
        let Column::Float64(col) = &batch.columns()[0] else {
            panic!("expected float64 output")
        };
        col.data()[0]
    }

    fn output_is_null(batch: &Batch) -> bool {
        match &batch.columns()[0] {
            Column::Int64(col) => col.nulls().is_some_and(|nulls| !nulls.get(0)),
            Column::Float64(col) => col.nulls().is_some_and(|nulls| !nulls.get(0)),
            other => panic!("unexpected output column {other:?}"),
        }
    }

    fn schema_i32_pair() -> Schema {
        Schema::new([
            Field::required("pred", DataType::Int32),
            Field::required("sum", DataType::Int32),
        ])
        .expect("schema")
    }

    fn schema_i64_pair() -> Schema {
        Schema::new([
            Field::required("pred", DataType::Int64),
            Field::required("sum", DataType::Int64),
        ])
        .expect("schema")
    }

    fn i32_pair_batch(rows: &[(i32, i32)]) -> Batch {
        Batch::new([
            Column::Int32(NumericColumn::from_data(
                rows.iter().map(|(pred, _)| *pred).collect(),
            )),
            Column::Int32(NumericColumn::from_data(
                rows.iter().map(|(_, sum)| *sum).collect(),
            )),
        ])
        .expect("batch")
    }

    fn i64_pair_batch(rows: &[(i64, i64)]) -> Batch {
        Batch::new([
            Column::Int64(NumericColumn::from_data(
                rows.iter().map(|(pred, _)| *pred).collect(),
            )),
            Column::Int64(NumericColumn::from_data(
                rows.iter().map(|(_, sum)| *sum).collect(),
            )),
        ])
        .expect("batch")
    }

    fn cached_i32(columns: Vec<Column>) -> Arc<CachedColumns> {
        Arc::new(CachedColumns::new(7, schema_i32_pair(), columns))
    }

    #[test]
    fn cached_filter_sum_uses_jit_when_enabled() {
        let schema = Schema::new([Field::required("x", DataType::Int32)]).expect("schema");
        let columns = CachedColumns::new(
            0,
            schema,
            vec![Column::Int32(NumericColumn::from_data(vec![
                -3, 0, 1, 2, 9, -11,
            ]))],
        );
        let mut op =
            CachedFilterSumI32Scan::new(Arc::new(columns), 0, 0, CmpOp::Gt, 0, "sum".to_owned())
                .with_jit(JitConfig {
                    enabled: true,
                    above_rows: 0,
                });
        let batch = op.next_batch().expect("ok").expect("row");
        assert_eq!(output_i64(&batch), 12);
        assert!(op.next_batch().expect("ok").is_none());
    }

    #[test]
    fn cached_filter_sum_i64_uses_jit_when_enabled() {
        let schema = Schema::new([Field::required("x", DataType::Int64)]).expect("schema");
        let columns = CachedColumns::new(
            0,
            schema,
            vec![Column::Int64(NumericColumn::from_data(vec![
                -30_i64, 0, 1, 2, 90, -110,
            ]))],
        );
        let mut op =
            CachedFilterSumI64Scan::new(Arc::new(columns), 0, 0, CmpOp::Gt, 0, "sum".to_owned())
                .with_jit(JitConfig {
                    enabled: true,
                    above_rows: 0,
                });
        let batch = op.next_batch().expect("ok").expect("row");
        assert_eq!(output_i64(&batch), 93);
        assert!(op.next_batch().expect("ok").is_none());
    }

    #[test]
    fn filter_sum_i32_streams_cross_column_predicate() {
        let scan = MemTableScan::new(
            schema_i32_pair(),
            vec![
                i32_pair_batch(&[(1, 10), (5, 50)]),
                i32_pair_batch(&[(2, 20), (9, 90)]),
            ],
        );
        let mut op = FilterSumI32Scan::new(Box::new(scan), 0, 3, CmpOp::Lt, 1, "sum".to_owned());

        let batch = op.next_batch().expect("ok").expect("row");
        assert_eq!(output_i64(&batch), 30);
        assert_eq!(op.estimated_row_count(), Some(1));
        assert!(op.next_batch().expect("ok").is_none());
    }

    #[test]
    fn filter_sum_i32_empty_input_emits_null() {
        let scan = MemTableScan::new(schema_i32_pair(), vec![]);
        let mut op = FilterSumI32Scan::new(Box::new(scan), 0, 0, CmpOp::Gt, 1, "sum".to_owned());

        let batch = op.next_batch().expect("ok").expect("row");
        assert!(output_is_null(&batch));
        assert!(op.next_batch().expect("ok").is_none());
    }

    #[test]
    fn filter_sum_i64_streams_and_reports_type_errors() {
        let scan = MemTableScan::new(
            schema_i64_pair(),
            vec![i64_pair_batch(&[(10, 100), (20, 200), (30, 300)])],
        );
        let mut op = FilterSumI64Scan::new(Box::new(scan), 0, 20, CmpOp::Ge, 1, "sum".to_owned());
        let batch = op.next_batch().expect("ok").expect("row");
        assert_eq!(output_i64(&batch), 500);

        let bad_scan = MemTableScan::new(schema_i32_pair(), vec![i32_pair_batch(&[(1, 2)])]);
        let mut bad =
            FilterSumI64Scan::new(Box::new(bad_scan), 0, 0, CmpOp::Gt, 1, "sum".to_owned());
        let err = bad.next_batch().expect_err("type mismatch");
        assert!(err.to_string().contains("must both be Int64"));
    }

    #[test]
    fn cached_filter_sum_i32_cross_column_and_type_error() {
        let columns = cached_i32(vec![
            Column::Int32(NumericColumn::from_data(vec![1, 2, 3])),
            Column::Int32(NumericColumn::from_data(vec![10, 20, 30])),
        ]);
        let mut op = CachedFilterSumI32Scan::new(columns, 0, 2, CmpOp::Ne, 1, "sum".to_owned());
        let batch = op.next_batch().expect("ok").expect("row");
        assert_eq!(output_i64(&batch), 40);
        assert_eq!(op.estimated_row_count(), Some(1));

        let bad_columns = Arc::new(CachedColumns::new(
            0,
            Schema::new([
                Field::required("pred", DataType::Int32),
                Field::required("sum", DataType::Int64),
            ])
            .expect("schema"),
            vec![
                Column::Int32(NumericColumn::from_data(vec![1])),
                Column::Int64(NumericColumn::from_data(vec![2])),
            ],
        ));
        let mut bad =
            CachedFilterSumI32Scan::new(bad_columns, 0, 0, CmpOp::Gt, 1, "sum".to_owned());
        let err = bad.next_batch().expect_err("type mismatch");
        assert!(err.to_string().contains("must both be Int32"));
    }

    #[test]
    fn cached_filter_sum_i64_empty_input_emits_null() {
        let columns = Arc::new(CachedColumns::new(
            0,
            schema_i64_pair(),
            vec![
                Column::Int64(NumericColumn::from_data(Vec::new())),
                Column::Int64(NumericColumn::from_data(Vec::new())),
            ],
        ));
        let mut op = CachedFilterSumI64Scan::new(columns, 0, 0, CmpOp::Gt, 1, "sum".to_owned());
        let batch = op.next_batch().expect("ok").expect("row");
        assert!(output_is_null(&batch));
    }

    #[test]
    fn cached_sum_and_avg_cover_non_empty_empty_and_null_bitmaps() {
        let columns = cached_i32(vec![
            Column::Int32(NumericColumn::from_data(vec![1, 2, 3])),
            Column::Int32(NumericColumn::from_data(vec![10, 20, 30])),
        ]);
        let mut sum = CachedSumI32Scan::new(Arc::clone(&columns), 1, "sum".to_owned());
        assert_eq!(output_i64(&sum.next_batch().expect("ok").expect("row")), 60);
        assert_eq!(sum.estimated_row_count(), Some(1));

        let mut avg = CachedAvgI32Scan::new(Arc::clone(&columns), 1, "avg".to_owned());
        assert_eq!(
            output_f64(&avg.next_batch().expect("ok").expect("row")),
            20.0
        );

        let empty = cached_i32(vec![
            Column::Int32(NumericColumn::from_data(Vec::new())),
            Column::Int32(NumericColumn::from_data(Vec::new())),
        ]);
        let mut empty_sum = CachedSumI32Scan::new(Arc::clone(&empty), 1, "sum".to_owned());
        assert!(output_is_null(
            &empty_sum.next_batch().expect("ok").expect("row")
        ));
        let mut empty_avg = CachedAvgI32Scan::new(empty, 1, "avg".to_owned());
        assert!(output_is_null(
            &empty_avg.next_batch().expect("ok").expect("row")
        ));

        let mut valid = Bitmap::new(3, true);
        valid.set(1, false);
        let nullable = cached_i32(vec![
            Column::Int32(NumericColumn::from_data(vec![1, 2, 3])),
            Column::Int32(
                NumericColumn::with_nulls(vec![10, 20, 30], valid).expect("matching lengths"),
            ),
        ]);
        let mut nullable_avg = CachedAvgI32Scan::new(nullable, 1, "avg".to_owned());
        assert_eq!(
            output_f64(&nullable_avg.next_batch().expect("ok").expect("row")),
            20.0
        );
    }
}
