//! General predicate filter operator.
//!
//! [`Filter`] is the production predicate operator backed by the full
//! [`Eval`] expression interpreter and vectorised comparison fast paths.
//!
//! # Fast-path: simple comparisons
//!
//! When the predicate matches the shape `column <cmp> literal` (or the
//! mirrored `literal <cmp> column` with the operator flipped), or
//! `left_column <cmp> right_column`, the filter dispatches to
//! vectorised kernels from `ultrasql-vec` that produce a `Bitmap` mask
//! in one pass over the input columns, then uses the mask to
//! materialise the surviving rows for every column of the input batch.
//! The path avoids per-row `Value`-decoding entirely and is
//! dramatically faster than the scalar path on i32/i64 columns.
//!
//! # Row-at-a-time evaluation (fallback)
//!
//! For predicates that do not match the fast path (multi-column
//! expressions, arithmetic, three-way conjunctions, string operators,
//! etc.), `Filter` falls back to the [`Eval`] interpreter: it decodes
//! each batch into rows, applies the predicate per row, and rebuilds a
//! new batch from the surviving rows. The fallback is correct by
//! construction and sufficient for OLTP-sized batches; the OLAP-shape
//! workloads land on the fast path.

mod decode;
mod fast_path;
mod select;

#[cfg(test)]
mod tests;

use num_traits::ToPrimitive;
use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::Column;
use ultrasql_vec::kernels::{CmpOp, cmp_i32_scalar, cmp_i64_scalar};

use crate::eval::Eval;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator, eval_error_to_exec_error};

use self::fast_path::{
    MaskCombine, cmp_columns_to_mask, combine_masks, const_mask_i32,
    estimate_predicate_selectivity, match_fast_predicate,
};
use self::select::build_empty_batch;

#[allow(unreachable_pub)]
pub use self::decode::batch_to_rows;
pub(crate) use self::select::select_column;

/// General predicate filter operator.
///
/// Pulls batches from `child`, evaluates `predicate` against each row,
/// and emits only rows where the predicate returns `Value::Bool(true)`.
/// NULL and `false` results are both discarded (SQL 3VL: only `true`
/// passes the filter).
///
/// The output schema is identical to the child's schema.
#[derive(Debug)]
pub struct Filter {
    child: Box<dyn Operator>,
    /// Compiled scalar interpreter used by the row-at-a-time fallback.
    predicate: Eval,
    /// `Some(_)` if the predicate matches a vectorised comparison
    /// shape; `None` otherwise. Cached at construction so we pay the
    /// shape-matching cost once.
    fast: Option<FastPredicate>,
    /// Heuristic selectivity hint derived from the predicate shape.
    selectivity_hint: f64,
    schema: Schema,
}

/// Cached, parsed comparison predicate.
#[derive(Debug, Clone)]
enum FastPredicate {
    /// `left AND right`.
    And(Box<FastPredicate>, Box<FastPredicate>),
    /// `left OR right`.
    Or(Box<FastPredicate>, Box<FastPredicate>),
    /// `column <op> literal`, with mirrored `literal <op> column`
    /// canonicalised by flipping `op`.
    ColumnLiteral {
        index: usize,
        op: CmpOp,
        literal: Value,
    },
    /// `left_column <op> right_column`.
    ColumnColumn {
        left_index: usize,
        right_index: usize,
        op: CmpOp,
    },
}

impl Filter {
    /// Construct a filter.
    ///
    /// The predicate is compiled into an [`Eval`] instance; the schema
    /// is cloned from `child` at construction time and remains fixed.
    /// If the predicate matches the column-op-literal shape, a cached
    /// fast-path descriptor is computed once here and reused for every
    /// batch.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, predicate: ScalarExpr) -> Self {
        let schema = child.schema().clone();
        let fast = match_fast_predicate(&predicate);
        let selectivity_hint = estimate_predicate_selectivity(&predicate);
        Self {
            child,
            predicate: Eval::new(predicate),
            fast,
            selectivity_hint,
            schema,
        }
    }

    /// Try the vectorised fast path. Returns `Ok(Some(batch))` when the
    /// fast path handled the input, `Ok(None)` when the predicate shape
    /// or type combination is not covered by the SIMD kernels and the
    /// caller must fall back to the scalar interpreter.
    /// Returns:
    /// - `Ok(Some(out))` when the fast path handled the input. `out`
    ///   is either a freshly-materialised selection or the input
    ///   itself when every row passed (see all-pass shortcut below).
    /// - `Ok(None)` when the predicate shape or type combination is
    ///   outside the SIMD kernels and the caller must fall back to
    ///   the scalar interpreter.
    fn try_fast_path(&self, input: Batch) -> Result<TryFastPath, ExecError> {
        let Some(fp) = self.fast.as_ref() else {
            return Ok(TryFastPath::Unhandled(input));
        };
        let cols = input.columns();
        let Some(mask) = self.mask_for_fast_predicate(fp, cols) else {
            return Ok(TryFastPath::Unhandled(input));
        };

        let selected = mask.count_ones();

        // All-pass shortcut. When every row in the input batch
        // satisfies the predicate, materialising a fresh
        // `Vec<Column>` via `select_column` is pure copy overhead —
        // the input batch already represents the desired output. We
        // hand the batch through unchanged.
        //
        // This is safe because `mask` is the AND of the predicate
        // result with the key column's validity bitmap. A 1-bit in
        // every row position therefore implies (a) the predicate
        // accepted every key, and (b) no key value was NULL, so the
        // input contains no row that the slow path would have
        // filtered out.
        //
        // Hot on the cross_compare_sql `UPDATE … WHERE id < n_rows`
        // shape, which is a predicate that matches every preloaded
        // row.
        if selected == input.rows() {
            return Ok(TryFastPath::Handled(input));
        }

        let mut out_cols: Vec<Column> = Vec::with_capacity(cols.len());
        for col in cols {
            out_cols.push(select_column(col, &mask, selected)?);
        }
        Ok(TryFastPath::Handled(Batch::new(out_cols)?))
    }

    fn mask_for_fast_predicate(&self, fp: &FastPredicate, cols: &[Column]) -> Option<Bitmap> {
        match fp {
            FastPredicate::And(left, right) => {
                let left = self.mask_for_fast_predicate(left, cols)?;
                let right = self.mask_for_fast_predicate(right, cols)?;
                Some(combine_masks(&left, &right, MaskCombine::And))
            }
            FastPredicate::Or(left, right) => {
                let left = self.mask_for_fast_predicate(left, cols)?;
                let right = self.mask_for_fast_predicate(right, cols)?;
                Some(combine_masks(&left, &right, MaskCombine::Or))
            }
            FastPredicate::ColumnLiteral { index, op, literal } => {
                let key_col = cols.get(*index)?;
                match (key_col, literal) {
                    (Column::Int32(c), Value::Int32(v)) => Some(cmp_i32_scalar(c, *v, *op)),
                    (Column::Int32(c), Value::Date(v)) => Some(cmp_i32_scalar(c, *v, *op)),
                    // For an Int32 column compared against an Int64 literal,
                    // narrow the literal where it fits. When it overflows the
                    // i32 range every row gives the same answer, so build a
                    // constant mask (NULL rows still get a 0 bit).
                    (Column::Int32(c), Value::Int64(v)) => Some(i32::try_from(*v).map_or_else(
                        |_| const_mask_i32(c, *v, *op),
                        |narrow| cmp_i32_scalar(c, narrow, *op),
                    )),
                    (Column::Int64(c), Value::Int64(v)) => Some(cmp_i64_scalar(c, *v, *op)),
                    (Column::Int64(c), Value::Money(v)) => Some(cmp_i64_scalar(c, *v, *op)),
                    (Column::Int64(c), Value::Oid(v))
                    | (Column::Int64(c), Value::RegClass(v))
                    | (Column::Int64(c), Value::RegType(v)) => {
                        Some(cmp_i64_scalar(c, i64::from(v.raw()), *op))
                    }
                    (Column::Int64(c), Value::Time(v))
                    | (Column::Int64(c), Value::Timestamp(v))
                    | (Column::Int64(c), Value::TimestampTz(v)) => Some(cmp_i64_scalar(c, *v, *op)),
                    (Column::Int64(c), Value::Int32(v)) => {
                        Some(cmp_i64_scalar(c, i64::from(*v), *op))
                    }
                    // Type combinations outside the i32/i64 happy path fall back
                    // to the scalar interpreter — correctness over coverage.
                    _ => None,
                }
            }
            FastPredicate::ColumnColumn {
                left_index,
                right_index,
                op,
            } => {
                let left_col = cols.get(*left_index)?;
                let right_col = cols.get(*right_index)?;
                let left_type = &self.schema.field_at(*left_index).data_type;
                let right_type = &self.schema.field_at(*right_index).data_type;
                cmp_columns_to_mask(left_col, right_col, left_type, right_type, *op)
            }
        }
    }
}

/// Result of [`Filter::try_fast_path`]. Carries the input batch
/// through when the fast path did not apply so the caller can hand
/// it to the slow path without re-fetching it from the child.
enum TryFastPath {
    Handled(Batch),
    Unhandled(Batch),
}

impl Operator for Filter {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let Some(input) = self.child.next_batch()? else {
            return Ok(None);
        };

        // Fast path: column-op-literal over Int32/Int64.
        let input = match self.try_fast_path(input)? {
            TryFastPath::Handled(out) => return Ok(Some(out)),
            TryFastPath::Unhandled(b) => b,
        };

        // Decode the batch into rows, apply the predicate, collect survivors.
        let rows = batch_to_rows(&input, &self.schema)?;
        let mut survivors: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for row in &rows {
            let result = self.predicate.eval(row).map_err(eval_error_to_exec_error)?;
            match result {
                Value::Bool(true) => survivors.push(row.clone()),
                Value::Bool(false) | Value::Null => {
                    // false and NULL are both non-passing in SQL 3VL.
                }
                other => {
                    return Err(ExecError::TypeMismatch(format!(
                        "filter predicate must evaluate to Bool or Null, got {:?}",
                        other.data_type()
                    )));
                }
            }
        }

        if survivors.is_empty() {
            // Return a properly-shaped empty batch (correct column count but 0
            // rows). `build_batch` with an empty slice produces a 0-column
            // batch, which would violate the operator's schema contract.
            let empty = build_empty_batch(&self.schema)?;
            return Ok(Some(empty));
        }
        build_batch(&survivors, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        let child_rows = self.child.estimated_row_count()?;
        if child_rows == 0 {
            return Some(0);
        }
        let child_rows_f64 = child_rows.to_f64().unwrap_or(f64::MAX);
        let estimated = (child_rows_f64 * self.selectivity_hint)
            .ceil()
            .clamp(1.0, child_rows_f64);
        Some(estimated.to_usize().unwrap_or(child_rows))
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }
}
