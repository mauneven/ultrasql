//! General predicate filter operator.
//!
//! [`Filter`] is the production-quality filter operator backed by the
//! full [`Eval`] expression interpreter. It replaces the placeholder
//! [`FilterEqI32`](crate::FilterEqI32) for all predicate shapes except
//! those where the specialised SIMD path is wired in.
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

use num_traits::ToPrimitive;
use ultrasql_core::{DataType, GeometryValue, RangeValue, Schema, Value};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};
use ultrasql_vec::kernels::{CmpOp, cmp_i32_scalar, cmp_i64_scalar, compare_i32, compare_i64};
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

use crate::eval::Eval;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

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
            out_cols.push(select_column(col, &mask, selected));
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
            let result = self
                .predicate
                .eval(row)
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
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
        let estimated = (child_rows as f64 * self.selectivity_hint)
            .ceil()
            .clamp(1.0, child_rows as f64);
        Some(estimated.to_usize().unwrap_or(child_rows))
    }
}

fn estimate_predicate_selectivity(expr: &ScalarExpr) -> f64 {
    let selectivity = match expr {
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => estimate_predicate_selectivity(left) * estimate_predicate_selectivity(right),
        ScalarExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            let left_sel = estimate_predicate_selectivity(left);
            let right_sel = estimate_predicate_selectivity(right);
            left_sel + right_sel - (left_sel * right_sel)
        }
        ScalarExpr::Binary { op, .. } => match op {
            BinaryOp::Eq => 0.1,
            BinaryOp::NotEq => 0.9,
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => 0.33,
            _ => 0.5,
        },
        ScalarExpr::IsNull { negated, .. } => {
            if *negated {
                0.95
            } else {
                0.05
            }
        }
        ScalarExpr::Unary { .. } | ScalarExpr::FunctionCall { .. } => 0.5,
        ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::Column { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => 0.5,
    };
    selectivity.clamp(0.0, 1.0)
}

/// Match a simple comparison shape and produce a cached descriptor for
/// the vectorised path.
///
/// Returns `None` for any other predicate shape, including:
/// - Nested expressions (`col + 1 > 5`).
/// - Logical conjunctions (`a > 5 AND b < 10`).
/// - NULL literals — `WHERE col = NULL` always evaluates to NULL/false
///   in SQL but the existing scalar path already handles that.
fn match_fast_predicate(expr: &ScalarExpr) -> Option<FastPredicate> {
    if let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = expr
    {
        return Some(FastPredicate::And(
            Box::new(match_fast_predicate(left)?),
            Box::new(match_fast_predicate(right)?),
        ));
    }
    if let ScalarExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = expr
    {
        return Some(FastPredicate::Or(
            Box::new(match_fast_predicate(left)?),
            Box::new(match_fast_predicate(right)?),
        ));
    }
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    let cmp = binary_op_to_cmp(*op)?;
    // Case 1: `column <op> literal`
    if let (ScalarExpr::Column { index, .. }, ScalarExpr::Literal { value, .. }) =
        (left.as_ref(), right.as_ref())
    {
        if matches!(value, Value::Null) {
            return None;
        }
        return Some(FastPredicate::ColumnLiteral {
            index: *index,
            op: cmp,
            literal: value.clone(),
        });
    }
    // Case 2: `literal <op> column` — flip the operator so the kernel
    // always sees the column on the left.
    if let (ScalarExpr::Literal { value, .. }, ScalarExpr::Column { index, .. }) =
        (left.as_ref(), right.as_ref())
    {
        if matches!(value, Value::Null) {
            return None;
        }
        return Some(FastPredicate::ColumnLiteral {
            index: *index,
            op: flip_cmp(cmp),
            literal: value.clone(),
        });
    }
    // Case 3: `left_column <op> right_column`.
    if let (
        ScalarExpr::Column {
            index: left_index, ..
        },
        ScalarExpr::Column {
            index: right_index, ..
        },
    ) = (left.as_ref(), right.as_ref())
    {
        return Some(FastPredicate::ColumnColumn {
            left_index: *left_index,
            right_index: *right_index,
            op: cmp,
        });
    }
    None
}

#[derive(Clone, Copy)]
enum MaskCombine {
    And,
    Or,
}

fn combine_masks(left: &Bitmap, right: &Bitmap, combine: MaskCombine) -> Bitmap {
    debug_assert_eq!(left.len(), right.len(), "mask lengths must align");
    let words = left
        .words()
        .iter()
        .zip(right.words().iter())
        .map(|(left_word, right_word)| match combine {
            MaskCombine::And => left_word & right_word,
            MaskCombine::Or => left_word | right_word,
        })
        .collect();
    Bitmap::from_words(words, left.len())
}

const fn binary_op_to_cmp(op: BinaryOp) -> Option<CmpOp> {
    match op {
        BinaryOp::Eq => Some(CmpOp::Eq),
        BinaryOp::NotEq => Some(CmpOp::Ne),
        BinaryOp::Lt => Some(CmpOp::Lt),
        BinaryOp::LtEq => Some(CmpOp::Le),
        BinaryOp::Gt => Some(CmpOp::Gt),
        BinaryOp::GtEq => Some(CmpOp::Ge),
        _ => None,
    }
}

/// Flip an ordering operator so that `lit <op> col` becomes the
/// equivalent `col <flipped_op> lit`. `Eq`/`Ne` are symmetric.
const fn flip_cmp(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Eq => CmpOp::Eq,
        CmpOp::Ne => CmpOp::Ne,
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
    }
}

/// Build a constant-valued mask for an `i32` column when the comparison
/// literal lies outside the `i32` range — every row gives the same
/// answer. NULL rows still get a 0 bit.
fn const_mask_i32(column: &NumericColumn<i32>, literal_i64: i64, op: CmpOp) -> Bitmap {
    let high = literal_i64 > i64::from(i32::MAX);
    let constant_result = match op {
        CmpOp::Eq => false,
        CmpOp::Ne => true,
        // i32 values are all < literal when literal > MAX, and
        // all > literal when literal < MIN.
        CmpOp::Lt | CmpOp::Le => high,
        CmpOp::Gt | CmpOp::Ge => !high,
    };
    let n = column.len();
    if !constant_result {
        return Bitmap::new(n, false);
    }
    let mut bm = Bitmap::new(n, true);
    if let Some(nulls) = column.nulls() {
        let words = bm.words_mut();
        for (w, &v) in words.iter_mut().zip(nulls.words().iter()) {
            *w &= v;
        }
    }
    bm
}

/// Vectorised `left_column <op> right_column` comparison for the
/// physical integer families used by dates, timestamps, decimals, and
/// regular integer columns.
fn cmp_columns_to_mask(
    left: &Column,
    right: &Column,
    left_type: &DataType,
    right_type: &DataType,
    op: CmpOp,
) -> Option<Bitmap> {
    if !raw_ordering_matches_logical_ordering(left_type, right_type) {
        return None;
    }
    match (left, right) {
        (Column::Int32(l), Column::Int32(r)) => {
            let validity = merge_numeric_validity(l, r);
            let cmp = compare_i32(l, r, validity.as_ref());
            Some(cmp_i32_scalar(&cmp, 0, op))
        }
        (Column::Int64(l), Column::Int64(r)) => {
            let validity = merge_numeric_validity(l, r);
            let cmp = compare_i64(l, r, validity.as_ref());
            Some(cmp_i64_scalar(&cmp, 0, op))
        }
        _ => None,
    }
}

fn raw_ordering_matches_logical_ordering(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::Int16, DataType::Int16)
        | (DataType::Int32, DataType::Int32)
        | (DataType::Int64, DataType::Int64)
        | (DataType::Date, DataType::Date)
        | (DataType::Time, DataType::Time)
        | (DataType::Timestamp, DataType::Timestamp)
        | (DataType::TimestampTz, DataType::TimestampTz)
        | (DataType::Timestamp, DataType::TimestampTz)
        | (DataType::TimestampTz, DataType::Timestamp) => true,
        (
            DataType::Decimal {
                scale: left_scale, ..
            },
            DataType::Decimal {
                scale: right_scale, ..
            },
        ) => left_scale.unwrap_or(0) == right_scale.unwrap_or(0),
        _ => false,
    }
}

/// Merge two numeric validity masks. `None` means all rows valid.
fn merge_numeric_validity<T>(left: &NumericColumn<T>, right: &NumericColumn<T>) -> Option<Bitmap> {
    match (left.nulls(), right.nulls()) {
        (None, None) => None,
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (Some(l), Some(r)) => {
            let mut merged = l.clone();
            for (word, &right_word) in merged.words_mut().iter_mut().zip(r.words().iter()) {
                *word &= right_word;
            }
            Some(merged)
        }
    }
}

/// Materialise the rows of `column` selected by `mask`. The output
/// length equals `selected` (the popcount of the mask, passed in to
/// avoid re-counting once per column).
///
/// Every per-type branch allocates a fresh non-nullable column. NULL
/// inputs are dropped because the mask already excluded them (the
/// comparison kernels AND the validity bitmap into the data-compare
/// result — see `cmp_i32_scalar` / `cmp_i64_scalar`).
fn select_column(column: &Column, mask: &Bitmap, selected: usize) -> Column {
    match column {
        Column::Int32(c) => {
            let data = c.data();
            let mut out = Vec::with_capacity(selected);
            for i in mask.iter_ones() {
                out.push(data[i]);
            }
            Column::Int32(NumericColumn::from_data(out))
        }
        Column::Int64(c) => {
            let data = c.data();
            let mut out = Vec::with_capacity(selected);
            for i in mask.iter_ones() {
                out.push(data[i]);
            }
            Column::Int64(NumericColumn::from_data(out))
        }
        Column::Float32(c) => {
            let data = c.data();
            let mut out = Vec::with_capacity(selected);
            for i in mask.iter_ones() {
                out.push(data[i]);
            }
            Column::Float32(NumericColumn::from_data(out))
        }
        Column::Float64(c) => {
            let data = c.data();
            let mut out = Vec::with_capacity(selected);
            for i in mask.iter_ones() {
                out.push(data[i]);
            }
            Column::Float64(NumericColumn::from_data(out))
        }
        Column::Bool(c) => {
            let mut out = Vec::with_capacity(selected);
            for i in mask.iter_ones() {
                out.push(c.value(i));
            }
            Column::Bool(BoolColumn::from_data(out))
        }
        Column::Utf8(_) | Column::DictionaryUtf8(_) => {
            let mut out: Vec<Option<String>> = Vec::with_capacity(selected);
            for i in mask.iter_ones() {
                out.push(column.text_value(i).map(str::to_owned));
            }
            match encode_strings_auto(
                out.iter().map(|v| v.as_deref()),
                DictionaryEncodingPolicy::default(),
            ) {
                StringEncoding::Raw(c) => Column::Utf8(c),
                StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
            }
        }
    }
}

/// Build an empty batch whose column types match `schema`.
///
/// The returned batch has 0 rows but the correct number of columns, each
/// with an empty data vec. This is required when the filter passes no rows
/// from a non-empty input batch — the caller must not mistake 0 rows for
/// EOF.
fn build_empty_batch(schema: &Schema) -> Result<Batch, ExecError> {
    let cols: Vec<Column> = schema
        .fields()
        .iter()
        .map(|f| match &f.data_type {
            DataType::Bool => Column::Bool(BoolColumn::from_data(vec![])),
            DataType::Int16 | DataType::Int32 | DataType::Date => {
                Column::Int32(NumericColumn::from_data(vec![]))
            }
            DataType::Int64 => Column::Int64(NumericColumn::from_data(vec![])),
            DataType::Decimal { .. }
            | DataType::Time
            | DataType::Timestamp
            | DataType::TimestampTz => Column::Int64(NumericColumn::from_data(vec![])),
            DataType::Float32 => Column::Float32(NumericColumn::from_data(vec![])),
            DataType::Float64 => Column::Float64(NumericColumn::from_data(vec![])),
            DataType::Text { .. }
            | DataType::Jsonb
            | DataType::Range(_)
            | DataType::Geometry(_)
            | DataType::Array(_) => Column::Utf8(StringColumn::from_data(vec![])),
            // For Int32 and any other type, fall back to an Int32 column.
            // In practice the binder only produces the above types at v0.5.
            _ => Column::Int32(NumericColumn::from_data(vec![])),
        })
        .collect();
    Batch::new(cols).map_err(ExecError::from)
}

/// Decode a [`Batch`] into a `Vec` of rows (each row is a `Vec<Value>`).
///
/// This is the inverse of [`build_batch`]: it reconstructs the row-at-a-time
/// representation from the columnar batch. Each column is decoded into the
/// corresponding `Value` variant; NULL cells use `Value::Null` (the
/// `BoolColumn` and numeric columns use a sentinel zero for NULL which is
/// re-encoded here as `Value::Null` only when the schema field is nullable
/// and the value equals the sentinel — for v0.5 simplicity we keep the
/// sentinel as-is since nullability is represented in the batch validity
/// bitmaps in future work; for now the filter treats the sentinel as a
/// non-null value).
///
/// For v0.5 this is a pure column-to-value decode without bitmap support.
#[allow(unreachable_pub)]
pub fn batch_to_rows(batch: &Batch, schema: &Schema) -> Result<Vec<Vec<Value>>, ExecError> {
    let n_rows = batch.rows();
    let n_cols = schema.len();
    let cols = batch.columns();

    if cols.len() != n_cols {
        return Err(ExecError::TypeMismatch(format!(
            "batch has {} columns but schema has {}",
            cols.len(),
            n_cols,
        )));
    }

    let mut rows: Vec<Vec<Value>> = (0..n_rows).map(|_| Vec::with_capacity(n_cols)).collect();

    for (col_idx, (col, field)) in cols.iter().zip(schema.fields().iter()).enumerate() {
        // Validity convention: 1 = valid, 0 = null. `is_null(i)` returns
        // `true` when the bitmap exists and the bit is unset.
        let is_null = |nulls: Option<&ultrasql_vec::bitmap::Bitmap>, i: usize| -> bool {
            nulls.is_some_and(|b| !b.get(i))
        };
        match (col, &field.data_type) {
            (Column::Int32(c), DataType::Int16) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        let v = i16::try_from(c.data()[row_idx]).map_err(|_| {
                            ExecError::TypeMismatch(format!(
                                "column {col_idx} ({name}): Int16 value out of range",
                                name = field.name,
                            ))
                        })?;
                        row.push(Value::Int16(v));
                    }
                }
            }
            (Column::Int32(c), DataType::Int32) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Int32(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::Int64) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Int64(c.data()[row_idx]));
                    }
                }
            }
            (Column::Float32(c), DataType::Float32) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Float32(c.data()[row_idx]));
                    }
                }
            }
            (Column::Float64(c), DataType::Float64) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Float64(c.data()[row_idx]));
                    }
                }
            }
            (Column::Bool(c), DataType::Bool) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Bool(c.value(row_idx)));
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Text { .. }) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Text(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Jsonb) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Jsonb(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Range(range_type)) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let range = RangeValue::parse(*range_type, v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid {expected_type} literal",
                                    name = field.name,
                                    expected_type = field.data_type,
                                ))
                            })?;
                            row.push(Value::Range(range));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Array(element_type)) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let array =
                                Value::parse_array((**element_type).clone(), v).ok_or_else(
                                    || {
                                        ExecError::TypeMismatch(format!(
                                            "column {col_idx} ({name}): invalid {expected_type} literal",
                                            name = field.name,
                                            expected_type = field.data_type,
                                        ))
                                    },
                                )?;
                            row.push(array);
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Geometry(geometry_type)) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let geometry =
                                GeometryValue::parse(*geometry_type, v).ok_or_else(|| {
                                    ExecError::TypeMismatch(format!(
                                        "column {col_idx} ({name}): invalid {expected_type} literal",
                                        name = field.name,
                                        expected_type = field.data_type,
                                    ))
                                })?;
                            row.push(Value::Geometry(geometry));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Uuid) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let uuid = Value::parse_uuid(v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid uuid literal",
                                    name = field.name,
                                ))
                            })?;
                            row.push(Value::Uuid(uuid));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Bytea) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let bytes = Value::parse_bytea(v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid bytea literal",
                                    name = field.name,
                                ))
                            })?;
                            row.push(Value::Bytea(bytes));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Int32(c), DataType::Date) => {
                // Date columns store as `Int32` (days since
                // 2000-01-01). The row materialiser re-tags the value
                // as `Value::Date` so downstream operators that
                // pattern-match on `Value` see the date semantics.
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Date(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::Decimal { scale, .. }) => {
                // Decimal columns store as `Int64` with a schema
                // scale tag. Re-tag the materialised value as
                // `Value::Decimal { value, scale }`.
                let s = scale.unwrap_or(0);
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Decimal {
                            value: c.data()[row_idx],
                            scale: s,
                        });
                    }
                }
            }
            (Column::Int64(c), DataType::Timestamp) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Timestamp(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::TimestampTz) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::TimestampTz(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::Time) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Time(c.data()[row_idx]));
                    }
                }
            }
            (col_var, expected_type) => {
                return Err(ExecError::TypeMismatch(format!(
                    "column {col_idx} ({name}): batch column type {:?} does not match schema type {expected_type}",
                    col_var.data_type(),
                    name = field.name,
                )));
            }
        }
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::Filter;
    use crate::Operator;
    use crate::mem_table_scan::MemTableScan;

    #[derive(Debug)]
    struct HintOnlyOp {
        schema: Schema,
        hint: Option<usize>,
    }

    impl Operator for HintOnlyOp {
        fn next_batch(&mut self) -> Result<Option<Batch>, crate::ExecError> {
            Ok(None)
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn estimated_row_count(&self) -> Option<usize> {
            self.hint
        }
    }

    fn schema_id_val() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema is well-formed")
    }

    fn pair_batch(rows: &[(i32, i64)]) -> Batch {
        let ids: Vec<i32> = rows.iter().map(|(a, _)| *a).collect();
        let vals: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
        Batch::new([
            Column::Int32(NumericColumn::from_data(ids)),
            Column::Int64(NumericColumn::from_data(vals)),
        ])
        .expect("batch is well-formed")
    }

    /// Predicate: `id = 7` (Int32 column at index 0 equals literal 7).
    fn pred_id_eq_7() -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(7),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        }
    }

    fn drain_id_val(op: &mut dyn Operator) -> Vec<(i32, i64)> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("operator must not error") {
            let cols = b.columns();
            match (&cols[0], &cols[1]) {
                (Column::Int32(ids), Column::Int64(vals)) => {
                    for (i, v) in ids.data().iter().zip(vals.data().iter()) {
                        out.push((*i, *v));
                    }
                }
                other => panic!("unexpected column types: {other:?}"),
            }
        }
        out
    }

    #[test]
    fn filter_keeps_rows_where_predicate_true() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![pair_batch(&[(7, 10), (1, 20), (7, 30), (2, 40)])],
        );
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        let rows = drain_id_val(&mut filter);
        assert_eq!(rows, vec![(7, 10), (7, 30)]);
    }

    #[test]
    fn filter_drops_rows_where_predicate_false_or_null() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![pair_batch(&[(1, 10), (2, 20), (3, 30)])],
        );
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        let rows = drain_id_val(&mut filter);
        assert!(rows.is_empty(), "expected no rows, got {rows:?}");
    }

    #[test]
    fn filter_chains_with_mem_table_scan() {
        let schema = schema_id_val();
        let b1 = pair_batch(&[(7, 1), (2, 2), (7, 3)]);
        let b2 = pair_batch(&[(7, 4), (5, 5)]);
        let scan = MemTableScan::new(schema, vec![b1, b2]);
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        let rows = drain_id_val(&mut filter);
        assert_eq!(rows, vec![(7, 1), (7, 3), (7, 4)]);
    }

    #[test]
    fn filter_schema_matches_child_schema() {
        let scan = MemTableScan::new(schema_id_val(), vec![]);
        let filter = Filter::new(Box::new(scan), pred_id_eq_7());
        assert_eq!(filter.schema().len(), 2);
        assert_eq!(filter.schema().field_at(0).name, "id");
    }

    #[test]
    fn filter_scales_child_row_count_hint_by_selectivity() {
        let child = HintOnlyOp {
            schema: schema_id_val(),
            hint: Some(123),
        };
        let filter = Filter::new(Box::new(child), pred_id_eq_7());
        assert_eq!(filter.estimated_row_count(), Some(13));
    }

    #[test]
    fn filter_empty_input_returns_none() {
        let scan = MemTableScan::new(schema_id_val(), vec![]);
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        assert!(filter.next_batch().unwrap().is_none());
    }

    #[test]
    fn filter_emits_empty_batch_when_nothing_matches() {
        let scan = MemTableScan::new(schema_id_val(), vec![pair_batch(&[(1, 1), (2, 2)])]);
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        // The filter emits a batch (possibly empty) per child batch, not None.
        let batch = filter.next_batch().unwrap().unwrap();
        assert_eq!(batch.rows(), 0, "expected empty batch");
        assert!(filter.next_batch().unwrap().is_none());
    }

    // ---- vectorised fast-path tests ----

    use ultrasql_vec::bitmap::Bitmap;

    fn schema_x_i64() -> Schema {
        Schema::new([Field::required("x", DataType::Int64)]).expect("schema ok")
    }

    fn batch_i64(data: Vec<i64>) -> Batch {
        Batch::new([Column::Int64(NumericColumn::from_data(data))]).expect("batch ok")
    }

    /// 4096-row Int64 batch with `x > lit`: vectorised output must
    /// agree row-for-row with a naive scalar reference.
    #[test]
    fn vectorized_gt_i64_matches_scalar() {
        let n = 4096_usize;
        let threshold = 1_000_000_i64;
        let data: Vec<i64> = (0..n)
            .map(|i| i64::try_from(i).expect("test index fits in i64") * 1_000 - 500_000)
            .collect();

        let pred = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(ScalarExpr::Column {
                name: "x".into(),
                index: 0,
                data_type: DataType::Int64,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int64(threshold),
                data_type: DataType::Int64,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema_x_i64(), vec![batch_i64(data.clone())]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i64> = match &out.columns()[0] {
            Column::Int64(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        let want: Vec<i64> = data.iter().filter(|&&v| v > threshold).copied().collect();
        assert_eq!(got, want);
        assert!(filter.next_batch().unwrap().is_none());
    }

    /// Vectorised `col = lit` over an Int32 column whose validity
    /// bitmap marks some rows NULL. NULL rows must NOT appear in the
    /// output — SQL `WHERE` treats `UNKNOWN` as `false`, and the
    /// kernel honours that by AND-ing the validity bitmap into the
    /// data-compare mask.
    #[test]
    fn vectorized_eq_i32_with_nulls() {
        let len = 8_usize;
        let data: Vec<i32> = vec![42, 999, 42, 999, 42, 999, 42, 7];
        let mut validity = Bitmap::new(len, true);
        for &null_row in &[1_usize, 3, 5] {
            validity.set(null_row, false);
        }
        let column = NumericColumn::with_nulls(data, validity).expect("valid column");
        let batch = Batch::new([Column::Int32(column)]).expect("batch ok");

        let schema = Schema::new([Field::required("k", DataType::Int32)]).expect("schema ok");
        let pred = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: "k".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(42),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i32> = match &out.columns()[0] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        // Rows {0, 2, 4, 6}: value 42, non-null. Rows 1/3/5: value 999
        // and NULL (validity = 0) — must be dropped. Row 7: 7.
        assert_eq!(got, vec![42, 42, 42, 42]);
    }

    /// TPC-H Q21 style predicate: `l_receiptdate > l_commitdate`.
    /// Date columns are stored as Int32 day offsets, so this must use
    /// the vectorised column-vs-column path instead of row decoding.
    #[test]
    fn vectorized_column_column_i32_date_gt() {
        let commit_dates = vec![10_i32, 20, 30, 40, 50, 60];
        let receipt_dates = vec![11_i32, 19, 30, 99, 49, 61];
        let batch = Batch::new([
            Column::Int32(NumericColumn::from_data(commit_dates)),
            Column::Int32(NumericColumn::from_data(receipt_dates)),
        ])
        .expect("batch ok");
        let schema = Schema::new([
            Field::required("l_commitdate", DataType::Date),
            Field::required("l_receiptdate", DataType::Date),
        ])
        .expect("schema ok");
        let pred = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(ScalarExpr::Column {
                name: "l_receiptdate".into(),
                index: 1,
                data_type: DataType::Date,
            }),
            right: Box::new(ScalarExpr::Column {
                name: "l_commitdate".into(),
                index: 0,
                data_type: DataType::Date,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<(i32, i32)> = match (&out.columns()[0], &out.columns()[1]) {
            (Column::Int32(commit), Column::Int32(receipt)) => commit
                .data()
                .iter()
                .copied()
                .zip(receipt.data().iter().copied())
                .collect(),
            other => panic!("unexpected column types: {other:?}"),
        };
        assert_eq!(got, vec![(10, 11), (40, 99), (60, 61)]);
        assert!(filter.next_batch().unwrap().is_none());
    }

    /// Date literals use the same raw Int32 ordering as stored date
    /// columns, so `col >= DATE '...'` should stay on the vectorized
    /// column-vs-literal path.
    #[test]
    fn vectorized_date_literal_i32_matches_scalar() {
        let data = vec![10_i32, 20, 30, 40, 50, 60];
        let batch =
            Batch::new([Column::Int32(NumericColumn::from_data(data.clone()))]).expect("batch ok");
        let schema =
            Schema::new([Field::required("o_orderdate", DataType::Date)]).expect("schema ok");
        let threshold = 40_i32;
        let pred = ScalarExpr::Binary {
            op: BinaryOp::GtEq,
            left: Box::new(ScalarExpr::Column {
                name: "o_orderdate".into(),
                index: 0,
                data_type: DataType::Date,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Date(threshold),
                data_type: DataType::Date,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i32> = match &out.columns()[0] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        assert_eq!(got, vec![40, 50, 60]);
        assert!(filter.next_batch().unwrap().is_none());
    }

    /// Timestamp literals use the raw Int64 ordering of timestamp
    /// columns, so simple range predicates stay on the vectorized fast
    /// path.
    #[test]
    fn vectorized_timestamp_literal_i64_matches_scalar() {
        let data = vec![10_i64, 20, 30, 40, 50, 60];
        let batch =
            Batch::new([Column::Int64(NumericColumn::from_data(data.clone()))]).expect("batch ok");
        let schema = Schema::new([Field::required("ts", DataType::Timestamp)]).expect("schema ok");
        let threshold = 25_i64;
        let pred = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(ScalarExpr::Column {
                name: "ts".into(),
                index: 0,
                data_type: DataType::Timestamp,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Timestamp(threshold),
                data_type: DataType::Timestamp,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i64> = match &out.columns()[0] {
            Column::Int64(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        assert_eq!(got, vec![30, 40, 50, 60]);
        assert!(filter.next_batch().unwrap().is_none());
    }

    /// A conjunction of simple date comparisons should stay on the
    /// vectorized path instead of forcing a full scalar fallback.
    #[test]
    fn vectorized_and_of_date_range_predicates_matches_scalar() {
        let data = vec![10_i32, 20, 30, 40, 50, 60];
        let batch =
            Batch::new([Column::Int32(NumericColumn::from_data(data.clone()))]).expect("batch ok");
        let schema =
            Schema::new([Field::required("o_orderdate", DataType::Date)]).expect("schema ok");
        let lower = ScalarExpr::Binary {
            op: BinaryOp::GtEq,
            left: Box::new(ScalarExpr::Column {
                name: "o_orderdate".into(),
                index: 0,
                data_type: DataType::Date,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Date(20),
                data_type: DataType::Date,
            }),
            data_type: DataType::Bool,
        };
        let upper = ScalarExpr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(ScalarExpr::Column {
                name: "o_orderdate".into(),
                index: 0,
                data_type: DataType::Date,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Date(50),
                data_type: DataType::Date,
            }),
            data_type: DataType::Bool,
        };
        let pred = ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(lower),
            right: Box::new(upper),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i32> = match &out.columns()[0] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        assert_eq!(got, vec![20, 30, 40]);
        assert!(filter.next_batch().unwrap().is_none());
    }

    /// Column-vs-column comparison must apply SQL NULL semantics:
    /// NULL on either side means UNKNOWN and does not pass WHERE.
    #[test]
    fn vectorized_column_column_i32_merges_nulls() {
        let len = 5_usize;
        let left_values = vec![5_i32, 7, 9, 11, 13];
        let right_values = vec![1_i32, 2, 3, 20, 4];
        let mut left_validity = Bitmap::new(len, true);
        let mut right_validity = Bitmap::new(len, true);
        left_validity.set(1, false);
        right_validity.set(2, false);
        let left =
            NumericColumn::with_nulls(left_values, left_validity).expect("valid left column");
        let right =
            NumericColumn::with_nulls(right_values, right_validity).expect("valid right column");
        let batch = Batch::new([Column::Int32(left), Column::Int32(right)]).expect("batch ok");
        let schema = Schema::new([
            Field::required("left", DataType::Int32),
            Field::required("right", DataType::Int32),
        ])
        .expect("schema ok");
        let pred = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(ScalarExpr::Column {
                name: "left".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Column {
                name: "right".into(),
                index: 1,
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i32> = match &out.columns()[0] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        // Rows 0 and 4 pass. Row 1 has NULL left, row 2 NULL right,
        // row 3 is 11 > 20 false.
        assert_eq!(got, vec![5, 13]);
    }

    /// Decimal columns store as Int64, but different scales do not
    /// share raw integer ordering. The column-vs-column fast path must
    /// decline so Eval can rescale before compare.
    #[test]
    fn decimal_column_column_different_scales_falls_back() {
        let left_values = vec![10_000_i64, 1_000, 500];
        let right_values = vec![200_000_i64, 2_000, 600];
        let batch = Batch::new([
            Column::Int64(NumericColumn::from_data(left_values)),
            Column::Int64(NumericColumn::from_data(right_values)),
        ])
        .expect("batch ok");
        let left_type = DataType::Decimal {
            precision: Some(12),
            scale: Some(2),
        };
        let right_type = DataType::Decimal {
            precision: Some(12),
            scale: Some(4),
        };
        let schema = Schema::new([
            Field::required("left_dec", left_type.clone()),
            Field::required("right_dec", right_type.clone()),
        ])
        .expect("schema ok");
        let pred = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(ScalarExpr::Column {
                name: "left_dec".into(),
                index: 0,
                data_type: left_type,
            }),
            right: Box::new(ScalarExpr::Column {
                name: "right_dec".into(),
                index: 1,
                data_type: right_type,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i64> = match &out.columns()[0] {
            Column::Int64(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        // Logical values: 100.00 > 20.0000, 10.00 > 0.2000,
        // 5.00 > 0.0600. Raw Int64 ordering would incorrectly drop
        // row 0 (10000 < 200000).
        assert_eq!(got, vec![10_000, 1_000, 500]);
    }

    /// `col + 1 > 5` does not match the col-op-literal shape (LHS is a
    /// `Binary(Add, ...)`, not a `Column`). Fast path must decline;
    /// the scalar fallback must produce the same answer.
    #[test]
    fn non_fast_path_falls_back() {
        let data: Vec<i32> = vec![3, 4, 5, 6, 7];
        let batch = Batch::new([Column::Int32(NumericColumn::from_data(data))]).expect("batch ok");
        let schema = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");

        // (x + 1) > 5  → keeps rows where x > 4, i.e. {5, 6, 7}.
        let lhs = ScalarExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(ScalarExpr::Column {
                name: "x".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(1),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Int32,
        };
        let pred = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(lhs),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(5),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i32> = match &out.columns()[0] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        assert_eq!(got, vec![5, 6, 7]);
    }

    /// `100 > col` is the swapped-operand variant: the matcher must
    /// flip the operator so the kernel sees `col < 100`.
    #[test]
    fn vectorized_literal_on_left_is_flipped() {
        let data: Vec<i64> = (0..200_i64).collect();
        let schema = schema_x_i64();
        let pred = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(ScalarExpr::Literal {
                value: Value::Int64(100),
                data_type: DataType::Int64,
            }),
            right: Box::new(ScalarExpr::Column {
                name: "x".into(),
                index: 0,
                data_type: DataType::Int64,
            }),
            data_type: DataType::Bool,
        };
        let scan = MemTableScan::new(schema, vec![batch_i64(data.clone())]);
        let mut filter = Filter::new(Box::new(scan), pred);

        let out = filter.next_batch().unwrap().unwrap();
        let got: Vec<i64> = match &out.columns()[0] {
            Column::Int64(c) => c.data().to_vec(),
            other => panic!("unexpected column type: {other:?}"),
        };
        let want: Vec<i64> = data.iter().copied().filter(|&v| v < 100).collect();
        assert_eq!(got, want);
    }
}
