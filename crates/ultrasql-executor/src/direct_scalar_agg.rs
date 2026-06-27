//! Direct columnar fast path for trivial scalar aggregates.
//!
//! Recognises the exact plan shape
//!
//! ```text
//! Aggregate { group_by: [], aggregates: [Sum|Avg|CountStar] }
//!   └── Scan { table }
//! ```
//!
//! over a single `Int32` or `Int64` column and
//! lowers it to [`DirectScalarAggScan`] — a single-pass operator that
//! drives its child a batch at a time, pulls the typed numeric column
//! directly, and accumulates through one of the SIMD kernels in
//! [`ultrasql_vec::kernels`] for dense batches:
//!
//! * `SUM(int)`   → `sum_i32_widening` / `sum_i64`            → `Int64` output
//! * `AVG(int)`   → `sum_*` + `count_i64` (column length)     → `numeric` output
//! * `COUNT(*)`   → column length per batch                     → `Int64` output
//!
//! The operator emits exactly one row in a single batch, then EOF.
//! Compared to the generic `HashAggregate(SeqScan)` chain it skips:
//!
//! * the per-row scalar push the binder-driven aggregator uses to feed
//!   a single hash-table slot (one push per accumulator + one
//!   `count_seen` increment per row of every batch),
//! * the hash-table allocation and key-equality machinery for a plan
//!   that contains zero group keys,
//! * the per-batch projection allocation, since the result schema is
//!   fixed (`Int64`, `numeric`, single column).
//!
//! NULL handling: dense batches stay on the SIMD kernel path. Nullable
//! batches use a compact per-row validity fold that skips invalid rows
//! and counts only non-null rows for `SUM` / `AVG`.
//!
//! Output schema mirrors PostgreSQL's widening rules:
//!
//! * `SUM(INT) → BIGINT`
//! * `SUM(BIGINT) → BIGINT`
//! * `AVG(INT) → DOUBLE PRECISION`
//! * `AVG(BIGINT) → DOUBLE PRECISION`
//! * `COUNT(*) → BIGINT`

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};
use ultrasql_vec::kernels::sum_i32_widening;

use crate::{ExecError, Operator};

/// Which scalar aggregate to compute.
///
/// `Count` (i.e. `COUNT(*)`) ignores the column type entirely — it
/// uses the batch row count rather than reading any column.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DirectScalarAggKind {
    /// `SUM(col)`. Output type is `Int64` for both `Int32` and `Int64`
    /// columns (PostgreSQL widens `SUM(INT)` to `BIGINT` to avoid
    /// 32-bit overflow on large groups).
    Sum,
    /// `AVG(col)`. Output type is `numeric` (PostgreSQL semantics — exact
    /// decimal division materialised as decimal text).
    Avg,
    /// `COUNT(*)`. Output type is `Int64`. Reads no column.
    CountStar,
}

/// Which column-data path the operator should expect on every batch.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum InputKind {
    /// No column input is consulted; only row counts are accumulated.
    Count,
    /// Pull `Column::Int32` at `col_idx`.
    Int32 { col_idx: usize },
    /// Pull `Column::Int64` at `col_idx`.
    Int64 { col_idx: usize },
}

/// Hand-rolled scalar-aggregate operator. See the module documentation
/// for the matched plan shape.
///
/// `child` is the source operator — typically a [`crate::SeqScan`] over a
/// persistent relation. The operator owns the child like a unary
/// operator and drains it to completion the first time `next_batch`
/// is called, then emits a single-row result batch on that same call.
/// Subsequent calls return `Ok(None)`.
pub struct DirectScalarAggScan {
    child: Box<dyn Operator>,
    kind: DirectScalarAggKind,
    input: InputKind,
    output_schema: Schema,
    done: bool,
}

impl std::fmt::Debug for DirectScalarAggScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectScalarAggScan")
            .field("kind", &self.kind)
            .field("input", &self.input)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl DirectScalarAggScan {
    /// Build a `SUM(int)` aggregator over `col_idx` of `child`. The
    /// caller is responsible for verifying that `col_idx` references
    /// an `Int32` or `Int64` column in the child's schema.
    #[must_use]
    pub fn sum_int32(child: Box<dyn Operator>, col_idx: usize, output_name: String) -> Self {
        Self::new_int_input(
            child,
            DirectScalarAggKind::Sum,
            InputKind::Int32 { col_idx },
            DataType::Int64,
            output_name,
        )
    }

    /// Build a `SUM(BIGINT)` aggregator over `col_idx` of `child`.
    #[must_use]
    pub fn sum_int64(child: Box<dyn Operator>, col_idx: usize, output_name: String) -> Self {
        Self::new_int_input(
            child,
            DirectScalarAggKind::Sum,
            InputKind::Int64 { col_idx },
            DataType::Int64,
            output_name,
        )
    }

    /// Build an `AVG(INT)` aggregator over `col_idx` of `child`. The output
    /// is `numeric` (PostgreSQL semantics — exact decimal division).
    #[must_use]
    pub fn avg_int32(child: Box<dyn Operator>, col_idx: usize, output_name: String) -> Self {
        Self::new_int_input(
            child,
            DirectScalarAggKind::Avg,
            InputKind::Int32 { col_idx },
            avg_decimal_type(),
            output_name,
        )
    }

    /// Build an `AVG(BIGINT)` aggregator over `col_idx` of `child`. The
    /// output is `numeric` (PostgreSQL semantics — exact decimal division).
    #[must_use]
    pub fn avg_int64(child: Box<dyn Operator>, col_idx: usize, output_name: String) -> Self {
        Self::new_int_input(
            child,
            DirectScalarAggKind::Avg,
            InputKind::Int64 { col_idx },
            avg_decimal_type(),
            output_name,
        )
    }

    /// Build a `COUNT(*)` aggregator over `child`. The operator never
    /// reads any of `child`'s columns; it accumulates `batch.rows()`
    /// per pull and emits the total as `Int64`.
    #[must_use]
    pub fn count_star(child: Box<dyn Operator>, output_name: String) -> Self {
        Self::new_int_input(
            child,
            DirectScalarAggKind::CountStar,
            InputKind::Count,
            DataType::Int64,
            output_name,
        )
    }

    fn new_int_input(
        child: Box<dyn Operator>,
        kind: DirectScalarAggKind,
        input: InputKind,
        output_type: DataType,
        output_name: String,
    ) -> Self {
        let output_schema = match Schema::new([Field::required(output_name, output_type)]) {
            Ok(schema) => schema,
            Err(err) => {
                tracing::error!(error = %err, "direct scalar aggregate schema construction failed");
                Schema::empty()
            }
        };
        Self {
            child,
            kind,
            input,
            output_schema,
            done: false,
        }
    }
}

impl Operator for DirectScalarAggScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Accumulator state.
        //
        // `total_sum` carries the SUM. Int32 widens to i64 and Int64
        // stays i64; overflow is a typed execution error instead of a
        // wrapped SQL-visible result. `count_rows` tracks COUNT(*) when
        // the operator was constructed for CountStar, and the non-null
        // row count for AVG. Both start at zero; the AVG/SUM division
        // below substitutes a NULL row when the row count is zero.
        let mut total_sum: i64 = 0;
        let mut count_rows: i64 = 0;

        while let Some(batch) = self.child.next_batch()? {
            let rows = batch.rows();
            if rows == 0 {
                continue;
            }
            match self.input {
                InputKind::Count => {
                    checked_count_increment(&mut count_rows, rows)?;
                }
                InputKind::Int32 { col_idx } => {
                    let cols = batch.columns();
                    if col_idx >= cols.len() {
                        return Err(ExecError::TypeMismatch(format!(
                            "DirectScalarAggScan: column index {col_idx} out of range \
                             for child batch of width {}",
                            cols.len()
                        )));
                    }
                    let Column::Int32(c) = &cols[col_idx] else {
                        return Err(ExecError::TypeMismatch(
                            "DirectScalarAggScan: expected Int32 column".to_owned(),
                        ));
                    };
                    let (delta, non_nulls) = sum_i32_nullable(c)?;
                    total_sum = checked_sum(total_sum, delta, "DirectScalarAggScan SUM(INT)")?;
                    checked_count_increment(&mut count_rows, non_nulls)?;
                }
                InputKind::Int64 { col_idx } => {
                    let cols = batch.columns();
                    if col_idx >= cols.len() {
                        return Err(ExecError::TypeMismatch(format!(
                            "DirectScalarAggScan: column index {col_idx} out of range \
                             for child batch of width {}",
                            cols.len()
                        )));
                    }
                    let Column::Int64(c) = &cols[col_idx] else {
                        return Err(ExecError::TypeMismatch(
                            "DirectScalarAggScan: expected Int64 column".to_owned(),
                        ));
                    };
                    let (delta, non_nulls) = sum_i64_nullable(c)?;
                    total_sum = checked_sum(total_sum, delta, "DirectScalarAggScan SUM(BIGINT)")?;
                    checked_count_increment(&mut count_rows, non_nulls)?;
                }
            }
        }

        // Emit exactly one row. `Sum` and `CountStar` emit `Int64`;
        // `Avg` emits `numeric` (decimal text). Empty input produces a
        // single SQL NULL row to match PostgreSQL semantics.
        let result_col = match self.kind {
            DirectScalarAggKind::Sum => {
                if count_rows == 0 {
                    null_int64_row()?
                } else {
                    Column::Int64(NumericColumn::from_data(vec![total_sum]))
                }
            }
            DirectScalarAggKind::CountStar => {
                Column::Int64(NumericColumn::from_data(vec![count_rows]))
            }
            DirectScalarAggKind::Avg => {
                if count_rows == 0 {
                    null_decimal_row()?
                } else {
                    // PostgreSQL `AVG(int)` returns `numeric`: divide the i64
                    // sum exactly in i128 decimal space at the
                    // PostgreSQL-compatible result scale, then materialise as
                    // decimal text (the standard Decimal column form).
                    let avg = crate::hash_aggregate::arith::avg_decimal_division(
                        i128::from(total_sum),
                        0,
                        count_rows,
                    )
                    .ok_or_else(|| {
                        ExecError::NumericFieldOverflow(
                            "DirectScalarAggScan AVG division overflow".to_owned(),
                        )
                    })?;
                    Column::Utf8(StringColumn::from_data(vec![avg.to_string()]))
                }
            }
        };
        let batch = Batch::new([result_col]).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        // The operator always emits exactly one row (the scalar
        // result), even for an empty child — the empty case lands as a
        // single SQL NULL row.
        Some(1)
    }
}

/// Build a single-row `Int64` column carrying SQL `NULL`.
fn null_int64_row() -> Result<Column, ExecError> {
    let mut nulls = ultrasql_vec::Bitmap::new(1, false);
    nulls.set(0, false);
    NumericColumn::with_nulls(vec![0_i64], nulls)
        .map(Column::Int64)
        .map_err(|err| ExecError::TypeMismatch(format!("direct scalar SUM NULL row: {err}")))
}

/// Logical type of an `AVG`-over-integer result column: `numeric`
/// (PostgreSQL semantics). Precision/scale are unconstrained — the rendered
/// scale is value-dependent (PostgreSQL `select_div_scale`).
pub fn avg_decimal_type() -> DataType {
    DataType::Decimal {
        precision: None,
        scale: None,
    }
}

/// Render `AVG(sum, count)` over integer input as PostgreSQL-compatible
/// `numeric` decimal text (exact i128 division at PG's `select_div_scale`).
/// `count` must be non-zero. Returns `None` on i128 overflow. Shared with
/// the server's cached scalar-aggregate fast path so every AVG path agrees.
#[must_use]
pub fn avg_int_decimal_text(sum: i128, count: i64) -> Option<String> {
    crate::hash_aggregate::arith::avg_decimal_division(sum, 0, count).map(|v| v.to_string())
}

/// Build a single-row Decimal column carrying SQL `NULL`. Decimal columns
/// materialise as text, so an empty-group AVG is a NULL text cell.
fn null_decimal_row() -> Result<Column, ExecError> {
    let mut nulls = ultrasql_vec::Bitmap::new(1, false);
    nulls.set(0, false);
    StringColumn::with_nulls(vec![String::new()], nulls)
        .map(Column::Utf8)
        .map_err(|err| ExecError::TypeMismatch(format!("direct scalar AVG NULL row: {err}")))
}

fn checked_sum(acc: i64, delta: i64, context: &str) -> Result<i64, ExecError> {
    acc.checked_add(delta)
        .ok_or_else(|| ExecError::NumericFieldOverflow(format!("{context} overflow")))
}

fn checked_count_increment(count: &mut i64, delta: usize) -> Result<(), ExecError> {
    let delta = i64::try_from(delta).map_err(|_| {
        ExecError::NumericFieldOverflow("DirectScalarAggScan COUNT overflow".to_owned())
    })?;
    *count = count.checked_add(delta).ok_or_else(|| {
        ExecError::NumericFieldOverflow("DirectScalarAggScan COUNT overflow".to_owned())
    })?;
    Ok(())
}

fn checked_local_count_increment(count: &mut usize) -> Result<(), ExecError> {
    *count = count.checked_add(1).ok_or_else(|| {
        ExecError::NumericFieldOverflow("DirectScalarAggScan COUNT overflow".to_owned())
    })?;
    Ok(())
}

fn sum_i32_nullable(c: &NumericColumn<i32>) -> Result<(i64, usize), ExecError> {
    match c.nulls() {
        None => Ok((sum_i32_widening(c), c.len())),
        Some(nulls) => {
            let mut sum = 0_i64;
            let mut count = 0_usize;
            for (idx, value) in c.data().iter().copied().enumerate() {
                if nulls.get(idx) {
                    sum = checked_sum(sum, i64::from(value), "DirectScalarAggScan SUM(INT)")?;
                    checked_local_count_increment(&mut count)?;
                }
            }
            Ok((sum, count))
        }
    }
}

fn sum_i64_nullable(c: &NumericColumn<i64>) -> Result<(i64, usize), ExecError> {
    match c.nulls() {
        None => {
            let mut sum = 0_i64;
            for value in c.data().iter().copied() {
                sum = checked_sum(sum, value, "DirectScalarAggScan SUM(BIGINT)")?;
            }
            Ok((sum, c.len()))
        }
        Some(nulls) => {
            let mut sum = 0_i64;
            let mut count = 0_usize;
            for (idx, value) in c.data().iter().copied().enumerate() {
                if nulls.get(idx) {
                    sum = checked_sum(sum, value, "DirectScalarAggScan SUM(BIGINT)")?;
                    checked_local_count_increment(&mut count)?;
                }
            }
            Ok((sum, count))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemTableScan;
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
    use ultrasql_vec::Bitmap;
    use ultrasql_vec::column::{Column, NumericColumn};

    fn make_int32_scan(name: &str, values: Vec<i32>) -> MemTableScan {
        let schema = Schema::new([Field::required(name, DataType::Int32)]).expect("schema");
        let batch = Batch::new([Column::Int32(NumericColumn::from_data(values))]).expect("batch");
        MemTableScan::new(schema, vec![batch])
    }

    fn make_int64_scan(name: &str, values: Vec<i64>) -> MemTableScan {
        let schema = Schema::new([Field::required(name, DataType::Int64)]).expect("schema");
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(values))]).expect("batch");
        MemTableScan::new(schema, vec![batch])
    }

    fn nullable_int32_scan(name: &str, values: Vec<i32>, valid: &[bool]) -> MemTableScan {
        let schema = Schema::new([Field::required(name, DataType::Int32)]).expect("schema");
        let mut nulls = Bitmap::new(valid.len(), false);
        for (idx, is_valid) in valid.iter().copied().enumerate() {
            nulls.set(idx, is_valid);
        }
        let batch = Batch::new([Column::Int32(
            NumericColumn::with_nulls(values, nulls).expect("bitmap len matches"),
        )])
        .expect("batch");
        MemTableScan::new(schema, vec![batch])
    }

    fn nullable_int64_scan(name: &str, values: Vec<i64>, valid: &[bool]) -> MemTableScan {
        let schema = Schema::new([Field::required(name, DataType::Int64)]).expect("schema");
        let mut nulls = Bitmap::new(valid.len(), false);
        for (idx, is_valid) in valid.iter().copied().enumerate() {
            nulls.set(idx, is_valid);
        }
        let batch = Batch::new([Column::Int64(
            NumericColumn::with_nulls(values, nulls).expect("bitmap len matches"),
        )])
        .expect("batch");
        MemTableScan::new(schema, vec![batch])
    }

    #[test]
    fn sum_int32_returns_widening_total_in_int64() {
        let scan = make_int32_scan("x", vec![1, 2, 3, 4, 5]);
        let mut agg = DirectScalarAggScan::sum_int32(Box::new(scan), 0, "sum".into());
        let batch = agg.next_batch().expect("ok").expect("single row emitted");
        assert_eq!(batch.rows(), 1);
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[15]),
            other => panic!("expected Int64 column, got {other:?}"),
        }
        assert!(
            agg.next_batch().expect("ok").is_none(),
            "EOF after single batch"
        );
    }

    #[test]
    fn sum_int64_passes_through_without_widening() {
        let scan = make_int64_scan("x", vec![10_i64, 20, 30]);
        let mut agg = DirectScalarAggScan::sum_int64(Box::new(scan), 0, "sum".into());
        let batch = agg.next_batch().expect("ok").expect("row");
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[60]),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn sum_int64_overflow_returns_typed_error() {
        let scan = make_int64_scan("x", vec![i64::MAX, 1]);
        let mut agg = DirectScalarAggScan::sum_int64(Box::new(scan), 0, "sum".into());

        let err = agg
            .next_batch()
            .expect_err("SUM(BIGINT) overflow must not wrap");

        assert!(matches!(err, ExecError::NumericFieldOverflow(_)), "{err:?}");
    }

    #[test]
    fn count_increment_overflow_returns_typed_error() {
        let mut count = i64::MAX;
        let err = checked_count_increment(&mut count, 1)
            .expect_err("direct scalar count overflow must not saturate");
        assert!(matches!(err, ExecError::NumericFieldOverflow(_)));
        assert_eq!(count, i64::MAX);
    }

    #[test]
    fn avg_int32_divides_exactly_to_numeric() {
        // AVG over INT returns numeric (PG): avg(2,4,6,8) = 5, rendered at the
        // AVG result scale (16) as decimal text.
        let scan = make_int32_scan("x", vec![2, 4, 6, 8]);
        let mut agg = DirectScalarAggScan::avg_int32(Box::new(scan), 0, "avg".into());
        let batch = agg.next_batch().expect("ok").expect("row");
        match &batch.columns()[0] {
            Column::Utf8(c) => assert_eq!(c.value(0), "5.0000000000000000"),
            other => panic!("expected decimal-text (Utf8), got {other:?}"),
        }
    }

    #[test]
    fn count_star_counts_rows_without_reading_columns() {
        let scan = make_int32_scan("x", vec![100, 200, 300, 400]);
        let mut agg = DirectScalarAggScan::count_star(Box::new(scan), "count".into());
        let batch = agg.next_batch().expect("ok").expect("row");
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[4]),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn sum_on_empty_input_emits_single_null_row() {
        let schema = Schema::new([Field::required("x", DataType::Int32)]).expect("schema");
        let scan = MemTableScan::new(schema, vec![]);
        let mut agg = DirectScalarAggScan::sum_int32(Box::new(scan), 0, "sum".into());
        let batch = agg.next_batch().expect("ok").expect("row");
        assert_eq!(batch.rows(), 1);
        match &batch.columns()[0] {
            Column::Int64(c) => {
                // Single-row NULL: data slot present but the validity
                // bit is `false` (1 = valid, 0 = null per the
                // ultrasql-vec convention).
                let nulls = c.nulls().expect("null bitmap present on empty SUM");
                assert!(!nulls.get(0));
            }
            other => panic!("expected Int64 column, got {other:?}"),
        }
    }

    #[test]
    fn avg_on_empty_input_emits_single_null_row() {
        let schema = Schema::new([Field::required("x", DataType::Int32)]).expect("schema");
        let scan = MemTableScan::new(schema, vec![]);
        let mut agg = DirectScalarAggScan::avg_int32(Box::new(scan), 0, "avg".into());
        let batch = agg.next_batch().expect("ok").expect("row");
        match &batch.columns()[0] {
            // Empty-group AVG is numeric NULL, materialised as a NULL text cell.
            Column::Utf8(c) => {
                let nulls = c.nulls().expect("null bitmap present on empty AVG");
                assert!(!nulls.get(0));
            }
            other => panic!("expected decimal-text (Utf8) column, got {other:?}"),
        }
    }

    #[test]
    fn count_star_on_empty_input_emits_zero() {
        let schema = Schema::new([Field::required("x", DataType::Int32)]).expect("schema");
        let scan = MemTableScan::new(schema, vec![]);
        let mut agg = DirectScalarAggScan::count_star(Box::new(scan), "count".into());
        let batch = agg.next_batch().expect("ok").expect("row");
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[0]),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn avg_int64_schema_debug_and_row_count_are_stable() {
        let scan = make_int64_scan("x", vec![10_i64, 20, 30]);
        let mut agg = DirectScalarAggScan::avg_int64(Box::new(scan), 0, "avg".into());
        assert_eq!(agg.schema().field_at(0).name, "avg");
        assert_eq!(agg.estimated_row_count(), Some(1));
        assert!(format!("{agg:?}").contains("DirectScalarAggScan"));

        let batch = agg.next_batch().expect("ok").expect("row");

        // AVG over BIGINT returns numeric: avg(10,20,30) = 20 (scale 16).
        match &batch.columns()[0] {
            Column::Utf8(c) => assert_eq!(c.value(0), "20.0000000000000000"),
            other => panic!("expected decimal-text (Utf8), got {other:?}"),
        }
    }

    #[test]
    fn direct_scalar_agg_skips_empty_batches_before_accumulating() {
        let schema = Schema::new([Field::required("x", DataType::Int64)]).expect("schema");
        let empty =
            Batch::new([Column::Int64(NumericColumn::from_data(Vec::<i64>::new()))]).unwrap();
        let non_empty =
            Batch::new([Column::Int64(NumericColumn::from_data(vec![2_i64, 3]))]).expect("batch");
        let scan = MemTableScan::new(schema, vec![empty, non_empty]);
        let mut agg = DirectScalarAggScan::sum_int64(Box::new(scan), 0, "sum".into());

        let batch = agg.next_batch().expect("ok").expect("row");

        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[5]),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn direct_scalar_sum_skips_nulls() {
        let scan = nullable_int32_scan("x", vec![10, 20, 30, 40], &[true, false, true, false]);
        let mut agg = DirectScalarAggScan::sum_int32(Box::new(scan), 0, "sum".into());

        let batch = agg.next_batch().expect("ok").expect("row");

        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[40]),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn direct_scalar_avg_skips_nulls() {
        let scan = nullable_int64_scan("x", vec![10, 20, 30, 40], &[true, false, true, false]);
        let mut agg = DirectScalarAggScan::avg_int64(Box::new(scan), 0, "avg".into());

        let batch = agg.next_batch().expect("ok").expect("row");

        // Skips the two NULLs: avg(10, 30) = 20 as numeric (scale 16).
        match &batch.columns()[0] {
            Column::Utf8(c) => assert_eq!(c.value(0), "20.0000000000000000"),
            other => panic!("expected decimal-text (Utf8), got {other:?}"),
        }
    }

    #[test]
    fn direct_scalar_sum_all_nulls_emits_null() {
        let scan = nullable_int64_scan("x", vec![10, 20], &[false, false]);
        let mut agg = DirectScalarAggScan::sum_int64(Box::new(scan), 0, "sum".into());

        let batch = agg.next_batch().expect("ok").expect("row");

        match &batch.columns()[0] {
            Column::Int64(c) => {
                let nulls = c.nulls().expect("null bitmap present");
                assert!(!nulls.get(0));
            }
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn direct_scalar_agg_reports_bad_column_shapes() {
        let mut out_of_range = DirectScalarAggScan::sum_int32(
            Box::new(make_int32_scan("x", vec![1])),
            4,
            "sum".into(),
        );
        let err = out_of_range
            .next_batch()
            .expect_err("out-of-range column must fail");
        assert!(err.to_string().contains("column index 4 out of range"));

        let mut wrong_i32 = DirectScalarAggScan::sum_int32(
            Box::new(make_int64_scan("x", vec![1])),
            0,
            "sum".into(),
        );
        let err = wrong_i32
            .next_batch()
            .expect_err("wrong Int32 type must fail");
        assert!(err.to_string().contains("expected Int32 column"));

        let mut wrong_i64 = DirectScalarAggScan::sum_int64(
            Box::new(make_int32_scan("x", vec![1])),
            0,
            "sum".into(),
        );
        let err = wrong_i64
            .next_batch()
            .expect_err("wrong Int64 type must fail");
        assert!(err.to_string().contains("expected Int64 column"));
    }
}
