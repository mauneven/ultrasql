//! Direct columnar fast path for trivial scalar aggregates.
//!
//! Recognises the exact plan shape
//!
//! ```text
//! Aggregate { group_by: [], aggregates: [Sum|Avg|CountStar] }
//!   └── Scan { table }
//! ```
//!
//! over a single `Int32` or `Int64` column with no NULL values and
//! lowers it to [`DirectScalarAggScan`] — a single-pass operator that
//! drives its child a batch at a time, pulls the typed numeric column
//! directly, and accumulates through one of the SIMD kernels in
//! [`ultrasql_vec::kernels`]:
//!
//! * `SUM(int)`   → `sum_i32_widening` / `sum_i64`            → `Int64` output
//! * `AVG(int)`   → `sum_*` + `count_i64` (column length)     → `Float64` output
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
//!   fixed (`Int64` or `Float64`, single column).
//!
//! NULL handling: when the input column carries a validity bitmap the
//! operator falls back to the slow path by returning `ExecError::Unsupported`
//! — the lowerer's contract is to only construct this operator for
//! columns that the caller has verified non-null. The bench shape
//! (`(id INT NOT NULL, x INT)` with monotonically generated values) is
//! always non-null; richer null-aware kernels remain on the
//! `HashAggregate` slow path until a workload demonstrates the need.
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
use ultrasql_vec::column::{Column, NumericColumn};
use ultrasql_vec::kernels::{sum_i32_widening, sum_i64};

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
    /// `AVG(col)`. Output type is `Float64`.
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

    /// Build an `AVG(INT)` aggregator over `col_idx` of `child`. The
    /// output is `Float64`.
    #[must_use]
    pub fn avg_int32(child: Box<dyn Operator>, col_idx: usize, output_name: String) -> Self {
        Self::new_int_input(
            child,
            DirectScalarAggKind::Avg,
            InputKind::Int32 { col_idx },
            DataType::Float64,
            output_name,
        )
    }

    /// Build an `AVG(BIGINT)` aggregator over `col_idx` of `child`. The
    /// output is `Float64`.
    #[must_use]
    pub fn avg_int64(child: Box<dyn Operator>, col_idx: usize, output_name: String) -> Self {
        Self::new_int_input(
            child,
            DirectScalarAggKind::Avg,
            InputKind::Int64 { col_idx },
            DataType::Float64,
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
        let output_schema = Schema::new([Field::required(output_name, output_type)])
            .expect("trivial single-column schema is well-formed");
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
        // `total_sum` carries the SUM (Int32 widens to i64;
        // Int64 stays i64 with wrapping semantics that match
        // PostgreSQL's overflow behaviour under the legacy
        // `SUM(BIGINT) → BIGINT` widening rule). `count_rows`
        // tracks COUNT(*) when the operator was constructed for
        // CountStar, and the non-null row count for AVG. Both
        // start at zero; the AVG/SUM division below substitutes
        // a NULL row when the row count is zero (PostgreSQL
        // `SUM([])` and `AVG([])` are both NULL on empty input).
        let mut total_sum: i64 = 0;
        let mut count_rows: usize = 0;

        while let Some(batch) = self.child.next_batch()? {
            let rows = batch.rows();
            if rows == 0 {
                continue;
            }
            match self.input {
                InputKind::Count => {
                    count_rows = count_rows.saturating_add(rows);
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
                    // Null bitmap fallback: the lowerer is supposed
                    // to only construct this operator over columns
                    // that the caller has verified non-null. A
                    // batch that carries a validity bitmap is the
                    // result of an upstream operator we did not
                    // expect; punt to keep correctness intact.
                    if c.nulls().is_some() {
                        return Err(ExecError::Unsupported(
                            "DirectScalarAggScan: NULL-aware path not implemented; \
                             fall back to HashAggregate",
                        ));
                    }
                    total_sum = total_sum.wrapping_add(sum_i32_widening(c));
                    count_rows = count_rows.saturating_add(c.len());
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
                    if c.nulls().is_some() {
                        return Err(ExecError::Unsupported(
                            "DirectScalarAggScan: NULL-aware path not implemented; \
                             fall back to HashAggregate",
                        ));
                    }
                    total_sum = total_sum.wrapping_add(sum_i64(c));
                    count_rows = count_rows.saturating_add(c.len());
                }
            }
        }

        // Emit exactly one row. `Sum` and `CountStar` emit `Int64`;
        // `Avg` emits `Float64`. Empty input produces a single SQL
        // NULL row to match PostgreSQL semantics.
        let result_col = match self.kind {
            DirectScalarAggKind::Sum => {
                if count_rows == 0 {
                    null_int64_row()
                } else {
                    Column::Int64(NumericColumn::from_data(vec![total_sum]))
                }
            }
            DirectScalarAggKind::CountStar => {
                let count_i64 = i64::try_from(count_rows).unwrap_or(i64::MAX);
                Column::Int64(NumericColumn::from_data(vec![count_i64]))
            }
            DirectScalarAggKind::Avg => {
                if count_rows == 0 {
                    null_float64_row()
                } else {
                    // Widen through f64 once. Matches PostgreSQL's
                    // `AVG(INT) → DOUBLE PRECISION` widening rule
                    // and the binder's declared aggregate result type.
                    #[allow(clippy::cast_precision_loss)]
                    let avg = (total_sum as f64) / (count_rows as f64);
                    Column::Float64(NumericColumn::from_data(vec![avg]))
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
fn null_int64_row() -> Column {
    let mut nulls = ultrasql_vec::Bitmap::new(1, false);
    nulls.set(0, false);
    Column::Int64(NumericColumn::with_nulls(vec![0_i64], nulls).expect("matching lengths"))
}

/// Build a single-row `Float64` column carrying SQL `NULL`.
fn null_float64_row() -> Column {
    let mut nulls = ultrasql_vec::Bitmap::new(1, false);
    nulls.set(0, false);
    Column::Float64(NumericColumn::with_nulls(vec![0.0_f64], nulls).expect("matching lengths"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemTableScan;
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
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
    fn avg_int32_divides_and_widens_to_float64() {
        let scan = make_int32_scan("x", vec![2, 4, 6, 8]);
        let mut agg = DirectScalarAggScan::avg_int32(Box::new(scan), 0, "avg".into());
        let batch = agg.next_batch().expect("ok").expect("row");
        match &batch.columns()[0] {
            Column::Float64(c) => assert_eq!(c.data(), &[5.0_f64]),
            other => panic!("expected Float64, got {other:?}"),
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
            Column::Float64(c) => {
                let nulls = c.nulls().expect("null bitmap present on empty AVG");
                assert!(!nulls.get(0));
            }
            other => panic!("expected Float64 column, got {other:?}"),
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
}
