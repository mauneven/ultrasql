//! Vectorized filter operator.
//!
//! [`VectorizedFilter`] applies a column-scalar equality predicate to each
//! incoming batch using a SIMD selection vector from `ultrasql-vec`, then
//! materialises only the selected rows into a new batch.
//!
//! For the general expression case, the operator falls back to row-at-a-time
//! evaluation via the expression interpreter [`Eval`].  The SIMD fast path is
//! taken only for the pattern `column == scalar` on `Int32` or `Int64`.

use ultrasql_core::{Schema, Value};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_vec::column::{Column, NumericColumn};
use ultrasql_vec::{
    Batch, Bitmap, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto, filter_eq_i32,
    filter_eq_i64,
};

use crate::ExecError;
use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::push_pipeline::{SinkVerdict, VectorizedOperator, VectorizedSink};
use crate::seq_scan::build_batch;

/// Vectorized filter operator.
///
/// Wraps a child [`VectorizedOperator`] and for each incoming batch:
/// 1. Attempts the SIMD fast path: `column == scalar` on `Int32`/`Int64`.
/// 2. Falls back to the [`Eval`] interpreter for all other predicates.
/// 3. Materialises surviving rows and pushes to the downstream sink.
pub struct VectorizedFilter {
    child: Box<dyn VectorizedOperator>,
    predicate: ScalarExpr,
    schema: Schema,
}

impl std::fmt::Debug for VectorizedFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorizedFilter")
            .field("predicate", &self.predicate)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl VectorizedFilter {
    /// Construct a vectorized filter.
    #[must_use]
    pub fn new(child: Box<dyn VectorizedOperator>, predicate: ScalarExpr) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            predicate,
            schema,
        }
    }
}

impl VectorizedOperator for VectorizedFilter {
    fn drive(&mut self, sink: &mut dyn VectorizedSink) -> Result<(), ExecError> {
        let pred = self.predicate.clone();
        let schema = self.schema.clone();
        let child = &mut self.child;

        let mut filter_sink = FilterSink {
            inner: sink,
            predicate: pred,
            schema,
        };

        child.drive(&mut filter_sink)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---- Internal sink that applies the predicate ----

struct FilterSink<'a> {
    inner: &'a mut dyn VectorizedSink,
    predicate: ScalarExpr,
    schema: Schema,
}

impl VectorizedSink for FilterSink<'_> {
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError> {
        let selection = try_simd_filter(&batch, &self.predicate)?;

        let selected_batch = if let Some(mask) = selection {
            materialise_selection(&batch, &mask)?
        } else {
            let rows = batch_to_rows(&batch, &self.schema)?;
            let interpreter = Eval::new(self.predicate.clone());
            let survivors: Vec<Vec<Value>> = rows
                .into_iter()
                .filter(|row| matches!(interpreter.eval(row), Ok(Value::Bool(true))))
                .collect();

            if survivors.is_empty() {
                return Ok(SinkVerdict::Continue);
            }
            build_batch(&survivors, &self.schema)?
        };

        if selected_batch.is_empty() {
            return Ok(SinkVerdict::Continue);
        }
        self.inner.consume(selected_batch)
    }

    fn finalize(&mut self) -> Result<Option<Batch>, ExecError> {
        self.inner.finalize()
    }
}

// ---- SIMD fast-path detection ----

/// Return `Some(mask)` when predicate is `col_ref == integer_literal`.
fn try_simd_filter(batch: &Batch, predicate: &ScalarExpr) -> Result<Option<Bitmap>, ExecError> {
    if let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = predicate
    {
        if let (ScalarExpr::Column { index, .. }, ScalarExpr::Literal { value, .. }) =
            (left.as_ref(), right.as_ref())
        {
            let col_idx = *index;
            let cols = batch.columns();
            if col_idx >= cols.len() {
                return Err(ExecError::TypeMismatch(format!(
                    "filter column index {col_idx} out of range"
                )));
            }
            match (&cols[col_idx], value) {
                (Column::Int32(col), Value::Int32(scalar)) => {
                    return Ok(Some(filter_eq_i32(col, *scalar)));
                }
                (Column::Int64(col), Value::Int64(scalar)) => {
                    return Ok(Some(filter_eq_i64(col, *scalar)));
                }
                _ => {}
            }
        }
    }
    Ok(None)
}

/// Materialise rows selected by `mask` into a new batch.
fn materialise_selection(batch: &Batch, mask: &Bitmap) -> Result<Batch, ExecError> {
    let cols = batch.columns();
    let mut out_cols: Vec<Column> = Vec::with_capacity(cols.len());

    for col in cols {
        let selected: Column = match col {
            Column::Int32(c) => Column::Int32(NumericColumn::from_data(
                mask.iter_ones().map(|i| c.data()[i]).collect(),
            )),
            Column::Int64(c) => Column::Int64(NumericColumn::from_data(
                mask.iter_ones().map(|i| c.data()[i]).collect(),
            )),
            Column::Float32(c) => Column::Float32(NumericColumn::from_data(
                mask.iter_ones().map(|i| c.data()[i]).collect(),
            )),
            Column::Float64(c) => Column::Float64(NumericColumn::from_data(
                mask.iter_ones().map(|i| c.data()[i]).collect(),
            )),
            Column::Bool(c) => {
                use ultrasql_vec::column::BoolColumn;
                Column::Bool(BoolColumn::from_data(
                    mask.iter_ones().map(|i| c.value(i)).collect(),
                ))
            }
            Column::Utf8(_) | Column::DictionaryUtf8(_) => {
                let rows: Vec<Option<String>> = mask
                    .iter_ones()
                    .map(|i| col.text_value(i).map(str::to_owned))
                    .collect();
                match encode_strings_auto(
                    rows.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                }
            }
        };
        out_cols.push(selected);
    }

    Batch::new(out_cols).map_err(ExecError::from)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;
    use crate::mem_table_scan::MemTableScan;
    use crate::push_pipeline::CollectSink;
    use crate::vec_ops::scan::VectorizedSeqScan;

    fn schema_id() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    fn batch_i32(data: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(data.to_vec()))]).unwrap()
    }

    fn pred_eq_i32(col_idx: usize, value: i32) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: "id".into(),
                index: col_idx,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(value),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        }
    }

    fn drain_i32(batches: Vec<Batch>) -> Vec<i32> {
        let mut out = Vec::new();
        for b in batches {
            match &b.columns()[0] {
                Column::Int32(c) => out.extend_from_slice(c.data()),
                other => panic!("unexpected: {other:?}"),
            }
        }
        out
    }

    #[test]
    fn simd_filter_eq_i32_selects_matching_rows() {
        let scan = MemTableScan::new(
            schema_id(),
            vec![batch_i32(&[1, 2, 3, 2, 5]), batch_i32(&[2, 7, 2])],
        );
        let child = VectorizedSeqScan::new(Box::new(scan));
        let mut filter = VectorizedFilter::new(Box::new(child), pred_eq_i32(0, 2));
        let mut sink = CollectSink::new();
        filter.drive(&mut sink).unwrap();
        let rows = drain_i32(sink.finish());
        assert_eq!(rows, vec![2, 2, 2, 2]);
    }

    #[test]
    fn simd_filter_eq_i32_no_match_returns_empty() {
        let scan = MemTableScan::new(schema_id(), vec![batch_i32(&[1, 3, 5, 7])]);
        let child = VectorizedSeqScan::new(Box::new(scan));
        let mut filter = VectorizedFilter::new(Box::new(child), pred_eq_i32(0, 42));
        let mut sink = CollectSink::new();
        filter.drive(&mut sink).unwrap();
        assert!(sink.finish().is_empty());
    }

    #[test]
    fn filter_schema_matches_child() {
        let scan = MemTableScan::new(schema_id(), vec![]);
        let child = VectorizedSeqScan::new(Box::new(scan));
        let filter = VectorizedFilter::new(Box::new(child), pred_eq_i32(0, 1));
        assert_eq!(filter.schema().len(), 1);
    }
}
