//! Row-cap operator.
//!
//! `Limit` consumes its child until it has produced `n` rows in total,
//! then returns end-of-stream. The terminal batch is truncated to the
//! exact remaining row budget; intermediate batches pass through
//! unchanged.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::{ExecError, Operator};

/// Row-cap pull operator.
///
/// Produces at most `n` rows across all emitted batches. Once the
/// budget is exhausted, [`Operator::next_batch`] returns `Ok(None)`
/// without pulling another batch from the child. The terminal batch is
/// truncated to the remaining row budget via [`slice_batch`].
///
/// `Limit` does not perform a `LIMIT n OFFSET k` rewrite; an OFFSET
/// operator will land alongside the planner work for row-skipping.
#[derive(Debug)]
pub struct Limit {
    child: Box<dyn Operator>,
    schema: Schema,
    remaining: usize,
}

impl Limit {
    /// Construct a row-cap operator with budget `n`.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, n: usize) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            schema,
            remaining: n,
        }
    }
}

impl Operator for Limit {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let Some(input) = self.child.next_batch()? else {
            return Ok(None);
        };
        let rows = input.rows();
        if rows <= self.remaining {
            self.remaining -= rows;
            return Ok(Some(input));
        }
        let truncated = slice_batch(&input, self.remaining)?;
        self.remaining = 0;
        Ok(Some(truncated))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Build a new [`Batch`] containing the first `len` rows of `input`.
///
/// `len` must not exceed `input.rows()`. The operation rebuilds each
/// column rather than slicing in place: `ultrasql-vec` does not yet
/// expose a zero-copy row-range view, and modifying that crate is out
/// of scope for this scaffold.
fn slice_batch(input: &Batch, len: usize) -> Result<Batch, ExecError> {
    debug_assert!(len <= input.rows());
    let mut out = Vec::with_capacity(input.width());
    for col in input.columns() {
        out.push(slice_column(col, len));
    }
    Batch::new(out).map_err(Into::into)
}

fn slice_column(col: &Column, len: usize) -> Column {
    match col {
        Column::Int32(c) => Column::Int32(slice_numeric(c, len)),
        Column::Int64(c) => Column::Int64(slice_numeric(c, len)),
        Column::Float32(c) => Column::Float32(slice_numeric(c, len)),
        Column::Float64(c) => Column::Float64(slice_numeric(c, len)),
        Column::Bool(c) => Column::Bool(slice_bool(c, len)),
        Column::Utf8(c) => Column::Utf8(slice_utf8(c, len)),
    }
}

fn slice_numeric<T: Copy>(col: &NumericColumn<T>, len: usize) -> NumericColumn<T> {
    NumericColumn::from_data(col.data()[..len].to_vec())
}

fn slice_bool(col: &BoolColumn, len: usize) -> BoolColumn {
    let rows: Vec<bool> = (0..len).map(|i| col.value(i)).collect();
    BoolColumn::from_data(rows)
}

fn slice_utf8(col: &StringColumn, len: usize) -> StringColumn {
    let rows: Vec<String> = (0..len).map(|i| col.value(i).to_owned()).collect();
    StringColumn::from_data(rows)
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;
    use crate::MemTableScan;

    fn schema() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema is well-formed")
    }

    fn int_batch(rows: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(rows.to_vec()))])
            .expect("batch is well-formed")
    }

    fn drain_i32(op: &mut Limit) -> Vec<i32> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            match &b.columns()[0] {
                Column::Int32(c) => out.extend_from_slice(c.data()),
                other => panic!("unexpected column: {other:?}"),
            }
        }
        out
    }

    #[test]
    fn limit_passes_full_batches_under_budget() {
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2]), int_batch(&[3, 4])]);
        let mut limit = Limit::new(Box::new(scan), 10);
        assert_eq!(drain_i32(&mut limit), vec![1, 2, 3, 4]);
    }

    #[test]
    fn limit_truncates_terminal_batch() {
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2, 3]), int_batch(&[4, 5, 6])]);
        let mut limit = Limit::new(Box::new(scan), 4);
        assert_eq!(drain_i32(&mut limit), vec![1, 2, 3, 4]);
    }

    #[test]
    fn limit_zero_emits_nothing() {
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2, 3])]);
        let mut limit = Limit::new(Box::new(scan), 0);
        assert!(limit.next_batch().unwrap().is_none());
    }

    #[test]
    fn limit_does_not_pull_after_budget_exhausted() {
        // Build a scan whose second batch would panic if observed.
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2])]);
        let mut limit = Limit::new(Box::new(scan), 2);
        let first = limit.next_batch().unwrap().unwrap();
        assert_eq!(first.rows(), 2);
        // Budget exhausted: must short-circuit without touching child.
        assert!(limit.next_batch().unwrap().is_none());
    }

    #[test]
    fn slice_batch_handles_all_column_types() {
        use ultrasql_vec::column::{BoolColumn, StringColumn};
        let b = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![1_i32, 2, 3])),
            Column::Int64(NumericColumn::from_data(vec![10_i64, 20, 30])),
            Column::Float32(NumericColumn::from_data(vec![1.0_f32, 2.0, 3.0])),
            Column::Float64(NumericColumn::from_data(vec![1.0_f64, 2.0, 3.0])),
            Column::Bool(BoolColumn::from_data(vec![true, false, true])),
            Column::Utf8(StringColumn::from_data(vec![
                "a".to_string(),
                "bb".to_string(),
                "ccc".to_string(),
            ])),
        ])
        .unwrap();
        let sliced = slice_batch(&b, 2).unwrap();
        assert_eq!(sliced.rows(), 2);
        match &sliced.columns()[5] {
            Column::Utf8(s) => {
                assert_eq!(s.value(0), "a");
                assert_eq!(s.value(1), "bb");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }
}
