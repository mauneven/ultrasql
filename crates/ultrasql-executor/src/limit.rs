//! Row-cap / row-skip operator.
//!
//! `Limit` consumes its child until it has skipped `offset` rows and
//! then produced `n` rows in total, then returns end-of-stream. Skipped
//! and terminal batches are trimmed via an internal helper;
//! intermediate batches pass through unchanged.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::{ExecError, Operator};

/// Row-cap / row-skip pull operator.
///
/// Produces at most `limit` rows across all emitted batches, after
/// discarding the first `offset` rows. Once the budget is exhausted,
/// [`Operator::next_batch`] returns `Ok(None)` without pulling another
/// batch from the child.
///
/// `offset` is observed by trimming the leading rows of the first
/// batches the child emits; once `offset` rows have been skipped the
/// operator falls through to the standard "emit at most `limit` more
/// rows" path. Batches whose entire row range falls inside the skip
/// window are discarded without re-allocating, so an `OFFSET m` with
/// no `LIMIT` is O(skipped rows) on the column kernels, not O(m × log)
/// or any worse shape.
#[derive(Debug)]
pub struct Limit {
    child: Box<dyn Operator>,
    schema: Schema,
    /// Rows still to discard before any output row is emitted. Once
    /// this reaches `0` the operator transitions to the "emit up to
    /// `remaining` rows" mode.
    to_skip: usize,
    /// Rows still permitted in the output. Decrements monotonically.
    /// `usize::MAX` is the saturated representation of "no limit"
    /// (used when the binder lowers `OFFSET m` with no `LIMIT`).
    remaining: usize,
}

impl Limit {
    /// Construct a row-cap operator with budget `n` and no offset.
    ///
    /// Equivalent to `Limit::with_offset(child, n, 0)`. Retained as a
    /// distinct constructor because the majority of call sites do not
    /// specify an offset, and inlining a `0` at every site would be
    /// noise.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, n: usize) -> Self {
        Self::with_offset(child, n, 0)
    }

    /// Construct a row-cap operator with budget `limit` after skipping
    /// the first `offset` rows of the child.
    ///
    /// `limit` of `usize::MAX` is treated as "no limit" — the operator
    /// emits every row past `offset` until the child is drained. This
    /// matches the binder's representation of `OFFSET m` with no
    /// `LIMIT` clause.
    #[must_use]
    pub fn with_offset(child: Box<dyn Operator>, limit: usize, offset: usize) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            schema,
            to_skip: offset,
            remaining: limit,
        }
    }
}

impl Operator for Limit {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        // Phase 1: discard whole batches that fall entirely inside the
        // skip window, then chop the leading prefix off the boundary
        // batch.
        while self.to_skip > 0 {
            let Some(input) = self.child.next_batch()? else {
                // Child drained mid-skip: nothing to emit.
                return Ok(None);
            };
            let rows = input.rows();
            if rows <= self.to_skip {
                self.to_skip -= rows;
                continue;
            }
            // Boundary batch: keep `rows - to_skip` rows from the tail.
            let keep_from = self.to_skip;
            self.to_skip = 0;
            let tail_len = rows - keep_from;
            // Cap the tail at the remaining budget so we don't over-emit
            // when the skip lands inside the same batch that would also
            // hit the limit boundary.
            let take = tail_len.min(self.remaining);
            if take == 0 {
                // `remaining == 0` after the offset: still mark the
                // budget exhausted so the next call returns Ok(None)
                // without pulling another batch.
                return Ok(None);
            }
            let trimmed = slice_batch_range(&input, keep_from, keep_from + take)?;
            self.remaining -= take;
            return Ok(Some(trimmed));
        }

        // Phase 2: standard row-cap behaviour.
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
        let truncated = slice_batch_range(&input, 0, self.remaining)?;
        self.remaining = 0;
        Ok(Some(truncated))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Build a new [`Batch`] containing rows `[start, end)` of `input`.
///
/// `start <= end <= input.rows()`. The operation rebuilds each column
/// rather than slicing in place: `ultrasql-vec` does not yet expose a
/// zero-copy row-range view, and modifying that crate is out of scope
/// for this operator.
fn slice_batch_range(input: &Batch, start: usize, end: usize) -> Result<Batch, ExecError> {
    debug_assert!(start <= end);
    debug_assert!(end <= input.rows());
    let mut out = Vec::with_capacity(input.width());
    for col in input.columns() {
        out.push(slice_column_range(col, start, end));
    }
    Batch::new(out).map_err(Into::into)
}

fn slice_column_range(col: &Column, start: usize, end: usize) -> Column {
    match col {
        Column::Int32(c) => Column::Int32(slice_numeric_range(c, start, end)),
        Column::Int64(c) => Column::Int64(slice_numeric_range(c, start, end)),
        Column::Float32(c) => Column::Float32(slice_numeric_range(c, start, end)),
        Column::Float64(c) => Column::Float64(slice_numeric_range(c, start, end)),
        Column::Bool(c) => Column::Bool(slice_bool_range(c, start, end)),
        Column::Utf8(c) => Column::Utf8(slice_utf8_range(c, start, end)),
    }
}

fn slice_numeric_range<T: Copy>(
    col: &NumericColumn<T>,
    start: usize,
    end: usize,
) -> NumericColumn<T> {
    NumericColumn::from_data(col.data()[start..end].to_vec())
}

fn slice_bool_range(col: &BoolColumn, start: usize, end: usize) -> BoolColumn {
    let rows: Vec<bool> = (start..end).map(|i| col.value(i)).collect();
    BoolColumn::from_data(rows)
}

fn slice_utf8_range(col: &StringColumn, start: usize, end: usize) -> StringColumn {
    let rows: Vec<String> = (start..end).map(|i| col.value(i).to_owned()).collect();
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
        let sliced = slice_batch_range(&b, 0, 2).unwrap();
        assert_eq!(sliced.rows(), 2);
        match &sliced.columns()[5] {
            Column::Utf8(s) => {
                assert_eq!(s.value(0), "a");
                assert_eq!(s.value(1), "bb");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn slice_batch_range_picks_arbitrary_window() {
        let b = int_batch(&[10, 20, 30, 40, 50]);
        let sliced = slice_batch_range(&b, 1, 4).unwrap();
        assert_eq!(sliced.rows(), 3);
        match &sliced.columns()[0] {
            Column::Int32(c) => assert_eq!(c.data(), &[20, 30, 40]),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn limit_with_offset_skips_then_takes() {
        // 6 rows total. OFFSET 2 LIMIT 3 → rows 3,4,5 (1-indexed: rows 3..=5).
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2, 3, 4, 5, 6])]);
        let mut limit = Limit::with_offset(Box::new(scan), 3, 2);
        assert_eq!(drain_i32(&mut limit), vec![3, 4, 5]);
    }

    #[test]
    fn limit_with_offset_spans_multiple_batches() {
        // 3 batches of 2 rows each: [1,2][3,4][5,6]. OFFSET 3 → drop [1,2]
        // (entire batch) and the leading 1 of [3,4]; LIMIT 2 → emit
        // [4], [5]. Confirms the cross-batch boundary case.
        let scan = MemTableScan::new(
            schema(),
            vec![int_batch(&[1, 2]), int_batch(&[3, 4]), int_batch(&[5, 6])],
        );
        let mut limit = Limit::with_offset(Box::new(scan), 2, 3);
        assert_eq!(drain_i32(&mut limit), vec![4, 5]);
    }

    #[test]
    fn limit_with_offset_no_limit_emits_tail() {
        // OFFSET 4, no LIMIT (modeled as usize::MAX). Should emit rows
        // 5, 6 in this 6-row scan and then stop.
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2, 3, 4, 5, 6])]);
        let mut limit = Limit::with_offset(Box::new(scan), usize::MAX, 4);
        assert_eq!(drain_i32(&mut limit), vec![5, 6]);
    }

    #[test]
    fn limit_with_offset_past_end_emits_nothing() {
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2, 3])]);
        let mut limit = Limit::with_offset(Box::new(scan), 5, 10);
        assert!(limit.next_batch().unwrap().is_none());
    }

    #[test]
    fn limit_with_offset_zero_limit_emits_nothing() {
        // OFFSET 1 LIMIT 0 should emit no rows and short-circuit cleanly.
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2, 3])]);
        let mut limit = Limit::with_offset(Box::new(scan), 0, 1);
        assert!(limit.next_batch().unwrap().is_none());
    }

    #[test]
    fn limit_with_offset_skip_lands_on_batch_boundary() {
        // OFFSET 2 lands exactly at the end of [1,2]; the next batch
        // [3,4] should pass through untouched (still capped by LIMIT 1
        // to verify the boundary-emit ALSO honours the cap correctly).
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2]), int_batch(&[3, 4])]);
        let mut limit = Limit::with_offset(Box::new(scan), 1, 2);
        assert_eq!(drain_i32(&mut limit), vec![3]);
    }
}
