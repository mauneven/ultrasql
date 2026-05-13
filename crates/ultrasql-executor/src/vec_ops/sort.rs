//! Vectorized sort operator.
//!
//! [`VectorizedSort`] collects all batches from the child, sorts all rows
//! globally by a list of sort keys, then emits sorted batches of up to 4096
//! rows into the downstream sink.
//!
//! ## Sort strategy
//!
//! For `Int64` and `Int32` single-column sort keys the implementation uses
//! the standard `slice::sort_unstable_by` comparator, which LLVM often
//! reduces to a pdqsort. A radix-sort optimisation for fixed-width keys
//! is deferred to a follow-up.
//!
//! For multi-column and heterogeneous keys the same comparator path is used
//! with a row-wise comparison in sort-key order.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::ExecError;
use crate::push_pipeline::{SinkVerdict, VectorizedOperator, VectorizedSink};

/// Sort direction for a sort key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending order (smallest first).
    Asc,
    /// Descending order (largest first).
    Desc,
}

/// Specification of one sort key column.
#[derive(Clone, Debug)]
pub struct SortKeySpec {
    /// 0-based column index.
    pub col_idx: usize,
    /// Sort direction.
    pub direction: SortDirection,
    /// Whether NULLs sort first.
    pub nulls_first: bool,
}

/// Vectorized sort operator.
///
/// Collects all incoming batches into memory, globally sorts the rows by the
/// specified sort keys, then emits sorted 4096-row batches.
///
/// The operator is a pipeline-breaker: it must see all input before emitting
/// any output.
#[derive(Debug)]
pub struct VectorizedSort {
    child: Box<dyn VectorizedOperator>,
    sort_keys: Vec<SortKeySpec>,
    schema: Schema,
}

impl VectorizedSort {
    /// Construct a vectorized sort.
    ///
    /// - `child`     — upstream push operator.
    /// - `sort_keys` — ordered list of sort key column specifications.
    /// - `schema`    — output schema (same as child schema).
    #[must_use]
    pub fn new(
        child: Box<dyn VectorizedOperator>,
        sort_keys: Vec<SortKeySpec>,
        schema: Schema,
    ) -> Self {
        Self {
            child,
            sort_keys,
            schema,
        }
    }
}

impl VectorizedOperator for VectorizedSort {
    fn drive(&mut self, sink: &mut dyn VectorizedSink) -> Result<(), ExecError> {
        // Accumulate all rows.
        let mut collect = CollectAllSink {
            batches: Vec::new(),
        };
        self.child.drive(&mut collect)?;

        if collect.batches.is_empty() {
            return Ok(());
        }

        // Flatten all batches into per-column vecs.
        let n_cols = self.schema.len();
        let total_rows: usize = collect.batches.iter().map(Batch::rows).sum();
        let mut cols_data: Vec<ColData> = (0..n_cols).map(|_| ColData::Empty).collect();

        // Initialise column storage from the first batch's column types.
        let first_batch = &collect.batches[0];
        for (ci, col) in first_batch.columns().iter().enumerate() {
            cols_data[ci] = match col {
                Column::Int32(_) => ColData::I32(Vec::with_capacity(total_rows)),
                Column::Int64(_) => ColData::I64(Vec::with_capacity(total_rows)),
                Column::Float64(_) => ColData::F64(Vec::with_capacity(total_rows)),
                _ => ColData::Empty,
            };
        }

        for batch in &collect.batches {
            for (ci, col) in batch.columns().iter().enumerate() {
                match (&mut cols_data[ci], col) {
                    (ColData::I32(v), Column::Int32(c)) => v.extend_from_slice(c.data()),
                    (ColData::I64(v), Column::Int64(c)) => v.extend_from_slice(c.data()),
                    (ColData::F64(v), Column::Float64(c)) => v.extend_from_slice(c.data()),
                    _ => {}
                }
            }
        }

        // Build a row-index permutation, then sort it.
        let mut perm: Vec<usize> = (0..total_rows).collect();

        let sort_keys = &self.sort_keys;
        let cols_for_cmp = &cols_data;
        perm.sort_unstable_by(|&a, &b| {
            for key in sort_keys {
                let ord = compare_rows(cols_for_cmp, key, a, b);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Emit sorted batches of up to 4096 rows.
        const BATCH_SIZE: usize = 4096;
        for chunk in perm.chunks(BATCH_SIZE) {
            let batch = gather_rows(&cols_data, chunk)?;
            if sink.consume(batch)? == SinkVerdict::Stop {
                return Ok(());
            }
        }

        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---- Internal helpers ----

struct CollectAllSink {
    batches: Vec<Batch>,
}

impl VectorizedSink for CollectAllSink {
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError> {
        if !batch.is_empty() {
            self.batches.push(batch);
        }
        Ok(SinkVerdict::Continue)
    }
    fn finalize(&mut self) -> Result<Option<Batch>, ExecError> {
        Ok(None)
    }
}

enum ColData {
    I32(Vec<i32>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Empty,
}

fn compare_rows(cols: &[ColData], key: &SortKeySpec, a: usize, b: usize) -> std::cmp::Ordering {
    let ci = key.col_idx;
    let ord = if ci < cols.len() {
        match &cols[ci] {
            ColData::I32(v) => v[a].cmp(&v[b]),
            ColData::I64(v) => v[a].cmp(&v[b]),
            ColData::F64(v) => v[a].partial_cmp(&v[b]).unwrap_or(std::cmp::Ordering::Equal),
            ColData::Empty => std::cmp::Ordering::Equal,
        }
    } else {
        std::cmp::Ordering::Equal
    };

    if key.direction == SortDirection::Desc {
        ord.reverse()
    } else {
        ord
    }
}

fn gather_rows(cols: &[ColData], perm: &[usize]) -> Result<Batch, ExecError> {
    let mut out_cols: Vec<Column> = Vec::with_capacity(cols.len());
    for col in cols {
        let c: Column = match col {
            ColData::I32(v) => Column::Int32(NumericColumn::from_data(
                perm.iter().map(|&i| v[i]).collect(),
            )),
            ColData::I64(v) => Column::Int64(NumericColumn::from_data(
                perm.iter().map(|&i| v[i]).collect(),
            )),
            ColData::F64(v) => Column::Float64(NumericColumn::from_data(
                perm.iter().map(|&i| v[i]).collect(),
            )),
            ColData::Empty => {
                return Err(ExecError::Unsupported(
                    "VectorizedSort: unsupported column type",
                ));
            }
        };
        out_cols.push(c);
    }
    Batch::new(out_cols).map_err(ExecError::from)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;
    use crate::mem_table_scan::MemTableScan;
    use crate::push_pipeline::CollectSink;
    use crate::vec_ops::scan::VectorizedSeqScan;

    fn schema_val() -> Schema {
        Schema::new([Field::required("v", DataType::Int64)]).expect("schema ok")
    }

    fn batch_i64(data: &[i64]) -> Batch {
        Batch::new([Column::Int64(NumericColumn::from_data(data.to_vec()))]).unwrap()
    }

    fn drain_i64_all(batches: Vec<Batch>) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            match &b.columns()[0] {
                Column::Int64(c) => out.extend_from_slice(c.data()),
                other => panic!("unexpected {other:?}"),
            }
        }
        out
    }

    #[test]
    fn sort_ascending_single_column() {
        let scan = MemTableScan::new(
            schema_val(),
            vec![batch_i64(&[5, 1, 3]), batch_i64(&[2, 4])],
        );
        let child = VectorizedSeqScan::new(Box::new(scan));
        let key = SortKeySpec {
            col_idx: 0,
            direction: SortDirection::Asc,
            nulls_first: false,
        };
        let mut sort = VectorizedSort::new(Box::new(child), vec![key], schema_val());
        let mut sink = CollectSink::new();
        sort.drive(&mut sink).unwrap();
        let rows = drain_i64_all(sink.finish());
        assert_eq!(rows, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn sort_descending_single_column() {
        let scan = MemTableScan::new(schema_val(), vec![batch_i64(&[3, 1, 4, 1, 5, 9])]);
        let child = VectorizedSeqScan::new(Box::new(scan));
        let key = SortKeySpec {
            col_idx: 0,
            direction: SortDirection::Desc,
            nulls_first: false,
        };
        let mut sort = VectorizedSort::new(Box::new(child), vec![key], schema_val());
        let mut sink = CollectSink::new();
        sort.drive(&mut sink).unwrap();
        let rows = drain_i64_all(sink.finish());
        assert_eq!(rows, vec![9, 5, 4, 3, 1, 1]);
    }

    #[test]
    fn sort_empty_input_emits_nothing() {
        let scan = MemTableScan::new(schema_val(), vec![]);
        let child = VectorizedSeqScan::new(Box::new(scan));
        let key = SortKeySpec {
            col_idx: 0,
            direction: SortDirection::Asc,
            nulls_first: false,
        };
        let mut sort = VectorizedSort::new(Box::new(child), vec![key], schema_val());
        let mut sink = CollectSink::new();
        sort.drive(&mut sink).unwrap();
        assert!(sink.finish().is_empty());
    }
}
