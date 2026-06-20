//! Unit tests for the [`Filter`](super::Filter) operator and its
//! supporting kernels.
//!
//! Shared fixtures and constructors live here; the test cases
//! themselves are split across the topic submodules below.

mod codec;
mod operator;
mod vectorized;

use std::sync::Arc;

use ultrasql_core::{DataType, Field, GeometryType, Oid, RangeType, Schema, Value, pack_timetz};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_vec::Batch;
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use super::{Filter, batch_to_rows, build_empty_batch, select_column};
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
