//! Unit tests for the hash-aggregate operator and its kernels.
//!
//! Shared fixtures and constructors live here; the test cases themselves are
//! split across the topic submodules below.

mod kernels;
mod operator;
mod vectorized;

use std::collections::HashSet;

use ultrasql_core::{
    BitString, DataType, Field, GeometryType, GeometryValue, Lsn, NetworkValue, Oid, RangeType,
    RangeValue, Schema, SparseVector, Value,
};
use ultrasql_planner::{AggregateFunc, BinaryOp, LogicalAggregateExpr, ScalarExpr, SortKey};
use ultrasql_vec::Batch;
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};
use ultrasql_vec::dict::DictionaryColumn;

use super::HashAggregate;
use super::key::GroupKey;
use super::state::{AggState, accumulate_value, finalise};
use super::vec_plan::{
    VecAggSlot, build_grouped_vectorized_plan, build_vectorized_plan, finalize_grouped_sum,
    grouped_vectorized_step, read_i32_key, read_i64_key, read_numeric_value,
};
use super::vec_step::{accumulate_sum, column_non_null_count, update_extremum, vectorized_step};
use crate::mem_table_scan::MemTableScan;
use crate::{CancelFlag, Operator, WorkMemBudget};

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.into(),
        index,
        data_type,
    }
}

fn lit_i32(v: i32) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Int32(v),
        data_type: DataType::Int32,
    }
}

fn lit_f64(v: f64) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Float64(v),
        data_type: DataType::Float64,
    }
}

fn divide_i32_by_zero(name: &str, index: usize) -> ScalarExpr {
    ScalarExpr::Binary {
        op: BinaryOp::Div,
        left: Box::new(col(name, index, DataType::Int32)),
        right: Box::new(lit_i32(0)),
        data_type: DataType::Int32,
    }
}

fn divide_i64_by_zero(name: &str, index: usize) -> ScalarExpr {
    ScalarExpr::Binary {
        op: BinaryOp::Div,
        left: Box::new(col(name, index, DataType::Int64)),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int64(0),
            data_type: DataType::Int64,
        }),
        data_type: DataType::Int64,
    }
}

fn count_star_agg() -> LogicalAggregateExpr {
    LogicalAggregateExpr {
        func: AggregateFunc::CountStar,
        arg: None,
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "cnt".into(),
        data_type: DataType::Int64,
    }
}

fn sum_agg(name: &str, index: usize) -> LogicalAggregateExpr {
    LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(col(name, index, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "total".into(),
        data_type: DataType::Int64,
    }
}

fn count_distinct_agg(name: &str, index: usize, data_type: DataType) -> LogicalAggregateExpr {
    LogicalAggregateExpr {
        func: AggregateFunc::Count,
        arg: Some(col(name, index, data_type)),
        direct_arg: None,
        order_by: None,
        distinct: true,
        output_name: "distinct_count".into(),
        data_type: DataType::Int64,
    }
}

fn min_agg(name: &str, index: usize) -> LogicalAggregateExpr {
    LogicalAggregateExpr {
        func: AggregateFunc::Min,
        arg: Some(col(name, index, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "mn".into(),
        data_type: DataType::Int64,
    }
}

fn max_agg(name: &str, index: usize) -> LogicalAggregateExpr {
    LogicalAggregateExpr {
        func: AggregateFunc::Max,
        arg: Some(col(name, index, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "mx".into(),
        data_type: DataType::Int64,
    }
}

/// Schema: (group i32, val i64)
fn schema_group_val() -> Schema {
    Schema::new([
        Field::required("group", DataType::Int32),
        Field::required("val", DataType::Int64),
    ])
    .expect("schema ok")
}

fn make_batch_i32_i64(rows: &[(i32, i64)]) -> Batch {
    Batch::new([
        Column::Int32(NumericColumn::from_data(
            rows.iter().map(|(a, _)| *a).collect(),
        )),
        Column::Int64(NumericColumn::from_data(
            rows.iter().map(|(_, b)| *b).collect(),
        )),
    ])
    .expect("batch ok")
}

#[derive(Debug)]
struct CancellingScan {
    schema: Schema,
    batch: Option<Batch>,
    flag: CancelFlag,
}

impl Operator for CancellingScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, crate::ExecError> {
        if let Some(batch) = self.batch.take() {
            self.flag.cancel();
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn sum_decimal_mul_i32_agg() -> LogicalAggregateExpr {
    LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(ScalarExpr::Binary {
            op: ultrasql_planner::BinaryOp::Mul,
            left: Box::new(col(
                "cost",
                1,
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            )),
            right: Box::new(col("qty", 2, DataType::Int32)),
            data_type: DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        }),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "value".into(),
        data_type: DataType::Decimal {
            precision: Some(15),
            scale: Some(2),
        },
    }
}

fn drain_all(op: &mut dyn Operator) -> Vec<Vec<Value>> {
    let schema = op.schema().clone();
    let mut out = Vec::new();
    while let Some(b) = op.next_batch().expect("no error") {
        let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
        out.extend(rows);
    }
    out
}

// ---------------------------------------------------------------------------
// Batch fixtures for the vectorised cross-validation tests
// ---------------------------------------------------------------------------

/// Build a single `(val i64 NULL)` batch with the given values and an
/// optional null bitmap. Used by the vectorised-path cross-checks.
fn make_i64_batch(values: Vec<i64>, nulls: Option<Vec<bool>>) -> (Schema, Batch) {
    use ultrasql_vec::Bitmap;
    let n = values.len();
    let schema = Schema::new([Field::nullable("val", DataType::Int64)]).expect("schema ok");
    let col = match nulls {
        None => Column::Int64(NumericColumn::from_data(values)),
        Some(pat) => {
            assert_eq!(pat.len(), n);
            let mut bm = Bitmap::new(n, false);
            for (i, &v) in pat.iter().enumerate() {
                if v {
                    bm.set(i, true);
                }
            }
            Column::Int64(NumericColumn::with_nulls(values, bm).expect("col ok"))
        }
    };
    let batch = Batch::new([col]).expect("batch ok");
    (schema, batch)
}

/// Build a single `(v i32 NULL)` batch with the given values and an
/// optional null bitmap.
fn make_i32_batch(values: Vec<i32>, nulls: Option<Vec<bool>>) -> (Schema, Batch) {
    use ultrasql_vec::Bitmap;
    let n = values.len();
    let schema = Schema::new([Field::nullable("v", DataType::Int32)]).expect("schema ok");
    let col = match nulls {
        None => Column::Int32(NumericColumn::from_data(values)),
        Some(pat) => {
            assert_eq!(pat.len(), n);
            let mut bm = Bitmap::new(n, false);
            for (i, &v) in pat.iter().enumerate() {
                if v {
                    bm.set(i, true);
                }
            }
            Column::Int32(NumericColumn::with_nulls(values, bm).expect("col ok"))
        }
    };
    let batch = Batch::new([col]).expect("batch ok");
    (schema, batch)
}
