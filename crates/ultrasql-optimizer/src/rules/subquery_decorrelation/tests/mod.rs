//! Unit tests for subquery decorrelation.
//!
//! Shared plan-builder helpers live here; topic-specific assertions are split
//! into the [`exists_in`], [`scalar`], and [`legacy`] child modules.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_planner::{AggregateFunc, BinaryOp, LogicalAggregateExpr, LogicalPlan, ScalarExpr};

use super::*;
use crate::rules::RewriteRule;

mod exists_in;
mod legacy;
mod scalar;

pub(super) fn scan(table: &str, fields: Vec<Field>) -> LogicalPlan {
    LogicalPlan::Scan {
        table: table.into(),
        schema: Schema::new(fields).expect("schema ok"),
        projection: None,
    }
}

pub(super) fn outer_scan() -> LogicalPlan {
    scan(
        "outer",
        vec![
            Field::required("id", DataType::Int32),
            Field::nullable("val", DataType::Int32),
        ],
    )
}

pub(super) fn sub_scan() -> LogicalPlan {
    scan(
        "sub",
        vec![
            Field::required("key", DataType::Int32),
            Field::nullable("data", DataType::Int32),
        ],
    )
}

pub(super) fn col(name: &str, idx: usize, dt: DataType) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.into(),
        index: idx,
        data_type: dt,
    }
}

pub(super) fn lit_i32(v: i32) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Int32(v),
        data_type: DataType::Int32,
    }
}

pub(super) fn outer_col(name: &str, idx: usize, dt: DataType) -> ScalarExpr {
    ScalarExpr::OuterColumn {
        name: name.into(),
        frame_depth: 1,
        column_index: idx,
        data_type: dt,
    }
}

pub(super) fn sub_key_project() -> LogicalPlan {
    let input = sub_scan();
    let schema = Schema::new([Field::required("key", DataType::Int32)]).expect("schema ok");
    LogicalPlan::Project {
        input: Box::new(input),
        exprs: vec![(col("key", 0, DataType::Int32), "key".into())],
        schema,
    }
}

/// Build the inner plan of `(SELECT <agg> FROM sub WHERE key = outer.id)`
/// as a `Project(Aggregate(Filter(scan)))`, the shape the binder produces.
pub(super) fn correlated_scalar_agg_subplan(
    func: AggregateFunc,
    arg: Option<ScalarExpr>,
    out_name: &str,
    out_type: DataType,
) -> LogicalPlan {
    let sub_filter = LogicalPlan::Filter {
        input: Box::new(sub_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("key", 0, DataType::Int32)),
            right: Box::new(outer_col("id", 0, DataType::Int32)),
            data_type: DataType::Bool,
        },
    };
    let agg_schema = Schema::new([Field::nullable(out_name, out_type.clone())]).expect("schema ok");
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(sub_filter),
        group_by: Vec::new(),
        aggregates: vec![LogicalAggregateExpr {
            func,
            arg,
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: out_name.to_owned(),
            data_type: out_type.clone(),
        }],
        schema: agg_schema.clone(),
    };
    LogicalPlan::Project {
        input: Box::new(aggregate),
        exprs: vec![(col(out_name, 0, out_type), out_name.to_owned())],
        schema: agg_schema,
    }
}

/// `True` when `expr` is a `COALESCE(_, 0)` call.
pub(super) fn is_coalesce_with_zero(expr: &ScalarExpr) -> bool {
    matches!(
        expr,
        ScalarExpr::FunctionCall { name, args, .. }
            if name == "coalesce"
                && args.len() == 2
                && matches!(
                    &args[1],
                    ScalarExpr::Literal {
                        value: Value::Int64(0),
                        ..
                    }
                )
    )
}

#[test]
fn rule_name_is_stable() {
    assert_eq!(SubqueryDecorrelation.name(), "subquery_decorrelation");
}

#[test]
fn no_op_on_plain_scan() {
    let plan = outer_scan();
    let result = SubqueryDecorrelation.apply(&plan).expect("no error");
    assert!(result.is_none(), "plain Scan should not be rewritten");
}

#[test]
fn no_op_on_filter_with_literal_predicate() {
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::Literal {
            value: Value::Bool(true),
            data_type: DataType::Bool,
        },
    };
    let result = SubqueryDecorrelation.apply(&plan).expect("no error");
    assert!(
        result.is_none(),
        "filter with literal pred should not be rewritten"
    );
}

#[test]
fn rule_apply_returns_none_for_ordinary_filter_with_column_predicate() {
    // An ordinary Filter(col = lit) should not be rewritten.
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("id", 0, DataType::Int32)),
            right: Box::new(lit_i32(42)),
            data_type: DataType::Bool,
        },
    };
    let result = SubqueryDecorrelation.apply(&plan).expect("no error");
    assert!(
        result.is_none(),
        "ordinary Filter should not be rewritten by SubqueryDecorrelation"
    );
}
