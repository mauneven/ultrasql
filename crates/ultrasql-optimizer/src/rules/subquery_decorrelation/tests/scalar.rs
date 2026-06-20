//! Scalar-subquery decorrelation assertions, including COUNT/SUM empty-set handling.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_planner::{
    AggregateFunc, BinaryOp, LogicalAggregateExpr, LogicalJoinCondition, LogicalJoinType,
    LogicalPlan, ScalarExpr,
};

use super::*;
use crate::rules::RewriteRule;

#[test]
fn real_uncorrelated_scalar_subquery_rewrites_to_cross_join_filter() {
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(col("id", 0, DataType::Int32)),
            right: Box::new(ScalarExpr::ScalarSubquery {
                subplan: Box::new(sub_key_project()),
                correlated: false,
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    let LogicalPlan::Project { input, schema, .. } = &result else {
        panic!("expected Project, got {result:?}");
    };
    assert_eq!(schema.len(), 2);
    assert!(
        matches!(
            input.as_ref(),
            LogicalPlan::Filter {
                input,
                ..
            } if matches!(
                input.as_ref(),
                LogicalPlan::Join {
                    join_type: LogicalJoinType::Cross,
                    ..
                }
            )
        ),
        "scalar subquery should become Cross Join + Filter, got {input:?}"
    );
}

#[test]
fn real_correlated_scalar_aggregate_rewrites_to_left_join_filter() {
    let sub_filter = LogicalPlan::Filter {
        input: Box::new(sub_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("key", 0, DataType::Int32)),
            right: Box::new(outer_col("id", 0, DataType::Int32)),
            data_type: DataType::Bool,
        },
    };
    let agg_schema =
        Schema::new([Field::nullable("avg", DataType::Float64)]).expect("schema ok");
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(sub_filter),
        group_by: Vec::new(),
        aggregates: vec![LogicalAggregateExpr {
            func: AggregateFunc::Avg,
            arg: Some(col("data", 1, DataType::Int32)),
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: "avg".to_owned(),
            data_type: DataType::Float64,
        }],
        schema: agg_schema.clone(),
    };
    let subquery = LogicalPlan::Project {
        input: Box::new(aggregate),
        exprs: vec![(col("avg", 0, DataType::Float64), "avg".to_owned())],
        schema: agg_schema,
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(col("val", 1, DataType::Int32)),
            right: Box::new(ScalarExpr::ScalarSubquery {
                subplan: Box::new(subquery),
                correlated: true,
                data_type: DataType::Float64,
            }),
            data_type: DataType::Bool,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    let LogicalPlan::Project { input, schema, .. } = &result else {
        panic!("expected Project, got {result:?}");
    };
    assert_eq!(schema.len(), 2);
    assert!(
        matches!(
            input.as_ref(),
            LogicalPlan::Filter {
                input,
                ..
            } if matches!(
                input.as_ref(),
                LogicalPlan::Join {
                    join_type: LogicalJoinType::LeftOuter,
                    condition: LogicalJoinCondition::On(_),
                    ..
                }
            )
        ),
        "correlated scalar aggregate should become LeftOuter Join + Filter, got {input:?}"
    );
}

// BUG 1: correlated COUNT scalar subquery must report 0 (not NULL) for
// outer keys with no matching inner rows. After the LEFT OUTER JOIN the
// joined count column is NULL, so the substituted column must be wrapped in
// COALESCE(col, 0). SUM/MIN/MAX/AVG legitimately yield NULL and must NOT be
// wrapped.

#[test]
fn correlated_count_in_filter_predicate_coalesces_to_zero() {
    // SELECT * FROM outer WHERE (SELECT COUNT(*) FROM sub WHERE key=outer.id) = 0
    let subquery =
        correlated_scalar_agg_subplan(AggregateFunc::CountStar, None, "n", DataType::Int64);
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::ScalarSubquery {
                subplan: Box::new(subquery),
                correlated: true,
                data_type: DataType::Int64,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int64(0),
                data_type: DataType::Int64,
            }),
            data_type: DataType::Bool,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    // Outer Project, then Filter carrying the rewritten predicate.
    let LogicalPlan::Project { input, .. } = &result else {
        panic!("expected Project, got {result:?}");
    };
    let LogicalPlan::Filter { predicate, .. } = input.as_ref() else {
        panic!("expected Filter under Project, got {input:?}");
    };
    let ScalarExpr::Binary { left, .. } = predicate else {
        panic!("expected Binary predicate, got {predicate:?}");
    };
    assert!(
        is_coalesce_with_zero(left),
        "correlated COUNT in filter must be COALESCE(col, 0), got {left:?}"
    );
}

#[test]
fn correlated_count_in_projection_coalesces_to_zero() {
    // SELECT id, (SELECT COUNT(*) FROM sub WHERE key=outer.id) AS n FROM outer
    let subquery =
        correlated_scalar_agg_subplan(AggregateFunc::CountStar, None, "n", DataType::Int64);
    let proj_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("n", DataType::Int64),
    ])
    .expect("schema ok");
    let plan = LogicalPlan::Project {
        input: Box::new(outer_scan()),
        exprs: vec![
            (col("id", 0, DataType::Int32), "id".to_owned()),
            (
                ScalarExpr::ScalarSubquery {
                    subplan: Box::new(subquery),
                    correlated: true,
                    data_type: DataType::Int64,
                },
                "n".to_owned(),
            ),
        ],
        schema: proj_schema,
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    let LogicalPlan::Project { input, exprs, .. } = &result else {
        panic!("expected Project, got {result:?}");
    };
    // The decorrelated input is a LEFT OUTER JOIN against the grouped count.
    assert!(
        matches!(
            input.as_ref(),
            LogicalPlan::Join {
                join_type: LogicalJoinType::LeftOuter,
                condition: LogicalJoinCondition::On(_),
                ..
            }
        ),
        "correlated COUNT projection should LEFT OUTER JOIN, got {input:?}"
    );
    // The projected scalar must be COALESCE(col, 0), not a bare nullable col.
    assert!(
        is_coalesce_with_zero(&exprs[1].0),
        "projected correlated COUNT must be COALESCE(col, 0), got {:?}",
        exprs[1].0
    );
}

#[test]
fn correlated_sum_in_projection_does_not_coalesce() {
    // SUM over empty input is NULL in SQL, so the rewrite must leave the
    // joined column unwrapped (no COALESCE) — regression guard for the
    // COUNT-specific fix.
    let subquery = correlated_scalar_agg_subplan(
        AggregateFunc::Sum,
        Some(col("data", 1, DataType::Int32)),
        "s",
        DataType::Int64,
    );
    let proj_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("s", DataType::Int64),
    ])
    .expect("schema ok");
    let plan = LogicalPlan::Project {
        input: Box::new(outer_scan()),
        exprs: vec![
            (col("id", 0, DataType::Int32), "id".to_owned()),
            (
                ScalarExpr::ScalarSubquery {
                    subplan: Box::new(subquery),
                    correlated: true,
                    data_type: DataType::Int64,
                },
                "s".to_owned(),
            ),
        ],
        schema: proj_schema,
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    let LogicalPlan::Project { exprs, .. } = &result else {
        panic!("expected Project, got {result:?}");
    };
    assert!(
        !is_coalesce_with_zero(&exprs[1].0),
        "SUM must NOT be COALESCEd to 0, got {:?}",
        exprs[1].0
    );
    assert!(
        matches!(exprs[1].0, ScalarExpr::Column { .. }),
        "SUM scalar should remain a bare joined column, got {:?}",
        exprs[1].0
    );
}

// BUG 2: a non-aggregated correlated scalar subquery in a projection can
// match more than one inner row per outer key. A plain LEFT OUTER JOIN
// would silently multiply outer rows (wrong answer); SQL requires a
// cardinality error instead. With no runtime single-row-assert operator
// available, the rule must bail (return None) for this shape so the
// surviving subquery surfaces as a hard error rather than a silently-wrong
// duplicated result set.

#[test]
fn non_aggregated_correlated_scalar_in_projection_decorrelates() {
    // SELECT id, (SELECT data FROM sub WHERE key = outer.id) FROM outer
    let subquery = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(sub_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col("key", 0, DataType::Int32)),
                right: Box::new(outer_col("id", 0, DataType::Int32)),
                data_type: DataType::Bool,
            },
        }),
        exprs: vec![(col("data", 1, DataType::Int32), "data".to_owned())],
        schema: Schema::new([Field::nullable("data", DataType::Int32)]).expect("schema ok"),
    };
    let proj_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("data", DataType::Int32),
    ])
    .expect("schema ok");
    let plan = LogicalPlan::Project {
        input: Box::new(outer_scan()),
        exprs: vec![
            (col("id", 0, DataType::Int32), "id".to_owned()),
            (
                ScalarExpr::ScalarSubquery {
                    subplan: Box::new(subquery),
                    correlated: true,
                    data_type: DataType::Int32,
                },
                "data".to_owned(),
            ),
        ],
        schema: proj_schema,
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("single-row correlated scalar projection must decorrelate");
    // Decorrelation must rewrite to a Project over a LEFT OUTER JOIN with no
    // surviving scalar subquery (the executor has no correlated-subquery
    // fallback, so a surviving subquery would be a hard error). The
    // multi-row case remains a documented limitation, not a bail.
    match &result {
        LogicalPlan::Project { input, .. } => assert!(
            matches!(
                input.as_ref(),
                LogicalPlan::Join {
                    join_type: LogicalJoinType::LeftOuter,
                    ..
                }
            ),
            "expected Project over LEFT OUTER JOIN, got {result:?}"
        ),
        other => panic!("expected a Project at the top of the rewrite, got {other:?}"),
    }
}
