//! EXISTS / IN decorrelation assertions over real subquery-expression plans.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use super::*;
use crate::rules::RewriteRule;

#[test]
fn real_correlated_exists_rewrites_to_semi_join() {
    let sub = LogicalPlan::Filter {
        input: Box::new(sub_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("key", 0, DataType::Int32)),
            right: Box::new(outer_col("id", 0, DataType::Int32)),
            data_type: DataType::Bool,
        },
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::Exists {
            subplan: Box::new(sub),
            negated: false,
            correlated: true,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    assert_eq!(result.schema().len(), 2);
    assert!(
        matches!(
            result,
            LogicalPlan::Join {
                join_type: LogicalJoinType::Semi,
                condition: LogicalJoinCondition::On(_),
                ..
            }
        ),
        "EXISTS should become Semi join, got {result:?}"
    );
}

#[test]
fn real_correlated_not_exists_rewrites_to_anti_join() {
    let sub = LogicalPlan::Filter {
        input: Box::new(sub_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("key", 0, DataType::Int32)),
            right: Box::new(outer_col("id", 0, DataType::Int32)),
            data_type: DataType::Bool,
        },
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::Exists {
            subplan: Box::new(sub),
            negated: true,
            correlated: true,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    assert_eq!(result.schema().len(), 2);
    assert!(
        matches!(
            result,
            LogicalPlan::Join {
                join_type: LogicalJoinType::Anti,
                condition: LogicalJoinCondition::On(_),
                ..
            }
        ),
        "NOT EXISTS should become Anti join, got {result:?}"
    );
}

#[test]
fn real_correlated_exists_with_residual_projects_inner_columns() {
    let corr = ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(col("key", 0, DataType::Int32)),
        right: Box::new(outer_col("id", 0, DataType::Int32)),
        data_type: DataType::Bool,
    };
    let residual = ScalarExpr::Binary {
        op: BinaryOp::NotEq,
        left: Box::new(col("data", 1, DataType::Int32)),
        right: Box::new(outer_col("val", 1, DataType::Int32)),
        data_type: DataType::Bool,
    };
    let sub = LogicalPlan::Filter {
        input: Box::new(sub_scan()),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(corr),
            right: Box::new(residual),
            data_type: DataType::Bool,
        },
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::Exists {
            subplan: Box::new(sub),
            negated: false,
            correlated: true,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    let LogicalPlan::Join {
        right,
        join_type,
        condition,
        ..
    } = result
    else {
        panic!("EXISTS should become join");
    };
    assert_eq!(join_type, LogicalJoinType::Semi);
    assert_eq!(right.schema().len(), 2);
    assert!(matches!(right.as_ref(), LogicalPlan::Project { .. },));
    let LogicalJoinCondition::On(predicate) = condition else {
        panic!("expected ON predicate");
    };
    let dump = predicate.to_string();
    assert!(
        dump.contains("data") && dump.contains("val"),
        "residual should survive after right projection, got {dump}"
    );
}

#[test]
fn real_uncorrelated_in_rewrites_to_semi_join() {
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::InSubquery {
            expr: Box::new(col("id", 0, DataType::Int32)),
            subplan: Box::new(sub_key_project()),
            negated: false,
            correlated: false,
            data_type: DataType::Int32,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    assert_eq!(result.schema().len(), 2);
    assert!(matches!(
        result,
        LogicalPlan::Join {
            join_type: LogicalJoinType::Semi,
            ..
        }
    ));
}

#[test]
fn real_uncorrelated_not_in_rewrites_to_anti_join() {
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::InSubquery {
            expr: Box::new(col("id", 0, DataType::Int32)),
            subplan: Box::new(sub_key_project()),
            negated: true,
            correlated: false,
            data_type: DataType::Int32,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    assert_eq!(result.schema().len(), 2);
    assert!(matches!(
        result,
        LogicalPlan::Join {
            join_type: LogicalJoinType::Anti,
            ..
        }
    ));
}

#[test]
fn real_correlated_in_rewrites_to_semi_join() {
    let sub = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(sub_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col("data", 1, DataType::Int32)),
                right: Box::new(outer_col("val", 1, DataType::Int32)),
                data_type: DataType::Bool,
            },
        }),
        exprs: vec![(col("key", 0, DataType::Int32), "key".into())],
        schema: Schema::new([Field::required("key", DataType::Int32)]).expect("schema ok"),
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::InSubquery {
            expr: Box::new(col("id", 0, DataType::Int32)),
            subplan: Box::new(sub),
            negated: false,
            correlated: true,
            data_type: DataType::Int32,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    let LogicalPlan::Join {
        right,
        join_type,
        condition,
        ..
    } = result
    else {
        panic!("correlated IN should become join");
    };
    assert_eq!(join_type, LogicalJoinType::Semi);
    assert!(matches!(right.as_ref(), LogicalPlan::Aggregate { .. }));
    let LogicalJoinCondition::On(predicate) = condition else {
        panic!("expected ON predicate");
    };
    let dump = predicate.to_string();
    assert!(
        dump.contains("val") && dump.contains("__in_subquery"),
        "correlated IN predicate should match both correlation key and projected value, got {dump}"
    );
}

#[test]
fn real_correlated_not_in_rewrites_to_anti_join() {
    let sub = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(sub_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col("data", 1, DataType::Int32)),
                right: Box::new(outer_col("val", 1, DataType::Int32)),
                data_type: DataType::Bool,
            },
        }),
        exprs: vec![(col("key", 0, DataType::Int32), "key".into())],
        schema: Schema::new([Field::required("key", DataType::Int32)]).expect("schema ok"),
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(outer_scan()),
        predicate: ScalarExpr::InSubquery {
            expr: Box::new(col("id", 0, DataType::Int32)),
            subplan: Box::new(sub),
            negated: true,
            correlated: true,
            data_type: DataType::Int32,
        },
    };

    let result = SubqueryDecorrelation
        .apply(&plan)
        .expect("no error")
        .expect("rewrite");
    assert!(matches!(
        result,
        LogicalPlan::Join {
            join_type: LogicalJoinType::Anti,
            ..
        }
    ));
}
