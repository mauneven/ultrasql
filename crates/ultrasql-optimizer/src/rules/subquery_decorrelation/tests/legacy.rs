//! Tests for the legacy LeftOuter + IS [NOT] NULL lowering convention.

use ultrasql_core::DataType;
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use super::*;

#[test]
fn exists_subquery_lowers_to_left_outer_join_with_is_not_null() {
    let outer = outer_scan();
    let sub = sub_scan();

    // Build the decorrelated form directly using the test helper.
    let result = make_exists_filter(&outer, sub, /* negated */ false);

    // Top node must be a Filter.
    let LogicalPlan::Filter { input, predicate } = &result else {
        panic!("expected Filter at top; got {result:?}");
    };

    // Predicate must be `IS NOT NULL`.
    assert!(
        matches!(predicate, ScalarExpr::IsNull { negated: true, .. }),
        "EXISTS should produce IS NOT NULL predicate; got {predicate:?}"
    );

    // Inner must be a LeftOuter Join.
    assert!(
        matches!(
            input.as_ref(),
            LogicalPlan::Join {
                join_type: LogicalJoinType::LeftOuter,
                ..
            }
        ),
        "EXISTS should lower to LeftOuter join; got {input:?}"
    );
}

#[test]
fn not_exists_subquery_lowers_to_left_outer_join_with_is_null() {
    let outer = outer_scan();
    let sub = sub_scan();

    let result = make_exists_filter(&outer, sub, /* negated */ true);

    let LogicalPlan::Filter { input, predicate } = &result else {
        panic!("expected Filter at top; got {result:?}");
    };

    // Predicate must be `IS NULL` (negated = false).
    assert!(
        matches!(predicate, ScalarExpr::IsNull { negated: false, .. }),
        "NOT EXISTS should produce IS NULL predicate; got {predicate:?}"
    );

    assert!(
        matches!(
            input.as_ref(),
            LogicalPlan::Join {
                join_type: LogicalJoinType::LeftOuter,
                ..
            }
        ),
        "NOT EXISTS should lower to LeftOuter join"
    );
}

#[test]
fn in_subquery_lowers_to_left_outer_join_with_equality_and_is_not_null() {
    let outer = outer_scan();
    let sub = sub_scan();

    // outer.id IN (SELECT key FROM sub)
    let outer_expr = col("id", 0, DataType::Int32);
    let inner_col = col("key", 0, DataType::Int32);

    let result =
        make_in_subquery_filter(&outer, outer_expr, inner_col, sub, /* negated */ false);

    let LogicalPlan::Filter { input, predicate } = &result else {
        panic!("expected Filter at top; got {result:?}");
    };

    // Filter predicate is IS NOT NULL.
    assert!(
        matches!(predicate, ScalarExpr::IsNull { negated: true, .. }),
        "IN should produce IS NOT NULL; got {predicate:?}"
    );

    // Join must be LeftOuter with an equality ON condition.
    match input.as_ref() {
        LogicalPlan::Join {
            join_type: LogicalJoinType::LeftOuter,
            condition: LogicalJoinCondition::On(cond),
            ..
        } => {
            assert!(
                matches!(
                    cond,
                    ScalarExpr::Binary {
                        op: BinaryOp::Eq,
                        ..
                    }
                ),
                "join condition should be equality; got {cond:?}"
            );
        }
        other => panic!("expected LeftOuter join with ON condition; got {other:?}"),
    }
}

#[test]
fn not_in_subquery_lowers_to_left_outer_join_with_is_null() {
    let outer = outer_scan();
    let sub = sub_scan();

    let outer_expr = col("id", 0, DataType::Int32);
    let inner_col = col("key", 0, DataType::Int32);

    let result =
        make_in_subquery_filter(&outer, outer_expr, inner_col, sub, /* negated */ true);

    let LogicalPlan::Filter { predicate, .. } = &result else {
        panic!("expected Filter at top; got {result:?}");
    };

    assert!(
        matches!(predicate, ScalarExpr::IsNull { negated: false, .. }),
        "NOT IN should produce IS NULL; got {predicate:?}"
    );
}

#[test]
fn exists_rewrite_produces_schema_wider_than_outer() {
    let outer = outer_scan(); // 2 cols
    let sub = sub_scan(); // 2 cols
    let outer_width = outer.schema().len();
    let sub_width = sub.schema().len();

    let result = make_exists_filter(&outer, sub, false);

    // The join schema should be outer_width + sub_width.
    let LogicalPlan::Filter { input, .. } = &result else {
        panic!("expected Filter");
    };
    assert_eq!(
        input.schema().len(),
        outer_width + sub_width,
        "join schema width should equal outer + sub"
    );
}
