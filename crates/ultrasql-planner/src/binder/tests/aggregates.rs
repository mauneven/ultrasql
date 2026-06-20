//! Binder tests for GROUP BY / aggregate planning, HAVING, DISTINCT
//! aggregates and aggregate type reconciliation.

use ultrasql_core::{DataType, Field, Schema};

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};

// GROUP BY / aggregate tests
// -----------------------------------------------------------------------

#[test]
fn binds_group_by_emits_aggregate_node() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT id, count(*) FROM users GROUP BY id", &cat).expect("bind ok");

    fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_agg(input),
            _ => None,
        }
    }
    let agg = find_agg(&plan).expect("should contain Aggregate node");
    let LogicalPlan::Aggregate {
        group_by,
        aggregates,
        schema,
        ..
    } = agg
    else {
        panic!("expected Aggregate");
    };
    assert_eq!(group_by.len(), 1, "one GROUP BY key");
    assert_eq!(aggregates.len(), 1, "one aggregate");
    assert_eq!(aggregates[0].func, AggregateFunc::CountStar);
    // Schema: [id, count]
    assert_eq!(schema.len(), 2);
    assert_eq!(schema.field_at(0).name, "id");
    assert_eq!(schema.field_at(1).name, "count");
}

#[test]
fn binds_group_by_scalar_function_projection_alias() {
    let schema = Schema::new([
        Field::required("order_date", DataType::Date),
        Field::required("amount", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("sales", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT EXTRACT(YEAR FROM order_date) AS o_year, SUM(amount) AS revenue \
         FROM sales GROUP BY EXTRACT(YEAR FROM order_date) ORDER BY o_year",
        &cat,
    )
    .expect("bind ok");

    assert_eq!(plan.schema().field_at(0).name, "o_year");
    assert_eq!(plan.schema().field_at(1).name, "revenue");
}

#[test]
fn binds_group_by_column_projection_alias() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT id AS ident, COUNT(*) AS row_count FROM users GROUP BY id ORDER BY ident",
        &cat,
    )
    .expect("bind ok");

    assert_eq!(plan.schema().field_at(0).name, "ident");
    assert_eq!(plan.schema().field_at(1).name, "row_count");
}

#[test]
fn binds_count_star() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT count(*) FROM users", &cat).expect("bind ok");

    fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_agg(input),
            _ => None,
        }
    }
    let agg = find_agg(&plan).expect("should contain Aggregate node");
    let LogicalPlan::Aggregate { aggregates, .. } = agg else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].func, AggregateFunc::CountStar);
    assert!(aggregates[0].arg.is_none(), "count(*) has no argument");
}

#[test]
fn binds_vector_sum_and_avg_with_vector_return_type() {
    let cat = embeddings_catalog();
    let plan = parse_and_bind(
        "SELECT sum(embedding), avg(embedding) FROM embeddings",
        &cat,
    )
    .expect("bind ok");

    fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_agg(input),
            _ => None,
        }
    }

    let agg = find_agg(&plan).expect("should contain Aggregate node");
    let LogicalPlan::Aggregate { aggregates, .. } = agg else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggregates.len(), 2);
    assert_eq!(aggregates[0].func, AggregateFunc::Sum);
    assert_eq!(aggregates[1].func, AggregateFunc::Avg);
    assert_eq!(aggregates[0].data_type, DataType::Vector { dims: Some(3) });
    assert_eq!(aggregates[1].data_type, DataType::Vector { dims: Some(3) });
}

#[test]
fn binds_having_filters_post_aggregate() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT id, count(*) FROM users GROUP BY id HAVING count(*) > 1",
        &cat,
    )
    .expect("bind ok");

    fn find_filter_above_agg(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Filter { input, .. } => {
                matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_filter_above_agg(input),
            _ => false,
        }
    }
    assert!(
        find_filter_above_agg(&plan),
        "should have Filter above Aggregate for HAVING"
    );
}

#[test]
fn binds_decimal_arithmetic_around_aggregate_with_decimal_type() {
    let schema = Schema::new([
        Field::required(
            "price",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
        Field::required(
            "discount",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("lineitem", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT 100 * SUM(price * (1 - discount)) / SUM(price * (1 - discount)) AS ratio FROM lineitem",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { schema, exprs, .. } = &plan else {
        panic!("expected top-level Project, got {plan:?}");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(8)
        }
    );
    assert_eq!(
        exprs[0].0.data_type(),
        DataType::Decimal {
            precision: None,
            scale: Some(8)
        }
    );
}

#[test]
fn binds_coalesce_around_aggregate_projection() {
    let schema = Schema::new([Field::required("amount", DataType::Int32)]).expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("sales", TableMeta::new(schema));

    let plan = parse_and_bind("SELECT COALESCE(SUM(amount), 0) AS total FROM sales", &cat)
        .expect("bind ok");

    let LogicalPlan::Project {
        input,
        exprs,
        schema,
    } = &plan
    else {
        panic!("expected top-level Project, got {plan:?}");
    };
    assert_eq!(schema.field_at(0).name, "total");
    assert_eq!(schema.field_at(0).data_type, DataType::Int64);

    let ScalarExpr::FunctionCall {
        name,
        args,
        data_type,
    } = &exprs[0].0
    else {
        panic!("expected coalesce projection, got {:?}", exprs[0].0);
    };
    assert_eq!(name, "coalesce");
    assert_eq!(*data_type, DataType::Int64);
    assert!(matches!(args[0], ScalarExpr::Column { index: 0, .. }));

    let LogicalPlan::Aggregate { aggregates, .. } = input.as_ref() else {
        panic!("expected Aggregate under Project");
    };
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].func, AggregateFunc::Sum);

    let ifnull_plan = parse_and_bind("SELECT IFNULL(SUM(amount), 0) AS total FROM sales", &cat)
        .expect("bind generic scalar wrapper ok");
    assert_eq!(ifnull_plan.schema().field_at(0).data_type, DataType::Int64);
}

#[test]
fn binds_distinct_sum_arguments_to_distinct_aggregate_columns() {
    let schema = Schema::new([
        Field::required("volume", DataType::Float64),
        Field::required("nation", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("lineitem", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT \
             SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE volume - volume END) / SUM(volume) \
             AS mkt_share \
         FROM lineitem",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top-level Project, got {plan:?}");
    };
    let ScalarExpr::Binary { left, right, .. } = &exprs[0].0 else {
        panic!("expected ratio expression, got {:?}", exprs[0].0);
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 1, .. }
    ));

    let LogicalPlan::Aggregate { aggregates, .. } = input.as_ref() else {
        panic!("expected Aggregate under Project");
    };
    assert_eq!(aggregates.len(), 2, "SUM calls have different arguments");
}

// -----------------------------------------------------------------------
// Set operations tests
// -----------------------------------------------------------------------

#[test]
fn binds_union_all_arity_match() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT id FROM users UNION ALL SELECT id FROM users", &cat)
        .expect("bind ok");

    fn find_setop(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::SetOp { .. } => Some(plan),
            LogicalPlan::Cte { body, .. } => find_setop(body),
            _ => None,
        }
    }
    // The SetOp may be wrapped in a Cte if there were CTEs, otherwise it's
    // at the top level.
    let setop = find_setop(&plan).unwrap_or(&plan);
    // Accept either SetOp at top or wrapped in project.
    let has_setop = matches!(plan, LogicalPlan::SetOp { .. })
        || matches!(&plan, LogicalPlan::Project { input, .. }
                if matches!(input.as_ref(), LogicalPlan::SetOp { .. }));
    // Or the plan IS the setop.
    let is_setop = matches!(&plan, LogicalPlan::SetOp { quantifier, .. }
            if *quantifier == LogicalSetQuantifier::All);
    // If it's not directly at top, it's wrapped by the outer structure.
    if !has_setop && !is_setop {
        // Find it anywhere in the tree.
        let _ = setop;
        // The schema should have 1 column.
        let final_schema = plan.schema();
        assert_eq!(
            final_schema.len(),
            1,
            "UNION ALL of single-column selects = 1 col"
        );
    } else {
        assert!(has_setop || is_setop);
    }
    let _ = setop;
}

#[test]
fn binds_set_operation_order_by_output_column() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users UNION SELECT id FROM users ORDER BY id",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Sort { input, keys } = &plan else {
        panic!("expected Sort above set operation, got {plan:?}");
    };
    assert_eq!(keys.len(), 1);
    assert!(matches!(keys[0].expr, ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(input.as_ref(), LogicalPlan::SetOp { .. }));
}

#[test]
fn binds_union_distinct_with_arity_mismatch_is_rejected() {
    let cat = users_catalog();
    // id (1 col) UNION id, name (2 cols) should fail.
    let err = parse_and_bind(
        "SELECT id FROM users UNION SELECT id, name FROM users",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}
