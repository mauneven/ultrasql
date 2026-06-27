//! Binder tests for JOINs: ON/USING/NATURAL predicates, outer-join
//! nullability, cross joins, join-depth limits and PIVOT/UNPIVOT.

use ultrasql_core::{DataType, Field, Schema, Value};

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};

// JOIN tests
// -----------------------------------------------------------------------

/// Build a two-table catalog: users (`id` INT, `name` TEXT) and orders (`oid` INT, `user_id` INT).
fn duplicate_id_catalog() -> InMemoryCatalog {
    let schema_a = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("marker", DataType::Int32),
    ])
    .expect("schema ok");
    let schema_b = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("marker", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("a", TableMeta::new(schema_a));
    cat.register("b", TableMeta::new(schema_b));
    cat
}

#[test]
fn binds_inner_join_with_on_predicate() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users INNER JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");
    // The top-level plan has a Project; find the Join underneath.
    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join node");
    let LogicalPlan::Join {
        join_type,
        condition,
        schema,
        ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::Inner);
    assert!(
        matches!(condition, LogicalJoinCondition::On(_)),
        "expected ON condition"
    );
    // Schema is concatenation: users(id, name) + orders(oid, user_id) = 4
    assert_eq!(schema.len(), 4, "join schema width should be 4");
}

#[test]
fn qualified_join_predicate_resolves_duplicate_right_column() {
    let cat = duplicate_id_catalog();
    let plan = parse_and_bind(
        "SELECT a.id AS aid, b.id AS bid \
         FROM a JOIN b ON a.id = b.id \
         ORDER BY b.id",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top Project, got {plan:?}");
    };
    assert!(matches!(&exprs[0].0, ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(&exprs[1].0, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Sort { input, keys } = input.as_ref() else {
        panic!("expected Sort under Project");
    };
    assert!(matches!(&keys[0].expr, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Join { condition, .. } = input.as_ref() else {
        panic!("expected Join under Sort");
    };
    let LogicalJoinCondition::On(ScalarExpr::Binary { left, right, .. }) = condition else {
        panic!("expected binary ON predicate, got {condition:?}");
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 2, .. }
    ));
}

#[test]
fn order_by_can_reference_projection_alias() {
    let cat = users_catalog();
    let plan =
        parse_and_bind("SELECT id AS ident FROM users ORDER BY ident DESC", &cat).expect("bind ok");

    // A bare ORDER BY name prefers the SELECT-list output alias (`ident`), which
    // maps to the already-bound projection expression `id` (input index 0). The
    // projection is order-preserving, so the Sort sits *below* it sorting the
    // input by `id` DESC: `Project(Sort(Scan))`.
    let LogicalPlan::Project { input, .. } = &plan else {
        panic!("expected Project over Sort, got {plan:?}");
    };
    let LogicalPlan::Sort { keys, .. } = input.as_ref() else {
        panic!("expected Sort below Project, got {input:?}");
    };
    assert_eq!(keys.len(), 1);
    assert!(!keys[0].asc, "ident DESC");
    assert!(
        matches!(&keys[0].expr, ScalarExpr::Column { index: 0, .. }),
        "alias `ident` resolves to projected column `id` (input index 0), got {:?}",
        keys[0].expr
    );
}

/// Catalog with one table `t(a INT, b INT)` for the bare-name ORDER BY tests.
fn ab_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("t", TableMeta::new(schema));
    cat
}

/// The single sort key of a `Project(Sort(..))` or bare `Sort(..)` plan.
fn only_sort_key(plan: &LogicalPlan) -> &ScalarExpr {
    let sort = match plan {
        LogicalPlan::Sort { .. } => plan,
        LogicalPlan::Project { input, .. } => input.as_ref(),
        other => panic!("expected Sort or Project(Sort), got {other:?}"),
    };
    let LogicalPlan::Sort { keys, .. } = sort else {
        panic!("expected Sort, got {sort:?}");
    };
    assert_eq!(keys.len(), 1, "exactly one sort key");
    &keys[0].expr
}

#[test]
fn order_by_bare_name_prefers_output_alias_over_input_column() {
    // PG: `SELECT name AS id … ORDER BY id` sorts by the OUTPUT alias `id`
    // (= the `name` column), NOT the input `users.id`. The projected expr for
    // `name AS id` is the `name` column (input index 1).
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT name AS id FROM users ORDER BY id", &cat).expect("bind ok");
    assert!(
        matches!(only_sort_key(&plan), ScalarExpr::Column { index: 1, .. }),
        "ORDER BY id must sort by output alias `id` = name (input index 1), got {:?}",
        only_sort_key(&plan)
    );
}

#[test]
fn order_by_bare_name_falls_back_to_input_when_not_an_output_alias() {
    // PG: `SELECT a AS x … ORDER BY a` — `a` is not an output name, so it
    // resolves to the input column `a` (index 0).
    let cat = ab_catalog();
    let plan = parse_and_bind("SELECT a AS x FROM t ORDER BY a", &cat).expect("bind ok");
    assert!(
        matches!(only_sort_key(&plan), ScalarExpr::Column { index: 0, .. }),
        "ORDER BY a must fall back to input column a (index 0), got {:?}",
        only_sort_key(&plan)
    );
}

#[test]
fn order_by_bare_name_matching_output_alias_uses_projection_expr() {
    // PG: `SELECT a+0 AS a … ORDER BY a` sorts by the OUTPUT `a` (= a+0), not by
    // a re-resolved input column. The sort key is the projection expression
    // (a Binary Add), proving output-alias resolution rather than input binding.
    let cat = ab_catalog();
    let plan = parse_and_bind("SELECT a+0 AS a FROM t ORDER BY a", &cat).expect("bind ok");
    assert!(
        matches!(only_sort_key(&plan), ScalarExpr::Binary { .. }),
        "ORDER BY a must reuse the projected `a+0` expression, got {:?}",
        only_sort_key(&plan)
    );
}

#[test]
fn order_by_expression_resolves_against_input_not_output_alias() {
    // PG: an ORDER BY *expression* (not a bare name) always uses input columns,
    // even when it would name-match an output alias. `a+1` uses input `a`.
    let cat = ab_catalog();
    let plan = parse_and_bind("SELECT a+1 AS a FROM t ORDER BY a+1", &cat).expect("bind ok");
    // The key is `a+1` over the input column a (index 0), independent of the
    // output alias `a`.
    let ScalarExpr::Binary { left, .. } = only_sort_key(&plan) else {
        panic!("expected Binary sort key");
    };
    assert!(
        matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }),
        "ORDER BY a+1 must reference input column a (index 0), got {left:?}"
    );
}

#[test]
fn order_by_qualified_name_resolves_against_input() {
    // A qualified name `t.a` is not a bare name, so it never prefers the output
    // alias; it binds to the input column.
    let cat = ab_catalog();
    let plan = parse_and_bind("SELECT b AS a FROM t ORDER BY t.a", &cat).expect("bind ok");
    assert!(
        matches!(only_sort_key(&plan), ScalarExpr::Column { index: 0, .. }),
        "ORDER BY t.a must resolve to input column a (index 0), got {:?}",
        only_sort_key(&plan)
    );
}

#[test]
fn order_by_bare_name_ambiguous_across_two_output_aliases() {
    // PG: two output columns named `a` → `ORDER BY "a" is ambiguous` (42702).
    let cat = ab_catalog();
    let err = parse_and_bind("SELECT a AS a, b AS a FROM t ORDER BY a", &cat)
        .expect_err("two output columns named `a` make ORDER BY a ambiguous");
    assert!(
        matches!(err, PlanError::Ambiguous(_)),
        "expected Ambiguous (42702), got {err:?}"
    );
}

#[test]
fn binds_duplicate_unaliased_function_output_labels() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT pg_get_expr(1, 1), pg_get_expr(2, 2) FROM users",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(schema.field_at(0).name, "pg_get_expr");
    assert_eq!(schema.field_at(1).name, "pg_get_expr");
}

#[test]
fn qualified_where_predicate_resolves_duplicate_right_column() {
    let cat = duplicate_id_catalog();
    let plan = parse_and_bind("SELECT b.id FROM a, b WHERE a.id = b.id", &cat).expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top Project, got {plan:?}");
    };
    assert!(matches!(&exprs[0].0, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected Filter under Project");
    };
    let ScalarExpr::Binary { left, right, .. } = predicate else {
        panic!("expected binary WHERE predicate, got {predicate:?}");
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 2, .. }
    ));
    assert!(matches!(input.as_ref(), LogicalPlan::Join { .. }));
}

#[test]
fn binds_left_outer_join_makes_right_columns_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users LEFT JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::LeftOuter);
    // Left columns (users.id, users.name): id was required, stays required.
    assert!(
        !schema.field_at(0).nullable,
        "left.id should remain required"
    );
    // Right columns (orders.oid, orders.user_id) should be nullable in LEFT JOIN.
    assert!(schema.field_at(2).nullable, "right.oid should be nullable");
    assert!(
        schema.field_at(3).nullable,
        "right.user_id should be nullable"
    );
}

#[test]
fn binds_right_outer_join_makes_left_columns_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::RightOuter);
    // In RIGHT JOIN: left columns become nullable.
    assert!(
        schema.field_at(0).nullable,
        "left.id should be nullable in RIGHT JOIN"
    );
    // Right columns keep their original nullability (both were required).
    assert!(!schema.field_at(2).nullable, "right.oid stays required");
}

#[test]
fn binds_full_outer_join_makes_both_sides_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::FullOuter);
    // Both sides should be nullable.
    assert!(
        schema.field_at(0).nullable,
        "left.id should be nullable in FULL OUTER JOIN"
    );
    assert!(
        schema.field_at(2).nullable,
        "right.oid should be nullable in FULL OUTER JOIN"
    );
}

#[test]
fn binds_cross_join_has_no_predicate() {
    let cat = two_table_catalog();
    let plan =
        parse_and_bind("SELECT users.id FROM users CROSS JOIN orders", &cat).expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type,
        condition,
        ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::Cross);
    assert!(
        matches!(condition, LogicalJoinCondition::None),
        "cross join should have no condition"
    );
}

fn join_chain_catalog(table_count: usize) -> InMemoryCatalog {
    let mut cat = InMemoryCatalog::new();
    let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
    for idx in 0..table_count {
        cat.register(&format!("t{idx}"), TableMeta::new(schema.clone()));
    }
    cat
}

fn join_chain_sql(table_count: usize) -> String {
    let mut sql = String::from("SELECT t0.id FROM t0");
    for idx in 1..table_count {
        sql.push_str(&format!(" JOIN t{idx} ON t0.id = t{idx}.id"));
    }
    sql
}

#[test]
fn accepts_explicit_join_chain_at_depth_limit() {
    let cat = join_chain_catalog(65);
    let sql = join_chain_sql(65);

    parse_and_bind(&sql, &cat).expect("join depth at planner limit should bind");
}

#[test]
fn rejects_explicit_join_chain_above_depth_limit() {
    let cat = join_chain_catalog(66);
    let sql = join_chain_sql(66);

    let err = parse_and_bind(&sql, &cat).expect_err("join chain should exceed planner limit");

    assert!(
        err.to_string().contains("join depth"),
        "expected join-depth error, got {err:?}"
    );
}

#[test]
fn binds_using_join_folds_to_equality_and_collapses_columns() {
    // Build a catalog where both tables have a column named `id`.
    let schema_a = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
    let schema_b = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("val", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("a", TableMeta::new(schema_a));
    cat.register("b", TableMeta::new(schema_b));

    let plan = parse_and_bind("SELECT a.id FROM a JOIN b USING (id)", &cat).expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        condition, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert!(
        matches!(condition, LogicalJoinCondition::Using(_)),
        "expected USING condition"
    );
    // USING(id) collapses: id once + val = 2 columns (not 3).
    assert_eq!(
        schema.len(),
        2,
        "USING join should collapse the shared column"
    );
}

#[test]
fn binds_natural_join_collapses_shared_columns_without_ambiguous_select() {
    let schema_a = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("left_name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let schema_b = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("right_name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("a", TableMeta::new(schema_a));
    cat.register("b", TableMeta::new(schema_b));

    let plan = parse_and_bind(
        "SELECT id, left_name, right_name FROM a NATURAL JOIN b",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        condition, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert!(
        matches!(condition, LogicalJoinCondition::Using(pairs) if pairs.as_slice() == [(0, 0)]),
        "natural join should bind as USING(id)"
    );
    assert_eq!(schema.len(), 3);
    assert_eq!(schema.field_at(0).name, "id");
    assert_eq!(schema.field_at(1).name, "left_name");
    assert_eq!(schema.field_at(2).name, "right_name");
}

/// Three tables `t1(x, a)`, `t2(x, b)`, `t3(x, c)` — distinct non-join columns
/// so that only the join column `x` is shared (mirrors PostgreSQL's repro for
/// the USING/NATURAL "appears more than once" error).
fn three_x_catalog() -> InMemoryCatalog {
    let mut cat = InMemoryCatalog::new();
    for (name, other) in [("t1", "a"), ("t2", "b"), ("t3", "c")] {
        let schema = Schema::new([
            Field::required("x", DataType::Int32),
            Field::required(other, DataType::Int32),
        ])
        .expect("schema ok");
        cat.register(name, TableMeta::new(schema));
    }
    cat
}

#[test]
fn using_join_column_duplicated_on_left_is_ambiguous() {
    // `(t1 CROSS JOIN t2)` has two `x` columns; USING (x) against t3 is 42702.
    let cat = three_x_catalog();
    let err = parse_and_bind("SELECT * FROM (t1 CROSS JOIN t2) JOIN t3 USING (x)", &cat)
        .expect_err("duplicated join column on the left must be rejected");
    assert!(
        matches!(err, PlanError::AmbiguousJoinColumn(_)),
        "expected AmbiguousJoinColumn (SQLSTATE 42702), got {err:?}"
    );
    assert_eq!(
        err.to_string(),
        "common column name \"x\" appears more than once in left table"
    );
}

#[test]
fn using_join_column_duplicated_on_right_is_ambiguous() {
    let cat = three_x_catalog();
    let err = parse_and_bind("SELECT * FROM t3 JOIN (t1 CROSS JOIN t2) USING (x)", &cat)
        .expect_err("duplicated join column on the right must be rejected");
    assert!(
        matches!(err, PlanError::AmbiguousJoinColumn(_)),
        "expected AmbiguousJoinColumn, got {err:?}"
    );
    assert_eq!(
        err.to_string(),
        "common column name \"x\" appears more than once in right table"
    );
}

#[test]
fn natural_join_column_duplicated_on_one_side_is_ambiguous() {
    let cat = three_x_catalog();
    let err = parse_and_bind("SELECT * FROM (t1 CROSS JOIN t2) NATURAL JOIN t3", &cat)
        .expect_err("NATURAL join with a duplicated common column must be rejected");
    assert!(
        matches!(err, PlanError::AmbiguousJoinColumn(_)),
        "expected AmbiguousJoinColumn, got {err:?}"
    );
    assert_eq!(
        err.to_string(),
        "common column name \"x\" appears more than once in left table"
    );
}

#[test]
fn using_join_with_unique_columns_still_binds() {
    // No duplicated common column — the normal path is unaffected.
    let cat = three_x_catalog();
    let plan =
        parse_and_bind("SELECT * FROM t1 JOIN t3 USING (x)", &cat).expect("unique USING binds");
    // Output: merged x, then t1.a, then t3.c = 3 columns.
    assert_eq!(plan.schema().len(), 3);
}

#[test]
fn binds_pivot_table_factor_schema_and_keys() {
    let cat = sales_pivot_catalog();
    let plan = parse_and_bind(
        "SELECT * FROM sales \
         PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2))",
        &cat,
    )
    .expect("bind ok");

    assert_eq!(
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["region", "q1", "q2"]
    );
    assert_eq!(plan.schema().field_at(1).data_type, DataType::Int64);

    fn find_pivot(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Pivot { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_pivot(input),
            _ => None,
        }
    }
    let pivot = find_pivot(&plan).expect("should contain Pivot");
    let LogicalPlan::Pivot {
        group_columns,
        pivot_column,
        aggregate,
        pivot_values,
        ..
    } = pivot
    else {
        panic!("expected Pivot");
    };
    assert_eq!(group_columns, &[0]);
    assert_eq!(*pivot_column, 1);
    assert_eq!(aggregate.func, AggregateFunc::Sum);
    assert!(aggregate.arg.is_some());
    assert_eq!(pivot_values.len(), 2);
    assert_eq!(pivot_values[0].output_name, "q1");
}

#[test]
fn pivot_values_coerce_to_pivot_column_type() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT * FROM users PIVOT (COUNT(*) FOR score IN (1 AS one))",
        &cat,
    )
    .expect("bind ok");

    fn find_pivot(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Pivot { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_pivot(input),
            _ => None,
        }
    }
    let pivot = find_pivot(&plan).expect("should contain Pivot");
    let LogicalPlan::Pivot { pivot_values, .. } = pivot else {
        panic!("expected Pivot");
    };
    assert_eq!(pivot_values[0].value, Value::Float64(1.0));
    assert_eq!(pivot_values[0].data_type, DataType::Float64);
}

#[test]
fn pivot_values_that_cannot_be_coerced_fail_fast() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "SELECT * FROM users PIVOT (COUNT(*) FOR id IN (1.5 AS bad))",
        &cat,
    )
    .expect_err("fractional pivot value should not match integer column");
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    assert!(err.to_string().contains("cannot be coerced"));
}

#[test]
fn binds_unpivot_table_factor_schema_and_columns() {
    let cat = quarterly_unpivot_catalog();
    let plan = parse_and_bind(
        "SELECT * FROM quarterly \
         UNPIVOT INCLUDE NULLS (amount FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2'))",
        &cat,
    )
    .expect("bind ok");

    assert_eq!(
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "quarter", "amount"]
    );
    assert_eq!(
        plan.schema().field_at(1).data_type,
        DataType::Text { max_len: None }
    );
    assert_eq!(plan.schema().field_at(2).data_type, DataType::Int32);

    fn find_unpivot(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Unpivot { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_unpivot(input),
            _ => None,
        }
    }
    let unpivot = find_unpivot(&plan).expect("should contain Unpivot");
    let LogicalPlan::Unpivot {
        passthrough_columns,
        columns,
        include_nulls,
        ..
    } = unpivot
    else {
        panic!("expected Unpivot");
    };
    assert_eq!(passthrough_columns, &[0]);
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].source_column, 1);
    assert_eq!(columns[0].label, "Q1");
    assert!(*include_nulls);
}

#[test]
fn pivot_duplicate_output_names_are_rejected() {
    let cat = sales_pivot_catalog();
    let err = parse_and_bind(
        "SELECT * FROM sales \
         PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q, 'Q2' AS q))",
        &cat,
    )
    .expect_err("duplicate pivot outputs");
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    assert!(err.to_string().contains("duplicate PIVOT output column"));
}

#[test]
fn pivot_duplicate_values_are_rejected() {
    let cat = sales_pivot_catalog();
    let err = parse_and_bind(
        "SELECT * FROM sales \
         PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q1' AS q1_again))",
        &cat,
    )
    .expect_err("duplicate pivot values");
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    assert!(err.to_string().contains("duplicate PIVOT value Q1"));
}

#[test]
fn pivot_sum_requires_supported_numeric_argument() {
    let cat = sales_pivot_catalog();
    let err = parse_and_bind(
        "SELECT * FROM sales \
         PIVOT (SUM(region) FOR quarter IN ('Q1' AS q1))",
        &cat,
    )
    .expect_err("text pivot SUM should be rejected");
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    assert!(err.to_string().contains("PIVOT Sum argument must be"));
}

#[test]
fn unpivot_missing_source_column_is_rejected() {
    let cat = quarterly_unpivot_catalog();
    let err = parse_and_bind(
        "SELECT * FROM quarterly UNPIVOT (amount FOR quarter IN (q1, q3))",
        &cat,
    )
    .expect_err("missing unpivot column");
    assert_eq!(err, PlanError::ColumnNotFound("q3".to_owned()));
}
