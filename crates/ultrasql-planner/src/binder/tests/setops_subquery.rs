//! Binder tests for set operations, CTEs, SELECT * wildcard expansion and
//! correlated/uncorrelated subqueries.

use ultrasql_core::{DataType, Field, Schema};

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};

// -----------------------------------------------------------------------
// CTE tests
// -----------------------------------------------------------------------

#[test]
fn binds_cte_then_references_it_in_body() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "WITH active AS (SELECT id FROM users) SELECT id FROM active",
        &cat,
    )
    .expect("bind ok");

    // Top-level plan should be a Cte node.
    let LogicalPlan::Cte {
        name, recursive, ..
    } = &plan
    else {
        panic!("expected Cte at top, got {plan:?}");
    };
    assert_eq!(name, "active");
    assert!(!recursive, "non-recursive CTE should have recursive=false");
}

#[test]
fn binds_scalar_subquery_that_references_outer_cte() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "WITH revenue AS (SELECT id, score FROM users) SELECT id FROM revenue WHERE score = (SELECT MAX(score) FROM revenue)",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Cte { body, .. } = &plan else {
        panic!("expected Cte at top, got {plan:?}");
    };

    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }

    let filter = find_filter(body).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Binary { right, .. } = predicate else {
        panic!("expected binary predicate, got {predicate:?}");
    };
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::ScalarSubquery {
            correlated: false,
            ..
        }
    ));
}

// -----------------------------------------------------------------------
// SELECT * wildcard tests
// -----------------------------------------------------------------------

#[test]
fn binds_select_star_expands_via_catalog() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT * FROM users", &cat).expect("bind ok");
    let LogicalPlan::Project { schema, exprs, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // users has id, name, score = 3 columns
    assert_eq!(schema.len(), 3, "SELECT * should expand to 3 columns");
    assert_eq!(exprs.len(), 3);
}

#[test]
fn binds_qualified_wildcard_restricts_to_table_alias() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::Project { schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // users u has 2 columns; u.* should expand to those 2 only.
    assert_eq!(schema.len(), 2, "u.* should expand to users' 2 columns");
}

// -----------------------------------------------------------------------
// Error / unsupported
// -----------------------------------------------------------------------

#[test]
fn binder_rejects_unknown_aggregate_with_not_supported() {
    let cat = users_catalog();
    // `mode` is not a known aggregate; the binder should reject it.
    let err = parse_and_bind("SELECT mode(score) FROM users GROUP BY id", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::NotSupported(_)),
        "unknown aggregate should be NotSupported, got {err:?}"
    );
}

// -----------------------------------------------------------------------
// Property test
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Subquery tests
// -----------------------------------------------------------------------

/// A two-table catalog: `users (id INT, name TEXT, score FLOAT8)`
/// and `orders (oid INT, user_id INT)`.
fn subquery_catalog() -> InMemoryCatalog {
    let users_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema ok");
    let orders_schema = Schema::new([
        Field::required("oid", DataType::Int32),
        Field::required("user_id", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(users_schema));
    cat.register("orders", TableMeta::new(orders_schema));
    cat
}

#[test]
fn binds_uncorrelated_exists_subquery() {
    // `EXISTS (SELECT oid FROM orders)` has no outer column references.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders)",
        &cat,
    )
    .expect("bind ok");
    // Walk to the Filter and check its predicate.
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Exists {
        negated,
        correlated,
        ..
    } = predicate
    else {
        panic!("expected Exists predicate, got {predicate:?}");
    };
    assert!(!negated, "should not be negated");
    assert!(!correlated, "no outer column reference → uncorrelated");
}

#[test]
fn binds_correlated_exists_subquery() {
    // `EXISTS (SELECT oid FROM orders WHERE user_id = id)` — `id` is not in
    // `orders`, so it resolves to the outer `users.id`.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders WHERE user_id = id)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Exists { correlated, .. } = predicate else {
        panic!("expected Exists, got {predicate:?}");
    };
    assert!(correlated, "id resolves to outer users.id → correlated");
}

#[test]
fn binds_in_subquery_arity_1_check_rejects_multi_column() {
    // `id IN (SELECT oid, user_id FROM orders)` — 2-column subquery must fail.
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id FROM users WHERE id IN (SELECT oid, user_id FROM orders)",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "multi-column IN subquery should be TypeMismatch, got {err:?}"
    );
}

#[test]
fn binds_scalar_subquery_returns_scalar_subquery_expr() {
    // `(SELECT oid FROM orders LIMIT 1)` used as a scalar in the projection.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id, (SELECT oid FROM orders LIMIT 1) FROM users",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::Project { exprs, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // Second expression should be a ScalarSubquery.
    let (second_expr, _) = &exprs[1];
    assert!(
        matches!(
            second_expr,
            ScalarExpr::ScalarSubquery {
                correlated: false,
                ..
            }
        ),
        "expected uncorrelated ScalarSubquery, got {second_expr:?}"
    );
}

#[test]
fn binds_not_in_subquery() {
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::InSubquery { negated, .. } = predicate else {
        panic!("expected InSubquery, got {predicate:?}");
    };
    assert!(negated, "NOT IN should produce negated=true");
}

#[test]
fn binds_any_eq_lowers_to_exists() {
    // `id = ANY (SELECT user_id FROM orders)` should bind as InSubquery with
    // negated=false (the same representation as `id IN (…)`).
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE id = ANY (SELECT user_id FROM orders)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    assert!(
        matches!(predicate, ScalarExpr::InSubquery { negated: false, .. }),
        "= ANY should lower to InSubquery(negated=false), got {predicate:?}"
    );
}

#[test]
fn binds_any_with_lt_returns_not_supported() {
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id FROM users WHERE id < ANY (SELECT user_id FROM orders)",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::NotSupported(_)),
        "< ANY should be NotSupported, got {err:?}"
    );
}

#[test]
fn binder_rejects_scalar_subquery_with_multi_column_projection() {
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id, (SELECT oid, user_id FROM orders LIMIT 1) FROM users",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "multi-column scalar subquery should be TypeMismatch, got {err:?}"
    );
}

#[test]
fn outer_column_correctly_tracks_frame_depth_in_nested_subquery() {
    // Outer query scans `users`.  The subquery scans `orders`.  Inside the
    // subquery's WHERE, `id` is not in `orders` so it should resolve as
    // `OuterColumn { frame_depth: 1, … }`.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders WHERE user_id = id)",
        &cat,
    )
    .expect("bind ok");
    // Navigate to the Exists predicate's inner plan.
    fn find_exists_pred(plan: &LogicalPlan) -> Option<&ScalarExpr> {
        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                if matches!(predicate, ScalarExpr::Exists { .. }) {
                    Some(predicate)
                } else {
                    None
                }
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_exists_pred(input),
            _ => None,
        }
    }
    let pred = find_exists_pred(&plan).expect("should find Exists predicate");
    let ScalarExpr::Exists { subplan, .. } = pred else {
        panic!("expected Exists");
    };
    // The inner plan should have a Filter with an outer-column reference.
    fn find_outer_col(plan: &LogicalPlan) -> Option<usize> {
        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                // Predicate is `user_id = id` — a Binary with the right side
                // being an OuterColumn.
                if let ScalarExpr::Binary { right, .. } = predicate {
                    if let ScalarExpr::OuterColumn { frame_depth, .. } = right.as_ref() {
                        return Some(*frame_depth);
                    }
                }
                None
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_outer_col(input),
            _ => None,
        }
    }
    let depth = find_outer_col(subplan).expect("should find OuterColumn in subplan");
    assert_eq!(depth, 1, "column is one level out → frame_depth = 1");
}

// -----------------------------------------------------------------------
// Mixed-width set-op casting
// -----------------------------------------------------------------------

/// A one-table catalog: `mix (a INT, b BIGINT, c INT)`.
fn mixed_width_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Int64),
        Field::required("c", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("mix", TableMeta::new(schema));
    cat
}

/// `true` if `plan` is a `Project` whose output schema column `col` has
/// type `ty` and whose expression for that column is a runtime cast call.
fn project_casts_col_to(plan: &LogicalPlan, col: usize, ty: &DataType, cast_fn: &str) -> bool {
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        return false;
    };
    schema.fields()[col].data_type == *ty
        && matches!(&exprs[col].0, ScalarExpr::FunctionCall { name, .. } if name == cast_fn)
}

/// When the two sides carry corresponding columns of different width, the
/// binder wraps the narrower side in a `Project` that casts it to the
/// unified type. (Each SELECT body is itself already a `Project` from the
/// projection list, so the cast lands in an *additional* outer Project.)
#[test]
fn set_op_mixed_width_inserts_cast_project() {
    let cat = mixed_width_catalog();
    let plan = parse_and_bind("SELECT a FROM mix UNION SELECT b FROM mix", &cat).expect("bind ok");

    let LogicalPlan::SetOp {
        left,
        right,
        schema,
        ..
    } = &plan
    else {
        panic!("expected SetOp at top, got {plan:?}");
    };

    // Unified output type is bigint (int4 + int8 -> int8).
    assert_eq!(schema.fields()[0].data_type, DataType::Int64);

    // Left side (`a int4`) gains an outer Project casting column 0 to int8.
    assert!(
        project_casts_col_to(left, 0, &DataType::Int64, "__ultrasql_cast_int8"),
        "narrower side must be wrapped in a casting Project, got {left:?}"
    );

    // Right side (`b int8`) already matches; its top node is the plain
    // projection Project whose column 0 is a bare Column (no cast).
    let LogicalPlan::Project { exprs, .. } = right.as_ref() else {
        panic!("expected projection Project on right, got {right:?}");
    };
    assert!(
        matches!(&exprs[0].0, ScalarExpr::Column { .. }),
        "matching side must not gain a cast, got {:?}",
        exprs[0].0
    );
}

/// Control: when both sides have the same width, no extra casting `Project`
/// is inserted on either side — each side's top node is the plain
/// projection whose column is a bare Column reference (no cast).
#[test]
fn set_op_same_width_inserts_no_cast_project() {
    let cat = mixed_width_catalog();
    let plan = parse_and_bind("SELECT a FROM mix UNION SELECT c FROM mix", &cat).expect("bind ok");

    let LogicalPlan::SetOp {
        left,
        right,
        schema,
        ..
    } = &plan
    else {
        panic!("expected SetOp at top, got {plan:?}");
    };

    assert_eq!(schema.fields()[0].data_type, DataType::Int32);
    for (label, side) in [("left", left), ("right", right)] {
        let LogicalPlan::Project { exprs, .. } = side.as_ref() else {
            panic!("expected projection Project on {label}, got {side:?}");
        };
        assert!(
            matches!(&exprs[0].0, ScalarExpr::Column { .. }),
            "{label} same-width side must not gain a cast, got {:?}",
            exprs[0].0
        );
    }
}

/// A three-column set op with mixed widths in only one position casts
/// exactly the column that differs and passes the rest through unchanged.
#[test]
fn set_op_three_column_mixed_positions_casts_each_differing_column() {
    let cat = mixed_width_catalog();
    // Left:  (a int4, c int4, c int4)
    // Right: (b int8, c int4, c int4)
    // Unified schema: (int8, int4, int4). Left needs a cast on column 0
    // only; columns 1 and 2 already match and pass through unchanged.
    let plan = parse_and_bind(
        "SELECT a, c, c FROM mix UNION SELECT b, c, c FROM mix",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::SetOp { left, schema, .. } = &plan else {
        panic!("expected SetOp, got {plan:?}");
    };
    assert_eq!(schema.fields()[0].data_type, DataType::Int64);
    assert_eq!(schema.fields()[1].data_type, DataType::Int32);
    assert_eq!(schema.fields()[2].data_type, DataType::Int32);

    // Left side casts only column 0 (a int4 -> int8); columns 1,2 pass
    // through as plain column references in the same casting Project.
    let LogicalPlan::Project { exprs, .. } = left.as_ref() else {
        panic!("expected casting Project on left, got {left:?}");
    };
    assert!(
        matches!(&exprs[0].0, ScalarExpr::FunctionCall { name, .. } if name == "__ultrasql_cast_int8"),
        "column 0 must be cast to int8, got {:?}",
        exprs[0].0
    );
    assert!(
        matches!(&exprs[1].0, ScalarExpr::Column { .. }),
        "column 1 already matches and must pass through, got {:?}",
        exprs[1].0
    );
    assert!(
        matches!(&exprs[2].0, ScalarExpr::Column { .. }),
        "column 2 already matches and must pass through, got {:?}",
        exprs[2].0
    );
}

/// The resolved set-op output type for `sql`'s first column.
fn setop_out_type(sql: &str) -> DataType {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(sql, &cat).expect("bind ok");
    let LogicalPlan::SetOp { schema, .. } = &plan else {
        panic!("expected SetOp, got {plan:?}");
    };
    schema.fields()[0].data_type.clone()
}

/// TEMPORAL supertype resolution: date/timestamp/timestamptz promote to the
/// widest member per the PG date/timestamp lattice.
#[test]
fn set_op_temporal_supertype_resolution() {
    assert_eq!(
        setop_out_type("SELECT date '2024-01-01' UNION SELECT timestamp '2024-01-01 00:00:00'"),
        DataType::Timestamp,
        "date + timestamp -> timestamp"
    );
    assert_eq!(
        setop_out_type(
            "SELECT date '2024-01-01' UNION SELECT timestamptz '2024-01-01 00:00:00+00'"
        ),
        DataType::TimestampTz,
        "date + timestamptz -> timestamptz"
    );
    assert_eq!(
        setop_out_type(
            "SELECT timestamp '2024-01-01 00:00:00' \
             UNION SELECT timestamptz '2024-01-01 00:00:00+00'"
        ),
        DataType::TimestampTz,
        "timestamp + timestamptz -> timestamptz"
    );
}

/// CHAINED temporal resolution: a 3-branch UNION mixing all three temporal
/// types resolves to `timestamptz` across the whole chain.
#[test]
fn set_op_chained_temporal_resolves_timestamptz() {
    assert_eq!(
        setop_out_type(
            "SELECT date '2024-01-01' \
             UNION SELECT timestamp '2024-06-01 12:00:00' \
             UNION SELECT timestamptz '2024-12-01 00:00:00+00'"
        ),
        DataType::TimestampTz,
        "chain date/timestamp/timestamptz -> timestamptz"
    );
}

/// STRING supertype resolution: char/varchar/text collapse to `text`.
#[test]
fn set_op_string_supertype_is_text() {
    assert_eq!(
        setop_out_type("SELECT 'abc'::char(3) UNION SELECT 'abc'::text"),
        DataType::Text { max_len: None },
        "char(n) + text -> text"
    );
    assert_eq!(
        setop_out_type("SELECT 'abc'::varchar(8) UNION SELECT 'abc'::char(3)"),
        DataType::Text { max_len: None },
        "varchar(n) + char(n) -> text"
    );
}

/// NETWORK supertype resolution: inet + cidr promote to `inet`, and the
/// cidr branch gains an inet cast Project.
#[test]
fn set_op_network_inet_cidr_resolves_inet() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "SELECT inet '10.0.0.0/8' UNION SELECT cidr '10.0.0.0/8'",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::SetOp { right, schema, .. } = &plan else {
        panic!("expected SetOp, got {plan:?}");
    };
    assert_eq!(schema.fields()[0].data_type, DataType::Inet);
    assert!(
        project_casts_col_to(right, 0, &DataType::Inet, "__ultrasql_cast_inet"),
        "cidr branch must be cast to inet, got {right:?}"
    );
}

/// NO-COMMON-TYPE: `int UNION date` shares no supertype, so the binder
/// rejects it with a `cannot be matched` TypeMismatch (SQLSTATE 42804).
#[test]
fn set_op_no_common_type_is_rejected() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("SELECT 1 UNION SELECT '2024-01-01'::date", &cat)
        .expect_err("int UNION date must be rejected");
    let PlanError::TypeMismatch(msg) = err else {
        panic!("expected TypeMismatch, got {err:?}");
    };
    assert!(
        msg.contains("cannot be matched"),
        "message must mirror PG, got {msg}"
    );
}

// -----------------------------------------------------------------------
// Set-operation precedence (INTERSECT binds tighter than UNION / EXCEPT)
// -----------------------------------------------------------------------

/// Render the SetOp plan tree as a fully-parenthesised string so a test can
/// assert the exact operator grouping. Each non-SetOp child (a bound SELECT
/// body) is rendered as `L`. Quantifiers are shown (`UNION ALL`) so
/// multiplicity-preserving folds are visible too.
fn setop_shape(plan: &LogicalPlan) -> String {
    let LogicalPlan::SetOp {
        op,
        quantifier,
        left,
        right,
        ..
    } = plan
    else {
        return "L".to_string();
    };
    let op_s = match op {
        LogicalSetOp::Union => "UNION",
        LogicalSetOp::Intersect => "INTERSECT",
        LogicalSetOp::Except => "EXCEPT",
    };
    let q_s = match quantifier {
        LogicalSetQuantifier::All => " ALL",
        LogicalSetQuantifier::Distinct => "",
    };
    format!(
        "({} {op_s}{q_s} {})",
        setop_shape(left),
        setop_shape(right)
    )
}

fn shape_of(sql: &str) -> String {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(sql, &cat).expect("bind ok");
    setop_shape(&plan)
}

/// The canonical bug: `UNION` then `INTERSECT` must bind as
/// `1 UNION (2 INTERSECT 2)`, never the naive left fold
/// `(1 UNION 2) INTERSECT 2` (which yields the wrong result {2}).
#[test]
fn intersect_binds_tighter_than_trailing_union() {
    assert_eq!(
        shape_of("SELECT 1 UNION SELECT 2 INTERSECT SELECT 2"),
        "(L UNION (L INTERSECT L))"
    );
}

/// A leading `INTERSECT` run forms the first UNION operand:
/// `1 INTERSECT 2 UNION 3` → `(1 INTERSECT 2) UNION 3`.
#[test]
fn leading_intersect_run_is_first_union_operand() {
    assert_eq!(
        shape_of("SELECT 1 INTERSECT SELECT 2 UNION SELECT 3"),
        "((L INTERSECT L) UNION L)"
    );
}

/// UNION and EXCEPT share one (lower) precedence and associate left-to-right:
/// `1 UNION 2 EXCEPT 3` → `(1 UNION 2) EXCEPT 3`.
#[test]
fn union_and_except_are_left_associative_at_equal_precedence() {
    assert_eq!(
        shape_of("SELECT 1 UNION SELECT 2 EXCEPT SELECT 3"),
        "((L UNION L) EXCEPT L)"
    );
}

/// EXCEPT also binds looser than INTERSECT:
/// `1 EXCEPT 1 INTERSECT 2` → `1 EXCEPT (1 INTERSECT 2)`.
#[test]
fn intersect_binds_tighter_than_except() {
    assert_eq!(
        shape_of("SELECT 1 EXCEPT SELECT 1 INTERSECT SELECT 2"),
        "(L EXCEPT (L INTERSECT L))"
    );
}

/// INTERSECT itself is left-associative among its own chain:
/// `1 INTERSECT 2 INTERSECT 3` → `(1 INTERSECT 2) INTERSECT 3`.
#[test]
fn intersect_chain_is_left_associative() {
    assert_eq!(
        shape_of("SELECT 1 INTERSECT SELECT 2 INTERSECT SELECT 3"),
        "((L INTERSECT L) INTERSECT L)"
    );
}

/// Mixed chain: each INTERSECT run is grouped, then the UNION/EXCEPT operators
/// fold left-to-right. `1 INTERSECT 2 UNION 3 INTERSECT 4` →
/// `(1 INTERSECT 2) UNION (3 INTERSECT 4)`.
#[test]
fn mixed_chain_groups_intersect_runs_then_folds_union_left() {
    assert_eq!(
        shape_of(
            "SELECT 1 INTERSECT SELECT 2 UNION SELECT 3 INTERSECT SELECT 4"
        ),
        "((L INTERSECT L) UNION (L INTERSECT L))"
    );
}

/// Interior INTERSECT run between two UNIONs:
/// `1 UNION 2 INTERSECT 3 UNION 4` → `(1 UNION (2 INTERSECT 3)) UNION 4`.
#[test]
fn interior_intersect_run_is_grouped_inside_union_chain() {
    assert_eq!(
        shape_of(
            "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3 UNION SELECT 4"
        ),
        "((L UNION (L INTERSECT L)) UNION L)"
    );
}

/// Quantifiers ride along with their operator through precedence folding, so
/// `UNION ALL` / `INTERSECT ALL` multiplicity is preserved in the right
/// branch: `1 UNION ALL 1 INTERSECT ALL 1` →
/// `1 UNION ALL (1 INTERSECT ALL 1)`.
#[test]
fn all_quantifier_is_preserved_through_precedence_fold() {
    assert_eq!(
        shape_of("SELECT 1 UNION ALL SELECT 1 INTERSECT ALL SELECT 1"),
        "(L UNION ALL (L INTERSECT ALL L))"
    );
}

/// A trailing ORDER BY / LIMIT still applies to the whole set-op result even
/// though INTERSECT now nests: the top node is the Limit/Sort wrapper, and the
/// set-op tree beneath it keeps the correct INTERSECT-tighter grouping.
#[test]
fn trailing_order_by_limit_wraps_the_whole_precedence_tree() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "SELECT 1 UNION SELECT 2 INTERSECT SELECT 2 ORDER BY 1 LIMIT 5",
        &cat,
    )
    .expect("bind ok");
    // LIMIT wraps SORT wraps the set-op tree.
    let LogicalPlan::Limit { input, n, .. } = &plan else {
        panic!("expected Limit at top, got {plan:?}");
    };
    assert_eq!(*n, 5);
    let LogicalPlan::Sort { input, .. } = input.as_ref() else {
        panic!("expected Sort under Limit, got {input:?}");
    };
    assert_eq!(setop_shape(input), "(L UNION (L INTERSECT L))");
}
