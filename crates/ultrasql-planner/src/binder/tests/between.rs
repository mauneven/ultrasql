//! Binder tests for the BETWEEN / NOT BETWEEN comparison-tree rewrite,
//! including symmetric forms and property tests.

use proptest::prelude::*;
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::ast::BinaryOp;

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};

// BETWEEN tests — the binder rewrites BETWEEN into a comparison tree
// -----------------------------------------------------------------------

/// Extract the bound WHERE predicate from a SELECT plan that the
/// binder shaped as `Project { Filter { Scan } }`.
fn predicate_of(plan: &LogicalPlan) -> &ScalarExpr {
    fn find_filter(plan: &LogicalPlan) -> &LogicalPlan {
        match plan {
            LogicalPlan::Filter { .. } => plan,
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_filter(input),
            _ => panic!("expected Filter under plan, got {plan:?}"),
        }
    }
    match find_filter(plan) {
        LogicalPlan::Filter { predicate, .. } => predicate,
        other => panic!("expected Filter, got {other:?}"),
    }
}

#[test]
fn binds_between_as_ge_and_le() {
    // The canonical rewrite: BETWEEN low AND high becomes
    // `expr >= low AND expr <= high`.
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    // Top-level: AND.
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        data_type,
    } = pred
    else {
        panic!("expected AND at the root, got {pred:?}");
    };
    assert_eq!(*data_type, DataType::Bool);

    // Left arm: `id >= 5`.
    let ScalarExpr::Binary {
        op: BinaryOp::GtEq,
        left: lo_l,
        right: lo_r,
        ..
    } = left.as_ref()
    else {
        panic!("expected GtEq on left, got {left:?}");
    };
    assert!(matches!(lo_l.as_ref(), ScalarExpr::Column { name, .. } if name == "id"));
    assert!(matches!(
        lo_r.as_ref(),
        ScalarExpr::Literal {
            value: Value::Int32(5),
            ..
        }
    ));

    // Right arm: `id <= 10`.
    let ScalarExpr::Binary {
        op: BinaryOp::LtEq,
        left: hi_l,
        right: hi_r,
        ..
    } = right.as_ref()
    else {
        panic!("expected LtEq on right, got {right:?}");
    };
    assert!(matches!(hi_l.as_ref(), ScalarExpr::Column { name, .. } if name == "id"));
    assert!(matches!(
        hi_r.as_ref(),
        ScalarExpr::Literal {
            value: Value::Int32(10),
            ..
        }
    ));
}

#[test]
fn binds_not_between_as_lt_or_gt() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id NOT BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    let ScalarExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected OR at the root, got {pred:?}");
    };
    assert!(matches!(
        left.as_ref(),
        ScalarExpr::Binary {
            op: BinaryOp::Lt,
            ..
        }
    ));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Binary {
            op: BinaryOp::Gt,
            ..
        }
    ));
}

#[test]
fn binds_between_mixed_numeric_types() {
    // `score` is FLOAT8 in the users catalog. A BETWEEN against an
    // integer pair must bind cleanly through the same numeric-join
    // promotion that the explicit comparison form uses.
    let plan = parse_bind_ok("SELECT id FROM users WHERE score BETWEEN 1 AND 100");
    let pred = predicate_of(&plan);
    assert!(matches!(
        pred,
        ScalarExpr::Binary {
            op: BinaryOp::And,
            ..
        }
    ));
}

#[test]
fn binds_between_symmetric_emits_or_of_two_ranges() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN SYMMETRIC 10 AND 5");
    let pred = predicate_of(&plan);
    // BETWEEN SYMMETRIC: (forward) OR (reversed).
    let ScalarExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected OR at the root, got {pred:?}");
    };
    // Each arm is a `(>= AND <=)` tree.
    for (label, arm) in [("forward", left.as_ref()), ("reversed", right.as_ref())] {
        assert!(
            matches!(
                arm,
                ScalarExpr::Binary {
                    op: BinaryOp::And,
                    ..
                }
            ),
            "SYMMETRIC {label} arm should be AND, got {arm:?}"
        );
    }
}

#[test]
fn binds_not_between_symmetric_emits_and_of_two_ranges() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id NOT BETWEEN SYMMETRIC 10 AND 5");
    let pred = predicate_of(&plan);
    // NOT BETWEEN SYMMETRIC: (forward NOT) AND (reversed NOT).
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected AND at the root, got {pred:?}");
    };
    for (label, arm) in [("forward", left.as_ref()), ("reversed", right.as_ref())] {
        assert!(
            matches!(
                arm,
                ScalarExpr::Binary {
                    op: BinaryOp::Or,
                    ..
                }
            ),
            "NOT SYMMETRIC {label} arm should be OR, got {arm:?}"
        );
    }
}

#[test]
fn binds_between_rewrite_renders_as_full_tree() {
    // Lock down the exact textual form of the rewrite so it surfaces
    // unambiguously in EXPLAIN-style output.
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    assert_eq!(pred.to_string(), "((id >= 5) AND (id <= 10))");
}

#[test]
fn binds_between_uses_existing_type_check_to_reject_incompatible_bounds() {
    // `name` is TEXT; bound by an integer literal is not comparable
    // — the binder must surface a TypeMismatch the same way it
    // would for the equivalent `name >= 1 AND name <= 10`.
    let cat = users_catalog();
    let err = parse_and_bind("SELECT id FROM users WHERE name BETWEEN 1 AND 10", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

proptest! {
    /// BETWEEN binds without error for every integer pair drawn from the
    /// supported i32 range — the rewrite never invents a type it does not
    /// already accept on plain comparisons.
    #[test]
    fn prop_between_int_pair_binds_ok(
        lo in -1_000_000_i32..=1_000_000_i32,
        hi in -1_000_000_i32..=1_000_000_i32,
    ) {
        let cat = users_catalog();
        let sql = format!("SELECT id FROM users WHERE id BETWEEN {lo} AND {hi}");
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "BETWEEN should bind, got {:?}", result);
    }

    /// NOT BETWEEN binds without error for every integer pair drawn from
    /// the supported i32 range.
    #[test]
    fn prop_not_between_int_pair_binds_ok(
        lo in -1_000_000_i32..=1_000_000_i32,
        hi in -1_000_000_i32..=1_000_000_i32,
    ) {
        let cat = users_catalog();
        let sql = format!("SELECT id FROM users WHERE id NOT BETWEEN {lo} AND {hi}");
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "NOT BETWEEN should bind, got {:?}", result);
    }
}

proptest! {
    /// Any random join tree over a fixed set of 3 tables binds without error.
    #[test]
    fn prop_join_tree_over_three_tables_binds_ok(
        // Choose join type index 0..4 for left join and right join.
        lj_type in 0_usize..2_usize,
        rj_type in 0_usize..2_usize,
    ) {
        // Catalog: a, b, c each with one column.
        let mut cat = InMemoryCatalog::new();
        let s = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");
        cat.register("ta", TableMeta::new(s));
        let sb = Schema::new([Field::required("y", DataType::Int32)]).expect("schema ok");
        cat.register("tb", TableMeta::new(sb));
        let sc = Schema::new([Field::required("z", DataType::Int32)]).expect("schema ok");
        cat.register("tc", TableMeta::new(sc));

        let join_kw = ["INNER JOIN", "CROSS JOIN"];
        let lj = join_kw[lj_type % join_kw.len()];
        let rj = join_kw[rj_type % join_kw.len()];
        let on_lj = if lj == "CROSS JOIN" { "" } else { " ON ta.x = tb.y" };
        let on_rj = if rj == "CROSS JOIN" { "" } else { " ON ta.x = tc.z" };
        let sql = format!(
            "SELECT ta.x FROM ta {lj} tb{on_lj} {rj} tc{on_rj}"
        );
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "join tree should bind ok, got {:?}", result);
    }
}

proptest! {
    /// For any arity in 1..=6 and 1..=4 matching VALUES rows, the bound
    /// INSERT plan has a Values source with the same arity.
    #[test]
    fn prop_insert_values_arity_preserved(
        arity in 1_usize..=6_usize,
        nrows in 1_usize..=4_usize,
    ) {
        // Build a catalog with a table that has `arity` INT columns.
        let fields: Vec<Field> = (0..arity)
            .map(|i| Field::nullable(format!("c{i}"), DataType::Int32))
            .collect();
        let schema = Schema::new(fields).expect("schema ok");
        let mut cat = InMemoryCatalog::new();
        cat.register("t", TableMeta::new(schema));

        // Build SQL: INSERT INTO t (c0, c1, …) VALUES (0, 0, …), …
        let cols: Vec<String> = (0..arity).map(|i| format!("c{i}")).collect();
        let one_row = vec!["0"; arity].join(", ");
        let values_clause = std::iter::repeat_n(format!("({one_row})"), nrows)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO t ({}) VALUES {}",
            cols.join(", "),
            values_clause
        );

        let plan = parse_and_bind(&sql, &cat).expect("bind ok");
        let LogicalPlan::Insert { columns, source, .. } = &plan else {
            panic!("expected Insert");
        };
        prop_assert_eq!(columns.len(), arity);
        let LogicalPlan::Values { rows, .. } = source.as_ref() else {
            panic!("expected Values source");
        };
        prop_assert_eq!(rows.len(), nrows);
        for r in rows {
            prop_assert_eq!(r.len(), arity);
        }
    }
}
