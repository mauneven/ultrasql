//! Binder tests for `SELECT DISTINCT ON (...)`.
//!
//! These assert the bound logical shape — `Project(DistinctOn(Sort(...)))`
//! — the ON-key set, the sort keys, and the 42P10 prefix rule. End-to-end
//! row results are covered by the server round-trip tests.

use super::*;
use crate::expr::ScalarExpr;

/// Walk down from the top `Project` to the `DistinctOn` it wraps.
fn distinct_on_node(plan: &LogicalPlan) -> (&[ScalarExpr], &LogicalPlan) {
    let LogicalPlan::Project { input, .. } = plan else {
        panic!("expected Project at top, got {plan:?}");
    };
    let LogicalPlan::DistinctOn { input, on_keys } = input.as_ref() else {
        panic!("expected DistinctOn under Project, got {input:?}");
    };
    (on_keys, input.as_ref())
}

#[test]
fn distinct_on_binds_project_over_distinct_on_over_sort() {
    let plan = parse_bind_ok("SELECT DISTINCT ON (id) id, name FROM users ORDER BY id, score DESC");
    let (on_keys, below) = distinct_on_node(&plan);
    assert_eq!(on_keys.len(), 1, "one ON key");
    // The ON key is the `id` column.
    assert!(
        matches!(&on_keys[0], ScalarExpr::Column { index: 0, .. }),
        "ON key should be id (index 0), got {:?}",
        on_keys[0]
    );
    // Below the dedup is a Sort whose leading key is the ON key, followed by
    // the rest of ORDER BY (score DESC).
    let LogicalPlan::Sort { keys, .. } = below else {
        panic!("expected Sort below DistinctOn, got {below:?}");
    };
    assert_eq!(keys.len(), 2, "sort by id then score");
    assert!(matches!(&keys[0].expr, ScalarExpr::Column { index: 0, .. }));
    assert!(keys[0].asc, "id ascending");
    assert!(!keys[1].asc, "score DESC");
}

#[test]
fn distinct_on_multiple_keys_are_carried() {
    let plan = parse_bind_ok("SELECT DISTINCT ON (id, name) id, name FROM users ORDER BY id, name");
    let (on_keys, _) = distinct_on_node(&plan);
    assert_eq!(on_keys.len(), 2, "two ON keys");
}

#[test]
fn distinct_on_key_not_in_select_list_is_allowed() {
    // Group by `score` while projecting only `id`/`name`.
    let plan = parse_bind_ok("SELECT DISTINCT ON (score) id FROM users ORDER BY score, id");
    let (on_keys, below) = distinct_on_node(&plan);
    assert_eq!(on_keys.len(), 1);
    // score is column index 2 in the users schema.
    assert!(
        matches!(&on_keys[0], ScalarExpr::Column { index: 2, .. }),
        "ON key should be score (index 2), got {:?}",
        on_keys[0]
    );
    assert!(matches!(below, LogicalPlan::Sort { .. }));
}

#[test]
fn distinct_on_without_order_by_sorts_by_on_keys() {
    let plan = parse_bind_ok("SELECT DISTINCT ON (id) id, name FROM users");
    let (on_keys, below) = distinct_on_node(&plan);
    assert_eq!(on_keys.len(), 1);
    // Without ORDER BY we still sort by the ON keys for determinism.
    let LogicalPlan::Sort { keys, .. } = below else {
        panic!("expected Sort below DistinctOn even without ORDER BY, got {below:?}");
    };
    assert_eq!(keys.len(), 1);
    assert!(matches!(&keys[0].expr, ScalarExpr::Column { index: 0, .. }));
    assert!(keys[0].asc);
}

#[test]
fn distinct_on_key_prefers_output_alias() {
    // PG: `SELECT DISTINCT ON (id) name AS id … ORDER BY id, score` resolves the
    // ON key `id` to the OUTPUT alias `id` (= name, input index 1), not the
    // input `users.id` (index 0). Both the ON key and the leading sort key bind
    // to the projected `name` column.
    let plan = parse_bind_ok("SELECT DISTINCT ON (id) name AS id FROM users ORDER BY id, score");
    let (on_keys, below) = distinct_on_node(&plan);
    assert_eq!(on_keys.len(), 1);
    assert!(
        matches!(&on_keys[0], ScalarExpr::Column { index: 1, .. }),
        "ON key `id` should resolve to output alias `id` = name (input index 1), got {:?}",
        on_keys[0]
    );
    let LogicalPlan::Sort { keys, .. } = below else {
        panic!("expected Sort below DistinctOn, got {below:?}");
    };
    assert!(
        matches!(&keys[0].expr, ScalarExpr::Column { index: 1, .. }),
        "leading ORDER BY `id` should also resolve to the output alias, got {:?}",
        keys[0].expr
    );
}

#[test]
fn distinct_on_non_prefix_order_by_is_rejected() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "SELECT DISTINCT ON (id) id, name FROM users ORDER BY name",
        &cat,
    )
    .expect_err("ON not a prefix of ORDER BY must be rejected");
    assert!(
        matches!(err, PlanError::DistinctOnOrderByMismatch(_)),
        "expected DistinctOnOrderByMismatch, got {err:?}"
    );
    assert!(
        err.to_string()
            .contains("DISTINCT ON expressions must match initial ORDER BY"),
        "unexpected message: {err}"
    );
}

#[test]
fn distinct_on_prefix_of_longer_order_by_is_accepted() {
    // ON (id) is a prefix of ORDER BY id, name — accepted.
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT DISTINCT ON (id) id, name FROM users ORDER BY id, name",
        &cat,
    )
    .expect("prefix ORDER BY binds");
    let (on_keys, _) = distinct_on_node(&plan);
    assert_eq!(on_keys.len(), 1);
}
