//! Binder tests for derived tables (FROM subqueries) and `LATERAL`.
//!
//! PostgreSQL semantics exercised here:
//! * A plain (non-`LATERAL`) derived table may not reference a sibling FROM
//!   item at the same query level — PG raises "invalid reference to FROM-clause
//!   entry"; UltraSQL surfaces a `ColumnNotFound` for the unresolved name.
//! * A `LATERAL` derived table may correlate to FROM items to its left. The
//!   uncorrelated case binds and runs; a *correlated* lateral reference is not
//!   yet decorrelatable, so the binder rejects it with a clear `NotSupported`
//!   rather than emitting a plan that fails at execution.

use ultrasql_core::{DataType, Field, Schema};

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};

/// One table `u(id INT, k INT)`.
fn u_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("k", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("u", TableMeta::new(schema));
    cat
}

#[test]
fn non_lateral_derived_table_cannot_reference_sibling() {
    // PG rejects `… FROM u a, (SELECT a.id) d` ("invalid reference to FROM-clause
    // entry for table a"): a non-LATERAL derived table sees no sibling. UltraSQL
    // surfaces the unresolved name as ColumnNotFound.
    let cat = u_catalog();
    let err = parse_and_bind("SELECT * FROM u a, (SELECT a.id AS x) d", &cat)
        .expect_err("non-LATERAL derived table must not see sibling `a`");
    assert!(
        matches!(err, PlanError::ColumnNotFound(_)),
        "expected ColumnNotFound for the sibling reference, got {err:?}"
    );
}

#[test]
fn non_lateral_derived_table_in_join_cannot_reference_left() {
    let cat = u_catalog();
    let err = parse_and_bind("SELECT * FROM u a JOIN (SELECT a.id AS x) d ON true", &cat)
        .expect_err("non-LATERAL join-right derived table must not see the left");
    assert!(
        matches!(err, PlanError::ColumnNotFound(_)),
        "expected ColumnNotFound, got {err:?}"
    );
}

#[test]
fn plain_derived_table_binds() {
    let cat = u_catalog();
    let plan =
        parse_and_bind("SELECT * FROM (SELECT 1 AS x) d", &cat).expect("plain derived binds");
    assert_eq!(plan.schema().len(), 1);
}

#[test]
fn uncorrelated_lateral_derived_table_binds() {
    // `LATERAL` that does not actually reference the left side binds normally.
    let cat = u_catalog();
    let plan = parse_and_bind("SELECT * FROM u a, LATERAL (SELECT 99 AS z) d", &cat)
        .expect("uncorrelated LATERAL binds");
    // u(id, k) cross the lateral single-column relation = 3 output columns.
    assert_eq!(plan.schema().len(), 3);
}

#[test]
fn correlated_lateral_derived_table_is_not_supported() {
    // The lateral subquery references the left (`a.id`); this correlation has no
    // decorrelation rule, so the binder rejects it up front rather than emitting
    // a plan that fails at execution.
    let cat = u_catalog();
    let err = parse_and_bind("SELECT * FROM u a, LATERAL (SELECT a.id AS lid) d", &cat)
        .expect_err("correlated LATERAL is not yet supported");
    assert!(
        matches!(
            err,
            PlanError::NotSupportedOwned(_) | PlanError::NotSupported(_)
        ),
        "expected NotSupported for correlated LATERAL, got {err:?}"
    );
}

#[test]
fn correlated_lateral_in_join_is_not_supported() {
    let cat = u_catalog();
    let err = parse_and_bind(
        "SELECT * FROM u a JOIN LATERAL (SELECT a.id AS lid) d ON true",
        &cat,
    )
    .expect_err("correlated JOIN LATERAL is not yet supported");
    assert!(
        matches!(
            err,
            PlanError::NotSupportedOwned(_) | PlanError::NotSupported(_)
        ),
        "expected NotSupported, got {err:?}"
    );
}
