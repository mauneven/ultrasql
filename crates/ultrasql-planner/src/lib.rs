//! UltraSQL logical planner.
//!
//! Pipeline: parser AST → [`binder::bind`] → typed [`plan::LogicalPlan`].
//! The binder resolves column and table names against a small
//! [`catalog::Catalog`] trait, type-checks expressions, and produces a
//! plan tree whose every operator carries a precise [`Schema`].
//!
//! This crate is the lower half of query compilation. The optimizer
//! consumes its output; the executor never sees the AST.
//!
//! Stability: the public surface is pre-1.0. Variants are
//! `#[non_exhaustive]` so new operators can be added without a
//! semver-major bump.
//!
//! [`Schema`]: ultrasql_core::Schema

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]

pub mod binder;
pub mod catalog;
pub mod error;
pub mod expr;
pub mod plan;
pub mod scope;

pub use binder::bind;
pub use catalog::{Catalog, InMemoryCatalog, TableMeta};
pub use error::PlanError;
pub use expr::{BinaryOp, ScalarExpr, UnaryOp};
pub use plan::{
    AggregateFunc, ConflictTarget, CopyDirection, CopyFormat, CopySource, ExplainFormat,
    LogicalAggregateExpr, LogicalAlterTableAction, LogicalJoinCondition, LogicalJoinType,
    LogicalOnConflict, LogicalPlan, LogicalSetOp, LogicalSetQuantifier, LogicalSetVariableAction,
    LogicalWindowFunc, SortKey, TxnIsolationLevel,
};

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_parser::Parser;

    use super::*;

    fn users_catalog() -> InMemoryCatalog {
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema invariants hold for test fixture");
        let mut cat = InMemoryCatalog::new();
        cat.register("users", TableMeta::new(schema));
        cat
    }

    fn parse_and_bind(sql: &str) -> Result<LogicalPlan, PlanError> {
        let cat = users_catalog();
        let stmt = Parser::new(sql)
            .parse_statement()
            .expect("test SQL parses cleanly");
        bind(&stmt, &cat)
    }

    #[test]
    fn select_single_column_yields_project_over_scan() {
        let plan = parse_and_bind("SELECT id FROM users").expect("bind ok");
        let LogicalPlan::Project { input, schema, .. } = &plan else {
            panic!("expected top-level Project, got {plan:?}");
        };
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.field_at(0).name, "id");
        assert_eq!(schema.field_at(0).data_type, DataType::Int32);
        assert!(matches!(input.as_ref(), LogicalPlan::Scan { .. }));
    }

    #[test]
    fn select_with_where_and_limit_produces_limit_over_filter_over_scan() {
        let plan =
            parse_and_bind("SELECT id FROM users WHERE score > 0.5 LIMIT 10").expect("bind ok");
        let LogicalPlan::Limit { input, n, offset } = &plan else {
            panic!("expected Limit at top, got {plan:?}");
        };
        assert_eq!(*n, 10);
        assert_eq!(*offset, 0);
        let LogicalPlan::Project { input: proj_in, .. } = input.as_ref() else {
            panic!("expected Project under Limit");
        };
        let LogicalPlan::Filter {
            input: filt_in,
            predicate,
        } = proj_in.as_ref()
        else {
            panic!("expected Filter under Project");
        };
        assert!(matches!(filt_in.as_ref(), LogicalPlan::Scan { .. }));
        assert_eq!(predicate.data_type(), DataType::Bool);
    }

    #[test]
    fn order_by_descending_inserts_sort_below_project() {
        let plan =
            parse_and_bind("SELECT id, name FROM users ORDER BY score DESC").expect("bind ok");
        let LogicalPlan::Project { input, schema, .. } = &plan else {
            panic!("expected Project at top");
        };
        assert_eq!(schema.len(), 2);
        let LogicalPlan::Sort { input, keys } = input.as_ref() else {
            panic!("expected Sort under Project");
        };
        assert_eq!(keys.len(), 1);
        assert!(!keys[0].asc);
        // Default for DESC is NULLS FIRST.
        assert!(keys[0].nulls_first);
        assert!(matches!(input.as_ref(), LogicalPlan::Scan { .. }));
    }

    #[test]
    fn unknown_table_is_table_not_found() {
        let err = parse_and_bind("SELECT id FROM nope").unwrap_err();
        assert!(
            matches!(err, PlanError::TableNotFound(ref s) if s == "nope"),
            "got {err:?}"
        );
    }

    #[test]
    fn unknown_column_is_column_not_found() {
        let err = parse_and_bind("SELECT bogus FROM users").unwrap_err();
        assert!(
            matches!(err, PlanError::ColumnNotFound(ref s) if s == "bogus"),
            "got {err:?}"
        );
    }

    #[test]
    fn integer_plus_text_is_type_mismatch() {
        let err = parse_and_bind("SELECT id + name FROM users").unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }

    #[test]
    fn select_star_expands_to_all_columns() {
        // After wave-3 binder expansion, SELECT * now works.
        let plan = parse_and_bind("SELECT * FROM users").expect("bind ok");
        let LogicalPlan::Project { schema, .. } = &plan else {
            panic!("expected Project, got {plan:?}");
        };
        // users has 3 columns: id, name, score
        assert_eq!(schema.len(), 3, "wildcard should expand to all 3 columns");
    }

    #[test]
    fn non_boolean_where_predicate_is_type_mismatch() {
        let err = parse_and_bind("SELECT id FROM users WHERE id").unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }

    #[test]
    fn limit_with_offset_carries_both_values() {
        let plan = parse_and_bind("SELECT id FROM users LIMIT 5 OFFSET 7").expect("bind ok");
        let LogicalPlan::Limit { n, offset, .. } = plan else {
            panic!("expected Limit at top");
        };
        assert_eq!(n, 5);
        assert_eq!(offset, 7);
    }

    #[test]
    fn projection_alias_overrides_column_name() {
        let plan = parse_and_bind("SELECT id AS user_id FROM users").expect("bind ok");
        let LogicalPlan::Project { schema, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(schema.field_at(0).name, "user_id");
    }

    #[test]
    fn display_renders_nested_plan() {
        let plan = parse_and_bind("SELECT id FROM users WHERE score > 0.5").expect("bind ok");
        let dump = plan.display(0);
        // Project at root, Filter inside, Scan at the leaf, with two
        // spaces of indent per level.
        assert!(dump.starts_with("Project:"));
        assert!(dump.contains("  Filter:"));
        assert!(dump.contains("    Scan: users"));
    }

    /// `BEGIN` / `COMMIT` / `ROLLBACK` produce the corresponding
    /// transaction-control [`LogicalPlan`] variants — they are no
    /// longer rejected as "not a planner target."
    #[test]
    fn transaction_control_statements_bind_to_their_variants() {
        assert!(matches!(
            parse_and_bind("BEGIN").expect("bind ok"),
            LogicalPlan::Begin { .. }
        ));
        assert!(matches!(
            parse_and_bind("COMMIT").expect("bind ok"),
            LogicalPlan::Commit { .. }
        ));
        assert!(matches!(
            parse_and_bind("ROLLBACK").expect("bind ok"),
            LogicalPlan::Rollback { .. }
        ));
    }

    /// `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT`
    /// bind to their variants and carry the lower-cased savepoint
    /// name so subsequent `ROLLBACK TO`/`RELEASE` match
    /// case-insensitively (PostgreSQL behaviour for unquoted
    /// identifiers).
    #[test]
    fn savepoint_statements_bind_with_lowercased_name() {
        let LogicalPlan::Savepoint { name, .. } = parse_and_bind("SAVEPOINT Sp1").expect("bind ok")
        else {
            panic!("expected Savepoint variant");
        };
        assert_eq!(name, "sp1");

        let LogicalPlan::RollbackToSavepoint { name, .. } =
            parse_and_bind("ROLLBACK TO SAVEPOINT Sp1").expect("bind ok")
        else {
            panic!("expected RollbackToSavepoint variant");
        };
        assert_eq!(name, "sp1");

        let LogicalPlan::ReleaseSavepoint { name, .. } =
            parse_and_bind("RELEASE SAVEPOINT Sp1").expect("bind ok")
        else {
            panic!("expected ReleaseSavepoint variant");
        };
        assert_eq!(name, "sp1");
    }
}
