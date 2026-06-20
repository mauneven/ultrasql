//! Shared helpers used across the DDL binder submodules: name
//! synthesis, namespace extraction, referential-action and index-method
//! mapping, default-expression safety analysis, and column nullability.

use ultrasql_core::DataType;
use ultrasql_parser::ast::{
    ColumnConstraint, Expr, Identifier, Literal, ObjectName,
    ReferentialAction as AstReferentialAction,
};

use super::super::expr_bind::coerce_literal_to_type;
use super::super::{PlanError, ScalarExpr};
use crate::plan::{LogicalIndexMethod, LogicalReferentialAction};

pub(super) const MAX_NUMERIC_PRECISION: u32 = 131_072;

pub(super) fn bind_index_method(method: &str) -> Result<LogicalIndexMethod, PlanError> {
    match method.to_ascii_lowercase().as_str() {
        "btree" => Ok(LogicalIndexMethod::Btree),
        "hash" => Ok(LogicalIndexMethod::Hash),
        "gin" => Ok(LogicalIndexMethod::Gin),
        "gist" => Ok(LogicalIndexMethod::Gist),
        "brin" => Ok(LogicalIndexMethod::Brin),
        "hnsw" => Ok(LogicalIndexMethod::Hnsw),
        "ivfflat" => Ok(LogicalIndexMethod::IvfFlat),
        _ => Err(PlanError::NotSupported(
            "only btree, hash, gin, gist, brin, hnsw, and ivfflat methods are supported",
        )),
    }
}

pub(super) const fn bind_referential_action(
    action: AstReferentialAction,
) -> LogicalReferentialAction {
    match action {
        AstReferentialAction::NoAction => LogicalReferentialAction::NoAction,
        AstReferentialAction::Restrict => LogicalReferentialAction::Restrict,
        AstReferentialAction::Cascade => LogicalReferentialAction::Cascade,
        AstReferentialAction::SetNull => LogicalReferentialAction::SetNull,
        AstReferentialAction::SetDefault => LogicalReferentialAction::SetDefault,
    }
}

pub(super) fn named_or<F>(name: Option<&Identifier>, fallback: F) -> String
where
    F: FnOnce() -> String,
{
    name.map_or_else(fallback, |n| n.value.to_ascii_lowercase())
}

pub(super) fn unique_name(table: &str, columns: &[String], primary_key: bool) -> String {
    if primary_key {
        return format!("{table}_pkey");
    }
    let mut s = String::from(table);
    for col in columns {
        s.push('_');
        s.push_str(col);
    }
    s.push_str("_key");
    s
}

pub(super) fn is_default_safe(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Literal { .. } => true,
        ScalarExpr::Column { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => false,
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => is_default_safe(expr),
        ScalarExpr::Binary { left, right, .. } => is_default_safe(left) && is_default_safe(right),
        ScalarExpr::FunctionCall { args, .. } => args.iter().all(is_default_safe),
    }
}

pub(super) fn coerce_default_expr_to_type(expr: &mut ScalarExpr, target: &DataType) {
    coerce_literal_to_type(expr, target);
    if let (
        DataType::Timestamp,
        ScalarExpr::FunctionCall {
            name, data_type, ..
        },
    ) = (target, expr)
        && matches!(name.as_str(), "now" | "current_timestamp")
        && matches!(data_type, DataType::TimestampTz)
    {
        *data_type = DataType::Timestamp;
    }
}

pub(super) fn is_generated_stored_safe(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Literal { .. } | ScalarExpr::Column { .. } => true,
        ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => false,
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            is_generated_stored_safe(expr)
        }
        ScalarExpr::Binary { left, right, .. } => {
            is_generated_stored_safe(left) && is_generated_stored_safe(right)
        }
        ScalarExpr::FunctionCall { args, .. } => args.iter().all(is_generated_stored_safe),
    }
}

pub(super) fn expr_references_generated_column(
    expr: &ScalarExpr,
    generated_columns: &[bool],
) -> bool {
    match expr {
        ScalarExpr::Column { index, .. } => generated_columns.get(*index).copied().unwrap_or(false),
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => false,
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            expr_references_generated_column(expr, generated_columns)
        }
        ScalarExpr::Binary { left, right, .. } => {
            expr_references_generated_column(left, generated_columns)
                || expr_references_generated_column(right, generated_columns)
        }
        ScalarExpr::OuterColumn { .. } => false,
        ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => false,
        ScalarExpr::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| expr_references_generated_column(arg, generated_columns)),
    }
}

/// Pull the namespace component out of a possibly-qualified relation
/// name. `t` → `"public"`; `s.t` → `"s"`; `c.s.t` → `"s"`.
pub(super) fn object_name_namespace(name: &ObjectName) -> String {
    if name.parts.len() >= 2 {
        let idx = name.parts.len() - 2;
        name.parts[idx].value.to_ascii_lowercase()
    } else {
        String::from("public")
    }
}

pub(super) fn object_name_explicit_namespace(name: &ObjectName) -> Option<String> {
    (name.parts.len() >= 2).then(|| object_name_namespace(name))
}

/// Determine whether a column is nullable from its constraint list.
///
/// Returns `true` (nullable) when no `NOT NULL` or `PRIMARY KEY`
/// constraint is present. `PRIMARY KEY` implies `NOT NULL`. Other
/// constraint kinds (DEFAULT, UNIQUE, CHECK, REFERENCES) return
/// [`PlanError::NotSupported`].
pub(super) fn resolve_column_nullability(
    constraints: &[ColumnConstraint],
) -> Result<bool, PlanError> {
    let mut nullable = true;
    for c in constraints {
        match c {
            ColumnConstraint::NotNull { .. } | ColumnConstraint::PrimaryKey { .. } => {
                nullable = false;
            }
            ColumnConstraint::Null { .. } => nullable = true,
            ColumnConstraint::Default { .. }
            | ColumnConstraint::Unique { .. }
            | ColumnConstraint::Check { .. }
            | ColumnConstraint::References { .. }
            | ColumnConstraint::GeneratedIdentity { .. }
            | ColumnConstraint::GeneratedStored { .. } => {}
        }
    }
    Ok(nullable)
}

pub(super) fn column_default(constraints: &[ColumnConstraint]) -> Result<Option<&Expr>, PlanError> {
    let mut out = None;
    for c in constraints {
        if let ColumnConstraint::Default { expr, .. } = c {
            if out.is_some() {
                return Err(PlanError::NotSupported(
                    "CREATE TABLE: multiple DEFAULT clauses on one column",
                ));
            }
            out = Some(expr);
        }
    }
    Ok(out)
}

pub(super) fn index_option_value_to_string(expr: &Expr) -> Result<String, PlanError> {
    match expr {
        Expr::Literal(Literal::Integer { text, .. })
        | Expr::Literal(Literal::Float { text, .. }) => Ok(text.clone()),
        Expr::Literal(Literal::String { value, .. })
        | Expr::Literal(Literal::Typed { value, .. }) => Ok(value.clone()),
        Expr::Literal(Literal::Bool { value, .. }) => {
            Ok(if *value { "true" } else { "false" }.to_owned())
        }
        Expr::Column { name } if name.parts.len() == 1 => Ok(name.parts[0].value.clone()),
        _ => Err(PlanError::NotSupported("CREATE INDEX WITH option value")),
    }
}

pub(super) fn unparen_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren { expr, .. } => unparen_expr(expr),
        _ => expr,
    }
}
