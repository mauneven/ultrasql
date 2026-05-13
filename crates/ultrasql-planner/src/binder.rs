//! Binder — turn a parser AST into a typed logical plan.
//!
//! The binder is a single pass over the AST. For a `SELECT` statement
//! it:
//!
//! 1. Resolves the `FROM` clause to a [`crate::plan::LogicalPlan::Scan`]
//!    over a single relation, looked up in the supplied catalog.
//! 2. Resolves column references in `WHERE` / `SELECT` / `ORDER BY`
//!    against the producing operator's schema; bare column names
//!    become [`crate::expr::ScalarExpr::Column`] nodes with an index.
//! 3. Type-checks expressions, using
//!    [`ultrasql_core::DataType::numeric_join`] for arithmetic and a
//!    simple shape rule for comparisons and boolean operators.
//! 4. Wraps the scan in `Filter` / `Project` / `Sort` / `Limit` in
//!    the canonical SQL evaluation order.
//!
//! The binder does *not* expand `SELECT *`; that is rejected with
//! [`crate::error::PlanError::NotSupported`]. Wildcard expansion is a
//! follow-up that needs alias tracking, which the binder will grow
//! when joins land.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::ast::{
    BinaryOp, Expr, Literal, NullsOrder, OrderItem, SelectItem, SelectStmt, SortDirection,
    Statement, TableRef, UnaryOp,
};

use crate::catalog::Catalog;
use crate::error::PlanError;
use crate::expr::ScalarExpr;
use crate::plan::{LogicalPlan, SortKey};

/// Bind a [`Statement`] against the supplied catalog and produce a
/// typed logical plan.
///
/// # Errors
///
/// Returns a [`PlanError`] for any of:
/// - missing table or column,
/// - ambiguous column reference,
/// - a type mismatch in an operator,
/// - a construct the binder does not yet implement.
pub fn bind(stmt: &Statement, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    match stmt {
        Statement::Select(s) => bind_select(s, catalog),
        Statement::Begin { .. } | Statement::Commit { .. } | Statement::Rollback { .. } => Err(
            PlanError::NotSupported("transaction control statements are not planner targets"),
        ),
        _ => Err(PlanError::NotSupported("statement variant")),
    }
}

fn bind_select(select: &SelectStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    if select.distinct {
        return Err(PlanError::NotSupported("SELECT DISTINCT"));
    }

    // FROM clause. We currently support a single named relation.
    let mut plan = match &select.from {
        Some(TableRef::Named { name, .. }) => {
            let table_name = name
                .parts
                .last()
                .map_or_else(String::new, |p| p.value.clone());
            let meta = catalog
                .lookup_table(&table_name)
                .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
            LogicalPlan::Scan {
                schema: meta.schema,
                table: table_name,
                projection: None,
            }
        }
        Some(_) => return Err(PlanError::NotSupported("FROM clause variant")),
        None => LogicalPlan::Empty {
            schema: Schema::empty(),
        },
    };

    // WHERE.
    if let Some(pred_ast) = &select.r#where {
        let pred = bind_expr(pred_ast, plan.schema())?;
        let pred_ty = pred.data_type();
        if pred_ty != DataType::Bool && pred_ty != DataType::Null {
            return Err(PlanError::TypeMismatch(format!(
                "WHERE predicate must be boolean, got {pred_ty}"
            )));
        }
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: pred,
        };
    }

    // SELECT list (must come logically after WHERE for column scope, but
    // before ORDER BY's projection-aware lookup; we resolve ORDER BY
    // against the scan schema below since we do not yet expose
    // projection aliases to ORDER BY).
    let projected = bind_projection(&select.projection, plan.schema())?;
    let proj_fields: Vec<Field> = projected
        .iter()
        .map(|(e, name)| {
            // Projection outputs are nullable in the general case (the
            // expression may produce NULL even from a NOT NULL column,
            // e.g. division). Conservative default.
            Field::nullable(name, e.data_type())
        })
        .collect();
    let proj_schema = Schema::new(proj_fields)
        .map_err(|e| PlanError::TypeMismatch(format!("projection: {e}")))?;

    // ORDER BY — resolved against the *input* (post-filter, pre-project)
    // schema. PostgreSQL allows references to projection aliases too;
    // this binder will grow that in a follow-up.
    let sort_keys = bind_order_by(&select.order_by, plan.schema())?;
    if !sort_keys.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys: sort_keys,
        };
    }

    // Apply the projection.
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        exprs: projected,
        schema: proj_schema,
    };

    // LIMIT / OFFSET.
    let limit_val = match &select.limit {
        Some(e) => Some(bind_unsigned_literal(e, "LIMIT")?),
        None => None,
    };
    let offset_val = match &select.offset {
        Some(e) => bind_unsigned_literal(e, "OFFSET")?,
        None => 0,
    };
    if let Some(n) = limit_val {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
            offset: offset_val,
        };
    } else if offset_val != 0 {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n: u64::MAX,
            offset: offset_val,
        };
    }

    Ok(plan)
}

fn bind_projection(
    items: &[SelectItem],
    input: &Schema,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                return Err(PlanError::NotSupported(
                    "wildcard projection (will land with join binding)",
                ));
            }
            SelectItem::Expr { expr, alias, .. } => {
                let bound = bind_expr(expr, input)?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}

/// Derive an output column name from an expression. Bare column
/// references inherit the column's name; everything else falls back to
/// a synthetic `"col{n}"`-style label produced by the caller via
/// [`Self::display`]. The synthetic label is the expression's display
/// form, which keeps EXPLAIN readable without claiming any particular
/// stability.
fn derive_output_name(ast: &Expr, bound: &ScalarExpr) -> String {
    match ast {
        Expr::Column { name } => name
            .parts
            .last()
            .map_or_else(String::new, |p| p.value.clone()),
        _ => bound.to_string(),
    }
}

fn bind_order_by(items: &[OrderItem], input: &Schema) -> Result<Vec<SortKey>, PlanError> {
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        let expr = bind_expr(&item.expr, input)?;
        let asc = matches!(item.direction, SortDirection::Asc);
        let nulls_first = match item.nulls {
            NullsOrder::First => true,
            NullsOrder::Last => false,
            // PostgreSQL default: NULLS LAST for ASC, NULLS FIRST for DESC.
            NullsOrder::Default => !asc,
        };
        keys.push(SortKey {
            expr,
            asc,
            nulls_first,
        });
    }
    Ok(keys)
}

fn bind_unsigned_literal(expr: &Expr, label: &'static str) -> Result<u64, PlanError> {
    match expr {
        Expr::Literal(Literal::Integer { text, .. }) => text.parse::<u64>().map_err(|_| {
            PlanError::TypeMismatch(format!(
                "{label} must be a non-negative integer, got '{text}'"
            ))
        }),
        Expr::Paren { expr, .. } => bind_unsigned_literal(expr, label),
        _ => Err(PlanError::NotSupported(
            "non-literal LIMIT/OFFSET expressions",
        )),
    }
}

fn bind_expr(expr: &Expr, input: &Schema) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Literal(lit) => Ok(bind_literal(lit)),
        Expr::Column { name } => bind_column(name, input),
        Expr::Parameter { index, .. } => Ok(ScalarExpr::Parameter {
            index: *index,
            data_type: DataType::Null,
        }),
        Expr::Paren { expr, .. } => bind_expr(expr, input),
        Expr::Unary {
            op, expr: inner, ..
        } => bind_unary(*op, inner, input),
        Expr::Binary {
            op, left, right, ..
        } => bind_binary(*op, left, right, input),
        Expr::IsNull { expr, negated, .. } => Ok(ScalarExpr::IsNull {
            expr: Box::new(bind_expr(expr, input)?),
            negated: *negated,
        }),
        Expr::Call { .. } => Err(PlanError::NotSupported("function calls")),
        Expr::Cast { .. } => Err(PlanError::NotSupported("CAST expressions")),
        _ => Err(PlanError::NotSupported("expression variant")),
    }
}

fn bind_literal(lit: &Literal) -> ScalarExpr {
    match lit {
        Literal::Bool { value, .. } => ScalarExpr::Literal {
            value: Value::Bool(*value),
            data_type: DataType::Bool,
        },
        Literal::Integer { text, .. } => {
            // Pick the narrowest integer width that fits, matching the
            // PostgreSQL convention.
            let (value, data_type) = parse_integer_literal(text);
            ScalarExpr::Literal { value, data_type }
        }
        Literal::Float { text, .. } => {
            // Float literals default to `double precision`. A future
            // pass can recognise an `f` suffix and pick `Float32`.
            let parsed = text.parse::<f64>().unwrap_or(f64::NAN);
            ScalarExpr::Literal {
                value: Value::Float64(parsed),
                data_type: DataType::Float64,
            }
        }
        Literal::String { value, .. } => ScalarExpr::Literal {
            value: Value::Text(value.clone()),
            data_type: DataType::Text { max_len: None },
        },
        // `Literal::Null` and any future non-exhaustive variant both
        // bind to a NULL placeholder; later passes specialize.
        _ => ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        },
    }
}

/// Pick the narrowest signed integer type that fits a decimal literal.
fn parse_integer_literal(text: &str) -> (Value, DataType) {
    if let Ok(v) = text.parse::<i32>() {
        return (Value::Int32(v), DataType::Int32);
    }
    if let Ok(v) = text.parse::<i64>() {
        return (Value::Int64(v), DataType::Int64);
    }
    // Out of i64 range — fall back to a Decimal placeholder; this
    // matches what `numeric_join` already promotes integer literals to
    // when paired with a Decimal column. We do not yet have a Decimal
    // Value variant, so park it as `Int64::MAX`. A future pass with
    // a Decimal datum will replace this branch.
    (
        Value::Int64(i64::MAX),
        DataType::Decimal {
            precision: None,
            scale: None,
        },
    )
}

fn bind_column(
    name: &ultrasql_parser::ast::ObjectName,
    input: &Schema,
) -> Result<ScalarExpr, PlanError> {
    let col_name = name
        .parts
        .last()
        .map_or_else(String::new, |p| p.value.clone());
    // We do not yet have multi-relation scopes, so we ignore any
    // qualifier and resolve unambiguously by column name in the input
    // schema.
    let mut hits = input
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name.eq_ignore_ascii_case(&col_name));
    let Some((index, field)) = hits.next() else {
        return Err(PlanError::ColumnNotFound(col_name));
    };
    if hits.next().is_some() {
        return Err(PlanError::Ambiguous(col_name));
    }
    Ok(ScalarExpr::Column {
        name: field.name.clone(),
        index,
        data_type: field.data_type.clone(),
    })
}

fn bind_unary(op: UnaryOp, inner: &Expr, input: &Schema) -> Result<ScalarExpr, PlanError> {
    let bound = bind_expr(inner, input)?;
    let inner_ty = bound.data_type();
    let data_type = match op {
        UnaryOp::Neg | UnaryOp::Pos => {
            if inner_ty.is_numeric() {
                inner_ty
            } else if matches!(inner_ty, DataType::Null) {
                DataType::Null
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "unary {} on non-numeric type {inner_ty}",
                    display_unary(op)
                )));
            }
        }
        UnaryOp::Not => {
            if matches!(inner_ty, DataType::Bool | DataType::Null) {
                DataType::Bool
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "NOT on non-boolean type {inner_ty}"
                )));
            }
        }
    };
    Ok(ScalarExpr::Unary {
        op,
        expr: Box::new(bound),
        data_type,
    })
}

fn bind_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    input: &Schema,
) -> Result<ScalarExpr, PlanError> {
    let l = bind_expr(left, input)?;
    let r = bind_expr(right, input)?;
    let lt = l.data_type();
    let rt = r.data_type();
    let data_type = match op {
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Pow => {
            if matches!(lt, DataType::Null) {
                rt
            } else if matches!(rt, DataType::Null) {
                lt
            } else {
                lt.numeric_join(&rt).map_err(|_| {
                    PlanError::TypeMismatch(format!(
                        "arithmetic operator {} on incompatible types {lt} and {rt}",
                        display_binary(op)
                    ))
                })?
            }
        }
        BinaryOp::Concat => {
            if (lt.is_textlike() || matches!(lt, DataType::Null))
                && (rt.is_textlike() || matches!(rt, DataType::Null))
            {
                DataType::Text { max_len: None }
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "string concatenation requires text operands, got {lt} and {rt}"
                )));
            }
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => {
            if comparable(&lt, &rt) {
                DataType::Bool
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "cannot compare {lt} and {rt}"
                )));
            }
        }
        BinaryOp::And | BinaryOp::Or => {
            if matches!(lt, DataType::Bool | DataType::Null)
                && matches!(rt, DataType::Bool | DataType::Null)
            {
                DataType::Bool
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "{} requires boolean operands, got {lt} and {rt}",
                    display_binary(op)
                )));
            }
        }
        BinaryOp::Like | BinaryOp::NotLike | BinaryOp::Ilike | BinaryOp::NotIlike => {
            if (lt.is_textlike() || matches!(lt, DataType::Null))
                && (rt.is_textlike() || matches!(rt, DataType::Null))
            {
                DataType::Bool
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "{} requires text operands, got {lt} and {rt}",
                    display_binary(op)
                )));
            }
        }
    };
    Ok(ScalarExpr::Binary {
        op,
        left: Box::new(l),
        right: Box::new(r),
        data_type,
    })
}

fn comparable(a: &DataType, b: &DataType) -> bool {
    if matches!(a, DataType::Null) || matches!(b, DataType::Null) {
        return true;
    }
    if a == b {
        return true;
    }
    if a.is_numeric() && b.is_numeric() {
        return true;
    }
    if a.is_textlike() && b.is_textlike() {
        return true;
    }
    if a.is_temporal() && b.is_temporal() {
        return true;
    }
    false
}

const fn display_unary(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "-",
        UnaryOp::Pos => "+",
        UnaryOp::Not => "NOT",
    }
}

const fn display_binary(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Pow => "^",
        BinaryOp::Concat => "||",
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Like => "LIKE",
        BinaryOp::NotLike => "NOT LIKE",
        BinaryOp::Ilike => "ILIKE",
        BinaryOp::NotIlike => "NOT ILIKE",
    }
}
