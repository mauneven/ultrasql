//! Window-function binder.
//!
//! Splits a SELECT projection's window-function calls out of the
//! per-row projection and wraps the FROM/WHERE plan in one
//! [`LogicalPlan::Window`] node per call. The projection is rewritten
//! so each call site becomes a [`Expr::Column`] reference to the
//! appended `"$wn_N"` column the new operator emits.
//!
//! The transformation runs **once per `SELECT`**, immediately after
//! `WHERE` binding and before the projection/aggregate logic. Because
//! the rewrite preserves left-to-right evaluation order and never
//! shifts pre-existing schema indices, the downstream projection
//! binder sees a schema whose new tail columns name each window
//! result with a unique synthetic identifier.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::ast::{
    Expr, Identifier, NullsOrder, ObjectName, SortDirection, UnaryOp, WindowSpec,
};

use crate::Catalog;
use crate::error::PlanError;
use crate::expr::ScalarExpr;
use crate::plan::{LogicalPlan, LogicalWindowFunc, SortKey};

use super::ScopeStack;
use super::expr_bind::bind_expr_with_ctes;

/// Walk `projection` and pull out every top-level `Expr::Call { over:
/// Some(_), .. }` into a parallel `Vec<WindowExtraction>`; replace the
/// in-place call expression with a synthetic [`Expr::Column`] that
/// references the appended `"$wn_N"` column.
///
/// Returns `(rewritten_projection, extractions)`. The caller is
/// responsible for wrapping the FROM/WHERE plan in one
/// [`LogicalPlan::Window`] per extraction and then binding
/// `rewritten_projection` against the resulting schema.
pub(super) fn extract_window_calls(
    projection: &[ultrasql_parser::ast::SelectItem],
) -> (Vec<ultrasql_parser::ast::SelectItem>, Vec<WindowExtraction>) {
    let mut extractions: Vec<WindowExtraction> = Vec::new();
    let rewritten: Vec<ultrasql_parser::ast::SelectItem> = projection
        .iter()
        .map(|item| match item {
            ultrasql_parser::ast::SelectItem::Expr { expr, alias, span } => {
                let rewritten_expr = rewrite_expr(expr, &mut extractions);
                ultrasql_parser::ast::SelectItem::Expr {
                    expr: rewritten_expr,
                    alias: alias.clone(),
                    span: *span,
                }
            }
            other => other.clone(),
        })
        .collect();
    (rewritten, extractions)
}

/// Recursively walk `expr`. If it is a window call, replace it with a
/// `Column` ref to the synthetic output name and push an extraction;
/// otherwise recurse into children so a nested window call inside a
/// `CASE` arm or a binary op is still discovered.
fn rewrite_expr(expr: &Expr, out: &mut Vec<WindowExtraction>) -> Expr {
    match expr {
        Expr::Call {
            name,
            args,
            distinct,
            over: Some(spec),
            span,
            ..
        } => {
            let output_name = format!("$wn_{}", out.len());
            out.push(WindowExtraction {
                name: name.clone(),
                args: args.clone(),
                distinct: *distinct,
                spec: spec.clone(),
                output_name: output_name.clone(),
            });
            Expr::Column {
                name: ObjectName {
                    parts: vec![Identifier {
                        value: output_name,
                        quoted: false,
                        span: *span,
                    }],
                    span: *span,
                },
            }
        }
        Expr::Binary {
            op,
            left,
            right,
            span,
        } => Expr::Binary {
            op: *op,
            left: Box::new(rewrite_expr(left, out)),
            right: Box::new(rewrite_expr(right, out)),
            span: *span,
        },
        Expr::Unary { op, expr, span } => Expr::Unary {
            op: *op,
            expr: Box::new(rewrite_expr(expr, out)),
            span: *span,
        },
        // Other shapes do not contain window calls in practice; leave
        // them untouched. The binder will fail with a useful error if a
        // window call appears in a context this rewriter does not
        // recognise.
        other => other.clone(),
    }
}

/// A single window-function call lifted out of the projection.
pub(super) struct WindowExtraction {
    pub name: ObjectName,
    pub args: Vec<Expr>,
    #[allow(dead_code)] // reserved for v0.6 DISTINCT window aggregates
    pub distinct: bool,
    pub spec: WindowSpec,
    pub output_name: String,
}

/// Wrap `plan` in one [`LogicalPlan::Window`] per extraction. Each
/// wrapper extends the schema with the synthetic `"$wn_N"` column the
/// extraction reserved. Returns the wrapped plan; on a malformed call
/// (unsupported function, wrong arity) returns a [`PlanError`].
pub(super) fn apply_window_extractions(
    mut plan: LogicalPlan,
    extractions: Vec<WindowExtraction>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    for ex in extractions {
        let func_name = ex
            .name
            .parts
            .last()
            .map_or(String::new(), |p| p.value.to_ascii_lowercase());

        let func = resolve_window_func(
            &func_name,
            &ex.args,
            plan.schema(),
            catalog,
            cte_catalog,
            scope,
        )?;

        let partition_by: Vec<ScalarExpr> = ex
            .spec
            .partition_by
            .iter()
            .map(|e| bind_expr_with_ctes(e, plan.schema(), catalog, cte_catalog, scope))
            .collect::<Result<_, _>>()?;
        let order_by: Vec<SortKey> = ex
            .spec
            .order_by
            .iter()
            .map(|item| -> Result<SortKey, PlanError> {
                let bound =
                    bind_expr_with_ctes(&item.expr, plan.schema(), catalog, cte_catalog, scope)?;
                Ok(SortKey {
                    expr: bound,
                    asc: matches!(item.direction, SortDirection::Asc),
                    nulls_first: matches!(item.nulls, NullsOrder::First),
                })
            })
            .collect::<Result<_, _>>()?;

        let result_type = window_func_result_type(&func);
        let mut new_fields: Vec<Field> = plan.schema().fields().to_vec();
        new_fields.push(Field::nullable(&ex.output_name, result_type));
        let new_schema = Schema::new(new_fields)
            .map_err(|e| PlanError::TypeMismatch(format!("window schema: {e}")))?;

        plan = LogicalPlan::Window {
            input: Box::new(plan),
            partition_by,
            order_by,
            func,
            output_name: ex.output_name,
            schema: new_schema,
        };
    }
    Ok(plan)
}

/// Map a function name + argument list to a [`LogicalWindowFunc`].
fn resolve_window_func(
    func_name: &str,
    args: &[Expr],
    input_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalWindowFunc, PlanError> {
    match func_name {
        "row_number" => {
            if !args.is_empty() {
                return Err(PlanError::TypeMismatch(
                    "row_number() takes no arguments".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::RowNumber)
        }
        "rank" => {
            if !args.is_empty() {
                return Err(PlanError::TypeMismatch(
                    "rank() takes no arguments".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::Rank)
        }
        "dense_rank" => {
            if !args.is_empty() {
                return Err(PlanError::TypeMismatch(
                    "dense_rank() takes no arguments".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::DenseRank)
        }
        "lag" | "lead" => {
            if args.is_empty() || args.len() > 3 {
                return Err(PlanError::TypeMismatch(format!(
                    "{func_name}: expected 1, 2, or 3 arguments, got {}",
                    args.len()
                )));
            }
            let expr = bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
            let offset = if args.len() >= 2 {
                bind_usize_literal(
                    &args[1],
                    func_name,
                    1,
                    input_schema,
                    catalog,
                    cte_catalog,
                    scope,
                )?
            } else {
                1
            };
            let default = if args.len() == 3 {
                let bound =
                    bind_expr_with_ctes(&args[2], input_schema, catalog, cte_catalog, scope)?;
                extract_literal_value(&bound).ok_or_else(|| {
                    PlanError::TypeMismatch(format!(
                        "{func_name}: default argument must be a literal"
                    ))
                })?
            } else {
                Value::Null
            };
            Ok(if func_name == "lag" {
                LogicalWindowFunc::Lag {
                    expr,
                    offset,
                    default,
                }
            } else {
                LogicalWindowFunc::Lead {
                    expr,
                    offset,
                    default,
                }
            })
        }
        "first_value" => {
            if args.len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "first_value: expected 1 argument, got {}",
                    args.len()
                )));
            }
            Ok(LogicalWindowFunc::FirstValue(bind_expr_with_ctes(
                &args[0],
                input_schema,
                catalog,
                cte_catalog,
                scope,
            )?))
        }
        "last_value" => {
            if args.len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "last_value: expected 1 argument, got {}",
                    args.len()
                )));
            }
            Ok(LogicalWindowFunc::LastValue(bind_expr_with_ctes(
                &args[0],
                input_schema,
                catalog,
                cte_catalog,
                scope,
            )?))
        }
        "nth_value" => {
            if args.len() != 2 {
                return Err(PlanError::TypeMismatch(format!(
                    "nth_value: expected 2 arguments, got {}",
                    args.len()
                )));
            }
            let expr = bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
            let n = bind_usize_literal(
                &args[1],
                "nth_value",
                1,
                input_schema,
                catalog,
                cte_catalog,
                scope,
            )?;
            if n == 0 {
                return Err(PlanError::TypeMismatch(
                    "nth_value: n must be ≥ 1".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::NthValue { expr, n })
        }
        "ntile" => {
            if args.len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "ntile: expected 1 argument, got {}",
                    args.len()
                )));
            }
            let n = bind_usize_literal(
                &args[0],
                "ntile",
                0,
                input_schema,
                catalog,
                cte_catalog,
                scope,
            )?;
            if n == 0 {
                return Err(PlanError::TypeMismatch(
                    "ntile: bucket count must be ≥ 1".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::Ntile(n))
        }
        other => Err(PlanError::NotSupported(Box::leak(
            format!("window function '{other}'").into_boxed_str(),
        ))),
    }
}

/// Fold a bound expression into a `Value` if it represents a constant
/// the binder can evaluate at plan time. Handles bare `Literal` nodes
/// plus `Unary { Neg, Literal(int) }` so SQL like `lag(x, 1, -1)`
/// parses correctly (the parser does not pre-fold the negation).
fn extract_literal_value(expr: &ScalarExpr) -> Option<Value> {
    match expr {
        ScalarExpr::Literal { value, .. } => Some(value.clone()),
        ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr: inner,
            ..
        } => match inner.as_ref() {
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            } => Some(Value::Int32(-v)),
            ScalarExpr::Literal {
                value: Value::Int64(v),
                ..
            } => Some(Value::Int64(-v)),
            ScalarExpr::Literal {
                value: Value::Float64(v),
                ..
            } => Some(Value::Float64(-v)),
            _ => None,
        },
        _ => None,
    }
}

fn bind_usize_literal(
    expr: &Expr,
    func_name: &str,
    arg_index: usize,
    schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<usize, PlanError> {
    match bind_expr_with_ctes(expr, schema, catalog, cte_catalog, scope)? {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            ..
        } if v >= 0 => Ok(
            usize::try_from(v).expect("non-negative i32 fits in usize on all supported targets")
        ),
        ScalarExpr::Literal {
            value: Value::Int64(v),
            ..
        } if v >= 0 => usize::try_from(v).map_err(|_| {
            PlanError::TypeMismatch(format!(
                "{func_name}: argument {arg_index} value {v} exceeds usize range"
            ))
        }),
        _ => Err(PlanError::TypeMismatch(format!(
            "{func_name}: argument {arg_index} must be a non-negative integer literal"
        ))),
    }
}

/// Return the [`DataType`] of the column appended by a window function.
fn window_func_result_type(func: &LogicalWindowFunc) -> DataType {
    match func {
        LogicalWindowFunc::RowNumber
        | LogicalWindowFunc::Rank
        | LogicalWindowFunc::DenseRank
        | LogicalWindowFunc::Ntile(_) => DataType::Int64,
        LogicalWindowFunc::Lag { expr, .. }
        | LogicalWindowFunc::Lead { expr, .. }
        | LogicalWindowFunc::FirstValue(expr)
        | LogicalWindowFunc::LastValue(expr)
        | LogicalWindowFunc::NthValue { expr, .. } => expr.data_type(),
    }
}
