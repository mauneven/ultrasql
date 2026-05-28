//! Expression binder. Split out of `binder/mod.rs` to keep each
//! production source file under the 600-line ceiling required by
//! AGENTS.md §3 while preserving the original public surface
//! (`make_binary` for the rest of the binder).
//!
//! Every entry point is `pub(super)` so other binder submodules can
//! call it; nothing leaves the `binder` module.
//!
//! Hot helpers carry `#[inline]` so cross-module inlining (which the
//! compiler does not do for `pub` items by default in non-LTO
//! builds) preserves the perf characteristics the original
//! single-file layout had.

use ultrasql_core::{
    BitString, DataType, GeometryType, GeometryValue, MAX_VECTOR_DIMS, Oid, RangeType, RangeValue,
    Value, coerce_bpchar_text, composite_text_matches_arity, parse_decimal_text, parse_money_text,
    parse_time_text, parse_timetz_text,
};
use ultrasql_parser::ast::{BinaryOp, Expr, Literal, UnaryOp};

use super::expr_type::{binary_result_type, comparable, display_unary};
use super::{
    Catalog, PlanError, ScalarExpr, Schema, ScopeFrame, ScopeStack, bind_select_with_ctes,
    derive_agg_output_name, is_aggregate_name, is_scalar_min_max_call, plan_contains_outer_column,
};

const MICROS_PER_DAY: i64 = 86_400_000_000;

pub(super) fn bind_expr(
    expr: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    bind_expr_with_ctes(expr, input, catalog, &[], scope)
}

pub(super) fn bind_expr_with_ctes(
    expr: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Literal(lit) => Ok(bind_literal(lit)),
        Expr::Column { name } => bind_column(name, input, scope),
        Expr::Parameter { index, .. } => Ok(ScalarExpr::Parameter {
            index: *index,
            data_type: DataType::Null,
        }),
        Expr::Paren { expr, .. } => bind_expr_with_ctes(expr, input, catalog, cte_catalog, scope),
        Expr::ArrayLiteral { elements, .. } => {
            bind_array_literal(elements, input, catalog, cte_catalog, scope)
        }
        Expr::ArraySubscript {
            expr: array_expr,
            index,
            ..
        } => bind_array_subscript(array_expr, index, input, catalog, cte_catalog, scope),
        Expr::Unary {
            op, expr: inner, ..
        } => bind_unary(*op, inner, input, catalog, cte_catalog, scope),
        Expr::Binary {
            op, left, right, ..
        } => bind_binary(*op, left, right, input, catalog, cte_catalog, scope),
        Expr::IsNull { expr, negated, .. } => Ok(ScalarExpr::IsNull {
            expr: Box::new(bind_expr_with_ctes(
                expr,
                input,
                catalog,
                cte_catalog,
                scope,
            )?),
            negated: *negated,
        }),
        Expr::Call {
            name,
            args,
            distinct,
            within_group,
            over,
            ..
        } => {
            // If this is a known aggregate and we have an aggregate output schema,
            // try to resolve it as a column reference into that schema.
            let func_name = name
                .parts
                .last()
                .map_or("", |p| p.value.as_str())
                .to_ascii_lowercase();
            let scalar_min_max = is_scalar_min_max_call(
                &func_name,
                args.len(),
                *distinct,
                within_group.is_some(),
                over.is_some(),
            );
            if is_aggregate_name(&func_name) && !scalar_min_max {
                let agg_col_name =
                    derive_agg_output_name(&func_name, args, within_group.as_deref());
                if let Some((i, f)) = input.find(&agg_col_name) {
                    return Ok(ScalarExpr::Column {
                        name: f.name.clone(),
                        index: i,
                        data_type: f.data_type.clone(),
                    });
                }
                // If not found by derived name, try to find any column matching
                // the function name prefix (e.g. "count" matches "count").
                if let Some((i, f)) = input.find(&func_name) {
                    return Ok(ScalarExpr::Column {
                        name: f.name.clone(),
                        index: i,
                        data_type: f.data_type.clone(),
                    });
                }
                return Err(PlanError::NotSupported(
                    "aggregate call outside aggregate context",
                ));
            }
            // Scalar builtin dispatch — bind every argument then emit
            // a `ScalarExpr::FunctionCall` the executor knows how to
            // evaluate. The v0.6 milestone covers the set TPC-H asks
            // for: `extract(unit, source)` (year/month/day/quarter),
            // `substring(text, from[, for])`. Unknown function names
            // surface the standard `non-aggregate function calls`
            // error so the binder stays strict.
            let bound_args: Result<Vec<ScalarExpr>, PlanError> = args
                .iter()
                .map(|a| bind_expr_with_ctes(a, input, catalog, cte_catalog, scope))
                .collect();
            let mut bound_args = bound_args?;
            validate_builtin_args(&func_name, &mut bound_args)?;
            let return_type = builtin_return_type(&func_name, &bound_args)?;
            coerce_common_builtin_args(&func_name, &mut bound_args, &return_type);
            Ok(ScalarExpr::FunctionCall {
                name: func_name,
                args: bound_args,
                data_type: return_type,
            })
        }
        Expr::Row { fields, .. } => {
            let bound_args: Result<Vec<ScalarExpr>, PlanError> = fields
                .iter()
                .map(|field| bind_expr_with_ctes(field, input, catalog, cte_catalog, scope))
                .collect();
            let bound_args = bound_args?;
            let record_type = DataType::Record(
                bound_args
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| (format!("f{}", idx + 1), expr.data_type()))
                    .collect(),
            );
            Ok(ScalarExpr::FunctionCall {
                name: "row".to_owned(),
                args: bound_args,
                data_type: record_type,
            })
        }
        Expr::Cast {
            expr: inner,
            target,
            ..
        }
        | Expr::PostfixCast {
            expr: inner,
            target,
            ..
        } => bind_cast_expr(inner, target, input, catalog, cte_catalog, scope),

        // ------------------------------------------------------------------
        // Subquery variants
        // ------------------------------------------------------------------

        // Scalar subquery: `(SELECT col FROM …)`.
        //
        // The inner plan must project exactly one column; otherwise the
        // binder returns [`PlanError::TypeMismatch`].
        //
        // Push `input` as an outer scope frame so that correlated column
        // references inside the inner SELECT resolve to the outer query's
        // columns at `frame_depth = 1`.
        Expr::Subquery {
            select: inner_select,
            ..
        } => {
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, cte_catalog, scope);
            scope.pop();
            let inner_plan = inner_result?;
            if inner_plan.schema().len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "scalar subquery must return exactly 1 column, got {}",
                    inner_plan.schema().len()
                )));
            }
            let data_type = inner_plan.schema().field_at(0).data_type.clone();
            let correlated = plan_contains_outer_column(&inner_plan);
            Ok(ScalarExpr::ScalarSubquery {
                subplan: Box::new(inner_plan),
                correlated,
                data_type,
            })
        }

        // `[NOT] EXISTS (SELECT …)`.
        Expr::Exists {
            select: inner_select,
            negated,
            ..
        } => {
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, cte_catalog, scope);
            scope.pop();
            let inner_plan = inner_result?;
            let correlated = plan_contains_outer_column(&inner_plan);
            Ok(ScalarExpr::Exists {
                subplan: Box::new(inner_plan),
                negated: *negated,
                correlated,
            })
        }

        // `expr [NOT] IN (SELECT single_col …)`.
        Expr::InSubquery {
            expr: lhs_ast,
            select: inner_select,
            negated,
            ..
        } => {
            let lhs = bind_expr_with_ctes(lhs_ast, input, catalog, cte_catalog, scope)?;
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, cte_catalog, scope);
            scope.pop();
            let inner_plan = inner_result?;
            if inner_plan.schema().len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "IN subquery must return exactly 1 column, got {}",
                    inner_plan.schema().len()
                )));
            }
            let inner_type = inner_plan.schema().field_at(0).data_type.clone();
            if !comparable(&lhs.data_type(), &inner_type) {
                return Err(PlanError::TypeMismatch(format!(
                    "IN subquery: left type {} is not comparable to subquery column type {}",
                    lhs.data_type(),
                    inner_type,
                )));
            }
            let correlated = plan_contains_outer_column(&inner_plan);
            Ok(ScalarExpr::InSubquery {
                expr: Box::new(lhs),
                subplan: Box::new(inner_plan),
                negated: *negated,
                correlated,
                data_type: inner_type,
            })
        }

        // `expr = ANY (SELECT …)` — lowered to `InSubquery` with negated=false.
        //
        // Only `=` is supported; any other operator returns
        // [`PlanError::NotSupported`].
        Expr::Any {
            expr: lhs_ast,
            op,
            select: inner_select,
            ..
        } => {
            if *op != BinaryOp::Eq {
                return Err(PlanError::NotSupported(
                    "ANY with non-equality operator (only `= ANY` is supported)",
                ));
            }
            let lhs = bind_expr_with_ctes(lhs_ast, input, catalog, cte_catalog, scope)?;
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, cte_catalog, scope);
            scope.pop();
            let inner_plan = inner_result?;
            if inner_plan.schema().len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "= ANY subquery must return exactly 1 column, got {}",
                    inner_plan.schema().len()
                )));
            }
            let inner_type = inner_plan.schema().field_at(0).data_type.clone();
            let correlated = plan_contains_outer_column(&inner_plan);
            Ok(ScalarExpr::InSubquery {
                expr: Box::new(lhs),
                subplan: Box::new(inner_plan),
                negated: false,
                correlated,
                data_type: inner_type,
            })
        }
        Expr::AnyArray {
            expr: lhs_ast,
            op,
            array,
            ..
        } => {
            if *op != BinaryOp::Eq {
                return Err(PlanError::NotSupported(
                    "ANY with non-equality operator (only `= ANY` is supported)",
                ));
            }
            let lhs = bind_expr_with_ctes(lhs_ast, input, catalog, cte_catalog, scope)?;
            let array = bind_expr_with_ctes(array, input, catalog, cte_catalog, scope)?;
            let DataType::Array(element_type) = array.data_type() else {
                return Err(PlanError::TypeMismatch(format!(
                    "= ANY array expression requires array argument, got {}",
                    array.data_type()
                )));
            };
            if !comparable(&lhs.data_type(), &element_type) {
                return Err(PlanError::TypeMismatch(format!(
                    "= ANY array: left type {} is not comparable to array element type {}",
                    lhs.data_type(),
                    element_type
                )));
            }
            Ok(ScalarExpr::FunctionCall {
                name: "__ultrasql_eq_any_array".to_owned(),
                args: vec![lhs, array],
                data_type: DataType::Bool,
            })
        }

        // `ALL (SELECT …)` — not supported at this layer.
        Expr::All { .. } => Err(PlanError::NotSupported(
            "ALL subquery expressions are not supported",
        )),

        // `expr [NOT] BETWEEN [SYMMETRIC] low AND high` is rewritten at
        // bind time into an equivalent boolean tree of comparisons.
        // SQL:2016 specifies the equivalence; PostgreSQL's planner uses
        // the same rewrite.
        Expr::Between {
            expr: subject,
            low,
            high,
            negated,
            symmetric,
            ..
        } => bind_between(
            subject,
            low,
            high,
            *negated,
            *symmetric,
            input,
            catalog,
            cte_catalog,
            scope,
        ),

        // `CASE [operand] WHEN c THEN v … ELSE e END` lowers to a
        // `case` builtin so the executor's function dispatcher can
        // evaluate it row-at-a-time. The argument layout is:
        //
        // - searched CASE: `[cond1, then1, cond2, then2, …, else]`
        // - simple CASE:   `[operand, when1, then1, when2, then2, …, else]`
        //
        // The else slot is always present; an absent SQL ELSE is
        // encoded as a `NULL` literal so the dispatcher does not need
        // to special-case the missing-tail shape.
        Expr::Case {
            operand,
            branches,
            else_expr,
            ..
        } => {
            let mut bound_args: Vec<ScalarExpr> = Vec::with_capacity(branches.len() * 2 + 2);
            let case_kind = if let Some(op_expr) = operand {
                bound_args.push(bind_expr_with_ctes(
                    op_expr,
                    input,
                    catalog,
                    cte_catalog,
                    scope,
                )?);
                "case_simple"
            } else {
                "case_searched"
            };
            let mut result_type = DataType::Null;
            for (when_e, then_e) in branches {
                bound_args.push(bind_expr_with_ctes(
                    when_e,
                    input,
                    catalog,
                    cte_catalog,
                    scope,
                )?);
                let then_bound = bind_expr_with_ctes(then_e, input, catalog, cte_catalog, scope)?;
                if matches!(result_type, DataType::Null) {
                    result_type = then_bound.data_type();
                }
                bound_args.push(then_bound);
            }
            if let Some(else_e) = else_expr {
                let bound = bind_expr_with_ctes(else_e, input, catalog, cte_catalog, scope)?;
                if matches!(result_type, DataType::Null) {
                    result_type = bound.data_type();
                }
                bound_args.push(bound);
            } else {
                bound_args.push(ScalarExpr::Literal {
                    value: Value::Null,
                    data_type: DataType::Null,
                });
            }
            Ok(ScalarExpr::FunctionCall {
                name: case_kind.to_owned(),
                args: bound_args,
                data_type: result_type,
            })
        }

        // `expr [NOT] IN (val, …)` → chain of `OR`-joined equality
        // comparisons. NOT IN flips to `AND`-joined `<>`.
        Expr::InList {
            expr: subject,
            items,
            negated,
            ..
        } => {
            let bound_subject = bind_expr_with_ctes(subject, input, catalog, cte_catalog, scope)?;
            let mut acc: Option<ScalarExpr> = None;
            for item in items {
                let bound_item = bind_expr_with_ctes(item, input, catalog, cte_catalog, scope)?;
                let cmp = ScalarExpr::Binary {
                    op: if *negated {
                        ultrasql_parser::ast::BinaryOp::NotEq
                    } else {
                        ultrasql_parser::ast::BinaryOp::Eq
                    },
                    left: Box::new(bound_subject.clone()),
                    right: Box::new(bound_item),
                    data_type: DataType::Bool,
                };
                acc = Some(match acc {
                    None => cmp,
                    Some(prev) => ScalarExpr::Binary {
                        op: if *negated {
                            ultrasql_parser::ast::BinaryOp::And
                        } else {
                            ultrasql_parser::ast::BinaryOp::Or
                        },
                        left: Box::new(prev),
                        right: Box::new(cmp),
                        data_type: DataType::Bool,
                    },
                });
            }
            Ok(acc.unwrap_or(ScalarExpr::Literal {
                value: Value::Bool(*negated),
                data_type: DataType::Bool,
            }))
        }

        // `COALESCE(a, b, …)` → `coalesce(args...)` builtin: return
        // the first non-NULL argument.
        Expr::Coalesce { args, .. } => {
            let bound_args: Result<Vec<_>, PlanError> = args
                .iter()
                .map(|a| bind_expr_with_ctes(a, input, catalog, cte_catalog, scope))
                .collect();
            let bound_args = bound_args?;
            let return_type = common_scalar_return_type("coalesce", &bound_args)?;
            let mut bound_args = bound_args;
            coerce_args_to_common_type(&mut bound_args, &return_type);
            Ok(ScalarExpr::FunctionCall {
                name: "coalesce".to_owned(),
                args: bound_args,
                data_type: return_type,
            })
        }

        Expr::NullIf { a, b, .. } => {
            let mut left = bind_expr_with_ctes(a, input, catalog, cte_catalog, scope)?;
            let mut right = bind_expr_with_ctes(b, input, catalog, cte_catalog, scope)?;
            coerce_literal_to_match(&mut left, &mut right);
            let left_type = left.data_type();
            let right_type = right.data_type();
            if !comparable(&left_type, &right_type) {
                return Err(PlanError::TypeMismatch(format!(
                    "nullif: cannot compare {left_type} and {right_type}"
                )));
            }
            Ok(ScalarExpr::FunctionCall {
                name: "nullif".to_owned(),
                args: vec![left, right],
                data_type: left_type,
            })
        }

        Expr::Greatest { args, .. } => {
            bind_extremum_expr("greatest", args, input, catalog, cte_catalog, scope)
        }

        Expr::Least { args, .. } => {
            bind_extremum_expr("least", args, input, catalog, cte_catalog, scope)
        }

        _ => Err(PlanError::NotSupported("expression variant")),
    }
}

fn bind_extremum_expr(
    func_name: &str,
    args: &[Expr],
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let bound_args: Result<Vec<_>, PlanError> = args
        .iter()
        .map(|a| bind_expr_with_ctes(a, input, catalog, cte_catalog, scope))
        .collect();
    let mut bound_args = bound_args?;
    let return_type = common_scalar_return_type(func_name, &bound_args)?;
    coerce_args_to_common_type(&mut bound_args, &return_type);
    Ok(ScalarExpr::FunctionCall {
        name: func_name.to_owned(),
        args: bound_args,
        data_type: return_type,
    })
}

fn common_scalar_return_type(func_name: &str, args: &[ScalarExpr]) -> Result<DataType, PlanError> {
    if args.is_empty() {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected at least 1 argument, got 0"
        )));
    }
    args.iter()
        .map(ScalarExpr::data_type)
        .try_fold(DataType::Null, |acc, data_type| {
            common_scalar_pair_type(func_name, &acc, &data_type)
        })
}

fn common_scalar_pair_type(
    func_name: &str,
    left: &DataType,
    right: &DataType,
) -> Result<DataType, PlanError> {
    if left == right || matches!(right, DataType::Null) {
        return Ok(left.clone());
    }
    if matches!(left, DataType::Null) {
        return Ok(right.clone());
    }
    if left.is_numeric() && right.is_numeric() {
        return left.numeric_join(right).map_err(|_| {
            PlanError::TypeMismatch(format!(
                "{func_name}: arguments must share a numeric type, got {left} and {right}"
            ))
        });
    }
    if left.is_textlike() && right.is_textlike() {
        return Ok(DataType::Text { max_len: None });
    }
    if matches!(
        (left, right),
        (DataType::Json, DataType::Jsonb) | (DataType::Jsonb, DataType::Json)
    ) {
        return Ok(DataType::Jsonb);
    }
    if comparable(left, right) {
        return Ok(left.clone());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: arguments must share a comparable type, got {left} and {right}"
    )))
}

fn coerce_args_to_common_type(args: &mut [ScalarExpr], target: &DataType) {
    for arg in args {
        coerce_literal_to_type(arg, target);
    }
}

fn coerce_common_builtin_args(func_name: &str, args: &mut [ScalarExpr], target: &DataType) {
    if matches!(
        func_name,
        "ifnull" | "nvl" | "least" | "greatest" | "min" | "max"
    ) {
        coerce_args_to_common_type(args, target);
    }
}

fn bind_cast_expr(
    inner: &Expr,
    target: &ultrasql_parser::ast::Identifier,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let target_type = resolve_cast_type_with_catalog(&target.value, catalog).ok_or(
        PlanError::NotSupported("CAST target type is not implemented"),
    )?;
    let mut bound = bind_expr_with_ctes(inner, input, catalog, cte_catalog, scope)?;
    if coerce_literal_to_bpchar(&mut bound, &target_type, true) {
        return Ok(bound);
    }
    if coerce_literal_to_bit_string(&mut bound, &target_type, true) {
        return Ok(bound);
    }
    if coerce_literal_to_oid_alias_with_catalog(&mut bound, &target_type, catalog) {
        return Ok(bound);
    }
    coerce_literal_to_type(&mut bound, &target_type);
    let actual_type = bound.data_type();
    if let Some(runtime_cast) = bind_runtime_cast(bound.clone(), &target_type, &actual_type) {
        return Ok(runtime_cast);
    }
    if cast_result_matches(&target_type, &actual_type) || matches!(actual_type, DataType::Null) {
        return Ok(bound);
    }
    if target_type.is_vector_family() {
        return Err(PlanError::TypeMismatch(format!(
            "cannot cast {} to {target_type}",
            actual_type
        )));
    }
    Err(PlanError::NotSupported(
        "non-literal CAST expressions are not implemented",
    ))
}

fn bind_runtime_cast(
    expr: ScalarExpr,
    target_type: &DataType,
    actual_type: &DataType,
) -> Option<ScalarExpr> {
    let name = match target_type {
        DataType::Oid if actual_type.is_oid_alias() || actual_type.is_integer() => {
            "__ultrasql_cast_oid"
        }
        DataType::RegClass if actual_type.is_oid_alias() || actual_type.is_integer() => {
            "__ultrasql_cast_regclass"
        }
        DataType::RegType if actual_type.is_oid_alias() || actual_type.is_integer() => {
            "__ultrasql_cast_regtype"
        }
        DataType::Text { .. }
            if actual_type.is_oid_alias() || matches!(actual_type, DataType::PgLsn) =>
        {
            "__ultrasql_cast_text"
        }
        _ => return None,
    };
    Some(ScalarExpr::FunctionCall {
        name: name.to_owned(),
        args: vec![expr],
        data_type: target_type.clone(),
    })
}

/// Bind `expr [NOT] BETWEEN [SYMMETRIC] low AND high` into an equivalent
/// boolean tree over the existing comparison and boolean operators.
///
/// The rewrites mirror the SQL:2016 specification and PostgreSQL's
/// planner behaviour:
///
/// - `expr BETWEEN low AND high` ⇒ `expr >= low AND expr <= high`.
/// - `expr NOT BETWEEN low AND high` ⇒ `expr < low OR expr > high`.
/// - `expr BETWEEN SYMMETRIC low AND high` ⇒
///   `(expr >= low AND expr <= high) OR (expr >= high AND expr <= low)`.
/// - `expr NOT BETWEEN SYMMETRIC low AND high` ⇒
///   `(expr < low OR expr > high) AND (expr < high OR expr > low)`.
///
/// Each of `expr`, `low`, and `high` is bound exactly once; the bound
/// `expr` is cloned wherever the rewrite needs an additional reference
/// to it. This means side-effectful expressions (function calls,
/// sequence next-val, etc.) are evaluated more than once at runtime —
/// PostgreSQL documents the same limitation and we accept it for the
/// same reason: the existing comparison + boolean operators already
/// flow through the SIMD-aware [`crate::expr::ScalarExpr::Binary`]
/// pipeline, and synthesising a Let-style binding would grow the plan
/// language for no measurable benefit on the SQL surface UltraSQL
/// implements today (pure column / literal predicates).
#[allow(clippy::too_many_arguments)]
pub(super) fn bind_between(
    subject: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    symmetric: bool,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let bound_expr = bind_expr_with_ctes(subject, input, catalog, cte_catalog, scope)?;
    let bound_low = bind_expr_with_ctes(low, input, catalog, cte_catalog, scope)?;
    let bound_high = bind_expr_with_ctes(high, input, catalog, cte_catalog, scope)?;

    // The forward range test: `expr >= low AND expr <= high`.
    let forward = make_range_test(
        bound_expr.clone(),
        bound_low.clone(),
        bound_high.clone(),
        negated,
    )?;
    if !symmetric {
        return Ok(forward);
    }
    // The reversed range test, with low/high swapped. The combining
    // connective is `OR` for the affirmative form (a value satisfies
    // either ordering) and `AND` for the negated form (the value lies
    // outside both ranges).
    let reversed = make_range_test(bound_expr, bound_high, bound_low, negated)?;
    let combine_op = if negated { BinaryOp::And } else { BinaryOp::Or };
    Ok(ScalarExpr::Binary {
        op: combine_op,
        left: Box::new(forward),
        right: Box::new(reversed),
        data_type: DataType::Bool,
    })
}

/// Build one bound boolean predicate of the form
/// `expr op_low low <connect> expr op_high high`, where the operators
/// are picked by `negated`:
///
/// - `negated = false` → `expr >= low AND expr <= high`.
/// - `negated = true`  → `expr <  low OR  expr >  high`.
///
/// The two comparison subterms are validated through
/// [`binary_result_type`] so that type errors (e.g. comparing a text
/// column to an integer bound) surface as
/// [`PlanError::TypeMismatch`], matching the diagnostics callers see
/// from an explicit `expr >= low AND expr <= high` predicate.
pub(super) fn make_range_test(
    bound_expr: ScalarExpr,
    bound_low: ScalarExpr,
    bound_high: ScalarExpr,
    negated: bool,
) -> Result<ScalarExpr, PlanError> {
    let (lo_op, hi_op, connect) = if negated {
        (BinaryOp::Lt, BinaryOp::Gt, BinaryOp::Or)
    } else {
        (BinaryOp::GtEq, BinaryOp::LtEq, BinaryOp::And)
    };
    let lo_cmp = make_binary(lo_op, bound_expr.clone(), bound_low)?;
    let hi_cmp = make_binary(hi_op, bound_expr, bound_high)?;
    Ok(ScalarExpr::Binary {
        op: connect,
        left: Box::new(lo_cmp),
        right: Box::new(hi_cmp),
        data_type: DataType::Bool,
    })
}

/// Construct a [`ScalarExpr::Binary`] over already-bound operands.
///
/// The operands' types are checked via [`binary_result_type`] exactly
/// as in [`bind_binary`], so the rewrite produces the same diagnostics
/// callers would see from the explicit `>=` / `<=` / `<` / `>` form.
pub(super) fn make_binary(
    op: BinaryOp,
    mut left: ScalarExpr,
    mut right: ScalarExpr,
) -> Result<ScalarExpr, PlanError> {
    if !binary_operator_uses_raw_text_pattern(op) {
        coerce_literal_to_match(&mut left, &mut right);
    }
    let data_type = binary_result_type(op, left.data_type(), right.data_type())?;
    Ok(ScalarExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
        data_type,
    })
}

pub(super) fn bind_literal(lit: &Literal) -> ScalarExpr {
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
        Literal::Float { text, .. } => bind_numeric_literal(text),
        Literal::String { value, .. } => ScalarExpr::Literal {
            value: Value::Text(value.clone()),
            data_type: DataType::Text { max_len: None },
        },
        Literal::Typed {
            type_name,
            value,
            unit,
            ..
        } => bind_typed_literal(type_name, value, unit.as_deref()),
        // `Literal::Null` and any future non-exhaustive variant both
        // bind to a NULL placeholder; later passes specialize.
        _ => ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        },
    }
}

fn bind_array_literal(
    elements: &[Expr],
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let mut bound_elements = Vec::with_capacity(elements.len());
    let mut element_type: Option<DataType> = None;
    for element in elements {
        let bound = bind_expr_with_ctes(element, input, catalog, cte_catalog, scope)?;
        let ScalarExpr::Literal { data_type, .. } = &bound else {
            return Err(PlanError::TypeMismatch(
                "array literal elements must be constant expressions".to_owned(),
            ));
        };
        if !matches!(data_type, DataType::Null) {
            element_type = Some(if let Some(expected) = element_type {
                common_array_element_type(&expected, data_type)?
            } else {
                data_type.clone()
            });
        }
        bound_elements.push(bound);
    }

    let element_type = element_type.unwrap_or(DataType::Null);
    let mut values = Vec::with_capacity(elements.len());
    for mut bound in bound_elements {
        coerce_literal_to_type(&mut bound, &element_type);
        let ScalarExpr::Literal { value, data_type } = bound else {
            return Err(PlanError::TypeMismatch(
                "array literal elements must be constant expressions".to_owned(),
            ));
        };
        if !matches!(data_type, DataType::Null) && data_type != element_type {
            return Err(PlanError::TypeMismatch(
                "array literal elements must share one type".to_owned(),
            ));
        }
        values.push(value);
    }
    let value = Value::Array {
        element_type: element_type.clone(),
        elements: values,
    };
    if value.array_dimensions().is_none() {
        return Err(PlanError::TypeMismatch(
            "multi-dimensional array literal must be rectangular".to_owned(),
        ));
    }
    Ok(ScalarExpr::Literal {
        value,
        data_type: DataType::Array(Box::new(element_type)),
    })
}

fn common_array_element_type(left: &DataType, right: &DataType) -> Result<DataType, PlanError> {
    if left == right || matches!(right, DataType::Null) {
        return Ok(left.clone());
    }
    if matches!(left, DataType::Null) {
        return Ok(right.clone());
    }
    match (left, right) {
        (DataType::Array(left_inner), DataType::Array(right_inner)) => {
            common_array_element_type(left_inner, right_inner)
                .map(|inner| DataType::Array(Box::new(inner)))
        }
        (DataType::Array(_), _) | (_, DataType::Array(_)) => Err(PlanError::TypeMismatch(
            "array literal dimensions must match".to_owned(),
        )),
        _ if left.is_numeric() && right.is_numeric() => left.numeric_join(right).map_err(|_| {
            PlanError::TypeMismatch(format!(
                "array literal elements must share a coercible type, got {left} and {right}"
            ))
        }),
        _ if left.is_textlike() && right.is_textlike() => Ok(DataType::Text { max_len: None }),
        (DataType::Json, DataType::Jsonb) | (DataType::Jsonb, DataType::Json) => {
            Ok(DataType::Jsonb)
        }
        _ => Err(PlanError::TypeMismatch(format!(
            "array literal elements must share a coercible type, got {left} and {right}"
        ))),
    }
}

/// Convert a `TYPENAME 'literal'` AST node into the matching
/// [`ScalarExpr::Literal`].
///
/// Supported today:
/// - `DATE 'YYYY-MM-DD'` → `Value::Date(days_since_2000_01_01)`.
/// - `INTERVAL 'n' YEAR|MONTH|DAY|HOUR|MINUTE|SECOND` →
///   `Value::Interval { months, days, microseconds }`.
///
/// Unsupported variants (TIME, TIMESTAMP, TIMESTAMPTZ, complex
/// interval syntaxes) bind to NULL today so the binder does not reject
/// queries upstream of the executor.
fn bind_typed_literal(type_name: &str, value: &str, unit: Option<&str>) -> ScalarExpr {
    let type_name = type_name.to_ascii_lowercase();
    if let Some(target) = parse_vector_family_type_name(&type_name) {
        return bind_vector_family_literal(value, target);
    }
    if matches!(type_name.as_str(), "bit" | "varbit" | "bit varying") {
        return bind_bit_string_literal(value, type_name.as_str());
    }
    if let Some(target) = parse_network_type_name(&type_name) {
        return bind_network_literal(value, target);
    }
    match type_name.as_str() {
        "date" => match parse_date_literal(value) {
            Some(days) => ScalarExpr::Literal {
                value: Value::Date(days),
                data_type: DataType::Date,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Date,
            },
        },
        "interval" => match parse_interval_literal(value, unit) {
            Some((months, days, microseconds)) => ScalarExpr::Literal {
                value: Value::Interval {
                    months,
                    days,
                    microseconds,
                },
                data_type: DataType::Interval,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Interval,
            },
        },
        "time" => match parse_time_of_day_micros(value) {
            Some(micros) => ScalarExpr::Literal {
                value: Value::Time(micros),
                data_type: DataType::Time,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Time,
            },
        },
        "timetz" | "time with time zone" => match parse_timetz_literal(value) {
            Some((micros, offset_seconds)) => ScalarExpr::Literal {
                value: Value::TimeTz {
                    micros,
                    offset_seconds,
                },
                data_type: DataType::TimeTz,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::TimeTz,
            },
        },
        "json" => match validate_json_text(value) {
            Some(text) => ScalarExpr::Literal {
                value: Value::Json(text),
                data_type: DataType::Json,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Json,
            },
        },
        "jsonb" => match normalize_jsonb_text(value) {
            Some(text) => ScalarExpr::Literal {
                value: Value::Jsonb(text),
                data_type: DataType::Jsonb,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Jsonb,
            },
        },
        "xml" => match Value::validate_xml_text(value) {
            Some(text) => ScalarExpr::Literal {
                value: Value::Xml(text),
                data_type: DataType::Xml,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Xml,
            },
        },
        "money" => match parse_money_text(value) {
            Ok(money) => ScalarExpr::Literal {
                value: money,
                data_type: DataType::Money,
            },
            Err(_) => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Money,
            },
        },
        "oid" => match Value::parse_oid_text(value) {
            Some(oid) => ScalarExpr::Literal {
                value: Value::Oid(oid),
                data_type: DataType::Oid,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Oid,
            },
        },
        "pg_lsn" => match Value::parse_pg_lsn_text(value) {
            Some(lsn) => ScalarExpr::Literal {
                value: Value::PgLsn(lsn),
                data_type: DataType::PgLsn,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::PgLsn,
            },
        },
        "timestamp" => match parse_timestamp_literal(value) {
            Some(micros) => ScalarExpr::Literal {
                value: Value::Timestamp(micros),
                data_type: DataType::Timestamp,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::Timestamp,
            },
        },
        "timestamptz" | "timestamp with time zone" => match parse_timestamptz_literal(value) {
            Some(micros) => ScalarExpr::Literal {
                value: Value::TimestampTz(micros),
                data_type: DataType::TimestampTz,
            },
            None => ScalarExpr::Literal {
                value: Value::Null,
                data_type: DataType::TimestampTz,
            },
        },
        "tsvector" | "tsquery" => ScalarExpr::Literal {
            value: Value::Text(value.to_owned()),
            data_type: DataType::Text { max_len: None },
        },
        _ => ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        },
    }
}

fn validate_json_text(value: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(value).ok()?;
    Some(value.to_owned())
}

fn normalize_jsonb_text(value: &str) -> Option<String> {
    let parsed = serde_json::from_str::<serde_json::Value>(value).ok()?;
    serde_json::to_string(&parsed).ok()
}

fn bind_bit_string_literal(value: &str, type_name: &str) -> ScalarExpr {
    let Some(Value::BitString(bits)) = Value::parse_bit_string(value) else {
        return ScalarExpr::Literal {
            value: Value::Null,
            data_type: if type_name == "bit" {
                DataType::Bit { len: None }
            } else {
                DataType::VarBit { max_len: None }
            },
        };
    };
    let len = Some(bits.len());
    ScalarExpr::Literal {
        value: Value::BitString(bits),
        data_type: if type_name == "bit" {
            DataType::Bit { len }
        } else {
            DataType::VarBit { max_len: len }
        },
    }
}

fn bind_network_literal(value: &str, data_type: DataType) -> ScalarExpr {
    let parsed =
        Value::parse_network(&data_type, value).unwrap_or_else(|| Value::Text(value.to_owned()));
    ScalarExpr::Literal {
        value: parsed,
        data_type,
    }
}

fn bind_vector_family_literal(value: &str, declared_type: DataType) -> ScalarExpr {
    let parsed = match declared_type {
        DataType::Vector { .. } => Value::parse_vector(value),
        DataType::HalfVec { .. } => Value::parse_halfvec(value),
        DataType::SparseVec { .. } => Value::parse_sparsevec(value),
        DataType::BitVec { .. } => Value::parse_bitvec(value),
        _ => None,
    };
    let Some(parsed) = parsed else {
        return ScalarExpr::Literal {
            value: Value::Null,
            data_type: declared_type,
        };
    };
    let actual_type = parsed.data_type();
    if !vector_family_cast_matches(&declared_type, &actual_type) {
        return ScalarExpr::Literal {
            value: Value::Null,
            data_type: declared_type,
        };
    }
    ScalarExpr::Literal {
        value: parsed,
        data_type: actual_type,
    }
}

fn parse_interval_literal(text: &str, unit: Option<&str>) -> Option<(i32, i32, i64)> {
    let magnitude = text.trim();
    let unit = unit?.to_ascii_lowercase();
    match unit.as_str() {
        "year" | "years" => {
            let years: i32 = magnitude.parse().ok()?;
            Some((years.checked_mul(12)?, 0, 0))
        }
        "month" | "months" => {
            let months: i32 = magnitude.parse().ok()?;
            Some((months, 0, 0))
        }
        "day" | "days" => {
            let days: i32 = magnitude.parse().ok()?;
            Some((0, days, 0))
        }
        "hour" | "hours" => {
            let hours: i64 = magnitude.parse().ok()?;
            Some((0, 0, hours.checked_mul(3_600_000_000)?))
        }
        "minute" | "minutes" => {
            let minutes: i64 = magnitude.parse().ok()?;
            Some((0, 0, minutes.checked_mul(60_000_000)?))
        }
        "second" | "seconds" => {
            let seconds: i64 = magnitude.parse().ok()?;
            Some((0, 0, seconds.checked_mul(1_000_000)?))
        }
        _ => None,
    }
}

/// Parse `YYYY-MM-DD` into days since 2000-01-01.
///
/// Uses the Howard Hinnant `civil_from_days` inverse, valid for any
/// Gregorian date the engine cares about. Returns `None` on
/// malformed input; the binder maps that to a typed NULL so the
/// downstream comparator still sees a `Date` typed expression.
fn parse_date_literal(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    if trimmed.len() < 10 {
        return None;
    }
    let bytes = trimmed.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year: i32 = std::str::from_utf8(&bytes[..4]).ok()?.parse().ok()?;
    let month: u32 = std::str::from_utf8(&bytes[5..7]).ok()?.parse().ok()?;
    let day: u32 = std::str::from_utf8(&bytes[8..10]).ok()?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(days_since_epoch(year, month, day))
}

fn parse_timestamp_literal(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    let split = trimmed.find(' ').or_else(|| trimmed.find('T'))?;
    let date = &trimmed[..split];
    let time = &trimmed[split + 1..];
    let days = i64::from(parse_date_literal(date)?);
    let micros = parse_time_of_day_micros(time)?;
    days.checked_mul(MICROS_PER_DAY)?.checked_add(micros)
}

fn parse_timestamptz_literal(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    let split = trimmed.find(' ').or_else(|| trimmed.find('T'))?;
    let date = &trimmed[..split];
    let time = &trimmed[split + 1..];
    let days = i64::from(parse_date_literal(date)?);
    let (micros, offset_seconds) = parse_timetz_literal(time)?;
    days.checked_mul(MICROS_PER_DAY)?
        .checked_add(micros)?
        .checked_sub(i64::from(offset_seconds).checked_mul(1_000_000)?)
}

fn parse_time_of_day_micros(text: &str) -> Option<i64> {
    parse_time_text(text)
}

fn parse_timetz_literal(text: &str) -> Option<(i64, i32)> {
    parse_timetz_text(text)
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "civil-from-days arithmetic; doe / yoe fit in i32 by construction"
)]
fn civil_from_days(days_since_2000_01_01: i32) -> (i32, u32, u32) {
    let z = days_since_2000_01_01 + 10_957;
    let z = z + 719_468;
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i32) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month_i32 = if mp < 10 {
        mp as i32 + 3
    } else {
        mp as i32 - 9
    };
    let year = if month_i32 <= 2 { y + 1 } else { y };
    let month =
        u32::try_from(month_i32).expect("civil_from_days month stays in 1..=12 by construction");
    (year, month, day)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 31,
    }
}

fn add_months_to_date(date_days: i32, month_delta: i32) -> Result<i32, PlanError> {
    let (year, month, day) = civil_from_days(date_days);
    let total_months = year
        .checked_mul(12)
        .and_then(|v| v.checked_add(i32::try_from(month).ok()? - 1))
        .and_then(|v| v.checked_add(month_delta))
        .ok_or_else(|| PlanError::TypeMismatch("date interval month overflow".to_owned()))?;
    let new_year = total_months.div_euclid(12);
    let new_month = u32::try_from(total_months.rem_euclid(12) + 1)
        .map_err(|_| PlanError::TypeMismatch("date interval month overflow".to_owned()))?;
    let new_day = day.min(days_in_month(new_year, new_month));
    Ok(days_since_epoch(new_year, new_month, new_day))
}

fn fold_date_interval(
    date_days: i32,
    month_delta: i32,
    day_delta: i32,
    microsecond_delta: i64,
) -> Result<ScalarExpr, PlanError> {
    let shifted_days = add_months_to_date(date_days, month_delta)?;
    let shifted_days = shifted_days
        .checked_add(day_delta)
        .ok_or_else(|| PlanError::TypeMismatch("date interval day overflow".to_owned()))?;
    if microsecond_delta == 0 {
        return Ok(ScalarExpr::Literal {
            value: Value::Date(shifted_days),
            data_type: DataType::Date,
        });
    }
    let timestamp = i64::from(shifted_days)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|base| base.checked_add(microsecond_delta))
        .ok_or_else(|| PlanError::TypeMismatch("date interval timestamp overflow".to_owned()))?;
    Ok(ScalarExpr::Literal {
        value: Value::Timestamp(timestamp),
        data_type: DataType::Timestamp,
    })
}

fn try_fold_literal_binary(
    op: BinaryOp,
    left: &ScalarExpr,
    right: &ScalarExpr,
) -> Result<Option<ScalarExpr>, PlanError> {
    let (lv, rv) = match (left, right) {
        (ScalarExpr::Literal { value: lv, .. }, ScalarExpr::Literal { value: rv, .. }) => (lv, rv),
        _ => return Ok(None),
    };
    match (op, lv, rv) {
        (
            BinaryOp::Add,
            Value::Date(date_days),
            Value::Interval {
                months,
                days,
                microseconds,
            },
        )
        | (
            BinaryOp::Add,
            Value::Interval {
                months,
                days,
                microseconds,
            },
            Value::Date(date_days),
        ) => fold_date_interval(*date_days, *months, *days, *microseconds).map(Some),
        (
            BinaryOp::Sub,
            Value::Date(date_days),
            Value::Interval {
                months,
                days,
                microseconds,
            },
        ) => {
            let neg_months = months.checked_neg().ok_or_else(|| {
                PlanError::TypeMismatch("date interval month overflow".to_owned())
            })?;
            let neg_days = days
                .checked_neg()
                .ok_or_else(|| PlanError::TypeMismatch("date interval day overflow".to_owned()))?;
            let neg_micros = microseconds.checked_neg().ok_or_else(|| {
                PlanError::TypeMismatch("date interval microsecond overflow".to_owned())
            })?;
            fold_date_interval(*date_days, neg_months, neg_days, neg_micros).map(Some)
        }
        _ if is_float_like_literal(lv) || is_float_like_literal(rv) => {
            let Some(left_value) = literal_numeric_as_f64(lv) else {
                return Ok(None);
            };
            let Some(right_value) = literal_numeric_as_f64(rv) else {
                return Ok(None);
            };
            let folded = match op {
                BinaryOp::Add => Some(left_value + right_value),
                BinaryOp::Sub => Some(left_value - right_value),
                BinaryOp::Mul => Some(left_value * right_value),
                BinaryOp::Div if right_value != 0.0 => Some(left_value / right_value),
                _ => None,
            };
            Ok(folded.map(|value| ScalarExpr::Literal {
                value: Value::Float64(value),
                data_type: DataType::Float64,
            }))
        }
        _ => Ok(None),
    }
}

fn is_float_like_literal(value: &Value) -> bool {
    matches!(value, Value::Float32(_) | Value::Float64(_))
}

#[allow(clippy::cast_precision_loss)]
fn literal_numeric_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => Some(*v as f64),
        Value::Float32(v) => Some(f64::from(*v)),
        Value::Float64(v) => Some(*v),
        Value::Decimal {
            value: decimal_value,
            scale,
        } => Some((*decimal_value as f64) / 10_f64.powi(*scale)),
        _ => None,
    }
}

/// Statically infer the return type of a builtin scalar function.
/// The set must stay in sync with the executor's `eval_function_call`
/// dispatcher in [`crates/ultrasql-executor/src/eval.rs`].
fn builtin_return_type(func_name: &str, args: &[ScalarExpr]) -> Result<DataType, PlanError> {
    match func_name {
        "ifnull" | "nvl" => common_scalar_return_type(func_name, args),
        "nullif" => {
            if args.len() != 2 {
                return Err(PlanError::TypeMismatch(format!(
                    "{func_name}: expected 2 arguments, got {}",
                    args.len()
                )));
            }
            Ok(args[0].data_type())
        }
        "least" | "greatest" | "min" | "max" => common_scalar_return_type(func_name, args),
        "extract" => Ok(DataType::Int64),
        "current_date" | "make_date" => Ok(DataType::Date),
        "now" | "current_timestamp" | "date_trunc" | "to_timestamp" | "date_bin" => {
            Ok(DataType::TimestampTz)
        }
        "age" => Ok(DataType::Interval),
        "abs" => Ok(DataType::Int64),
        "ceil" | "floor" | "round" | "trunc" | "mod" | "power" | "sqrt" | "exp" | "ln" | "log"
        | "random" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "pi" => {
            Ok(DataType::Float64)
        }
        "length" | "position" | "bit_length" | "octet_length" | "get_bit" => Ok(DataType::Int32),
        "bit_count" => Ok(DataType::Int64),
        "set_bit" => Ok(DataType::VarBit { max_len: None }),
        "lower" | "upper" | "trim" | "lpad" | "rpad" | "left" | "right" | "substr"
        | "substring" | "replace" | "split_part" | "concat" | "concat_ws" | "repeat"
        | "reverse" | "md5" | "sha256" | "quote_ident" | "format" | "regexp_replace" => {
            Ok(DataType::Text { max_len: None })
        }
        "row_to_json" | "json_build_object" | "jsonb_set" => Ok(DataType::Jsonb),
        "jsonb_path_exists"
        | "xml_is_well_formed"
        | "xml_is_well_formed_content"
        | "xml_is_well_formed_document"
        | "xpath_exists" => Ok(DataType::Bool),
        "xpath" => Ok(DataType::Array(Box::new(DataType::Xml))),
        "pg_advisory_lock" | "pg_advisory_unlock_all" => Ok(DataType::Null),
        "pg_try_advisory_lock" | "pg_try_advisory_xact_lock" | "pg_advisory_unlock" => {
            Ok(DataType::Bool)
        }
        "has_table_privilege"
        | "has_schema_privilege"
        | "has_database_privilege"
        | "has_sequence_privilege"
        | "has_function_privilege"
        | "has_column_privilege"
        | "pg_table_is_visible"
        | "pg_is_other_temp_schema"
        | "pg_function_is_visible"
        | "pg_relation_is_publishable" => Ok(DataType::Bool),
        "pg_get_userbyid" => Ok(DataType::Text { max_len: None }),
        "to_regtype" => Ok(DataType::RegType),
        "gen_random_uuid" => Ok(DataType::Uuid),
        "pg_relation_size" => Ok(DataType::Int64),
        "current_schemas" => Ok(DataType::Array(Box::new(DataType::Text { max_len: None }))),
        "version"
        | "current_catalog"
        | "current_database"
        | "current_schema"
        | "current_user"
        | "session_user"
        | "pg_typeof"
        | "pg_size_pretty"
        | "set_config"
        | "format_type"
        | "pg_get_expr"
        | "pg_get_indexdef"
        | "pg_get_constraintdef"
        | "pg_get_statisticsobjdef_columns"
        | "pg_get_function_result"
        | "pg_get_function_arguments"
        | "pg_encoding_to_char"
        | "obj_description"
        | "shobj_description"
        | "col_description"
        | "pg_get_serial_sequence" => Ok(DataType::Text { max_len: None }),
        "array_length" => Ok(DataType::Int32),
        "array_position" => Ok(DataType::Int32),
        "array_to_string" => Ok(DataType::Text { max_len: None }),
        "string_to_array" | "array_cat" => {
            Ok(DataType::Array(Box::new(DataType::Text { max_len: None })))
        }
        "l2_distance" | "cosine_distance" | "inner_product" | "dot_product" | "l1_distance" => {
            Ok(DataType::Float64)
        }
        "hybrid_search" => Ok(DataType::Float64),
        "vector_norm" | "l2_norm" => Ok(DataType::Float64),
        "vector_dims" => Ok(DataType::Int32),
        _ => Err(PlanError::NotSupported("non-aggregate function calls")),
    }
}

/// True when the binder accepts the function name as a v0.6 builtin.
/// Used by the `_` fallback in the expression-variant path to keep
/// the diagnostic precise: unknown function names still report
/// `non-aggregate function calls`.
#[allow(dead_code)]
pub(super) fn is_supported_builtin(func_name: &str) -> bool {
    matches!(
        func_name,
        "abs"
            | "ifnull"
            | "nvl"
            | "nullif"
            | "least"
            | "greatest"
            | "extract"
            | "current_date"
            | "current_timestamp"
            | "now"
            | "age"
            | "date_trunc"
            | "to_timestamp"
            | "make_date"
            | "date_bin"
            | "ceil"
            | "floor"
            | "round"
            | "trunc"
            | "mod"
            | "power"
            | "sqrt"
            | "exp"
            | "ln"
            | "log"
            | "random"
            | "sin"
            | "cos"
            | "tan"
            | "asin"
            | "acos"
            | "atan"
            | "pi"
            | "length"
            | "bit_length"
            | "octet_length"
            | "bit_count"
            | "get_bit"
            | "set_bit"
            | "lower"
            | "upper"
            | "trim"
            | "lpad"
            | "rpad"
            | "left"
            | "right"
            | "pg_get_userbyid"
            | "to_regtype"
            | "substr"
            | "substring"
            | "position"
            | "replace"
            | "split_part"
            | "concat"
            | "concat_ws"
            | "repeat"
            | "reverse"
            | "md5"
            | "sha256"
            | "quote_ident"
            | "format"
            | "regexp_replace"
            | "row_to_json"
            | "json_build_object"
            | "jsonb_set"
            | "jsonb_path_exists"
            | "xml_is_well_formed"
            | "xml_is_well_formed_content"
            | "xml_is_well_formed_document"
            | "xpath"
            | "xpath_exists"
            | "pg_advisory_lock"
            | "pg_try_advisory_lock"
            | "pg_try_advisory_xact_lock"
            | "pg_advisory_unlock"
            | "pg_advisory_unlock_all"
            | "has_table_privilege"
            | "has_schema_privilege"
            | "has_database_privilege"
            | "has_sequence_privilege"
            | "has_function_privilege"
            | "has_column_privilege"
            | "pg_table_is_visible"
            | "pg_is_other_temp_schema"
            | "pg_function_is_visible"
            | "pg_relation_is_publishable"
            | "gen_random_uuid"
            | "version"
            | "current_catalog"
            | "current_database"
            | "current_schema"
            | "current_user"
            | "session_user"
            | "pg_typeof"
            | "set_config"
            | "format_type"
            | "pg_get_expr"
            | "pg_get_indexdef"
            | "pg_get_constraintdef"
            | "pg_get_statisticsobjdef_columns"
            | "pg_get_function_result"
            | "pg_get_function_arguments"
            | "pg_encoding_to_char"
            | "obj_description"
            | "shobj_description"
            | "col_description"
            | "pg_get_serial_sequence"
            | "pg_relation_size"
            | "current_schemas"
            | "pg_size_pretty"
            | "array_length"
            | "array_position"
            | "array_to_string"
            | "string_to_array"
            | "array_cat"
            | "min"
            | "max"
            | "l2_distance"
            | "cosine_distance"
            | "inner_product"
            | "dot_product"
            | "l1_distance"
            | "hybrid_search"
            | "vector_norm"
            | "l2_norm"
            | "vector_dims"
    )
}

fn validate_builtin_args(func_name: &str, args: &mut [ScalarExpr]) -> Result<(), PlanError> {
    match func_name {
        "ifnull" | "nvl" | "nullif" => validate_exact_arg_count(func_name, args, 2),
        "least" | "greatest" => validate_min_arg_count(func_name, args, 1),
        "min" | "max" => validate_min_arg_count(func_name, args, 2),
        "l2_distance" | "cosine_distance" | "inner_product" | "dot_product" | "l1_distance" => {
            validate_vector_metric_args(func_name, args)
        }
        "hybrid_search" => validate_hybrid_search_args(args),
        "vector_norm" | "l2_norm" => validate_vector_norm_args(func_name, args),
        "vector_dims" => validate_vector_dims_args(args),
        "jsonb_path_exists" => validate_jsonb_path_exists_args(args),
        "xml_is_well_formed" | "xml_is_well_formed_content" | "xml_is_well_formed_document" => {
            validate_xml_well_formed_args(func_name, args)
        }
        "xpath" | "xpath_exists" => validate_xpath_args(func_name, args),
        "has_table_privilege"
        | "has_schema_privilege"
        | "has_database_privilege"
        | "has_sequence_privilege"
        | "has_function_privilege"
        | "has_column_privilege" => validate_has_privilege_args(func_name, args),
        "pg_table_is_visible" | "pg_is_other_temp_schema" => {
            validate_single_oidish_arg(func_name, args)
        }
        "current_schemas" => validate_current_schemas_args(args),
        "to_regtype" => validate_to_regtype_args(args),
        "set_config" => validate_set_config_args(args),
        _ => Ok(()),
    }
}

fn validate_exact_arg_count(
    func_name: &str,
    args: &[ScalarExpr],
    expected: usize,
) -> Result<(), PlanError> {
    if args.len() == expected {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected {expected} arguments, got {}",
        args.len()
    )))
}

fn validate_min_arg_count(
    func_name: &str,
    args: &[ScalarExpr],
    min: usize,
) -> Result<(), PlanError> {
    if args.len() >= min {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected at least {min} arguments, got {}",
        args.len()
    )))
}

fn validate_current_schemas_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "current_schemas: expected 1 argument, got {}",
            args.len()
        )));
    }
    let data_type = args[0].data_type();
    if matches!(data_type, DataType::Bool | DataType::Null) {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "current_schemas: boolean argument required, got {data_type}"
    )))
}

fn validate_set_config_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 3 {
        return Err(PlanError::TypeMismatch(format!(
            "set_config: expected 3 arguments, got {}",
            args.len()
        )));
    }
    let name_type = args[0].data_type();
    let value_type = args[1].data_type();
    let local_type = args[2].data_type();
    if !matches!(
        name_type,
        DataType::Text { .. } | DataType::Char { .. } | DataType::Null
    ) {
        return Err(PlanError::TypeMismatch(format!(
            "set_config: setting name must be text, got {name_type}"
        )));
    }
    if !matches!(
        value_type,
        DataType::Text { .. } | DataType::Char { .. } | DataType::Null
    ) {
        return Err(PlanError::TypeMismatch(format!(
            "set_config: setting value must be text, got {value_type}"
        )));
    }
    if !matches!(local_type, DataType::Bool | DataType::Null) {
        return Err(PlanError::TypeMismatch(format!(
            "set_config: local flag must be boolean, got {local_type}"
        )));
    }
    Ok(())
}

fn validate_single_oidish_arg(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected 1 argument, got {}",
            args.len()
        )));
    }
    let data_type = args[0].data_type();
    if data_type.is_oid_alias() || data_type.is_integer() || matches!(data_type, DataType::Null) {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: OID argument required, got {data_type}"
    )))
}

fn validate_to_regtype_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "to_regtype: expected 1 argument, got {}",
            args.len()
        )));
    }
    let data_type = args[0].data_type();
    if matches!(
        data_type,
        DataType::Null | DataType::Text { .. } | DataType::Char { .. } | DataType::RegType
    ) {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "to_regtype: text argument required, got {data_type}"
    )))
}

fn validate_has_privilege_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    let expected = if func_name == "has_column_privilege" {
        4
    } else {
        3
    };
    if args.len() != expected {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected {expected} arguments, got {}",
            args.len()
        )));
    }
    for arg in args {
        let data_type = arg.data_type();
        if !matches!(data_type, DataType::Null | DataType::Text { .. }) {
            return Err(PlanError::TypeMismatch(format!(
                "{func_name}: text arguments required, got {data_type}"
            )));
        }
    }
    Ok(())
}

fn validate_jsonb_path_exists_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 2 {
        return Err(PlanError::TypeMismatch(format!(
            "jsonb_path_exists: expected 2 arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

fn validate_xml_well_formed_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    validate_exact_arg_count(func_name, args, 1)?;
    validate_text_or_xml_arg(func_name, &args[0])
}

fn validate_xpath_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    validate_exact_arg_count(func_name, args, 2)?;
    validate_text_or_xml_arg(func_name, &args[0])?;
    validate_text_or_xml_arg(func_name, &args[1])
}

fn validate_text_or_xml_arg(func_name: &str, arg: &ScalarExpr) -> Result<(), PlanError> {
    let data_type = arg.data_type();
    if matches!(data_type, DataType::Null | DataType::Xml) || data_type.is_textlike() {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected text or xml, got {data_type}"
    )))
}

fn validate_vector_metric_args(func_name: &str, args: &mut [ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 2 {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected 2 arguments, got {}",
            args.len()
        )));
    }
    coerce_vector_metric_literals(args);
    let left = args[0].data_type();
    let right = args[1].data_type();
    if matches!((&left, &right), (DataType::Null, DataType::Null)) {
        return Ok(());
    }
    if matches!(left, DataType::Null) && vector_metric_family_kind(&right).is_some() {
        return Ok(());
    }
    if matches!(right, DataType::Null) && vector_metric_family_kind(&left).is_some() {
        return Ok(());
    }
    if vector_metric_family_kind(&left).is_some()
        && vector_metric_family_kind(&left) == vector_metric_family_kind(&right)
        && dims_compatible(left.vector_dims().flatten(), right.vector_dims().flatten())
    {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: compatible vector, halfvec, or sparsevec operands required, got {left} and {right}"
    )))
}

fn coerce_vector_metric_literals(args: &mut [ScalarExpr]) {
    let left_type = args[0].data_type();
    let right_type = args[1].data_type();
    if vector_metric_family_kind(&left_type).is_some() {
        coerce_literal_to_type(&mut args[1], &left_type);
    }
    if vector_metric_family_kind(&right_type).is_some() {
        coerce_literal_to_type(&mut args[0], &right_type);
    }
}

fn validate_vector_norm_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected 1 argument, got {}",
            args.len()
        )));
    }
    let data_type = args[0].data_type();
    if matches!(data_type, DataType::Null) || vector_metric_family_kind(&data_type).is_some() {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: vector, halfvec, or sparsevec argument required, got {data_type}"
    )))
}

fn validate_vector_dims_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "vector_dims: expected 1 argument, got {}",
            args.len()
        )));
    }
    let data_type = args[0].data_type();
    if matches!(data_type, DataType::Null) || data_type.is_vector_family() {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "vector_dims: vector-family argument required, got {data_type}"
    )))
}

fn validate_hybrid_search_args(args: &mut [ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 4 {
        return Err(PlanError::TypeMismatch(format!(
            "hybrid_search: expected 4 arguments, got {}",
            args.len()
        )));
    }

    let text_type = args[0].data_type();
    if !matches!(
        text_type,
        DataType::Text { .. } | DataType::Json | DataType::Jsonb
    ) {
        return Err(PlanError::TypeMismatch(format!(
            "hybrid_search: first argument must be text/json/jsonb, got {text_type}"
        )));
    }

    let query_type = args[1].data_type();
    if !matches!(query_type, DataType::Text { .. }) {
        return Err(PlanError::TypeMismatch(format!(
            "hybrid_search: second argument must be text, got {query_type}"
        )));
    }

    coerce_hybrid_vector_literals(args);
    let vector_type = args[2].data_type();
    let probe_type = args[3].data_type();
    if dense_vector_family_kind(&vector_type).is_some()
        && dense_vector_family_kind(&vector_type) == dense_vector_family_kind(&probe_type)
        && dims_compatible(
            vector_type.vector_dims().flatten(),
            probe_type.vector_dims().flatten(),
        )
    {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "hybrid_search: third and fourth arguments must be compatible vector or halfvec values, got {vector_type} and {probe_type}"
    )))
}

fn coerce_hybrid_vector_literals(args: &mut [ScalarExpr]) {
    let vector_type = args[2].data_type();
    let probe_type = args[3].data_type();
    if dense_vector_family_kind(&vector_type).is_some() {
        coerce_literal_to_type(&mut args[3], &vector_type);
    }
    if dense_vector_family_kind(&probe_type).is_some() {
        coerce_literal_to_type(&mut args[2], &probe_type);
    }
}

fn vector_metric_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => None,
        _ => None,
    }
}

fn dense_vector_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        _ => None,
    }
}

/// Days from the 2000-01-01 epoch to (year, month, day), positive or
/// negative. The algorithm is Howard Hinnant's `days_from_civil`,
/// rebased on 2000-03-01 internally then offset back to 2000-01-01.
/// Source: <https://howardhinnant.github.io/date_algorithms.html>.
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Howard Hinnant civil_from_days algorithm: y - era*400 is provably in [0, 399], so the i32 → u32 cast cannot lose information; doe < 146_097 always fits in i32"
)]
fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = (y - era * 400) as u32; // [0, 399]
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days_from_1970_03_01 = era * 146_097 + doe as i32 - 719_468;
    // Rebase from 1970-01-01 to 2000-01-01 (10_957 days).
    days_from_1970_03_01 - 10_957
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

fn bind_numeric_literal(text: &str) -> ScalarExpr {
    if let Some((value, scale)) = parse_decimal_literal(text) {
        return ScalarExpr::Literal {
            value: Value::Decimal { value, scale },
            data_type: DataType::Decimal {
                precision: None,
                scale: Some(scale),
            },
        };
    }

    // Exponent notation is approximate in the current literal model.
    let parsed = text.parse::<f64>().unwrap_or(f64::NAN);
    ScalarExpr::Literal {
        value: Value::Float64(parsed),
        data_type: DataType::Float64,
    }
}

fn parse_decimal_literal(text: &str) -> Option<(i64, i32)> {
    if text.contains('e') || text.contains('E') {
        return None;
    }
    let Value::Decimal { value, scale } = parse_decimal_text(text, None).ok()? else {
        return None;
    };
    Some((value, scale))
}

fn pow10_i64(exp: u32) -> Option<i64> {
    (0..exp).try_fold(1_i64, |acc, _| acc.checked_mul(10))
}

fn infer_decimal_scale(value: &Value) -> Option<i32> {
    match value {
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => Some(0),
        Value::Float32(v) => infer_decimal_scale_from_text(&v.to_string()),
        Value::Float64(v) => infer_decimal_scale_from_text(&v.to_string()),
        Value::Decimal { scale, .. } => Some(*scale),
        _ => None,
    }
}

fn infer_decimal_scale_from_text(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    let dot = trimmed.find('.')?;
    i32::try_from(trimmed[dot + 1..].trim_end_matches('0').len()).ok()
}

fn decimal_from_numeric_value(value: &Value, target_scale: Option<i32>) -> Option<(i64, i32)> {
    let inferred_scale = infer_decimal_scale(value);
    let scale = match (target_scale, inferred_scale) {
        (Some(target), Some(inferred)) => target.max(inferred),
        (Some(target), None) => target,
        (None, Some(inferred)) => inferred,
        (None, None) => return None,
    };
    if scale < 0 {
        return None;
    }
    let factor = pow10_i64(u32::try_from(scale).ok()?)?;
    match value {
        Value::Int16(v) => i64::from(*v)
            .checked_mul(factor)
            .map(|scaled| (scaled, scale)),
        Value::Int32(v) => i64::from(*v)
            .checked_mul(factor)
            .map(|scaled| (scaled, scale)),
        Value::Int64(v) => v.checked_mul(factor).map(|scaled| (scaled, scale)),
        Value::Float32(v) => decimal_from_f64(f64::from(*v), scale).map(|scaled| (scaled, scale)),
        Value::Float64(v) => decimal_from_f64(*v, scale).map(|scaled| (scaled, scale)),
        Value::Decimal {
            value: decimal_value,
            scale: decimal_scale,
        } if *decimal_scale == scale => Some((*decimal_value, scale)),
        _ => None,
    }
}

fn money_from_literal_value(value: &Value) -> Option<i64> {
    match value {
        Value::Int16(v) => i64::from(*v).checked_mul(100),
        Value::Int32(v) => i64::from(*v).checked_mul(100),
        Value::Int64(v) => v.checked_mul(100),
        Value::Float32(_) | Value::Float64(_) => None,
        Value::Decimal {
            value: decimal_value,
            scale,
        } => {
            let rendered = Value::Decimal {
                value: *decimal_value,
                scale: *scale,
            }
            .to_string();
            let Value::Money(cents) = parse_money_text(&rendered).ok()? else {
                return None;
            };
            Some(cents)
        }
        Value::Text(text) => {
            let Value::Money(cents) = parse_money_text(text).ok()? else {
                return None;
            };
            Some(cents)
        }
        Value::Money(cents) => Some(*cents),
        _ => None,
    }
}

fn oid_from_literal_value(value: &Value) -> Option<Oid> {
    match value {
        Value::Int16(v) => u32::try_from(*v).ok().map(Oid::new),
        Value::Int32(v) => u32::try_from(*v).ok().map(Oid::new),
        Value::Int64(v) => u32::try_from(*v).ok().map(Oid::new),
        Value::Text(text) | Value::Char(text) => Value::parse_oid_text(text),
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Some(*oid),
        _ => None,
    }
}

fn coerce_literal_to_oid_alias(expr: &mut ScalarExpr, target: &DataType) -> bool {
    fold_signed_literal(expr);
    let ScalarExpr::Literal { value, data_type } = expr else {
        return false;
    };
    if matches!(data_type, DataType::Null) && matches!(value, Value::Null) {
        if target.is_oid_alias() || matches!(target, DataType::PgLsn) {
            *data_type = target.clone();
            return true;
        }
        return false;
    }
    match target {
        DataType::Oid | DataType::RegClass | DataType::RegType => {
            let Some(oid) = oid_from_literal_value(value) else {
                return false;
            };
            *value = match target {
                DataType::Oid => Value::Oid(oid),
                DataType::RegClass => Value::RegClass(oid),
                DataType::RegType => Value::RegType(oid),
                _ => unreachable!(),
            };
            *data_type = target.clone();
            true
        }
        DataType::PgLsn => {
            let parsed = match value {
                Value::PgLsn(lsn) => Some(*lsn),
                Value::Text(text) | Value::Char(text) => Value::parse_pg_lsn_text(text),
                _ => None,
            };
            let Some(lsn) = parsed else {
                return false;
            };
            *value = Value::PgLsn(lsn);
            *data_type = DataType::PgLsn;
            true
        }
        _ => false,
    }
}

fn coerce_literal_to_oid_alias_with_catalog(
    expr: &mut ScalarExpr,
    target: &DataType,
    catalog: &dyn Catalog,
) -> bool {
    fold_signed_literal(expr);
    if matches!(target, DataType::RegClass | DataType::RegType) {
        let ScalarExpr::Literal { value, data_type } = expr else {
            return false;
        };
        if matches!(data_type, DataType::Null) && matches!(value, Value::Null) {
            *data_type = target.clone();
            return true;
        }
        let resolved = match (target, &*value) {
            (DataType::RegClass, Value::Text(text) | Value::Char(text)) => {
                resolve_regclass_literal(text, catalog)
            }
            (DataType::RegType, Value::Text(text) | Value::Char(text)) => {
                Value::parse_oid_text(text).or_else(|| catalog.lookup_type_oid(text))
            }
            _ => oid_from_literal_value(value),
        };
        let Some(oid) = resolved else {
            return false;
        };
        *value = match target {
            DataType::RegClass => Value::RegClass(oid),
            DataType::RegType => Value::RegType(oid),
            _ => unreachable!(),
        };
        *data_type = target.clone();
        return true;
    }
    coerce_literal_to_oid_alias(expr, target)
}

fn resolve_regclass_literal(text: &str, catalog: &dyn Catalog) -> Option<Oid> {
    Value::parse_oid_text(text)
        .or_else(|| catalog.lookup_table_oid(text))
        .or_else(|| {
            parse_pg_identifier_path(text)
                .and_then(|parts| parts.last().cloned())
                .and_then(|name| catalog.lookup_table_oid(&name))
        })
}

fn parse_pg_identifier_path(text: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = text.chars().peekable();
    loop {
        match chars.peek().copied()? {
            '"' => {
                chars.next();
                let mut part = String::new();
                loop {
                    match chars.next()? {
                        '"' if chars.peek() == Some(&'"') => {
                            chars.next();
                            part.push('"');
                        }
                        '"' => break,
                        ch => part.push(ch),
                    }
                }
                parts.push(part);
            }
            _ => {
                let mut part = String::new();
                while let Some(ch) = chars.peek().copied() {
                    if ch == '.' {
                        break;
                    }
                    part.push(ch);
                    chars.next();
                }
                if part.is_empty() {
                    return None;
                }
                parts.push(part);
            }
        }
        match chars.next() {
            Some('.') => continue,
            None => return Some(parts),
            Some(_) => return None,
        }
    }
}

fn decimal_from_f64(value: f64, scale: i32) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    let scale_usize = usize::try_from(scale).ok()?;
    let rendered = format!("{value:.scale_usize$}");
    scaled_decimal_text_to_i64(&rendered)
}

fn scaled_decimal_text_to_i64(text: &str) -> Option<i64> {
    let (negative, unsigned) = text
        .strip_prefix('-')
        .map_or((false, text), |stripped| (true, stripped));
    let (whole, frac) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(whole.len() + frac.len());
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.push_str(frac);
    let mut value = digits.parse::<i64>().ok()?;
    if negative {
        value = value.checked_neg()?;
    }
    Some(value)
}

fn fold_signed_literal(expr: &mut ScalarExpr) {
    let ScalarExpr::Unary {
        op,
        expr: inner,
        data_type: _,
    } = expr
    else {
        return;
    };
    if !matches!(op, UnaryOp::Neg | UnaryOp::Pos) {
        return;
    }

    let ScalarExpr::Literal { value, data_type } = inner.as_ref() else {
        return;
    };

    let folded = match (op, value) {
        (UnaryOp::Pos, value) => Some((value.clone(), data_type.clone())),
        (UnaryOp::Neg, Value::Int16(v)) => v
            .checked_neg()
            .map(|neg| (Value::Int16(neg), data_type.clone())),
        (UnaryOp::Neg, Value::Int32(v)) => v
            .checked_neg()
            .map(|neg| (Value::Int32(neg), data_type.clone())),
        (UnaryOp::Neg, Value::Int64(v)) => v
            .checked_neg()
            .map(|neg| (Value::Int64(neg), data_type.clone())),
        (UnaryOp::Neg, Value::Float32(v)) => Some((Value::Float32(-v), data_type.clone())),
        (UnaryOp::Neg, Value::Float64(v)) => Some((Value::Float64(-v), data_type.clone())),
        (UnaryOp::Neg, Value::Decimal { value, scale }) => value.checked_neg().map(|neg| {
            (
                Value::Decimal {
                    value: neg,
                    scale: *scale,
                },
                data_type.clone(),
            )
        }),
        _ => None,
    };

    if let Some((value, data_type)) = folded {
        *expr = ScalarExpr::Literal { value, data_type };
    }
}

pub(super) fn coerce_literal_to_type(expr: &mut ScalarExpr, target: &DataType) {
    fold_signed_literal(expr);
    if let DataType::Domain { base_type, .. } = target {
        coerce_literal_to_type(expr, base_type);
        let ScalarExpr::Literal { data_type, .. } = expr else {
            return;
        };
        if *data_type == **base_type || matches!(data_type, DataType::Null) {
            *data_type = target.clone();
        }
        return;
    }
    if coerce_literal_to_bit_string(expr, target, false) {
        return;
    }
    if coerce_literal_to_network(expr, target) {
        return;
    }
    if coerce_literal_to_bpchar(expr, target, false) {
        return;
    }
    if coerce_literal_to_enum(expr, target) {
        return;
    }
    if coerce_literal_to_composite(expr, target) {
        return;
    }
    if coerce_literal_to_array(expr, target) {
        return;
    }
    if coerce_literal_to_oid_alias(expr, target) {
        return;
    }
    let ScalarExpr::Literal { value, data_type } = expr else {
        return;
    };
    if matches!(target, DataType::Null) || data_type == target {
        return;
    }
    match (target, &*value) {
        (DataType::Int16, Value::Int32(v)) => {
            if let Ok(narrow) = i16::try_from(*v) {
                *value = Value::Int16(narrow);
                *data_type = DataType::Int16;
            }
        }
        (DataType::Int16, Value::Int64(v)) => {
            if let Ok(narrow) = i16::try_from(*v) {
                *value = Value::Int16(narrow);
                *data_type = DataType::Int16;
            }
        }
        (DataType::Int16, Value::Text(text)) => {
            if let Ok(parsed) = text.parse::<i16>() {
                *value = Value::Int16(parsed);
                *data_type = DataType::Int16;
            }
        }
        (DataType::Int32, Value::Int64(v)) => {
            if let Ok(narrow) = i32::try_from(*v) {
                *value = Value::Int32(narrow);
                *data_type = DataType::Int32;
            }
        }
        (DataType::Int32, Value::Int16(v)) => {
            *value = Value::Int32(i32::from(*v));
            *data_type = DataType::Int32;
        }
        (DataType::Int32, Value::Text(text)) => {
            if let Ok(parsed) = text.parse::<i32>() {
                *value = Value::Int32(parsed);
                *data_type = DataType::Int32;
            }
        }
        (DataType::Int64, Value::Int16(v)) => {
            *value = Value::Int64(i64::from(*v));
            *data_type = DataType::Int64;
        }
        (DataType::Int64, Value::Int32(v)) => {
            *value = Value::Int64(i64::from(*v));
            *data_type = DataType::Int64;
        }
        (DataType::Int64, Value::Text(text)) => {
            if let Ok(parsed) = text.parse::<i64>() {
                *value = Value::Int64(parsed);
                *data_type = DataType::Int64;
            }
        }
        (DataType::Float64, Value::Float32(v)) => {
            *value = Value::Float64(f64::from(*v));
            *data_type = DataType::Float64;
        }
        (DataType::Float64, Value::Int16(v)) => {
            *value = Value::Float64(f64::from(*v));
            *data_type = DataType::Float64;
        }
        (DataType::Float64, Value::Int32(v)) => {
            *value = Value::Float64(f64::from(*v));
            *data_type = DataType::Float64;
        }
        (DataType::Float64, Value::Int64(v)) => {
            #[allow(clippy::cast_precision_loss)]
            let widened = *v as f64;
            *value = Value::Float64(widened);
            *data_type = DataType::Float64;
        }
        (
            DataType::Float64,
            Value::Decimal {
                value: decimal_value,
                scale,
            },
        ) => {
            #[allow(clippy::cast_precision_loss)]
            let widened = (*decimal_value as f64) / 10_f64.powi(*scale);
            *value = Value::Float64(widened);
            *data_type = DataType::Float64;
        }
        (DataType::Float32, Value::Float64(v)) => {
            #[allow(clippy::cast_possible_truncation)]
            let narrow = *v as f32;
            *value = Value::Float32(narrow);
            *data_type = DataType::Float32;
        }
        (DataType::Float32, Value::Int16(v)) => {
            *value = Value::Float32(f32::from(*v));
            *data_type = DataType::Float32;
        }
        (DataType::Float32, Value::Int32(v)) => {
            #[allow(clippy::cast_precision_loss)]
            let widened = *v as f32;
            *value = Value::Float32(widened);
            *data_type = DataType::Float32;
        }
        (DataType::Float32, Value::Int64(v)) => {
            #[allow(clippy::cast_precision_loss)]
            let widened = *v as f32;
            *value = Value::Float32(widened);
            *data_type = DataType::Float32;
        }
        (DataType::Text { .. }, Value::Char(text)) => {
            *value = Value::Text(text.clone());
            *data_type = DataType::Text { max_len: None };
        }
        (DataType::TimestampTz, Value::Timestamp(v)) => {
            *value = Value::TimestampTz(*v);
            *data_type = DataType::TimestampTz;
        }
        (DataType::Timestamp, Value::TimestampTz(v)) => {
            *value = Value::Timestamp(*v);
            *data_type = DataType::Timestamp;
        }
        (DataType::Time, Value::Text(text)) => {
            if let Some(micros) = parse_time_of_day_micros(text) {
                *value = Value::Time(micros);
                *data_type = DataType::Time;
            }
        }
        (DataType::TimeTz, Value::Text(text)) => {
            if let Some((micros, offset_seconds)) = parse_timetz_literal(text) {
                *value = Value::TimeTz {
                    micros,
                    offset_seconds,
                };
                *data_type = DataType::TimeTz;
            }
        }
        (DataType::Timestamp, Value::Text(text)) => {
            if let Some(micros) = parse_timestamp_literal(text) {
                *value = Value::Timestamp(micros);
                *data_type = DataType::Timestamp;
            }
        }
        (DataType::TimestampTz, Value::Text(text)) => {
            if let Some(micros) = parse_timestamptz_literal(text) {
                *value = Value::TimestampTz(micros);
                *data_type = DataType::TimestampTz;
            }
        }
        (
            DataType::Float32,
            Value::Decimal {
                value: decimal_value,
                scale,
            },
        ) => {
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
            let narrow = ((*decimal_value as f64) / 10_f64.powi(*scale)) as f32;
            *value = Value::Float32(narrow);
            *data_type = DataType::Float32;
        }
        (DataType::Decimal { scale, .. }, Value::Text(text)) => {
            if let Ok(Value::Decimal {
                value: decimal_value,
                scale: decimal_scale,
            }) = parse_decimal_text(text, *scale)
            {
                *value = Value::Decimal {
                    value: decimal_value,
                    scale: decimal_scale,
                };
                *data_type = DataType::Decimal {
                    precision: None,
                    scale: Some(decimal_scale),
                };
            }
        }
        (DataType::Decimal { scale, .. }, _) => {
            if let Some((decimal_value, decimal_scale)) = decimal_from_numeric_value(value, *scale)
            {
                *value = Value::Decimal {
                    value: decimal_value,
                    scale: decimal_scale,
                };
                *data_type = DataType::Decimal {
                    precision: None,
                    scale: Some(decimal_scale),
                };
            }
        }
        (DataType::Money, _) => {
            if let Some(cents) = money_from_literal_value(value) {
                *value = Value::Money(cents);
                *data_type = DataType::Money;
            }
        }
        (DataType::Range(range_type), Value::Text(text)) => {
            if let Some(range) = RangeValue::parse(*range_type, text) {
                *value = Value::Range(range);
                *data_type = DataType::Range(*range_type);
            }
        }
        (DataType::Geometry(geometry_type), Value::Text(text)) => {
            if let Some(geometry) = GeometryValue::parse(*geometry_type, text) {
                *value = Value::Geometry(geometry);
                *data_type = DataType::Geometry(*geometry_type);
            }
        }
        (target, Value::Text(text)) if target.is_vector_family() => {
            if let Some(parsed) = parse_vector_family_value(target, text) {
                let actual_type = parsed.data_type();
                if vector_family_cast_matches(target, &actual_type) {
                    *value = parsed;
                    *data_type = actual_type;
                }
            }
        }
        (DataType::Uuid, Value::Text(text)) => {
            if let Some(uuid) = Value::parse_uuid(text) {
                *value = Value::Uuid(uuid);
                *data_type = DataType::Uuid;
            }
        }
        (DataType::Bytea, Value::Text(text)) => {
            if let Some(bytes) = Value::parse_bytea(text) {
                *value = Value::Bytea(bytes);
                *data_type = DataType::Bytea;
            }
        }
        (DataType::Json, Value::Text(text)) => {
            if let Some(parsed) = validate_json_text(text) {
                *value = Value::Json(parsed);
                *data_type = DataType::Json;
            }
        }
        (DataType::Jsonb, Value::Text(text) | Value::Json(text)) => {
            if let Some(parsed) = normalize_jsonb_text(text) {
                *value = Value::Jsonb(parsed);
                *data_type = DataType::Jsonb;
            }
        }
        (DataType::Json, Value::Jsonb(text)) => {
            *value = Value::Json(text.clone());
            *data_type = DataType::Json;
        }
        (DataType::Xml, Value::Text(text)) => {
            if let Some(parsed) = Value::validate_xml_text(text) {
                *value = Value::Xml(parsed);
                *data_type = DataType::Xml;
            }
        }
        _ => {}
    }
}

fn coerce_literal_to_enum(expr: &mut ScalarExpr, target: &DataType) -> bool {
    let DataType::Enum { labels, .. } = target else {
        return false;
    };
    let ScalarExpr::Literal { value, data_type } = expr else {
        return false;
    };
    let Value::Text(text) = value else {
        return false;
    };
    if !labels.iter().any(|label| label == text) {
        return false;
    }
    *data_type = target.clone();
    true
}

fn coerce_literal_to_composite(expr: &mut ScalarExpr, target: &DataType) -> bool {
    let DataType::Composite { fields, .. } = target else {
        return false;
    };
    let ScalarExpr::Literal { value, data_type } = expr else {
        return false;
    };
    let Value::Text(text) = value else {
        return false;
    };
    if !composite_text_matches_arity(text, fields.len()) {
        return false;
    }
    *data_type = target.clone();
    true
}

fn coerce_literal_to_array(expr: &mut ScalarExpr, target: &DataType) -> bool {
    let DataType::Array(target_element) = target else {
        return false;
    };
    let ScalarExpr::Literal { value, data_type } = expr else {
        return false;
    };
    match value {
        Value::Array { elements, .. } => {
            let mut coerced_elements = Vec::with_capacity(elements.len());
            for element in elements.iter() {
                if element.is_null() {
                    coerced_elements.push(Value::Null);
                    continue;
                }
                let mut element_expr = ScalarExpr::Literal {
                    value: element.clone(),
                    data_type: element.data_type(),
                };
                coerce_literal_to_type(&mut element_expr, target_element);
                let ScalarExpr::Literal {
                    value: coerced_value,
                    data_type: coerced_type,
                } = element_expr
                else {
                    return false;
                };
                if !matches!(coerced_type, DataType::Null) && coerced_type != **target_element {
                    return false;
                }
                coerced_elements.push(coerced_value);
            }
            let coerced = Value::Array {
                element_type: (**target_element).clone(),
                elements: coerced_elements,
            };
            if coerced.array_dimensions().is_none() {
                return false;
            }
            *value = coerced;
            *data_type = target.clone();
            true
        }
        Value::Text(text) => {
            let Some(parsed) = Value::parse_array((**target_element).clone(), text) else {
                return false;
            };
            *value = parsed;
            *data_type = target.clone();
            true
        }
        Value::Null => true,
        _ => false,
    }
}

fn coerce_literal_to_bit_string(
    expr: &mut ScalarExpr,
    target: &DataType,
    explicit_cast: bool,
) -> bool {
    fold_signed_literal(expr);
    if !target.is_bit_string() {
        return false;
    }
    let ScalarExpr::Literal { value, data_type } = expr else {
        return false;
    };
    if matches!(data_type, DataType::Null) {
        return true;
    }
    let parsed = match &*value {
        Value::BitString(bits) => Some(bits.clone()),
        Value::Text(text) | Value::Char(text) => BitString::parse(text),
        Value::Int16(v) if explicit_cast => bit_string_from_integer_target(i64::from(*v), target),
        Value::Int32(v) if explicit_cast => bit_string_from_integer_target(i64::from(*v), target),
        Value::Int64(v) if explicit_cast => bit_string_from_integer_target(*v, target),
        _ => None,
    };
    let Some(bits) = parsed else {
        return false;
    };
    let Some(coerced) = bits.coerce_to(target, explicit_cast) else {
        return false;
    };
    *value = Value::BitString(coerced);
    *data_type = target.clone();
    true
}

fn coerce_literal_to_network(expr: &mut ScalarExpr, target: &DataType) -> bool {
    if !target.is_network_address() {
        return false;
    }
    let ScalarExpr::Literal { value, data_type } = expr else {
        return false;
    };
    if matches!(data_type, DataType::Null) || data_type == target {
        return true;
    }
    let parsed = match &*value {
        Value::Network(network) if network.data_type() == *target => Some(Value::Network(*network)),
        Value::Text(text) | Value::Char(text) => Value::parse_network(target, text),
        _ => None,
    };
    let Some(parsed) = parsed else {
        return false;
    };
    *value = parsed;
    *data_type = target.clone();
    true
}

fn bit_string_from_integer_target(value: i64, target: &DataType) -> Option<BitString> {
    let width = match target {
        DataType::Bit { len: Some(len) } => *len,
        DataType::Bit { len: None } => 1,
        DataType::VarBit { max_len: Some(len) } => *len,
        DataType::VarBit { max_len: None } => 64,
        _ => return None,
    };
    BitString::from_i64(width, value)
}

fn coerce_literal_to_bpchar(expr: &mut ScalarExpr, target: &DataType, explicit_cast: bool) -> bool {
    fold_signed_literal(expr);
    let DataType::Char { len } = target else {
        return false;
    };
    let ScalarExpr::Literal { value, data_type } = expr else {
        return false;
    };
    if matches!(data_type, DataType::Null) || data_type == target {
        return true;
    }
    let text = match (&*value, explicit_cast) {
        (Value::Text(text) | Value::Char(text), _) => text.clone(),
        (_, true) => value.to_string(),
        (_, false) => return false,
    };
    let Ok(coerced) = coerce_bpchar_text(&text, *len, explicit_cast) else {
        return false;
    };
    *value = Value::Char(coerced);
    *data_type = target.clone();
    true
}

fn resolve_cast_type(type_name: &str) -> Option<DataType> {
    let type_name = type_name.to_ascii_lowercase();
    if let Some(data_type) = parse_vector_family_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_bpchar_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_varchar_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_bit_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_network_type_name(&type_name) {
        return Some(data_type);
    }
    match type_name.as_str() {
        "int" | "integer" | "int4" => Some(DataType::Int32),
        "bigint" | "int8" => Some(DataType::Int64),
        "smallint" | "int2" => Some(DataType::Int16),
        "bool" | "boolean" => Some(DataType::Bool),
        "real" | "float4" => Some(DataType::Float32),
        "double" | "double precision" | "float" | "float8" => Some(DataType::Float64),
        "text" | "tsvector" | "tsquery" => Some(DataType::Text { max_len: None }),
        "bytea" => Some(DataType::Bytea),
        "date" => Some(DataType::Date),
        "time" | "time without time zone" => Some(DataType::Time),
        "timetz" | "time with time zone" => Some(DataType::TimeTz),
        "timestamp" | "timestamp without time zone" => Some(DataType::Timestamp),
        "timestamptz" | "timestamp with time zone" => Some(DataType::TimestampTz),
        "uuid" => Some(DataType::Uuid),
        "json" => Some(DataType::Json),
        "jsonb" => Some(DataType::Jsonb),
        "xml" => Some(DataType::Xml),
        "numeric" | "decimal" => Some(DataType::Decimal {
            precision: None,
            scale: None,
        }),
        "money" => Some(DataType::Money),
        "oid" => Some(DataType::Oid),
        "regnamespace" => Some(DataType::Oid),
        "regclass" => Some(DataType::RegClass),
        "regtype" => Some(DataType::RegType),
        "pg_lsn" => Some(DataType::PgLsn),
        "int4range" => Some(DataType::Range(RangeType::Int4)),
        "int8range" => Some(DataType::Range(RangeType::Int8)),
        "numrange" => Some(DataType::Range(RangeType::Num)),
        "daterange" => Some(DataType::Range(RangeType::Date)),
        "tsrange" => Some(DataType::Range(RangeType::Timestamp)),
        "tstzrange" => Some(DataType::Range(RangeType::TimestampTz)),
        "point" => Some(DataType::Geometry(GeometryType::Point)),
        "box" => Some(DataType::Geometry(GeometryType::Box)),
        "circle" => Some(DataType::Geometry(GeometryType::Circle)),
        "line" => Some(DataType::Geometry(GeometryType::Line)),
        "lseg" => Some(DataType::Geometry(GeometryType::Lseg)),
        "path" => Some(DataType::Geometry(GeometryType::Path)),
        "polygon" => Some(DataType::Geometry(GeometryType::Polygon)),
        _ => None,
    }
}

fn resolve_cast_type_with_catalog(type_name: &str, catalog: &dyn Catalog) -> Option<DataType> {
    resolve_cast_type(type_name).or_else(|| catalog.lookup_type(type_name))
}

fn parse_network_type_name(type_name: &str) -> Option<DataType> {
    match type_name {
        "inet" => Some(DataType::Inet),
        "cidr" => Some(DataType::Cidr),
        "macaddr" => Some(DataType::MacAddr),
        "macaddr8" => Some(DataType::MacAddr8),
        _ => None,
    }
}

fn parse_bpchar_type_name(type_name: &str) -> Option<DataType> {
    match type_name {
        "char" | "character" => return Some(DataType::Char { len: Some(1) }),
        "bpchar" => return Some(DataType::Char { len: None }),
        _ => {}
    }
    let (base, len) = parse_single_type_modifier(type_name)?;
    match base {
        "char" | "character" | "bpchar" if len > 0 => Some(DataType::Char { len: Some(len) }),
        _ => None,
    }
}

fn parse_varchar_type_name(type_name: &str) -> Option<DataType> {
    if type_name == "varchar" {
        return Some(DataType::Text { max_len: None });
    }
    let (base, len) = parse_single_type_modifier(type_name)?;
    (base == "varchar").then_some(DataType::Text { max_len: Some(len) })
}

fn parse_bit_type_name(type_name: &str) -> Option<DataType> {
    match type_name {
        "bit" => return Some(DataType::Bit { len: Some(1) }),
        "varbit" | "bit varying" => return Some(DataType::VarBit { max_len: None }),
        _ => {}
    }
    let (base, len) = parse_single_type_modifier(type_name)?;
    if len == 0 {
        return None;
    }
    match base {
        "bit" => Some(DataType::Bit { len: Some(len) }),
        "varbit" | "bit varying" => Some(DataType::VarBit { max_len: Some(len) }),
        _ => None,
    }
}

fn parse_single_type_modifier(type_name: &str) -> Option<(&str, u32)> {
    let (base, rest) = type_name.split_once('(')?;
    let len_text = rest.strip_suffix(')')?;
    let len = len_text.parse::<u32>().ok()?;
    Some((base, len))
}

fn parse_vector_family_type_name(type_name: &str) -> Option<DataType> {
    for base in ["vector", "halfvec", "sparsevec", "bitvec"] {
        if type_name == base {
            return build_vector_family_type(base, None);
        }
        if let Some(dim_text) = type_name
            .strip_prefix(base)
            .and_then(|rest| rest.strip_prefix('('))
            .and_then(|rest| rest.strip_suffix(')'))
        {
            let dims: u32 = dim_text.parse().ok()?;
            if dims == 0 || dims > MAX_VECTOR_DIMS {
                return None;
            }
            return build_vector_family_type(base, Some(dims));
        }
    }
    None
}

fn build_vector_family_type(base: &str, dims: Option<u32>) -> Option<DataType> {
    match base {
        "vector" => Some(DataType::Vector { dims }),
        "halfvec" => Some(DataType::HalfVec { dims }),
        "sparsevec" => Some(DataType::SparseVec { dims }),
        "bitvec" => Some(DataType::BitVec { dims }),
        _ => None,
    }
}

fn parse_vector_family_value(target: &DataType, text: &str) -> Option<Value> {
    match target {
        DataType::Vector { .. } => Value::parse_vector(text),
        DataType::HalfVec { .. } => Value::parse_halfvec(text),
        DataType::SparseVec { .. } => Value::parse_sparsevec(text),
        DataType::BitVec { .. } => Value::parse_bitvec(text),
        _ => None,
    }
}

fn vector_family_cast_matches(target: &DataType, actual: &DataType) -> bool {
    vector_family_kind(target) == vector_family_kind(actual)
        && dims_compatible(
            target.vector_dims().flatten(),
            actual.vector_dims().flatten(),
        )
}

fn vector_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => Some(3),
        _ => None,
    }
}

const fn dims_compatible(left: Option<u32>, right: Option<u32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

fn cast_result_matches(target: &DataType, actual: &DataType) -> bool {
    target == actual
        || matches!(
            (target, actual),
            (
                DataType::Vector { dims: None },
                DataType::Vector { dims: Some(_) }
            ) | (
                DataType::Decimal {
                    precision: None,
                    scale: None
                },
                DataType::Decimal { .. }
            )
        )
        || (target.is_vector_family()
            && actual.is_vector_family()
            && vector_family_cast_matches(target, actual))
}

pub(super) fn coerce_literal_to_match(left: &mut ScalarExpr, right: &mut ScalarExpr) {
    let right_target = right.data_type();
    let left_target = left.data_type();
    coerce_literal_to_type(left, &right_target);
    coerce_literal_to_type(right, &left_target);
}

pub(super) fn bind_column(
    name: &ultrasql_parser::ast::ObjectName,
    input: &Schema,
    scope: &ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let col_name = name
        .parts
        .last()
        .map_or_else(String::new, |p| p.value.clone());

    if let Some(qualified_name) = qualified_column_name(name) {
        if let Some((index, field)) = input.find(&qualified_name) {
            return Ok(ScalarExpr::Column {
                name: field.name.clone(),
                index,
                data_type: field.data_type.clone(),
            });
        }
        if let Some(outer_ref) = scope.resolve(&qualified_name) {
            return Ok(ScalarExpr::OuterColumn {
                name: qualified_name,
                frame_depth: outer_ref.frame_depth,
                column_index: outer_ref.column_index,
                data_type: outer_ref.data_type,
            });
        }
        if input.fields().iter().any(|f| {
            f.name
                .rsplit_once('.')
                .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case(&col_name))
        }) {
            return Err(PlanError::ColumnNotFound(qualified_name));
        }
    }

    let mut hits = input
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name.eq_ignore_ascii_case(&col_name));
    if let Some((index, field)) = hits.next() {
        if hits.next().is_some() {
            return Err(PlanError::Ambiguous(col_name));
        }
        return Ok(ScalarExpr::Column {
            name: field.name.clone(),
            index,
            data_type: field.data_type.clone(),
        });
    }

    let mut suffix_hits = input.fields().iter().enumerate().filter(|(_, f)| {
        f.name
            .rsplit_once('.')
            .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case(&col_name))
    });
    let Some((index, field)) = suffix_hits.next() else {
        // Column not found in the inner scope — try outer scopes.  This
        // produces an OuterColumn when we are inside a subquery.
        if let Some(outer_ref) = scope.resolve(&col_name) {
            return Ok(ScalarExpr::OuterColumn {
                name: col_name,
                frame_depth: outer_ref.frame_depth,
                column_index: outer_ref.column_index,
                data_type: outer_ref.data_type,
            });
        }
        if input.is_empty()
            && name.parts.len() == 1
            && matches!(
                col_name.as_str(),
                "current_catalog" | "current_user" | "session_user"
            )
        {
            return Ok(ScalarExpr::FunctionCall {
                name: col_name,
                args: Vec::new(),
                data_type: DataType::Text { max_len: None },
            });
        }
        return Err(PlanError::ColumnNotFound(col_name));
    };
    if suffix_hits.next().is_some() {
        return Err(PlanError::Ambiguous(col_name));
    }
    Ok(ScalarExpr::Column {
        name: col_name,
        index,
        data_type: field.data_type.clone(),
    })
}

fn qualified_column_name(name: &ultrasql_parser::ast::ObjectName) -> Option<String> {
    let col = name.parts.last()?;
    let qualifier = name.parts.iter().rev().nth(1)?;
    Some(format!("{}.{}", qualifier.value, col.value))
}

fn bind_array_subscript(
    array_expr: &Expr,
    index: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let array = bind_expr_with_ctes(array_expr, input, catalog, cte_catalog, scope)?;
    let index = bind_expr_with_ctes(index, input, catalog, cte_catalog, scope)?;
    let element_type = match array.data_type() {
        DataType::Array(element_type) => *element_type,
        other => {
            return Err(PlanError::TypeMismatch(format!(
                "array subscript requires array input, got {other}"
            )));
        }
    };
    let index_type = index.data_type();
    if !matches!(
        index_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Null
    ) {
        return Err(PlanError::TypeMismatch(format!(
            "array subscript index must be integer, got {index_type}"
        )));
    }
    Ok(ScalarExpr::FunctionCall {
        name: "__ultrasql_array_subscript".to_owned(),
        args: vec![array, index],
        data_type: element_type,
    })
}

pub(super) fn bind_unary(
    op: UnaryOp,
    inner: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let bound = bind_expr_with_ctes(inner, input, catalog, cte_catalog, scope)?;
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
        UnaryOp::BitNot => {
            if inner_ty.is_integer()
                || inner_ty.is_bit_string()
                || inner_ty.is_network_address()
                || matches!(inner_ty, DataType::Null)
            {
                inner_ty
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "bitwise NOT (~) requires integer, bit string, or network operand, got {inner_ty}"
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

#[allow(clippy::too_many_lines)]
pub(super) fn bind_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let mut l = bind_expr_with_ctes(left, input, catalog, cte_catalog, scope)?;
    let mut r = bind_expr_with_ctes(right, input, catalog, cte_catalog, scope)?;
    if !binary_operator_uses_raw_text_pattern(op) {
        coerce_literal_to_match(&mut l, &mut r);
    }
    if let Some(folded) = try_fold_literal_binary(op, &l, &r)? {
        return Ok(folded);
    }
    let data_type = binary_result_type(op, l.data_type(), r.data_type())?;
    Ok(ScalarExpr::Binary {
        op,
        left: Box::new(l),
        right: Box::new(r),
        data_type,
    })
}

const fn binary_operator_uses_raw_text_pattern(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Like
            | BinaryOp::NotLike
            | BinaryOp::Ilike
            | BinaryOp::NotIlike
            | BinaryOp::RegexMatch
            | BinaryOp::RegexIMatch
            | BinaryOp::RegexNotMatch
            | BinaryOp::RegexNotIMatch
    )
}

#[cfg(test)]
mod typed_literal_tests {
    use ultrasql_core::{DataType, Value};
    use ultrasql_parser::Span;
    use ultrasql_parser::ast::Literal;

    use super::{
        BinaryOp, ScalarExpr, bind_literal, coerce_literal_to_type, days_since_epoch,
        fold_date_interval, parse_date_literal, parse_interval_literal, parse_time_of_day_micros,
        parse_timestamp_literal, parse_timetz_literal, try_fold_literal_binary,
    };

    #[test]
    fn epoch_day_is_zero() {
        assert_eq!(parse_date_literal("2000-01-01"), Some(0));
    }

    #[test]
    fn one_day_after_epoch() {
        assert_eq!(parse_date_literal("2000-01-02"), Some(1));
    }

    #[test]
    fn pre_epoch_six_years_back() {
        // 1994-01-01: six 365-day years back plus one leap (1996),
        // so 6*365 + 1 = 2191 days before the epoch.
        assert_eq!(parse_date_literal("1994-01-01"), Some(-2191));
    }

    #[test]
    fn one_year_forward_is_365_or_366() {
        let y2000 = parse_date_literal("2000-01-01").unwrap();
        let y2001 = parse_date_literal("2001-01-01").unwrap();
        assert_eq!(y2001 - y2000, 366, "2000 was a leap year");
        let y2002 = parse_date_literal("2002-01-01").unwrap();
        assert_eq!(y2002 - y2001, 365);
    }

    #[test]
    fn rejects_malformed() {
        assert!(parse_date_literal("not-a-date").is_none());
        assert!(parse_date_literal("2000/01/01").is_none());
        assert!(parse_date_literal("2000-13-01").is_none());
        assert!(parse_date_literal("2000-01-32").is_none());
    }

    #[test]
    fn timestamp_literal_parses_microseconds_since_epoch() {
        assert_eq!(parse_timestamp_literal("2000-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_timestamp_literal("2000-01-02 00:00:00"),
            Some(86_400_000_000)
        );
        assert_eq!(
            parse_timestamp_literal("2000-01-01 01:02:03.456789"),
            Some(3_723_456_789)
        );
        assert_eq!(
            parse_timestamp_literal("2000-01-01 01:02:03.456789-08"),
            Some(3_723_456_789),
            "timestamp without time zone ignores input offset"
        );
    }

    #[test]
    fn time_and_timetz_literals_parse_postgres_shapes() {
        assert_eq!(
            parse_time_of_day_micros("01:02:03.456789-08"),
            Some(3_723_456_789)
        );
        assert_eq!(
            parse_timetz_literal("04:05:06.789-08:00"),
            Some((14_706_789_000, -28_800))
        );
    }

    #[test]
    fn algorithm_handles_leap_year_february() {
        let feb29 = days_since_epoch(2000, 2, 29);
        let mar01 = days_since_epoch(2000, 3, 1);
        assert_eq!(mar01 - feb29, 1, "2000-02-29 → 2000-03-01 is one day");
    }

    #[test]
    fn parses_interval_year_unit_into_months() {
        assert_eq!(parse_interval_literal("1", Some("year")), Some((12, 0, 0)));
        assert_eq!(parse_interval_literal("3", Some("month")), Some((3, 0, 0)));
        assert_eq!(parse_interval_literal("90", Some("day")), Some((0, 90, 0)));
    }

    #[test]
    fn decimal_coercion_preserves_literal_fractional_precision() {
        let mut expr = ScalarExpr::Literal {
            value: Value::Float64(0.0001),
            data_type: DataType::Float64,
        };
        coerce_literal_to_type(
            &mut expr,
            &DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        );
        let ScalarExpr::Literal { value, data_type } = expr else {
            panic!("expected literal");
        };
        assert_eq!(value, Value::Decimal { value: 1, scale: 4 });
        assert_eq!(
            data_type,
            DataType::Decimal {
                precision: None,
                scale: Some(4)
            }
        );
    }

    #[test]
    fn dotted_numeric_literal_binds_as_exact_decimal() {
        let expr = bind_literal(&Literal::Float {
            text: "0.0001".to_owned(),
            span: Span::default(),
        });
        let ScalarExpr::Literal { value, data_type } = expr else {
            panic!("expected literal");
        };
        assert_eq!(value, Value::Decimal { value: 1, scale: 4 });
        assert_eq!(
            data_type,
            DataType::Decimal {
                precision: None,
                scale: Some(4)
            }
        );
    }

    #[test]
    fn decimal_literal_arithmetic_is_not_folded_through_float() {
        let left = ScalarExpr::Literal {
            value: Value::Decimal { value: 6, scale: 2 },
            data_type: DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        };
        let right = ScalarExpr::Literal {
            value: Value::Decimal { value: 1, scale: 2 },
            data_type: DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        };
        let folded = try_fold_literal_binary(BinaryOp::Sub, &left, &right)
            .expect("fold attempt should not error");
        assert!(folded.is_none(), "decimal arithmetic must stay exact");
    }

    #[test]
    fn decimal_literal_coerces_to_float64_target() {
        let mut expr = bind_literal(&Literal::Float {
            text: "1.5".to_owned(),
            span: Span::default(),
        });
        coerce_literal_to_type(&mut expr, &DataType::Float64);
        let ScalarExpr::Literal { value, data_type } = expr else {
            panic!("expected literal");
        };
        assert_eq!(data_type, DataType::Float64);
        let Value::Float64(v) = value else {
            panic!("expected float64");
        };
        assert!((v - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn typed_vector_literal_binds_to_vector_value() {
        let expr = bind_literal(&Literal::Typed {
            type_name: "vector".to_owned(),
            value: "[1,2,3]".to_owned(),
            unit: None,
            span: Span::default(),
        });
        let ScalarExpr::Literal { value, data_type } = expr else {
            panic!("expected literal");
        };
        assert_eq!(value, Value::Vector(vec![1.0, 2.0, 3.0]));
        assert_eq!(data_type, DataType::Vector { dims: Some(3) });
    }

    #[test]
    fn typed_vector_literal_with_modifier_validates_dimension() {
        let expr = bind_literal(&Literal::Typed {
            type_name: "vector(3)".to_owned(),
            value: "[1,2,3]".to_owned(),
            unit: None,
            span: Span::default(),
        });
        let ScalarExpr::Literal { value, data_type } = expr else {
            panic!("expected literal");
        };
        assert_eq!(value, Value::Vector(vec![1.0, 2.0, 3.0]));
        assert_eq!(data_type, DataType::Vector { dims: Some(3) });
    }

    #[test]
    fn typed_vector_literal_rejects_dimension_mismatch() {
        let expr = bind_literal(&Literal::Typed {
            type_name: "vector(3)".to_owned(),
            value: "[1,2]".to_owned(),
            unit: None,
            span: Span::default(),
        });
        let ScalarExpr::Literal { value, data_type } = expr else {
            panic!("expected literal");
        };
        assert_eq!(value, Value::Null);
        assert_eq!(data_type, DataType::Vector { dims: Some(3) });
    }

    #[test]
    fn bind_time_and_timetz_literals_from_ast() {
        let time_expr = bind_literal(&Literal::Typed {
            type_name: "time".into(),
            value: "04:05:06-08".into(),
            unit: None,
            span: Span::new(0, 0),
        });
        let ScalarExpr::Literal { value, data_type } = time_expr else {
            panic!("expected time literal");
        };
        assert_eq!(data_type, DataType::Time);
        assert_eq!(value, Value::Time(14_706_000_000));

        let timetz_expr = bind_literal(&Literal::Typed {
            type_name: "time with time zone".into(),
            value: "04:05:06-08".into(),
            unit: None,
            span: Span::new(0, 0),
        });
        let ScalarExpr::Literal { value, data_type } = timetz_expr else {
            panic!("expected timetz literal");
        };
        assert_eq!(data_type, DataType::TimeTz);
        assert_eq!(
            value,
            Value::TimeTz {
                micros: 14_706_000_000,
                offset_seconds: -28_800,
            }
        );
    }

    #[test]
    fn fold_date_interval_keeps_calendar_month_semantics() {
        let folded = fold_date_interval(days_since_epoch(2000, 1, 31), 1, 0, 0).unwrap();
        let super::ScalarExpr::Literal { value, data_type } = folded else {
            panic!("expected folded literal");
        };
        assert_eq!(data_type, DataType::Date);
        assert_eq!(value, Value::Date(days_since_epoch(2000, 2, 29)));
    }

    #[test]
    fn folds_float_literal_subtraction() {
        let left = ScalarExpr::Literal {
            value: Value::Float64(0.06),
            data_type: DataType::Float64,
        };
        let right = ScalarExpr::Literal {
            value: Value::Float64(0.01),
            data_type: DataType::Float64,
        };

        let folded = try_fold_literal_binary(BinaryOp::Sub, &left, &right)
            .expect("fold succeeds")
            .expect("float literals should fold");
        let ScalarExpr::Literal {
            value: Value::Float64(value),
            data_type,
        } = folded
        else {
            panic!("expected float literal");
        };
        assert_eq!(data_type, DataType::Float64);
        assert!((value - 0.05).abs() < 1.0e-12, "expected 0.05, got {value}");
    }
}
