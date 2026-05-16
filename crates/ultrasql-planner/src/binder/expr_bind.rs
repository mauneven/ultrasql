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

use ultrasql_core::{DataType, Value};
use ultrasql_parser::ast::{BinaryOp, Expr, Literal, UnaryOp};

use super::expr_type::{binary_result_type, comparable, display_unary};
use super::{
    Catalog, PlanError, ScalarExpr, Schema, ScopeFrame, ScopeStack, bind_select_with_ctes,
    derive_agg_output_name, is_aggregate_name, plan_contains_outer_column,
};

pub(super) fn bind_expr(
    expr: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Literal(lit) => Ok(bind_literal(lit)),
        Expr::Column { name } => bind_column(name, input, scope),
        Expr::Parameter { index, .. } => Ok(ScalarExpr::Parameter {
            index: *index,
            data_type: DataType::Null,
        }),
        Expr::Paren { expr, .. } => bind_expr(expr, input, catalog, scope),
        Expr::Unary {
            op, expr: inner, ..
        } => bind_unary(*op, inner, input, catalog, scope),
        Expr::Binary {
            op, left, right, ..
        } => bind_binary(*op, left, right, input, catalog, scope),
        Expr::IsNull { expr, negated, .. } => Ok(ScalarExpr::IsNull {
            expr: Box::new(bind_expr(expr, input, catalog, scope)?),
            negated: *negated,
        }),
        Expr::Call { name, args, .. } => {
            // If this is a known aggregate and we have an aggregate output schema,
            // try to resolve it as a column reference into that schema.
            let func_name = name
                .parts
                .last()
                .map_or("", |p| p.value.as_str())
                .to_ascii_lowercase();
            if is_aggregate_name(&func_name) {
                let agg_col_name = derive_agg_output_name(&func_name, args);
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
                .map(|a| bind_expr(a, input, catalog, scope))
                .collect();
            let bound_args = bound_args?;
            let return_type = builtin_return_type(&func_name)?;
            Ok(ScalarExpr::FunctionCall {
                name: func_name,
                args: bound_args,
                data_type: return_type,
            })
        }
        Expr::Cast { .. } => Err(PlanError::NotSupported("CAST expressions")),

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
            let inner_result = bind_select_with_ctes(inner_select, catalog, &[], scope);
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
            let inner_result = bind_select_with_ctes(inner_select, catalog, &[], scope);
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
            let lhs = bind_expr(lhs_ast, input, catalog, scope)?;
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, &[], scope);
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
            let lhs = bind_expr(lhs_ast, input, catalog, scope)?;
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, &[], scope);
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
            subject, low, high, *negated, *symmetric, input, catalog, scope,
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
                bound_args.push(bind_expr(op_expr, input, catalog, scope)?);
                "case_simple"
            } else {
                "case_searched"
            };
            let mut result_type = DataType::Null;
            for (when_e, then_e) in branches {
                bound_args.push(bind_expr(when_e, input, catalog, scope)?);
                let then_bound = bind_expr(then_e, input, catalog, scope)?;
                if matches!(result_type, DataType::Null) {
                    result_type = then_bound.data_type();
                }
                bound_args.push(then_bound);
            }
            if let Some(else_e) = else_expr {
                let bound = bind_expr(else_e, input, catalog, scope)?;
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
            let bound_subject = bind_expr(subject, input, catalog, scope)?;
            let mut acc: Option<ScalarExpr> = None;
            for item in items {
                let bound_item = bind_expr(item, input, catalog, scope)?;
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
                .map(|a| bind_expr(a, input, catalog, scope))
                .collect();
            let bound_args = bound_args?;
            let return_type = bound_args
                .first()
                .map(ScalarExpr::data_type)
                .unwrap_or(DataType::Null);
            Ok(ScalarExpr::FunctionCall {
                name: "coalesce".to_owned(),
                args: bound_args,
                data_type: return_type,
            })
        }

        _ => Err(PlanError::NotSupported("expression variant")),
    }
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
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let bound_expr = bind_expr(subject, input, catalog, scope)?;
    let bound_low = bind_expr(low, input, catalog, scope)?;
    let bound_high = bind_expr(high, input, catalog, scope)?;

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
    left: ScalarExpr,
    right: ScalarExpr,
) -> Result<ScalarExpr, PlanError> {
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
        Literal::Typed {
            type_name,
            value,
            unit: _,
            ..
        } => bind_typed_literal(type_name, value),
        // `Literal::Null` and any future non-exhaustive variant both
        // bind to a NULL placeholder; later passes specialize.
        _ => ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        },
    }
}

/// Convert a `TYPENAME 'literal'` AST node into the matching
/// [`ScalarExpr::Literal`].
///
/// Supported today:
/// - `DATE 'YYYY-MM-DD'` → `Value::Date(days_since_2000_01_01)`.
///
/// Unsupported variants (TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL) bind
/// to NULL today so the binder does not reject queries upstream of the
/// executor. Adding full support is a tracked v0.6 follow-up; the
/// upstream executor will surface a clearer message when those values
/// flow into a comparison.
fn bind_typed_literal(type_name: &str, value: &str) -> ScalarExpr {
    match type_name {
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
        _ => ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        },
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

/// Statically infer the return type of a builtin scalar function.
/// The set must stay in sync with the executor's `eval_function_call`
/// dispatcher in [`crates/ultrasql-executor/src/eval.rs`].
fn builtin_return_type(func_name: &str) -> Result<DataType, PlanError> {
    match func_name {
        "extract" => Ok(DataType::Int64),
        "substring" => Ok(DataType::Text { max_len: None }),
        _ => Err(PlanError::NotSupported("non-aggregate function calls")),
    }
}

/// True when the binder accepts the function name as a v0.6 builtin.
/// Used by the `_` fallback in the expression-variant path to keep
/// the diagnostic precise: unknown function names still report
/// `non-aggregate function calls`.
#[allow(dead_code)]
fn is_supported_builtin(func_name: &str) -> bool {
    matches!(func_name, "extract" | "substring")
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

pub(super) fn bind_column(
    name: &ultrasql_parser::ast::ObjectName,
    input: &Schema,
    scope: &ScopeStack,
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

pub(super) fn bind_unary(
    op: UnaryOp,
    inner: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let bound = bind_expr(inner, input, catalog, scope)?;
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
            if inner_ty.is_integer() || matches!(inner_ty, DataType::Null) {
                inner_ty
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "bitwise NOT (~) requires integer operand, got {inner_ty}"
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
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let l = bind_expr(left, input, catalog, scope)?;
    let r = bind_expr(right, input, catalog, scope)?;
    let data_type = binary_result_type(op, l.data_type(), r.data_type())?;
    Ok(ScalarExpr::Binary {
        op,
        left: Box::new(l),
        right: Box::new(r),
        data_type,
    })
}

#[cfg(test)]
mod typed_literal_tests {
    use super::{days_since_epoch, parse_date_literal};

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
    fn algorithm_handles_leap_year_february() {
        let feb29 = days_since_epoch(2000, 2, 29);
        let mar01 = days_since_epoch(2000, 3, 1);
        assert_eq!(mar01 - feb29, 1, "2000-02-29 → 2000-03-01 is one day");
    }
}
