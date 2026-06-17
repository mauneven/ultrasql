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

use num_traits::ToPrimitive;
use ultrasql_core::{
    BitString, DataType, GeometryType, GeometryValue, MAX_VECTOR_DIMS, Oid, RangeType, RangeValue,
    Value, coerce_bpchar_text, composite_text_matches_arity, parse_decimal_text, parse_money_text,
    parse_time_text, parse_timestamptz_text, parse_timetz_text,
};
use ultrasql_parser::ast::{BinaryOp, Expr, Literal, ObjectName, UnaryOp};

use super::expr_type::{binary_result_type, comparable, display_unary};
use super::{
    Catalog, PlanError, ScalarExpr, Schema, ScopeFrame, ScopeStack, bind_select_with_ctes,
    derive_agg_output_name, is_aggregate_name, is_scalar_min_max_call, parse_pg_identifier_path,
    plan_contains_outer_column,
};

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BuiltinCollation {
    Default,
    C,
    Posix,
}

#[derive(Clone, Copy, Debug)]
struct BooleanPredicate {
    value: bool,
    is_unknown: bool,
    negated: bool,
}

impl BuiltinCollation {
    pub(super) const fn oid(self) -> u32 {
        match self {
            Self::Default => 100,
            Self::C => 950,
            Self::Posix => 951,
        }
    }
}

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
        Expr::Collate {
            expr: inner,
            collation,
            ..
        } => {
            let bound = bind_expr_with_ctes(inner, input, catalog, cte_catalog, scope)?;
            bind_collated_expr(collation, bound)
        }
        Expr::AtTimeZone {
            expr: inner, zone, ..
        } => bind_at_time_zone(inner, zone, input, catalog, cte_catalog, scope),
        Expr::ArrayLiteral { elements, .. } => {
            bind_array_literal(elements, input, catalog, cte_catalog, scope)
        }
        Expr::ArraySubscript {
            expr: array_expr,
            index,
            ..
        } => bind_array_subscript(array_expr, index, input, catalog, cte_catalog, scope),
        Expr::ArraySlice {
            expr: array_expr,
            lower,
            upper,
            ..
        } => bind_array_slice(
            array_expr,
            lower.as_deref(),
            upper.as_deref(),
            input,
            catalog,
            cte_catalog,
            scope,
        ),
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
        } => bind_between(BindBetweenArgs {
            subject,
            low,
            high,
            negated: *negated,
            symmetric: *symmetric,
            input,
            catalog,
            cte_catalog,
            scope,
        }),

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

        Expr::IsBoolean {
            expr: inner,
            value,
            is_unknown,
            negated,
            ..
        } => bind_is_boolean_expr(
            inner,
            BooleanPredicate {
                value: *value,
                is_unknown: *is_unknown,
                negated: *negated,
            },
            input,
            catalog,
            cte_catalog,
            scope,
        ),

        Expr::IsDistinctFrom {
            left,
            right,
            negated,
            ..
        } => bind_is_distinct_from_expr(left, right, *negated, input, catalog, cte_catalog, scope),

        _ => Err(PlanError::NotSupported("expression variant")),
    }
}

fn bind_is_boolean_expr(
    inner: &Expr,
    predicate: BooleanPredicate,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let mut bound = bind_expr_with_ctes(inner, input, catalog, cte_catalog, scope)?;
    coerce_literal_to_type(&mut bound, &DataType::Bool);
    let data_type = bound.data_type();
    if !matches!(data_type, DataType::Bool | DataType::Null) {
        return Err(PlanError::TypeMismatch(format!(
            "IS boolean predicate requires boolean input, got {data_type}"
        )));
    }
    if predicate.is_unknown {
        return Ok(ScalarExpr::IsNull {
            expr: Box::new(bound),
            negated: predicate.negated,
        });
    }
    let name = match (predicate.value, predicate.negated) {
        (true, false) => "is_true",
        (true, true) => "is_not_true",
        (false, false) => "is_false",
        (false, true) => "is_not_false",
    };
    Ok(ScalarExpr::FunctionCall {
        name: name.to_owned(),
        args: vec![bound],
        data_type: DataType::Bool,
    })
}

fn bind_is_distinct_from_expr(
    left: &Expr,
    right: &Expr,
    negated: bool,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let mut left = bind_expr_with_ctes(left, input, catalog, cte_catalog, scope)?;
    let mut right = bind_expr_with_ctes(right, input, catalog, cte_catalog, scope)?;
    coerce_literal_to_match(&mut left, &mut right);
    let left_type = left.data_type();
    let right_type = right.data_type();
    if !comparable(&left_type, &right_type) {
        return Err(PlanError::TypeMismatch(format!(
            "IS DISTINCT FROM: cannot compare {left_type} and {right_type}"
        )));
    }
    Ok(ScalarExpr::FunctionCall {
        name: if negated {
            "is_not_distinct_from".to_owned()
        } else {
            "is_distinct_from".to_owned()
        },
        args: vec![left, right],
        data_type: DataType::Bool,
    })
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

pub(super) fn bind_collated_expr(
    collation: &ObjectName,
    bound: ScalarExpr,
) -> Result<ScalarExpr, PlanError> {
    resolve_builtin_collation(collation)?;
    let data_type = bound.data_type();
    if !data_type.is_textlike() {
        return Err(PlanError::TypeMismatch(format!(
            "COLLATE applies to text types, got {data_type}"
        )));
    }
    Ok(bound)
}

pub(super) fn resolve_builtin_collation(
    collation: &ObjectName,
) -> Result<BuiltinCollation, PlanError> {
    let parts: Vec<String> = collation
        .parts
        .iter()
        .map(|part| part.value.to_ascii_lowercase())
        .collect();
    let name = match parts.as_slice() {
        [name] => name.as_str(),
        [schema, name] if schema == "pg_catalog" => name.as_str(),
        _ => {
            return Err(PlanError::TypeMismatch(format!(
                "unsupported collation {collation}"
            )));
        }
    };
    match name {
        "default" => Ok(BuiltinCollation::Default),
        "c" => Ok(BuiltinCollation::C),
        "posix" => Ok(BuiltinCollation::Posix),
        _ => Err(PlanError::TypeMismatch(format!(
            "unsupported collation {collation}"
        ))),
    }
}

pub(super) fn common_scalar_return_type(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<DataType, PlanError> {
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

pub(super) fn coerce_args_to_common_type(args: &mut [ScalarExpr], target: &DataType) {
    for arg in args {
        coerce_literal_to_type(arg, target);
    }
}

pub(super) fn coerce_common_builtin_args(
    func_name: &str,
    args: &mut [ScalarExpr],
    target: &DataType,
) {
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
    if let ScalarExpr::Parameter { index, .. } = bound {
        return Ok(ScalarExpr::Parameter {
            index,
            data_type: target_type,
        });
    }
    let actual_type = bound.data_type();
    if cast_result_matches(&target_type, &actual_type) || matches!(actual_type, DataType::Null) {
        return Ok(bound);
    }
    if let Some(runtime_cast) = bind_runtime_cast(bound.clone(), &target_type, &actual_type) {
        return Ok(runtime_cast);
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
        DataType::Int16 if actual_type.is_integer() || actual_type.is_textlike() => {
            "__ultrasql_cast_int2"
        }
        DataType::Int32 if actual_type.is_integer() || actual_type.is_textlike() => {
            "__ultrasql_cast_int4"
        }
        DataType::Int64 if actual_type.is_integer() || actual_type.is_textlike() => {
            "__ultrasql_cast_int8"
        }
        DataType::Float32 if actual_type.is_numeric() || actual_type.is_textlike() => {
            "__ultrasql_cast_float4"
        }
        DataType::Float64 if actual_type.is_numeric() || actual_type.is_textlike() => {
            "__ultrasql_cast_float8"
        }
        DataType::Bool if actual_type.is_textlike() => "__ultrasql_cast_bool",
        DataType::Date if actual_type.is_textlike() => "__ultrasql_cast_date",
        DataType::Time if actual_type.is_textlike() => "__ultrasql_cast_time",
        DataType::Timestamp if actual_type.is_textlike() => "__ultrasql_cast_timestamp",
        DataType::TimestampTz if actual_type.is_textlike() => "__ultrasql_cast_timestamptz",
        DataType::TimeTz if actual_type.is_textlike() => "__ultrasql_cast_timetz",
        DataType::Uuid if actual_type.is_textlike() => "__ultrasql_cast_uuid",
        DataType::Json if actual_type.is_textlike() => "__ultrasql_cast_json",
        DataType::Jsonb if actual_type.is_textlike() => "__ultrasql_cast_jsonb",
        DataType::Xml if actual_type.is_textlike() => "__ultrasql_cast_xml",
        DataType::Money
            if actual_type.is_integer()
                || actual_type.is_textlike()
                || matches!(actual_type, DataType::Decimal { .. }) =>
        {
            "__ultrasql_cast_money"
        }
        DataType::Decimal { .. }
            if actual_type.is_numeric()
                || actual_type.is_textlike()
                || matches!(actual_type, DataType::Money) =>
        {
            "__ultrasql_cast_numeric"
        }
        DataType::Oid if actual_type.is_oid_alias() || actual_type.is_integer() => {
            "__ultrasql_cast_oid"
        }
        DataType::RegClass if actual_type.is_oid_alias() || actual_type.is_integer() => {
            "__ultrasql_cast_regclass"
        }
        DataType::RegType if actual_type.is_oid_alias() || actual_type.is_integer() => {
            "__ultrasql_cast_regtype"
        }
        DataType::Text { .. } => "__ultrasql_cast_text",
        _ => return None,
    };
    let data_type = if matches!(
        (target_type, actual_type),
        (
            DataType::Decimal {
                precision: None,
                scale: None
            },
            DataType::Money
        )
    ) {
        DataType::Decimal {
            precision: None,
            scale: Some(2),
        }
    } else {
        target_type.clone()
    };
    let args = if let DataType::Decimal { precision, scale } = target_type {
        vec![
            expr,
            runtime_typmod_i32(precision.and_then(|value| i32::try_from(value).ok())),
            runtime_typmod_i32(*scale),
        ]
    } else {
        vec![expr]
    };
    Some(ScalarExpr::FunctionCall {
        name: name.to_owned(),
        args,
        data_type,
    })
}

fn runtime_typmod_i32(value: Option<i32>) -> ScalarExpr {
    match value {
        Some(value) => ScalarExpr::Literal {
            value: Value::Int32(value),
            data_type: DataType::Int32,
        },
        None => ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        },
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
pub(super) struct BindBetweenArgs<'a> {
    subject: &'a Expr,
    low: &'a Expr,
    high: &'a Expr,
    negated: bool,
    symmetric: bool,
    input: &'a Schema,
    catalog: &'a dyn Catalog,
    cte_catalog: &'a [(String, Schema)],
    scope: &'a mut ScopeStack,
}

pub(super) fn bind_between(args: BindBetweenArgs<'_>) -> Result<ScalarExpr, PlanError> {
    let BindBetweenArgs {
        subject,
        low,
        high,
        negated,
        symmetric,
        input,
        catalog,
        cte_catalog,
        scope,
    } = args;
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
    coerce_binary_literals(op, &mut left, &mut right);
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
        "tsvector" => ScalarExpr::Literal {
            value: Value::Text(value.to_owned()),
            data_type: DataType::TsVector,
        },
        "tsquery" => ScalarExpr::Literal {
            value: Value::Text(value.to_owned()),
            data_type: DataType::TsQuery,
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
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    days_since_epoch(year, month, day)
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
    parse_timestamptz_text(text)
}

fn parse_time_of_day_micros(text: &str) -> Option<i64> {
    parse_time_text(text)
}

fn parse_timetz_literal(text: &str) -> Option<(i64, i32)> {
    parse_timetz_text(text)
}

fn civil_from_days(days_since_2000_01_01: i32) -> Result<(i32, u32, u32), PlanError> {
    let z = days_since_2000_01_01 + 10_957;
    let z = z + 719_468;
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day_i32 = doy - (153 * mp + 2) / 5 + 1;
    let month_i32 = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month_i32 <= 2 { y + 1 } else { y };
    let month = u32::try_from(month_i32)
        .map_err(|_| PlanError::TypeMismatch("date interval month overflow".to_owned()))?;
    let day = u32::try_from(day_i32)
        .map_err(|_| PlanError::TypeMismatch("date interval day overflow".to_owned()))?;
    Ok((year, month, day))
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
    let (year, month, day) = civil_from_days(date_days)?;
    let total_months = year
        .checked_mul(12)
        .and_then(|v| v.checked_add(i32::try_from(month).ok()? - 1))
        .and_then(|v| v.checked_add(month_delta))
        .ok_or_else(|| PlanError::TypeMismatch("date interval month overflow".to_owned()))?;
    let new_year = total_months.div_euclid(12);
    let new_month = u32::try_from(total_months.rem_euclid(12) + 1)
        .map_err(|_| PlanError::TypeMismatch("date interval month overflow".to_owned()))?;
    let new_day = day.min(days_in_month(new_year, new_month));
    days_since_epoch(new_year, new_month, new_day)
        .ok_or_else(|| PlanError::TypeMismatch("date interval day overflow".to_owned()))
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

fn literal_numeric_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => v.to_f64(),
        Value::Float32(v) => Some(f64::from(*v)),
        Value::Float64(v) => Some(*v),
        Value::Decimal {
            value: decimal_value,
            scale,
        } => decimal_value_to_f64(*decimal_value, *scale),
        _ => None,
    }
}

/// Statically infer the return type of a builtin scalar function.
/// The set must stay in sync with the executor's `eval_function_call`
/// dispatcher in [`crates/ultrasql-executor/src/eval.rs`].
pub(super) fn builtin_return_type(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<DataType, PlanError> {
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
        "timezone" => timezone_return_type(args),
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
        | "reverse" | "md5" | "sha256" | "quote_ident" | "quote_literal" | "format"
        | "regexp_replace" => Ok(DataType::Text { max_len: None }),
        "to_tsvector" => Ok(DataType::TsVector),
        "to_tsquery" | "plainto_tsquery" | "websearch_to_tsquery" | "phraseto_tsquery" => {
            Ok(DataType::TsQuery)
        }
        "ts_rank" | "ts_rank_cd" => Ok(DataType::Float64),
        "ts_headline" => Ok(DataType::Text { max_len: None }),
        "numnode" => Ok(DataType::Int32),
        "querytree" => Ok(DataType::Text { max_len: None }),
        "row_to_json" | "json_build_object" | "jsonb_set" => Ok(DataType::Jsonb),
        "jsonb_path_exists"
        | "xml_is_well_formed"
        | "xml_is_well_formed_content"
        | "xml_is_well_formed_document"
        | "xpath_exists" => Ok(DataType::Bool),
        "xmlparse" => Ok(DataType::Xml),
        "xmlserialize" => Ok(DataType::Text { max_len: None }),
        "xpath" => Ok(DataType::Array(Box::new(DataType::Xml))),
        "host" => Ok(DataType::Text { max_len: None }),
        "family" | "masklen" => Ok(DataType::Int32),
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
        "array_length" | "array_ndims" | "array_lower" | "array_upper" | "cardinality" => {
            Ok(DataType::Int32)
        }
        "array_position" => Ok(DataType::Int32),
        "array_dims" => Ok(DataType::Text { max_len: None }),
        "array_to_string" => Ok(DataType::Text { max_len: None }),
        "string_to_array" | "array_cat" => {
            Ok(DataType::Array(Box::new(DataType::Text { max_len: None })))
        }
        "array_append" | "array_remove" => array_mutation_return_type(func_name, args, 0),
        "array_prepend" => array_mutation_return_type(func_name, args, 1),
        "array_replace" => array_replace_return_type(func_name, args),
        "trim_array" => array_argument_return_type(func_name, args, 0, 2),
        "array_positions" => {
            validate_array_element_argument(func_name, args, 0, 1, 2)?;
            Ok(DataType::Array(Box::new(DataType::Int32)))
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

fn array_argument_return_type(
    func_name: &str,
    args: &[ScalarExpr],
    array_arg_idx: usize,
    expected_args: usize,
) -> Result<DataType, PlanError> {
    if args.len() != expected_args {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected {expected_args} arguments, got {}",
            args.len()
        )));
    }
    let array_type = args[array_arg_idx].data_type();
    if matches!(array_type, DataType::Array(_)) {
        Ok(array_type)
    } else {
        Err(PlanError::TypeMismatch(format!(
            "{func_name}: array argument required, got {array_type:?}"
        )))
    }
}

fn array_mutation_return_type(
    func_name: &str,
    args: &[ScalarExpr],
    array_arg_idx: usize,
) -> Result<DataType, PlanError> {
    validate_array_element_argument(func_name, args, array_arg_idx, 1 - array_arg_idx, 2)
}

fn array_replace_return_type(func_name: &str, args: &[ScalarExpr]) -> Result<DataType, PlanError> {
    let array_type = validate_array_element_argument(func_name, args, 0, 1, 3)?;
    let DataType::Array(element_type) = &array_type else {
        return Ok(array_type);
    };
    let replacement_type = args[2].data_type();
    if matches!(replacement_type, DataType::Null) || replacement_type == *element_type.as_ref() {
        Ok(array_type)
    } else {
        Err(PlanError::TypeMismatch(format!(
            "{func_name}: replacement type mismatch, expected {:?}, got {:?}",
            element_type.as_ref(),
            replacement_type
        )))
    }
}

fn validate_array_element_argument(
    func_name: &str,
    args: &[ScalarExpr],
    array_arg_idx: usize,
    value_arg_idx: usize,
    expected_args: usize,
) -> Result<DataType, PlanError> {
    if args.len() != expected_args {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected {expected_args} arguments, got {}",
            args.len()
        )));
    }
    let array_type = args[array_arg_idx].data_type();
    let DataType::Array(element_type) = &array_type else {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: array argument required, got {array_type:?}"
        )));
    };
    let value_type = args[value_arg_idx].data_type();
    if matches!(value_type, DataType::Null) || value_type == *element_type.as_ref() {
        Ok(array_type)
    } else {
        Err(PlanError::TypeMismatch(format!(
            "{func_name}: element type mismatch, expected {:?}, got {:?}",
            element_type.as_ref(),
            value_type
        )))
    }
}

/// True when the binder accepts the function name as a v0.6 builtin.
/// Used by the `_` fallback in the expression-variant path to keep
/// the diagnostic precise: unknown function names still report
/// `non-aggregate function calls`.
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
            | "quote_literal"
            | "format"
            | "regexp_replace"
            | "to_tsvector"
            | "to_tsquery"
            | "plainto_tsquery"
            | "websearch_to_tsquery"
            | "phraseto_tsquery"
            | "ts_rank"
            | "ts_rank_cd"
            | "ts_headline"
            | "numnode"
            | "querytree"
            | "row_to_json"
            | "json_build_object"
            | "jsonb_set"
            | "jsonb_path_exists"
            | "xmlparse"
            | "xmlserialize"
            | "xml_is_well_formed"
            | "xml_is_well_formed_content"
            | "xml_is_well_formed_document"
            | "xpath"
            | "xpath_exists"
            | "host"
            | "family"
            | "masklen"
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
            | "array_ndims"
            | "array_lower"
            | "array_upper"
            | "array_dims"
            | "cardinality"
            | "array_position"
            | "array_to_string"
            | "string_to_array"
            | "array_cat"
            | "array_append"
            | "array_prepend"
            | "array_remove"
            | "array_replace"
            | "array_positions"
            | "trim_array"
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

pub(super) fn validate_builtin_args(
    func_name: &str,
    args: &mut [ScalarExpr],
) -> Result<(), PlanError> {
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
        "to_tsvector"
        | "to_tsquery"
        | "plainto_tsquery"
        | "websearch_to_tsquery"
        | "phraseto_tsquery" => validate_text_search_constructor_args(func_name, args),
        "ts_rank" | "ts_rank_cd" => validate_ts_rank_args(func_name, args),
        "ts_headline" => validate_ts_headline_args(args),
        "numnode" | "querytree" => validate_tsquery_inspector_args(func_name, args),
        "xmlparse" => validate_xmlparse_args(args),
        "xmlserialize" => validate_xmlserialize_args(args),
        "xml_is_well_formed" | "xml_is_well_formed_content" | "xml_is_well_formed_document" => {
            validate_xml_well_formed_args(func_name, args)
        }
        "xpath" | "xpath_exists" => validate_xpath_args(func_name, args),
        "host" | "family" | "masklen" => validate_network_inspector_args(func_name, args),
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
    if !(2..=3).contains(&args.len()) {
        return Err(PlanError::TypeMismatch(format!(
            "jsonb_path_exists: expected 2 or 3 arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

fn validate_xml_well_formed_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    validate_exact_arg_count(func_name, args, 1)?;
    validate_text_or_xml_arg(func_name, &args[0])
}

fn validate_xmlparse_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    validate_exact_arg_count("xmlparse", args, 2)?;
    validate_xml_mode_arg("xmlparse", &args[0])?;
    validate_text_or_xml_arg("xmlparse", &args[1])
}

fn validate_xmlserialize_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    validate_exact_arg_count("xmlserialize", args, 3)?;
    validate_xml_mode_arg("xmlserialize", &args[0])?;
    validate_text_or_xml_arg("xmlserialize", &args[1])?;
    let Some(target) = literal_text_arg(&args[2]) else {
        return Err(PlanError::TypeMismatch(
            "xmlserialize: target type must be a parser-supplied text literal".to_owned(),
        ));
    };
    if target.eq_ignore_ascii_case("text") {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "xmlserialize: only AS TEXT is supported, got {target}"
    )))
}

fn validate_xpath_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    if !(2..=3).contains(&args.len()) {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected 2 or 3 arguments, got {}",
            args.len()
        )));
    }
    validate_text_or_xml_arg(func_name, &args[0])?;
    validate_text_or_xml_arg(func_name, &args[1])?;
    if let Some(namespace_arg) = args.get(2) {
        let data_type = namespace_arg.data_type();
        if !matches!(data_type, DataType::Null | DataType::Array(_)) {
            return Err(PlanError::TypeMismatch(format!(
                "{func_name}: namespace argument must be text[][], got {data_type}"
            )));
        }
    }
    Ok(())
}

fn validate_network_inspector_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    validate_exact_arg_count(func_name, args, 1)?;
    let data_type = args[0].data_type();
    if matches!(data_type, DataType::Null) || data_type.is_ip_network() {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected inet or cidr, got {data_type}"
    )))
}

fn validate_text_search_constructor_args(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
    match args.len() {
        1 => validate_text_arg(func_name, &args[0]),
        2 => {
            validate_text_arg(func_name, &args[0])?;
            validate_text_arg(func_name, &args[1])
        }
        n => Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected 1 or 2 arguments, got {n}"
        ))),
    }
}

fn validate_ts_rank_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    if args.len() != 2 {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected 2 arguments, got {}",
            args.len()
        )));
    }
    validate_tsvector_arg(func_name, &args[0])?;
    validate_tsquery_arg(func_name, &args[1])
}

fn validate_ts_headline_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    match args.len() {
        2 => {
            validate_text_arg("ts_headline", &args[0])?;
            validate_tsquery_arg("ts_headline", &args[1])
        }
        3 => {
            validate_text_arg("ts_headline", &args[0])?;
            validate_text_arg("ts_headline", &args[1])?;
            validate_tsquery_arg("ts_headline", &args[2])
        }
        n => Err(PlanError::TypeMismatch(format!(
            "ts_headline: expected 2 or 3 arguments, got {n}"
        ))),
    }
}

fn validate_tsquery_inspector_args(func_name: &str, args: &[ScalarExpr]) -> Result<(), PlanError> {
    validate_exact_arg_count(func_name, args, 1)?;
    validate_tsquery_arg(func_name, &args[0])
}

fn validate_tsvector_arg(func_name: &str, arg: &ScalarExpr) -> Result<(), PlanError> {
    let data_type = arg.data_type();
    if matches!(data_type, DataType::Null | DataType::TsVector) {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected tsvector, got {data_type}"
    )))
}

fn validate_tsquery_arg(func_name: &str, arg: &ScalarExpr) -> Result<(), PlanError> {
    let data_type = arg.data_type();
    if matches!(data_type, DataType::Null | DataType::TsQuery) {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected tsquery, got {data_type}"
    )))
}

fn validate_xml_mode_arg(func_name: &str, arg: &ScalarExpr) -> Result<(), PlanError> {
    let Some(mode) = literal_text_arg(arg) else {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: mode must be DOCUMENT or CONTENT"
        )));
    };
    if mode.eq_ignore_ascii_case("document") || mode.eq_ignore_ascii_case("content") {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: mode must be DOCUMENT or CONTENT, got {mode}"
    )))
}

fn literal_text_arg(arg: &ScalarExpr) -> Option<&str> {
    match arg {
        ScalarExpr::Literal {
            value: Value::Text(text) | Value::Char(text),
            ..
        } => Some(text),
        _ => None,
    }
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

fn validate_text_arg(func_name: &str, arg: &ScalarExpr) -> Result<(), PlanError> {
    let data_type = arg.data_type();
    if matches!(data_type, DataType::Null) || data_type.is_textlike() {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected text, got {data_type}"
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
    if args.len() != 4 && args.len() != 5 {
        return Err(PlanError::TypeMismatch(format!(
            "hybrid_search: expected 4 or 5 arguments, got {}",
            args.len()
        )));
    }

    // Optional 5th argument selects the fusion method ('rrf' | 'weighted').
    if let Some(fusion_arg) = args.get(4) {
        let fusion_type = fusion_arg.data_type();
        if !matches!(fusion_type, DataType::Text { .. }) {
            return Err(PlanError::TypeMismatch(format!(
                "hybrid_search: fifth argument (fusion method) must be text, got {fusion_type}"
            )));
        }
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
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    let y = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let month_i32 = i32::try_from(month).ok()?;
    let day_i32 = i32::try_from(day).ok()?;
    let month_offset = if month > 2 {
        month_i32 - 3
    } else {
        month_i32 + 9
    };
    let doy = (153 * month_offset + 2) / 5 + day_i32 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days_from_1970_03_01 = i64::from(era)
        .checked_mul(146_097)?
        .checked_add(i64::from(doe))?
        .checked_sub(719_468)?;
    // Rebase from 1970-01-01 to 2000-01-01 (10_957 days).
    let days_since_2000_01_01 = days_from_1970_03_01.checked_sub(10_957)?;
    i32::try_from(days_since_2000_01_01).ok()
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

fn parse_bool_text(text: &str) -> Option<bool> {
    match text.trim() {
        "t" | "true" | "TRUE" | "T" | "1" | "y" | "Y" | "yes" | "YES" | "on" | "ON" => Some(true),
        "f" | "false" | "FALSE" | "F" | "0" | "n" | "N" | "no" | "NO" | "off" | "OFF" => {
            Some(false)
        }
        _ => None,
    }
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
        (Some(target), _) => target,
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

fn decimal_value_to_f64(value: i64, scale: i32) -> Option<f64> {
    value.to_f64().map(|raw| raw / 10_f64.powi(scale))
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
                resolve_regtype_literal(text, catalog)
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
    if let Some(oid) = Value::parse_oid_text(text) {
        return Some(oid);
    }
    let parts = parse_pg_identifier_path(text)?;
    match parts.as_slice() {
        [name] => catalog.lookup_table_oid(name),
        [schema_name, relation_name] => {
            catalog.lookup_table_oid_in_schema(schema_name, relation_name)
        }
        _ => None,
    }
}

fn resolve_regtype_literal(text: &str, catalog: &dyn Catalog) -> Option<Oid> {
    if let Some(oid) = Value::parse_oid_text(text) {
        return Some(oid);
    }
    let parts = parse_pg_identifier_path(text)?;
    match parts.as_slice() {
        [name] => catalog.lookup_type_oid(name),
        [schema_name, type_name] => catalog.lookup_type_oid_in_schema(schema_name, type_name),
        _ => None,
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
        (UnaryOp::Neg, Value::Money(v)) => v
            .checked_neg()
            .map(|neg| (Value::Money(neg), data_type.clone())),
        _ => None,
    };

    if let Some((value, data_type)) = folded {
        *expr = ScalarExpr::Literal { value, data_type };
    }
}

fn parse_negative_i64_boundary(text: &str) -> Option<i64> {
    let unsigned = text.replace('_', "");
    let magnitude = unsigned.parse::<u128>().ok()?;
    let max_plus_one = u128::try_from(i64::MAX).ok()?.checked_add(1)?;
    (magnitude == max_plus_one).then_some(i64::MIN)
}

fn parse_negative_i64_boundary_expr(expr: &Expr) -> Option<i64> {
    let Expr::Literal(Literal::Integer { text, .. }) = expr else {
        return None;
    };
    parse_negative_i64_boundary(text)
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
        (DataType::Bool, Value::Text(text)) => {
            if let Some(parsed) = parse_bool_text(text) {
                *value = Value::Bool(parsed);
                *data_type = DataType::Bool;
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
            if let Some(widened) = v.to_f64() {
                *value = Value::Float64(widened);
                *data_type = DataType::Float64;
            }
        }
        (
            DataType::Float64,
            Value::Decimal {
                value: decimal_value,
                scale,
            },
        ) => {
            if let Some(widened) = decimal_value_to_f64(*decimal_value, *scale) {
                *value = Value::Float64(widened);
                *data_type = DataType::Float64;
            }
        }
        (DataType::Float32, Value::Float64(v)) => {
            if let Some(narrow) = v.to_f32() {
                *value = Value::Float32(narrow);
                *data_type = DataType::Float32;
            }
        }
        (DataType::Float32, Value::Int16(v)) => {
            *value = Value::Float32(f32::from(*v));
            *data_type = DataType::Float32;
        }
        (DataType::Float32, Value::Int32(v)) => {
            if let Some(widened) = v.to_f32() {
                *value = Value::Float32(widened);
                *data_type = DataType::Float32;
            }
        }
        (DataType::Float32, Value::Int64(v)) => {
            if let Some(widened) = v.to_f32() {
                *value = Value::Float32(widened);
                *data_type = DataType::Float32;
            }
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
            if let Some(narrow) =
                decimal_value_to_f64(*decimal_value, *scale).and_then(|value| value.to_f32())
            {
                *value = Value::Float32(narrow);
                *data_type = DataType::Float32;
            }
        }
        (DataType::Decimal { precision, scale }, Value::Text(text)) => {
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
                    precision: *precision,
                    scale: scale.or(Some(decimal_scale)),
                };
            }
        }
        (DataType::Decimal { precision, scale }, _) => {
            if let Some((decimal_value, decimal_scale)) = decimal_from_numeric_value(value, *scale)
            {
                *value = Value::Decimal {
                    value: decimal_value,
                    scale: decimal_scale,
                };
                *data_type = DataType::Decimal {
                    precision: *precision,
                    scale: scale.or(Some(decimal_scale)),
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
    if let Some(data_type) = parse_decimal_type_name(&type_name) {
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
        "text" => Some(DataType::Text { max_len: None }),
        "tsvector" => Some(DataType::TsVector),
        "tsquery" => Some(DataType::TsQuery),
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

const MAX_CAST_NUMERIC_PRECISION: u32 = 131_072;

fn parse_decimal_type_name(type_name: &str) -> Option<DataType> {
    if matches!(type_name, "numeric" | "decimal") {
        return Some(DataType::Decimal {
            precision: None,
            scale: None,
        });
    }
    let (base, modifiers) = parse_type_modifiers(type_name)?;
    if !matches!(base, "numeric" | "decimal") || modifiers.is_empty() || modifiers.len() > 2 {
        return None;
    }
    let precision = *modifiers.first()?;
    if precision == 0 || precision > MAX_CAST_NUMERIC_PRECISION {
        return None;
    }
    let scale = match modifiers.as_slice() {
        [_] => Some(0),
        [_, scale] => Some(i32::try_from(*scale).ok()?),
        _ => return None,
    };
    Some(DataType::Decimal {
        precision: Some(precision),
        scale,
    })
}

fn resolve_cast_type_with_catalog(type_name: &str, catalog: &dyn Catalog) -> Option<DataType> {
    resolve_cast_type(type_name).or_else(|| {
        let parts = parse_pg_identifier_path(type_name)?;
        match parts.as_slice() {
            [name] => resolve_cast_type(name).or_else(|| catalog.lookup_type(name)),
            [schema_name, type_name] => {
                if schema_name.eq_ignore_ascii_case("pg_catalog")
                    && let Some(data_type) = resolve_cast_type(type_name)
                {
                    return Some(data_type);
                }
                catalog.lookup_type_in_schema(schema_name, type_name)
            }
            _ => None,
        }
    })
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
    let (base, modifiers) = parse_type_modifiers(type_name)?;
    let [len] = modifiers.as_slice() else {
        return None;
    };
    Some((base, *len))
}

fn parse_type_modifiers(type_name: &str) -> Option<(&str, Vec<u32>)> {
    let (base, rest) = type_name.split_once('(')?;
    let raw = rest.strip_suffix(')')?;
    let modifiers = raw
        .split(',')
        .map(str::trim)
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    Some((base, modifiers))
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

fn bind_array_slice(
    array_expr: &Expr,
    lower: Option<&Expr>,
    upper: Option<&Expr>,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let array = bind_expr_with_ctes(array_expr, input, catalog, cte_catalog, scope)?;
    let array_type = array.data_type();
    let DataType::Array(_) = array_type else {
        return Err(PlanError::TypeMismatch(format!(
            "array slice requires array input, got {array_type}"
        )));
    };
    let lower = bind_optional_array_bound(lower, input, catalog, cte_catalog, scope)?;
    let upper = bind_optional_array_bound(upper, input, catalog, cte_catalog, scope)?;
    Ok(ScalarExpr::FunctionCall {
        name: "__ultrasql_array_slice".to_owned(),
        args: vec![array, lower, upper],
        data_type: array_type,
    })
}

fn bind_at_time_zone(
    expr: &Expr,
    zone: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let source = bind_expr_with_ctes(expr, input, catalog, cte_catalog, scope)?;
    let mut zone = bind_expr_with_ctes(zone, input, catalog, cte_catalog, scope)?;
    coerce_literal_to_type(&mut zone, &DataType::Text { max_len: None });
    let args = vec![zone, source];
    let data_type = timezone_return_type(&args)?;
    Ok(ScalarExpr::FunctionCall {
        name: "timezone".to_owned(),
        args,
        data_type,
    })
}

fn timezone_return_type(args: &[ScalarExpr]) -> Result<DataType, PlanError> {
    if args.len() != 2 {
        return Err(PlanError::TypeMismatch(format!(
            "timezone: expected 2 arguments, got {}",
            args.len()
        )));
    }
    let zone_type = args[0].data_type();
    if !zone_type.is_textlike() && !matches!(zone_type, DataType::Null) {
        return Err(PlanError::TypeMismatch(format!(
            "timezone: zone must be text, got {zone_type}"
        )));
    }
    match args[1].data_type() {
        DataType::Timestamp => Ok(DataType::TimestampTz),
        DataType::TimestampTz => Ok(DataType::Timestamp),
        DataType::TimeTz => Ok(DataType::TimeTz),
        DataType::Null => Ok(DataType::Null),
        other => Err(PlanError::TypeMismatch(format!(
            "timezone: source must be timestamp, timestamptz, or timetz, got {other}"
        ))),
    }
}

fn bind_optional_array_bound(
    bound: Option<&Expr>,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let Some(bound) = bound else {
        return Ok(ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        });
    };
    let bound = bind_expr_with_ctes(bound, input, catalog, cte_catalog, scope)?;
    let bound_type = bound.data_type();
    if !matches!(
        bound_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Null
    ) {
        return Err(PlanError::TypeMismatch(format!(
            "array slice bound must be integer, got {bound_type}"
        )));
    }
    Ok(bound)
}

pub(super) fn bind_unary(
    op: UnaryOp,
    inner: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    if matches!(op, UnaryOp::Neg)
        && let Some(value) = parse_negative_i64_boundary_expr(inner)
    {
        return Ok(ScalarExpr::Literal {
            value: Value::Int64(value),
            data_type: DataType::Int64,
        });
    }
    let bound = bind_expr_with_ctes(inner, input, catalog, cte_catalog, scope)?;
    let inner_ty = bound.data_type();
    let data_type = match op {
        UnaryOp::Neg | UnaryOp::Pos => {
            if inner_ty.is_numeric() || matches!(inner_ty, DataType::Money) {
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
    let mut expr = ScalarExpr::Unary {
        op,
        expr: Box::new(bound),
        data_type,
    };
    fold_signed_literal(&mut expr);
    Ok(expr)
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
    coerce_binary_literals(op, &mut l, &mut r);
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

fn coerce_binary_literals(op: BinaryOp, left: &mut ScalarExpr, right: &mut ScalarExpr) {
    if binary_operator_uses_raw_text_pattern(op)
        || money_scalar_arithmetic_keeps_operand_types(op, left, right)
    {
        return;
    }
    coerce_literal_to_match(left, right);
}

fn money_scalar_arithmetic_keeps_operand_types(
    op: BinaryOp,
    left: &ScalarExpr,
    right: &ScalarExpr,
) -> bool {
    matches!(op, BinaryOp::Mul | BinaryOp::Div)
        && (matches!(left.data_type(), DataType::Money)
            || matches!(right.data_type(), DataType::Money))
}

#[cfg(test)]
mod typed_literal_tests {
    use std::sync::Arc;

    use ultrasql_core::{
        DataType, GeometryType, Oid, RangeType, Value, composite_text_matches_arity,
    };
    use ultrasql_parser::Span;
    use ultrasql_parser::ast::Literal;

    use super::{
        BinaryOp, ScalarExpr, bind_literal, builtin_return_type, cast_result_matches,
        coerce_literal_to_match, coerce_literal_to_type, common_scalar_return_type,
        days_since_epoch, decimal_from_numeric_value, dense_vector_family_kind, fold_date_interval,
        is_supported_builtin, literal_numeric_as_f64, money_from_literal_value, parse_date_literal,
        parse_decimal_literal, parse_interval_literal, parse_negative_i64_boundary,
        parse_pg_identifier_path, parse_time_of_day_micros, parse_timestamp_literal,
        parse_timestamptz_literal, parse_timetz_literal, pow10_i64, resolve_cast_type,
        scaled_decimal_text_to_i64, try_fold_literal_binary, validate_builtin_args,
        vector_family_cast_matches, vector_metric_family_kind,
    };

    fn lit(value: Value) -> ScalarExpr {
        let data_type = value.data_type();
        ScalarExpr::Literal { value, data_type }
    }

    fn coerce(mut expr: ScalarExpr, target: &DataType) -> ScalarExpr {
        coerce_literal_to_type(&mut expr, target);
        expr
    }

    fn literal_type(expr: &ScalarExpr) -> DataType {
        let ScalarExpr::Literal { data_type, .. } = expr else {
            panic!("expected literal, got {expr:?}");
        };
        data_type.clone()
    }

    fn literal_value(expr: &ScalarExpr) -> Value {
        let ScalarExpr::Literal { value, .. } = expr else {
            panic!("expected literal, got {expr:?}");
        };
        value.clone()
    }

    fn typed(type_name: &str, value: &str, unit: Option<&str>) -> ScalarExpr {
        bind_literal(&Literal::Typed {
            type_name: type_name.to_owned(),
            value: value.to_owned(),
            unit: unit.map(str::to_owned),
            span: Span::default(),
        })
    }

    fn null_arg(data_type: DataType) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Null,
            data_type,
        }
    }

    #[test]
    fn epoch_day_is_zero() {
        assert_eq!(parse_date_literal("2000-01-01"), Some(0));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn typed_literal_matrix_covers_storage_families() {
        let cases = [
            ("date", "2000-01-01", None, DataType::Date, Value::Date(0)),
            ("date", "2000-99-99", None, DataType::Date, Value::Null),
            (
                "time",
                "01:02:03.000004",
                None,
                DataType::Time,
                Value::Time(3_723_000_004),
            ),
            (
                "timetz",
                "01:02:03-05",
                None,
                DataType::TimeTz,
                Value::TimeTz {
                    micros: 3_723_000_000,
                    offset_seconds: -18_000,
                },
            ),
            (
                "timestamp",
                "2000-01-01 00:00:01",
                None,
                DataType::Timestamp,
                Value::Timestamp(1_000_000),
            ),
            (
                "timestamptz",
                "2000-01-01 00:00:01+00",
                None,
                DataType::TimestampTz,
                Value::TimestampTz(1_000_000),
            ),
            (
                "json",
                "{\"ok\":true}",
                None,
                DataType::Json,
                Value::Json("{\"ok\":true}".to_owned()),
            ),
            ("json", "{bad", None, DataType::Json, Value::Null),
            (
                "jsonb",
                "[1,2]",
                None,
                DataType::Jsonb,
                Value::Jsonb("[1,2]".to_owned()),
            ),
            (
                "xml",
                "<root/>",
                None,
                DataType::Xml,
                Value::Xml("<root/>".to_owned()),
            ),
            ("xml", "<root>", None, DataType::Xml, Value::Null),
            ("money", "12.34", None, DataType::Money, Value::Money(1234)),
            ("oid", "42", None, DataType::Oid, Value::Oid(Oid::new(42))),
            (
                "pg_lsn",
                "1/10",
                None,
                DataType::PgLsn,
                Value::PgLsn(ultrasql_core::Lsn::new(0x1_0000_0010)),
            ),
            (
                "tsvector",
                "hello",
                None,
                DataType::TsVector,
                Value::Text("hello".to_owned()),
            ),
            ("unknown_type", "x", None, DataType::Null, Value::Null),
        ];

        for (type_name, text, unit, data_type, value) in cases {
            let expr = typed(type_name, text, unit);
            assert_eq!(literal_type(&expr), data_type, "{type_name} {text}");
            assert_eq!(literal_value(&expr), value, "{type_name} {text}");
        }

        for (unit, expected) in [
            ("years", (24, 0, 0)),
            ("months", (2, 0, 0)),
            ("days", (0, 2, 0)),
            ("hours", (0, 0, 7_200_000_000)),
            ("minutes", (0, 0, 120_000_000)),
            ("seconds", (0, 0, 2_000_000)),
        ] {
            assert_eq!(
                parse_interval_literal("2", Some(unit)),
                Some(expected),
                "{unit}"
            );
        }
        assert!(parse_interval_literal("2", Some("fortnights")).is_none());
        assert!(parse_interval_literal("999999999999999999", Some("hours")).is_none());

        assert_eq!(
            literal_type(&typed("bit", "101", None)),
            DataType::Bit { len: Some(3) }
        );
        assert_eq!(
            literal_type(&typed("bit", "102", None)),
            DataType::Bit { len: None }
        );
        assert_eq!(
            literal_type(&typed("varbit", "1010", None)),
            DataType::VarBit { max_len: Some(4) }
        );
        assert_eq!(
            literal_type(&typed("bit varying", "1010", None)),
            DataType::VarBit { max_len: Some(4) }
        );

        for (type_name, value, data_type) in [
            ("inet", "127.0.0.1", DataType::Inet),
            ("cidr", "10.0.0.0/8", DataType::Cidr),
            ("macaddr", "08:00:2b:01:02:03", DataType::MacAddr),
            ("macaddr8", "08:00:2b:01:02:03:04:05", DataType::MacAddr8),
        ] {
            let expr = typed(type_name, value, None);
            assert_eq!(literal_type(&expr), data_type, "{type_name}");
        }

        for (type_name, value, data_type) in [
            ("halfvec(2)", "[1,2]", DataType::HalfVec { dims: Some(2) }),
            (
                "sparsevec(5)",
                "{1:1,5:2}/5",
                DataType::SparseVec { dims: Some(5) },
            ),
            ("bitvec(4)", "1010", DataType::BitVec { dims: Some(4) }),
        ] {
            let expr = typed(type_name, value, None);
            assert_eq!(literal_type(&expr), data_type, "{type_name}");
            assert!(!matches!(literal_value(&expr), Value::Null));
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn literal_coercion_matrix_covers_cast_targets() {
        let enum_type = DataType::Enum {
            oid: Oid::new(70_001),
            name: Arc::from("mood"),
            labels: Arc::from(vec!["sad".to_owned(), "ok".to_owned()].into_boxed_slice()),
        };
        let composite_type = DataType::Composite {
            oid: Oid::new(70_002),
            name: Arc::from("pair_type"),
            fields: Arc::from(
                vec![
                    ("a".to_owned(), DataType::Int32),
                    ("b".to_owned(), DataType::Text { max_len: None }),
                ]
                .into_boxed_slice(),
            ),
        };
        let domain_type = DataType::Domain {
            oid: Oid::new(70_003),
            name: Arc::from("positive_int"),
            base_type: Box::new(DataType::Int32),
            not_null: true,
        };

        let scalar_cases = [
            (lit(Value::Int32(7)), DataType::Int16, Value::Int16(7)),
            (lit(Value::Int64(8)), DataType::Int16, Value::Int16(8)),
            (
                lit(Value::Text("9".to_owned())),
                DataType::Int16,
                Value::Int16(9),
            ),
            (lit(Value::Int16(10)), DataType::Int32, Value::Int32(10)),
            (lit(Value::Int64(11)), DataType::Int32, Value::Int32(11)),
            (
                lit(Value::Text("12".to_owned())),
                DataType::Int32,
                Value::Int32(12),
            ),
            (lit(Value::Int16(13)), DataType::Int64, Value::Int64(13)),
            (lit(Value::Int32(14)), DataType::Int64, Value::Int64(14)),
            (
                lit(Value::Text("15".to_owned())),
                DataType::Int64,
                Value::Int64(15),
            ),
            (
                lit(Value::Text("true".to_owned())),
                DataType::Bool,
                Value::Bool(true),
            ),
            (
                lit(Value::Text("off".to_owned())),
                DataType::Bool,
                Value::Bool(false),
            ),
            (
                lit(Value::Float32(1.25)),
                DataType::Float64,
                Value::Float64(1.25),
            ),
            (lit(Value::Int16(2)), DataType::Float64, Value::Float64(2.0)),
            (lit(Value::Int32(3)), DataType::Float64, Value::Float64(3.0)),
            (lit(Value::Int64(4)), DataType::Float64, Value::Float64(4.0)),
            (
                lit(Value::Decimal {
                    value: 125,
                    scale: 2,
                }),
                DataType::Float64,
                Value::Float64(1.25),
            ),
            (
                lit(Value::Float64(1.5)),
                DataType::Float32,
                Value::Float32(1.5),
            ),
            (lit(Value::Int16(2)), DataType::Float32, Value::Float32(2.0)),
            (lit(Value::Int32(3)), DataType::Float32, Value::Float32(3.0)),
            (lit(Value::Int64(4)), DataType::Float32, Value::Float32(4.0)),
            (
                lit(Value::Decimal {
                    value: 125,
                    scale: 2,
                }),
                DataType::Float32,
                Value::Float32(1.25),
            ),
            (
                lit(Value::Char("hi  ".to_owned())),
                DataType::Text { max_len: None },
                Value::Text("hi  ".to_owned()),
            ),
            (
                lit(Value::Timestamp(7)),
                DataType::TimestampTz,
                Value::TimestampTz(7),
            ),
            (
                lit(Value::TimestampTz(8)),
                DataType::Timestamp,
                Value::Timestamp(8),
            ),
            (
                lit(Value::Text("01:02:03".to_owned())),
                DataType::Time,
                Value::Time(3_723_000_000),
            ),
            (
                lit(Value::Text("01:02:03+02".to_owned())),
                DataType::TimeTz,
                Value::TimeTz {
                    micros: 3_723_000_000,
                    offset_seconds: 7_200,
                },
            ),
            (
                lit(Value::Text("2000-01-01 00:00:01".to_owned())),
                DataType::Timestamp,
                Value::Timestamp(1_000_000),
            ),
            (
                lit(Value::Text("2000-01-01 00:00:01+00".to_owned())),
                DataType::TimestampTz,
                Value::TimestampTz(1_000_000),
            ),
            (
                lit(Value::Text("12.34".to_owned())),
                DataType::Decimal {
                    precision: None,
                    scale: Some(2),
                },
                Value::Decimal {
                    value: 1234,
                    scale: 2,
                },
            ),
            (
                lit(Value::Int32(12)),
                DataType::Decimal {
                    precision: None,
                    scale: Some(2),
                },
                Value::Decimal {
                    value: 1200,
                    scale: 2,
                },
            ),
            (
                lit(Value::Text("12.34".to_owned())),
                DataType::Money,
                Value::Money(1234),
            ),
            (
                lit(Value::Text("[1,3)".to_owned())),
                DataType::Range(RangeType::Int4),
                Value::Range(
                    ultrasql_core::RangeValue::parse(RangeType::Int4, "[1,3)")
                        .expect("range parses"),
                ),
            ),
            (
                lit(Value::Text("(1,2)".to_owned())),
                DataType::Geometry(GeometryType::Point),
                Value::Geometry(
                    ultrasql_core::GeometryValue::parse(GeometryType::Point, "(1,2)")
                        .expect("point parses"),
                ),
            ),
            (
                lit(Value::Text("[1,2,3]".to_owned())),
                DataType::Vector { dims: Some(3) },
                Value::Vector(vec![1.0, 2.0, 3.0]),
            ),
            (
                lit(Value::Text(
                    "550e8400-e29b-41d4-a716-446655440000".to_owned(),
                )),
                DataType::Uuid,
                Value::Uuid(Value::parse_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            ),
            (
                lit(Value::Text("\\x0aff".to_owned())),
                DataType::Bytea,
                Value::Bytea(vec![0x0a, 0xff]),
            ),
            (
                lit(Value::Text("{\"a\":1}".to_owned())),
                DataType::Json,
                Value::Json("{\"a\":1}".to_owned()),
            ),
            (
                lit(Value::Json("{\"a\":1}".to_owned())),
                DataType::Jsonb,
                Value::Jsonb("{\"a\":1}".to_owned()),
            ),
            (
                lit(Value::Jsonb("{\"a\":1}".to_owned())),
                DataType::Json,
                Value::Json("{\"a\":1}".to_owned()),
            ),
            (
                lit(Value::Text("<root/>".to_owned())),
                DataType::Xml,
                Value::Xml("<root/>".to_owned()),
            ),
        ];

        for (input, target, expected) in scalar_cases {
            let expr = coerce(input, &target);
            assert_eq!(literal_value(&expr), expected, "{target}");
        }

        let decimal_target = DataType::Decimal {
            precision: Some(8),
            scale: Some(2),
        };
        let decimal_expr = coerce(lit(Value::Text("12.34".to_owned())), &decimal_target);
        assert_eq!(literal_type(&decimal_expr), decimal_target);

        let enum_expr = coerce(lit(Value::Text("ok".to_owned())), &enum_type);
        assert_eq!(literal_type(&enum_expr), enum_type);

        assert!(composite_text_matches_arity("(1,two)", 2));
        let composite_expr = coerce(lit(Value::Text("(1,two)".to_owned())), &composite_type);
        assert_eq!(literal_type(&composite_expr), composite_type);

        let domain_expr = coerce(lit(Value::Text("42".to_owned())), &domain_type);
        assert_eq!(literal_value(&domain_expr), Value::Int32(42));
        assert_eq!(literal_type(&domain_expr), domain_type);

        let array_expr = coerce(
            lit(Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Null, Value::Int32(2)],
            }),
            &DataType::Array(Box::new(DataType::Int64)),
        );
        assert_eq!(
            literal_type(&array_expr),
            DataType::Array(Box::new(DataType::Int64))
        );
        let text_array_expr = coerce(
            lit(Value::Text("{1,2}".to_owned())),
            &DataType::Array(Box::new(DataType::Int32)),
        );
        assert_eq!(
            literal_type(&text_array_expr),
            DataType::Array(Box::new(DataType::Int32))
        );

        let bit_expr = coerce(
            lit(Value::Text("1010".to_owned())),
            &DataType::Bit { len: Some(4) },
        );
        assert_eq!(literal_type(&bit_expr), DataType::Bit { len: Some(4) });

        let bpchar_expr = coerce(
            lit(Value::Text("hi".to_owned())),
            &DataType::Char { len: Some(4) },
        );
        assert_eq!(literal_value(&bpchar_expr), Value::Char("hi  ".to_owned()));

        let inet_expr = coerce(lit(Value::Text("127.0.0.1".to_owned())), &DataType::Inet);
        assert_eq!(literal_type(&inet_expr), DataType::Inet);

        let regclass_expr = coerce(lit(Value::Text("42".to_owned())), &DataType::RegClass);
        assert_eq!(literal_value(&regclass_expr), Value::RegClass(Oid::new(42)));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn builtin_validation_and_type_matrix_covers_catalog_introspection_surface() {
        let text = DataType::Text { max_len: None };
        let vector3 = DataType::Vector { dims: Some(3) };
        let halfvec3 = DataType::HalfVec { dims: Some(3) };
        let sparse5 = DataType::SparseVec { dims: Some(5) };

        let return_cases = [
            (
                "ifnull",
                vec![null_arg(DataType::Null), null_arg(text.clone())],
                text.clone(),
            ),
            (
                "nullif",
                vec![null_arg(DataType::Int32), null_arg(DataType::Int32)],
                DataType::Int32,
            ),
            ("least", vec![null_arg(DataType::Int32)], DataType::Int32),
            (
                "greatest",
                vec![null_arg(DataType::Float64)],
                DataType::Float64,
            ),
            ("extract", Vec::new(), DataType::Int64),
            ("current_date", Vec::new(), DataType::Date),
            ("now", Vec::new(), DataType::TimestampTz),
            ("age", Vec::new(), DataType::Interval),
            ("abs", Vec::new(), DataType::Int64),
            ("sqrt", Vec::new(), DataType::Float64),
            ("length", Vec::new(), DataType::Int32),
            ("bit_count", Vec::new(), DataType::Int64),
            ("set_bit", Vec::new(), DataType::VarBit { max_len: None }),
            ("lower", Vec::new(), text.clone()),
            ("to_tsvector", Vec::new(), DataType::TsVector),
            ("to_tsquery", Vec::new(), DataType::TsQuery),
            ("plainto_tsquery", Vec::new(), DataType::TsQuery),
            ("ts_rank", Vec::new(), DataType::Float64),
            ("ts_rank_cd", Vec::new(), DataType::Float64),
            ("ts_headline", Vec::new(), text.clone()),
            ("numnode", Vec::new(), DataType::Int32),
            ("querytree", Vec::new(), text.clone()),
            ("row_to_json", Vec::new(), DataType::Jsonb),
            ("jsonb_path_exists", Vec::new(), DataType::Bool),
            (
                "xpath",
                Vec::new(),
                DataType::Array(Box::new(DataType::Xml)),
            ),
            ("pg_advisory_lock", Vec::new(), DataType::Null),
            ("pg_try_advisory_lock", Vec::new(), DataType::Bool),
            ("has_table_privilege", Vec::new(), DataType::Bool),
            ("pg_get_userbyid", Vec::new(), text.clone()),
            ("to_regtype", Vec::new(), DataType::RegType),
            ("gen_random_uuid", Vec::new(), DataType::Uuid),
            ("pg_relation_size", Vec::new(), DataType::Int64),
            (
                "current_schemas",
                Vec::new(),
                DataType::Array(Box::new(text.clone())),
            ),
            ("version", Vec::new(), text.clone()),
            ("array_length", Vec::new(), DataType::Int32),
            ("array_to_string", Vec::new(), text.clone()),
            (
                "string_to_array",
                Vec::new(),
                DataType::Array(Box::new(text.clone())),
            ),
            ("l2_distance", Vec::new(), DataType::Float64),
            ("hybrid_search", Vec::new(), DataType::Float64),
            ("vector_norm", Vec::new(), DataType::Float64),
            ("vector_dims", Vec::new(), DataType::Int32),
        ];
        for (name, args, expected) in return_cases {
            assert_eq!(
                builtin_return_type(name, &args).unwrap(),
                expected,
                "{name}"
            );
            assert!(is_supported_builtin(name), "{name}");
        }
        assert!(builtin_return_type("missing_builtin", &[]).is_err());
        assert!(!is_supported_builtin("missing_builtin"));

        assert!(validate_builtin_args("current_schemas", &mut [null_arg(DataType::Bool)]).is_ok());
        assert!(
            validate_builtin_args("current_schemas", &mut [null_arg(DataType::Int32)]).is_err()
        );
        assert!(
            validate_builtin_args(
                "set_config",
                &mut [
                    null_arg(text.clone()),
                    null_arg(text.clone()),
                    null_arg(DataType::Bool),
                ],
            )
            .is_ok()
        );
        assert!(
            validate_builtin_args(
                "set_config",
                &mut [
                    null_arg(DataType::Int32),
                    null_arg(text.clone()),
                    null_arg(DataType::Bool),
                ],
            )
            .is_err()
        );
        assert!(
            validate_builtin_args("pg_table_is_visible", &mut [null_arg(DataType::RegClass)])
                .is_ok()
        );
        assert!(
            validate_builtin_args("pg_table_is_visible", &mut [null_arg(text.clone())]).is_err()
        );
        assert!(validate_builtin_args("to_regtype", &mut [null_arg(text.clone())]).is_ok());
        assert!(validate_builtin_args("to_regtype", &mut [null_arg(DataType::Int32)]).is_err());
        assert!(validate_builtin_args("to_tsvector", &mut [null_arg(text.clone())]).is_ok());
        assert!(
            validate_builtin_args(
                "to_tsvector",
                &mut [null_arg(text.clone()), null_arg(text.clone())]
            )
            .is_ok()
        );
        assert!(validate_builtin_args("to_tsvector", &mut [null_arg(DataType::Int32)]).is_err());
        assert!(
            validate_builtin_args(
                "ts_rank",
                &mut [null_arg(DataType::TsVector), null_arg(DataType::TsQuery)]
            )
            .is_ok()
        );
        assert!(
            validate_builtin_args(
                "ts_rank",
                &mut [
                    null_arg(text.clone()),
                    null_arg(DataType::TsVector),
                    null_arg(DataType::TsQuery),
                ]
            )
            .is_err()
        );
        assert!(validate_builtin_args("ts_rank", &mut [null_arg(text.clone())]).is_err());
        assert!(
            validate_builtin_args(
                "ts_headline",
                &mut [null_arg(text.clone()), null_arg(DataType::TsQuery)]
            )
            .is_ok()
        );
        assert!(validate_builtin_args("ts_headline", &mut [null_arg(text.clone())]).is_err());
        assert!(validate_builtin_args("numnode", &mut [null_arg(DataType::TsQuery)]).is_ok());
        assert!(validate_builtin_args("numnode", &mut [null_arg(text.clone())]).is_err());
        assert!(validate_builtin_args("querytree", &mut [null_arg(DataType::TsQuery)]).is_ok());
        assert!(validate_builtin_args("querytree", &mut [null_arg(text.clone())]).is_err());
        assert!(
            validate_builtin_args(
                "has_column_privilege",
                &mut [
                    null_arg(text.clone()),
                    null_arg(text.clone()),
                    null_arg(text.clone()),
                    null_arg(text.clone()),
                ],
            )
            .is_ok()
        );
        assert!(
            validate_builtin_args(
                "has_column_privilege",
                &mut [
                    null_arg(text.clone()),
                    null_arg(text.clone()),
                    null_arg(text.clone()),
                ],
            )
            .is_err()
        );
        assert!(
            validate_builtin_args("jsonb_path_exists", &mut [null_arg(DataType::Jsonb)]).is_err()
        );
        assert!(
            validate_builtin_args("xml_is_well_formed", &mut [null_arg(DataType::Xml)],).is_ok()
        );
        assert!(
            validate_builtin_args("xml_is_well_formed", &mut [null_arg(DataType::Int32)]).is_err()
        );
        assert!(
            validate_builtin_args(
                "xpath",
                &mut [null_arg(text.clone()), null_arg(DataType::Xml)],
            )
            .is_ok()
        );
        assert!(
            validate_builtin_args(
                "xpath",
                &mut [
                    null_arg(text.clone()),
                    null_arg(DataType::Xml),
                    null_arg(DataType::Array(Box::new(DataType::Array(Box::new(
                        text.clone()
                    ))))),
                ],
            )
            .is_ok()
        );

        assert!(
            validate_builtin_args(
                "l2_distance",
                &mut [null_arg(vector3.clone()), null_arg(vector3.clone())],
            )
            .is_ok()
        );
        assert!(
            validate_builtin_args(
                "cosine_distance",
                &mut [null_arg(halfvec3.clone()), null_arg(halfvec3.clone())],
            )
            .is_ok()
        );
        assert!(
            validate_builtin_args(
                "l1_distance",
                &mut [null_arg(sparse5.clone()), null_arg(sparse5.clone())],
            )
            .is_ok()
        );
        assert!(
            validate_builtin_args(
                "l2_distance",
                &mut [
                    null_arg(DataType::Vector { dims: Some(2) }),
                    null_arg(vector3.clone()),
                ],
            )
            .is_err()
        );
        assert!(validate_builtin_args("vector_norm", &mut [null_arg(halfvec3.clone())]).is_ok());
        assert!(
            validate_builtin_args(
                "vector_norm",
                &mut [null_arg(DataType::BitVec { dims: Some(3) })]
            )
            .is_err()
        );
        assert!(
            validate_builtin_args(
                "vector_dims",
                &mut [null_arg(DataType::BitVec { dims: Some(3) })]
            )
            .is_ok()
        );
        assert!(validate_builtin_args("vector_dims", &mut [null_arg(DataType::Int32)]).is_err());
        assert!(
            validate_builtin_args(
                "hybrid_search",
                &mut [
                    null_arg(DataType::Jsonb),
                    null_arg(text.clone()),
                    null_arg(vector3.clone()),
                    null_arg(vector3.clone()),
                ],
            )
            .is_ok()
        );
        assert!(
            validate_builtin_args(
                "hybrid_search",
                &mut [
                    null_arg(DataType::Int32),
                    null_arg(text.clone()),
                    null_arg(vector3.clone()),
                    null_arg(vector3),
                ],
            )
            .is_err()
        );

        assert_eq!(
            vector_metric_family_kind(&DataType::Vector { dims: None }),
            Some(0)
        );
        assert_eq!(
            vector_metric_family_kind(&DataType::HalfVec { dims: None }),
            Some(1)
        );
        assert_eq!(
            vector_metric_family_kind(&DataType::SparseVec { dims: None }),
            Some(2)
        );
        assert_eq!(
            vector_metric_family_kind(&DataType::BitVec { dims: None }),
            None
        );
        assert_eq!(
            dense_vector_family_kind(&DataType::Vector { dims: None }),
            Some(0)
        );
        assert_eq!(
            dense_vector_family_kind(&DataType::HalfVec { dims: None }),
            Some(1)
        );
        assert_eq!(
            dense_vector_family_kind(&DataType::SparseVec { dims: None }),
            None
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn cast_type_and_numeric_helpers_cover_edge_paths() {
        for (name, expected) in [
            ("int", DataType::Int32),
            ("bigint", DataType::Int64),
            ("smallint", DataType::Int16),
            ("boolean", DataType::Bool),
            ("real", DataType::Float32),
            ("double precision", DataType::Float64),
            ("text", DataType::Text { max_len: None }),
            ("bytea", DataType::Bytea),
            ("date", DataType::Date),
            ("time with time zone", DataType::TimeTz),
            ("timestamp without time zone", DataType::Timestamp),
            ("timestamp with time zone", DataType::TimestampTz),
            ("uuid", DataType::Uuid),
            ("json", DataType::Json),
            ("jsonb", DataType::Jsonb),
            ("xml", DataType::Xml),
            (
                "numeric",
                DataType::Decimal {
                    precision: None,
                    scale: None,
                },
            ),
            (
                "numeric(8,2)",
                DataType::Decimal {
                    precision: Some(8),
                    scale: Some(2),
                },
            ),
            (
                "decimal(8)",
                DataType::Decimal {
                    precision: Some(8),
                    scale: Some(0),
                },
            ),
            ("money", DataType::Money),
            ("regclass", DataType::RegClass),
            ("regtype", DataType::RegType),
            ("pg_lsn", DataType::PgLsn),
            ("int4range", DataType::Range(RangeType::Int4)),
            ("point", DataType::Geometry(GeometryType::Point)),
            ("polygon", DataType::Geometry(GeometryType::Polygon)),
            ("char(3)", DataType::Char { len: Some(3) }),
            ("varchar(12)", DataType::Text { max_len: Some(12) }),
            ("bit(4)", DataType::Bit { len: Some(4) }),
            ("varbit(4)", DataType::VarBit { max_len: Some(4) }),
            ("inet", DataType::Inet),
            ("vector(3)", DataType::Vector { dims: Some(3) }),
            ("halfvec", DataType::HalfVec { dims: None }),
        ] {
            assert_eq!(resolve_cast_type(name), Some(expected), "{name}");
        }
        assert_eq!(resolve_cast_type("vector(0)"), None);
        assert_eq!(resolve_cast_type("not_a_type"), None);

        assert_eq!(pow10_i64(3), Some(1000));
        assert_eq!(scaled_decimal_text_to_i64("-12.30"), Some(-1230));
        assert_eq!(scaled_decimal_text_to_i64("bad"), None);
        assert_eq!(parse_decimal_literal("12.30"), Some((1230, 2)));
        assert_eq!(parse_decimal_literal("1e2"), None);
        assert_eq!(
            decimal_from_numeric_value(&Value::Int32(12), Some(2)),
            Some((1200, 2))
        );
        assert_eq!(
            decimal_from_numeric_value(&Value::Float64(12.25), Some(2)),
            Some((1225, 2))
        );
        assert_eq!(
            decimal_from_numeric_value(&Value::Float64(f64::NAN), Some(2)),
            None
        );
        assert_eq!(
            literal_numeric_as_f64(&Value::Decimal {
                value: 123,
                scale: 2
            }),
            Some(1.23)
        );
        assert_eq!(literal_numeric_as_f64(&Value::Text("x".to_owned())), None);
        assert_eq!(
            money_from_literal_value(&Value::Text("12.34".to_owned())),
            Some(1234)
        );
        assert_eq!(money_from_literal_value(&Value::Float64(1.0)), None);

        assert_eq!(
            parse_pg_identifier_path(r#"public."weird.name""#),
            Some(vec!["public".to_owned(), "weird.name".to_owned()])
        );
        assert_eq!(parse_pg_identifier_path(".bad"), None);

        assert!(vector_family_cast_matches(
            &DataType::Vector { dims: Some(3) },
            &DataType::Vector { dims: Some(3) }
        ));
        assert!(!vector_family_cast_matches(
            &DataType::Vector { dims: Some(3) },
            &DataType::Vector { dims: Some(2) }
        ));
        assert!(cast_result_matches(
            &DataType::Vector { dims: None },
            &DataType::Vector { dims: Some(3) }
        ));
        assert!(cast_result_matches(
            &DataType::Decimal {
                precision: None,
                scale: None
            },
            &DataType::Decimal {
                precision: None,
                scale: Some(2)
            }
        ));

        let mut left = lit(Value::Text("12".to_owned()));
        let mut right = lit(Value::Int32(12));
        coerce_literal_to_match(&mut left, &mut right);
        assert_eq!(literal_value(&left), Value::Int32(12));
        assert_eq!(literal_value(&right), Value::Int32(12));

        let result = common_scalar_return_type(
            "coalesce",
            &[null_arg(DataType::Int32), null_arg(DataType::Float64)],
        )
        .expect("numeric common type");
        assert_eq!(result, DataType::Float64);
        assert!(
            common_scalar_return_type(
                "coalesce",
                &[null_arg(DataType::Int32), null_arg(DataType::Xml)]
            )
            .is_err()
        );

        let _non_literal = coerce(
            ScalarExpr::Unary {
                op: ultrasql_parser::ast::UnaryOp::Neg,
                expr: Box::new(lit(Value::Int32(1))),
                data_type: DataType::Int32,
            },
            &DataType::Int32,
        );
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
        assert!(parse_date_literal("2000-02-30").is_none());
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
        assert_eq!(
            parse_timetz_literal("04:05:06.789 EST"),
            Some((14_706_789_000, -18_000))
        );
        assert_eq!(
            parse_timetz_literal("2000-07-01 04:05:06.789 America/New_York"),
            Some((14_706_789_000, -14_400))
        );
        assert_eq!(
            parse_timestamptz_literal("2000-01-02 03:04:05 EST"),
            Some(115_445_000_000)
        );
        assert_eq!(
            parse_timestamptz_literal("2000-07-01 00:00:00 America/New_York"),
            parse_timestamp_literal("2000-07-01 04:00:00")
        );
    }

    #[test]
    fn algorithm_handles_leap_year_february() {
        let feb29 = days_since_epoch(2000, 2, 29).expect("valid leap day");
        let mar01 = days_since_epoch(2000, 3, 1).expect("valid March day");
        assert_eq!(mar01 - feb29, 1, "2000-02-29 → 2000-03-01 is one day");
    }

    #[test]
    fn parses_interval_year_unit_into_months() {
        assert_eq!(parse_interval_literal("1", Some("year")), Some((12, 0, 0)));
        assert_eq!(parse_interval_literal("3", Some("month")), Some((3, 0, 0)));
        assert_eq!(parse_interval_literal("90", Some("day")), Some((0, 90, 0)));
    }

    #[test]
    fn decimal_coercion_honors_target_scale() {
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
        assert_eq!(value, Value::Decimal { value: 0, scale: 2 });
        assert_eq!(
            data_type,
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2)
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
        let folded =
            fold_date_interval(days_since_epoch(2000, 1, 31).expect("valid date"), 1, 0, 0)
                .unwrap();
        let super::ScalarExpr::Literal { value, data_type } = folded else {
            panic!("expected folded literal");
        };
        assert_eq!(data_type, DataType::Date);
        assert_eq!(
            value,
            Value::Date(days_since_epoch(2000, 2, 29).expect("valid leap day"))
        );
    }

    #[test]
    fn negative_i64_boundary_literal_folds_exactly() {
        assert_eq!(
            parse_negative_i64_boundary("9223372036854775808"),
            Some(i64::MIN)
        );
        assert_eq!(
            parse_negative_i64_boundary("9_223_372_036_854_775_808"),
            Some(i64::MIN)
        );
        assert_eq!(parse_negative_i64_boundary("9223372036854775809"), None);
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
