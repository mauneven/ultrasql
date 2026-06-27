//! Top-level expression dispatch: `bind_expr` and the per-variant
//! binders it delegates to.

use super::*;

#[derive(Clone, Copy, Debug)]
struct BooleanPredicate {
    value: bool,
    is_unknown: bool,
    negated: bool,
}

pub(in crate::binder) fn bind_expr(
    expr: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    bind_expr_with_ctes(expr, input, catalog, &[], scope)
}

pub(in crate::binder) fn bind_expr_with_ctes(
    expr: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Literal(lit) => bind_literal(lit),
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
            // PostgreSQL exposes `char_length` and `character_length` as
            // SQL-standard aliases of `length`. Normalize them here so the
            // single `length` arm in return-type/validation/eval handles
            // all three uniformly.
            let func_name = normalize_builtin_alias(&func_name).to_owned();
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
            // Indices into `bound_args` of the *result* branches (each THEN value
            // plus the ELSE). The output type of a CASE is the common type of
            // these branches — exactly like COALESCE — so they (and only they,
            // not the boolean WHEN conditions) drive type reconciliation.
            let mut result_indices: Vec<usize> = Vec::with_capacity(branches.len() + 1);
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
            for (when_e, then_e) in branches {
                bound_args.push(bind_expr_with_ctes(
                    when_e,
                    input,
                    catalog,
                    cte_catalog,
                    scope,
                )?);
                let then_bound = bind_expr_with_ctes(then_e, input, catalog, cte_catalog, scope)?;
                result_indices.push(bound_args.len());
                bound_args.push(then_bound);
            }
            if let Some(else_e) = else_expr {
                let bound = bind_expr_with_ctes(else_e, input, catalog, cte_catalog, scope)?;
                result_indices.push(bound_args.len());
                bound_args.push(bound);
            } else {
                result_indices.push(bound_args.len());
                bound_args.push(ScalarExpr::Literal {
                    value: Value::Null,
                    data_type: DataType::Null,
                });
            }
            // Reconcile the result branches into one output type, rejecting
            // incompatible branches (e.g. INT vs TEXT) with a TypeMismatch
            // instead of silently adopting the first branch's type.
            let result_args: Vec<ScalarExpr> = result_indices
                .iter()
                .map(|&i| bound_args[i].clone())
                .collect();
            let result_type = common_scalar_return_type("case", &result_args)?;
            for &i in &result_indices {
                coerce_literal_to_type(&mut bound_args[i], &result_type);
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
