//! Column reference binding, array subscript/slice, AT TIME ZONE,
//! and the unary/binary operator binders.

use super::*;

pub(in crate::binder) fn bind_column(
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

pub(in crate::binder) fn qualified_column_name(
    name: &ultrasql_parser::ast::ObjectName,
) -> Option<String> {
    let col = name.parts.last()?;
    let qualifier = name.parts.iter().rev().nth(1)?;
    Some(format!("{}.{}", qualifier.value, col.value))
}

pub(in crate::binder) fn bind_array_subscript(
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

pub(in crate::binder) fn bind_array_slice(
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

pub(in crate::binder) fn bind_at_time_zone(
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

pub(in crate::binder) fn timezone_return_type(args: &[ScalarExpr]) -> Result<DataType, PlanError> {
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

pub(in crate::binder) fn bind_optional_array_bound(
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

pub(in crate::binder) fn bind_unary(
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
pub(in crate::binder) fn bind_binary(
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

pub(in crate::binder) const fn binary_operator_uses_raw_text_pattern(op: BinaryOp) -> bool {
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

pub(in crate::binder) fn coerce_binary_literals(
    op: BinaryOp,
    left: &mut ScalarExpr,
    right: &mut ScalarExpr,
) {
    if binary_operator_uses_raw_text_pattern(op)
        || money_scalar_arithmetic_keeps_operand_types(op, left, right)
    {
        return;
    }
    coerce_literal_to_match(left, right);
}

pub(in crate::binder) fn money_scalar_arithmetic_keeps_operand_types(
    op: BinaryOp,
    left: &ScalarExpr,
    right: &ScalarExpr,
) -> bool {
    matches!(op, BinaryOp::Mul | BinaryOp::Div)
        && (matches!(left.data_type(), DataType::Money)
            || matches!(right.data_type(), DataType::Money))
}
