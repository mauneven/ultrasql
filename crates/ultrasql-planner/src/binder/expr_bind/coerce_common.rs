//! Collation resolution, common-type reconciliation, and CAST
//! lowering helpers shared across the expression binder.

use super::*;

pub(in crate::binder) fn bind_collated_expr(
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

pub(in crate::binder) fn resolve_builtin_collation(
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

pub(in crate::binder) fn common_scalar_return_type(
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

pub(in crate::binder) fn common_scalar_pair_type(
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

pub(in crate::binder) fn coerce_args_to_common_type(args: &mut [ScalarExpr], target: &DataType) {
    for arg in args {
        coerce_literal_to_type(arg, target);
    }
}

pub(in crate::binder) fn coerce_common_builtin_args(
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

pub(in crate::binder) fn bind_cast_expr(
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

pub(in crate::binder) fn bind_runtime_cast(
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

pub(in crate::binder) fn runtime_typmod_i32(value: Option<i32>) -> ScalarExpr {
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
