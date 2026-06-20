//! `pg_catalog` introspection and system builtins.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_pg_typeof(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_typeof: expected 1 arg, got {}",
            args.len()
        )));
    }
    Ok(Value::Text(args[0].data_type().to_string()))
}

pub(crate) fn eval_current_schemas(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "current_schemas: expected 1 arg, got {}",
            args.len()
        )));
    }
    let include_implicit = match args[0] {
        Value::Bool(value) => value,
        Value::Null => false,
        ref other => {
            return Err(EvalError::Type(format!(
                "current_schemas: boolean argument required, got {:?}",
                other.data_type()
            )));
        }
    };
    let mut elements = Vec::new();
    if include_implicit {
        elements.push(Value::Text("pg_catalog".to_owned()));
    }
    elements.push(Value::Text("public".to_owned()));
    Ok(Value::Array {
        element_type: DataType::Text { max_len: None },
        elements,
    })
}

pub(crate) fn eval_to_regtype(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "to_regtype: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::RegType(oid) => Ok(Value::RegType(*oid)),
        Value::Text(text) | Value::Char(text) => Ok(resolve_regtype_text(text)
            .map(Value::RegType)
            .unwrap_or(Value::Null)),
        other => Err(EvalError::Type(format!(
            "to_regtype: text argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_pg_table_is_visible(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_table_is_visible: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(_)
        | Value::RegClass(_)
        | Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_) => Ok(Value::Bool(true)),
        other => Err(EvalError::Type(format!(
            "pg_table_is_visible: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_pg_is_other_temp_schema(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_is_other_temp_schema: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(_)
        | Value::RegClass(_)
        | Value::RegType(_)
        | Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_) => Ok(Value::Bool(false)),
        other => Err(EvalError::Type(format!(
            "pg_is_other_temp_schema: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_pg_function_is_visible(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_function_is_visible: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(_)
        | Value::RegClass(_)
        | Value::RegType(_)
        | Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_) => Ok(Value::Bool(true)),
        other => Err(EvalError::Type(format!(
            "pg_function_is_visible: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_pg_relation_is_publishable(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_relation_is_publishable: expected 1 arg, got {}",
            args.len()
        )));
    }
    Ok(Value::Bool(!matches!(args[0], Value::Null)))
}

pub(crate) fn eval_set_config(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "set_config: expected 3 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    if !matches!(args[0], Value::Text(_) | Value::Char(_)) {
        return Err(EvalError::Type(format!(
            "set_config: setting name must be text, got {:?}",
            args[0].data_type()
        )));
    }
    let value = match &args[1] {
        Value::Text(text) | Value::Char(text) => text.clone(),
        other => {
            return Err(EvalError::Type(format!(
                "set_config: setting value must be text, got {:?}",
                other.data_type()
            )));
        }
    };
    if !matches!(args[2], Value::Bool(_) | Value::Null) {
        return Err(EvalError::Type(format!(
            "set_config: local flag must be boolean, got {:?}",
            args[2].data_type()
        )));
    }
    Ok(Value::Text(value))
}

pub(crate) fn eval_format_type(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "format_type: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let Some(oid) = oid_or_integer_arg(&args[0]) else {
        return Err(EvalError::Type(format!(
            "format_type: oid argument required, got {:?}",
            args[0].data_type()
        )));
    };
    let typmod = format_type_typmod(&args[1])?;
    let name = format_builtin_type(oid, typmod)
        .unwrap_or_else(|| builtin_type_display_name(oid).unwrap_or("text").to_owned());
    Ok(Value::Text(name))
}

pub(crate) fn format_type_typmod(value: &Value) -> Result<Option<i32>, EvalError> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let Some(raw) = integer_value_i128(value) else {
        return Err(EvalError::Type(format!(
            "format_type: typmod argument must be integer or null, got {:?}",
            value.data_type()
        )));
    };
    i32::try_from(raw)
        .map(Some)
        .map_err(|_| EvalError::Type("format_type: typmod argument out of range".to_owned()))
}

pub(crate) fn format_builtin_type(oid: u32, typmod: Option<i32>) -> Option<String> {
    match (oid, typmod) {
        (1700, Some(typmod)) => format_numeric_type(typmod),
        (1042, Some(typmod)) => format_char_type(typmod),
        _ => None,
    }
}

pub(crate) fn format_numeric_type(typmod: i32) -> Option<String> {
    let packed = u32::try_from(typmod.checked_sub(4)?).ok()?;
    let precision = packed >> 16;
    let scale = packed & u32::from(u16::MAX);
    Some(format!("numeric({precision},{scale})"))
}

pub(crate) fn format_char_type(typmod: i32) -> Option<String> {
    let len = typmod.checked_sub(4)?;
    if len < 0 {
        return None;
    }
    Some(format!("character({len})"))
}

pub(crate) fn builtin_type_display_name(oid: u32) -> Option<&'static str> {
    match oid {
        16 => Some("boolean"),
        17 => Some("bytea"),
        20 => Some("bigint"),
        21 => Some("smallint"),
        23 => Some("integer"),
        25 => Some("text"),
        26 => Some("oid"),
        700 => Some("real"),
        701 => Some("double precision"),
        790 => Some("money"),
        114 => Some("json"),
        142 => Some("xml"),
        143 => Some("xml[]"),
        650 => Some("cidr"),
        829 => Some("macaddr"),
        869 => Some("inet"),
        1042 => Some("character"),
        1082 => Some("date"),
        1083 => Some("time without time zone"),
        1114 => Some("timestamp without time zone"),
        1184 => Some("timestamp with time zone"),
        1266 => Some("time with time zone"),
        1560 => Some("bit"),
        1562 => Some("bit varying"),
        1700 => Some("numeric"),
        2950 => Some("uuid"),
        3220 => Some("pg_lsn"),
        3614 => Some("tsvector"),
        3615 => Some("tsquery"),
        3802 => Some("jsonb"),
        2205 => Some("regclass"),
        2206 => Some("regtype"),
        _ => None,
    }
}

pub(crate) fn eval_pg_get_expr(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 2 || args.len() == 3) {
        return Err(EvalError::Type(format!(
            "pg_get_expr: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(text) | Value::Char(text) => Ok(Value::Text(text.clone())),
        other => Err(EvalError::Type(format!(
            "pg_get_expr: expression text required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_pg_get_indexdef(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 1 || args.len() == 2 || args.len() == 3) {
        return Err(EvalError::Type(format!(
            "pg_get_indexdef: expected 1 to 3 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let Some(oid) = oid_or_integer_arg(&args[0]) else {
        return Err(EvalError::Type(format!(
            "pg_get_indexdef: oid argument required, got {:?}",
            args[0].data_type()
        )));
    };
    Ok(Value::Text(format!("index {oid}")))
}

pub(crate) fn eval_pg_get_constraintdef(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 1 || args.len() == 2) {
        return Err(EvalError::Type(format!(
            "pg_get_constraintdef: expected 1 or 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let Some(oid) = oid_or_integer_arg(&args[0]) else {
        return Err(EvalError::Type(format!(
            "pg_get_constraintdef: oid argument required, got {:?}",
            args[0].data_type()
        )));
    };
    if oid == 0 {
        return Ok(Value::Null);
    }
    Ok(Value::Text(format!("constraint {oid}")))
}

pub(crate) fn eval_pg_get_statisticsobjdef_columns(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_get_statisticsobjdef_columns: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() {
        return Err(EvalError::Type(format!(
            "pg_get_statisticsobjdef_columns: oid argument required, got {:?}",
            args[0].data_type()
        )));
    }
    Ok(Value::Text(String::new()))
}

pub(crate) fn eval_pg_get_function_result(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_get_function_result: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() {
        return Err(EvalError::Type(format!(
            "pg_get_function_result: oid argument required, got {:?}",
            args[0].data_type()
        )));
    }
    Ok(Value::Text(String::new()))
}

pub(crate) fn eval_pg_get_function_arguments(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_get_function_arguments: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() {
        return Err(EvalError::Type(format!(
            "pg_get_function_arguments: oid argument required, got {:?}",
            args[0].data_type()
        )));
    }
    Ok(Value::Text(String::new()))
}

pub(crate) fn eval_pg_encoding_to_char(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_encoding_to_char: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(encoding) = args[0].as_i64() else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "pg_encoding_to_char: integer argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let name = if encoding == 6 { "UTF8" } else { "" };
    Ok(Value::Text(name.to_owned()))
}

pub(crate) fn eval_obj_description(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "obj_description: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() {
        return Err(EvalError::Type(format!(
            "obj_description: oid argument required, got {:?}",
            args[0].data_type()
        )));
    }
    match &args[1] {
        Value::Text(_) | Value::Char(_) => Ok(Value::Null),
        other => Err(EvalError::Type(format!(
            "obj_description: catalog name must be text, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_col_description(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "col_description: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() || integer_value_i128(&args[1]).is_none() {
        return Err(EvalError::Type(format!(
            "col_description: oid and integer arguments required, got {:?}, {:?}",
            args[0].data_type(),
            args[1].data_type()
        )));
    }
    Ok(Value::Null)
}

pub(crate) fn eval_pg_get_serial_sequence(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "pg_get_serial_sequence: expected 2 args, got {}",
            args.len()
        )));
    }
    for arg in args {
        if matches!(arg, Value::Null) {
            return Ok(Value::Null);
        }
        if !matches!(arg, Value::Text(_) | Value::Char(_)) {
            return Err(EvalError::Type(format!(
                "pg_get_serial_sequence: text arguments required, got {:?}",
                arg.data_type()
            )));
        }
    }
    Ok(Value::Null)
}
