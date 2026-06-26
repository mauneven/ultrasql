//! Static return-type inference for builtin scalar functions and the
//! `is_supported_builtin` allow-list.

use super::*;

/// Statically infer the return type of a builtin scalar function.
/// The set must stay in sync with the executor's `eval_function_call`
/// dispatcher in [`crates/ultrasql-executor/src/eval.rs`].
pub(in crate::binder) fn builtin_return_type(
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
        "abs" => abs_return_type(args),
        "mod" => mod_return_type(args),
        "ceil" | "floor" | "round" | "trunc" => round_family_return_type(func_name, args),
        "power" | "sqrt" | "exp" | "ln" | "log" | "random" | "sin" | "cos" | "tan" | "asin"
        | "acos" | "atan" | "pi" => Ok(DataType::Float64),
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

/// `abs(x)` preserves the argument's numeric type, matching both the
/// executor (which keeps each integer width) and PostgreSQL (`abs(int4)`
/// stays `integer`). Float/decimal/money inputs return themselves.
fn abs_return_type(args: &[ScalarExpr]) -> Result<DataType, PlanError> {
    if args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "abs: expected 1 argument, got {}",
            args.len()
        )));
    }
    let arg = args[0].data_type();
    match arg {
        DataType::Null
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal { .. }
        | DataType::Money => Ok(arg),
        other => Err(PlanError::TypeMismatch(format!(
            "abs: numeric argument required, got {other}"
        ))),
    }
}

/// `mod(a, b)` keeps integer inputs in the integer domain (matching the
/// executor's integer fast path, which returns the wider integer width);
/// any other numeric combination routes through `f64` and returns
/// `Float64`. Keeping this in lockstep with the executor preserves the
/// declared-type / produced-value agreement the wire encoder relies on.
fn mod_return_type(args: &[ScalarExpr]) -> Result<DataType, PlanError> {
    if args.len() != 2 {
        return Err(PlanError::TypeMismatch(format!(
            "mod: expected 2 arguments, got {}",
            args.len()
        )));
    }
    let left = args[0].data_type();
    let right = args[1].data_type();
    if left.is_integer() && right.is_integer() {
        // Two integers: `numeric_join` yields the wider integer width,
        // exactly what the executor's integer fast path returns.
        left.numeric_join(&right).map_err(|_| {
            PlanError::TypeMismatch(format!(
                "mod: arguments must share a numeric type, got {left} and {right}"
            ))
        })
    } else {
        Ok(DataType::Float64)
    }
}

/// `round`/`floor`/`ceil`/`trunc` follow PostgreSQL's result-type matrix:
/// `numeric -> numeric`, `double precision -> double precision`. PostgreSQL
/// has no integer-domain variants, so an integer argument is cast to its
/// preferred numeric type (`numeric`) and the result is `numeric`. A
/// `real`/`float4` argument promotes to `double precision`, matching the
/// executor's `f64` path. Kept in lockstep with `eval_round_family` so the
/// declared type equals the produced value type.
fn round_family_return_type(func_name: &str, args: &[ScalarExpr]) -> Result<DataType, PlanError> {
    if args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected 1 argument, got {}",
            args.len()
        )));
    }
    let arg = args[0].data_type();
    if arg.is_float() {
        // `double precision` (and `real`, which the executor widens to f64).
        Ok(DataType::Float64)
    } else if matches!(arg, DataType::Decimal { .. }) || arg.is_integer() {
        // `numeric` stays numeric; integers cast to numeric in PostgreSQL.
        Ok(DataType::Decimal {
            precision: None,
            scale: None,
        })
    } else if matches!(arg, DataType::Null) {
        Ok(DataType::Null)
    } else {
        Err(PlanError::TypeMismatch(format!(
            "{func_name}: numeric argument required, got {arg}"
        )))
    }
}

pub(in crate::binder) fn array_argument_return_type(
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

pub(in crate::binder) fn array_mutation_return_type(
    func_name: &str,
    args: &[ScalarExpr],
    array_arg_idx: usize,
) -> Result<DataType, PlanError> {
    validate_array_element_argument(func_name, args, array_arg_idx, 1 - array_arg_idx, 2)
}

pub(in crate::binder) fn array_replace_return_type(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<DataType, PlanError> {
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

pub(in crate::binder) fn validate_array_element_argument(
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
pub(in crate::binder) fn is_supported_builtin(func_name: &str) -> bool {
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
