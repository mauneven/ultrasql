//! Argument validation for builtin scalar functions.

use super::*;

pub(in crate::binder) fn validate_builtin_args(
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

pub(in crate::binder) fn validate_exact_arg_count(
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

pub(in crate::binder) fn validate_min_arg_count(
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

pub(in crate::binder) fn validate_current_schemas_args(
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_set_config_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_single_oidish_arg(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_to_regtype_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_has_privilege_args(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_jsonb_path_exists_args(
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
    if !(2..=3).contains(&args.len()) {
        return Err(PlanError::TypeMismatch(format!(
            "jsonb_path_exists: expected 2 or 3 arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

pub(in crate::binder) fn validate_xml_well_formed_args(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
    validate_exact_arg_count(func_name, args, 1)?;
    validate_text_or_xml_arg(func_name, &args[0])
}

pub(in crate::binder) fn validate_xmlparse_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
    validate_exact_arg_count("xmlparse", args, 2)?;
    validate_xml_mode_arg("xmlparse", &args[0])?;
    validate_text_or_xml_arg("xmlparse", &args[1])
}

pub(in crate::binder) fn validate_xmlserialize_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_xpath_args(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_network_inspector_args(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
    validate_exact_arg_count(func_name, args, 1)?;
    let data_type = args[0].data_type();
    if matches!(data_type, DataType::Null) || data_type.is_ip_network() {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected inet or cidr, got {data_type}"
    )))
}

pub(in crate::binder) fn validate_text_search_constructor_args(
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

pub(in crate::binder) fn validate_ts_rank_args(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
    if args.len() != 2 {
        return Err(PlanError::TypeMismatch(format!(
            "{func_name}: expected 2 arguments, got {}",
            args.len()
        )));
    }
    validate_tsvector_arg(func_name, &args[0])?;
    validate_tsquery_arg(func_name, &args[1])
}

pub(in crate::binder) fn validate_ts_headline_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_tsquery_inspector_args(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
    validate_exact_arg_count(func_name, args, 1)?;
    validate_tsquery_arg(func_name, &args[0])
}

pub(in crate::binder) fn validate_tsvector_arg(
    func_name: &str,
    arg: &ScalarExpr,
) -> Result<(), PlanError> {
    let data_type = arg.data_type();
    if matches!(data_type, DataType::Null | DataType::TsVector) {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected tsvector, got {data_type}"
    )))
}

pub(in crate::binder) fn validate_tsquery_arg(
    func_name: &str,
    arg: &ScalarExpr,
) -> Result<(), PlanError> {
    let data_type = arg.data_type();
    if matches!(data_type, DataType::Null | DataType::TsQuery) {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected tsquery, got {data_type}"
    )))
}

pub(in crate::binder) fn validate_xml_mode_arg(
    func_name: &str,
    arg: &ScalarExpr,
) -> Result<(), PlanError> {
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

pub(in crate::binder) fn literal_text_arg(arg: &ScalarExpr) -> Option<&str> {
    match arg {
        ScalarExpr::Literal {
            value: Value::Text(text) | Value::Char(text),
            ..
        } => Some(text),
        _ => None,
    }
}

pub(in crate::binder) fn validate_text_or_xml_arg(
    func_name: &str,
    arg: &ScalarExpr,
) -> Result<(), PlanError> {
    let data_type = arg.data_type();
    if matches!(data_type, DataType::Null | DataType::Xml) || data_type.is_textlike() {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected text or xml, got {data_type}"
    )))
}

pub(in crate::binder) fn validate_text_arg(
    func_name: &str,
    arg: &ScalarExpr,
) -> Result<(), PlanError> {
    let data_type = arg.data_type();
    if matches!(data_type, DataType::Null) || data_type.is_textlike() {
        return Ok(());
    }
    Err(PlanError::TypeMismatch(format!(
        "{func_name}: expected text, got {data_type}"
    )))
}

pub(in crate::binder) fn validate_vector_metric_args(
    func_name: &str,
    args: &mut [ScalarExpr],
) -> Result<(), PlanError> {
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

pub(in crate::binder) fn coerce_vector_metric_literals(args: &mut [ScalarExpr]) {
    let left_type = args[0].data_type();
    let right_type = args[1].data_type();
    if vector_metric_family_kind(&left_type).is_some() {
        coerce_literal_to_type(&mut args[1], &left_type);
    }
    if vector_metric_family_kind(&right_type).is_some() {
        coerce_literal_to_type(&mut args[0], &right_type);
    }
}

pub(in crate::binder) fn validate_vector_norm_args(
    func_name: &str,
    args: &[ScalarExpr],
) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_vector_dims_args(args: &[ScalarExpr]) -> Result<(), PlanError> {
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

pub(in crate::binder) fn validate_hybrid_search_args(
    args: &mut [ScalarExpr],
) -> Result<(), PlanError> {
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

pub(in crate::binder) fn coerce_hybrid_vector_literals(args: &mut [ScalarExpr]) {
    let vector_type = args[2].data_type();
    let probe_type = args[3].data_type();
    if dense_vector_family_kind(&vector_type).is_some() {
        coerce_literal_to_type(&mut args[3], &vector_type);
    }
    if dense_vector_family_kind(&probe_type).is_some() {
        coerce_literal_to_type(&mut args[2], &probe_type);
    }
}

pub(in crate::binder) fn vector_metric_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => None,
        _ => None,
    }
}

pub(in crate::binder) fn dense_vector_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        _ => None,
    }
}
