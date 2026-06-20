//! Sidecar-metadata token codecs for data types, operators, values, and
//! bound scalar expressions.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) fn data_type_token(ty: &DataType) -> Option<String> {
    match ty {
        DataType::Bool => Some("bool".to_owned()),
        DataType::Int16 => Some("i16".to_owned()),
        DataType::Int32 => Some("i32".to_owned()),
        DataType::Int64 => Some("i64".to_owned()),
        DataType::Money => Some("money".to_owned()),
        DataType::Float32 => Some("f32".to_owned()),
        DataType::Float64 => Some("f64".to_owned()),
        DataType::Text {
            max_len: Some(max_len),
        } => Some(format!("varchar:{max_len}")),
        DataType::Text { max_len: None } => Some("text".to_owned()),
        DataType::TsVector => Some("tsvector".to_owned()),
        DataType::TsQuery => Some("tsquery".to_owned()),
        DataType::Char { len: Some(len) } => Some(format!("char:{len}")),
        DataType::Char { len: None } => Some("char".to_owned()),
        DataType::Bit { len: Some(len) } => Some(format!("bit:{len}")),
        DataType::Bit { len: None } => Some("bit".to_owned()),
        DataType::VarBit {
            max_len: Some(max_len),
        } => Some(format!("varbit:{max_len}")),
        DataType::VarBit { max_len: None } => Some("varbit".to_owned()),
        DataType::Inet => Some("inet".to_owned()),
        DataType::Cidr => Some("cidr".to_owned()),
        DataType::MacAddr => Some("macaddr".to_owned()),
        DataType::MacAddr8 => Some("macaddr8".to_owned()),
        DataType::Date => Some("date".to_owned()),
        DataType::Time => Some("time".to_owned()),
        DataType::TimeTz => Some("timetz".to_owned()),
        DataType::Timestamp => Some("ts".to_owned()),
        DataType::TimestampTz => Some("tstz".to_owned()),
        DataType::Null => Some("null".to_owned()),
        _ => None,
    }
}

pub(crate) fn data_type_from_token(token: &str) -> Option<DataType> {
    if let Some(len_text) = token.strip_prefix("char:") {
        return len_text
            .parse::<u32>()
            .ok()
            .map(|len| DataType::Char { len: Some(len) });
    }
    if let Some(max_len_text) = token.strip_prefix("varchar:") {
        return max_len_text
            .parse::<u32>()
            .ok()
            .map(|max_len| DataType::Text {
                max_len: Some(max_len),
            });
    }
    if let Some(len_text) = token.strip_prefix("bit:") {
        return len_text
            .parse::<u32>()
            .ok()
            .map(|len| DataType::Bit { len: Some(len) });
    }
    if let Some(max_len_text) = token.strip_prefix("varbit:") {
        return max_len_text
            .parse::<u32>()
            .ok()
            .map(|max_len| DataType::VarBit {
                max_len: Some(max_len),
            });
    }
    match token {
        "bool" => Some(DataType::Bool),
        "i16" => Some(DataType::Int16),
        "i32" => Some(DataType::Int32),
        "i64" => Some(DataType::Int64),
        "money" => Some(DataType::Money),
        "f32" => Some(DataType::Float32),
        "f64" => Some(DataType::Float64),
        "text" => Some(DataType::Text { max_len: None }),
        "tsvector" => Some(DataType::TsVector),
        "tsquery" => Some(DataType::TsQuery),
        "char" => Some(DataType::Char { len: None }),
        "bit" => Some(DataType::Bit { len: None }),
        "varbit" => Some(DataType::VarBit { max_len: None }),
        "inet" => Some(DataType::Inet),
        "cidr" => Some(DataType::Cidr),
        "macaddr" => Some(DataType::MacAddr),
        "macaddr8" => Some(DataType::MacAddr8),
        "date" => Some(DataType::Date),
        "time" => Some(DataType::Time),
        "timetz" => Some(DataType::TimeTz),
        "ts" => Some(DataType::Timestamp),
        "tstz" => Some(DataType::TimestampTz),
        "null" => Some(DataType::Null),
        _ => None,
    }
}

pub(crate) fn operator_data_type_token(
    ty: &Option<DataType>,
    operator_name: &str,
) -> Result<String, ServerError> {
    let Some(ty) = ty else {
        return Ok(String::new());
    };
    data_type_token(ty).ok_or_else(|| {
        ServerError::ddl(format!(
            "operator '{operator_name}' argument type is outside restart-persistable metadata subset"
        ))
    })
}

pub(crate) fn parse_operator_data_type_token(
    token: &str,
    line_no: usize,
    field: &str,
) -> Result<Option<DataType>, ServerError> {
    if token.is_empty() {
        return Ok(None);
    }
    data_type_from_token(token).map(Some).ok_or_else(|| {
        ServerError::ddl(format!(
            "operator metadata line {} has unknown {field} type '{}'",
            line_no + 1,
            token
        ))
    })
}

pub(crate) fn validate_runtime_operator_metadata(
    operator: &RuntimeOperator,
    line_no: usize,
) -> Result<(), ServerError> {
    if operator.procedure == "bool_eq"
        && operator.left_type == Some(DataType::Bool)
        && operator.right_type == Some(DataType::Bool)
        && operator.result_type == DataType::Bool
    {
        return Ok(());
    }
    Err(ServerError::ddl(format!(
        "operator metadata line {} uses unsupported procedure/type signature",
        line_no + 1
    )))
}

pub(crate) fn binary_op_token(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::Mod => "mod",
        BinaryOp::Pow => "pow",
        BinaryOp::Concat => "concat",
        BinaryOp::Eq => "eq",
        BinaryOp::NotEq => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::LtEq => "le",
        BinaryOp::Gt => "gt",
        BinaryOp::GtEq => "ge",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Like => "like",
        BinaryOp::NotLike => "not_like",
        BinaryOp::Ilike => "ilike",
        BinaryOp::NotIlike => "not_ilike",
        BinaryOp::RegexMatch => "regex",
        BinaryOp::RegexIMatch => "iregex",
        BinaryOp::RegexNotMatch => "not_regex",
        BinaryOp::RegexNotIMatch => "not_iregex",
        BinaryOp::BitAnd => "bit_and",
        BinaryOp::BitOr => "bit_or",
        BinaryOp::BitXor => "bit_xor",
        BinaryOp::ShiftLeft => "shl",
        BinaryOp::ShiftRight => "shr",
        BinaryOp::NetworkContainedEq => "net_contained_eq",
        BinaryOp::NetworkContainsEq => "net_contains_eq",
        BinaryOp::JsonGet => "json_get",
        BinaryOp::JsonGetText => "json_get_text",
        BinaryOp::JsonGetPath => "json_get_path",
        BinaryOp::JsonGetPathText => "json_get_path_text",
        BinaryOp::JsonContains => "json_contains",
        BinaryOp::JsonContained => "json_contained",
        BinaryOp::JsonHasKey => "json_has_key",
        BinaryOp::JsonHasAnyKey => "json_has_any_key",
        BinaryOp::JsonHasAllKeys => "json_has_all_keys",
        BinaryOp::TextSearchMatch => "text_search",
        BinaryOp::Overlap => "overlap",
        BinaryOp::VectorL2Distance => "vec_l2",
        BinaryOp::VectorNegativeInnerProduct => "vec_ip",
        BinaryOp::VectorCosineDistance => "vec_cos",
        BinaryOp::VectorL1Distance => "vec_l1",
    }
}

pub(crate) fn binary_op_from_token(token: &str) -> Option<BinaryOp> {
    Some(match token {
        "add" => BinaryOp::Add,
        "sub" => BinaryOp::Sub,
        "mul" => BinaryOp::Mul,
        "div" => BinaryOp::Div,
        "mod" => BinaryOp::Mod,
        "pow" => BinaryOp::Pow,
        "concat" => BinaryOp::Concat,
        "eq" => BinaryOp::Eq,
        "ne" => BinaryOp::NotEq,
        "lt" => BinaryOp::Lt,
        "le" => BinaryOp::LtEq,
        "gt" => BinaryOp::Gt,
        "ge" => BinaryOp::GtEq,
        "and" => BinaryOp::And,
        "or" => BinaryOp::Or,
        "like" => BinaryOp::Like,
        "not_like" => BinaryOp::NotLike,
        "ilike" => BinaryOp::Ilike,
        "not_ilike" => BinaryOp::NotIlike,
        "regex" => BinaryOp::RegexMatch,
        "iregex" => BinaryOp::RegexIMatch,
        "not_regex" => BinaryOp::RegexNotMatch,
        "not_iregex" => BinaryOp::RegexNotIMatch,
        "bit_and" => BinaryOp::BitAnd,
        "bit_or" => BinaryOp::BitOr,
        "bit_xor" => BinaryOp::BitXor,
        "shl" => BinaryOp::ShiftLeft,
        "shr" => BinaryOp::ShiftRight,
        "net_contained_eq" => BinaryOp::NetworkContainedEq,
        "net_contains_eq" => BinaryOp::NetworkContainsEq,
        "json_get" => BinaryOp::JsonGet,
        "json_get_text" => BinaryOp::JsonGetText,
        "json_get_path" => BinaryOp::JsonGetPath,
        "json_get_path_text" => BinaryOp::JsonGetPathText,
        "json_contains" => BinaryOp::JsonContains,
        "json_contained" => BinaryOp::JsonContained,
        "json_has_key" => BinaryOp::JsonHasKey,
        "json_has_any_key" => BinaryOp::JsonHasAnyKey,
        "json_has_all_keys" => BinaryOp::JsonHasAllKeys,
        "text_search" => BinaryOp::TextSearchMatch,
        "overlap" => BinaryOp::Overlap,
        "vec_l2" => BinaryOp::VectorL2Distance,
        "vec_ip" => BinaryOp::VectorNegativeInnerProduct,
        "vec_cos" => BinaryOp::VectorCosineDistance,
        "vec_l1" => BinaryOp::VectorL1Distance,
        _ => return None,
    })
}

pub(crate) fn unary_op_token(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "neg",
        UnaryOp::Pos => "pos",
        UnaryOp::Not => "not",
        UnaryOp::BitNot => "bit_not",
    }
}

pub(crate) fn unary_op_from_token(token: &str) -> Option<UnaryOp> {
    Some(match token {
        "neg" => UnaryOp::Neg,
        "pos" => UnaryOp::Pos,
        "not" => UnaryOp::Not,
        "bit_not" => UnaryOp::BitNot,
        _ => return None,
    })
}

pub(crate) fn value_token(value: &Value) -> Option<String> {
    Some(match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => v.to_string(),
        Value::Float32(v) => v.to_bits().to_string(),
        Value::Float64(v) => v.to_bits().to_string(),
        Value::Text(v) | Value::Char(v) | Value::Json(v) | Value::Jsonb(v) | Value::Xml(v) => {
            metadata_escape(v)
        }
        Value::BitString(v) => metadata_escape(&v.to_string()),
        Value::Network(v) => metadata_escape(&v.to_string()),
        Value::Date(v) => v.to_string(),
        Value::Time(v) | Value::Timestamp(v) | Value::TimestampTz(v) => v.to_string(),
        Value::TimeTz {
            micros,
            offset_seconds,
        } => format!("{micros}:{offset_seconds}"),
        _ => return None,
    })
}

pub(crate) fn value_from_token(ty: &DataType, token: &str) -> Result<Value, ServerError> {
    Ok(match ty {
        DataType::Null => Value::Null,
        DataType::Bool => Value::Bool(
            token
                .parse::<bool>()
                .map_err(|err| ServerError::Ddl(format!("bad bool literal: {err}")))?,
        ),
        DataType::Int16 => Value::Int16(
            token
                .parse::<i16>()
                .map_err(|err| ServerError::Ddl(format!("bad int16 literal: {err}")))?,
        ),
        DataType::Int32 => Value::Int32(
            token
                .parse::<i32>()
                .map_err(|err| ServerError::Ddl(format!("bad int32 literal: {err}")))?,
        ),
        DataType::Int64 => Value::Int64(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad int64 literal: {err}")))?,
        ),
        DataType::Money => Value::Money(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad money literal: {err}")))?,
        ),
        DataType::Float32 => {
            Value::Float32(f32::from_bits(token.parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!("bad float32 literal: {err}"))
            })?))
        }
        DataType::Float64 => {
            Value::Float64(f64::from_bits(token.parse::<u64>().map_err(|err| {
                ServerError::Ddl(format!("bad float64 literal: {err}"))
            })?))
        }
        DataType::Text { .. } => Value::Text(metadata_unescape(token)?),
        DataType::Char { .. } => Value::Char(metadata_unescape(token)?),
        DataType::Bit { .. } | DataType::VarBit { .. } => {
            let text = metadata_unescape(token)?;
            Value::parse_bit_string(&text)
                .ok_or_else(|| ServerError::Ddl("bad bit string literal".to_owned()))?
        }
        DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8 => {
            let text = metadata_unescape(token)?;
            Value::parse_network(ty, &text)
                .ok_or_else(|| ServerError::Ddl("bad network literal".to_owned()))?
        }
        DataType::Date => Value::Date(
            token
                .parse::<i32>()
                .map_err(|err| ServerError::Ddl(format!("bad date literal: {err}")))?,
        ),
        DataType::Time => Value::Time(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad time literal: {err}")))?,
        ),
        DataType::TimeTz => {
            let (micros, offset_seconds) = token
                .split_once(':')
                .ok_or_else(|| ServerError::Ddl("bad timetz literal".to_owned()))?;
            Value::TimeTz {
                micros: micros
                    .parse::<i64>()
                    .map_err(|err| ServerError::Ddl(format!("bad timetz time literal: {err}")))?,
                offset_seconds: offset_seconds
                    .parse::<i32>()
                    .map_err(|err| ServerError::Ddl(format!("bad timetz offset literal: {err}")))?,
            }
        }
        DataType::Timestamp => Value::Timestamp(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad timestamp literal: {err}")))?,
        ),
        DataType::TimestampTz => Value::TimestampTz(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad timestamptz literal: {err}")))?,
        ),
        _ => {
            return Err(ServerError::Ddl(format!(
                "unsupported persisted literal type {ty:?}"
            )));
        }
    })
}

pub(crate) fn encode_scalar_expr(expr: &ScalarExpr, out: &mut Vec<String>) -> Option<()> {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => {
            out.push("col".to_owned());
            out.push(index.to_string());
            out.push(metadata_escape(name));
            out.push(data_type_token(data_type)?);
        }
        ScalarExpr::Literal { value, data_type } => {
            out.push("lit".to_owned());
            out.push(data_type_token(data_type)?);
            out.push(value_token(value)?);
        }
        ScalarExpr::Unary {
            op,
            expr,
            data_type,
        } => {
            out.push("unary".to_owned());
            out.push(unary_op_token(*op).to_owned());
            out.push(data_type_token(data_type)?);
            encode_scalar_expr(expr, out)?;
        }
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => {
            out.push("binary".to_owned());
            out.push(binary_op_token(*op).to_owned());
            out.push(data_type_token(data_type)?);
            encode_scalar_expr(left, out)?;
            encode_scalar_expr(right, out)?;
        }
        ScalarExpr::IsNull { expr, negated } => {
            out.push("isnull".to_owned());
            out.push(negated.to_string());
            encode_scalar_expr(expr, out)?;
        }
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => {
            out.push("func".to_owned());
            out.push(metadata_escape(name));
            out.push(data_type_token(data_type)?);
            out.push(args.len().to_string());
            for arg in args {
                encode_scalar_expr(arg, out)?;
            }
        }
        _ => return None,
    }
    Some(())
}

pub(crate) fn encode_scalar_expr_field(expr: &ScalarExpr) -> Option<String> {
    let mut tokens = Vec::new();
    encode_scalar_expr(expr, &mut tokens)?;
    Some(tokens.join("\u{1f}"))
}

pub(crate) fn encode_table_runtime_scalar_expr(
    table_name: &str,
    subject: String,
    expr: &ScalarExpr,
) -> Result<String, ServerError> {
    encode_scalar_expr_field(expr).ok_or_else(|| {
        ServerError::ddl(format!(
            "table '{table_name}' {subject} is outside restart-persistable metadata subset"
        ))
    })
}

pub(crate) fn encode_table_runtime_scalar_expr_list(
    table_name: &str,
    subject: String,
    exprs: &[ScalarExpr],
) -> Result<String, ServerError> {
    let encoded = exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            encode_table_runtime_scalar_expr(
                table_name,
                format!("{subject} expression {idx}"),
                expr,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(metadata_encode_list(&encoded))
}

pub(crate) fn decode_scalar_expr(
    tokens: &[&str],
    pos: &mut usize,
) -> Result<ScalarExpr, ServerError> {
    let Some(kind) = tokens.get(*pos).copied() else {
        return Err(ServerError::Ddl(
            "truncated scalar expression metadata".to_owned(),
        ));
    };
    *pos += 1;
    match kind {
        "col" => {
            let index = tokens
                .get(*pos)
                .ok_or_else(|| ServerError::Ddl("truncated column expr".to_owned()))?
                .parse::<usize>()
                .map_err(|err| ServerError::Ddl(format!("bad column index: {err}")))?;
            *pos += 1;
            let name = metadata_unescape(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated column name".to_owned()))?,
            )?;
            *pos += 1;
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated column type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown column type".to_owned()))?;
            *pos += 1;
            Ok(ScalarExpr::Column {
                name,
                index,
                data_type,
            })
        }
        "lit" => {
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated literal type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown literal type".to_owned()))?;
            *pos += 1;
            let value = value_from_token(
                &data_type,
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated literal value".to_owned()))?,
            )?;
            *pos += 1;
            Ok(ScalarExpr::Literal { value, data_type })
        }
        "unary" => {
            let op = unary_op_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated unary op".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown unary op".to_owned()))?;
            *pos += 1;
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated unary type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown unary type".to_owned()))?;
            *pos += 1;
            let expr = Box::new(decode_scalar_expr(tokens, pos)?);
            Ok(ScalarExpr::Unary {
                op,
                expr,
                data_type,
            })
        }
        "binary" => {
            let op = binary_op_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated binary op".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown binary op".to_owned()))?;
            *pos += 1;
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated binary type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown binary type".to_owned()))?;
            *pos += 1;
            let left = Box::new(decode_scalar_expr(tokens, pos)?);
            let right = Box::new(decode_scalar_expr(tokens, pos)?);
            Ok(ScalarExpr::Binary {
                op,
                left,
                right,
                data_type,
            })
        }
        "isnull" => {
            let negated = tokens
                .get(*pos)
                .ok_or_else(|| ServerError::Ddl("truncated isnull flag".to_owned()))?
                .parse::<bool>()
                .map_err(|err| ServerError::Ddl(format!("bad isnull flag: {err}")))?;
            *pos += 1;
            let expr = Box::new(decode_scalar_expr(tokens, pos)?);
            Ok(ScalarExpr::IsNull { expr, negated })
        }
        "func" => {
            let name = metadata_unescape(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated function name".to_owned()))?,
            )?;
            *pos += 1;
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated function type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown function type".to_owned()))?;
            *pos += 1;
            let arg_count = tokens
                .get(*pos)
                .ok_or_else(|| ServerError::Ddl("truncated function arg count".to_owned()))?
                .parse::<usize>()
                .map_err(|err| ServerError::Ddl(format!("bad function arg count: {err}")))?;
            *pos += 1;
            let mut args = Vec::with_capacity(arg_count);
            for _ in 0..arg_count {
                args.push(decode_scalar_expr(tokens, pos)?);
            }
            Ok(ScalarExpr::FunctionCall {
                name,
                args,
                data_type,
            })
        }
        other => Err(ServerError::Ddl(format!(
            "unknown scalar expression token {other}"
        ))),
    }
}

pub(crate) fn decode_scalar_expr_field(raw: &str) -> Result<ScalarExpr, ServerError> {
    let tokens = raw.split('\u{1f}').collect::<Vec<_>>();
    let mut pos = 0;
    let expr = decode_scalar_expr(&tokens, &mut pos)?;
    if pos != tokens.len() {
        return Err(ServerError::Ddl(
            "trailing scalar expression metadata tokens".to_owned(),
        ));
    }
    Ok(expr)
}

pub(crate) fn decode_scalar_expr_list_field(raw: &str) -> Result<Vec<ScalarExpr>, ServerError> {
    metadata_decode_list(raw)?
        .into_iter()
        .map(|expr| decode_scalar_expr_field(&expr))
        .collect()
}
