//! Explicit `CAST` builtins and numeric typmod coercion.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_cast_int16(args: &[Value]) -> Result<Value, EvalError> {
    let Some(raw) = integer_cast_arg("smallint cast", args)? else {
        return Ok(Value::Null);
    };
    i16::try_from(raw).map(Value::Int16).map_err(|_| {
        EvalError::NumericFieldOverflow(format!("smallint cast: value out of range: {raw}"))
    })
}

pub(crate) fn eval_cast_int32(args: &[Value]) -> Result<Value, EvalError> {
    let Some(raw) = integer_cast_arg("integer cast", args)? else {
        return Ok(Value::Null);
    };
    i32::try_from(raw).map(Value::Int32).map_err(|_| {
        EvalError::NumericFieldOverflow(format!("integer cast: value out of range: {raw}"))
    })
}

pub(crate) fn eval_cast_int64(args: &[Value]) -> Result<Value, EvalError> {
    let Some(raw) = integer_cast_arg("bigint cast", args)? else {
        return Ok(Value::Null);
    };
    i64::try_from(raw).map(Value::Int64).map_err(|_| {
        EvalError::NumericFieldOverflow(format!("bigint cast: value out of range: {raw}"))
    })
}

pub(crate) fn integer_cast_arg(func: &str, args: &[Value]) -> Result<Option<i128>, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "{func}: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(None),
        Value::Int16(v) => Ok(Some(i128::from(*v))),
        Value::Int32(v) => Ok(Some(i128::from(*v))),
        Value::Int64(v) => Ok(Some(i128::from(*v))),
        Value::Text(text) | Value::Char(text) => {
            text.trim().parse::<i128>().map(Some).map_err(|_| {
                EvalError::InvalidTextRepresentation(format!(
                    "{func}: invalid integer syntax: {text}"
                ))
            })
        }
        other => Err(EvalError::Type(format!(
            "{func}: integer argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_cast_float32(args: &[Value]) -> Result<Value, EvalError> {
    let Some(raw) = numeric_cast_arg_f64("real cast", args)? else {
        return Ok(Value::Null);
    };
    let value = raw
        .to_string()
        .parse::<f32>()
        .map_err(|_| EvalError::Overflow)?;
    if raw.is_finite() && !value.is_finite() {
        // numeric_value_out_of_range — a finite f64 (e.g. 1e40) does not
        // fit in f32 and overflows to infinity. Mirror PostgreSQL's 22003.
        return Err(EvalError::Overflow);
    }
    Ok(Value::Float32(value))
}

pub(crate) fn eval_cast_float64(args: &[Value]) -> Result<Value, EvalError> {
    let Some(raw) = numeric_cast_arg_f64("double precision cast", args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Float64(raw))
}

pub(crate) fn numeric_cast_arg_f64(func: &str, args: &[Value]) -> Result<Option<f64>, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "{func}: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(None),
        Value::Int16(v) => Ok(Some(f64::from(*v))),
        Value::Int32(v) => Ok(Some(f64::from(*v))),
        Value::Int64(v) => v
            .to_string()
            .parse::<f64>()
            .map(Some)
            // numeric_value_out_of_range — value does not fit the target
            // floating-point type. Mirror PostgreSQL's 22003.
            .map_err(|_| EvalError::Overflow),
        Value::Float32(v) => Ok(Some(f64::from(*v))),
        Value::Float64(v) => Ok(Some(*v)),
        Value::Decimal { value, scale } => decimal_value_to_f64(*value, *scale)
            .map(Some)
            .ok_or(EvalError::Overflow),
        Value::Text(text) | Value::Char(text) => {
            text.trim().parse::<f64>().map(Some).map_err(|_| {
                EvalError::InvalidTextRepresentation(format!(
                    "{func}: invalid numeric syntax: {text}"
                ))
            })
        }
        other => Err(EvalError::Type(format!(
            "{func}: numeric argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_cast_bool(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "boolean cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(text) | Value::Char(text) => {
            parse_bool_cast_text(text).map(Value::Bool).ok_or_else(|| {
                EvalError::InvalidTextRepresentation(format!(
                    "boolean cast: invalid syntax: {text}"
                ))
            })
        }
        other => Err(EvalError::Type(format!(
            "boolean cast: text argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn parse_bool_cast_text(text: &str) -> Option<bool> {
    match text.trim() {
        "t" | "true" | "TRUE" | "T" | "1" | "y" | "Y" | "yes" | "YES" | "on" | "ON" => Some(true),
        "f" | "false" | "FALSE" | "F" | "0" | "n" | "N" | "no" | "NO" | "off" | "OFF" => {
            Some(false)
        }
        _ => None,
    }
}

pub(crate) fn eval_cast_date(args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = textlike_cast_arg("date cast", args)? else {
        return Ok(Value::Null);
    };
    parse_date_text(text.trim())
        .map(Value::Date)
        .ok_or_else(|| {
            EvalError::InvalidTextRepresentation(format!("date cast: invalid syntax: {text}"))
        })
}

pub(crate) fn eval_cast_time(args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = textlike_cast_arg("time cast", args)? else {
        return Ok(Value::Null);
    };
    parse_time_text(text.trim())
        .map(Value::Time)
        .ok_or_else(|| {
            EvalError::InvalidTextRepresentation(format!("time cast: invalid syntax: {text}"))
        })
}

pub(crate) fn eval_cast_timestamp(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "timestamp cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    // `date -> timestamp`: midnight of the date (engine stores both as
    // UTC-epoch micros, with `date` measured in whole days). Used by set-op
    // supertype resolution for `DATE UNION TIMESTAMP`.
    if let Value::Date(days) = &args[0] {
        return i64::from(*days)
            .checked_mul(MICROS_PER_DAY)
            .map(Value::Timestamp)
            .ok_or_else(|| {
                EvalError::NumericFieldOverflow(
                    "timestamp cast: date out of timestamp range".to_owned(),
                )
            });
    }
    let Some(text) = textlike_cast_arg("timestamp cast", args)? else {
        return Ok(Value::Null);
    };
    parse_timestamp_text(text.trim())
        .map(Value::Timestamp)
        .ok_or_else(|| {
            EvalError::InvalidTextRepresentation(format!("timestamp cast: invalid syntax: {text}"))
        })
}

pub(crate) fn eval_cast_timestamptz(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "timestamptz cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    // `date`/`timestamp` -> `timestamptz`: the engine stores all three as
    // UTC-epoch micros, so the instant is preserved. Used by set-op
    // supertype resolution (`DATE`/`TIMESTAMP UNION TIMESTAMPTZ`).
    match &args[0] {
        Value::Timestamp(micros) => return Ok(Value::TimestampTz(*micros)),
        Value::Date(days) => {
            return i64::from(*days)
                .checked_mul(MICROS_PER_DAY)
                .map(Value::TimestampTz)
                .ok_or_else(|| {
                    EvalError::NumericFieldOverflow(
                        "timestamptz cast: date out of timestamp range".to_owned(),
                    )
                });
        }
        _ => {}
    }
    let Some(text) = textlike_cast_arg("timestamptz cast", args)? else {
        return Ok(Value::Null);
    };
    parse_timestamptz_text(text.trim())
        .map(Value::TimestampTz)
        .ok_or_else(|| {
            EvalError::InvalidTextRepresentation(format!(
                "timestamptz cast: invalid syntax: {text}"
            ))
        })
}

pub(crate) fn eval_cast_timetz(args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = textlike_cast_arg("timetz cast", args)? else {
        return Ok(Value::Null);
    };
    parse_timetz_text(text.trim())
        .map(|(micros, offset_seconds)| Value::TimeTz {
            micros,
            offset_seconds,
        })
        .ok_or_else(|| {
            EvalError::InvalidTextRepresentation(format!("timetz cast: invalid syntax: {text}"))
        })
}

pub(crate) fn eval_cast_uuid(args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = textlike_cast_arg("uuid cast", args)? else {
        return Ok(Value::Null);
    };
    Value::parse_uuid(text.trim())
        .map(Value::Uuid)
        .ok_or_else(|| {
            EvalError::InvalidTextRepresentation(format!("uuid cast: invalid syntax: {text}"))
        })
}

pub(crate) fn eval_cast_json(args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = textlike_cast_arg("json cast", args)? else {
        return Ok(Value::Null);
    };
    serde_json::from_str::<JsonValue>(text)
        .map(|_| Value::Json(text.to_owned()))
        .map_err(|err| {
            EvalError::InvalidTextRepresentation(format!("json cast: invalid JSON: {err}"))
        })
}

pub(crate) fn eval_cast_jsonb(args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = textlike_cast_arg("jsonb cast", args)? else {
        return Ok(Value::Null);
    };
    let parsed = serde_json::from_str::<JsonValue>(text).map_err(|err| {
        EvalError::InvalidTextRepresentation(format!("jsonb cast: invalid JSON: {err}"))
    })?;
    json_value_to_jsonb(parsed, "jsonb cast")
}

pub(crate) fn eval_cast_xml(args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = textlike_cast_arg("xml cast", args)? else {
        return Ok(Value::Null);
    };
    Value::validate_xml_text(text)
        .map(Value::Xml)
        .ok_or_else(|| {
            EvalError::InvalidXmlDocument(format!("xml cast: invalid XML document: {text}"))
        })
}

pub(crate) fn textlike_cast_arg<'a>(
    func: &str,
    args: &'a [Value],
) -> Result<Option<&'a str>, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "{func}: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(None),
        Value::Text(text) | Value::Char(text) => Ok(Some(text)),
        other => Err(EvalError::Type(format!(
            "{func}: text argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_cast_oid(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "oid cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Ok(Value::Oid(*oid)),
        Value::Int16(v) => cast_i64_to_oid(i64::from(*v)).map(Value::Oid),
        Value::Int32(v) => cast_i64_to_oid(i64::from(*v)).map(Value::Oid),
        Value::Int64(v) => cast_i64_to_oid(*v).map(Value::Oid),
        other => Err(EvalError::Type(format!(
            "oid cast: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_cast_regclass(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "regclass cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Ok(Value::RegClass(*oid)),
        Value::Int16(v) => cast_i64_to_oid(i64::from(*v)).map(Value::RegClass),
        Value::Int32(v) => cast_i64_to_oid(i64::from(*v)).map(Value::RegClass),
        Value::Int64(v) => cast_i64_to_oid(*v).map(Value::RegClass),
        other => Err(EvalError::Type(format!(
            "regclass cast: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_cast_regtype(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "regtype cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Ok(Value::RegType(*oid)),
        Value::Int16(v) => cast_i64_to_oid(i64::from(*v)).map(Value::RegType),
        Value::Int32(v) => cast_i64_to_oid(i64::from(*v)).map(Value::RegType),
        Value::Int64(v) => cast_i64_to_oid(*v).map(Value::RegType),
        other => Err(EvalError::Type(format!(
            "regtype cast: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn cast_i64_to_oid(raw: i64) -> Result<Oid, EvalError> {
    u32::try_from(raw).map(Oid::new).map_err(|_| {
        EvalError::NumericFieldOverflow(format!("OID cast: value out of range: {raw}"))
    })
}

pub(crate) fn eval_cast_text(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "text cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if let Value::RegType(oid) = &args[0] {
        return Ok(Value::Text(
            builtin_type_display_name(oid.raw())
                .map_or_else(|| oid.raw().to_string(), str::to_owned),
        ));
    }
    Ok(Value::Text(args[0].to_string()))
}

pub(crate) fn eval_cast_inet(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "inet cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        // Already `inet`: pass through unchanged.
        Value::Network(net @ ultrasql_core::NetworkValue::Inet(_)) => Ok(Value::Network(*net)),
        // `cidr -> inet`: keep the same address payload, change the family.
        // Used by set-op supertype resolution (`inet UNION cidr`); without
        // this the `Cidr(x)` and `Inet(x)` variants never compare equal.
        Value::Network(ultrasql_core::NetworkValue::Cidr(addr)) => {
            Ok(Value::Network(ultrasql_core::NetworkValue::Inet(*addr)))
        }
        Value::Text(text) | Value::Char(text) => {
            ultrasql_core::NetworkValue::parse_for_type(&DataType::Inet, text.trim())
                .map(Value::Network)
                .ok_or_else(|| {
                    EvalError::InvalidTextRepresentation(format!(
                        "inet cast: invalid syntax: {text}"
                    ))
                })
        }
        other => Err(EvalError::Type(format!(
            "inet cast: inet/cidr/text argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_cast_money(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "money cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Money(cents) => Ok(Value::Money(*cents)),
        Value::Int16(v) => int_to_money(i64::from(*v)),
        Value::Int32(v) => int_to_money(i64::from(*v)),
        Value::Int64(v) => int_to_money(*v),
        Value::Decimal { .. } => {
            parse_money_text(&args[0].to_string()).map_err(money_cast_parse_error)
        }
        Value::Text(text) | Value::Char(text) => {
            parse_money_text(text).map_err(money_cast_parse_error)
        }
        other => Err(EvalError::Type(format!(
            "money cast: numeric argument required, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn money_cast_parse_error(err: impl std::fmt::Display) -> EvalError {
    let message = err.to_string();
    if message.contains("overflow") || message.contains("out of range") {
        EvalError::NumericFieldOverflow("money value out of range".to_owned())
    } else {
        EvalError::InvalidTextRepresentation(format!("money cast: {message}"))
    }
}

pub(crate) fn int_to_money(value: i64) -> Result<Value, EvalError> {
    value
        .checked_mul(100)
        .map(Value::Money)
        .ok_or(EvalError::Overflow)
}

pub(crate) fn eval_cast_numeric(args: &[Value]) -> Result<Value, EvalError> {
    if !matches!(args.len(), 1 | 3) {
        return Err(EvalError::Type(format!(
            "numeric cast: expected 1 or 3 args, got {}",
            args.len()
        )));
    }
    let (precision, target_scale) = numeric_cast_typmod(args)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Money(cents) => coerce_numeric_typmod(*cents, 2, precision, target_scale),
        Value::Decimal { value, scale } => {
            coerce_numeric_typmod(*value, *scale, precision, target_scale)
        }
        Value::Text(text) | Value::Char(text) => {
            let Value::Decimal { value, scale } =
                parse_decimal_text(text, target_scale).map_err(numeric_cast_parse_error)?
            else {
                return Err(EvalError::Type(
                    "numeric cast: decimal parser returned non-decimal".to_owned(),
                ));
            };
            coerce_numeric_typmod(value, scale, precision, target_scale)
        }
        other => match numeric_to_decimal(other)? {
            Some((value, scale)) => coerce_numeric_typmod(value, scale, precision, target_scale),
            None => Err(EvalError::Type(format!(
                "numeric cast: numeric argument required, got {:?}",
                other.data_type()
            ))),
        },
    }
}

pub(crate) fn numeric_cast_typmod(args: &[Value]) -> Result<(Option<u32>, Option<i32>), EvalError> {
    if args.len() == 1 {
        return Ok((None, None));
    }
    let precision = match args.get(1) {
        Some(Value::Null) => None,
        Some(Value::Int32(value)) if *value > 0 => Some(u32::try_from(*value).map_err(|_| {
            EvalError::Type(format!("numeric cast: precision out of range: {value}"))
        })?),
        Some(other) => {
            return Err(EvalError::Type(format!(
                "numeric cast: precision typmod must be positive integer or NULL, got {:?}",
                other.data_type()
            )));
        }
        None => None,
    };
    let scale = match args.get(2) {
        Some(Value::Null) => None,
        Some(Value::Int32(value)) => Some(*value),
        Some(other) => {
            return Err(EvalError::Type(format!(
                "numeric cast: scale typmod must be integer or NULL, got {:?}",
                other.data_type()
            )));
        }
        None => None,
    };
    Ok((precision, scale))
}

pub(crate) fn coerce_numeric_typmod(
    value: i64,
    scale: i32,
    precision: Option<u32>,
    target_scale: Option<i32>,
) -> Result<Value, EvalError> {
    let (value, scale) = if let Some(target_scale) = target_scale {
        let rendered = Value::Decimal { value, scale }.to_string();
        let Value::Decimal {
            value: rounded,
            scale,
        } = parse_decimal_text(&rendered, Some(target_scale)).map_err(numeric_cast_parse_error)?
        else {
            return Err(EvalError::Type(
                "numeric cast: decimal parser returned non-decimal".to_owned(),
            ));
        };
        (rounded, scale)
    } else {
        (value, scale)
    };
    validate_numeric_precision(value, scale, precision, target_scale)?;
    Ok(Value::Decimal { value, scale })
}

pub(crate) fn numeric_cast_parse_error(err: impl std::fmt::Display) -> EvalError {
    let message = err.to_string();
    if message.contains("overflow") || message.contains("out of range") {
        EvalError::NumericFieldOverflow("numeric value out of range".to_owned())
    } else {
        EvalError::InvalidTextRepresentation(format!("numeric cast: invalid syntax: {message}"))
    }
}

pub(crate) fn validate_numeric_precision(
    value: i64,
    scale: i32,
    precision: Option<u32>,
    declared_scale: Option<i32>,
) -> Result<(), EvalError> {
    let Some(precision) = precision else {
        return Ok(());
    };
    let precision = usize::try_from(precision)
        .map_err(|_| EvalError::NumericFieldOverflow("numeric precision out of range".into()))?;
    let actual_scale = usize::try_from(scale.max(0))
        .map_err(|_| EvalError::NumericFieldOverflow("numeric scale out of range".into()))?;
    let declared_scale = usize::try_from(declared_scale.unwrap_or(0).max(0))
        .map_err(|_| EvalError::NumericFieldOverflow("numeric scale out of range".into()))?;
    let magnitude = i128::from(value)
        .checked_abs()
        .ok_or_else(|| EvalError::NumericFieldOverflow("numeric magnitude overflow".into()))?;
    let total_digits = decimal_magnitude_digits(magnitude);
    let integer_digits = total_digits.saturating_sub(actual_scale);
    let max_integer_digits = precision.saturating_sub(declared_scale);
    if total_digits > precision || integer_digits > max_integer_digits {
        return Err(EvalError::NumericFieldOverflow(
            "numeric field overflow".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn decimal_magnitude_digits(mut magnitude: i128) -> usize {
    let mut digits = 1;
    while magnitude >= 10 {
        magnitude /= 10;
        digits += 1;
    }
    digits
}

pub(crate) fn resolve_regtype_text(text: &str) -> Option<Oid> {
    let trimmed = text.trim();
    if let Some(oid) = Value::parse_oid_text(trimmed) {
        return Some(oid);
    }
    let parts = parse_pg_identifier_path(trimmed)?;
    match parts.as_slice() {
        [name] => builtin_type_oid(name),
        [schema_name, name] if schema_name.eq_ignore_ascii_case("pg_catalog") => {
            builtin_type_oid(name)
        }
        _ => None,
    }
}

pub(crate) fn parse_pg_identifier_path(text: &str) -> Option<Vec<String>> {
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
