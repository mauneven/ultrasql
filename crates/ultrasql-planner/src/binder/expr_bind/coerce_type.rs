//! `coerce_literal_to_type` and the per-target literal coercion
//! helpers it dispatches to.

use super::*;

pub(in crate::binder) fn coerce_literal_to_type(expr: &mut ScalarExpr, target: &DataType) {
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

pub(in crate::binder) fn coerce_literal_to_enum(expr: &mut ScalarExpr, target: &DataType) -> bool {
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

pub(in crate::binder) fn coerce_literal_to_composite(expr: &mut ScalarExpr, target: &DataType) -> bool {
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

pub(in crate::binder) fn coerce_literal_to_array(expr: &mut ScalarExpr, target: &DataType) -> bool {
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

pub(in crate::binder) fn coerce_literal_to_bit_string(
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

pub(in crate::binder) fn coerce_literal_to_network(expr: &mut ScalarExpr, target: &DataType) -> bool {
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

pub(in crate::binder) fn bit_string_from_integer_target(value: i64, target: &DataType) -> Option<BitString> {
    let width = match target {
        DataType::Bit { len: Some(len) } => *len,
        DataType::Bit { len: None } => 1,
        DataType::VarBit { max_len: Some(len) } => *len,
        DataType::VarBit { max_len: None } => 64,
        _ => return None,
    };
    BitString::from_i64(width, value)
}

pub(in crate::binder) fn coerce_literal_to_bpchar(expr: &mut ScalarExpr, target: &DataType, explicit_cast: bool) -> bool {
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

