//! Literal binding: scalar, array, and typed (DATE / INTERVAL /
//! network / vector / temporal) literals, plus literal constant
//! folding for date-interval arithmetic.

use super::*;

pub(in crate::binder) fn bind_literal(lit: &Literal) -> ScalarExpr {
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

pub(in crate::binder) fn bind_array_literal(
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

pub(in crate::binder) fn common_array_element_type(left: &DataType, right: &DataType) -> Result<DataType, PlanError> {
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
pub(in crate::binder) fn bind_typed_literal(type_name: &str, value: &str, unit: Option<&str>) -> ScalarExpr {
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

pub(in crate::binder) fn validate_json_text(value: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(value).ok()?;
    Some(value.to_owned())
}

pub(in crate::binder) fn normalize_jsonb_text(value: &str) -> Option<String> {
    let parsed = serde_json::from_str::<serde_json::Value>(value).ok()?;
    serde_json::to_string(&parsed).ok()
}

pub(in crate::binder) fn bind_bit_string_literal(value: &str, type_name: &str) -> ScalarExpr {
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

pub(in crate::binder) fn bind_network_literal(value: &str, data_type: DataType) -> ScalarExpr {
    let parsed =
        Value::parse_network(&data_type, value).unwrap_or_else(|| Value::Text(value.to_owned()));
    ScalarExpr::Literal {
        value: parsed,
        data_type,
    }
}

pub(in crate::binder) fn bind_vector_family_literal(value: &str, declared_type: DataType) -> ScalarExpr {
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

pub(in crate::binder) fn parse_interval_literal(text: &str, unit: Option<&str>) -> Option<(i32, i32, i64)> {
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
pub(in crate::binder) fn parse_date_literal(text: &str) -> Option<i32> {
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

pub(in crate::binder) fn parse_timestamp_literal(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    let split = trimmed.find(' ').or_else(|| trimmed.find('T'))?;
    let date = &trimmed[..split];
    let time = &trimmed[split + 1..];
    let days = i64::from(parse_date_literal(date)?);
    let micros = parse_time_of_day_micros(time)?;
    days.checked_mul(MICROS_PER_DAY)?.checked_add(micros)
}

pub(in crate::binder) fn parse_timestamptz_literal(text: &str) -> Option<i64> {
    parse_timestamptz_text(text)
}

pub(in crate::binder) fn parse_time_of_day_micros(text: &str) -> Option<i64> {
    parse_time_text(text)
}

pub(in crate::binder) fn parse_timetz_literal(text: &str) -> Option<(i64, i32)> {
    parse_timetz_text(text)
}

pub(in crate::binder) fn civil_from_days(days_since_2000_01_01: i32) -> Result<(i32, u32, u32), PlanError> {
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

pub(in crate::binder) fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub(in crate::binder) fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 31,
    }
}

pub(in crate::binder) fn add_months_to_date(date_days: i32, month_delta: i32) -> Result<i32, PlanError> {
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

pub(in crate::binder) fn fold_date_interval(
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

pub(in crate::binder) fn try_fold_literal_binary(
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

pub(in crate::binder) fn is_float_like_literal(value: &Value) -> bool {
    matches!(value, Value::Float32(_) | Value::Float64(_))
}

pub(in crate::binder) fn literal_numeric_as_f64(value: &Value) -> Option<f64> {
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
