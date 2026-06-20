//! Numeric, decimal, money, and OID-alias literal parsing and
//! coercion helpers, plus the civil-date epoch conversion.

use super::*;

/// Days from the 2000-01-01 epoch to (year, month, day), positive or
/// negative. The algorithm is Howard Hinnant's `days_from_civil`,
/// rebased on 2000-03-01 internally then offset back to 2000-01-01.
/// Source: <https://howardhinnant.github.io/date_algorithms.html>.
pub(in crate::binder) fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    let y = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let month_i32 = i32::try_from(month).ok()?;
    let day_i32 = i32::try_from(day).ok()?;
    let month_offset = if month > 2 {
        month_i32 - 3
    } else {
        month_i32 + 9
    };
    let doy = (153 * month_offset + 2) / 5 + day_i32 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days_from_1970_03_01 = i64::from(era)
        .checked_mul(146_097)?
        .checked_add(i64::from(doe))?
        .checked_sub(719_468)?;
    // Rebase from 1970-01-01 to 2000-01-01 (10_957 days).
    let days_since_2000_01_01 = days_from_1970_03_01.checked_sub(10_957)?;
    i32::try_from(days_since_2000_01_01).ok()
}

/// Pick the narrowest signed integer type that fits a decimal literal.
pub(in crate::binder) fn parse_integer_literal(text: &str) -> (Value, DataType) {
    if let Ok(v) = text.parse::<i32>() {
        return (Value::Int32(v), DataType::Int32);
    }
    if let Ok(v) = text.parse::<i64>() {
        return (Value::Int64(v), DataType::Int64);
    }
    // Out of i64 range — fall back to a Decimal placeholder; this
    // matches what `numeric_join` already promotes integer literals to
    // when paired with a Decimal column. We do not yet have a Decimal
    // Value variant, so park it as `Int64::MAX`. A future pass with
    // a Decimal datum will replace this branch.
    (
        Value::Int64(i64::MAX),
        DataType::Decimal {
            precision: None,
            scale: None,
        },
    )
}

pub(in crate::binder) fn bind_numeric_literal(text: &str) -> ScalarExpr {
    if let Some((value, scale)) = parse_decimal_literal(text) {
        return ScalarExpr::Literal {
            value: Value::Decimal { value, scale },
            data_type: DataType::Decimal {
                precision: None,
                scale: Some(scale),
            },
        };
    }

    // Exponent notation is approximate in the current literal model.
    let parsed = text.parse::<f64>().unwrap_or(f64::NAN);
    ScalarExpr::Literal {
        value: Value::Float64(parsed),
        data_type: DataType::Float64,
    }
}

pub(in crate::binder) fn parse_decimal_literal(text: &str) -> Option<(i64, i32)> {
    if text.contains('e') || text.contains('E') {
        return None;
    }
    let Value::Decimal { value, scale } = parse_decimal_text(text, None).ok()? else {
        return None;
    };
    Some((value, scale))
}

pub(in crate::binder) fn parse_bool_text(text: &str) -> Option<bool> {
    match text.trim() {
        "t" | "true" | "TRUE" | "T" | "1" | "y" | "Y" | "yes" | "YES" | "on" | "ON" => Some(true),
        "f" | "false" | "FALSE" | "F" | "0" | "n" | "N" | "no" | "NO" | "off" | "OFF" => {
            Some(false)
        }
        _ => None,
    }
}

pub(in crate::binder) fn pow10_i64(exp: u32) -> Option<i64> {
    (0..exp).try_fold(1_i64, |acc, _| acc.checked_mul(10))
}

pub(in crate::binder) fn infer_decimal_scale(value: &Value) -> Option<i32> {
    match value {
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => Some(0),
        Value::Float32(v) => infer_decimal_scale_from_text(&v.to_string()),
        Value::Float64(v) => infer_decimal_scale_from_text(&v.to_string()),
        Value::Decimal { scale, .. } => Some(*scale),
        _ => None,
    }
}

pub(in crate::binder) fn infer_decimal_scale_from_text(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    let dot = trimmed.find('.')?;
    i32::try_from(trimmed[dot + 1..].trim_end_matches('0').len()).ok()
}

pub(in crate::binder) fn decimal_from_numeric_value(
    value: &Value,
    target_scale: Option<i32>,
) -> Option<(i64, i32)> {
    let inferred_scale = infer_decimal_scale(value);
    let scale = match (target_scale, inferred_scale) {
        (Some(target), _) => target,
        (None, Some(inferred)) => inferred,
        (None, None) => return None,
    };
    if scale < 0 {
        return None;
    }
    let factor = pow10_i64(u32::try_from(scale).ok()?)?;
    match value {
        Value::Int16(v) => i64::from(*v)
            .checked_mul(factor)
            .map(|scaled| (scaled, scale)),
        Value::Int32(v) => i64::from(*v)
            .checked_mul(factor)
            .map(|scaled| (scaled, scale)),
        Value::Int64(v) => v.checked_mul(factor).map(|scaled| (scaled, scale)),
        Value::Float32(v) => decimal_from_f64(f64::from(*v), scale).map(|scaled| (scaled, scale)),
        Value::Float64(v) => decimal_from_f64(*v, scale).map(|scaled| (scaled, scale)),
        Value::Decimal {
            value: decimal_value,
            scale: decimal_scale,
        } if *decimal_scale == scale => Some((*decimal_value, scale)),
        _ => None,
    }
}

pub(in crate::binder) fn decimal_value_to_f64(value: i64, scale: i32) -> Option<f64> {
    value.to_f64().map(|raw| raw / 10_f64.powi(scale))
}

pub(in crate::binder) fn money_from_literal_value(value: &Value) -> Option<i64> {
    match value {
        Value::Int16(v) => i64::from(*v).checked_mul(100),
        Value::Int32(v) => i64::from(*v).checked_mul(100),
        Value::Int64(v) => v.checked_mul(100),
        Value::Float32(_) | Value::Float64(_) => None,
        Value::Decimal {
            value: decimal_value,
            scale,
        } => {
            let rendered = Value::Decimal {
                value: *decimal_value,
                scale: *scale,
            }
            .to_string();
            let Value::Money(cents) = parse_money_text(&rendered).ok()? else {
                return None;
            };
            Some(cents)
        }
        Value::Text(text) => {
            let Value::Money(cents) = parse_money_text(text).ok()? else {
                return None;
            };
            Some(cents)
        }
        Value::Money(cents) => Some(*cents),
        _ => None,
    }
}

pub(in crate::binder) fn oid_from_literal_value(value: &Value) -> Option<Oid> {
    match value {
        Value::Int16(v) => u32::try_from(*v).ok().map(Oid::new),
        Value::Int32(v) => u32::try_from(*v).ok().map(Oid::new),
        Value::Int64(v) => u32::try_from(*v).ok().map(Oid::new),
        Value::Text(text) | Value::Char(text) => Value::parse_oid_text(text),
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Some(*oid),
        _ => None,
    }
}

pub(in crate::binder) fn coerce_literal_to_oid_alias(
    expr: &mut ScalarExpr,
    target: &DataType,
) -> bool {
    fold_signed_literal(expr);
    let ScalarExpr::Literal { value, data_type } = expr else {
        return false;
    };
    if matches!(data_type, DataType::Null) && matches!(value, Value::Null) {
        if target.is_oid_alias() || matches!(target, DataType::PgLsn) {
            *data_type = target.clone();
            return true;
        }
        return false;
    }
    match target {
        DataType::Oid | DataType::RegClass | DataType::RegType => {
            let Some(oid) = oid_from_literal_value(value) else {
                return false;
            };
            *value = match target {
                DataType::Oid => Value::Oid(oid),
                DataType::RegClass => Value::RegClass(oid),
                DataType::RegType => Value::RegType(oid),
                _ => unreachable!(),
            };
            *data_type = target.clone();
            true
        }
        DataType::PgLsn => {
            let parsed = match value {
                Value::PgLsn(lsn) => Some(*lsn),
                Value::Text(text) | Value::Char(text) => Value::parse_pg_lsn_text(text),
                _ => None,
            };
            let Some(lsn) = parsed else {
                return false;
            };
            *value = Value::PgLsn(lsn);
            *data_type = DataType::PgLsn;
            true
        }
        _ => false,
    }
}

pub(in crate::binder) fn coerce_literal_to_oid_alias_with_catalog(
    expr: &mut ScalarExpr,
    target: &DataType,
    catalog: &dyn Catalog,
) -> bool {
    fold_signed_literal(expr);
    if matches!(target, DataType::RegClass | DataType::RegType) {
        let ScalarExpr::Literal { value, data_type } = expr else {
            return false;
        };
        if matches!(data_type, DataType::Null) && matches!(value, Value::Null) {
            *data_type = target.clone();
            return true;
        }
        let resolved = match (target, &*value) {
            (DataType::RegClass, Value::Text(text) | Value::Char(text)) => {
                resolve_regclass_literal(text, catalog)
            }
            (DataType::RegType, Value::Text(text) | Value::Char(text)) => {
                resolve_regtype_literal(text, catalog)
            }
            _ => oid_from_literal_value(value),
        };
        let Some(oid) = resolved else {
            return false;
        };
        *value = match target {
            DataType::RegClass => Value::RegClass(oid),
            DataType::RegType => Value::RegType(oid),
            _ => unreachable!(),
        };
        *data_type = target.clone();
        return true;
    }
    coerce_literal_to_oid_alias(expr, target)
}

pub(in crate::binder) fn resolve_regclass_literal(
    text: &str,
    catalog: &dyn Catalog,
) -> Option<Oid> {
    if let Some(oid) = Value::parse_oid_text(text) {
        return Some(oid);
    }
    let parts = parse_pg_identifier_path(text)?;
    match parts.as_slice() {
        [name] => catalog.lookup_table_oid(name),
        [schema_name, relation_name] => {
            catalog.lookup_table_oid_in_schema(schema_name, relation_name)
        }
        _ => None,
    }
}

pub(in crate::binder) fn resolve_regtype_literal(text: &str, catalog: &dyn Catalog) -> Option<Oid> {
    if let Some(oid) = Value::parse_oid_text(text) {
        return Some(oid);
    }
    let parts = parse_pg_identifier_path(text)?;
    match parts.as_slice() {
        [name] => catalog.lookup_type_oid(name),
        [schema_name, type_name] => catalog.lookup_type_oid_in_schema(schema_name, type_name),
        _ => None,
    }
}

pub(in crate::binder) fn decimal_from_f64(value: f64, scale: i32) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    let scale_usize = usize::try_from(scale).ok()?;
    let rendered = format!("{value:.scale_usize$}");
    scaled_decimal_text_to_i64(&rendered)
}

pub(in crate::binder) fn scaled_decimal_text_to_i64(text: &str) -> Option<i64> {
    let (negative, unsigned) = text
        .strip_prefix('-')
        .map_or((false, text), |stripped| (true, stripped));
    let (whole, frac) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(whole.len() + frac.len());
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.push_str(frac);
    let mut value = digits.parse::<i64>().ok()?;
    if negative {
        value = value.checked_neg()?;
    }
    Some(value)
}

pub(in crate::binder) fn fold_signed_literal(expr: &mut ScalarExpr) {
    let ScalarExpr::Unary {
        op,
        expr: inner,
        data_type: _,
    } = expr
    else {
        return;
    };
    if !matches!(op, UnaryOp::Neg | UnaryOp::Pos) {
        return;
    }

    let ScalarExpr::Literal { value, data_type } = inner.as_ref() else {
        return;
    };

    let folded = match (op, value) {
        (UnaryOp::Pos, value) => Some((value.clone(), data_type.clone())),
        (UnaryOp::Neg, Value::Int16(v)) => v
            .checked_neg()
            .map(|neg| (Value::Int16(neg), data_type.clone())),
        (UnaryOp::Neg, Value::Int32(v)) => v
            .checked_neg()
            .map(|neg| (Value::Int32(neg), data_type.clone())),
        (UnaryOp::Neg, Value::Int64(v)) => v
            .checked_neg()
            .map(|neg| (Value::Int64(neg), data_type.clone())),
        (UnaryOp::Neg, Value::Float32(v)) => Some((Value::Float32(-v), data_type.clone())),
        (UnaryOp::Neg, Value::Float64(v)) => Some((Value::Float64(-v), data_type.clone())),
        (UnaryOp::Neg, Value::Decimal { value, scale }) => value.checked_neg().map(|neg| {
            (
                Value::Decimal {
                    value: neg,
                    scale: *scale,
                },
                data_type.clone(),
            )
        }),
        (UnaryOp::Neg, Value::Money(v)) => v
            .checked_neg()
            .map(|neg| (Value::Money(neg), data_type.clone())),
        _ => None,
    };

    if let Some((value, data_type)) = folded {
        *expr = ScalarExpr::Literal { value, data_type };
    }
}

pub(in crate::binder) fn parse_negative_i64_boundary(text: &str) -> Option<i64> {
    let unsigned = text.replace('_', "");
    let magnitude = unsigned.parse::<u128>().ok()?;
    let max_plus_one = u128::try_from(i64::MAX).ok()?.checked_add(1)?;
    (magnitude == max_plus_one).then_some(i64::MIN)
}

pub(in crate::binder) fn parse_negative_i64_boundary_expr(expr: &Expr) -> Option<i64> {
    let Expr::Literal(Literal::Integer { text, .. }) = expr else {
        return None;
    };
    parse_negative_i64_boundary(text)
}
