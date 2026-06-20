//! NUMERIC / DECIMAL / MONEY binary payload codec helpers.

use super::*;
use ultrasql_core::{DataType, Value};

#[derive(Debug)]
pub(crate) struct NumericBinaryParts {
    weight: i16,
    sign: u16,
    dscale: i16,
    digits: Vec<u16>,
}

pub(crate) fn encode_numeric_value_payload(
    payload: &mut Vec<u8>,
    value: i64,
    scale: i32,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    let parts = decimal_to_numeric_parts(value, scale, column, ty)?;
    let payload_len = NUMERIC_BINARY_HEADER_WIDTH
        .checked_add(
            parts
                .digits
                .len()
                .checked_mul(NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?,
        )
        .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?;
    let payload_len_u32 = u32::try_from(payload_len)
        .map_err(|_| numeric_type_error(column, ty, "numeric payload too large"))?;
    let ndigits = i16::try_from(parts.digits.len())
        .map_err(|_| numeric_type_error(column, ty, "numeric has too many digit groups"))?;

    payload.extend_from_slice(&payload_len_u32.to_le_bytes());
    payload.extend_from_slice(&ndigits.to_be_bytes());
    payload.extend_from_slice(&parts.weight.to_be_bytes());
    payload.extend_from_slice(&parts.sign.to_be_bytes());
    payload.extend_from_slice(&parts.dscale.to_be_bytes());
    for digit in parts.digits {
        payload.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(())
}

pub(crate) fn decimal_to_numeric_parts(
    value: i64,
    scale: i32,
    column: usize,
    ty: &DataType,
) -> Result<NumericBinaryParts, RowCodecError> {
    let sign = if value < 0 { NUMERIC_NEG } else { NUMERIC_POS };
    let mut magnitude = i128::from(value)
        .checked_abs()
        .ok_or_else(|| numeric_type_error(column, ty, "numeric magnitude overflow"))?;
    let dscale_i32 = if scale < 0 {
        let exp = scale
            .checked_neg()
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| numeric_type_error(column, ty, "numeric scale out of range"))?;
        magnitude =
            magnitude
                .checked_mul(pow10_i128(exp).ok_or_else(|| {
                    numeric_type_error(column, ty, "numeric negative scale overflow")
                })?)
                .ok_or_else(|| numeric_type_error(column, ty, "numeric negative scale overflow"))?;
        0
    } else {
        scale
    };
    if dscale_i32 > NUMERIC_DSCALE_MAX {
        return Err(numeric_type_error(
            column,
            ty,
            "numeric display scale out of range",
        ));
    }
    let dscale = i16::try_from(dscale_i32)
        .map_err(|_| numeric_type_error(column, ty, "numeric display scale out of range"))?;
    if magnitude == 0 {
        return Ok(NumericBinaryParts {
            weight: 0,
            sign: NUMERIC_POS,
            dscale,
            digits: Vec::new(),
        });
    }

    let magnitude_digits = magnitude.to_string();
    let dscale_usize = usize::try_from(dscale_i32)
        .map_err(|_| numeric_type_error(column, ty, "numeric display scale out of range"))?;
    let digit_len = magnitude_digits.len();
    let integer_digits = digit_len.saturating_sub(dscale_usize);
    let groups_before_decimal = integer_digits.div_ceil(NUMERIC_DEC_DIGITS_USIZE);
    let mut grouped = String::new();

    if groups_before_decimal > 0 {
        let padded_integer_digits = groups_before_decimal
            .checked_mul(NUMERIC_DEC_DIGITS_USIZE)
            .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?;
        for _ in 0..padded_integer_digits.saturating_sub(integer_digits) {
            grouped.push('0');
        }
        grouped.push_str(&magnitude_digits[..integer_digits]);
    }

    if dscale_usize > 0 {
        if dscale_usize > digit_len {
            for _ in 0..dscale_usize - digit_len {
                grouped.push('0');
            }
            grouped.push_str(&magnitude_digits);
        } else {
            grouped.push_str(&magnitude_digits[digit_len - dscale_usize..]);
        }
        let rem = grouped.len() % NUMERIC_DEC_DIGITS_USIZE;
        if rem != 0 {
            for _ in 0..NUMERIC_DEC_DIGITS_USIZE - rem {
                grouped.push('0');
            }
        }
    }

    let mut digits = grouped
        .as_bytes()
        .chunks_exact(NUMERIC_DEC_DIGITS_USIZE)
        .map(decimal_group_to_u16)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| numeric_type_error(column, ty, "invalid numeric digit group"))?;
    let mut weight = i32::try_from(groups_before_decimal)
        .map_err(|_| numeric_type_error(column, ty, "numeric weight out of range"))?
        - 1;

    let leading_zeroes = digits.iter().take_while(|digit| **digit == 0).count();
    if leading_zeroes > 0 {
        digits.drain(..leading_zeroes);
        weight -= i32::try_from(leading_zeroes)
            .map_err(|_| numeric_type_error(column, ty, "numeric weight out of range"))?;
    }
    while digits.last().is_some_and(|digit| *digit == 0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }

    Ok(NumericBinaryParts {
        weight: i16::try_from(weight)
            .map_err(|_| numeric_type_error(column, ty, "numeric weight out of range"))?,
        sign,
        dscale,
        digits,
    })
}

pub(crate) fn validate_decimal_precision(
    value: i64,
    value_scale: i32,
    precision: Option<u32>,
    declared_scale: Option<i32>,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    let Some(precision) = precision else {
        return Ok(());
    };
    let precision = usize::try_from(precision)
        .map_err(|_| numeric_field_overflow(column, ty, "numeric precision out of range"))?;
    let actual_scale = usize::try_from(value_scale.max(0))
        .map_err(|_| numeric_field_overflow(column, ty, "numeric scale out of range"))?;
    let declared_scale = usize::try_from(declared_scale.unwrap_or(0).max(0))
        .map_err(|_| numeric_field_overflow(column, ty, "numeric scale out of range"))?;

    let magnitude = i128::from(value)
        .checked_abs()
        .ok_or_else(|| numeric_field_overflow(column, ty, "numeric magnitude overflow"))?;
    let total_digits = decimal_magnitude_digits(magnitude);
    let integer_digits = total_digits.saturating_sub(actual_scale);
    let max_integer_digits = precision.saturating_sub(declared_scale);

    if total_digits > precision || integer_digits > max_integer_digits {
        return Err(numeric_field_overflow(column, ty, "numeric field overflow"));
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

pub(crate) fn numeric_field_overflow(column: usize, ty: &DataType, detail: &str) -> RowCodecError {
    RowCodecError::NumericFieldOverflow {
        column,
        ty: ty.clone(),
        detail: detail.to_owned(),
    }
}

pub(crate) fn decimal_group_to_u16(group: &[u8]) -> Option<u16> {
    let mut value = 0_u16;
    for byte in group {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value
            .checked_mul(10)?
            .checked_add(u16::from(*byte - b'0'))?;
    }
    Some(value)
}

pub(crate) fn decode_numeric_value(
    bytes: &[u8],
    cursor: &mut usize,
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    let payload = read_varlena_slice(bytes, cursor)?;
    decode_numeric_payload(payload, column, ty)
}

pub(crate) fn decode_numeric_scaled_i64(
    bytes: &[u8],
    cursor: &mut usize,
    column: usize,
    ty: &DataType,
) -> Result<i64, RowCodecError> {
    match decode_numeric_value(bytes, cursor, column, ty)? {
        Value::Decimal { value, .. } => Ok(value),
        _ => unreachable!("decode_numeric_value always returns Decimal"),
    }
}

pub(crate) fn decode_numeric_payload(
    payload: &[u8],
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    if payload.len() < NUMERIC_BINARY_HEADER_WIDTH {
        return Err(RowCodecError::Truncated {
            needed: NUMERIC_BINARY_HEADER_WIDTH,
            have: payload.len(),
        });
    }
    let ndigits = i16::from_be_bytes([payload[0], payload[1]]);
    if ndigits < 0 {
        return Err(numeric_type_error(
            column,
            ty,
            "negative numeric digit count",
        ));
    }
    let ndigits_usize = usize::try_from(ndigits)
        .map_err(|_| numeric_type_error(column, ty, "invalid numeric digit count"))?;
    let weight = i16::from_be_bytes([payload[2], payload[3]]);
    let sign = u16::from_be_bytes([payload[4], payload[5]]);
    if !matches!(sign, NUMERIC_POS | NUMERIC_NEG) {
        return Err(numeric_type_error(column, ty, "unsupported numeric sign"));
    }
    let dscale = i16::from_be_bytes([payload[6], payload[7]]);
    if dscale < 0 {
        return Err(numeric_type_error(
            column,
            ty,
            "negative numeric display scale",
        ));
    }
    let expected_len = NUMERIC_BINARY_HEADER_WIDTH
        .checked_add(
            ndigits_usize
                .checked_mul(NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?,
        )
        .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?;
    if payload.len() != expected_len {
        return Err(numeric_type_error(
            column,
            ty,
            "numeric payload length mismatch",
        ));
    }

    let mut digits = Vec::with_capacity(ndigits_usize);
    for raw in payload[NUMERIC_BINARY_HEADER_WIDTH..].chunks_exact(NUMERIC_DIGIT_WIDTH) {
        let digit = u16::from_be_bytes([raw[0], raw[1]]);
        if digit >= NUMERIC_NBASE {
            return Err(numeric_type_error(
                column,
                ty,
                "numeric digit outside base-10000",
            ));
        }
        digits.push(digit);
    }
    numeric_parts_to_value(&digits, weight, sign, dscale, column, ty)
}

pub(crate) fn numeric_parts_to_value(
    digits: &[u16],
    weight: i16,
    sign: u16,
    dscale: i16,
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    if digits.is_empty() {
        return Ok(Value::Decimal {
            value: 0,
            scale: i32::from(dscale),
        });
    }
    let mut acc = 0_i128;
    for (idx, digit) in digits.iter().enumerate() {
        if *digit == 0 {
            continue;
        }
        let idx_i32 = i32::try_from(idx)
            .map_err(|_| numeric_type_error(column, ty, "numeric payload too large"))?;
        let base_exp = i32::from(weight)
            .checked_sub(idx_i32)
            .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent underflow"))?;
        let decimal_exp = base_exp
            .checked_mul(NUMERIC_DEC_DIGITS)
            .and_then(|exp| exp.checked_add(i32::from(dscale)))
            .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent overflow"))?;
        let term = if decimal_exp < 0 {
            let divisor = pow10_i128(
                decimal_exp
                    .checked_neg()
                    .and_then(|exp| u32::try_from(exp).ok())
                    .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent overflow"))?,
            )
            .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent overflow"))?;
            let digit = i128::from(*digit);
            if digit % divisor != 0 {
                return Err(numeric_type_error(
                    column,
                    ty,
                    "numeric stores more fractional digits than display scale",
                ));
            }
            digit / divisor
        } else {
            let pow = pow10_i128(
                u32::try_from(decimal_exp)
                    .map_err(|_| numeric_type_error(column, ty, "numeric exponent overflow"))?,
            )
            .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent overflow"))?;
            i128::from(*digit)
                .checked_mul(pow)
                .ok_or_else(|| numeric_type_error(column, ty, "numeric value overflow"))?
        };
        acc = acc
            .checked_add(term)
            .ok_or_else(|| numeric_type_error(column, ty, "numeric value overflow"))?;
    }
    if sign == NUMERIC_NEG {
        acc = acc
            .checked_neg()
            .ok_or_else(|| numeric_type_error(column, ty, "numeric value overflow"))?;
    }
    Ok(Value::Decimal {
        value: i64::try_from(acc)
            .map_err(|_| numeric_type_error(column, ty, "numeric value overflows i64 runtime"))?,
        scale: i32::from(dscale),
    })
}

pub(crate) fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

pub(crate) fn numeric_type_error(column: usize, ty: &DataType, got: &str) -> RowCodecError {
    RowCodecError::Type {
        column,
        expected: ty.clone(),
        got: got.to_owned(),
    }
}
