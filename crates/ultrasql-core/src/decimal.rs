//! Decimal parsing and PostgreSQL numeric binary helpers.
//!
//! UltraSQL's runtime decimal value is still a scaled `i64`, but the SQL
//! surface and PostgreSQL wire/COPY formats need shared exact parsing,
//! scale rounding, and base-10000 numeric payload conversion.

use crate::Value;

const NUMERIC_NBASE: u16 = 10_000;
const NUMERIC_DEC_DIGITS: i32 = 4;
const NUMERIC_GROUP_WIDTH: usize = 4;
const NUMERIC_DSCALE_MAX: i32 = 0x3fff;
const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_BINARY_HEADER_WIDTH: usize = 8;
const NUMERIC_DIGIT_WIDTH: usize = std::mem::size_of::<u16>();

/// Error raised while parsing or encoding a decimal value.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{message}")]
pub struct DecimalError {
    /// Human-readable error message suitable for wrapping by callers.
    pub message: String,
}

impl DecimalError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug)]
struct NumericBinaryParts {
    weight: i16,
    sign: u16,
    dscale: i16,
    digits: Vec<u16>,
}

/// Parse SQL decimal text into UltraSQL's scaled `i64` runtime value.
///
/// When `target_scale` is `Some`, the value is rounded to that scale
/// using PostgreSQL-style half-away-from-zero numeric rounding. When it
/// is `None`, the literal's fractional scale is preserved.
pub fn parse_decimal_text(raw: &str, target_scale: Option<i32>) -> Result<Value, DecimalError> {
    let raw = raw.trim();
    let (negative, digits) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    let mut parts = digits.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (whole.is_empty() && frac.is_empty())
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(DecimalError::new(format!(
            "invalid decimal literal {raw:?}"
        )));
    }

    let literal_scale =
        i32::try_from(frac.len()).map_err(|_| DecimalError::new("decimal scale out of range"))?;
    let scale = target_scale.unwrap_or(literal_scale);
    if scale < 0 {
        return Err(DecimalError::new(format!(
            "negative decimal scale {scale} is not supported"
        )));
    }
    let scale_usize =
        usize::try_from(scale).map_err(|_| DecimalError::new("decimal scale out of range"))?;

    let mut value: i128 = 0;
    for digit in whole.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| DecimalError::new("decimal overflow"))?;
    }

    for digit in frac.bytes().take(scale_usize) {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| DecimalError::new("decimal overflow"))?;
    }
    let copied_frac = frac.len().min(scale_usize);
    for _ in 0..scale_usize.saturating_sub(copied_frac) {
        value = value
            .checked_mul(10)
            .ok_or_else(|| DecimalError::new("decimal overflow"))?;
    }

    if frac.len() > scale_usize && frac.as_bytes()[scale_usize] >= b'5' {
        value = value
            .checked_add(1)
            .ok_or_else(|| DecimalError::new("decimal overflow"))?;
    }
    if negative {
        value = value
            .checked_neg()
            .ok_or_else(|| DecimalError::new("decimal overflow"))?;
    }
    let value = i64::try_from(value).map_err(|_| DecimalError::new("decimal overflow"))?;
    Ok(Value::Decimal { value, scale })
}

/// Encode UltraSQL's scaled decimal as PostgreSQL numeric binary payload.
///
/// Returned bytes are the field payload used by numeric send/receive and
/// binary COPY: `ndigits`, `weight`, `sign`, `dscale`, then base-10000
/// digit groups, all big-endian.
pub fn encode_pg_numeric_binary(value: i64, scale: i32) -> Result<Vec<u8>, DecimalError> {
    let parts = decimal_to_numeric_parts(value, scale)?;
    let payload_len = NUMERIC_BINARY_HEADER_WIDTH
        .checked_add(
            parts
                .digits
                .len()
                .checked_mul(NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| DecimalError::new("numeric payload too large"))?,
        )
        .ok_or_else(|| DecimalError::new("numeric payload too large"))?;
    let ndigits = i16::try_from(parts.digits.len())
        .map_err(|_| DecimalError::new("numeric has too many digit groups"))?;

    let mut out = Vec::with_capacity(payload_len);
    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&parts.weight.to_be_bytes());
    out.extend_from_slice(&parts.sign.to_be_bytes());
    out.extend_from_slice(&parts.dscale.to_be_bytes());
    for digit in parts.digits {
        out.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(out)
}

/// Decode PostgreSQL numeric binary payload into UltraSQL's decimal value.
pub fn decode_pg_numeric_binary(payload: &[u8]) -> Result<Value, DecimalError> {
    if payload.len() < NUMERIC_BINARY_HEADER_WIDTH {
        return Err(DecimalError::new("truncated numeric header"));
    }
    let ndigits = i16::from_be_bytes([payload[0], payload[1]]);
    if ndigits < 0 {
        return Err(DecimalError::new("negative numeric digit count"));
    }
    let ndigits_usize =
        usize::try_from(ndigits).map_err(|_| DecimalError::new("invalid numeric digit count"))?;
    let weight = i16::from_be_bytes([payload[2], payload[3]]);
    let sign = u16::from_be_bytes([payload[4], payload[5]]);
    if !matches!(sign, NUMERIC_POS | NUMERIC_NEG) {
        return Err(DecimalError::new("unsupported numeric sign"));
    }
    let dscale = i16::from_be_bytes([payload[6], payload[7]]);
    if dscale < 0 {
        return Err(DecimalError::new("negative numeric display scale"));
    }
    let expected_len = NUMERIC_BINARY_HEADER_WIDTH
        .checked_add(
            ndigits_usize
                .checked_mul(NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| DecimalError::new("numeric payload too large"))?,
        )
        .ok_or_else(|| DecimalError::new("numeric payload too large"))?;
    if payload.len() != expected_len {
        return Err(DecimalError::new("numeric payload length mismatch"));
    }

    let mut digits = Vec::with_capacity(ndigits_usize);
    for raw in payload[NUMERIC_BINARY_HEADER_WIDTH..].chunks_exact(NUMERIC_DIGIT_WIDTH) {
        let digit = u16::from_be_bytes([raw[0], raw[1]]);
        if digit >= NUMERIC_NBASE {
            return Err(DecimalError::new("numeric digit outside base-10000"));
        }
        digits.push(digit);
    }
    numeric_parts_to_value(&digits, weight, sign, dscale)
}

fn decimal_to_numeric_parts(value: i64, scale: i32) -> Result<NumericBinaryParts, DecimalError> {
    let sign = if value < 0 { NUMERIC_NEG } else { NUMERIC_POS };
    let mut magnitude = i128::from(value)
        .checked_abs()
        .ok_or_else(|| DecimalError::new("numeric magnitude overflow"))?;
    let dscale_i32 = if scale < 0 {
        let exp = scale
            .checked_neg()
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| DecimalError::new("numeric scale out of range"))?;
        magnitude = magnitude
            .checked_mul(
                pow10_i128(exp)
                    .ok_or_else(|| DecimalError::new("numeric negative scale overflow"))?,
            )
            .ok_or_else(|| DecimalError::new("numeric negative scale overflow"))?;
        0
    } else {
        scale
    };
    if dscale_i32 > NUMERIC_DSCALE_MAX {
        return Err(DecimalError::new("numeric display scale out of range"));
    }
    let dscale = i16::try_from(dscale_i32)
        .map_err(|_| DecimalError::new("numeric display scale out of range"))?;
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
        .map_err(|_| DecimalError::new("numeric display scale out of range"))?;
    let digit_len = magnitude_digits.len();
    let integer_digits = digit_len.saturating_sub(dscale_usize);
    let groups_before_decimal = integer_digits.div_ceil(NUMERIC_GROUP_WIDTH);
    let mut grouped = String::new();

    if groups_before_decimal > 0 {
        let padded_integer_digits = groups_before_decimal
            .checked_mul(NUMERIC_GROUP_WIDTH)
            .ok_or_else(|| DecimalError::new("numeric payload too large"))?;
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
        let rem = grouped.len() % NUMERIC_GROUP_WIDTH;
        if rem != 0 {
            for _ in 0..NUMERIC_GROUP_WIDTH - rem {
                grouped.push('0');
            }
        }
    }

    let mut digits = grouped
        .as_bytes()
        .chunks_exact(NUMERIC_GROUP_WIDTH)
        .map(decimal_group_to_u16)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| DecimalError::new("invalid numeric digit group"))?;
    let mut weight = i32::try_from(groups_before_decimal)
        .map_err(|_| DecimalError::new("numeric weight out of range"))?
        - 1;

    let leading_zeroes = digits.iter().take_while(|digit| **digit == 0).count();
    if leading_zeroes > 0 {
        digits.drain(..leading_zeroes);
        weight -= i32::try_from(leading_zeroes)
            .map_err(|_| DecimalError::new("numeric weight out of range"))?;
    }
    while digits.last().is_some_and(|digit| *digit == 0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }

    Ok(NumericBinaryParts {
        weight: i16::try_from(weight)
            .map_err(|_| DecimalError::new("numeric weight out of range"))?,
        sign,
        dscale,
        digits,
    })
}

fn decimal_group_to_u16(group: &[u8]) -> Option<u16> {
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

fn numeric_parts_to_value(
    digits: &[u16],
    weight: i16,
    sign: u16,
    dscale: i16,
) -> Result<Value, DecimalError> {
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
        let idx_i32 =
            i32::try_from(idx).map_err(|_| DecimalError::new("numeric payload too large"))?;
        let base_exp = i32::from(weight)
            .checked_sub(idx_i32)
            .ok_or_else(|| DecimalError::new("numeric exponent underflow"))?;
        let decimal_exp = base_exp
            .checked_mul(NUMERIC_DEC_DIGITS)
            .and_then(|exp| exp.checked_add(i32::from(dscale)))
            .ok_or_else(|| DecimalError::new("numeric exponent overflow"))?;
        let term = if decimal_exp < 0 {
            let divisor = pow10_i128(
                decimal_exp
                    .checked_neg()
                    .and_then(|exp| u32::try_from(exp).ok())
                    .ok_or_else(|| DecimalError::new("numeric exponent overflow"))?,
            )
            .ok_or_else(|| DecimalError::new("numeric exponent overflow"))?;
            let digit = i128::from(*digit);
            if digit % divisor != 0 {
                return Err(DecimalError::new(
                    "numeric stores more fractional digits than display scale",
                ));
            }
            digit / divisor
        } else {
            let pow = pow10_i128(
                u32::try_from(decimal_exp)
                    .map_err(|_| DecimalError::new("numeric exponent overflow"))?,
            )
            .ok_or_else(|| DecimalError::new("numeric exponent overflow"))?;
            i128::from(*digit)
                .checked_mul(pow)
                .ok_or_else(|| DecimalError::new("numeric value overflow"))?
        };
        acc = acc
            .checked_add(term)
            .ok_or_else(|| DecimalError::new("numeric value overflow"))?;
    }
    if sign == NUMERIC_NEG {
        acc = acc
            .checked_neg()
            .ok_or_else(|| DecimalError::new("numeric value overflow"))?;
    }
    Ok(Value::Decimal {
        value: i64::try_from(acc)
            .map_err(|_| DecimalError::new("numeric value overflows i64 runtime"))?,
        scale: i32::from(dscale),
    })
}

fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rounds_half_away_from_zero() {
        assert_eq!(
            parse_decimal_text("1.235", Some(2)).unwrap(),
            Value::Decimal {
                value: 124,
                scale: 2
            }
        );
        assert_eq!(
            parse_decimal_text("-1.235", Some(2)).unwrap(),
            Value::Decimal {
                value: -124,
                scale: 2
            }
        );
    }

    #[test]
    fn pg_numeric_binary_round_trip() {
        let bytes = encode_pg_numeric_binary(12_340, 3).unwrap();
        assert_eq!(
            bytes,
            vec![
                0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x0c, 0x0d, 0x48,
            ]
        );
        assert_eq!(
            decode_pg_numeric_binary(&bytes).unwrap(),
            Value::Decimal {
                value: 12_340,
                scale: 3
            }
        );
    }
}
