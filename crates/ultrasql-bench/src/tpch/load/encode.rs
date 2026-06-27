//! Row encoders, literal formatters, and direct heap codecs for the TPC-H
//! loader.
//!
//! Two encoding paths live here:
//!
//! * the `INSERT ... VALUES` SQL builder used by the wire loader
//!   ([`build_ultrasql_insert_sql`] and friends), gated on `any(test,
//!   feature = "sql-bench")`; and
//! * the binary row codec ([`encode_direct_tbl_row`] /
//!   [`decode_direct_decimal_i64`]) used by the in-process direct loader,
//!   gated on `feature = "sql-bench"`.

#[cfg(any(test, feature = "sql-bench"))]
use anyhow::{Context, Result, bail};
#[cfg(any(test, feature = "sql-bench"))]
use std::fmt::Write as _;

#[cfg(any(test, feature = "sql-bench"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnKind {
    Int,
    Text,
    Decimal,
    Date,
}

#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_NUMERIC_NBASE: u16 = 10_000;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_NUMERIC_DEC_DIGITS: i32 = 4;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_NUMERIC_POS: u16 = 0x0000;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_NUMERIC_NEG: u16 = 0x4000;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_NUMERIC_HEADER_WIDTH: usize = 8;
#[cfg(feature = "sql-bench")]
pub(crate) const DIRECT_NUMERIC_DIGIT_WIDTH: usize = std::mem::size_of::<u16>();

#[cfg(any(test, feature = "sql-bench"))]
pub(crate) fn column_kinds(table: &str) -> &'static [ColumnKind] {
    use ColumnKind::{Date, Decimal, Int, Text};

    match table {
        "region" => &[Int, Text, Text],
        "nation" => &[Int, Text, Int, Text],
        "supplier" => &[Int, Text, Text, Int, Text, Decimal, Text],
        "customer" => &[Int, Text, Text, Int, Text, Decimal, Text, Text],
        "part" => &[Int, Text, Text, Text, Text, Int, Text, Decimal, Text],
        "partsupp" => &[Int, Int, Int, Decimal, Text],
        "orders" => &[Int, Int, Text, Decimal, Date, Text, Text, Int, Text],
        "lineitem" => &[
            Int, Int, Int, Int, Decimal, Decimal, Decimal, Decimal, Text, Text, Date, Date, Date,
            Text, Text, Text,
        ],
        _ => &[],
    }
}

#[cfg(any(test, feature = "sql-bench"))]
pub(crate) fn escape_sql_text(text: &str) -> String {
    text.replace('\'', "''")
}

#[cfg(any(test, feature = "sql-bench"))]
pub(crate) fn format_ultrasql_literal(kind: ColumnKind, raw: &str) -> Result<String> {
    match kind {
        ColumnKind::Int => {
            raw.parse::<i64>()
                .with_context(|| format!("parse integer literal `{raw}`"))?;
            Ok(raw.to_owned())
        }
        ColumnKind::Decimal => {
            raw.parse::<f64>()
                .with_context(|| format!("parse decimal literal `{raw}`"))?;
            Ok(raw.to_owned())
        }
        ColumnKind::Date => Ok(format!("DATE '{}'", escape_sql_text(raw))),
        ColumnKind::Text => Ok(format!("'{}'", escape_sql_text(raw))),
    }
}

#[cfg(any(test, feature = "sql-bench"))]
pub(crate) fn build_ultrasql_insert_sql(table: &str, rows: &[Vec<String>]) -> Result<String> {
    let kinds = column_kinds(table);
    if kinds.is_empty() {
        bail!("unknown TPC-H table `{table}`");
    }
    let mut sql = String::new();
    write!(&mut sql, "INSERT INTO {table} VALUES ").context("format insert SQL prefix")?;
    for (row_idx, row) in rows.iter().enumerate() {
        if row.len() != kinds.len() {
            bail!(
                "{table}: row {} has {} fields, expected {}",
                row_idx + 1,
                row.len(),
                kinds.len()
            );
        }
        if row_idx > 0 {
            sql.push(',');
        }
        sql.push('(');
        for (col_idx, field) in row.iter().enumerate() {
            if col_idx > 0 {
                sql.push(',');
            }
            sql.push_str(&format_ultrasql_literal(kinds[col_idx], field)?);
        }
        sql.push(')');
    }
    Ok(sql)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn encode_direct_tbl_row(schema: &ultrasql_core::Schema, line: &str) -> Result<Vec<u8>> {
    let bitmap_bytes = schema.len().div_ceil(8);
    let mut out = Vec::with_capacity(bitmap_bytes.saturating_add(line.len()));
    out.resize(bitmap_bytes, 0);
    let mut fields = line.split('|');
    for (idx, field) in schema.fields().iter().enumerate() {
        let raw = fields
            .next()
            .ok_or_else(|| anyhow::anyhow!("field count mismatch: missing column {idx}"))?;
        encode_direct_value(&field.data_type, raw, idx, &mut out)?;
    }
    if fields.next().is_some() {
        bail!(
            "field count mismatch: got more than {} fields",
            schema.len()
        );
    }
    Ok(out)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn encode_direct_value(
    dtype: &ultrasql_core::DataType,
    raw: &str,
    column_idx: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    use std::borrow::Cow;

    use ultrasql_core::{DataType, Value, coerce_bpchar_text};

    match dtype {
        DataType::Bool => out.push(u8::from(parse_direct_bool(raw, column_idx)?)),
        DataType::Int16 => out.extend_from_slice(
            &raw.parse::<i16>()
                .with_context(|| format!("column {column_idx}: parse SMALLINT `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Int32 => out.extend_from_slice(
            &raw.parse::<i32>()
                .with_context(|| format!("column {column_idx}: parse INTEGER `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Int64 => out.extend_from_slice(
            &raw.parse::<i64>()
                .with_context(|| format!("column {column_idx}: parse BIGINT `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Float32 => out.extend_from_slice(
            &raw.parse::<f32>()
                .with_context(|| format!("column {column_idx}: parse REAL `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Float64 => out.extend_from_slice(
            &raw.parse::<f64>()
                .with_context(|| format!("column {column_idx}: parse DOUBLE `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Decimal { scale, .. } => {
            let Value::Decimal {
                value,
                scale: value_scale,
            } = parse_direct_decimal(raw, scale.unwrap_or(0), column_idx)?
            else {
                unreachable!("parse_direct_decimal always returns Decimal");
            };
            encode_direct_decimal(out, value, value_scale, column_idx)?;
        }
        DataType::Date => {
            out.extend_from_slice(&parse_direct_date(raw, column_idx)?.to_le_bytes());
        }
        DataType::Text { .. } | DataType::Char { .. } => {
            let text = match dtype {
                DataType::Text { .. } => Cow::Borrowed(raw),
                DataType::Char { len } => Cow::Owned(
                    coerce_bpchar_text(raw, *len, false)
                        .with_context(|| format!("column {column_idx}: coerce CHAR `{raw}`"))?,
                ),
                _ => unreachable!("textlike branch only matches Text or Char"),
            };
            let bytes = text.as_bytes();
            let len = u32::try_from(bytes.len())
                .with_context(|| format!("column {column_idx}: text too large"))?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        other => bail!("column {column_idx}: direct TPC-H load unsupported type {other}"),
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
pub(crate) fn parse_direct_bool(raw: &str, column_idx: usize) -> Result<bool> {
    match raw {
        "t" | "true" | "TRUE" | "T" | "1" | "y" | "Y" | "yes" | "YES" => Ok(true),
        "f" | "false" | "FALSE" | "F" | "0" | "n" | "N" | "no" | "NO" => Ok(false),
        other => bail!("column {column_idx}: not a boolean ({other:?})"),
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn parse_direct_decimal(
    raw: &str,
    scale: i32,
    column_idx: usize,
) -> Result<ultrasql_core::Value> {
    let raw = raw.trim();
    let scale_usize = usize::try_from(scale)
        .with_context(|| format!("column {column_idx}: negative decimal scale {scale}"))?;
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
        bail!("column {column_idx}: invalid decimal literal {raw:?}");
    }
    if frac.len() > scale_usize && frac.as_bytes()[scale_usize..].iter().any(|&b| b != b'0') {
        bail!("column {column_idx}: decimal literal {raw:?} has scale greater than {scale}");
    }

    let mut value: i128 = 0;
    for digit in whole.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    for digit in frac.bytes().take(scale_usize) {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    let missing_frac_digits = scale_usize.saturating_sub(frac.len().min(scale_usize));
    for _ in 0..missing_frac_digits {
        value = value
            .checked_mul(10)
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    if negative {
        value = value
            .checked_neg()
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    Ok(ultrasql_core::Value::Decimal { value, scale })
}

#[cfg(feature = "sql-bench")]
pub(crate) fn encode_direct_decimal(
    out: &mut Vec<u8>,
    value: i128,
    scale: i32,
    column_idx: usize,
) -> Result<()> {
    let (weight, sign, dscale, digits) = direct_decimal_parts(value, scale, column_idx)?;
    let payload_len = DIRECT_NUMERIC_HEADER_WIDTH
        .checked_add(
            digits
                .len()
                .checked_mul(DIRECT_NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal payload too large"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal payload too large"))?;
    let payload_len_u32 = u32::try_from(payload_len)
        .with_context(|| format!("column {column_idx}: decimal payload too large"))?;
    let ndigits = i16::try_from(digits.len())
        .with_context(|| format!("column {column_idx}: decimal has too many digit groups"))?;

    out.extend_from_slice(&payload_len_u32.to_le_bytes());
    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for digit in digits {
        out.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_decimal_parts(
    value: i128,
    scale: i32,
    column_idx: usize,
) -> Result<(i16, u16, i16, Vec<u16>)> {
    let sign = if value < 0 {
        DIRECT_NUMERIC_NEG
    } else {
        DIRECT_NUMERIC_POS
    };
    let magnitude = value
        .checked_abs()
        .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal magnitude overflow"))?;
    let dscale = i16::try_from(scale)
        .with_context(|| format!("column {column_idx}: decimal display scale out of range"))?;
    if dscale < 0 {
        bail!("column {column_idx}: negative decimal scale {scale}");
    }
    if magnitude == 0 {
        return Ok((0, DIRECT_NUMERIC_POS, dscale, Vec::new()));
    }

    let magnitude_digits = magnitude.to_string();
    let dscale_usize = usize::try_from(scale)
        .with_context(|| format!("column {column_idx}: decimal display scale out of range"))?;
    let group_width = usize::try_from(DIRECT_NUMERIC_DEC_DIGITS)
        .context("direct numeric decimal digit group width")?;
    let digit_len = magnitude_digits.len();
    let integer_digits = digit_len.saturating_sub(dscale_usize);
    let groups_before_decimal = integer_digits.div_ceil(group_width);
    let mut grouped = String::new();

    if groups_before_decimal > 0 {
        let padded_integer_digits = groups_before_decimal
            .checked_mul(group_width)
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal payload too large"))?;
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
        let rem = grouped.len() % group_width;
        if rem != 0 {
            for _ in 0..group_width - rem {
                grouped.push('0');
            }
        }
    }

    let mut digits = grouped
        .as_bytes()
        .chunks_exact(group_width)
        .map(direct_decimal_group_to_u16)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| anyhow::anyhow!("column {column_idx}: invalid decimal digit group"))?;
    let mut weight = i32::try_from(groups_before_decimal)
        .with_context(|| format!("column {column_idx}: decimal weight out of range"))?
        - 1;
    let leading_zeroes = digits.iter().take_while(|digit| **digit == 0).count();
    if leading_zeroes > 0 {
        digits.drain(..leading_zeroes);
        weight -= i32::try_from(leading_zeroes)
            .with_context(|| format!("column {column_idx}: decimal weight out of range"))?;
    }
    while digits.last().is_some_and(|digit| *digit == 0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }

    Ok((
        i16::try_from(weight)
            .with_context(|| format!("column {column_idx}: decimal weight out of range"))?,
        sign,
        dscale,
        digits,
    ))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_decimal_group_to_u16(group: &[u8]) -> Option<u16> {
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

#[cfg(feature = "sql-bench")]
pub(crate) fn parse_direct_date(raw: &str, column_idx: usize) -> Result<i32> {
    let raw = raw.trim();
    if raw.len() != 10 {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    let bytes = raw.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    let year = raw[..4]
        .parse::<i32>()
        .with_context(|| format!("column {column_idx}: invalid date year"))?;
    let month = raw[5..7]
        .parse::<u32>()
        .with_context(|| format!("column {column_idx}: invalid date month"))?;
    let day = raw[8..10]
        .parse::<u32>()
        .with_context(|| format!("column {column_idx}: invalid date day"))?;
    if !(1..=12).contains(&month) || day == 0 || day > direct_days_in_month(year, month) {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    Ok(direct_days_since_epoch(year, month, day))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if direct_is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = direct_i32_to_u32(y - era * 400);
    let month_prime = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_since_1970 = era * 146_097 + direct_u32_to_i32(doe) - 719_468;
    days_since_1970 - 10_957
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_i32_to_u32(value: i32) -> u32 {
    u32::try_from(value).unwrap_or_default()
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_u32_to_i32(value: u32) -> i32 {
    i32::try_from(value).unwrap_or_default()
}

#[cfg(feature = "sql-bench")]
pub(crate) fn read_direct_i32(payload: &[u8], off: &mut usize, label: &str) -> Result<i32> {
    let end = off.saturating_add(4);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated i32"))?;
    *off = end;
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label}: i32 width checked"))?;
    Ok(i32::from_le_bytes(bytes))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn read_direct_decimal_i64(payload: &[u8], off: &mut usize, label: &str) -> Result<i64> {
    let len = read_direct_u32(payload, off, label)?;
    let len = usize::try_from(len).with_context(|| format!("{label}: numeric too large"))?;
    let end = off.saturating_add(len);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated numeric"))?;
    *off = end;
    decode_direct_decimal_i64(bytes, label)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn decode_direct_decimal_i64(bytes: &[u8], label: &str) -> Result<i64> {
    if bytes.len() < DIRECT_NUMERIC_HEADER_WIDTH {
        bail!("{label}: truncated numeric header");
    }
    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]);
    if ndigits < 0 {
        bail!("{label}: negative numeric digit count");
    }
    let ndigits = usize::try_from(ndigits)
        .with_context(|| format!("{label}: invalid numeric digit count"))?;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    if !matches!(sign, DIRECT_NUMERIC_POS | DIRECT_NUMERIC_NEG) {
        bail!("{label}: unsupported numeric sign");
    }
    let dscale = i16::from_be_bytes([bytes[6], bytes[7]]);
    if dscale < 0 {
        bail!("{label}: negative numeric display scale");
    }
    let expected_len = DIRECT_NUMERIC_HEADER_WIDTH
        .checked_add(
            ndigits
                .checked_mul(DIRECT_NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| anyhow::anyhow!("{label}: numeric payload too large"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("{label}: numeric payload too large"))?;
    if bytes.len() != expected_len {
        bail!("{label}: numeric payload length mismatch");
    }

    let mut acc = 0_i128;
    for (idx, raw) in bytes[DIRECT_NUMERIC_HEADER_WIDTH..]
        .chunks_exact(DIRECT_NUMERIC_DIGIT_WIDTH)
        .enumerate()
    {
        let digit = u16::from_be_bytes([raw[0], raw[1]]);
        if digit >= DIRECT_NUMERIC_NBASE {
            bail!("{label}: numeric digit outside base-10000");
        }
        if digit == 0 {
            continue;
        }
        let idx_i32 = i32::try_from(idx).with_context(|| format!("{label}: numeric too large"))?;
        let base_exp = i32::from(weight)
            .checked_sub(idx_i32)
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent underflow"))?;
        let decimal_exp = base_exp
            .checked_mul(DIRECT_NUMERIC_DEC_DIGITS)
            .and_then(|exp| exp.checked_add(i32::from(dscale)))
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent overflow"))?;
        let term = if decimal_exp < 0 {
            let divisor = pow10_i128(
                decimal_exp
                    .checked_neg()
                    .and_then(|exp| u32::try_from(exp).ok())
                    .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent overflow"))?;
            let digit = i128::from(digit);
            if digit % divisor != 0 {
                bail!("{label}: numeric stores more fractional digits than display scale");
            }
            digit / divisor
        } else {
            let pow = pow10_i128(
                u32::try_from(decimal_exp)
                    .with_context(|| format!("{label}: numeric exponent overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent overflow"))?;
            i128::from(digit)
                .checked_mul(pow)
                .ok_or_else(|| anyhow::anyhow!("{label}: numeric value overflow"))?
        };
        acc = acc
            .checked_add(term)
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric value overflow"))?;
    }
    if sign == DIRECT_NUMERIC_NEG {
        acc = acc
            .checked_neg()
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric value overflow"))?;
    }
    i64::try_from(acc).with_context(|| format!("{label}: numeric value overflows i64"))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn read_direct_u32(payload: &[u8], off: &mut usize, label: &str) -> Result<u32> {
    let end = off.saturating_add(4);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated u32"))?;
    *off = end;
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label}: u32 width checked"))?;
    Ok(u32::from_le_bytes(bytes))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn read_direct_one_byte_text(
    payload: &[u8],
    off: &mut usize,
    label: &str,
) -> Result<u8> {
    let len = read_direct_u32(payload, off, label)?;
    let len = usize::try_from(len).with_context(|| format!("{label}: text too large"))?;
    let bytes = payload
        .get(*off..off.saturating_add(len))
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated text"))?;
    *off = off.saturating_add(len);
    bytes
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("{label}: empty text"))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn read_direct_text<'a>(
    payload: &'a [u8],
    off: &mut usize,
    label: &str,
) -> Result<&'a str> {
    let len = read_direct_u32(payload, off, label)?;
    let len = usize::try_from(len).with_context(|| format!("{label}: text too large"))?;
    let bytes = payload
        .get(*off..off.saturating_add(len))
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated text"))?;
    *off = off.saturating_add(len);
    std::str::from_utf8(bytes).with_context(|| format!("{label}: invalid utf8"))
}
