//! MONEY helpers.
//!
//! UltraSQL stores `MONEY` as PostgreSQL's `Cash` shape: a signed
//! 64-bit integer counting fractional currency units. This v0.8 surface
//! uses a deterministic cents scale for storage, COPY, and wire tests.
//! Session-level text rendering can select a small deterministic set of
//! locale templates through `lc_monetary`; storage stays locale-free.

use crate::{Value, parse_decimal_text};

const MONEY_SCALE: i32 = 2;
const MONEY_BINARY_WIDTH: usize = std::mem::size_of::<i64>();
const CENTS_PER_UNIT: i128 = 100;
const GROUP_WIDTH: usize = 3;

/// Error raised while parsing or encoding a money value.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{message}")]
pub struct MoneyError {
    /// Human-readable error message suitable for wrapping by callers.
    pub message: String,
}

impl MoneyError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Parse SQL money text into a signed-cent runtime value.
///
/// Accepted forms include plain decimals (`12.34`), currency-prefixed
/// input (`$1,234.56`), leading signs, and parenthesised negatives.
/// The value is rounded to two fractional digits, matching the current
/// deterministic money surface.
pub fn parse_money_text(raw: &str) -> Result<Value, MoneyError> {
    let mut text = raw.trim();
    if text.is_empty() {
        return Err(MoneyError::new("empty money literal"));
    }

    let mut negative = false;
    if let Some(parenthesized) = text
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
    {
        negative = true;
        text = parenthesized.trim();
    }

    text = consume_sign(text, &mut negative);
    text = strip_currency_markers(text);
    text = consume_sign(text, &mut negative);
    text = strip_currency_markers(text);

    let Some(cleaned) = normalize_money_number(text) else {
        return Err(MoneyError::new(format!("invalid money literal {raw:?}")));
    };
    let decimal_text = if negative {
        format!("-{cleaned}")
    } else {
        cleaned
    };
    let decimal = parse_decimal_text(&decimal_text, Some(MONEY_SCALE))
        .map_err(|err| MoneyError::new(err.to_string()))?;
    let Value::Decimal { value, .. } = decimal else {
        return Err(MoneyError::new("money parser returned non-decimal value"));
    };
    let cents = i64::try_from(value)
        .map_err(|_| MoneyError::new(format!("money value {raw:?} out of range")))?;
    Ok(Value::Money(cents))
}

fn strip_currency_markers(mut text: &str) -> &str {
    loop {
        let before = text;
        text = text.trim();
        for marker in ["USD", "EUR", "GBP", "BRL", "R$"] {
            if let Some(rest) = strip_ascii_prefix(text, marker) {
                text = rest.trim_start();
            }
            if let Some(rest) = strip_ascii_suffix(text, marker) {
                text = rest.trim_end();
            }
        }
        for symbol in ['$', '\u{20ac}', '\u{00a3}', '\u{00a5}'] {
            if let Some(rest) = text.strip_prefix(symbol) {
                text = rest.trim_start();
            }
            if let Some(rest) = text.strip_suffix(symbol) {
                text = rest.trim_end();
            }
        }
        if text == before {
            return text;
        }
    }
}

fn strip_ascii_prefix<'a>(text: &'a str, marker: &str) -> Option<&'a str> {
    let prefix = text.get(..marker.len())?;
    prefix
        .eq_ignore_ascii_case(marker)
        .then_some(&text[marker.len()..])
}

fn strip_ascii_suffix<'a>(text: &'a str, marker: &str) -> Option<&'a str> {
    let start = text.len().checked_sub(marker.len())?;
    let suffix = text.get(start..)?;
    suffix
        .eq_ignore_ascii_case(marker)
        .then_some(&text[..start])
}

fn normalize_money_number(text: &str) -> Option<String> {
    let compact: String = text.chars().filter(|ch| !ch.is_whitespace()).collect();
    if compact.is_empty()
        || !compact
            .chars()
            .all(|ch| ch.is_ascii_digit() || ch == '.' || ch == ',')
    {
        return None;
    }

    let last_dot = compact.rfind('.');
    let last_comma = compact.rfind(',');
    let decimal_sep = match (last_dot, last_comma) {
        (Some(dot), Some(comma)) => Some(if dot > comma { '.' } else { ',' }),
        (None, Some(comma)) if comma_marks_fraction(&compact, comma) => Some(','),
        _ => Some('.'),
    };

    let mut normalized = String::with_capacity(compact.len());
    let mut saw_decimal = false;
    for ch in compact.chars() {
        if ch.is_ascii_digit() {
            normalized.push(ch);
        } else if Some(ch) == decimal_sep {
            if saw_decimal {
                return None;
            }
            saw_decimal = true;
            normalized.push('.');
        } else if ch == '.' || ch == ',' {
            // Treat the non-decimal punctuation as a thousands separator.
        } else {
            return None;
        }
    }
    if normalized.is_empty() || normalized == "." {
        None
    } else {
        Some(normalized)
    }
}

fn comma_marks_fraction(text: &str, comma: usize) -> bool {
    let before_has_digit = text[..comma].chars().any(|ch| ch.is_ascii_digit());
    let Some(after_start) = comma.checked_add(1) else {
        return false;
    };
    let Some(after_comma) = text.get(after_start..) else {
        return false;
    };
    let digits_after = after_comma.chars().filter(|ch| ch.is_ascii_digit()).count();
    before_has_digit && (1..=2).contains(&digits_after)
}

fn consume_sign<'a>(text: &'a str, negative: &mut bool) -> &'a str {
    if let Some(rest) = text.strip_prefix('-') {
        *negative = !*negative;
        rest.trim_start()
    } else if let Some(rest) = text.strip_prefix('+') {
        rest.trim_start()
    } else {
        text
    }
}

/// Format signed cents as deterministic PostgreSQL-style money text.
#[must_use]
pub fn format_money_text(cents: i64) -> String {
    let magnitude = i128::from(cents).abs();
    let (dollars, cents_part) = split_money_parts(magnitude);
    let mut out = String::new();
    if cents < 0 {
        out.push('-');
    }
    out.push('$');
    push_grouped_digits(&mut out, &dollars.to_string());
    out.push('.');
    out.push_str(&format!("{cents_part:02}"));
    out
}

/// Format signed cents using a deterministic `lc_monetary` template.
///
/// The helper intentionally avoids host locale APIs so release artifacts are
/// reproducible across CI runners and containers. Unknown locales fall back to
/// the `C` / `en_US` dollar template used by [`format_money_text`].
#[must_use]
pub fn format_money_text_with_locale(cents: i64, lc_monetary: &str) -> String {
    let locale = normalize_locale_name(lc_monetary);
    match locale.as_str() {
        "de" | "de_de" | "de_at" | "de_ch" => {
            format_money_with_template(cents, "", " \u{20ac}", '.', ',')
        }
        "fr" | "fr_fr" | "fr_ca" | "fr_be" | "fr_ch" => {
            format_money_with_template(cents, "", " \u{20ac}", ' ', ',')
        }
        "pt_br" => format_money_with_template(cents, "R$ ", "", '.', ','),
        "en_gb" => format_money_with_template(cents, "\u{00a3}", "", ',', '.'),
        _ => format_money_text(cents),
    }
}

fn normalize_locale_name(value: &str) -> String {
    value
        .trim()
        .split(['.', '@'])
        .next()
        .unwrap_or(value)
        .replace('-', "_")
        .to_ascii_lowercase()
}

fn format_money_with_template(
    cents: i64,
    prefix: &str,
    suffix: &str,
    group_sep: char,
    decimal_sep: char,
) -> String {
    let magnitude = i128::from(cents).abs();
    let (dollars, cents_part) = split_money_parts(magnitude);
    let mut out = String::new();
    if cents < 0 {
        out.push('-');
    }
    out.push_str(prefix);
    push_grouped_digits_with_separator(&mut out, &dollars.to_string(), group_sep);
    out.push(decimal_sep);
    out.push_str(&format!("{cents_part:02}"));
    out.push_str(suffix);
    out
}

fn push_grouped_digits(out: &mut String, digits: &str) {
    push_grouped_digits_with_separator(out, digits, ',');
}

fn push_grouped_digits_with_separator(out: &mut String, digits: &str, separator: char) {
    for (idx, ch) in digits.chars().enumerate() {
        if idx > 0
            && digits
                .len()
                .checked_sub(idx)
                .and_then(|remaining| remaining.checked_rem(GROUP_WIDTH))
                == Some(0)
        {
            out.push(separator);
        }
        out.push(ch);
    }
}

fn split_money_parts(magnitude: i128) -> (i128, i128) {
    (
        magnitude.checked_div(CENTS_PER_UNIT).unwrap_or(0),
        magnitude.checked_rem(CENTS_PER_UNIT).unwrap_or(0),
    )
}

/// Encode `MONEY` as PostgreSQL binary `cash` payload.
#[must_use]
pub const fn encode_pg_money_binary(cents: i64) -> [u8; MONEY_BINARY_WIDTH] {
    cents.to_be_bytes()
}

/// Decode PostgreSQL binary `cash` payload into [`Value::Money`].
pub fn decode_pg_money_binary(payload: &[u8]) -> Result<Value, MoneyError> {
    let raw: [u8; MONEY_BINARY_WIDTH] = payload
        .try_into()
        .map_err(|_| MoneyError::new("money binary payload must be 8 bytes"))?;
    Ok(Value::Money(i64::from_be_bytes(raw)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_money_text_accepts_common_signed_currency_forms() {
        assert_eq!(parse_money_text(" $1,234.565 "), Ok(Value::Money(123_457)));
        assert_eq!(parse_money_text("-$1.235"), Ok(Value::Money(-124)));
        assert_eq!(parse_money_text("$-0.015"), Ok(Value::Money(-2)));
        assert_eq!(parse_money_text("(+$2.00)"), Ok(Value::Money(-200)));
        assert_eq!(
            parse_money_text("1.234,565 \u{20ac}"),
            Ok(Value::Money(123_457))
        );
        assert_eq!(parse_money_text("R$ 1.234,56"), Ok(Value::Money(123_456)));
        assert_eq!(
            parse_money_text(""),
            Err(MoneyError::new("empty money literal"))
        );
        assert!(parse_money_text("$").is_err());
    }

    #[test]
    fn format_money_text_groups_negative_and_large_values() {
        assert_eq!(format_money_text(0), "$0.00");
        assert_eq!(format_money_text(123_456_789), "$1,234,567.89");
        assert_eq!(format_money_text(-123_456_789), "-$1,234,567.89");
    }

    #[test]
    fn format_money_text_with_locale_uses_deterministic_templates() {
        assert_eq!(
            format_money_text_with_locale(123_456, "de_DE.UTF-8"),
            "1.234,56 \u{20ac}"
        );
        assert_eq!(
            format_money_text_with_locale(-123_456, "fr_FR"),
            "-1 234,56 \u{20ac}"
        );
        assert_eq!(
            format_money_text_with_locale(123_456, "pt_BR"),
            "R$ 1.234,56"
        );
        assert_eq!(
            format_money_text_with_locale(123_456, "unknown"),
            "$1,234.56"
        );
    }

    #[test]
    fn pg_money_binary_round_trips_and_rejects_bad_width() {
        let encoded = encode_pg_money_binary(-123_456);
        assert_eq!(decode_pg_money_binary(&encoded), Ok(Value::Money(-123_456)));
        assert_eq!(
            decode_pg_money_binary(&encoded[..7]),
            Err(MoneyError::new("money binary payload must be 8 bytes"))
        );
    }
}
