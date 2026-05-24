//! PostgreSQL-compatible MONEY helpers.
//!
//! UltraSQL stores `MONEY` as PostgreSQL's `Cash` shape: a signed
//! 64-bit integer counting fractional currency units. This v0.8 surface
//! uses a deterministic cents scale for text, COPY, and wire tests;
//! locale-sensitive `lc_monetary` formatting remains a higher-level
//! session setting concern.

use crate::{Value, parse_decimal_text};

const MONEY_SCALE: i32 = 2;
const MONEY_BINARY_WIDTH: usize = std::mem::size_of::<i64>();

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
    if text.starts_with('(') && text.ends_with(')') {
        negative = true;
        text = text[1..text.len() - 1].trim();
    }

    text = consume_sign(text, &mut negative);
    if let Some(rest) = text.strip_prefix('$') {
        text = rest.trim_start();
    }
    text = consume_sign(text, &mut negative);

    let cleaned: String = text.chars().filter(|ch| *ch != ',').collect();
    if cleaned.is_empty() {
        return Err(MoneyError::new(format!("invalid money literal {raw:?}")));
    }
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
    Ok(Value::Money(value))
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
    let dollars = magnitude / 100;
    let cents_part = magnitude % 100;
    let mut out = String::new();
    if cents < 0 {
        out.push('-');
    }
    out.push('$');
    push_grouped_digits(&mut out, &dollars.to_string());
    use std::fmt::Write as _;
    write!(&mut out, ".{cents_part:02}").expect("write to String cannot fail");
    out
}

fn push_grouped_digits(out: &mut String, digits: &str) {
    for (idx, ch) in digits.chars().enumerate() {
        if idx > 0 && (digits.len() - idx) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
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
