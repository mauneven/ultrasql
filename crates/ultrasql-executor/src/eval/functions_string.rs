//! Numeric/math and string scalar builtins.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_abs(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "abs: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        // Preserve the argument's integer width so the produced value
        // type matches the planner-declared type (and PostgreSQL, which
        // keeps `abs(int4)` as `integer`). `i16::MIN`/`i32::MIN` have no
        // representable absolute value, hence the checked variants.
        Value::Int16(v) => v.checked_abs().map(Value::Int16).ok_or(EvalError::Overflow),
        Value::Int32(v) => v.checked_abs().map(Value::Int32).ok_or(EvalError::Overflow),
        Value::Int64(v) => v.checked_abs().map(Value::Int64).ok_or(EvalError::Overflow),
        Value::Float32(v) => Ok(Value::Float32(v.abs())),
        Value::Float64(v) => Ok(Value::Float64(v.abs())),
        Value::Decimal { value, scale } => Ok(Value::Decimal {
            value: value.checked_abs().ok_or(EvalError::Overflow)?,
            scale: *scale,
        }),
        Value::Money(c) => Ok(Value::Money(c.checked_abs().ok_or(EvalError::Overflow)?)),
        Value::Null => Ok(Value::Null),
        other => Err(EvalError::Type(format!(
            "abs: numeric argument required, got {:?}",
            other.data_type()
        ))),
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TextCase {
    Lower,
    Upper,
}

pub(crate) fn eval_text_case(args: &[Value], mode: TextCase) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "text case function: expected 1 arg, got {}",
            args.len()
        )));
    }
    let (Value::Text(s) | Value::Char(s)) = &args[0] else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "text case function: argument must be text, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let out = match mode {
        TextCase::Lower => s.to_lowercase(),
        TextCase::Upper => s.to_uppercase(),
    };
    Ok(Value::Text(out))
}

pub(crate) fn text_arg<'a>(
    func: &str,
    args: &'a [Value],
    idx: usize,
) -> Result<Option<&'a str>, EvalError> {
    match args.get(idx) {
        Some(Value::Text(text) | Value::Char(text)) => Ok(Some(text.as_str())),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(EvalError::Type(format!(
            "{func}: argument {} must be text, got {:?}",
            idx + 1,
            other.data_type()
        ))),
        None => Err(EvalError::Type(format!(
            "{func}: missing argument {}",
            idx + 1
        ))),
    }
}

pub(crate) fn int_arg(func: &str, args: &[Value], idx: usize) -> Result<Option<i64>, EvalError> {
    match args.get(idx) {
        Some(value) => match value.as_i64() {
            Some(v) => Ok(Some(v)),
            None if matches!(value, Value::Null) => Ok(None),
            None => Err(EvalError::Type(format!(
                "{func}: argument {} must be integer, got {:?}",
                idx + 1,
                value.data_type()
            ))),
        },
        None => Err(EvalError::Type(format!(
            "{func}: missing argument {}",
            idx + 1
        ))),
    }
}

pub(crate) fn numeric_arg(
    func: &str,
    args: &[Value],
    idx: usize,
) -> Result<Option<f64>, EvalError> {
    match args.get(idx) {
        Some(Value::Float32(v)) => Ok(Some(f64::from(*v))),
        Some(Value::Float64(v)) => Ok(Some(*v)),
        Some(Value::Decimal { value, scale }) => {
            let base = value.to_f64().ok_or(EvalError::Overflow)?;
            Ok(Some(base / 10_f64.powi(*scale)))
        }
        Some(value) => match value.as_i64() {
            Some(v) => v.to_f64().map(Some).ok_or(EvalError::Overflow),
            None if matches!(value, Value::Null) => Ok(None),
            None => Err(EvalError::Type(format!(
                "{func}: argument {} must be numeric, got {:?}",
                idx + 1,
                value.data_type()
            ))),
        },
        None => Err(EvalError::Type(format!(
            "{func}: missing argument {}",
            idx + 1
        ))),
    }
}

pub(crate) fn eval_numeric_unary(
    args: &[Value],
    func: &str,
    op: impl FnOnce(f64) -> f64,
) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "{func}: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(value) = numeric_arg(func, args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Float64(op(value)))
}

/// The four PostgreSQL rounding-family functions, used to pick the right
/// rounding rule per input type.
#[derive(Clone, Copy)]
pub(crate) enum RoundMode {
    /// `round` — ties to even for `double precision` (PG uses `rint`),
    /// ties away from zero for `numeric`.
    Round,
    /// `floor` — toward negative infinity.
    Floor,
    /// `ceil`/`ceiling` — toward positive infinity.
    Ceil,
    /// `trunc` — toward zero.
    Trunc,
}

impl RoundMode {
    /// Apply this rounding to an `f64` (the `double precision` path). PG's
    /// `dround` uses round-half-to-even (`rint`), which differs from Rust's
    /// `f64::round` (half away from zero); ceil/floor/trunc are directional
    /// and tie-free.
    fn apply_f64(self, x: f64) -> f64 {
        match self {
            Self::Round => x.round_ties_even(),
            Self::Floor => x.floor(),
            Self::Ceil => x.ceil(),
            Self::Trunc => x.trunc(),
        }
    }

    /// Apply this rounding to a scaled-integer `Decimal` (the `numeric`
    /// path), producing the integer result. PG's `numeric` rounding is
    /// half **away from zero** (not banker's); floor/ceil/trunc are
    /// directional.
    fn apply_decimal(self, value: i128, scale: i32) -> Result<i128, EvalError> {
        if scale <= 0 {
            // Already an integer (or negative scale, which we do not mint).
            return Ok(value);
        }
        let divisor = 10_i128.checked_pow(u32::try_from(scale).map_err(|_| EvalError::Overflow)?);
        let Some(divisor) = divisor else {
            return Err(EvalError::Overflow);
        };
        let quotient = value / divisor;
        let remainder = value % divisor;
        if remainder == 0 {
            return Ok(quotient);
        }
        let result = match self {
            Self::Trunc => quotient,
            Self::Floor => {
                if value < 0 {
                    quotient - 1
                } else {
                    quotient
                }
            }
            Self::Ceil => {
                if value > 0 {
                    quotient + 1
                } else {
                    quotient
                }
            }
            Self::Round => {
                // Half away from zero: compare 2*|remainder| against divisor.
                let twice = remainder
                    .checked_abs()
                    .and_then(|r| r.checked_mul(2))
                    .ok_or(EvalError::Overflow)?;
                if twice >= divisor {
                    if value < 0 {
                        quotient - 1
                    } else {
                        quotient + 1
                    }
                } else {
                    quotient
                }
            }
        };
        Ok(result)
    }
}

/// `round`/`floor`/`ceil`/`trunc` with PostgreSQL's result-type matrix:
/// `numeric -> numeric`, `double precision -> double precision`, and an
/// integer argument casts to `numeric` (PG's preferred cast), so it also
/// yields `numeric`. The produced value type matches the planner's
/// `round_family_return_type`.
pub(crate) fn eval_round_family(
    args: &[Value],
    func: &str,
    mode: RoundMode,
) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "{func}: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        // `double precision` stays `double precision`.
        Value::Float32(v) => Ok(Value::Float64(mode.apply_f64(f64::from(*v)))),
        Value::Float64(v) => Ok(Value::Float64(mode.apply_f64(*v))),
        // `numeric` stays `numeric`; result is an integer-valued numeric.
        Value::Decimal { value, scale } => Ok(Value::Decimal {
            value: mode.apply_decimal(*value, *scale)?,
            scale: 0,
        }),
        // Integer arguments cast to `numeric` in PG, so return `numeric`.
        other => match other.as_i64() {
            Some(v) => Ok(Value::Decimal {
                value: i128::from(v),
                scale: 0,
            }),
            None => Err(EvalError::Type(format!(
                "{func}: argument must be numeric, got {:?}",
                other.data_type()
            ))),
        },
    }
}

pub(crate) fn eval_numeric_binary(
    args: &[Value],
    func: &str,
    op: impl FnOnce(f64, f64) -> f64,
) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "{func}: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(left) = numeric_arg(func, args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(right) = numeric_arg(func, args, 1)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Float64(op(left, right)))
}

/// `log(x)` returns base-10 logarithm; `log(b, x)` returns the logarithm
/// of `x` to base `b` (computed as `ln(x) / ln(b)`). This mirrors
/// PostgreSQL, where `log` is overloaded on arity. Both forms return
/// `double precision` here (numeric-typing of `log` is out of scope), so
/// the produced `Float64` agrees with the planner's declared `Float64`.
pub(crate) fn eval_log(args: &[Value]) -> Result<Value, EvalError> {
    match args.len() {
        1 => eval_numeric_unary(args, "log", f64::log10),
        2 => {
            let Some(base) = numeric_arg("log", args, 0)? else {
                return Ok(Value::Null);
            };
            let Some(x) = numeric_arg("log", args, 1)? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(x.ln() / base.ln()))
        }
        n => Err(EvalError::Type(format!(
            "log: expected 1 or 2 args, got {n}"
        ))),
    }
}

/// `mod(a, b)` with an integer fast path.
///
/// When both arguments are integers, the remainder is computed exactly
/// on `i64` (avoiding the precision loss of a round-trip through `f64`,
/// e.g. `mod(9007199254740993, 2)` must be `1`, not `0`) and returned in
/// the wider of the two integer widths. This matches the planner's
/// declared type (`mod_return_type`) so the produced value type and the
/// declared type agree. Any other numeric combination falls back to the
/// `f64` path and returns `Float64`.
pub(crate) fn eval_mod(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "mod: expected 2 args, got {}",
            args.len()
        )));
    }
    /// Decompose an integer value into `(i64 value, width rank)`; `None`
    /// for any non-integer value (NULL, float, decimal, ...).
    fn as_int(v: &Value) -> Option<(i64, u8)> {
        match v {
            Value::Int16(x) => Some((i64::from(*x), 0)),
            Value::Int32(x) => Some((i64::from(*x), 1)),
            Value::Int64(x) => Some((*x, 2)),
            _ => None,
        }
    }

    match (as_int(&args[0]), as_int(&args[1])) {
        (Some((left, lrank)), Some((right, rrank))) => {
            if right == 0 {
                return Err(EvalError::DivByZero);
            }
            // `checked_rem` only fails here on `i64::MIN % -1`, whose true
            // remainder is 0; substitute it directly.
            let rem = left.checked_rem(right).unwrap_or(0);
            // Return the wider integer width. The remainder always fits
            // in the narrower operand's range, so it fits in the wider.
            Ok(match lrank.max(rrank) {
                0 => Value::Int16(i16::try_from(rem).map_err(|_| EvalError::Overflow)?),
                1 => Value::Int32(i32::try_from(rem).map_err(|_| EvalError::Overflow)?),
                _ => Value::Int64(rem),
            })
        }
        // NULL or non-integer numeric: route through the f64 path.
        _ => eval_numeric_binary(args, "mod", |left, right| left % right),
    }
}

pub(crate) fn eval_pi(args: &[Value]) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "pi: expected 0 args, got {}",
            args.len()
        )));
    }
    Ok(Value::Float64(std::f64::consts::PI))
}

pub(crate) fn eval_random(args: &[Value]) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "random: expected 0 args, got {}",
            args.len()
        )));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let low = u64::try_from(now & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(now >> 64).unwrap_or(0);
    let mut state =
        low ^ high.rotate_left(11) ^ UUID_FALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed);
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    let mantissa = state & ((1_u64 << 53) - 1);
    let numerator = mantissa.to_f64().ok_or(EvalError::Overflow)?;
    Ok(Value::Float64(numerator / 9_007_199_254_740_992.0))
}

pub(crate) fn eval_length(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "length: expected 1 arg, got {}",
            args.len()
        )));
    }
    let len = match &args[0] {
        Value::Text(text) => text.chars().count(),
        Value::Char(text) => bpchar_semantic_text(text).chars().count(),
        Value::BitString(bits) => usize::try_from(bits.len())
            .map_err(|_| EvalError::Type("length: result overflow".to_owned()))?,
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "length: argument 1 must be text or bit string, got {:?}",
                other.data_type()
            )));
        }
    };
    let len =
        i32::try_from(len).map_err(|_| EvalError::Type("length: result overflow".to_owned()))?;
    Ok(Value::Int32(len))
}

pub(crate) fn eval_bit_length(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "bit_length: expected 1 arg, got {}",
            args.len()
        )));
    }
    // PostgreSQL: `bit_length(text)` is the byte length times 8;
    // `bit_length(bit)` is the exact bit count of the bit string.
    let len = match &args[0] {
        Value::Text(s) => s.len().checked_mul(8),
        Value::Char(s) => bpchar_semantic_text(s).len().checked_mul(8),
        Value::BitString(bits) => usize::try_from(bits.len()).ok(),
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "bit_length: argument 1 must be text or bit string, got {:?}",
                other.data_type()
            )));
        }
    };
    let len = len
        .and_then(|len| i32::try_from(len).ok())
        .ok_or_else(|| EvalError::Type("bit_length: result overflow".to_owned()))?;
    Ok(Value::Int32(len))
}

pub(crate) fn eval_octet_length(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "octet_length: expected 1 arg, got {}",
            args.len()
        )));
    }
    // PostgreSQL: `octet_length(text)` is the number of bytes in the UTF-8
    // encoding (so multibyte characters count more than once);
    // `octet_length(bit)` is the byte length of the bit string.
    let len = match &args[0] {
        Value::Text(s) => s.len(),
        Value::Char(s) => bpchar_semantic_text(s).len(),
        Value::BitString(bits) => bits.octet_len() as usize,
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "octet_length: argument 1 must be text or bit string, got {:?}",
                other.data_type()
            )));
        }
    };
    let len = i32::try_from(len)
        .map_err(|_| EvalError::Type("octet_length: result overflow".to_owned()))?;
    Ok(Value::Int32(len))
}

pub(crate) fn eval_bit_count(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "bit_count: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(bits) = bit_string_arg("bit_count", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int64(i64::from(bits.bit_count())))
}

pub(crate) fn eval_get_bit(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "get_bit: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(bits) = bit_string_arg("get_bit", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(idx) = integer_arg_as_usize("get_bit", args, 1)? else {
        return Ok(Value::Null);
    };
    let bit = bits
        .bit(idx)
        .ok_or_else(|| EvalError::Type("get_bit: bit index out of range".to_owned()))?;
    Ok(Value::Int32(if bit { 1 } else { 0 }))
}

pub(crate) fn eval_set_bit(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "set_bit: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(bits) = bit_string_arg("set_bit", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(idx) = integer_arg_as_usize("set_bit", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(value) = integer_arg_as_usize("set_bit", args, 2)? else {
        return Ok(Value::Null);
    };
    if value > 1 {
        return Err(EvalError::Type(
            "set_bit: new value must be 0 or 1".to_owned(),
        ));
    }
    bits.set_bit(idx, value == 1)
        .map(Value::BitString)
        .ok_or_else(|| EvalError::Type("set_bit: bit index out of range".to_owned()))
}

pub(crate) fn eval_trim(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "trim: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("trim", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text.trim().to_owned()))
}

#[derive(Clone, Copy)]
pub(crate) enum PadSide {
    Left,
    Right,
}

pub(crate) fn eval_pad(args: &[Value], side: PadSide) -> Result<Value, EvalError> {
    let func = match side {
        PadSide::Left => "lpad",
        PadSide::Right => "rpad",
    };
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "{func}: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg(func, args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(target_len) = int_arg(func, args, 1)? else {
        return Ok(Value::Null);
    };
    let fill = if args.len() == 3 {
        let Some(fill) = text_arg(func, args, 2)? else {
            return Ok(Value::Null);
        };
        fill
    } else {
        " "
    };
    let target = generated_text_target_len(func, target_len)?;
    let current = text.chars().count();
    if target <= current {
        return Ok(Value::Text(text.chars().take(target).collect()));
    }
    if fill.is_empty() {
        return Err(EvalError::Type(format!(
            "{func}: fill string cannot be empty"
        )));
    }
    let pad_needed = target - current;
    let mut padding = String::new();
    for ch in fill.chars().cycle().take(pad_needed) {
        padding.push(ch);
    }
    let out = match side {
        PadSide::Left => format!("{padding}{text}"),
        PadSide::Right => format!("{text}{padding}"),
    };
    Ok(Value::Text(out))
}

pub(crate) fn eval_left(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "left: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("left", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(count) = int_arg("left", args, 1)? else {
        return Ok(Value::Null);
    };
    let chars: Vec<char> = text.chars().collect();
    let keep = if count >= 0 {
        usize::try_from(count)
            .unwrap_or(usize::MAX)
            .min(chars.len())
    } else {
        chars.len().saturating_sub(i64_abs_to_usize(count))
    };
    Ok(Value::Text(chars.into_iter().take(keep).collect()))
}

pub(crate) fn eval_right(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "right: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("right", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(count) = int_arg("right", args, 1)? else {
        return Ok(Value::Null);
    };
    let chars: Vec<char> = text.chars().collect();
    let skip = if count >= 0 {
        chars
            .len()
            .saturating_sub(usize::try_from(count).unwrap_or(usize::MAX))
    } else {
        i64_abs_to_usize(count).min(chars.len())
    };
    Ok(Value::Text(chars.into_iter().skip(skip).collect()))
}

pub(crate) fn i64_abs_to_usize(value: i64) -> usize {
    usize::try_from(value.unsigned_abs()).unwrap_or(usize::MAX)
}

pub(crate) fn generated_text_target_len(func: &str, len: i64) -> Result<usize, EvalError> {
    if len <= 0 {
        return Ok(0);
    }
    let len = usize::try_from(len)
        .map_err(|_| EvalError::Type(format!("{func}: output length exceeds supported maximum")))?;
    if len > MAX_EVAL_GENERATED_TEXT_CHARS {
        return Err(EvalError::Type(format!(
            "{func}: output length exceeds supported maximum"
        )));
    }
    Ok(len)
}

pub(crate) fn generated_text_repeat_count(
    func: &str,
    text: &str,
    count: i64,
) -> Result<usize, EvalError> {
    if count <= 0 {
        return Ok(0);
    }
    let count = usize::try_from(count)
        .map_err(|_| EvalError::Type(format!("{func}: output length exceeds supported maximum")))?;
    let chars = text.chars().count();
    let output_chars = chars.checked_mul(count).ok_or_else(|| {
        EvalError::Type(format!("{func}: output length exceeds supported maximum"))
    })?;
    if output_chars > MAX_EVAL_GENERATED_TEXT_CHARS {
        return Err(EvalError::Type(format!(
            "{func}: output length exceeds supported maximum"
        )));
    }
    Ok(count)
}

pub(crate) fn eval_position(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "position: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(needle) = text_arg("position", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(haystack) = text_arg("position", args, 1)? else {
        return Ok(Value::Null);
    };
    let pos = haystack.find(needle).map_or(0_i32, |byte_idx| {
        let chars_before = haystack[..byte_idx].chars().count();
        i32::try_from(chars_before.saturating_add(1)).unwrap_or(i32::MAX)
    });
    Ok(Value::Int32(pos))
}

pub(crate) fn eval_replace(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "replace: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("replace", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(from) = text_arg("replace", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(to) = text_arg("replace", args, 2)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text.replace(from, to)))
}

pub(crate) fn eval_split_part(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "split_part: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("split_part", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(delimiter) = text_arg("split_part", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(field) = int_arg("split_part", args, 2)? else {
        return Ok(Value::Null);
    };
    if field <= 0 {
        return Err(EvalError::Type(
            "split_part: field position must be greater than zero".to_owned(),
        ));
    }
    // PostgreSQL treats an empty delimiter as "no split": field 1 is the
    // whole string and every other field is empty. Rust's
    // `"abc".split("")` instead yields `["", "a", "b", "c", ""]`, so we
    // special-case it to match PG.
    if delimiter.is_empty() {
        let result = if field == 1 { text } else { "" };
        return Ok(Value::Text(result.to_owned()));
    }
    let target = usize::try_from(field.saturating_sub(1)).unwrap_or(usize::MAX);
    Ok(Value::Text(
        text.split(delimiter).nth(target).unwrap_or("").to_owned(),
    ))
}

pub(crate) fn eval_concat(args: &[Value]) -> Result<Value, EvalError> {
    let mut out = String::new();
    for arg in args {
        if !matches!(arg, Value::Null) {
            out.push_str(&value_to_pg_output_text(arg));
        }
    }
    Ok(Value::Text(out))
}

pub(crate) fn eval_concat_ws(args: &[Value]) -> Result<Value, EvalError> {
    if args.is_empty() {
        return Err(EvalError::Type(
            "concat_ws: expected at least 1 arg".to_owned(),
        ));
    }
    let Some(separator) = text_arg("concat_ws", args, 0)? else {
        return Ok(Value::Null);
    };
    let mut parts = Vec::new();
    for arg in &args[1..] {
        if !matches!(arg, Value::Null) {
            parts.push(value_to_pg_output_text(arg));
        }
    }
    Ok(Value::Text(parts.join(separator)))
}

pub(crate) fn eval_repeat(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "repeat: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("repeat", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(count) = int_arg("repeat", args, 1)? else {
        return Ok(Value::Null);
    };
    if text.is_empty() {
        return Ok(Value::Text(String::new()));
    }
    let count = generated_text_repeat_count("repeat", text, count)?;
    Ok(Value::Text(text.repeat(count)))
}

pub(crate) fn eval_reverse(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "reverse: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("reverse", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text.chars().rev().collect()))
}

pub(crate) fn eval_md5(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "md5: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("md5", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(format!("{:x}", md5::compute(text.as_bytes()))))
}

pub(crate) fn eval_sha256(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "sha256: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("sha256", args, 0)? else {
        return Ok(Value::Null);
    };
    use sha2::Digest;
    let digest = sha2::Sha256::digest(text.as_bytes());
    let mut out = String::with_capacity(digest.len().saturating_mul(2));
    for byte in digest {
        write!(&mut out, "{byte:02x}")
            .map_err(|_| EvalError::Type("sha256: hex encoding failed".to_owned()))?;
    }
    Ok(Value::Text(out))
}

pub(crate) fn eval_quote_ident(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "quote_ident: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("quote_ident", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(quote_identifier(text)))
}

pub(crate) fn eval_quote_literal(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "quote_literal: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("quote_literal", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(quote_literal(text)))
}
