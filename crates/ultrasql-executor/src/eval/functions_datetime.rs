//! Case expressions and date/time scalar builtins.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_pg_get_userbyid(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_get_userbyid: expected 1 arg, got {}",
            args.len()
        )));
    }
    let oid = match &args[0] {
        Value::Int16(v) => i64::from(*v),
        Value::Int32(v) => i64::from(*v),
        Value::Int64(v) => *v,
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "pg_get_userbyid: oid must be integer, got {:?}",
                other.data_type()
            )));
        }
    };
    let name = if oid == 10 {
        "ultrasql".to_owned()
    } else {
        format!("unknown (OID={oid})")
    };
    Ok(Value::Text(name))
}

/// `CASE WHEN c1 THEN v1 … ELSE e END` — args layout:
/// `[c1, v1, c2, v2, …, else]`.
pub(crate) fn eval_case_searched(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(EvalError::Type(
            "case_searched: expected odd arg count (cond, then pairs + else)".into(),
        ));
    }
    let else_val = args.last().cloned().unwrap_or(Value::Null);
    let mut i = 0;
    while i + 1 < args.len() - 1 {
        match &args[i] {
            Value::Bool(true) => return Ok(args[i + 1].clone()),
            Value::Bool(false) | Value::Null => {}
            other => {
                return Err(EvalError::Type(format!(
                    "case_searched: WHEN clause must yield bool, got {:?}",
                    other.data_type()
                )));
            }
        }
        i += 2;
    }
    Ok(else_val)
}

/// `CASE op WHEN w1 THEN v1 … ELSE e END` — args layout:
/// `[op, w1, v1, w2, v2, …, else]`.
pub(crate) fn eval_case_simple(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() < 4 || args.len() % 2 != 0 {
        return Err(EvalError::Type(
            "case_simple: expected even arg count ≥ 4 (operand + pairs + else)".into(),
        ));
    }
    let op = &args[0];
    let else_val = args.last().cloned().unwrap_or(Value::Null);
    let mut i = 1;
    while i + 1 < args.len() - 1 {
        if values_equal_for_case(op, &args[i]) {
            return Ok(args[i + 1].clone());
        }
        i += 2;
    }
    Ok(else_val)
}

pub(crate) fn eval_now(args: &[Value], return_type: &DataType) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "now: expected 0 args, got {}",
            args.len()
        )));
    }
    let micros = transaction_start_timestamp_micros();
    if matches!(return_type, DataType::Timestamp) {
        Ok(Value::Timestamp(micros))
    } else {
        Ok(Value::TimestampTz(micros))
    }
}

pub(crate) fn eval_current_date(args: &[Value]) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "current_date: expected 0 args, got {}",
            args.len()
        )));
    }
    let days = transaction_start_timestamp_micros().div_euclid(MICROS_PER_DAY);
    Ok(Value::Date(i32::try_from(days).unwrap_or(i32::MAX)))
}

pub(crate) fn eval_to_timestamp(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "to_timestamp: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(seconds) = numeric_arg("to_timestamp", args, 0)? else {
        return Ok(Value::Null);
    };
    let unix_micros = (seconds * 1_000_000.0).round();
    let min_micros = i64::MIN.to_f64().ok_or(EvalError::Overflow)?;
    let max_micros = i64::MAX.to_f64().ok_or(EvalError::Overflow)?;
    if !unix_micros.is_finite() || unix_micros < min_micros || unix_micros > max_micros {
        return Err(EvalError::Type(
            "to_timestamp: timestamp overflow".to_owned(),
        ));
    }
    let unix_micros_text = format!("{unix_micros:.0}");
    let unix_micros = unix_micros_text
        .parse::<i64>()
        .map_err(|_| EvalError::Type("to_timestamp: timestamp overflow".to_owned()))?;
    Ok(Value::TimestampTz(
        unix_micros
            .checked_sub(UNIX_TO_ENGINE_EPOCH_MICROS)
            .ok_or_else(|| EvalError::Type("to_timestamp: timestamp overflow".to_owned()))?,
    ))
}

pub(crate) fn eval_make_date(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "make_date: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(year) = int_arg("make_date", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(month) = int_arg("make_date", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(day) = int_arg("make_date", args, 2)? else {
        return Ok(Value::Null);
    };
    let year = i32::try_from(year)
        .map_err(|_| EvalError::Type("make_date: year out of range".to_owned()))?;
    let month = u32::try_from(month)
        .map_err(|_| EvalError::Type("make_date: month out of range".to_owned()))?;
    let day = u32::try_from(day)
        .map_err(|_| EvalError::Type("make_date: day out of range".to_owned()))?;
    if !(1..=12).contains(&month) || !(1..=days_in_month(year, month)).contains(&day) {
        return Err(EvalError::Type("make_date: invalid date".to_owned()));
    }
    Ok(Value::Date(days_from_civil(year, month, day)?))
}

pub(crate) fn values_equal_for_case(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => false,
        _ => a == b,
    }
}

pub(crate) fn eval_extract(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "extract: expected 2 args, got {}",
            args.len()
        )));
    }
    let unit = match &args[0] {
        Value::Text(s) => s.as_str(),
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "extract: unit must be text, got {:?}",
                other.data_type()
            )));
        }
    };
    if matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let unit_norm = unit.to_ascii_lowercase();
    let out_i64 = extract_datetime_part(&unit_norm, &args[1])?;
    Ok(Value::Int64(out_i64))
}

pub(crate) fn extract_datetime_part(unit: &str, source: &Value) -> Result<i64, EvalError> {
    match source {
        Value::Date(days) => {
            let (year, month, day) = civil_from_days(*days);
            match unit {
                "year" => Ok(i64::from(year)),
                "month" => Ok(i64::from(month)),
                "day" => Ok(i64::from(day)),
                "quarter" => Ok(i64::from((month - 1) / 3 + 1)),
                "epoch" => Ok(date_as_timestamp(*days)?
                    .checked_add(UNIX_TO_ENGINE_EPOCH_MICROS)
                    .ok_or_else(|| EvalError::Type("extract: epoch overflow".to_owned()))?
                    / 1_000_000),
                other => Err(EvalError::Type(format!(
                    "extract: unit `{other}` not implemented"
                ))),
            }
        }
        Value::Timestamp(us) | Value::TimestampTz(us) => extract_timestamp_part(unit, *us),
        Value::Time(us) => extract_time_part(unit, *us),
        Value::TimeTz { micros, .. } => extract_time_part(unit, *micros),
        Value::Interval {
            months,
            days,
            microseconds,
        } => extract_interval_part(unit, *months, *days, *microseconds),
        Value::Null => Err(EvalError::Type("extract: null source".to_owned())),
        other => Err(EvalError::Type(format!(
            "extract: source must be date/time/timestamp/interval, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn extract_timestamp_part(unit: &str, micros: i64) -> Result<i64, EvalError> {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let days_i32 = i32::try_from(days).unwrap_or(i32::MAX);
    let (year, month, day) = civil_from_days(days_i32);
    match unit {
        "year" => Ok(i64::from(year)),
        "month" => Ok(i64::from(month)),
        "day" => Ok(i64::from(day)),
        "quarter" => Ok(i64::from((month - 1) / 3 + 1)),
        "hour" => Ok(time / 3_600_000_000),
        "minute" => Ok(time % 3_600_000_000 / 60_000_000),
        "second" => Ok(time % 60_000_000 / 1_000_000),
        "epoch" => Ok(micros
            .checked_add(UNIX_TO_ENGINE_EPOCH_MICROS)
            .ok_or_else(|| EvalError::Type("extract: epoch overflow".to_owned()))?
            / 1_000_000),
        other => Err(EvalError::Type(format!(
            "extract: unit `{other}` not implemented"
        ))),
    }
}

pub(crate) fn extract_time_part(unit: &str, micros: i64) -> Result<i64, EvalError> {
    let time = micros.rem_euclid(MICROS_PER_DAY);
    match unit {
        "hour" => Ok(time / 3_600_000_000),
        "minute" => Ok(time % 3_600_000_000 / 60_000_000),
        "second" => Ok(time % 60_000_000 / 1_000_000),
        other => Err(EvalError::Type(format!(
            "extract: unit `{other}` not implemented"
        ))),
    }
}

pub(crate) fn extract_interval_part(
    unit: &str,
    months: i32,
    days: i32,
    microseconds: i64,
) -> Result<i64, EvalError> {
    match unit {
        "year" => Ok(i64::from(months / 12)),
        "month" => Ok(i64::from(months % 12)),
        "day" => Ok(i64::from(days)),
        "hour" => Ok(microseconds / 3_600_000_000),
        "minute" => Ok(microseconds % 3_600_000_000 / 60_000_000),
        "second" => Ok(microseconds % 60_000_000 / 1_000_000),
        "epoch" => Ok(i64::from(days)
            .checked_mul(86_400)
            .and_then(|base| base.checked_add(microseconds / 1_000_000))
            .ok_or_else(|| EvalError::Type("extract: interval epoch overflow".to_owned()))?),
        other => Err(EvalError::Type(format!(
            "extract: unit `{other}` not implemented"
        ))),
    }
}

pub(crate) fn eval_date_trunc(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "date_trunc: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(unit) = text_arg("date_trunc", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(source) = timestamp_micros_arg("date_trunc", &args[1])? else {
        return Ok(Value::Null);
    };
    let unit = unit.to_ascii_lowercase();
    let truncated = truncate_timestamp(&unit, source)?;
    Ok(Value::TimestampTz(truncated))
}

pub(crate) fn eval_age(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 1 || args.len() == 2) {
        return Err(EvalError::Type(format!(
            "age: expected 1 or 2 args, got {}",
            args.len()
        )));
    }
    let end = if args.len() == 2 {
        timestamp_micros_arg("age", &args[0])?
    } else {
        Some(current_engine_timestamp_micros())
    };
    let Some(end) = end else {
        return Ok(Value::Null);
    };
    let Some(start) = timestamp_micros_arg("age", &args[args.len() - 1])? else {
        return Ok(Value::Null);
    };
    let delta = end
        .checked_sub(start)
        .ok_or_else(|| EvalError::Type("age: interval overflow".to_owned()))?;
    Ok(Value::Interval {
        months: 0,
        days: i32::try_from(delta.div_euclid(MICROS_PER_DAY)).unwrap_or(i32::MAX),
        microseconds: delta.rem_euclid(MICROS_PER_DAY),
    })
}

pub(crate) fn eval_timezone(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "timezone: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(zone) = text_arg("timezone", args, 0)? else {
        return Ok(Value::Null);
    };
    match &args[1] {
        Value::Timestamp(micros) => timestamp_micros_at_timezone(*micros, zone)
            .map(Value::TimestampTz)
            .ok_or_else(|| EvalError::Type("timezone: invalid timezone conversion".to_owned())),
        Value::TimestampTz(micros) => timestamptz_display_in_timezone(*micros, zone)
            .map(|display| Value::Timestamp(display.local_micros))
            .ok_or_else(|| EvalError::Type("timezone: invalid timezone conversion".to_owned())),
        Value::TimeTz {
            micros,
            offset_seconds,
        } => timetz_at_timezone(*micros, *offset_seconds, zone)
            .map(|(micros, offset_seconds)| Value::TimeTz {
                micros,
                offset_seconds,
            })
            .ok_or_else(|| EvalError::Type("timezone: invalid timezone conversion".to_owned())),
        Value::Null => Ok(Value::Null),
        other => Err(EvalError::Type(format!(
            "timezone: argument 2 must be timestamp, timestamptz, or timetz, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_date_bin(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "date_bin: expected 3 args, got {}",
            args.len()
        )));
    }
    let stride = match &args[0] {
        Value::Interval {
            months,
            days,
            microseconds,
        } => {
            if *months != 0 {
                return Err(EvalError::Type(
                    "date_bin: month stride is not supported".to_owned(),
                ));
            }
            i64::from(*days)
                .checked_mul(MICROS_PER_DAY)
                .and_then(|base| base.checked_add(*microseconds))
                .ok_or_else(|| EvalError::Type("date_bin: stride overflow".to_owned()))?
        }
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "date_bin: stride must be interval, got {:?}",
                other.data_type()
            )));
        }
    };
    if stride <= 0 {
        return Err(EvalError::Type(
            "date_bin: stride must be positive".to_owned(),
        ));
    }
    let Some(source) = timestamp_micros_arg("date_bin", &args[1])? else {
        return Ok(Value::Null);
    };
    let Some(origin) = timestamp_micros_arg("date_bin", &args[2])? else {
        return Ok(Value::Null);
    };
    let offset = source
        .checked_sub(origin)
        .ok_or_else(|| EvalError::Type("date_bin: timestamp overflow".to_owned()))?;
    let bucket_offset = offset
        .div_euclid(stride)
        .checked_mul(stride)
        .ok_or_else(|| EvalError::Type("date_bin: timestamp overflow".to_owned()))?;
    Ok(Value::TimestampTz(
        origin
            .checked_add(bucket_offset)
            .ok_or_else(|| EvalError::Type("date_bin: timestamp overflow".to_owned()))?,
    ))
}

pub(crate) fn timestamp_micros_arg(func: &str, value: &Value) -> Result<Option<i64>, EvalError> {
    match value {
        Value::Timestamp(us) | Value::TimestampTz(us) => Ok(Some(*us)),
        Value::Date(days) => date_as_timestamp(*days).map(Some),
        Value::Null => Ok(None),
        other => Err(EvalError::Type(format!(
            "{func}: argument must be date/timestamp, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn truncate_timestamp(unit: &str, micros: i64) -> Result<i64, EvalError> {
    match unit {
        "second" => Ok(micros.div_euclid(1_000_000) * 1_000_000),
        "minute" => Ok(micros.div_euclid(60_000_000) * 60_000_000),
        "hour" => Ok(micros.div_euclid(3_600_000_000) * 3_600_000_000),
        "day" => Ok(micros.div_euclid(MICROS_PER_DAY) * MICROS_PER_DAY),
        "month" | "year" => {
            let days = micros.div_euclid(MICROS_PER_DAY);
            let days_i32 = i32::try_from(days).unwrap_or(i32::MAX);
            let (year, month, _) = civil_from_days(days_i32);
            let truncated_days = if unit == "year" {
                days_from_civil(year, 1, 1)?
            } else {
                days_from_civil(year, u32::try_from(month).unwrap_or(1), 1)?
            };
            date_as_timestamp(truncated_days)
        }
        other => Err(EvalError::Type(format!(
            "date_trunc: unit `{other}` not implemented"
        ))),
    }
}

/// Inverse of the Howard-Hinnant `days_from_civil` algorithm, rebased
/// on the 2000-01-01 epoch the engine uses. Returns `(year, month, day)`
/// in the standard 1-based calendar.
pub(crate) fn civil_from_days(days_since_2000_01_01: i32) -> (i32, i32, i32) {
    let z = days_since_2000_01_01 + 10_957; // rebase to 1970-01-01
    let z = z + 719_468; // shift to year 0
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let final_y = if m <= 2 { y + 1 } else { y };
    (final_y, m, d)
}

pub(crate) fn current_engine_timestamp_micros() -> i64 {
    let unix_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or(0);
    let unix_micros = i64::try_from(unix_micros).unwrap_or(i64::MAX);
    unix_micros.saturating_sub(UNIX_TO_ENGINE_EPOCH_MICROS)
}

/// Engine-epoch micros pinned for the transaction-start builtins
/// (`now()` / `current_timestamp` / `current_date`).
///
/// Reads the statement-scoped [`EvalClock`](super::eval_clock) the server
/// installs at statement dispatch so every row of every statement in a
/// transaction observes the same instant (PostgreSQL semantics). Falls back
/// to the live engine clock when no clock is installed — the path taken by
/// constraint-default evaluation, embedded helpers, and unit tests, which
/// preserves the prior live-wall-clock behavior.
fn transaction_start_timestamp_micros() -> i64 {
    super::eval_clock::txn_start_micros().unwrap_or_else(current_engine_timestamp_micros)
}

pub(crate) fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub(crate) fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> Result<i32, EvalError> {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let month_i32 =
        i32::try_from(month).map_err(|_| EvalError::Type("date conversion overflow".to_owned()))?;
    let mp = month_i32 + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5
        + i32::try_from(day).map_err(|_| EvalError::Type("date conversion overflow".to_owned()))?
        - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_since_1970 = era * 146_097 + doe - 719_468;
    days_since_1970
        .checked_sub(i32::try_from(UNIX_TO_ENGINE_EPOCH_DAYS).unwrap_or(10_957))
        .ok_or_else(|| EvalError::Type("date conversion overflow".to_owned()))
}

pub(crate) fn eval_substring(args: &[Value]) -> Result<Value, EvalError> {
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "substring: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Some(s) = text_arg("substring", args, 0)? else {
        return Ok(Value::Null);
    };
    let from = match args[1].as_i64() {
        Some(v) => v,
        None if matches!(args[1], Value::Null) => return Ok(Value::Null),
        _ => {
            return Err(EvalError::Type("substring: `from` must be integer".into()));
        }
    };
    // SQL substring is 1-based and counts text characters, not UTF-8 bytes.
    let char_count = s.chars().count();
    let start_char_signed = from.saturating_sub(1);
    let start = if start_char_signed < 0 {
        0
    } else {
        usize::try_from(start_char_signed)
            .unwrap_or(char_count)
            .min(char_count)
    };
    let end = if args.len() == 3 {
        let len = match args[2].as_i64() {
            Some(v) => v,
            None if matches!(args[2], Value::Null) => return Ok(Value::Null),
            _ => {
                return Err(EvalError::Type(
                    "substring: `for` length must be integer".into(),
                ));
            }
        };
        let len = len.max(0);
        let mut effective = usize::try_from(len).unwrap_or(0);
        if start_char_signed < 0 {
            let abs_back = usize::try_from(start_char_signed.unsigned_abs()).unwrap_or(usize::MAX);
            effective = effective.saturating_sub(abs_back);
        }
        start.saturating_add(effective).min(char_count)
    } else {
        char_count
    };
    let start_byte = byte_index_for_char(s, start);
    let end_byte = byte_index_for_char(s, end);
    Ok(Value::Text(s[start_byte..end_byte].to_owned()))
}

pub(crate) fn byte_index_for_char(text: &str, char_index: usize) -> usize {
    if char_index == 0 {
        return 0;
    }
    text.char_indices()
        .nth(char_index)
        .map_or(text.len(), |(byte_index, _)| byte_index)
}
