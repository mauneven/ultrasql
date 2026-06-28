use super::*;
use num_traits::ToPrimitive;

pub(crate) fn parse_date_days(text: &str) -> Option<i32> {
    let (year, month, day) = parse_date_parts(text)?;
    days_from_civil(year, month, day)
}

pub(crate) fn parse_date_parts(text: &str) -> Option<(i32, u32, u32)> {
    let mut parts = text.split('-');
    let year = parts.next()?.parse::<i32>().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((year, month, day))
}

/// Parse PostgreSQL ISO `DATE` text into days since UltraSQL's date epoch.
#[must_use]
pub fn parse_date_text(text: &str) -> Option<i32> {
    parse_date_days(text.trim())
}

pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i32> {
    if day == 0 || day > days_in_month(year, month)? {
        return None;
    }
    let month = i64::from(month);
    let day = i64::from(day);
    let march_year_adjust = if month <= 2 { 1_i64 } else { 0 };
    let y = i64::from(year).checked_sub(march_year_adjust)?;
    let era = if y >= 0 {
        y.div_euclid(400)
    } else {
        y.checked_sub(399)?.div_euclid(400)
    };
    let yoe = y.checked_sub(era.checked_mul(400)?)?;
    let mp = if month > 2 {
        month.checked_sub(3)?
    } else {
        month.checked_add(9)?
    };
    let doy = 153_i64
        .checked_mul(mp)?
        .checked_add(2)?
        .div_euclid(5)
        .checked_add(day)?
        .checked_sub(1)?;
    let leap_days = yoe.div_euclid(4).checked_sub(yoe.div_euclid(100))?;
    let doe = yoe
        .checked_mul(365)?
        .checked_add(leap_days)?
        .checked_add(doy)?;
    let days_since_unix = era
        .checked_mul(146_097)?
        .checked_add(doe)?
        .checked_sub(719_468)?;
    let days_since_ultrasql = days_since_unix.checked_sub(10_957)?;
    i32::try_from(days_since_ultrasql).ok()
}

pub(crate) fn days_in_month(year: i32, month: u32) -> Option<u32> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 if is_leap_year(year) => Some(29),
        2 => Some(28),
        _ => None,
    }
}

pub(crate) fn is_leap_year(year: i32) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

pub(crate) fn format_date(days_since_2000_01_01: i32) -> String {
    let Some((year, month, day)) = civil_from_days(days_since_2000_01_01) else {
        return format!("{days_since_2000_01_01}d");
    };
    format!("{year:04}-{month:02}-{day:02}")
}

/// Format days since UltraSQL's date epoch as PostgreSQL ISO `DATE` text.
#[must_use]
pub fn format_date_days(days_since_2000_01_01: i32) -> String {
    format_date(days_since_2000_01_01)
}

/// Return `(year, month, day)` for days since UltraSQL's date epoch.
#[must_use]
pub fn date_parts_from_days(days_since_2000_01_01: i32) -> Option<(i32, u32, u32)> {
    civil_from_days(days_since_2000_01_01)
}

/// Format `TIME` in PostgreSQL's default ISO style.
#[must_use]
pub fn format_time_micros(micros: i64) -> String {
    if !(0..=MICROS_PER_DAY).contains(&micros) {
        return format!("{micros}us");
    }
    let hour = micros.div_euclid(MICROS_PER_HOUR);
    let rem = micros.rem_euclid(MICROS_PER_HOUR);
    let minute = rem.div_euclid(MICROS_PER_MINUTE);
    let rem = rem.rem_euclid(MICROS_PER_MINUTE);
    let second = rem.div_euclid(MICROS_PER_SECOND);
    let frac = rem.rem_euclid(MICROS_PER_SECOND);
    format_time_parts(hour, minute, second, frac)
}

/// Format `TIMESTAMP WITHOUT TIME ZONE` in PostgreSQL ISO style.
#[must_use]
pub fn format_timestamp_micros(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let Ok(days) = i32::try_from(days) else {
        return format!("{micros}us");
    };
    format!("{} {}", format_date(days), format_time_micros(time))
}

/// Return `(year, month, day, time_micros)` for timestamp micros.
#[must_use]
pub fn timestamp_parts_from_micros(micros: i64) -> Option<(i32, u32, u32, i64)> {
    let days = i32::try_from(micros.div_euclid(MICROS_PER_DAY)).ok()?;
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = date_parts_from_days(days)?;
    Some((year, month, day, time))
}

/// Format `TIMESTAMP WITH TIME ZONE` using UTC display.
#[must_use]
pub fn format_timestamptz_micros_utc(micros: i64) -> String {
    format!("{}+00", format_timestamp_micros(micros))
}

/// Format `TIMESTAMP WITH TIME ZONE` using an explicit display offset.
#[must_use]
pub fn format_timestamptz_micros_with_offset(micros: i64, offset_seconds: i32) -> Option<String> {
    let local_micros =
        micros.checked_add(i64::from(offset_seconds).checked_mul(MICROS_PER_SECOND)?)?;
    Some(format!(
        "{}{}",
        format_timestamp_micros(local_micros),
        format_timezone_offset(offset_seconds)
    ))
}

/// Format `TIMESTAMP WITH TIME ZONE` using a fixed-offset or IANA timezone.
#[must_use]
pub fn format_timestamptz_micros_in_timezone(micros: i64, timezone: &str) -> Option<String> {
    let display = timestamptz_display_in_timezone(micros, timezone)?;
    format_timestamptz_micros_with_offset(micros, display.offset_seconds)
}

/// Resolve timezone display metadata for a `TIMESTAMPTZ` instant.
#[must_use]
pub fn timestamptz_display_in_timezone(micros: i64, timezone: &str) -> Option<TimestampTzDisplay> {
    let trimmed = timezone.trim();
    if let Some(offset_seconds) = parse_timezone_offset(trimmed) {
        return Some(TimestampTzDisplay {
            local_micros: apply_timezone_offset(micros, offset_seconds)?,
            offset_seconds,
            zone_name: fixed_timezone_display_name(trimmed),
        });
    }
    let timezone = trimmed.parse::<chrono_tz::Tz>().ok()?;
    let utc = naive_datetime_from_timestamp_micros(micros)?;
    let offset = timezone.offset_from_utc_datetime(&utc);
    let offset_seconds = offset.fix().local_minus_utc();
    Some(TimestampTzDisplay {
        local_micros: apply_timezone_offset(micros, offset_seconds)?,
        offset_seconds,
        zone_name: offset.abbreviation().map(ToOwned::to_owned),
    })
}

/// Interpret timestamp micros as local wall time in a fixed or IANA timezone.
#[must_use]
pub fn timestamp_micros_at_timezone(micros: i64, timezone: &str) -> Option<i64> {
    let trimmed = timezone.trim();
    let (year, month, day, time_micros) = timestamp_parts_from_micros(micros)?;
    let date_text = format!("{year:04}-{month:02}-{day:02}");
    let offset_seconds = parse_timezone_offset(trimmed)
        .or_else(|| parse_named_timezone_offset(&date_text, time_micros, trimmed))?;
    micros.checked_sub(i64::from(offset_seconds).checked_mul(MICROS_PER_SECOND)?)
}

/// Convert `TIMETZ` time-of-day plus offset into a fixed target timezone.
#[must_use]
pub fn timetz_at_timezone(
    micros: i64,
    source_offset_seconds: i32,
    timezone: &str,
) -> Option<(i64, i32)> {
    let target_offset_seconds = parse_timezone_offset(timezone.trim())?;
    let utc_micros =
        micros.checked_sub(i64::from(source_offset_seconds).checked_mul(MICROS_PER_SECOND)?)?;
    let target_micros = utc_micros
        .checked_add(i64::from(target_offset_seconds).checked_mul(MICROS_PER_SECOND)?)?
        .rem_euclid(MICROS_PER_DAY);
    Some((target_micros, target_offset_seconds))
}

/// Format a UTC offset in PostgreSQL text form.
#[must_use]
pub fn format_timezone_offset_seconds(offset_seconds: i32) -> String {
    format_timezone_offset(offset_seconds)
}

/// Format `TIME WITH TIME ZONE` in PostgreSQL ISO style.
#[must_use]
pub fn format_timetz(micros: i64, offset_seconds: i32) -> String {
    format!(
        "{}{}",
        format_time_micros(micros),
        format_timezone_offset(offset_seconds)
    )
}

/// Format an `INTERVAL` `(months, days, microseconds)` in PostgreSQL's
/// default `postgres` `IntervalStyle`, e.g. `1 day`, `2 mons 03:04:05`,
/// `-00:00:01`, `-1 days +02:00:00`.
///
/// This is the canonical text a libpq client expects for OID 1186; the
/// result encoder and batch text materializers use it. The global
/// [`Value`](crate::Value) `Display` deliberately keeps UltraSQL's internal
/// `"{months}mon {days}d {micros}us"` debug form, which error/debug messages
/// rely on, so this is a separate function rather than a `Display` change.
///
/// PostgreSQL only splits `months` into years/months for display; `days` and
/// the time field are emitted verbatim (no 24h→day or 30d→month carry).
#[must_use]
pub fn format_interval_pg(months: i32, days: i32, microseconds: i64) -> String {
    // Year/month split is sign-preserving truncation, matching PG's
    // `interval2tm` (`month / 12`, `month % 12`).
    let year = i64::from(months) / 12;
    let mon = i64::from(months) % 12;
    let mday = i64::from(days);
    // Decompose the time field; integer `/`/`%` truncate toward zero so every
    // component shares the sign of `microseconds`, matching PG.
    let hour = microseconds / MICROS_PER_HOUR;
    let rem = microseconds % MICROS_PER_HOUR;
    let minute = rem / MICROS_PER_MINUTE;
    let rem = rem % MICROS_PER_MINUTE;
    let second = rem / MICROS_PER_SECOND;
    let fsec = rem % MICROS_PER_SECOND;

    let mut out = String::new();
    let mut is_zero = true;
    let mut is_before = false;
    append_interval_int_part(&mut out, year, "year", &mut is_zero, &mut is_before);
    append_interval_int_part(&mut out, mon, "mon", &mut is_zero, &mut is_before);
    append_interval_int_part(&mut out, mday, "day", &mut is_zero, &mut is_before);

    if is_zero || hour != 0 || minute != 0 || second != 0 || fsec != 0 {
        let minus = hour < 0 || minute < 0 || second < 0 || fsec < 0;
        if !is_zero {
            out.push(' ');
        }
        if minus {
            out.push('-');
        } else if is_before {
            out.push('+');
        }
        out.push_str(&format!("{:02}:{:02}:", hour.abs(), minute.abs()));
        append_interval_seconds(&mut out, second.abs(), fsec.abs());
    }
    out
}

/// Append one signed year/month/day field, mirroring PostgreSQL's
/// `AddPostgresIntPart`: a leading space between fields, a `+` when the
/// previous field was negative but this one is positive, and `s`
/// pluralization for any magnitude other than exactly `1`.
fn append_interval_int_part(
    out: &mut String,
    value: i64,
    unit: &str,
    is_zero: &mut bool,
    is_before: &mut bool,
) {
    if value == 0 {
        return;
    }
    if !*is_zero {
        out.push(' ');
    }
    if *is_before && value > 0 {
        out.push('+');
    }
    out.push_str(&value.to_string());
    out.push(' ');
    out.push_str(unit);
    if value != 1 {
        out.push('s');
    }
    *is_before = value < 0;
    *is_zero = false;
}

/// Append the seconds field as zero-padded `SS` plus, when nonzero, a
/// fractional part with trailing zeros trimmed (PG's `AppendSeconds`). Both
/// arguments are already absolute values.
fn append_interval_seconds(out: &mut String, sec: i64, fsec: i64) {
    out.push_str(&format!("{sec:02}"));
    if fsec != 0 {
        let mut frac = format!("{fsec:06}");
        while frac.ends_with('0') {
            frac.pop();
        }
        out.push('.');
        out.push_str(&frac);
    }
}

/// Accumulator for [`parse_interval_pg`] mirroring PostgreSQL's `struct
/// pg_tm` field-wise decode with fractional-unit cascade.
#[derive(Default)]
struct IntervalAccumulator {
    year: i64,
    mon: i64,
    mday: i64,
    hour: i64,
    min: i64,
    sec: i64,
    fsec: i64,
}

impl IntervalAccumulator {
    /// Cascade a fractional quantity (already a fraction of `scale_secs`
    /// seconds) into whole seconds plus microseconds, matching PG's
    /// `AdjustFractSeconds`.
    fn adjust_fract_seconds(&mut self, frac: f64, scale_secs: i64) -> Option<()> {
        if frac == 0.0 {
            return Some(());
        }
        let total = frac * i64_to_f64(scale_secs);
        let secs = whole_f64(total)?;
        self.sec = self.sec.checked_add(secs)?;
        let remainder = total - i64_to_f64(secs);
        self.fsec = self
            .fsec
            .checked_add(whole_f64((remainder * 1_000_000.0).round())?)?;
        Some(())
    }

    /// Cascade a fractional quantity (a fraction of `scale_days` days) into
    /// whole days then seconds, matching PG's `AdjustFractDays`.
    fn adjust_fract_days(&mut self, frac: f64, scale_days: i64) -> Option<()> {
        if frac == 0.0 {
            return Some(());
        }
        let total = frac * i64_to_f64(scale_days);
        let days = whole_f64(total)?;
        self.mday = self.mday.checked_add(days)?;
        let remainder = total - i64_to_f64(days);
        self.adjust_fract_seconds(remainder, SECS_PER_DAY)
    }

    /// Apply `<value><unit>` (integer part `val`, fractional part `fval`) for
    /// a recognized unit word. Returns `None` for an unknown unit.
    fn apply_unit(&mut self, unit: &str, val: i64, fval: f64) -> Option<()> {
        match unit.to_ascii_lowercase().as_str() {
            "year" | "years" | "y" => {
                self.year = self.year.checked_add(val)?;
                // PG truncates the fractional months from a fractional year and
                // does not cascade further (`2.3 years` -> `2 years 3 mons`);
                // `whole_f64` truncates toward zero.
                self.mon = self.mon.checked_add(whole_f64(fval * 12.0)?)?;
                Some(())
            }
            "month" | "months" | "mon" | "mons" => {
                self.mon = self.mon.checked_add(val)?;
                self.adjust_fract_days(fval, 30)
            }
            "week" | "weeks" | "w" => {
                self.mday = self.mday.checked_add(val.checked_mul(7)?)?;
                self.adjust_fract_days(fval, 7)
            }
            "day" | "days" | "d" => {
                self.mday = self.mday.checked_add(val)?;
                self.adjust_fract_seconds(fval, SECS_PER_DAY)
            }
            "hour" | "hours" | "hr" | "hrs" | "h" => {
                self.hour = self.hour.checked_add(val)?;
                self.adjust_fract_seconds(fval, SECS_PER_HOUR)
            }
            "minute" | "minutes" | "min" | "mins" | "m" => {
                self.min = self.min.checked_add(val)?;
                self.adjust_fract_seconds(fval, SECS_PER_MINUTE)
            }
            "second" | "seconds" | "sec" | "secs" | "s" => {
                self.sec = self.sec.checked_add(val)?;
                self.adjust_fract_seconds(fval, 1)
            }
            "millisecond" | "milliseconds" | "msec" | "msecs" | "ms" => {
                self.fsec = self.fsec.checked_add(val.checked_mul(1_000)?)?;
                self.fsec = self
                    .fsec
                    .checked_add(whole_f64((fval * 1_000.0).round())?)?;
                Some(())
            }
            "microsecond" | "microseconds" | "usec" | "usecs" | "us" => {
                self.fsec = self.fsec.checked_add(val)?;
                Some(())
            }
            _ => None,
        }
    }

    /// Parse and add a `HH:MM[:SS[.ffffff]]` time field with an optional
    /// leading sign, applied to every component.
    fn add_time_component(&mut self, token: &str) -> Option<()> {
        let (negative, body) = match token.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, token.strip_prefix('+').unwrap_or(token)),
        };
        let mut parts = body.split(':');
        let hours: i64 = parts.next()?.parse().ok()?;
        let minutes: i64 = parts.next()?.parse().ok()?;
        let seconds_text = parts.next();
        if parts.next().is_some() {
            return None;
        }
        let (seconds, fsec) = match seconds_text {
            None => (0_i64, 0_i64),
            Some(text) => {
                let (whole, frac) = text.split_once('.').unwrap_or((text, ""));
                let seconds: i64 = whole.parse().ok()?;
                let mut fsec = 0_i64;
                let mut scale = 100_000_i64;
                for ch in frac.chars().take(6) {
                    fsec = fsec.checked_add(i64::from(ch.to_digit(10)?).checked_mul(scale)?)?;
                    scale /= 10;
                }
                (seconds, fsec)
            }
        };
        let sign: i64 = if negative { -1 } else { 1 };
        self.hour = self.hour.checked_add(sign.checked_mul(hours)?)?;
        self.min = self.min.checked_add(sign.checked_mul(minutes)?)?;
        self.sec = self.sec.checked_add(sign.checked_mul(seconds)?)?;
        self.fsec = self.fsec.checked_add(sign.checked_mul(fsec)?)?;
        Some(())
    }

    fn into_triple(self) -> Option<(i32, i32, i64)> {
        let months = self.year.checked_mul(12)?.checked_add(self.mon)?;
        let micros = self
            .hour
            .checked_mul(MICROS_PER_HOUR)?
            .checked_add(self.min.checked_mul(MICROS_PER_MINUTE)?)?
            .checked_add(self.sec.checked_mul(MICROS_PER_SECOND)?)?
            .checked_add(self.fsec)?;
        Some((
            i32::try_from(months).ok()?,
            i32::try_from(self.mday).ok()?,
            micros,
        ))
    }
}

/// Truncate a finite `f64` toward zero into `i64`, returning `None` for
/// non-finite or out-of-range values (so fractional-cascade overflow degrades
/// to a typed NULL rather than a wrapping cast). `ToPrimitive::to_i64` does
/// the range-checked truncation without a lossy `as` cast.
fn whole_f64(value: f64) -> Option<i64> {
    value.to_i64()
}

const SECS_PER_DAY: i64 = 86_400;
const SECS_PER_HOUR: i64 = 3_600;
const SECS_PER_MINUTE: i64 = 60;

/// Parse a PostgreSQL `INTERVAL` input string into
/// `(months, days, microseconds)`.
///
/// Inverts [`format_interval_pg`] (so batch text round-trips through the
/// filter/binary paths) and additionally accepts the common PostgreSQL input
/// spellings: unit phrases (`'1 day'`, `'2 mons 3 days'`, `'1.5 hours'`,
/// `'90 minutes'`), a trailing `HH:MM:SS[.ffffff]` time field with optional
/// sign, the SQL `Y-M` and `D H:M:S` shorthands, and a trailing `ago`.
/// Returns `None` for input it does not recognize; the binder maps that to a
/// typed NULL, preserving the prior reject-to-NULL behavior.
#[must_use]
pub fn parse_interval_pg(text: &str) -> Option<(i32, i32, i64)> {
    let tokens: Vec<&str> = text.split_whitespace().collect();
    if tokens.is_empty() {
        return None;
    }
    let mut acc = IntervalAccumulator::default();
    let mut ago = false;
    let mut saw_field = false;
    let mut idx = 0;
    while idx < tokens.len() {
        let token = tokens[idx];
        if token.eq_ignore_ascii_case("ago") {
            if !saw_field || ago {
                return None;
            }
            ago = true;
            idx += 1;
            continue;
        }
        // A `:`-bearing token is always a clock time field.
        if token.contains(':') {
            acc.add_time_component(token)?;
            saw_field = true;
            idx += 1;
            continue;
        }
        // SQL `Y-M` shorthand, distinguished from a negative bare number by
        // the interior hyphen (`1-2`, `-1-2`).
        if let Some((years, months)) = parse_year_month_field(token) {
            acc.year = acc.year.checked_add(years)?;
            acc.mon = acc.mon.checked_add(months)?;
            saw_field = true;
            idx += 1;
            continue;
        }
        let (val, fval) = parse_interval_number(token)?;
        match tokens.get(idx + 1).copied() {
            Some(unit) if is_interval_unit(unit) => {
                acc.apply_unit(unit, val, fval)?;
                idx += 2;
            }
            // SQL `D H:M:S`: a bare number directly before a time field is a
            // day count; otherwise a bare number is seconds.
            next => {
                if next.is_some_and(|t| t.contains(':')) {
                    acc.mday = acc.mday.checked_add(val)?;
                    acc.adjust_fract_seconds(fval, SECS_PER_DAY)?;
                } else {
                    acc.sec = acc.sec.checked_add(val)?;
                    acc.adjust_fract_seconds(fval, 1)?;
                }
                idx += 1;
            }
        }
        saw_field = true;
    }
    if !saw_field {
        return None;
    }
    let (mut months, mut days, mut micros) = acc.into_triple()?;
    if ago {
        months = months.checked_neg()?;
        days = days.checked_neg()?;
        micros = micros.checked_neg()?;
    }
    Some((months, days, micros))
}

fn is_interval_unit(word: &str) -> bool {
    IntervalAccumulator::default().apply_unit(word, 0, 0.0).is_some()
}

/// Split a numeric interval field into `(integer_part, fractional_part)`,
/// both sign-preserving (`-2.5` -> `(-2, -0.5)`). Returns `None` for tokens
/// that are not a plain decimal number.
fn parse_interval_number(token: &str) -> Option<(i64, f64)> {
    let value: f64 = token.parse().ok()?;
    if !value.is_finite() {
        return None;
    }
    // Reject hex/inf/nan spellings that `f64::parse` would otherwise accept.
    if token
        .bytes()
        .any(|b| !matches!(b, b'0'..=b'9' | b'.' | b'-' | b'+' | b'e' | b'E'))
    {
        return None;
    }
    let integer = whole_f64(value)?;
    let fractional = value - i64_to_f64(integer);
    Some((integer, fractional))
}

fn parse_year_month_field(token: &str) -> Option<(i64, i64)> {
    let (negative, body) = match token.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, token),
    };
    let (years_text, months_text) = body.split_once('-')?;
    let years: i64 = years_text.parse().ok()?;
    let months: i64 = months_text.parse().ok()?;
    if years < 0 || months < 0 {
        return None;
    }
    let sign = if negative { -1 } else { 1 };
    Some((sign * years, sign * months))
}

pub(crate) fn format_time_parts(hour: i64, minute: i64, second: i64, frac: i64) -> String {
    if frac == 0 {
        return format!("{hour:02}:{minute:02}:{second:02}");
    }
    let mut frac_text = format!("{frac:06}");
    while frac_text.ends_with('0') {
        frac_text.pop();
    }
    format!("{hour:02}:{minute:02}:{second:02}.{frac_text}")
}

pub(crate) fn format_timezone_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs = offset_seconds.unsigned_abs();
    let hours = abs.div_euclid(3_600);
    let minutes = abs.rem_euclid(3_600).div_euclid(60);
    let seconds = abs.rem_euclid(60);
    if seconds != 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else if minutes != 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}")
    }
}

/// Parse PostgreSQL-style `TIME` text. Any numeric timezone suffix is
/// silently ignored, matching `time without time zone` coercion.
#[must_use]
pub fn parse_time_text(text: &str) -> Option<i64> {
    parse_time_and_optional_offset(text).map(|(micros, _)| micros)
}

/// Parse PostgreSQL ISO `TIMESTAMP WITHOUT TIME ZONE` text into
/// microseconds since UltraSQL's timestamp epoch.
#[must_use]
pub fn parse_timestamp_text(text: &str) -> Option<i64> {
    let (date, time) = split_timestamp_text(text)?;
    let days = i64::from(parse_date_text(date)?);
    let micros = parse_time_text(time)?;
    days.checked_mul(MICROS_PER_DAY)?.checked_add(micros)
}

/// Parse PostgreSQL-style `TIMESTAMPTZ` text into UTC microseconds since
/// UltraSQL's timestamp epoch.
#[must_use]
pub fn parse_timestamptz_text(text: &str) -> Option<i64> {
    let (date, time) = split_timestamp_text(text)?;
    let days = i64::from(parse_date_text(date)?);
    let (_, time_token, zone_token) = split_time_and_optional_zone(time)?;
    let micros = parse_time_token(time_token)?;
    let offset_seconds = match zone_token {
        Some(zone) => parse_timezone_offset(zone)
            .or_else(|| parse_named_timezone_offset(date, micros, zone))?,
        None => 0,
    };
    days.checked_mul(MICROS_PER_DAY)?
        .checked_add(micros)?
        .checked_sub(i64::from(offset_seconds).checked_mul(MICROS_PER_SECOND)?)
}

/// Parse PostgreSQL-style `TIMETZ` text into time-of-day and UTC offset.
#[must_use]
pub fn parse_timetz_text(text: &str) -> Option<(i64, i32)> {
    parse_time_and_optional_offset(text).map(|(micros, offset)| (micros, offset.unwrap_or(0)))
}

pub(crate) fn parse_time_and_optional_offset(text: &str) -> Option<(i64, Option<i32>)> {
    let (date_token, time_token, zone_token) = split_time_and_optional_zone(text)?;
    let micros = parse_time_token(time_token)?;
    let offset = match zone_token {
        Some(zone) => Some(parse_timezone_offset(zone).or_else(|| {
            date_token.and_then(|date| parse_named_timezone_offset(date, micros, zone))
        })?),
        None => None,
    };
    Some((micros, offset))
}

pub(crate) fn split_time_and_optional_zone(
    text: &str,
) -> Option<(Option<&str>, &str, Option<&str>)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let (date_token, time_token, zone_token) = match tokens.as_slice() {
        [single] => {
            let (time, zone) = split_inline_timezone(single);
            (None, time, zone)
        }
        [first, second] if looks_like_iso_date(first) => (Some(*first), *second, None),
        [first, second] => (None, *first, Some(*second)),
        [first, second, third, ..] if looks_like_iso_date(first) => {
            (Some(*first), *second, Some(*third))
        }
        _ => return None,
    };
    Some((date_token, time_token, zone_token))
}

pub(crate) fn looks_like_iso_date(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() >= 10 && bytes.get(4) == Some(&b'-') && bytes.get(7) == Some(&b'-')
}

pub(crate) fn split_timestamp_text(text: &str) -> Option<(&str, &str)> {
    let trimmed = text.trim();
    let split_at = trimmed
        .char_indices()
        .find_map(|(idx, ch)| (ch == 'T' || ch.is_ascii_whitespace()).then_some(idx))?;
    let date = trimmed[..split_at].trim();
    let time =
        trimmed[split_at..].trim_start_matches(|ch: char| ch == 'T' || ch.is_ascii_whitespace());
    (!date.is_empty() && !time.is_empty()).then_some((date, time))
}

pub(crate) fn split_inline_timezone(token: &str) -> (&str, Option<&str>) {
    let mut split_at = None;
    for (idx, ch) in token.char_indices().skip(1) {
        if ch == '+' || ch == '-' {
            split_at = Some(idx);
        }
    }
    split_at.map_or((token, None), |idx| (&token[..idx], Some(&token[idx..])))
}

pub(crate) fn parse_time_token(token: &str) -> Option<i64> {
    let mut parts = token.splitn(3, ':');
    let hour_text = parts.next()?;
    let minute_text = parts.next()?;
    let second_text = parts.next().unwrap_or("0");
    let hour: i64 = hour_text.parse().ok()?;
    let minute: i64 = minute_text.parse().ok()?;
    let (second_part, frac_part) = second_text
        .split_once('.')
        .map_or((second_text, ""), |(sec, frac)| (sec, frac));
    let second: i64 = second_part.parse().ok()?;
    if !(0..=24).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }
    let mut frac_micros = 0_i64;
    let mut scale = 100_000_i64;
    for ch in frac_part.chars().take(6) {
        let digit = i64::from(ch.to_digit(10)?);
        frac_micros = frac_micros.checked_add(digit.checked_mul(scale)?)?;
        scale /= 10;
    }
    if hour == 24 && (minute != 0 || second != 0 || frac_micros != 0) {
        return None;
    }
    hour.checked_mul(MICROS_PER_HOUR)?
        .checked_add(minute.checked_mul(MICROS_PER_MINUTE)?)?
        .checked_add(second.checked_mul(MICROS_PER_SECOND)?)?
        .checked_add(frac_micros)
}

pub(crate) fn parse_timezone_offset(token: &str) -> Option<i32> {
    let lower = token.to_ascii_lowercase();
    if matches!(lower.as_str(), "z" | "zulu" | "utc") {
        return Some(0);
    }
    if let Some(offset) = parse_timezone_abbreviation(&lower) {
        return Some(offset);
    }
    let sign = match token.as_bytes().first()? {
        b'+' => 1_i32,
        b'-' => -1_i32,
        _ => return None,
    };
    let body = &token[1..];
    let (hours, minutes, seconds) = if body.contains(':') {
        let mut parts = body.split(':');
        let hours = parts.next()?.parse::<i32>().ok()?;
        let minutes = parts.next().unwrap_or("0").parse::<i32>().ok()?;
        let seconds = parts.next().unwrap_or("0").parse::<i32>().ok()?;
        if parts.next().is_some() {
            return None;
        }
        (hours, minutes, seconds)
    } else if body.len() > 2 {
        let minute_start = body.len().checked_sub(2)?;
        let hours = body.get(..minute_start)?.parse::<i32>().ok()?;
        let minutes = body.get(minute_start..)?.parse::<i32>().ok()?;
        (hours, minutes, 0)
    } else {
        (body.parse::<i32>().ok()?, 0, 0)
    };
    if !(0..=15).contains(&hours) || !(0..=59).contains(&minutes) || !(0..=59).contains(&seconds) {
        return None;
    }
    let total = hours
        .checked_mul(3_600)?
        .checked_add(minutes.checked_mul(60)?)?
        .checked_add(seconds)?;
    sign.checked_mul(total)
}

pub(crate) fn parse_timezone_abbreviation(lower: &str) -> Option<i32> {
    let hours: i32 = match lower {
        "gmt" | "ut" | "wet" => 0,
        "west" | "cet" => 1,
        "cest" | "eet" => 2,
        "eest" => 3,
        "edt" => -4,
        "est" | "cdt" => -5,
        "cst" | "mdt" => -6,
        "mst" | "pdt" => -7,
        "pst" => -8,
        _ => return None,
    };
    hours.checked_mul(3_600)
}

pub(crate) fn apply_timezone_offset(micros: i64, offset_seconds: i32) -> Option<i64> {
    micros.checked_add(i64::from(offset_seconds).checked_mul(MICROS_PER_SECOND)?)
}

pub(crate) fn fixed_timezone_display_name(token: &str) -> Option<String> {
    let lower = token.to_ascii_lowercase();
    if matches!(lower.as_str(), "z" | "zulu" | "utc") {
        return Some("UTC".to_owned());
    }
    if parse_timezone_abbreviation(&lower).is_some()
        && !matches!(token.as_bytes().first(), Some(b'+' | b'-'))
    {
        return Some(token.to_ascii_uppercase());
    }
    None
}

pub(crate) fn naive_datetime_from_timestamp_micros(micros: i64) -> Option<chrono::NaiveDateTime> {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let time_micros = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(i32::try_from(days).ok()?)?;
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let (hour, minute, second, micros) = split_micros_of_day(time_micros)?;
    let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micros)?;
    Some(date.and_time(time))
}

pub(crate) fn parse_named_timezone_offset(date_text: &str, micros: i64, zone: &str) -> Option<i32> {
    let timezone = zone.parse::<chrono_tz::Tz>().ok()?;
    let (year, month, day) = parse_date_parts(date_text)?;
    let mut date = NaiveDate::from_ymd_opt(year, month, day)?;
    let mut local_micros = micros;
    if local_micros == MICROS_PER_DAY {
        date = date.checked_add_days(Days::new(1))?;
        local_micros = 0;
    }
    let (hour, minute, second, micros) = split_micros_of_day(local_micros)?;
    let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micros)?;
    let local = date.and_time(time);
    let resolved = match timezone.from_local_datetime(&local) {
        LocalResult::Single(value) => value,
        LocalResult::Ambiguous(earliest, _) => earliest,
        LocalResult::None => return None,
    };
    Some(resolved.offset().fix().local_minus_utc())
}

/// Pack `TIMETZ` into an `i64` batch payload.
#[must_use]
pub fn pack_timetz(micros: i64, offset_seconds: i32) -> Option<i64> {
    if !(0..=MICROS_PER_DAY).contains(&micros)
        || !(-TIMETZ_OFFSET_BIAS_SECONDS..=TIMETZ_OFFSET_BIAS_SECONDS).contains(&offset_seconds)
    {
        return None;
    }
    let biased = i64::from(offset_seconds.checked_add(TIMETZ_OFFSET_BIAS_SECONDS)?);
    Some((micros << TIMETZ_OFFSET_BITS) | biased)
}

/// Unpack an `i64` batch payload into `TIMETZ` components.
#[must_use]
pub fn unpack_timetz(packed: i64) -> Option<(i64, i32)> {
    if packed < 0 {
        return None;
    }
    let micros = packed >> TIMETZ_OFFSET_BITS;
    let biased = i32::try_from(packed & TIMETZ_OFFSET_MASK).ok()?;
    let offset_seconds = biased.checked_sub(TIMETZ_OFFSET_BIAS_SECONDS)?;
    if !(0..=MICROS_PER_DAY).contains(&micros) {
        return None;
    }
    Some((micros, offset_seconds))
}

/// Normalize `TIMETZ` to UTC time-of-day micros for equality, hashing,
/// ordering, and hash joins.
#[must_use]
pub fn timetz_utc_micros(micros: i64, offset_seconds: i32) -> i64 {
    micros
        .saturating_sub(i64::from(offset_seconds).saturating_mul(MICROS_PER_SECOND))
        .rem_euclid(MICROS_PER_DAY)
}

pub(crate) fn split_micros_of_day(micros: i64) -> Option<(u32, u32, u32, u32)> {
    if !(0..MICROS_PER_DAY).contains(&micros) {
        return None;
    }
    let hour = u32::try_from(micros.div_euclid(MICROS_PER_HOUR)).ok()?;
    let rem = micros.rem_euclid(MICROS_PER_HOUR);
    let minute = u32::try_from(rem.div_euclid(MICROS_PER_MINUTE)).ok()?;
    let rem = rem.rem_euclid(MICROS_PER_MINUTE);
    let second = u32::try_from(rem.div_euclid(MICROS_PER_SECOND)).ok()?;
    let micros = u32::try_from(rem.rem_euclid(MICROS_PER_SECOND)).ok()?;
    Some((hour, minute, second, micros))
}

/// Inverse of Howard Hinnant's `days_from_civil`, rebased on UltraSQL's
/// 2000-01-01 date epoch.
pub(crate) fn civil_from_days(days_since_2000_01_01: i32) -> Option<(i32, u32, u32)> {
    let z = i64::from(days_since_2000_01_01)
        .checked_add(10_957)?
        .checked_add(719_468)?;
    let era = if z >= 0 {
        z.div_euclid(146_097)
    } else {
        z.checked_sub(146_096)?.div_euclid(146_097)
    };
    let doe = z.checked_sub(era.checked_mul(146_097)?)?;
    let yoe_numerator = doe
        .checked_sub(doe.div_euclid(1_460))?
        .checked_add(doe.div_euclid(36_524))?
        .checked_sub(doe.div_euclid(146_096))?;
    let yoe = yoe_numerator.div_euclid(365);
    let y = yoe.checked_add(era.checked_mul(400)?)?;
    let doy = doe.checked_sub(
        365_i64
            .checked_mul(yoe)?
            .checked_add(yoe.div_euclid(4))?
            .checked_sub(yoe.div_euclid(100))?,
    )?;
    let mp = 5_i64.checked_mul(doy)?.checked_add(2)?.div_euclid(153);
    let day = doy
        .checked_sub(153_i64.checked_mul(mp)?.checked_add(2)?.div_euclid(5))?
        .checked_add(1)?;
    let month = if mp < 10 {
        mp.checked_add(3)?
    } else {
        mp.checked_sub(9)?
    };
    let year = if month <= 2 { y.checked_add(1)? } else { y };
    Some((
        i32::try_from(year).ok()?,
        u32::try_from(month).ok()?,
        u32::try_from(day).ok()?,
    ))
}

#[cfg(test)]
mod interval_tests {
    use super::{format_interval_pg, parse_interval_pg};

    // Reference (m, d, us, canonical text) verified against PostgreSQL 14 via
    // `make_interval`/text input. See the INTERVAL wire-format fix.
    const FORMAT_CASES: &[(i32, i32, i64, &str)] = &[
        (0, 0, 0, "00:00:00"),
        (0, 1, 0, "1 day"),
        (0, 2, 0, "2 days"),
        (1, 0, 0, "1 mon"),
        (2, 0, 0, "2 mons"),
        (12, 0, 0, "1 year"),
        (13, 0, 0, "1 year 1 mon"),
        (24, 0, 0, "2 years"),
        (25, 0, 0, "2 years 1 mon"),
        (14, 3, 0, "1 year 2 mons 3 days"),
        (0, 0, 3_600_000_000, "01:00:00"),
        (0, 0, 10_800_000_000, "03:00:00"),
        (0, 0, 3_845_000_000, "01:04:05"),
        (0, 1, 3_845_000_000, "1 day 01:04:05"),
        (0, 0, -1_000_000, "-00:00:01"),
        (0, -1, 0, "-1 days"),
        (-1, 0, 0, "-1 mons"),
        (0, -1, -7_200_000_000, "-1 days -02:00:00"),
        (0, -1, 7_200_000_000, "-1 days +02:00:00"),
        (0, 1, -7_200_000_000, "1 day -02:00:00"),
        (14, 3, 3_845_000_000, "1 year 2 mons 3 days 01:04:05"),
        (0, 0, 1_500_000, "00:00:01.5"),
        (0, 0, 1_234_560, "00:00:01.23456"),
        (0, 0, 90_000_000, "00:01:30"),
        (-14, -3, -3_845_000_000, "-1 years -2 mons -3 days -01:04:05"),
        (0, 0, 500_000, "00:00:00.5"),
        (0, 0, -500_000, "-00:00:00.5"),
        (1, 0, 500_000, "1 mon 00:00:00.5"),
        (-1, 2, 0, "-1 mons +2 days"),
        (0, 0, 86_400_000_000, "24:00:00"),
        (0, 0, -86_400_000_000, "-24:00:00"),
    ];

    #[test]
    fn format_interval_pg_matches_postgres() {
        for &(months, days, micros, want) in FORMAT_CASES {
            assert_eq!(
                format_interval_pg(months, days, micros),
                want,
                "format ({months}, {days}, {micros})"
            );
        }
    }

    #[test]
    fn parse_interval_pg_round_trips_formatter_output() {
        for &(months, days, micros, _) in FORMAT_CASES {
            let text = format_interval_pg(months, days, micros);
            assert_eq!(
                parse_interval_pg(&text),
                Some((months, days, micros)),
                "round-trip {text:?}"
            );
        }
    }

    #[test]
    fn parse_interval_pg_accepts_postgres_input_forms() {
        // (input, expected canonical text) verified against PostgreSQL 14.
        let cases: &[(&str, &str)] = &[
            ("1 day", "1 day"),
            ("1 day 2 hours", "1 day 02:00:00"),
            ("1 year 2 months 3 days 04:05:06", "1 year 2 mons 3 days 04:05:06"),
            ("-00:00:01", "-00:00:01"),
            ("1.5 hours", "01:30:00"),
            ("2 weeks", "14 days"),
            ("90 minutes", "01:30:00"),
            ("1 mon 2 days 03:04:05", "1 mon 2 days 03:04:05"),
            ("1", "00:00:01"),
            ("04:05", "04:05:00"),
            ("100", "00:01:40"),
            ("1.234 secs", "00:00:01.234"),
            ("3 days ago", "-3 days"),
            ("-1 day -2 hours", "-1 days -02:00:00"),
            ("-1 day +2 hours", "-1 days +02:00:00"),
            ("1:2:3", "01:02:03"),
            ("1-2", "1 year 2 mons"),
            ("3 4:05:06", "3 days 04:05:06"),
            ("2.3 years", "2 years 3 mons"),
            ("1.5 mons", "1 mon 15 days"),
            ("5 ms", "00:00:00.005"),
            ("5 us", "00:00:00.000005"),
            ("1 hour 30 minutes", "01:30:00"),
            ("-5 mons", "-5 mons"),
        ];
        for &(input, want) in cases {
            let parsed = parse_interval_pg(input).unwrap_or_else(|| panic!("parse {input:?}"));
            let (months, days, micros) = parsed;
            assert_eq!(
                format_interval_pg(months, days, micros),
                want,
                "input {input:?}"
            );
        }
    }

    #[test]
    fn parse_interval_pg_rejects_garbage() {
        for bad in ["", "   ", "nonsense", "1 fortnight", "ago", "abc def"] {
            assert_eq!(parse_interval_pg(bad), None, "should reject {bad:?}");
        }
    }
}
