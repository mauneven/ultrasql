//! Regression tests for six PostgreSQL-compatibility fixes in builtin
//! functions. Each expected value was verified against PostgreSQL 14.
//!
//! * `age()` returns a normalized year/month/day interval.
//! * `regexp_replace` honours the `i`/`g`/`m` flags.
//! * `extract(epoch from interval)` accounts for the months component.
//! * `array_position(arr, NULL)` locates NULL entries (IS NOT DISTINCT FROM).
//! * boolean text cast accepts mixed-case / prefix inputs.
//! * boolean *output-function* text (`concat`/`format`/`array_to_string`) is
//!   `t`/`f`, while the explicit `::text` cast stays `true`/`false`.

use super::*;

fn ts(text: &str) -> Value {
    Value::Timestamp(parse_timestamp_text(text).expect("timestamp parses"))
}

fn interval(months: i32, days: i32, microseconds: i64) -> Value {
    Value::Interval {
        months,
        days,
        microseconds,
    }
}

// -------------------------------------------------------------------------
// BUG 1 — age() normalized year/month/day interval
// -------------------------------------------------------------------------

#[test]
fn age_returns_normalized_calendar_interval() {
    // 2 mons
    assert_eq!(
        eval_fn(
            "age",
            vec![ts("2021-03-01 00:00:00"), ts("2021-01-01 00:00:00")]
        ),
        interval(2, 0, 0)
    );
    // 43 years 9 mons 27 days -> total months = 43*12 + 9 = 525
    assert_eq!(
        eval_fn(
            "age",
            vec![ts("2001-04-10 00:00:00"), ts("1957-06-13 00:00:00")]
        ),
        interval(525, 27, 0)
    );
    // same day -> zero
    assert_eq!(
        eval_fn(
            "age",
            vec![ts("2021-01-01 00:00:00"), ts("2021-01-01 00:00:00")]
        ),
        interval(0, 0, 0)
    );
    // sub-day diff -> 12:00:00
    assert_eq!(
        eval_fn(
            "age",
            vec![ts("2021-01-02 06:00:00"), ts("2021-01-01 18:00:00")]
        ),
        interval(0, 0, 12 * 3_600_000_000)
    );
}

#[test]
fn age_negates_all_fields_when_end_before_start() {
    // PG: -2 mons -9 days (not a borrowed -3 mons 22 days)
    assert_eq!(
        eval_fn(
            "age",
            vec![ts("2021-01-01 00:00:00"), ts("2021-03-10 00:00:00")]
        ),
        interval(-2, -9, 0)
    );
    // PG: -1 days -12:00:00
    assert_eq!(
        eval_fn(
            "age",
            vec![ts("2021-03-01 06:00:00"), ts("2021-03-02 18:00:00")]
        ),
        interval(0, -1, -12 * 3_600_000_000)
    );
}

// -------------------------------------------------------------------------
// BUG 2 — regexp_replace honours flags
// -------------------------------------------------------------------------

fn t(text: &str) -> Value {
    Value::Text(text.to_owned())
}

#[test]
fn regexp_replace_honours_case_insensitive_flag() {
    assert_eq!(
        eval_fn(
            "regexp_replace",
            vec![t("Hello World"), t("hello"), t("X"), t("i")]
        ),
        t("X World")
    );
}

#[test]
fn regexp_replace_global_and_combined_flags() {
    // g still replaces all
    assert_eq!(
        eval_fn("regexp_replace", vec![t("a-b-c"), t("-"), t("_"), t("g")]),
        t("a_b_c")
    );
    // gi combined: case-insensitive global replace
    assert_eq!(
        eval_fn(
            "regexp_replace",
            vec![t("Hello hELLO"), t("hello"), t("X"), t("gi")]
        ),
        t("X X")
    );
}

#[test]
fn regexp_replace_no_flags_unchanged() {
    // first-only, case-sensitive
    assert_eq!(
        eval_fn("regexp_replace", vec![t("a-b-c"), t("-"), t("_")]),
        t("a_b-c")
    );
    // case-sensitive: no match
    assert_eq!(
        eval_fn("regexp_replace", vec![t("Hello World"), t("hello"), t("X")]),
        t("Hello World")
    );
}

// -------------------------------------------------------------------------
// BUG 3 — extract(epoch from interval) accounts for months
// -------------------------------------------------------------------------

fn epoch_of_interval(months: i32, days: i32, microseconds: i64) -> Value {
    eval_fn(
        "extract",
        vec![t("epoch"), interval(months, days, microseconds)],
    )
}

fn decimal(value: i128, scale: i32) -> Value {
    Value::Decimal { value, scale }
}

#[test]
fn extract_epoch_from_interval_includes_months() {
    // 1 month = 30 days = 2_592_000 s
    assert_eq!(epoch_of_interval(1, 0, 0), decimal(2_592_000_000_000, 6));
    // 1 year (12 months) = 365.25 days = 31_557_600 s (PG 14 behaviour)
    assert_eq!(epoch_of_interval(12, 0, 0), decimal(31_557_600_000_000, 6));
    // 1 year 2 mons (14 months) = 31_557_600 + 2*2_592_000 = 36_741_600 s
    assert_eq!(epoch_of_interval(14, 0, 0), decimal(36_741_600_000_000, 6));
    // 1 day
    assert_eq!(epoch_of_interval(0, 1, 0), decimal(86_400_000_000, 6));
    // negative month
    assert_eq!(epoch_of_interval(-1, 0, 0), decimal(-2_592_000_000_000, 6));
    // combined: 1 mon 1 day 1 sec
    assert_eq!(
        epoch_of_interval(1, 1, 1_000_000),
        decimal(2_678_401_000_000, 6)
    );
}

// -------------------------------------------------------------------------
// BUG 4 — array_position(arr, NULL) locates NULL entries
// -------------------------------------------------------------------------

fn int_array(values: &[Option<i32>]) -> Value {
    Value::Array {
        element_type: DataType::Int32,
        elements: values
            .iter()
            .map(|v| v.map_or(Value::Null, Value::Int32))
            .collect(),
    }
}

#[test]
fn array_position_locates_null_element() {
    assert_eq!(
        eval_fn(
            "array_position",
            vec![int_array(&[Some(1), None, Some(3)]), Value::Null]
        ),
        Value::Int32(2)
    );
}

#[test]
fn array_position_null_search_without_null_is_null() {
    assert_eq!(
        eval_fn(
            "array_position",
            vec![int_array(&[Some(1), Some(2), Some(3)]), Value::Null]
        ),
        Value::Null
    );
}

#[test]
fn array_position_non_null_search_still_works() {
    assert_eq!(
        eval_fn(
            "array_position",
            vec![int_array(&[Some(1), Some(2), Some(3)]), Value::Int32(2)]
        ),
        Value::Int32(2)
    );
    // not found
    assert_eq!(
        eval_fn(
            "array_position",
            vec![int_array(&[Some(1), Some(2), Some(3)]), Value::Int32(9)]
        ),
        Value::Null
    );
}

// -------------------------------------------------------------------------
// BUG 5 — boolean text cast accepts mixed-case / prefix inputs
// -------------------------------------------------------------------------

fn cast_bool(text: &str) -> Value {
    eval_fn("__ultrasql_cast_bool", vec![t(text)])
}

fn cast_bool_err(text: &str) -> String {
    eval_function_call("__ultrasql_cast_bool", &[t(text)], &DataType::Null)
        .expect_err("expected cast error")
        .to_string()
}

#[test]
fn bool_cast_accepts_prefix_and_mixed_case() {
    for s in [
        "True", "tr", "TRUE", "yes", "Yes", "on", "t", "y", "1", "  true  ", "TrUe", "YE",
    ] {
        assert_eq!(cast_bool(s), Value::Bool(true), "{s} should be true");
    }
    for s in ["Off", "f", "0", "n", "no", "fal", "of"] {
        assert_eq!(cast_bool(s), Value::Bool(false), "{s} should be false");
    }
}

#[test]
fn bool_cast_rejects_ambiguous_and_garbage() {
    for s in ["o", "tru e", "x", "", "truee", "yess"] {
        assert!(!cast_bool_err(s).is_empty(), "{s} should error");
    }
}

// -------------------------------------------------------------------------
// BUG 6 — boolean output-function text (t/f) vs ::text cast (true/false)
// -------------------------------------------------------------------------

fn bool_array(values: &[Value]) -> Value {
    Value::Array {
        element_type: DataType::Bool,
        elements: values.to_vec(),
    }
}

#[test]
fn bool_text_cast_stays_true_false() {
    assert_eq!(
        eval_fn("__ultrasql_cast_text", vec![Value::Bool(true)]),
        t("true")
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_text", vec![Value::Bool(false)]),
        t("false")
    );
}

#[test]
fn bool_output_function_text_is_t_f() {
    // concat
    assert_eq!(eval_fn("concat", vec![t("x"), Value::Bool(true)]), t("xt"));
    assert_eq!(
        eval_fn("concat", vec![Value::Bool(true), Value::Bool(false)]),
        t("tf")
    );
    // concat_ws
    assert_eq!(
        eval_fn(
            "concat_ws",
            vec![t(","), Value::Bool(true), Value::Bool(false)]
        ),
        t("t,f")
    );
    // format %s
    assert_eq!(eval_fn("format", vec![t("%s"), Value::Bool(false)]), t("f"));
    assert_eq!(eval_fn("format", vec![t("%s"), Value::Bool(true)]), t("t"));
    // array_to_string
    assert_eq!(
        eval_fn(
            "array_to_string",
            vec![bool_array(&[Value::Bool(true), Value::Bool(false)]), t(",")]
        ),
        t("t,f")
    );
}
