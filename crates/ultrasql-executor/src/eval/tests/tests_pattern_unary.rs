//! LIKE/regex, IS NULL, unary, and property tests.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn like_percent_matches_any_suffix() {
    let ev = Eval::new(binop(BinaryOp::Like, lit_text("foobar"), lit_text("foo%")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn like_no_match() {
    let ev = Eval::new(binop(BinaryOp::Like, lit_text("foobar"), lit_text("baz%")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn like_underscore_single_char() {
    let ev = Eval::new(binop(BinaryOp::Like, lit_text("foo"), lit_text("f_o")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn like_backslash_escapes_wildcards() {
    let escaped_underscore = Eval::new(binop(BinaryOp::Like, lit_text("a_b"), lit_text(r"a\_b")));
    assert_eq!(escaped_underscore.eval(&[]).unwrap(), Value::Bool(true));

    let escaped_percent = Eval::new(binop(
        BinaryOp::Like,
        lit_text("sale%2026"),
        lit_text(r"sale\%2026"),
    ));
    assert_eq!(escaped_percent.eval(&[]).unwrap(), Value::Bool(true));

    let wildcard_still_wild = Eval::new(binop(BinaryOp::Like, lit_text("axb"), lit_text(r"a\_b")));
    assert_eq!(wildcard_still_wild.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn like_constant_pattern_is_stable_across_repeats() {
    // Re-evaluating the same constant pattern many times exercises the
    // thread-local LIKE-pattern compile cache; results must match a single
    // inline compile.
    let yes = Eval::new(binop(BinaryOp::Like, lit_text("foobar"), lit_text("foo%")));
    let no = Eval::new(binop(BinaryOp::Like, lit_text("bazbar"), lit_text("foo%")));
    for _ in 0..1000 {
        assert_eq!(yes.eval(&[]).unwrap(), Value::Bool(true));
        assert_eq!(no.eval(&[]).unwrap(), Value::Bool(false));
    }
}

#[test]
fn like_and_ilike_distinguished_under_same_pattern() {
    // The LIKE cache key includes the case-insensitivity flag, so a pattern
    // compiled case-sensitively must not be served for an ILIKE request.
    let sensitive = Eval::new(binop(BinaryOp::Like, lit_text("FOO"), lit_text("foo")));
    let insensitive = Eval::new(binop(BinaryOp::Ilike, lit_text("FOO"), lit_text("foo")));
    assert_eq!(sensitive.eval(&[]).unwrap(), Value::Bool(false));
    assert_eq!(insensitive.eval(&[]).unwrap(), Value::Bool(true));
    // Order-independence: re-check after the case-insensitive compile.
    assert_eq!(sensitive.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn not_like_positive() {
    let ev = Eval::new(binop(
        BinaryOp::NotLike,
        lit_text("foobar"),
        lit_text("baz%"),
    ));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn ilike_case_insensitive() {
    let ev = Eval::new(binop(BinaryOp::Ilike, lit_text("FooBar"), lit_text("foo%")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn ilike_no_match() {
    let ev = Eval::new(binop(BinaryOp::Ilike, lit_text("foobar"), lit_text("baz%")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn regex_match_operator_matches_psql_meta_patterns() {
    let ev = Eval::new(binop(
        BinaryOp::RegexMatch,
        lit_text("psql_meta_table"),
        lit_text("^(psql_meta_table)$"),
    ));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn regex_match_constant_pattern_is_stable_across_repeats() {
    // Re-evaluating the same constant pattern many times exercises the
    // thread-local compiled-regex cache; results must be identical to a
    // single inline compile.
    let yes = Eval::new(binop(
        BinaryOp::RegexMatch,
        lit_text("alpha123"),
        lit_text("^[a-z]+[0-9]+$"),
    ));
    let no = Eval::new(binop(
        BinaryOp::RegexMatch,
        lit_text("ALPHA"),
        lit_text("^[a-z]+[0-9]+$"),
    ));
    for _ in 0..1000 {
        assert_eq!(yes.eval(&[]).unwrap(), Value::Bool(true));
        assert_eq!(no.eval(&[]).unwrap(), Value::Bool(false));
    }
}

#[test]
fn regex_imatch_distinguished_from_match_under_same_pattern() {
    // The cache key includes the case-insensitivity flag, so the same
    // pattern string compiled case-sensitively must not be served for a
    // case-insensitive request.
    let sensitive = Eval::new(binop(
        BinaryOp::RegexMatch,
        lit_text("FOO"),
        lit_text("^foo$"),
    ));
    let insensitive = Eval::new(binop(
        BinaryOp::RegexIMatch,
        lit_text("FOO"),
        lit_text("^foo$"),
    ));
    assert_eq!(sensitive.eval(&[]).unwrap(), Value::Bool(false));
    assert_eq!(insensitive.eval(&[]).unwrap(), Value::Bool(true));
    // Order-independence: re-check after the case-insensitive compile.
    assert_eq!(sensitive.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn regex_match_invalid_pattern_errors_every_time() {
    // An invalid pattern is never cached, so each evaluation re-reports the
    // same error rather than succeeding spuriously.
    let ev = Eval::new(binop(
        BinaryOp::RegexMatch,
        lit_text("anything"),
        lit_text("("),
    ));
    for _ in 0..3 {
        let err = ev.eval(&[]).unwrap_err();
        assert!(format!("{err}").contains("regex operator: invalid pattern"));
    }
}

// -----------------------------------------------------------------------
// IsNull
// -----------------------------------------------------------------------

#[test]
fn is_null_true_for_null() {
    let ev = Eval::new(ScalarExpr::IsNull {
        expr: Box::new(lit_null()),
        negated: false,
    });
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn is_null_false_for_non_null() {
    let ev = Eval::new(ScalarExpr::IsNull {
        expr: Box::new(lit_i32(0)),
        negated: false,
    });
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn is_not_null_true_for_non_null() {
    let ev = Eval::new(ScalarExpr::IsNull {
        expr: Box::new(lit_i32(42)),
        negated: true,
    });
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

// -----------------------------------------------------------------------
// Unary operators
// -----------------------------------------------------------------------

#[test]
fn unary_neg_i32() {
    let ev = Eval::new(unop(UnaryOp::Neg, lit_i32(5)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(-5));
}

#[test]
fn unary_neg_overflow() {
    let ev = Eval::new(unop(UnaryOp::Neg, lit_i32(i32::MIN)));
    assert!(matches!(ev.eval(&[]).unwrap_err(), EvalError::Overflow));
}

#[test]
fn unary_pos_is_noop() {
    let ev = Eval::new(unop(UnaryOp::Pos, lit_i32(7)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(7));
}

#[test]
fn unary_not_true() {
    let ev = Eval::new(unop(UnaryOp::Not, lit_bool(true)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn unary_not_null_is_null() {
    let ev = Eval::new(unop(UnaryOp::Not, lit_null()));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
}

// -----------------------------------------------------------------------
// Property test: integer arithmetic matches i64::checked_*
// -----------------------------------------------------------------------

proptest! {
    #[test]
    fn prop_int32_add_matches_checked(a: i32, b: i32) {
        let ev = Eval::new(binop(BinaryOp::Add, lit_i32(a), lit_i32(b)));
        let result = ev.eval(&[]);
        match a.checked_add(b) {
            Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int32(expected)),
            None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
        }
    }

    #[test]
    fn prop_int32_sub_matches_checked(a: i32, b: i32) {
        let ev = Eval::new(binop(BinaryOp::Sub, lit_i32(a), lit_i32(b)));
        let result = ev.eval(&[]);
        match a.checked_sub(b) {
            Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int32(expected)),
            None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
        }
    }

    #[test]
    fn prop_int32_mul_matches_checked(a: i32, b: i32) {
        let ev = Eval::new(binop(BinaryOp::Mul, lit_i32(a), lit_i32(b)));
        let result = ev.eval(&[]);
        match a.checked_mul(b) {
            Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int32(expected)),
            None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
        }
    }

    #[test]
    fn prop_int32_div_matches_checked(a: i32, b: i32) {
        let ev = Eval::new(binop(BinaryOp::Div, lit_i32(a), lit_i32(b)));
        let result = ev.eval(&[]);
        if b == 0 {
            prop_assert!(matches!(result.unwrap_err(), EvalError::DivByZero));
        } else {
            match a.checked_div(b) {
                Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int32(expected)),
                None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
            }
        }
    }

    #[test]
    fn prop_int64_add_matches_checked(a: i64, b: i64) {
        let ev = Eval::new(binop(BinaryOp::Add, lit_i64(a), lit_i64(b)));
        let result = ev.eval(&[]);
        match a.checked_add(b) {
            Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int64(expected)),
            None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
        }
    }
}
