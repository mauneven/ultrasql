//! Parameter, arithmetic, and comparison tests.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn parameter_substitution_returns_bound_value() {
    let ev = Eval::with_params(param(1), vec![Value::Int32(99)]);
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(99));
}

#[test]
fn parameter_out_of_range_returns_error() {
    let ev = Eval::with_params(param(3), vec![Value::Int32(1)]);
    let err = ev.eval(&[]).unwrap_err();
    assert!(
        matches!(err, EvalError::ParameterIndex { index: 3, len: 1 }),
        "unexpected: {err}"
    );
}

#[test]
fn zero_parameter_index_returns_error() {
    let ev = Eval::with_params(param(0), vec![Value::Int32(99)]);
    let err = ev.eval(&[]).unwrap_err();
    assert!(
        matches!(err, EvalError::ParameterIndex { index: 0, len: 1 }),
        "unexpected: {err}"
    );
}

// -----------------------------------------------------------------------
// Arithmetic: Int32
// -----------------------------------------------------------------------

#[test]
fn int32_add() {
    let ev = Eval::new(binop(BinaryOp::Add, lit_i32(3), lit_i32(4)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(7));
}

#[test]
fn int32_sub() {
    let ev = Eval::new(binop(BinaryOp::Sub, lit_i32(10), lit_i32(3)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(7));
}

#[test]
fn int32_mul() {
    let ev = Eval::new(binop(BinaryOp::Mul, lit_i32(3), lit_i32(4)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(12));
}

#[test]
fn int32_div() {
    let ev = Eval::new(binop(BinaryOp::Div, lit_i32(10), lit_i32(3)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(3));
}

#[test]
fn int32_mod() {
    let ev = Eval::new(binop(BinaryOp::Mod, lit_i32(10), lit_i32(3)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(1));
}

#[test]
fn int32_div_by_zero_returns_error() {
    let ev = Eval::new(binop(BinaryOp::Div, lit_i32(5), lit_i32(0)));
    assert!(matches!(ev.eval(&[]).unwrap_err(), EvalError::DivByZero));
}

#[test]
fn int32_overflow_returns_error() {
    let ev = Eval::new(binop(BinaryOp::Add, lit_i32(i32::MAX), lit_i32(1)));
    assert!(matches!(ev.eval(&[]).unwrap_err(), EvalError::Overflow));
}

// -----------------------------------------------------------------------
// Arithmetic: Float64
// -----------------------------------------------------------------------

#[test]
fn float64_add() {
    let ev = Eval::new(binop(BinaryOp::Add, lit_f64(1.5), lit_f64(2.5)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(4.0));
}

#[test]
fn float64_div_by_zero_returns_error() {
    let ev = Eval::new(binop(BinaryOp::Div, lit_f64(5.0), lit_f64(0.0)));
    assert!(matches!(ev.eval(&[]).unwrap_err(), EvalError::DivByZero));
}

#[test]
fn decimal_multiplies_integer_literal() {
    let ev = Eval::new(binop(BinaryOp::Mul, lit_i32(100), lit_decimal(1234, 2)));
    assert_eq!(
        ev.eval(&[]).unwrap(),
        Value::Decimal {
            value: 123400,
            scale: 2
        }
    );
}

#[test]
fn decimal_mixed_with_float_returns_float64() {
    let ev = Eval::new(binop(BinaryOp::Mul, lit_decimal(2, 1), lit_f64(18.0)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(3.6));
}

#[test]
fn decimal_divides_float_literal() {
    let ev = Eval::new(binop(BinaryOp::Div, lit_decimal(12345, 2), lit_f64(7.0)));
    let Value::Float64(v) = ev.eval(&[]).unwrap() else {
        panic!("expected Float64");
    };
    assert!((v - 17.635_714_285_714_286).abs() < f64::EPSILON);
}

#[test]
fn decimal_division_rounds_to_result_scale() {
    let ev = Eval::new(binop(BinaryOp::Div, lit_decimal(1, 0), lit_decimal(6, 0)));
    assert_eq!(
        ev.eval(&[]).unwrap(),
        Value::Decimal {
            value: 166_667,
            scale: 6
        }
    );
}

#[test]
fn decimal_compares_float_literal() {
    let ev = Eval::new(binop(BinaryOp::Lt, lit_decimal(123, 2), lit_f64(2.0)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn decimal_compare_handles_large_scale_gap_without_overflow() {
    let ev = Eval::new(binop(BinaryOp::Gt, lit_decimal(1, 0), lit_decimal(2, 100)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

// -----------------------------------------------------------------------
// NULL propagation through arithmetic
// -----------------------------------------------------------------------

#[test]
fn null_propagates_through_add() {
    let ev = Eval::new(binop(BinaryOp::Add, lit_null(), lit_i32(5)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
}

#[test]
fn null_propagates_through_mul_right() {
    let ev = Eval::new(binop(BinaryOp::Mul, lit_i32(3), lit_null()));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
}

// -----------------------------------------------------------------------
// Comparison: Int32
// -----------------------------------------------------------------------

#[test]
fn int32_eq_true() {
    let ev = Eval::new(binop(BinaryOp::Eq, lit_i32(7), lit_i32(7)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn int32_eq_false() {
    let ev = Eval::new(binop(BinaryOp::Eq, lit_i32(7), lit_i32(8)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn int32_lt() {
    let ev = Eval::new(binop(BinaryOp::Lt, lit_i32(3), lit_i32(7)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

// -----------------------------------------------------------------------
// Comparison: Text
// -----------------------------------------------------------------------

#[test]
fn text_eq() {
    let ev = Eval::new(binop(BinaryOp::Eq, lit_text("hello"), lit_text("hello")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn text_lt() {
    let ev = Eval::new(binop(BinaryOp::Lt, lit_text("abc"), lit_text("abd")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

// -----------------------------------------------------------------------
// NULL comparison returns NULL
// -----------------------------------------------------------------------

#[test]
fn null_eq_null_returns_null() {
    let ev = Eval::new(binop(BinaryOp::Eq, lit_null(), lit_null()));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
}

#[test]
fn record_eq_uses_three_valued_field_semantics() {
    let equal = Eval::new(binop(
        BinaryOp::Eq,
        lit_record(vec![Value::Int32(5), Value::Int32(10)]),
        lit_record(vec![Value::Int32(5), Value::Int32(10)]),
    ));
    assert_eq!(equal.eval(&[]).unwrap(), Value::Bool(true));

    let unknown = Eval::new(binop(
        BinaryOp::Eq,
        lit_record(vec![Value::Int32(5), Value::Null]),
        lit_record(vec![Value::Int32(5), Value::Int32(10)]),
    ));
    assert_eq!(unknown.eval(&[]).unwrap(), Value::Null);

    let different = Eval::new(binop(
        BinaryOp::Eq,
        lit_record(vec![Value::Int32(5), Value::Null]),
        lit_record(vec![Value::Int32(6), Value::Null]),
    ));
    assert_eq!(different.eval(&[]).unwrap(), Value::Bool(false));
}

// -----------------------------------------------------------------------
// Kleene AND/OR
// -----------------------------------------------------------------------
