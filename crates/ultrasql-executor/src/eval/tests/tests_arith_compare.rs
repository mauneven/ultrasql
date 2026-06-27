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
// Mixed integer/float comparison (PostgreSQL compares numerically)
// -----------------------------------------------------------------------

#[test]
fn mixed_int_float_comparison_compares_numerically() {
    // 3 < 2.5 -> false, 3 > 2.5 -> true, in both operand orders and widths.
    assert_eq!(
        apply_binary(BinaryOp::Lt, Value::Int32(3), Value::Float64(2.5)).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        apply_binary(BinaryOp::Gt, Value::Float64(2.5), Value::Int32(3)).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        apply_binary(BinaryOp::Gt, Value::Int32(3), Value::Float64(2.5)).unwrap(),
        Value::Bool(true)
    );
    // Equality across int/float (real column case): 3 = 3.0 -> true.
    assert_eq!(
        apply_binary(BinaryOp::Eq, Value::Int32(3), Value::Float32(3.0)).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        apply_binary(BinaryOp::Eq, Value::Float32(2.5), Value::Int32(3)).unwrap(),
        Value::Bool(false)
    );
    // All integer widths against both float widths, both orders.
    for cmp in [Value::Int16(3), Value::Int32(3), Value::Int64(3)] {
        for flt in [Value::Float32(2.5), Value::Float64(2.5)] {
            assert_eq!(
                apply_binary(BinaryOp::Gt, cmp.clone(), flt.clone()).unwrap(),
                Value::Bool(true),
                "{cmp:?} > {flt:?}"
            );
            assert_eq!(
                apply_binary(BinaryOp::Lt, flt.clone(), cmp.clone()).unwrap(),
                Value::Bool(true),
                "{flt:?} < {cmp:?}"
            );
        }
    }
}

#[test]
fn mixed_int_float_comparison_rejects_nan() {
    let err = apply_binary(BinaryOp::Lt, Value::Int32(1), Value::Float64(f64::NAN)).unwrap_err();
    assert!(matches!(err, EvalError::Type(_)), "unexpected: {err}");
}

// -----------------------------------------------------------------------
// Mixed integer/float arithmetic (PostgreSQL promotes to double precision)
// -----------------------------------------------------------------------

#[test]
fn mixed_int_float_arithmetic_returns_float64() {
    // bigint + float8, float8 * bigint
    assert_eq!(
        apply_binary(BinaryOp::Add, Value::Int64(5), Value::Float64(1.5)).unwrap(),
        Value::Float64(6.5)
    );
    assert_eq!(
        apply_binary(BinaryOp::Mul, Value::Float64(1.5), Value::Int64(2)).unwrap(),
        Value::Float64(3.0)
    );
    // real + int4 and int4 * real both promote to float8 (double precision).
    assert_eq!(
        apply_binary(BinaryOp::Add, Value::Float32(1.5), Value::Int32(2)).unwrap(),
        Value::Float64(3.5)
    );
    assert_eq!(
        apply_binary(BinaryOp::Mul, Value::Int32(2), Value::Float32(1.5)).unwrap(),
        Value::Float64(3.0)
    );
    // Every (int width x float width) pair in both orders yields Float64.
    for i in [Value::Int16(4), Value::Int32(4), Value::Int64(4)] {
        for f in [Value::Float32(2.0), Value::Float64(2.0)] {
            assert!(
                matches!(
                    apply_binary(BinaryOp::Add, i.clone(), f.clone()).unwrap(),
                    Value::Float64(_)
                ),
                "{i:?} + {f:?} should be Float64"
            );
            assert!(
                matches!(
                    apply_binary(BinaryOp::Sub, f.clone(), i.clone()).unwrap(),
                    Value::Float64(_)
                ),
                "{f:?} - {i:?} should be Float64"
            );
        }
    }
}

// -----------------------------------------------------------------------
// `^` power operator: PostgreSQL returns double precision, no overflow
// -----------------------------------------------------------------------

#[test]
fn power_operator_returns_double_precision() {
    // 2 ^ 3 = 8 (double precision)
    assert_eq!(
        apply_binary(BinaryOp::Pow, Value::Int32(2), Value::Int32(3)).unwrap(),
        Value::Float64(8.0)
    );
    // 10 ^ 19 does not overflow (integer power would).
    assert_eq!(
        apply_binary(BinaryOp::Pow, Value::Int32(10), Value::Int32(19)).unwrap(),
        Value::Float64(1e19)
    );
    // 2 ^ 0.5 = sqrt(2)
    let Value::Float64(root) =
        apply_binary(BinaryOp::Pow, Value::Int32(2), Value::Float64(0.5)).unwrap()
    else {
        panic!("expected Float64");
    };
    assert!((root - std::f64::consts::SQRT_2).abs() < 1e-12);
    // bigint base no longer overflows.
    assert_eq!(
        apply_binary(BinaryOp::Pow, Value::Int64(10), Value::Int64(19)).unwrap(),
        Value::Float64(1e19)
    );
    // numeric ^ int returns the value as double precision (6.25).
    assert_eq!(
        apply_binary(
            BinaryOp::Pow,
            Value::Decimal {
                value: 25,
                scale: 1
            },
            Value::Int32(2),
        )
        .unwrap(),
        Value::Float64(6.25)
    );
}

// -----------------------------------------------------------------------
// Kleene AND/OR
// -----------------------------------------------------------------------
