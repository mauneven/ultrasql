//! Vector distance/norm/dims tests.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn vector_l2_distance_evaluates() {
    let ev = Eval::new(binop(
        BinaryOp::VectorL2Distance,
        lit_vector(vec![1.0, 2.0, 3.0]),
        lit_vector(vec![1.0, 2.0, 4.0]),
    ));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(1.0));
}

#[test]
fn vector_negative_inner_product_evaluates() {
    let ev = Eval::new(binop(
        BinaryOp::VectorNegativeInnerProduct,
        lit_vector(vec![1.0, 2.0, 3.0]),
        lit_vector(vec![4.0, 5.0, 6.0]),
    ));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(-32.0));
}

#[test]
fn vector_inner_product_functions_evaluate_positive_dot() {
    for name in ["inner_product", "dot_product"] {
        let ev = Eval::new(call(
            name,
            vec![
                lit_vector(vec![1.0, 2.0, 3.0]),
                lit_vector(vec![4.0, 5.0, 6.0]),
            ],
            DataType::Float64,
        ));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(32.0), "{name}");
    }
}

#[test]
fn vector_distance_functions_evaluate() {
    let l2 = Eval::new(call(
        "l2_distance",
        vec![
            lit_vector(vec![1.0, 2.0, 3.0]),
            lit_vector(vec![1.0, 2.0, 4.0]),
        ],
        DataType::Float64,
    ));
    assert_eq!(l2.eval(&[]).unwrap(), Value::Float64(1.0));

    let cosine = Eval::new(call(
        "cosine_distance",
        vec![lit_vector(vec![1.0, 0.0]), lit_vector(vec![0.0, 1.0])],
        DataType::Float64,
    ));
    assert_eq!(cosine.eval(&[]).unwrap(), Value::Float64(1.0));

    let l1 = Eval::new(call(
        "l1_distance",
        vec![
            lit_vector(vec![1.0, 2.0, 3.0]),
            lit_vector(vec![3.0, 2.0, -1.0]),
        ],
        DataType::Float64,
    ));
    assert_eq!(l1.eval(&[]).unwrap(), Value::Float64(6.0));
}

#[test]
fn vector_norm_function_returns_euclidean_norm() {
    for (name, expr) in [
        ("vector_norm", lit_vector(vec![3.0, 4.0])),
        ("l2_norm", lit_halfvec(vec![3.0, 4.0])),
        ("sparse-l2_norm", lit_sparsevec(4, vec![(1, 3.0), (4, 4.0)])),
    ] {
        let func_name = if name == "sparse-l2_norm" {
            "l2_norm"
        } else {
            name
        };
        let ev = Eval::new(call(func_name, vec![expr], DataType::Float64));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(5.0), "{name}");
    }
}

#[test]
fn vector_dims_function_returns_dimension() {
    for expr in [
        lit_vector(vec![1.0, 2.0, 3.0]),
        lit_halfvec(vec![1.0, 2.0, 3.0]),
        lit_sparsevec(3, vec![(1, 1.0), (3, 3.0)]),
    ] {
        let ev = Eval::new(call("vector_dims", vec![expr], DataType::Int32));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(3));
    }
}

#[test]
fn halfvec_distance_operators_evaluate() {
    let l2 = Eval::new(binop(
        BinaryOp::VectorL2Distance,
        lit_halfvec(vec![1.0, 2.0, 3.0]),
        lit_halfvec(vec![1.0, 2.0, 4.0]),
    ));
    assert_eq!(l2.eval(&[]).unwrap(), Value::Float64(1.0));

    let inner = Eval::new(binop(
        BinaryOp::VectorNegativeInnerProduct,
        lit_halfvec(vec![1.0, 2.0, 3.0]),
        lit_halfvec(vec![4.0, 5.0, 6.0]),
    ));
    assert_eq!(inner.eval(&[]).unwrap(), Value::Float64(-32.0));
}

#[test]
fn sparsevec_distance_operators_evaluate_without_dense_expansion() {
    let left = lit_sparsevec(5, vec![(1, 1.0), (3, 2.0), (5, -1.0)]);
    let right = lit_sparsevec(5, vec![(1, 2.0), (4, 3.0), (5, 1.0)]);

    let l2 = Eval::new(binop(
        BinaryOp::VectorL2Distance,
        left.clone(),
        right.clone(),
    ));
    assert_eq!(l2.eval(&[]).unwrap(), Value::Float64(18.0_f64.sqrt()));

    let inner = Eval::new(binop(
        BinaryOp::VectorNegativeInnerProduct,
        left.clone(),
        right.clone(),
    ));
    assert_eq!(inner.eval(&[]).unwrap(), Value::Float64(-1.0));

    let l1 = Eval::new(binop(BinaryOp::VectorL1Distance, left, right));
    assert_eq!(l1.eval(&[]).unwrap(), Value::Float64(8.0));
}

#[test]
fn vector_cosine_distance_evaluates() {
    let ev = Eval::new(binop(
        BinaryOp::VectorCosineDistance,
        lit_vector(vec![1.0, 0.0]),
        lit_vector(vec![0.0, 1.0]),
    ));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(1.0));
}

#[test]
fn vector_l1_distance_evaluates() {
    let ev = Eval::new(binop(
        BinaryOp::VectorL1Distance,
        lit_vector(vec![1.0, 2.0, 3.0]),
        lit_vector(vec![3.0, 2.0, -1.0]),
    ));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(6.0));
}

#[test]
fn vector_distance_rejects_runtime_dimension_mismatch() {
    let ev = Eval::new(binop(
        BinaryOp::VectorL2Distance,
        lit_vector(vec![1.0, 2.0, 3.0]),
        lit_vector(vec![1.0, 2.0]),
    ));
    let err = ev.eval(&[]).unwrap_err();
    assert!(matches!(err, EvalError::Type(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// LIKE / ILIKE
// -----------------------------------------------------------------------
