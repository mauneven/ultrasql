//! Kleene logic, boolean predicate, concat, and row_to_json tests.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn kleene_null_and_false_is_false() {
    let ev = Eval::new(binop(BinaryOp::And, lit_null(), lit_bool(false)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn kleene_false_and_null_is_false() {
    let ev = Eval::new(binop(BinaryOp::And, lit_bool(false), lit_null()));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
}

#[test]
fn kleene_null_and_true_is_null() {
    let ev = Eval::new(binop(BinaryOp::And, lit_null(), lit_bool(true)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
}

#[test]
fn kleene_null_or_true_is_true() {
    let ev = Eval::new(binop(BinaryOp::Or, lit_null(), lit_bool(true)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn kleene_null_or_false_is_null() {
    let ev = Eval::new(binop(BinaryOp::Or, lit_null(), lit_bool(false)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
}

#[test]
fn kleene_true_and_true_is_true() {
    let ev = Eval::new(binop(BinaryOp::And, lit_bool(true), lit_bool(true)));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn is_boolean_predicates_use_sql_truth_table() {
    let eval_call = |name: &str, arg: ScalarExpr| {
        Eval::new(call(name, vec![arg], DataType::Bool))
            .eval(&[])
            .unwrap()
    };

    assert_eq!(eval_call("is_true", lit_bool(true)), Value::Bool(true));
    assert_eq!(eval_call("is_true", lit_bool(false)), Value::Bool(false));
    assert_eq!(eval_call("is_true", lit_null()), Value::Bool(false));
    assert_eq!(eval_call("is_not_true", lit_bool(false)), Value::Bool(true));
    assert_eq!(eval_call("is_not_true", lit_null()), Value::Bool(true));

    assert_eq!(eval_call("is_false", lit_bool(false)), Value::Bool(true));
    assert_eq!(eval_call("is_false", lit_bool(true)), Value::Bool(false));
    assert_eq!(eval_call("is_false", lit_null()), Value::Bool(false));
    assert_eq!(eval_call("is_not_false", lit_bool(true)), Value::Bool(true));
    assert_eq!(eval_call("is_not_false", lit_null()), Value::Bool(true));
}

#[test]
fn apply_binary_rejects_logical_ops_without_panicking() {
    for op in [BinaryOp::And, BinaryOp::Or] {
        let err = apply_binary(op, Value::Bool(true), Value::Bool(false))
            .expect_err("logical operators are evaluated by short-circuit path");
        assert!(
            err.to_string().contains("short-circuit"),
            "unexpected error: {err}"
        );
    }
}

// -----------------------------------------------------------------------
// Concat
// -----------------------------------------------------------------------

#[test]
fn concat_two_strings() {
    let ev = Eval::new(binop(BinaryOp::Concat, lit_text("foo"), lit_text("bar")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Text("foobar".into()));
}

#[test]
fn concat_null_propagation() {
    let ev = Eval::new(binop(BinaryOp::Concat, lit_null(), lit_text("bar")));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
}

#[test]
fn row_to_json_uses_record_field_names() {
    let record_type = DataType::Record(vec![
        ("id".to_owned(), DataType::Int32),
        ("name".to_owned(), DataType::Text { max_len: None }),
        ("meta".to_owned(), DataType::Jsonb),
    ]);
    let ev = Eval::new(call(
        "row_to_json",
        vec![call(
            "row",
            vec![
                lit_i32(1),
                lit_text("Ada"),
                lit_jsonb("{\"kind\":\"guide\"}"),
            ],
            record_type,
        )],
        DataType::Jsonb,
    ));
    let Value::Jsonb(json) = ev.eval(&[]).unwrap() else {
        panic!("expected jsonb row object");
    };
    let got: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        got,
        serde_json::json!({
            "id": 1,
            "name": "Ada",
            "meta": {"kind": "guide"},
        })
    );
}

