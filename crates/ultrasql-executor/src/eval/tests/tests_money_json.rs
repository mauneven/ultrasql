//! Money arithmetic, JSONB/array containment, and substring tests.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn money_addition_and_subtraction_evaluate() {
    let add = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(lit_money(125)),
        right: Box::new(lit_money(375)),
        data_type: DataType::Money,
    });
    assert_eq!(add.eval(&[]).expect("money add"), Value::Money(500));

    let sub = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Sub,
        left: Box::new(lit_money(500)),
        right: Box::new(lit_money(125)),
        data_type: DataType::Money,
    });
    assert_eq!(sub.eval(&[]).expect("money sub"), Value::Money(375));
}

#[test]
fn money_division_matrix_evaluates() {
    let ratio = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Div,
        left: Box::new(lit_money(500)),
        right: Box::new(lit_money(200)),
        data_type: DataType::Float64,
    });
    assert_eq!(ratio.eval(&[]).expect("money ratio"), Value::Float64(2.5));

    let divided = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Div,
        left: Box::new(lit_money(501)),
        right: Box::new(lit_i32(2)),
        data_type: DataType::Money,
    });
    assert_eq!(divided.eval(&[]).expect("money int div"), Value::Money(250));

    let zero_money = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Div,
        left: Box::new(lit_money(500)),
        right: Box::new(lit_money(0)),
        data_type: DataType::Float64,
    });
    assert!(matches!(zero_money.eval(&[]), Err(EvalError::DivByZero)));

    let zero_int = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Div,
        left: Box::new(lit_money(500)),
        right: Box::new(lit_i32(0)),
        data_type: DataType::Money,
    });
    assert!(matches!(zero_int.eval(&[]), Err(EvalError::DivByZero)));

    let rounded = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Div,
        left: Box::new(lit_money(501)),
        right: Box::new(lit_f64(2.0)),
        data_type: DataType::Money,
    });
    assert_eq!(
        rounded.eval(&[]).expect("money float div"),
        Value::Money(251)
    );
}

#[test]
fn money_scalar_multiplication_evaluates() {
    let money_int = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Mul,
        left: Box::new(lit_money(125)),
        right: Box::new(lit_i32(3)),
        data_type: DataType::Money,
    });
    assert_eq!(
        money_int.eval(&[]).expect("money int mul"),
        Value::Money(375)
    );

    let int_money = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Mul,
        left: Box::new(lit_i32(3)),
        right: Box::new(lit_money(125)),
        data_type: DataType::Money,
    });
    assert_eq!(
        int_money.eval(&[]).expect("int money mul"),
        Value::Money(375)
    );

    let money_float = Eval::new(ScalarExpr::Binary {
        op: BinaryOp::Mul,
        left: Box::new(lit_money(125)),
        right: Box::new(lit_f64(1.5)),
        data_type: DataType::Money,
    });
    assert_eq!(
        money_float.eval(&[]).expect("money float mul"),
        Value::Money(188)
    );
}

#[test]
fn money_unary_signs_evaluate() {
    let neg = Eval::new(ScalarExpr::Unary {
        op: UnaryOp::Neg,
        expr: Box::new(lit_money(125)),
        data_type: DataType::Money,
    });
    assert_eq!(neg.eval(&[]).expect("money neg"), Value::Money(-125));

    let pos = Eval::new(ScalarExpr::Unary {
        op: UnaryOp::Pos,
        expr: Box::new(lit_money(125)),
        data_type: DataType::Money,
    });
    assert_eq!(pos.eval(&[]).expect("money pos"), Value::Money(125));
}

#[test]
fn jsonb_contains_and_key_ops_evaluate() {
    let doc = lit_text(r#"{"a":1,"b":"two"}"#);
    let contains = Eval::new(binop(
        BinaryOp::JsonContains,
        doc.clone(),
        lit_text(r#"{"a":1}"#),
    ));
    assert_eq!(contains.eval(&[]).unwrap(), Value::Bool(true));

    let has_key = Eval::new(binop(BinaryOp::JsonHasKey, doc.clone(), lit_text("b")));
    assert_eq!(has_key.eval(&[]).unwrap(), Value::Bool(true));

    let has_all = Eval::new(binop(
        BinaryOp::JsonHasAllKeys,
        doc,
        lit_text(r#"["a","b"]"#),
    ));
    assert_eq!(has_all.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn native_jsonb_access_contains_and_key_ops_evaluate() {
    let doc = lit_jsonb(r#"{"a":1,"b":"x"}"#);
    let get_text = Eval::new(binop(BinaryOp::JsonGetText, doc.clone(), lit_text("b")));
    assert_eq!(get_text.eval(&[]).unwrap(), Value::Text("x".into()));

    let contains = Eval::new(binop(
        BinaryOp::JsonContains,
        doc.clone(),
        lit_jsonb(r#"{"a":1}"#),
    ));
    assert_eq!(contains.eval(&[]).unwrap(), Value::Bool(true));

    let has_key = Eval::new(binop(BinaryOp::JsonHasKey, doc, lit_text("b")));
    assert_eq!(has_key.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn array_contains_and_overlap_evaluate() {
    let contains = Eval::new(binop(
        BinaryOp::JsonContains,
        lit_text("{red,green,blue}"),
        lit_text("{red,blue}"),
    ));
    assert_eq!(contains.eval(&[]).unwrap(), Value::Bool(true));

    let overlaps = Eval::new(binop(
        BinaryOp::Overlap,
        lit_text("{red,green}"),
        lit_text("{yellow,green}"),
    ));
    assert_eq!(overlaps.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn native_array_contains_and_overlap_evaluate() {
    let contains = Eval::new(binop(
        BinaryOp::JsonContains,
        lit_text_array(&["red", "green", "blue"]),
        lit_text_array(&["red", "blue"]),
    ));
    assert_eq!(contains.eval(&[]).unwrap(), Value::Bool(true));

    let overlaps = Eval::new(binop(
        BinaryOp::Overlap,
        lit_text_array(&["red", "green"]),
        lit_text_array(&["yellow", "green"]),
    ));
    assert_eq!(overlaps.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn substring_accepts_bpchar_source() {
    let ev = Eval::new(call(
        "substring",
        vec![
            lit_char("13-111-1111    ", Some(15)),
            lit_i32(1),
            lit_i32(2),
        ],
        DataType::Text { max_len: None },
    ));

    assert_eq!(ev.eval(&[]).unwrap(), Value::Text("13".to_owned()));
}

#[test]
fn substring_counts_unicode_characters() {
    assert_eq!(
        eval_fn(
            "substring",
            vec![
                Value::Text("a\u{00e9}bc".to_owned()),
                Value::Int32(2),
                Value::Int32(1),
            ],
        ),
        Value::Text("\u{00e9}".to_owned())
    );
}

