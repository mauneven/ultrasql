//! Array scalar, multidimensional, and full-text-search tests.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn array_scalar_functions_evaluate() {
    let string_to_array = call(
        "string_to_array",
        vec![lit_text("red,green,blue"), lit_text(",")],
        DataType::Array(Box::new(DataType::Text { max_len: None })),
    );
    let parsed = Eval::new(string_to_array.clone()).eval(&[]).unwrap();
    assert_eq!(
        parsed,
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![
                Value::Text("red".into()),
                Value::Text("green".into()),
                Value::Text("blue".into())
            ]
        }
    );

    let len = Eval::new(call(
        "array_length",
        vec![string_to_array.clone(), lit_i32(1)],
        DataType::Int32,
    ));
    assert_eq!(len.eval(&[]).unwrap(), Value::Int32(3));

    let joined = Eval::new(call(
        "array_to_string",
        vec![string_to_array, lit_text("|")],
        DataType::Text { max_len: None },
    ));
    assert_eq!(
        joined.eval(&[]).unwrap(),
        Value::Text("red|green|blue".into())
    );

    let cat = Eval::new(call(
        "array_cat",
        vec![lit_text_array(&["red"]), lit_text_array(&["green"])],
        DataType::Array(Box::new(DataType::Text { max_len: None })),
    ));
    assert_eq!(
        cat.eval(&[]).unwrap(),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![Value::Text("red".into()), Value::Text("green".into())]
        }
    );

    assert_eq!(
        eval_fn(
            "array_append",
            vec![
                text_array_value(&["red", "green"]),
                Value::Text("blue".into())
            ]
        ),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![
                Value::Text("red".into()),
                Value::Text("green".into()),
                Value::Text("blue".into())
            ]
        }
    );
    assert_eq!(
        eval_fn(
            "array_prepend",
            vec![Value::Text("red".into()), text_array_value(&["green"])]
        ),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![Value::Text("red".into()), Value::Text("green".into())]
        }
    );
    assert_eq!(
        eval_fn(
            "array_remove",
            vec![
                text_array_value(&["red", "green", "red"]),
                Value::Text("red".into())
            ]
        ),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![Value::Text("green".into())]
        }
    );

    let matrix = Value::Array {
        element_type: DataType::Array(Box::new(DataType::Int32)),
        elements: vec![
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Int32(2)],
            },
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(3), Value::Int32(4)],
            },
        ],
    };
    assert_eq!(
        eval_fn("cardinality", vec![matrix.clone()]),
        Value::Int32(4)
    );
    assert_eq!(
        eval_fn("array_ndims", vec![matrix.clone()]),
        Value::Int32(2)
    );
    assert_eq!(
        eval_fn("array_lower", vec![matrix.clone(), Value::Int32(1)]),
        Value::Int32(1)
    );
    assert_eq!(
        eval_fn("array_upper", vec![matrix.clone(), Value::Int32(2)]),
        Value::Int32(2)
    );
    assert_eq!(
        eval_fn("array_dims", vec![matrix]),
        Value::Text("[1:2][1:2]".into())
    );

    assert_eq!(
        eval_fn(
            "array_replace",
            vec![
                text_array_value(&["red", "green", "red"]),
                Value::Text("red".into()),
                Value::Text("blue".into())
            ]
        ),
        text_array_value(&["blue", "green", "blue"])
    );
    assert_eq!(
        eval_fn(
            "array_positions",
            vec![
                text_array_value(&["red", "green", "red"]),
                Value::Text("red".into())
            ]
        ),
        Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1), Value::Int32(3)]
        }
    );
    assert_eq!(
        eval_fn(
            "trim_array",
            vec![text_array_value(&["red", "green", "blue"]), Value::Int32(1)]
        ),
        text_array_value(&["red", "green"])
    );
}

#[test]
fn multidimensional_array_length_evaluates_dimensions() {
    let matrix_type = DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int32))));
    let matrix = ScalarExpr::Literal {
        value: Value::Array {
            element_type: DataType::Array(Box::new(DataType::Int32)),
            elements: vec![
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1), Value::Int32(2)],
                },
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(3), Value::Int32(4)],
                },
            ],
        },
        data_type: matrix_type,
    };

    let len_dim_1 = Eval::new(call(
        "array_length",
        vec![matrix.clone(), lit_i32(1)],
        DataType::Int32,
    ));
    assert_eq!(len_dim_1.eval(&[]).unwrap(), Value::Int32(2));

    let len_dim_2 = Eval::new(call(
        "array_length",
        vec![matrix.clone(), lit_i32(2)],
        DataType::Int32,
    ));
    assert_eq!(len_dim_2.eval(&[]).unwrap(), Value::Int32(2));

    let len_dim_3 = Eval::new(call(
        "array_length",
        vec![matrix, lit_i32(3)],
        DataType::Int32,
    ));
    assert_eq!(len_dim_3.eval(&[]).unwrap(), Value::Null);
}

#[test]
fn multidimensional_array_to_string_flattens_elements() {
    let matrix = ScalarExpr::Literal {
        value: Value::Array {
            element_type: DataType::Array(Box::new(DataType::Int32)),
            elements: vec![
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1), Value::Int32(2)],
                },
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(3), Value::Int32(4)],
                },
            ],
        },
        data_type: DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int32)))),
    };
    let joined = Eval::new(call(
        "array_to_string",
        vec![matrix, lit_text(":")],
        DataType::Text { max_len: None },
    ));
    assert_eq!(joined.eval(&[]).unwrap(), Value::Text("1:2:3:4".into()));
}

#[test]
fn tsvector_match_evaluates() {
    let ev = Eval::new(binop(
        BinaryOp::TextSearchMatch,
        lit_text("quick brown fox"),
        lit_text("quick & fox"),
    ));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
}

#[test]
fn text_search_constructor_functions_evaluate() {
    let vector = Eval::new(call(
        "to_tsvector",
        vec![lit_text("The Quick brown fox")],
        DataType::Text { max_len: None },
    ))
    .eval(&[])
    .unwrap();
    assert_eq!(vector, Value::Text("the:1 quick:2 brown:3 fox:4".into()));

    let query = Eval::new(call(
        "to_tsquery",
        vec![lit_text("Quick fox")],
        DataType::Text { max_len: None },
    ))
    .eval(&[])
    .unwrap();
    assert_eq!(query, Value::Text("quick & fox".into()));

    let rank = Eval::new(call(
        "ts_rank_cd",
        vec![
            lit_text("the:1 quick:2 brown:3 fox:4"),
            lit_text("quick & missing"),
        ],
        DataType::Float64,
    ))
    .eval(&[])
    .unwrap();
    assert_eq!(rank, Value::Float64(0.5));

    let rank_extra_arg = Eval::new(call(
        "ts_rank_cd",
        vec![
            lit_text("ignored"),
            lit_text("the:1 quick:2 brown:3 fox:4"),
            lit_text("quick & missing"),
        ],
        DataType::Float64,
    ))
    .eval(&[])
    .unwrap_err()
    .to_string();
    assert!(rank_extra_arg.contains("expected 2 args"));

    let headline = Eval::new(call(
        "ts_headline",
        vec![lit_text("The Quick brown fox."), lit_text("quick & fox")],
        DataType::Text { max_len: None },
    ))
    .eval(&[])
    .unwrap();
    assert_eq!(
        headline,
        Value::Text("The <b>Quick</b> brown <b>fox</b>.".into())
    );

    let node_count = Eval::new(call(
        "numnode",
        vec![lit_text("quick & missing")],
        DataType::Int32,
    ))
    .eval(&[])
    .unwrap();
    assert_eq!(node_count, Value::Int32(2));

    let querytree = Eval::new(call(
        "querytree",
        vec![lit_text("Quick & missing")],
        DataType::Text { max_len: None },
    ))
    .eval(&[])
    .unwrap();
    assert_eq!(querytree, Value::Text("quick & missing".into()));
}

// -----------------------------------------------------------------------
// Parameter substitution
// -----------------------------------------------------------------------

